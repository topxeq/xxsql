// Package executor provides SQL query execution for XxSql.
package executor

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/topxeq/xxsql/internal/backup"
	"github.com/topxeq/xxsql/internal/sql"
	"github.com/topxeq/xxsql/internal/storage"
	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/table"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// Permission represents a permission bit.
type Permission uint32

const (
	PermManageUsers Permission = 1 << iota
	PermManageConfig
	PermStartStop
	PermCreateTable
	PermDropTable
	PermCreateDatabase
	PermDropDatabase
	PermSelect
	PermInsert
	PermUpdate
	PermDelete
	PermCreateIndex
	PermDropIndex
	PermBackup
	PermRestore
)

// PermissionChecker checks if a permission is granted.
type PermissionChecker interface {
	HasPermission(perm Permission) bool
}

// SessionPermissionAdapter adapts auth session permissions to executor permissions.
type SessionPermissionAdapter struct {
	hasPerm func(perm uint32) bool
}

// NewSessionPermissionAdapter creates a new adapter from a session's HasPermission function.
func NewSessionPermissionAdapter(hasPermFunc func(perm uint32) bool) *SessionPermissionAdapter {
	return &SessionPermissionAdapter{hasPerm: hasPermFunc}
}

// HasPermission implements PermissionChecker.
func (a *SessionPermissionAdapter) HasPermission(perm Permission) bool {
	if a.hasPerm == nil {
		return true // No permission check, allow all
	}
	return a.hasPerm(uint32(perm))
}

// Result represents the result of a query execution.
type Result struct {
	Columns    []ColumnInfo
	Rows       [][]interface{}
	RowCount   int
	Affected   int
	LastInsert uint64
	Message    string
}

// ColumnInfo represents column information for results.
type ColumnInfo struct {
	Name  string
	Type  string
	Alias string // optional alias for ORDER BY support
}

// Executor executes SQL queries against the storage engine.
type Executor struct {
	engine        *storage.Engine
	database      string
	perms         PermissionChecker
	authMgr       AuthManager
	udfManager    *UDFManager
	outerContext  map[string]interface{} // For correlated subqueries
	currentTable  string                 // Current table being queried (for outer context)
	subqueryCache map[string]interface{} // Cache for non-correlated subquery results (optimization)
}

// Note on Subquery Optimization:
// The current implementation evaluates correlated subqueries by re-executing the subquery
// for each row from the outer query. This works correctly but may not be optimal for
// large datasets. Future optimizations could include:
// 1. Decorrelation: Transform correlated subqueries into JOINs where possible
// 2. Result caching: Cache results of non-correlated subqueries
// 3. Index utilization: Use indexes for faster subquery lookups
// The subqueryCache field is reserved for implementing result caching in the future.

// AuthManager interface for auth operations.
type AuthManager interface {
	CreateUser(username, password string, role int) (interface{}, error)
	DeleteUser(username string) error
	GetUser(username string) (interface{}, error)
	ChangePassword(username, oldPassword, newPassword string) error
	GrantGlobal(username string, priv interface{}) error
	GrantDatabase(username, database string, priv interface{}) error
	GrantTable(username, database, table string, priv interface{}) error
	RevokeGlobal(username string, priv interface{}) error
	RevokeDatabase(username, database string, priv interface{}) error
	RevokeTable(username, database, table string, priv interface{}) error
	GetGrants(username string) ([]string, error)
}

// NewExecutor creates a new executor.
func NewExecutor(engine *storage.Engine) *Executor {
	return &Executor{
		engine:        engine,
		subqueryCache: make(map[string]interface{}),
	}
}

// SetUDFManager sets the UDF manager for the executor.
func (e *Executor) SetUDFManager(m *UDFManager) {
	e.udfManager = m
}

// SetDatabase sets the current database.
func (e *Executor) SetDatabase(db string) {
	e.database = db
}

// SetPermissionChecker sets the permission checker.
func (e *Executor) SetPermissionChecker(checker PermissionChecker) {
	e.perms = checker
}

// SetAuthManager sets the auth manager.
func (e *Executor) SetAuthManager(mgr AuthManager) {
	e.authMgr = mgr
}

// Execute executes a SQL statement.
func (e *Executor) Execute(sqlStr string) (*Result, error) {
	return e.ExecuteWithPerms(sqlStr, e.perms)
}

// ExecuteWithPerms executes a SQL statement with a specific permission checker.
func (e *Executor) ExecuteWithPerms(sqlStr string, checker PermissionChecker) (*Result, error) {
	// Parse the SQL statement
	stmt, err := sql.Parse(sqlStr)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	// Helper to check permission
	checkPerm := func(perm Permission) error {
		if checker == nil {
			return nil // No permission checker, allow all
		}
		if !checker.HasPermission(perm) {
			return fmt.Errorf("permission denied")
		}
		return nil
	}

	// Execute based on statement type
	switch s := stmt.(type) {
	case *sql.SelectStmt:
		if err := checkPerm(PermSelect); err != nil {
			return nil, err
		}
		return e.executeSelect(s)
	case *sql.InsertStmt:
		if err := checkPerm(PermInsert); err != nil {
			return nil, err
		}
		return e.executeInsert(s)
	case *sql.UpdateStmt:
		if err := checkPerm(PermUpdate); err != nil {
			return nil, err
		}
		return e.executeUpdate(s)
	case *sql.DeleteStmt:
		if err := checkPerm(PermDelete); err != nil {
			return nil, err
		}
		return e.executeDelete(s)
	case *sql.CreateTableStmt:
		if err := checkPerm(PermCreateTable); err != nil {
			return nil, err
		}
		return e.executeCreateTable(s)
	case *sql.DropTableStmt:
		if err := checkPerm(PermDropTable); err != nil {
			return nil, err
		}
		return e.executeDropTable(s)
	case *sql.CreateIndexStmt:
		if err := checkPerm(PermCreateIndex); err != nil {
			return nil, err
		}
		return e.executeCreateIndex(s)
	case *sql.DropIndexStmt:
		if err := checkPerm(PermDropIndex); err != nil {
			return nil, err
		}
		return e.executeDropIndex(s)
	case *sql.AlterTableStmt:
		if err := checkPerm(PermCreateTable); err != nil {
			return nil, err
		}
		return e.executeAlterTable(s)
	case *sql.UseStmt:
		return e.executeUse(s)
	case *sql.ShowStmt:
		return e.executeShow(s)
	case *sql.TruncateTableStmt:
		if err := checkPerm(PermDropTable); err != nil {
			return nil, err
		}
		return e.executeTruncate(s)
	case *sql.CreateUserStmt:
		if err := checkPerm(PermManageUsers); err != nil {
			return nil, err
		}
		return e.executeCreateUser(s)
	case *sql.DropUserStmt:
		if err := checkPerm(PermManageUsers); err != nil {
			return nil, err
		}
		return e.executeDropUser(s)
	case *sql.AlterUserStmt:
		if err := checkPerm(PermManageUsers); err != nil {
			return nil, err
		}
		return e.executeAlterUser(s)
	case *sql.SetPasswordStmt:
		return e.executeSetPassword(s)
	case *sql.GrantStmt:
		if err := checkPerm(PermManageUsers); err != nil {
			return nil, err
		}
		return e.executeGrant(s)
	case *sql.RevokeStmt:
		if err := checkPerm(PermManageUsers); err != nil {
			return nil, err
		}
		return e.executeRevoke(s)
	case *sql.ShowGrantsStmt:
		return e.executeShowGrants(s)
	case *sql.UnionStmt:
		if err := checkPerm(PermSelect); err != nil {
			return nil, err
		}
		return e.executeUnion(s)
	case *sql.DescribeStmt:
		if err := checkPerm(PermSelect); err != nil {
			return nil, err
		}
		return e.executeDescribe(s)
	case *sql.ShowCreateTableStmt:
		if err := checkPerm(PermSelect); err != nil {
			return nil, err
		}
		return e.executeShowCreateTable(s)
	case *sql.BackupStmt:
		if err := checkPerm(PermBackup); err != nil {
			return nil, err
		}
		return e.executeBackup(s)
	case *sql.RestoreStmt:
		if err := checkPerm(PermRestore); err != nil {
			return nil, err
		}
		return e.executeRestore(s)
	case *sql.CreateFunctionStmt:
		return e.executeCreateFunction(s)
	case *sql.DropFunctionStmt:
		return e.executeDropFunction(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// executeSelect executes a SELECT statement.
func (e *Executor) executeSelect(stmt *sql.SelectStmt) (*Result, error) {
	// Check for JOINs
	if stmt.From != nil && len(stmt.From.Joins) > 0 {
		return e.executeSelectWithJoin(stmt)
	}

	// Check if we have a FROM clause
	if stmt.From == nil || stmt.From.Table == nil {
		// SELECT without FROM (e.g., SELECT 1)
		return e.executeSelectWithoutFrom(stmt)
	}

	// Check if this is a derived table (subquery in FROM clause)
	if stmt.From.Table.Subquery != nil {
		return e.executeSelectFromDerivedTable(stmt)
	}

	tableName := stmt.From.Table.Name
	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	// Set current table for correlated subqueries
	oldTable := e.currentTable
	e.currentTable = strings.ToLower(tableName)
	defer func() { e.currentTable = oldTable }()

	// Get the table
	tbl, err := e.engine.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Get column info
	tblInfo := tbl.GetInfo()
	columnMap := make(map[string]*types.ColumnInfo)
	columnOrder := make([]*types.ColumnInfo, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		columnMap[strings.ToLower(col.Name)] = col
		columnOrder[i] = col
	}

	// Scan all rows
	rows, err := e.engine.Scan(tableName)
	if err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}

	// Determine result columns
	var resultCols []ColumnInfo
	var colIndices []int
	var funcExprs []sql.Expression // Non-aggregate function expressions
	var aggregateFuncs []struct {
		name  string
		arg   string // column name for the aggregate argument
		index int    // result column index
	}

	for _, colExpr := range stmt.Columns {
		switch expr := colExpr.(type) {
		case *sql.StarExpr:
			// SELECT * - include all columns
			for i, col := range tblInfo.Columns {
				resultCols = append(resultCols, ColumnInfo{
					Name: col.Name,
					Type: col.Type.String(),
				})
				colIndices = append(colIndices, i)
				funcExprs = append(funcExprs, nil)
			}
		case *sql.ColumnRef:
			colName := strings.ToLower(expr.Name)
			idx := -1
			for i, col := range tblInfo.Columns {
				if strings.ToLower(col.Name) == colName {
					idx = i
					ci := ColumnInfo{
						Name: col.Name,
						Type: col.Type.String(),
					}
					if expr.Alias != "" {
						ci.Alias = expr.Alias
					}
					resultCols = append(resultCols, ci)
					break
				}
			}
			if idx == -1 {
				return nil, fmt.Errorf("unknown column: %s", expr.Name)
			}
			colIndices = append(colIndices, idx)
			funcExprs = append(funcExprs, nil)
		case *sql.FunctionCall:
			funcName := strings.ToUpper(expr.Name)
			colName := ""
			if len(expr.Args) > 0 {
				if _, ok := expr.Args[0].(*sql.StarExpr); ok {
					colName = "*"
				} else if colRef, ok := expr.Args[0].(*sql.ColumnRef); ok {
					colName = strings.ToLower(colRef.Name)
				}
			}

			// Check if it's an aggregate function
			isAggregate := funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX"

			// Determine result type
			resultType := "VARCHAR"
			if isAggregate {
				resultType = "INT"
				if funcName == "AVG" {
					resultType = "FLOAT"
				}
			}

			resultCols = append(resultCols, ColumnInfo{
				Name: expr.Name + "()",
				Type: resultType,
			})
			if isAggregate {
				aggregateFuncs = append(aggregateFuncs, struct {
					name  string
					arg   string
					index int
				}{funcName, colName, len(resultCols) - 1})
				colIndices = append(colIndices, -1)
				funcExprs = append(funcExprs, nil)
			} else {
				// Non-aggregate function - will be evaluated per row
				colIndices = append(colIndices, -1)
				funcExprs = append(funcExprs, expr)
			}
		case *sql.SubqueryExpr, *sql.ScalarSubquery:
			// Subquery in SELECT list - will be evaluated per row for correlated support
			colIndices = append(colIndices, -1)
			funcExprs = append(funcExprs, expr)
			resultCols = append(resultCols, ColumnInfo{
				Name: "subquery",
				Type: "VARCHAR",
			})
		}
	}

	// If there are aggregate functions, compute aggregates
	if len(aggregateFuncs) > 0 || len(stmt.GroupBy) > 0 {
		// Handle GROUP BY with aggregates
		return e.executeGroupBy(stmt, rows, resultCols, colIndices, funcExprs, aggregateFuncs, tblInfo, columnMap, columnOrder)
	}

	// Build result rows
	result := &Result{
		Columns: resultCols,
		Rows:    make([][]interface{}, 0),
	}

	for _, r := range rows {
		if stmt.Where != nil {
			// Evaluate WHERE clause
			match, err := e.evaluateWhere(stmt.Where, r, columnMap, columnOrder)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}

		// Build result row
		resultRow := make([]interface{}, len(colIndices))
		for i, idx := range colIndices {
			if funcExprs[i] != nil {
				// Evaluate function expression
				val, err := e.evaluateExpression(funcExprs[i], r, columnMap, columnOrder)
				if err != nil {
					return nil, err
				}
				resultRow[i] = val
			} else if idx >= 0 && idx < len(r.Values) {
				resultRow[i] = e.valueToInterface(r.Values[idx])
			} else {
				resultRow[i] = nil
			}
		}
		result.Rows = append(result.Rows, resultRow)
	}

	// Apply ORDER BY if present
	if len(stmt.OrderBy) > 0 {
		result.Rows = e.sortRows(stmt.OrderBy, resultCols, result.Rows)
	}

	result.RowCount = len(result.Rows)
	return result, nil
}

// sortRows sorts the result rows according to ORDER BY clause.
func (e *Executor) sortRows(orderBy []*sql.OrderByItem, cols []ColumnInfo, rows [][]interface{}) [][]interface{} {
	// Build column index map (including aliases)
	colIndexMap := make(map[string]int)
	for i, col := range cols {
		colIndexMap[strings.ToLower(col.Name)] = i
		// Also add alias to the map if present
		if col.Alias != "" {
			colIndexMap[strings.ToLower(col.Alias)] = i
		}
	}

	// sortKeyType indicates whether this is a column index or expression
	type sortKeyType int
	const (
		sortKeyIndex sortKeyType = iota
		sortKeyExpr
	)

	// sortKeyData holds either an index or expression
	type sortKeyData struct {
		keyType   sortKeyType
		index     int
		expr      sql.Expression
		ascending bool
	}
	sortKeys := make([]sortKeyData, 0, len(orderBy))

	for _, item := range orderBy {
		switch expr := item.Expr.(type) {
		case *sql.ColumnRef:
			colName := strings.ToLower(expr.Name)
			idx, ok := colIndexMap[colName]
			if ok {
				sortKeys = append(sortKeys, sortKeyData{keyType: sortKeyIndex, index: idx, ascending: item.Ascending})
			}
		case *sql.Literal:
			// Handle numeric column reference (e.g., ORDER BY 1)
			if expr.Type == sql.LiteralNumber {
				var idx int
				switch v := expr.Value.(type) {
				case int:
					idx = v - 1
				case int64:
					idx = int(v) - 1
				case float64:
					idx = int(v) - 1
				default:
					continue
				}
				if idx >= 0 && idx < len(cols) {
					sortKeys = append(sortKeys, sortKeyData{keyType: sortKeyIndex, index: idx, ascending: item.Ascending})
				}
			}
		case *sql.BinaryExpr, *sql.UnaryExpr:
			// Handle expression-based ORDER BY (e.g., ORDER BY amount*2)
			sortKeys = append(sortKeys, sortKeyData{keyType: sortKeyExpr, expr: expr, ascending: item.Ascending})
		}
	}

	if len(sortKeys) == 0 {
		return rows
	}

	// Sort rows
	sort.Slice(rows, func(i, j int) bool {
		for _, key := range sortKeys {
			var vi, vj interface{}

			if key.keyType == sortKeyIndex {
				if key.index >= len(rows[i]) || key.index >= len(rows[j]) {
					continue
				}
				vi = rows[i][key.index]
				vj = rows[j][key.index]
			} else {
				// Evaluate expression for each row
				var err error
				vi, err = e.evaluateSortExpression(key.expr, rows[i], colIndexMap)
				if err != nil {
					continue
				}
				vj, err = e.evaluateSortExpression(key.expr, rows[j], colIndexMap)
				if err != nil {
					continue
				}
			}

			cmp := compareValues(vi, vj)
			if cmp == 0 {
				continue
			}

			if key.ascending {
				return cmp < 0
			}
			return cmp > 0
		}
		return false
	})

	return rows
}

// evaluateSortExpression evaluates an expression for sorting purposes.
func (e *Executor) evaluateSortExpression(expr sql.Expression, row []interface{}, colIndexMap map[string]int) (interface{}, error) {
	switch ex := expr.(type) {
	case *sql.Literal:
		return ex.Value, nil

	case *sql.ColumnRef:
		colName := strings.ToLower(ex.Name)
		idx, ok := colIndexMap[colName]
		if !ok {
			return nil, fmt.Errorf("unknown column: %s", ex.Name)
		}
		if idx < len(row) {
			return row[idx], nil
		}
		return nil, nil

	case *sql.BinaryExpr:
		left, err := e.evaluateSortExpression(ex.Left, row, colIndexMap)
		if err != nil {
			return nil, err
		}
		right, err := e.evaluateSortExpression(ex.Right, row, colIndexMap)
		if err != nil {
			return nil, err
		}
		return e.evaluateBinaryOp(left, ex.Op, right)

	case *sql.UnaryExpr:
		val, err := e.evaluateSortExpression(ex.Right, row, colIndexMap)
		if err != nil {
			return nil, err
		}
		if ex.Op == sql.OpNeg {
			switch v := val.(type) {
			case int:
				return -v, nil
			case int64:
				return -v, nil
			case float64:
				return -v, nil
			}
		}
		return val, nil
	}
	return nil, nil
}

// evaluateBinaryOp evaluates a binary operation for sorting.
func (e *Executor) evaluateBinaryOp(left interface{}, op sql.BinaryOp, right interface{}) (interface{}, error) {
	// Convert to float for arithmetic operations
	leftFloat, leftOk := toFloat(left)
	rightFloat, rightOk := toFloat(right)

	switch op {
	case sql.OpAdd:
		if leftOk && rightOk {
			return leftFloat + rightFloat, nil
		}
	case sql.OpSub:
		if leftOk && rightOk {
			return leftFloat - rightFloat, nil
		}
	case sql.OpMul:
		if leftOk && rightOk {
			return leftFloat * rightFloat, nil
		}
	case sql.OpDiv:
		if leftOk && rightOk && rightFloat != 0 {
			return leftFloat / rightFloat, nil
		}
	case sql.OpMod:
		if leftOk && rightOk && rightFloat != 0 {
			return float64(int(leftFloat) % int(rightFloat)), nil
		}
	// Comparison operators
	case sql.OpLt:
		if leftOk && rightOk {
			return leftFloat < rightFloat, nil
		}
		return fmt.Sprintf("%v", left) < fmt.Sprintf("%v", right), nil
	case sql.OpLe:
		if leftOk && rightOk {
			return leftFloat <= rightFloat, nil
		}
		return fmt.Sprintf("%v", left) <= fmt.Sprintf("%v", right), nil
	case sql.OpGt:
		if leftOk && rightOk {
			return leftFloat > rightFloat, nil
		}
		return fmt.Sprintf("%v", left) > fmt.Sprintf("%v", right), nil
	case sql.OpGe:
		if leftOk && rightOk {
			return leftFloat >= rightFloat, nil
		}
		return fmt.Sprintf("%v", left) >= fmt.Sprintf("%v", right), nil
	case sql.OpEq:
		if leftOk && rightOk {
			return leftFloat == rightFloat, nil
		}
		return fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right), nil
	case sql.OpNe:
		if leftOk && rightOk {
			return leftFloat != rightFloat, nil
		}
		return fmt.Sprintf("%v", left) != fmt.Sprintf("%v", right), nil
	}
	return nil, fmt.Errorf("cannot evaluate binary operation")
}

// toFloat converts a value to float64 for arithmetic.
func toFloat(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case float64:
		return val, true
	case float32:
		return float64(val), true
	default:
		str := fmt.Sprintf("%v", v)
		if f, err := strconv.ParseFloat(str, 64); err == nil {
			return f, true
		}
		return 0, false
	}
}

// evaluateUnaryExpr evaluates a unary expression.
func (e *Executor) evaluateUnaryExpr(op sql.UnaryOp, val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}

	switch op {
	case sql.OpNeg:
		switch v := val.(type) {
		case int:
			return -v, nil
		case int64:
			return -v, nil
		case float64:
			return -v, nil
		case float32:
			return -float64(v), nil
		default:
			if f, ok := toFloat(v); ok {
				return -f, nil
			}
			return nil, fmt.Errorf("cannot negate value of type %T", val)
		}
	case sql.OpNot:
		switch v := val.(type) {
		case bool:
			return !v, nil
		case int, int64, float64:
			return v == 0, nil
		default:
			return nil, fmt.Errorf("cannot apply NOT to type %T", val)
		}
	default:
		return val, nil
	}
}

// executeGroupBy handles GROUP BY and aggregate functions.
func (e *Executor) executeGroupBy(stmt *sql.SelectStmt, rows []*row.Row, resultCols []ColumnInfo, colIndices []int, funcExprs []sql.Expression, aggregateFuncs []struct {
	name  string
	arg   string
	index int
}, tblInfo *table.TableInfo, columnMap map[string]*types.ColumnInfo, columnOrder []*types.ColumnInfo) (*Result, error) {
	// Build GROUP BY column indices
	groupByIndices := make([]int, 0)
	for _, gbExpr := range stmt.GroupBy {
		if colRef, ok := gbExpr.(*sql.ColumnRef); ok {
			colName := strings.ToLower(colRef.Name)
			for i, col := range tblInfo.Columns {
				if strings.ToLower(col.Name) == colName {
					groupByIndices = append(groupByIndices, i)
					break
				}
			}
		}
	}

	// Group rows by GROUP BY key
	groups := make(map[string][]*row.Row)
	var groupOrder []string // maintain group order

	for _, r := range rows {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, err := e.evaluateWhere(stmt.Where, r, columnMap, columnOrder)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}

		// Build group key
		var key strings.Builder
		if len(groupByIndices) > 0 {
			for _, idx := range groupByIndices {
				if idx < len(r.Values) {
					key.WriteString(fmt.Sprintf("%v|", e.valueToInterface(r.Values[idx])))
				} else {
					key.WriteString("nil|")
				}
			}
		} else {
			// No GROUP BY - single group
			key.WriteString("all")
		}

		keyStr := key.String()
		if _, exists := groups[keyStr]; !exists {
			groupOrder = append(groupOrder, keyStr)
		}
		groups[keyStr] = append(groups[keyStr], r)
	}

	// Build result
	result := &Result{
		Columns: resultCols,
		Rows:    make([][]interface{}, 0),
	}

	// Process each group
	for _, groupKey := range groupOrder {
		groupRows := groups[groupKey]
		resultRow := make([]interface{}, len(resultCols))

		// Fill in non-aggregate columns (GROUP BY columns)
		if len(groupRows) > 0 {
			firstRow := groupRows[0]
			for i, idx := range colIndices {
				if idx >= 0 && idx < len(firstRow.Values) {
					resultRow[i] = e.valueToInterface(firstRow.Values[idx])
				} else if funcExprs[i] != nil {
					// Evaluate function expression for the first row in group
					val, err := e.evaluateExpression(funcExprs[i], firstRow, columnMap, columnOrder)
					if err != nil {
						return nil, err
					}
					resultRow[i] = val
				}
			}
		}

		// Compute aggregates for this group
		for _, agg := range aggregateFuncs {
			switch agg.name {
			case "COUNT":
				resultRow[agg.index] = len(groupRows)
			case "SUM":
				var sum int64
				for _, r := range groupRows {
					if agg.arg != "" && agg.arg != "*" {
						for j, col := range tblInfo.Columns {
							if strings.ToLower(col.Name) == agg.arg {
								if j < len(r.Values) && !r.Values[j].Null {
									sum += r.Values[j].AsInt()
								}
							}
						}
					}
				}
				resultRow[agg.index] = sum
			case "AVG":
				var sum int64
				count := 0
				for _, r := range groupRows {
					if agg.arg != "" {
						for j, col := range tblInfo.Columns {
							if strings.ToLower(col.Name) == agg.arg {
								if j < len(r.Values) && !r.Values[j].Null {
									sum += r.Values[j].AsInt()
									count++
								}
							}
						}
					}
				}
				if count > 0 {
					resultRow[agg.index] = float64(sum) / float64(count)
				} else {
					resultRow[agg.index] = nil
				}
			case "MIN":
				var minVal int64
				hasMin := false
				for _, r := range groupRows {
					if agg.arg != "" {
						for j, col := range tblInfo.Columns {
							if strings.ToLower(col.Name) == agg.arg {
								if j < len(r.Values) && !r.Values[j].Null {
									v := r.Values[j].AsInt()
									if !hasMin || v < minVal {
										minVal = v
										hasMin = true
									}
								}
							}
						}
					}
				}
				if hasMin {
					resultRow[agg.index] = minVal
				} else {
					resultRow[agg.index] = nil
				}
			case "MAX":
				var maxVal int64
				hasMax := false
				for _, r := range groupRows {
					if agg.arg != "" {
						for j, col := range tblInfo.Columns {
							if strings.ToLower(col.Name) == agg.arg {
								if j < len(r.Values) && !r.Values[j].Null {
									v := r.Values[j].AsInt()
									if !hasMax || v > maxVal {
										maxVal = v
										hasMax = true
									}
								}
							}
						}
					}
				}
				if hasMax {
					resultRow[agg.index] = maxVal
				} else {
					resultRow[agg.index] = nil
				}
			default:
				resultRow[agg.index] = nil
			}
		}

		// Apply HAVING clause if present
		if stmt.Having != nil {
			match, err := e.evaluateHaving(stmt.Having, resultRow, resultCols, aggregateFuncs, groupRows, tblInfo)
			if err != nil {
				return nil, fmt.Errorf("HAVING clause error: %w", err)
			}
			if !match {
				continue // Skip this group
			}
		}

		result.Rows = append(result.Rows, resultRow)
	}

	result.RowCount = len(result.Rows)
	return result, nil
}

// evaluateHaving evaluates a HAVING clause expression.
// It handles aggregate functions and subqueries within the HAVING clause.
func (e *Executor) evaluateHaving(expr sql.Expression, resultRow []interface{}, resultCols []ColumnInfo, aggregateFuncs []struct {
	name  string
	arg   string
	index int
}, groupRows []*row.Row, tblInfo *table.TableInfo) (bool, error) {
	switch ex := expr.(type) {
	case *sql.BinaryExpr:
		// Special handling for IN operator
		if ex.Op == sql.OpIn {
			left, err := e.evaluateHavingExpr(ex.Left, resultRow, resultCols, aggregateFuncs, groupRows, tblInfo)
			if err != nil {
				return false, err
			}

			// Build outer context for correlated subquery
			outerCtx := make(map[string]interface{})
			for i, col := range resultCols {
				colName := strings.ToLower(col.Name)
				if i < len(resultRow) {
					outerCtx[colName] = resultRow[i]
					if col.Alias != "" {
						outerCtx[strings.ToLower(col.Alias)] = resultRow[i]
					}
				}
			}
			oldOuterCtx := e.outerContext
			e.outerContext = outerCtx

			// Handle subquery on the right side
			var subResult *Result
			var execErr error
			if subq, ok := ex.Right.(*sql.SubqueryExpr); ok {
				subResult, execErr = e.executeStatement(subq.Select)
			} else if paren, ok := ex.Right.(*sql.ParenExpr); ok {
				if subq, ok := paren.Expr.(*sql.SubqueryExpr); ok {
					subResult, execErr = e.executeStatement(subq.Select)
				}
			}

			e.outerContext = oldOuterCtx

			if execErr != nil {
				return false, execErr
			}
			if subResult != nil {
				for _, row := range subResult.Rows {
					if len(row) > 0 && compareEqual(left, row[0]) {
						return true, nil
					}
				}
				return false, nil
			}
			return false, nil
		}

		left, err := e.evaluateHavingExpr(ex.Left, resultRow, resultCols, aggregateFuncs, groupRows, tblInfo)
		if err != nil {
			return false, err
		}
		right, err := e.evaluateHavingExpr(ex.Right, resultRow, resultCols, aggregateFuncs, groupRows, tblInfo)
		if err != nil {
			return false, err
		}
		if left == nil || right == nil {
			return false, nil
		}
		return e.compareValues(left, ex.Op, right)

	case *sql.UnaryExpr:
		if ex.Op == sql.OpNot {
			val, err := e.evaluateHaving(ex.Right, resultRow, resultCols, aggregateFuncs, groupRows, tblInfo)
			if err != nil {
				return false, err
			}
			return !val, nil
		}

	case *sql.InExpr:
		val, err := e.evaluateHavingExpr(ex.Expr, resultRow, resultCols, aggregateFuncs, groupRows, tblInfo)
		if err != nil {
			return false, err
		}
		// Handle subquery
		if ex.Select != nil {
			// Build outer context for correlated subquery
			outerCtx := make(map[string]interface{})
			for i, col := range resultCols {
				colName := strings.ToLower(col.Name)
				if i < len(resultRow) {
					outerCtx[colName] = resultRow[i]
					if col.Alias != "" {
						outerCtx[strings.ToLower(col.Alias)] = resultRow[i]
					}
				}
			}
			oldOuterCtx := e.outerContext
			e.outerContext = outerCtx

			subResult, err := e.executeStatement(ex.Select)

			e.outerContext = oldOuterCtx

			if err != nil {
				return false, err
			}
			for _, row := range subResult.Rows {
				if len(row) > 0 && compareEqual(val, row[0]) {
					return !ex.Not, nil
				}
			}
			return ex.Not, nil
		}
		// Handle value list
		for _, listExpr := range ex.List {
			listVal, err := e.evaluateHavingExpr(listExpr, resultRow, resultCols, aggregateFuncs, groupRows, tblInfo)
			if err != nil {
				continue
			}
			if compareEqual(val, listVal) {
				return !ex.Not, nil
			}
		}
		return ex.Not, nil

	case *sql.ExistsExpr:
		// Build outer context from the current group's result row
		outerCtx := make(map[string]interface{})
		for i, col := range resultCols {
			colName := strings.ToLower(col.Name)
			if i < len(resultRow) {
				outerCtx[colName] = resultRow[i]
				// Also add with alias if present
				if col.Alias != "" {
					outerCtx[strings.ToLower(col.Alias)] = resultRow[i]
				}
			}
		}

		// Save and set outer context
		oldOuterCtx := e.outerContext
		e.outerContext = outerCtx

		// Execute the subquery
		result, err := e.executeStatement(ex.Subquery.Select)

		// Restore outer context
		e.outerContext = oldOuterCtx

		if err != nil {
			return false, err
		}
		return len(result.Rows) > 0, nil

	case *sql.ScalarSubquery:
		// Build outer context from the current group's result row
		outerCtx := make(map[string]interface{})
		for i, col := range resultCols {
			colName := strings.ToLower(col.Name)
			if i < len(resultRow) {
				outerCtx[colName] = resultRow[i]
				if col.Alias != "" {
					outerCtx[strings.ToLower(col.Alias)] = resultRow[i]
				}
			}
		}

		// Save and set outer context
		oldOuterCtx := e.outerContext
		e.outerContext = outerCtx

		// Execute the subquery
		result, err := e.executeStatement(ex.Subquery.Select)

		// Restore outer context
		e.outerContext = oldOuterCtx

		if err != nil {
			return false, err
		}
		if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
			return false, nil
		}
		if len(result.Rows) > 1 {
			return false, fmt.Errorf("scalar subquery in HAVING returned more than one row")
		}
		val := result.Rows[0][0]
		if val == nil {
			return false, nil
		}
		switch v := val.(type) {
		case bool:
			return v, nil
		case int, int64, float64:
			return v != 0, nil
		}
		return val != nil, nil
	}

	return false, nil
}

// evaluateHavingExpr evaluates an expression within a HAVING clause.
func (e *Executor) evaluateHavingExpr(expr sql.Expression, resultRow []interface{}, resultCols []ColumnInfo, aggregateFuncs []struct {
	name  string
	arg   string
	index int
}, groupRows []*row.Row, tblInfo *table.TableInfo) (interface{}, error) {
	switch ex := expr.(type) {
	case *sql.Literal:
		return ex.Value, nil

	case *sql.ColumnRef:
		// Look for the column in result columns
		colName := strings.ToLower(ex.Name)
		for i, col := range resultCols {
			if strings.ToLower(col.Name) == colName || (col.Alias != "" && strings.ToLower(col.Alias) == colName) {
				if i < len(resultRow) {
					return resultRow[i], nil
				}
			}
		}
		return nil, fmt.Errorf("unknown column in HAVING: %s", ex.Name)

	case *sql.FunctionCall:
		funcName := strings.ToUpper(ex.Name)
		// Check if it's an aggregate function that we've already computed
		for _, agg := range aggregateFuncs {
			if agg.name == funcName {
				// Check if the argument matches
				argMatch := false
				if len(ex.Args) == 0 && (agg.arg == "" || agg.arg == "*") {
					argMatch = true
				} else if len(ex.Args) > 0 {
					if _, ok := ex.Args[0].(*sql.StarExpr); ok && agg.arg == "*" {
						argMatch = true
					} else if colRef, ok := ex.Args[0].(*sql.ColumnRef); ok && strings.ToLower(colRef.Name) == agg.arg {
						argMatch = true
					}
				}
				if argMatch && agg.index < len(resultRow) {
					return resultRow[agg.index], nil
				}
			}
		}
		// If not found in pre-computed aggregates, compute it now
		return e.computeAggregateForHaving(ex, groupRows, tblInfo)

	case *sql.BinaryExpr:
		left, err := e.evaluateHavingExpr(ex.Left, resultRow, resultCols, aggregateFuncs, groupRows, tblInfo)
		if err != nil {
			return nil, err
		}
		right, err := e.evaluateHavingExpr(ex.Right, resultRow, resultCols, aggregateFuncs, groupRows, tblInfo)
		if err != nil {
			return nil, err
		}
		return e.evaluateBinaryOp(left, ex.Op, right)

	case *sql.ScalarSubquery:
		// Build outer context for correlated subquery
		outerCtx := make(map[string]interface{})
		for i, col := range resultCols {
			colName := strings.ToLower(col.Name)
			if i < len(resultRow) {
				outerCtx[colName] = resultRow[i]
				if col.Alias != "" {
					outerCtx[strings.ToLower(col.Alias)] = resultRow[i]
				}
			}
		}
		oldOuterCtx := e.outerContext
		e.outerContext = outerCtx

		result, err := e.executeStatement(ex.Subquery.Select)

		e.outerContext = oldOuterCtx

		if err != nil {
			return nil, err
		}
		if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
			return nil, nil
		}
		if len(result.Rows) > 1 {
			return nil, fmt.Errorf("scalar subquery returned more than one row")
		}
		return result.Rows[0][0], nil

	case *sql.SubqueryExpr:
		// Build outer context for correlated subquery
		outerCtx := make(map[string]interface{})
		for i, col := range resultCols {
			colName := strings.ToLower(col.Name)
			if i < len(resultRow) {
				outerCtx[colName] = resultRow[i]
				if col.Alias != "" {
					outerCtx[strings.ToLower(col.Alias)] = resultRow[i]
				}
			}
		}
		oldOuterCtx := e.outerContext
		e.outerContext = outerCtx

		result, err := e.executeStatement(ex.Select)

		e.outerContext = oldOuterCtx

		if err != nil {
			return nil, err
		}
		if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
			return nil, nil
		}
		if len(result.Rows) > 1 {
			return nil, fmt.Errorf("scalar subquery returned more than one row")
		}
		return result.Rows[0][0], nil
	}

	return nil, nil
}

// computeAggregateForHaving computes an aggregate function for the HAVING clause.
func (e *Executor) computeAggregateForHaving(fc *sql.FunctionCall, groupRows []*row.Row, tblInfo *table.TableInfo) (interface{}, error) {
	funcName := strings.ToUpper(fc.Name)
	argName := ""
	if len(fc.Args) > 0 {
		if colRef, ok := fc.Args[0].(*sql.ColumnRef); ok {
			argName = strings.ToLower(colRef.Name)
		}
	}

	switch funcName {
	case "COUNT":
		return len(groupRows), nil

	case "SUM":
		var sum int64
		for _, r := range groupRows {
			if argName != "" {
				for j, col := range tblInfo.Columns {
					if strings.ToLower(col.Name) == argName {
						if j < len(r.Values) && !r.Values[j].Null {
							sum += r.Values[j].AsInt()
						}
					}
				}
			}
		}
		return sum, nil

	case "AVG":
		var sum int64
		count := 0
		for _, r := range groupRows {
			if argName != "" {
				for j, col := range tblInfo.Columns {
					if strings.ToLower(col.Name) == argName {
						if j < len(r.Values) && !r.Values[j].Null {
							sum += r.Values[j].AsInt()
							count++
						}
					}
				}
			}
		}
		if count > 0 {
			return float64(sum) / float64(count), nil
		}
		return nil, nil

	case "MIN":
		var minVal int64
		hasMin := false
		for _, r := range groupRows {
			if argName != "" {
				for j, col := range tblInfo.Columns {
					if strings.ToLower(col.Name) == argName {
						if j < len(r.Values) && !r.Values[j].Null {
							v := r.Values[j].AsInt()
							if !hasMin || v < minVal {
								minVal = v
								hasMin = true
							}
						}
					}
				}
			}
		}
		if hasMin {
			return minVal, nil
		}
		return nil, nil

	case "MAX":
		var maxVal int64
		hasMax := false
		for _, r := range groupRows {
			if argName != "" {
				for j, col := range tblInfo.Columns {
					if strings.ToLower(col.Name) == argName {
						if j < len(r.Values) && !r.Values[j].Null {
							v := r.Values[j].AsInt()
							if !hasMax || v > maxVal {
								maxVal = v
								hasMax = true
							}
						}
					}
				}
			}
		}
		if hasMax {
			return maxVal, nil
		}
		return nil, nil
	}

	return nil, fmt.Errorf("unknown aggregate function in HAVING: %s", funcName)
}

// executeUnion executes a UNION statement.
func (e *Executor) executeUnion(stmt *sql.UnionStmt) (*Result, error) {
	// Execute left side
	leftResult, err := e.executeStatement(stmt.Left)
	if err != nil {
		opName := stmt.Op.String()
		return nil, fmt.Errorf("left side of %s error: %w", opName, err)
	}

	// Execute right side
	rightResult, err := e.executeStatement(stmt.Right)
	if err != nil {
		opName := stmt.Op.String()
		return nil, fmt.Errorf("right side of %s error: %w", opName, err)
	}

	// Validate column count match
	opName := stmt.Op.String()
	if len(leftResult.Columns) != len(rightResult.Columns) {
		return nil, fmt.Errorf("%s: column count mismatch (%d vs %d)",
			opName, len(leftResult.Columns), len(rightResult.Columns))
	}

	// Build combined result
	result := &Result{
		Columns: leftResult.Columns,
		Rows:    make([][]interface{}, 0),
	}

	switch stmt.Op {
	case sql.SetUnion:
		// Add left rows
		result.Rows = append(result.Rows, leftResult.Rows...)
		// Add right rows
		result.Rows = append(result.Rows, rightResult.Rows...)
		// For UNION (not UNION ALL), remove duplicates
		if !stmt.All {
			result.Rows = e.removeDuplicateRows(result.Rows)
		}

	case sql.SetIntersect:
		// INTERSECT: rows that exist in both results
		result.Rows = e.intersectRows(leftResult.Rows, rightResult.Rows)

	case sql.SetExcept:
		// EXCEPT: rows in left but not in right
		result.Rows = e.exceptRows(leftResult.Rows, rightResult.Rows)
	}

	result.RowCount = len(result.Rows)
	return result, nil
}

// intersectRows returns rows that exist in both left and right.
func (e *Executor) intersectRows(left, right [][]interface{}) [][]interface{} {
	rightSet := make(map[string]bool)
	for _, row := range right {
		key := e.rowKey(row)
		rightSet[key] = true
	}

	var result [][]interface{}
	seen := make(map[string]bool)
	for _, row := range left {
		key := e.rowKey(row)
		if rightSet[key] && !seen[key] {
			result = append(result, row)
			seen[key] = true
		}
	}
	return result
}

// exceptRows returns rows in left but not in right.
func (e *Executor) exceptRows(left, right [][]interface{}) [][]interface{} {
	rightSet := make(map[string]bool)
	for _, row := range right {
		key := e.rowKey(row)
		rightSet[key] = true
	}

	var result [][]interface{}
	seen := make(map[string]bool)
	for _, row := range left {
		key := e.rowKey(row)
		if !rightSet[key] && !seen[key] {
			result = append(result, row)
			seen[key] = true
		}
	}
	return result
}

// rowKey creates a string key for a row for comparison.
func (e *Executor) rowKey(row []interface{}) string {
	var parts []string
	for _, val := range row {
		if val == nil {
			parts = append(parts, "NULL")
		} else {
			parts = append(parts, fmt.Sprintf("%v", val))
		}
	}
	return strings.Join(parts, "|")
}

// executeStatement executes a statement and returns a result.
// Used for UNION components which can be SELECT or another UNION.
func (e *Executor) executeStatement(stmt sql.Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *sql.SelectStmt:
		return e.executeSelect(s)
	case *sql.UnionStmt:
		return e.executeUnion(s)
	default:
		return nil, fmt.Errorf("unsupported statement in UNION: %T", stmt)
	}
}

// removeDuplicateRows removes duplicate rows from a slice.
func (e *Executor) removeDuplicateRows(rows [][]interface{}) [][]interface{} {
	seen := make(map[string]bool)
	result := make([][]interface{}, 0, len(rows))

	for _, row := range rows {
		key := e.rowKey(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}

	return result
}

// rowKey creates a string key from a row for deduplication.
// executeSelectFromDerivedTable handles SELECT from a derived table (subquery in FROM clause).
func (e *Executor) executeSelectFromDerivedTable(stmt *sql.SelectStmt) (*Result, error) {
	// Execute the subquery first
	subquery := stmt.From.Table.Subquery
	derivedResult, err := e.executeStatement(subquery.Select)
	if err != nil {
		return nil, fmt.Errorf("derived table error: %w", err)
	}

	// Use the alias if provided
	tableAlias := stmt.From.Table.Alias
	if tableAlias == "" {
		tableAlias = "derived_table"
	}

	// Set current table for correlated subqueries
	oldTable := e.currentTable
	e.currentTable = strings.ToLower(tableAlias)
	defer func() { e.currentTable = oldTable }()

	// Build column index map for the derived table
	colIdxMap := make(map[string]int)
	for i, col := range derivedResult.Columns {
		colIdxMap[strings.ToLower(col.Name)] = i
	}

	// Build column info for the derived table (simplified)
	columnOrder := make([]*types.ColumnInfo, len(derivedResult.Columns))
	for i, col := range derivedResult.Columns {
		columnOrder[i] = &types.ColumnInfo{
			Name: col.Name,
			Type: types.TypeVarchar,
		}
	}

	result := &Result{
		Columns: make([]ColumnInfo, 0),
		Rows:    make([][]interface{}, 0),
	}

	// Determine result columns
	for _, colExpr := range stmt.Columns {
		switch expr := colExpr.(type) {
		case *sql.StarExpr:
			for _, col := range derivedResult.Columns {
				result.Columns = append(result.Columns, col)
			}
		case *sql.ColumnRef:
			colName := strings.ToLower(expr.Name)
			if _, ok := colIdxMap[colName]; !ok {
				return nil, fmt.Errorf("unknown column: %s", expr.Name)
			}
			ci := ColumnInfo{
				Name: expr.Name,
				Type: "VARCHAR",
			}
			if expr.Alias != "" {
				ci.Alias = expr.Alias
			}
			result.Columns = append(result.Columns, ci)
		default:
			// Handle other expressions (literals, functions, etc.)
			ci := ColumnInfo{
				Name: fmt.Sprintf("expr_%d", len(result.Columns)+1),
				Type: "VARCHAR",
			}
			result.Columns = append(result.Columns, ci)
		}
	}

	// Process each row from the derived table
	for _, srcRow := range derivedResult.Rows {
		// Apply WHERE filter if present
		if stmt.Where != nil {
			// Build a pseudo row.Row for evaluation
			pseudoRow := &row.Row{}
			values := make([]types.Value, len(srcRow))
			for i, v := range srcRow {
				switch val := v.(type) {
				case int:
					values[i] = types.NewIntValue(int64(val))
				case int64:
					values[i] = types.NewIntValue(val)
				case float64:
					values[i] = types.NewFloatValue(val)
				case string:
					values[i] = types.NewStringValue(val, types.TypeVarchar)
				case []byte:
					values[i] = types.NewBlobValue(val)
				default:
					values[i] = types.NewStringValue(fmt.Sprintf("%v", val), types.TypeVarchar)
				}
			}
			pseudoRow.Values = values

			match, err := e.evaluateWhereForRow(stmt.Where, pseudoRow, columnOrder, colIdxMap)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}

		// Build result row
		resultRow := make([]interface{}, len(result.Columns))
		colIdx := 0
		for _, colExpr := range stmt.Columns {
			switch expr := colExpr.(type) {
			case *sql.StarExpr:
				for _, srcVal := range srcRow {
					resultRow[colIdx] = srcVal
					colIdx++
				}
			case *sql.ColumnRef:
				colName := strings.ToLower(expr.Name)
				if idx, ok := colIdxMap[colName]; ok && idx < len(srcRow) {
					resultRow[colIdx] = srcRow[idx]
				}
				colIdx++
			default:
				// For other expressions, just use nil for now
				colIdx++
			}
		}
		result.Rows = append(result.Rows, resultRow)
	}

	// Apply ORDER BY (simplified - just pass through for now)
	// Apply LIMIT/OFFSET
	if stmt.Limit != nil {
		offset := 0
		if stmt.Offset != nil {
			offset = int(*stmt.Offset)
		}
		limit := int(*stmt.Limit)
		if offset < len(result.Rows) {
			end := offset + limit
			if end > len(result.Rows) {
				end = len(result.Rows)
			}
			result.Rows = result.Rows[offset:end]
		} else {
			result.Rows = nil
		}
	}

	result.RowCount = len(result.Rows)
	return result, nil
}

// executeSelectWithoutFrom handles SELECT without FROM (e.g., SELECT 1, SELECT NOW()).
func (e *Executor) executeSelectWithoutFrom(stmt *sql.SelectStmt) (*Result, error) {
	result := &Result{
		Columns: make([]ColumnInfo, 0),
		Rows:    make([][]interface{}, 0),
	}

	row := make([]interface{}, 0)
	for _, colExpr := range stmt.Columns {
		switch expr := colExpr.(type) {
		case *sql.Literal:
			result.Columns = append(result.Columns, ColumnInfo{
				Name: fmt.Sprintf("%d", len(result.Columns)+1),
				Type: "INT",
			})
			row = append(row, expr.Value)
		case *sql.ColumnRef:
			result.Columns = append(result.Columns, ColumnInfo{
				Name: expr.Name,
				Type: "INT",
			})
			row = append(row, 1)
		case *sql.FunctionCall:
			colName := expr.Name
			if strings.ToUpper(colName) == "NOW" || strings.ToUpper(colName) == "CURRENT_TIMESTAMP" {
				result.Columns = append(result.Columns, ColumnInfo{
					Name: colName + "()",
					Type: "DATETIME",
				})
				row = append(row, time.Now().Format("2006-01-02 15:04:05"))
			} else if strings.ToUpper(colName) == "DATABASE" {
				result.Columns = append(result.Columns, ColumnInfo{
					Name: "DATABASE()",
					Type: "VARCHAR",
				})
				row = append(row, e.database)
			} else if strings.ToUpper(colName) == "VERSION" {
				result.Columns = append(result.Columns, ColumnInfo{
					Name: "VERSION()",
					Type: "VARCHAR",
				})
				row = append(row, "5.7.0-XxSql")
			} else {
				// Evaluate function using evaluateFunction
				val, err := e.evaluateFunction(expr, nil, nil, nil)
				if err != nil {
					return nil, err
				}
				result.Columns = append(result.Columns, ColumnInfo{
					Name: colName + "()",
					Type: "VARCHAR",
				})
				row = append(row, val)
			}
		case *sql.CastExpr:
			// Evaluate the cast expression
			val, err := e.castValueFromExpr(expr)
			if err != nil {
				return nil, err
			}
			result.Columns = append(result.Columns, ColumnInfo{
				Name: "CAST",
				Type: strings.ToUpper(expr.Type.Name),
			})
			row = append(row, val)
		case *sql.BinaryExpr:
			// Evaluate binary expression
			val, err := e.evaluateBinaryExprWithoutRow(expr)
			if err != nil {
				return nil, err
			}
			result.Columns = append(result.Columns, ColumnInfo{
				Name: "expr",
				Type: "VARCHAR",
			})
			row = append(row, val)
		case *sql.StarExpr:
			// SELECT * without FROM is invalid, return 1
			result.Columns = append(result.Columns, ColumnInfo{
				Name: "1",
				Type: "INT",
			})
			row = append(row, 1)
		case *sql.SubqueryExpr:
			// Scalar subquery in SELECT list
			subResult, err := e.executeStatement(expr.Select)
			if err != nil {
				return nil, err
			}
			colName := "subquery"
			result.Columns = append(result.Columns, ColumnInfo{
				Name: colName,
				Type: "VARCHAR",
			})
			if len(subResult.Rows) == 0 || len(subResult.Rows[0]) == 0 {
				row = append(row, nil)
			} else if len(subResult.Rows) > 1 {
				return nil, fmt.Errorf("scalar subquery returned more than one row")
			} else {
				row = append(row, subResult.Rows[0][0])
			}
		case *sql.ScalarSubquery:
			// Scalar subquery in SELECT list
			subResult, err := e.executeStatement(expr.Subquery.Select)
			if err != nil {
				return nil, err
			}
			colName := "subquery"
			result.Columns = append(result.Columns, ColumnInfo{
				Name: colName,
				Type: "VARCHAR",
			})
			if len(subResult.Rows) == 0 || len(subResult.Rows[0]) == 0 {
				row = append(row, nil)
			} else if len(subResult.Rows) > 1 {
				return nil, fmt.Errorf("scalar subquery returned more than one row")
			} else {
				row = append(row, subResult.Rows[0][0])
			}
		default:
			// Try to handle as a general expression
			result.Columns = append(result.Columns, ColumnInfo{
				Name: fmt.Sprintf("%d", len(result.Columns)+1),
				Type: "VARCHAR",
			})
			row = append(row, nil)
		}
	}

	result.Rows = append(result.Rows, row)
	result.RowCount = 1
	return result, nil
}

// castValueFromExpr evaluates a cast expression without a row context.
func (e *Executor) castValueFromExpr(expr *sql.CastExpr) (interface{}, error) {
	var val interface{}
	switch inner := expr.Expr.(type) {
	case *sql.Literal:
		val = inner.Value
	case *sql.ColumnRef:
		val = inner.Name
	default:
		return nil, fmt.Errorf("unsupported expression in CAST")
	}
	return e.castValue(val, expr.Type.Name)
}

// evaluateBinaryExprWithoutRow evaluates a binary expression without a row context.
func (e *Executor) evaluateBinaryExprWithoutRow(expr *sql.BinaryExpr) (interface{}, error) {
	var left, right interface{}
	var err error

	switch l := expr.Left.(type) {
	case *sql.Literal:
		left = l.Value
	case *sql.CastExpr:
		left, err = e.castValueFromExpr(l)
		if err != nil {
			return nil, err
		}
	default:
		left = nil
	}

	switch r := expr.Right.(type) {
	case *sql.Literal:
		right = r.Value
	case *sql.CastExpr:
		right, err = e.castValueFromExpr(r)
		if err != nil {
			return nil, err
		}
	default:
		right = nil
	}

	return e.evaluateBinaryOp(left, expr.Op, right)
}

// executeInsert executes an INSERT statement.
func (e *Executor) executeInsert(stmt *sql.InsertStmt) (*Result, error) {
	tableName := stmt.Table
	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	// Check if table exists
	if !e.engine.TableExists(tableName) {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Get table info for column mapping
	tbl, err := e.engine.GetTable(tableName)
	if err != nil {
		return nil, err
	}
	tblInfo := tbl.GetInfo()

	// Determine column order
	var colOrder []string
	if len(stmt.Columns) > 0 {
		colOrder = stmt.Columns
	} else {
		for _, col := range tblInfo.Columns {
			colOrder = append(colOrder, col.Name)
		}
	}

	// Build column index map
	colIdxMap := make(map[string]int)
	for i, col := range tblInfo.Columns {
		colIdxMap[strings.ToLower(col.Name)] = i
	}

	var totalAffected int
	var lastInsertID uint64

	// Insert each row
	for _, valueRow := range stmt.Values {
		if len(valueRow) != len(colOrder) {
			return nil, fmt.Errorf("column count mismatch: expected %d, got %d", len(colOrder), len(valueRow))
		}

		// Build values array in correct column order
		values := make([]types.Value, len(tblInfo.Columns))
		for i := range values {
			values[i] = types.Value{Null: true}
		}

		for i, colName := range colOrder {
			idx, ok := colIdxMap[strings.ToLower(colName)]
			if !ok {
				return nil, fmt.Errorf("unknown column: %s", colName)
			}

			val, err := e.expressionToValue(valueRow[i], tblInfo.Columns[idx])
			if err != nil {
				return nil, err
			}
			values[idx] = val
		}

		// Apply DEFAULT values for missing columns
		for i, col := range tblInfo.Columns {
			if values[i].Null && col.Default.Type != types.TypeNull {
				values[i] = col.Default
			}
		}

		// Handle auto-increment columns - mark as NULL so table layer can generate value
		for i, col := range tblInfo.Columns {
			if col.AutoIncr && values[i].Null {
				// Keep as NULL - table layer will generate the sequence value
				// Don't set a placeholder value here
			}
		}

		// Validate NOT NULL constraints
		for i, col := range tblInfo.Columns {
			if !col.Nullable && values[i].Null {
				return nil, fmt.Errorf("column '%s' cannot be null", col.Name)
			}
		}

		// Validate UNIQUE constraints (check existing data)
		for i, col := range tblInfo.Columns {
			if col.Unique && !values[i].Null {
				// Check if value already exists
				if e.valueExistsInColumn(tbl, i, values[i]) {
					return nil, fmt.Errorf("duplicate entry '%s' for key '%s'", values[i].String(), col.Name)
				}
			}
		}

		// Validate CHECK constraints
		if err := e.validateCheckConstraints(tbl, values); err != nil {
			return nil, err
		}

		// Validate FOREIGN KEY constraints
		if err := e.validateForeignKeys(tbl, values, false); err != nil {
			return nil, err
		}

		// Insert the row
		rowID, err := e.engine.Insert(tableName, values)
		if err != nil {
			return nil, fmt.Errorf("insert error: %w", err)
		}

		totalAffected++
		lastInsertID = uint64(rowID)
	}

	return &Result{
		Affected:   totalAffected,
		LastInsert: lastInsertID,
		Message:    "OK",
	}, nil
}

// executeUpdate executes an UPDATE statement.
func (e *Executor) executeUpdate(stmt *sql.UpdateStmt) (*Result, error) {
	tableName := stmt.Table
	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	// Check if table exists
	if !e.engine.TableExists(tableName) {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Get table info
	tbl, err := e.engine.GetTable(tableName)
	if err != nil {
		return nil, err
	}
	tblInfo := tbl.GetInfo()

	// Build column index map
	colIdxMap := make(map[string]int)
	for i, col := range tblInfo.Columns {
		colIdxMap[strings.ToLower(col.Name)] = i
	}

	// Build updates map (column index -> new value)
	updates := make(map[int]types.Value)
	for _, a := range stmt.Assignments {
		idx, ok := colIdxMap[strings.ToLower(a.Column)]
		if !ok {
			return nil, fmt.Errorf("unknown column: %s", a.Column)
		}

		// Convert expression to value
		val, err := e.expressionToValue(a.Value, tblInfo.Columns[idx])
		if err != nil {
			return nil, fmt.Errorf("invalid value for column %s: %w", a.Column, err)
		}

		// Validate NOT NULL constraint
		if !tblInfo.Columns[idx].Nullable && val.Null {
			return nil, fmt.Errorf("column '%s' cannot be null", tblInfo.Columns[idx].Name)
		}

		// Validate UNIQUE constraint (if not NULL)
		if tblInfo.Columns[idx].Unique && !val.Null {
			if e.valueExistsInColumn(tbl, idx, val) {
				return nil, fmt.Errorf("duplicate entry '%s' for key '%s'", val.String(), tblInfo.Columns[idx].Name)
			}
		}

		updates[idx] = val
	}

	// Build predicate function
	var predicate func(*row.Row) bool
	if stmt.Where != nil {
		predicate = func(r *row.Row) bool {
			match, _ := e.evaluateWhereForRow(stmt.Where, r, tblInfo.Columns, colIdxMap)
			return match
		}
	} else {
		// No WHERE clause = update all rows
		predicate = func(r *row.Row) bool {
			return true
		}
	}

	// Execute update
	affected, err := tbl.Update(predicate, updates)
	if err != nil {
		return nil, fmt.Errorf("update error: %w", err)
	}

	return &Result{
		Affected: affected,
		Message:  fmt.Sprintf("Query OK, %d rows affected", affected),
	}, nil
}

// executeDelete executes a DELETE statement.
func (e *Executor) executeDelete(stmt *sql.DeleteStmt) (*Result, error) {
	tableName := stmt.Table
	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	// Check if table exists
	if !e.engine.TableExists(tableName) {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Get table
	tbl, err := e.engine.GetTable(tableName)
	if err != nil {
		return nil, err
	}
	tblInfo := tbl.GetInfo()

	// Build column index map
	colIdxMap := make(map[string]int)
	for i, col := range tblInfo.Columns {
		colIdxMap[strings.ToLower(col.Name)] = i
	}

	// Build predicate function
	var predicate func(*row.Row) bool
	if stmt.Where != nil {
		predicate = func(r *row.Row) bool {
			match, _ := e.evaluateWhereForRow(stmt.Where, r, tblInfo.Columns, colIdxMap)
			return match
		}
	} else {
		// No WHERE clause = delete all rows (use Truncate for efficiency)
		// Capture row count BEFORE truncate since Truncate modifies it
		rowCount := tblInfo.RowCount
		if err := tbl.Truncate(); err != nil {
			return nil, fmt.Errorf("delete error: %w", err)
		}
		return &Result{
			Affected: int(rowCount),
			Message:  "Query OK, all rows deleted",
		}, nil
	}

	// Execute delete
	affected, err := tbl.Delete(predicate)
	if err != nil {
		return nil, fmt.Errorf("delete error: %w", err)
	}

	return &Result{
		Affected: affected,
		Message:  fmt.Sprintf("Query OK, %d rows affected", affected),
	}, nil
}

// evaluateWhereForRow evaluates a WHERE expression for a row.
func (e *Executor) evaluateWhereForRow(expr sql.Expression, r *row.Row, columns []*types.ColumnInfo, colIdxMap map[string]int) (bool, error) {
	switch ex := expr.(type) {
	case *sql.BinaryExpr:
		// Handle IN operator specially (right side could be a subquery)
		if ex.Op == sql.OpIn {
			left, err := e.evaluateExprForRow(ex.Left, r, columns, colIdxMap)
			if err != nil {
				return false, err
			}

			// Check if right side is a subquery
			if subq, ok := ex.Right.(*sql.SubqueryExpr); ok {
				result, err := e.executeStatement(subq.Select)
				if err != nil {
					return false, err
				}
				// Check if value is in subquery results
				for _, row := range result.Rows {
					if len(row) > 0 {
						if compareEqual(left, row[0]) {
							return true, nil
						}
					}
				}
				return false, nil
			}

			// Check if right side is a parenthesized expression with subquery
			if paren, ok := ex.Right.(*sql.ParenExpr); ok {
				if subq, ok := paren.Expr.(*sql.SubqueryExpr); ok {
					result, err := e.executeStatement(subq.Select)
					if err != nil {
						return false, err
					}
					// Check if value is in subquery results
					for _, row := range result.Rows {
						if len(row) > 0 {
							if compareEqual(left, row[0]) {
								return true, nil
							}
						}
					}
					return false, nil
				}
			}

			// For non-subquery IN, we'd need a list - not implemented yet
			return false, nil
		}

		left, err := e.evaluateExprForRow(ex.Left, r, columns, colIdxMap)
		if err != nil {
			return false, err
		}
		right, err := e.evaluateExprForRow(ex.Right, r, columns, colIdxMap)
		if err != nil {
			return false, err
		}
		return e.compareValues(left, ex.Op, right)

	case *sql.UnaryExpr:
		if ex.Op == sql.OpNot {
			val, err := e.evaluateWhereForRow(ex.Right, r, columns, colIdxMap)
			if err != nil {
				return false, err
			}
			return !val, nil
		}

	case *sql.IsNullExpr:
		val, err := e.evaluateExprForRow(ex.Expr, r, columns, colIdxMap)
		if err != nil {
			return false, err
		}
		if ex.Not {
			return val != nil, nil
		}
		return val == nil, nil

	case *sql.InExpr:
		// Evaluate the expression
		val, err := e.evaluateExprForRow(ex.Expr, r, columns, colIdxMap)
		if err != nil {
			return false, err
		}

		// Handle subquery
		if ex.Select != nil {
			result, err := e.executeStatement(ex.Select)
			if err != nil {
				return false, err
			}
			// Check if value is in subquery results
			for _, row := range result.Rows {
				if len(row) > 0 {
					if compareEqual(val, row[0]) {
						return !ex.Not, nil
					}
				}
			}
			return ex.Not, nil
		}

		// Handle value list
		for _, listExpr := range ex.List {
			listVal, err := e.evaluateExprForRow(listExpr, r, columns, colIdxMap)
			if err != nil {
				continue
			}
			if compareEqual(val, listVal) {
				return !ex.Not, nil
			}
		}
		return ex.Not, nil

	case *sql.ExistsExpr:
		// Build outer context from the current row for correlated subqueries
		outerCtx := make(map[string]interface{})
		tablePrefix := ""
		if e.currentTable != "" {
			tablePrefix = e.currentTable + "."
		}
		for i, col := range columns {
			if i < len(r.Values) {
				val := e.valueToInterface(r.Values[i])
				colName := strings.ToLower(col.Name)
				// Store without table prefix
				outerCtx[colName] = val
				// Store with table prefix (e.g., users.id)
				if tablePrefix != "" {
					outerCtx[tablePrefix+colName] = val
				}
			}
		}

		// Save current outer context and set new one
		oldOuterCtx := e.outerContext
		e.outerContext = outerCtx

		// Execute the subquery
		result, err := e.executeStatement(ex.Subquery.Select)

		// Restore old outer context
		e.outerContext = oldOuterCtx

		if err != nil {
			return false, err
		}
		// EXISTS returns true if the subquery returns any rows
		return len(result.Rows) > 0, nil

	case *sql.AnyAllExpr:
		// Evaluate the left expression
		left, err := e.evaluateExprForRow(ex.Left, r, columns, colIdxMap)
		if err != nil {
			return false, err
		}

		// Build outer context from the current row for correlated subqueries
		outerCtx := make(map[string]interface{})
		tablePrefix := ""
		if e.currentTable != "" {
			tablePrefix = e.currentTable + "."
		}
		for i, col := range columns {
			if i < len(r.Values) {
				val := e.valueToInterface(r.Values[i])
				colName := strings.ToLower(col.Name)
				outerCtx[colName] = val
				if tablePrefix != "" {
					outerCtx[tablePrefix+colName] = val
				}
			}
		}

		// Save current outer context and set new one
		oldOuterCtx := e.outerContext
		e.outerContext = outerCtx

		// Execute the subquery
		subqResult, err := e.executeStatement(ex.Subquery.Select)

		// Restore old outer context
		e.outerContext = oldOuterCtx

		if err != nil {
			return false, err
		}

		// Evaluate ANY/ALL
		if ex.IsAny {
			// ANY: returns true if comparison is true for at least one value
			for _, row := range subqResult.Rows {
				if len(row) > 0 {
					cmp, err := e.compareValues(left, ex.Op, row[0])
					if err == nil && cmp {
						return true, nil
					}
				}
			}
			return false, nil
		} else {
			// ALL: returns true if comparison is true for all values
			if len(subqResult.Rows) == 0 {
				return true, nil // ALL on empty set is true
			}
			for _, row := range subqResult.Rows {
				if len(row) > 0 {
					cmp, err := e.compareValues(left, ex.Op, row[0])
					if err != nil || !cmp {
						return false, nil
					}
				}
			}
			return true, nil
		}

	case *sql.ScalarSubquery:
		// Execute the scalar subquery
		oldOuterCtx := e.outerContext
		result, err := e.executeStatement(ex.Subquery.Select)
		e.outerContext = oldOuterCtx

		if err != nil {
			return false, err
		}

		// Scalar subquery must return exactly one row and one column
		if len(result.Rows) == 0 {
			return false, nil // No rows = NULL, which is false
		}
		if len(result.Rows) > 1 {
			return false, fmt.Errorf("scalar subquery returned more than one row")
		}
		if len(result.Rows[0]) == 0 {
			return false, nil
		}

		// Check if the result is truthy
		val := result.Rows[0][0]
		if val == nil {
			return false, nil
		}
		switch v := val.(type) {
		case bool:
			return v, nil
		case int, int64, float64:
			return v != 0, nil
		case string:
			return v != "", nil
		default:
			return val != nil, nil
		}

	case *sql.Literal:
		if ex.Type == sql.LiteralBool {
			if b, ok := ex.Value.(bool); ok {
				return b, nil
			}
		}
	}

	return false, nil
}

// evaluateExprForRow evaluates an expression for a row.
func (e *Executor) evaluateExprForRow(expr sql.Expression, r *row.Row, columns []*types.ColumnInfo, colIdxMap map[string]int) (interface{}, error) {
	switch ex := expr.(type) {
	case *sql.Literal:
		return ex.Value, nil

	case *sql.ColumnRef:
		colName := strings.ToLower(ex.Name)

		// If column has a table prefix, check if it matches the current table
		if ex.Table != "" {
			tableName := strings.ToLower(ex.Table)
			// Check if the table prefix matches the current table
			if e.currentTable != "" && tableName != e.currentTable {
				// Column is from a different table - check outer context
				if e.outerContext != nil {
					qualifiedName := tableName + "." + colName
					if val, ok := e.outerContext[qualifiedName]; ok {
						return val, nil
					}
					// Also try without table prefix
					if val, ok := e.outerContext[colName]; ok {
						return val, nil
					}
				}
				return nil, fmt.Errorf("unknown column: %s.%s", ex.Table, ex.Name)
			}
			// Table prefix matches current table, look up the column
		}

		idx, ok := colIdxMap[colName]
		if !ok {
			// Check outer context for correlated subqueries
			if e.outerContext != nil {
				if val, ok := e.outerContext[colName]; ok {
					return val, nil
				}
			}
			return nil, fmt.Errorf("unknown column: %s", ex.Name)
		}
		if idx < len(r.Values) {
			return e.valueToInterface(r.Values[idx]), nil
		}
		return nil, nil

	case *sql.ScalarSubquery:
		// Execute the scalar subquery
		result, err := e.executeStatement(ex.Subquery.Select)
		if err != nil {
			return nil, err
		}
		if len(result.Rows) == 0 {
			return nil, nil
		}
		if len(result.Rows) > 1 {
			return nil, fmt.Errorf("scalar subquery returned more than one row")
		}
		if len(result.Rows[0]) == 0 {
			return nil, nil
		}
		return result.Rows[0][0], nil

	case *sql.SubqueryExpr:
		// Treat as scalar subquery in expression context
		result, err := e.executeStatement(ex.Select)
		if err != nil {
			return nil, err
		}
		if len(result.Rows) == 0 {
			return nil, nil
		}
		if len(result.Rows) > 1 {
			return nil, fmt.Errorf("scalar subquery returned more than one row")
		}
		if len(result.Rows[0]) == 0 {
			return nil, nil
		}
		return result.Rows[0][0], nil

	case *sql.BinaryExpr:
		left, err := e.evaluateExprForRow(ex.Left, r, columns, colIdxMap)
		if err != nil {
			return nil, err
		}
		right, err := e.evaluateExprForRow(ex.Right, r, columns, colIdxMap)
		if err != nil {
			return nil, err
		}
		return e.evaluateBinaryOp(left, ex.Op, right)

	case *sql.FunctionCall:
		// Build column info from the columns slice
		columnOrder := make([]*types.ColumnInfo, len(columns))
		columnMap := make(map[string]*types.ColumnInfo)
		for i, col := range columns {
			columnOrder[i] = col
			columnMap[strings.ToLower(col.Name)] = col
		}
		return e.evaluateFunction(ex, r, columnMap, columnOrder)
	}
	return nil, nil
}

// executeCreateTable executes a CREATE TABLE statement.
func (e *Executor) executeCreateTable(stmt *sql.CreateTableStmt) (*Result, error) {
	// Check if table already exists
	if e.engine.TableExists(stmt.TableName) {
		if stmt.IfNotExists {
			return &Result{Message: "OK"}, nil
		}
		return nil, fmt.Errorf("table %s already exists", stmt.TableName)
	}

	// Convert column definitions
	columns := make([]*types.ColumnInfo, len(stmt.Columns))
	for i, colDef := range stmt.Columns {
		colType := types.ParseTypeID(colDef.Type.Name)
		if colType == types.TypeNull {
			return nil, fmt.Errorf("unknown type: %s", colDef.Type.Name)
		}

		col := &types.ColumnInfo{
			Name:       colDef.Name,
			Type:       colType,
			Size:       colDef.Type.Size,
			Precision:  colDef.Type.Precision,
			Scale:      colDef.Type.Scale,
			Nullable:   colDef.Nullable,
			PrimaryKey: colDef.PrimaryKey,
			AutoIncr:   colDef.AutoIncr || colType == types.TypeSeq, // SEQ type is auto-increment
			Unique:     colDef.Unique,
		}

		// Process DEFAULT value
		if colDef.Default != nil {
			defaultVal, err := e.expressionToValue(colDef.Default, col)
			if err != nil {
				return nil, fmt.Errorf("invalid default value for column %s: %w", colDef.Name, err)
			}
			col.Default = defaultVal
		}

		columns[i] = col
	}

	// Check for primary key in constraints
	var checkConstraints []*types.CheckConstraintInfo
	var foreignKeys []*types.ForeignKeyInfo

	for _, c := range stmt.Constraints {
		if c.Type == sql.ConstraintPrimaryKey {
			for _, colName := range c.Columns {
				for _, col := range columns {
					if strings.EqualFold(col.Name, colName) {
						col.PrimaryKey = true
					}
				}
			}
		}
		// Check for UNIQUE constraint at table level
		if c.Type == sql.ConstraintUnique {
			for _, colName := range c.Columns {
				for _, col := range columns {
					if strings.EqualFold(col.Name, colName) {
						col.Unique = true
					}
				}
			}
		}
		// CHECK constraint
		if c.Type == sql.ConstraintCheck {
			checkConstraints = append(checkConstraints, &types.CheckConstraintInfo{
				Name:       c.Name,
				Expression: c.CheckExpr.String(),
			})
		}
		// FOREIGN KEY constraint
		if c.Type == sql.ConstraintForeignKey {
			fk := &types.ForeignKeyInfo{
				Name:       c.Name,
				Columns:    c.Columns,
				RefTable:   c.RefTable,
				RefColumns: c.RefColumns,
				OnDelete:   c.OnDelete,
				OnUpdate:   c.OnUpdate,
			}
			// Set default actions if not specified
			if fk.OnDelete == "" {
				fk.OnDelete = "RESTRICT"
			}
			if fk.OnUpdate == "" {
				fk.OnUpdate = "RESTRICT"
			}
			foreignKeys = append(foreignKeys, fk)
		}
	}

	// Create the table
	if err := e.engine.CreateTable(stmt.TableName, columns); err != nil {
		return nil, fmt.Errorf("create table error: %w", err)
	}

	// Get the table and add constraints
	tbl, err := e.engine.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Add CHECK constraints
	if len(checkConstraints) > 0 {
		if err := tbl.AddCheckConstraints(checkConstraints); err != nil {
			return nil, fmt.Errorf("failed to add check constraints: %w", err)
		}
	}

	// Add FOREIGN KEY constraints
	if len(foreignKeys) > 0 {
		if err := tbl.AddForeignKeys(foreignKeys); err != nil {
			return nil, fmt.Errorf("failed to add foreign keys: %w", err)
		}
	}

	// Create indexes for UNIQUE columns
	for _, col := range columns {
		if col.Unique && !col.PrimaryKey {
			if err := tbl.CreateIndex(col.Name, []string{col.Name}, true); err != nil {
				// Log warning but don't fail
			}
		}
	}

	return &Result{Message: "OK"}, nil
}

// executeDropTable executes a DROP TABLE statement.
func (e *Executor) executeDropTable(stmt *sql.DropTableStmt) (*Result, error) {
	// Check if table exists
	if !e.engine.TableExists(stmt.TableName) {
		if stmt.IfExists {
			return &Result{Message: "OK"}, nil
		}
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	if err := e.engine.DropTable(stmt.TableName); err != nil {
		return nil, fmt.Errorf("drop table error: %w", err)
	}

	return &Result{Message: "OK"}, nil
}

// executeCreateIndex executes a CREATE INDEX statement.
func (e *Executor) executeCreateIndex(stmt *sql.CreateIndexStmt) (*Result, error) {
	// Check if table exists
	if !e.engine.TableExists(stmt.TableName) {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	// Get the table
	tbl, err := e.engine.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Create the index
	if err := tbl.CreateIndex(stmt.IndexName, stmt.Columns, stmt.Unique); err != nil {
		return nil, fmt.Errorf("create index error: %w", err)
	}

	return &Result{Message: "OK"}, nil
}

// executeDropIndex executes a DROP INDEX statement.
func (e *Executor) executeDropIndex(stmt *sql.DropIndexStmt) (*Result, error) {
	// Check if table is specified
	if stmt.TableName == "" {
		return nil, fmt.Errorf("table name required for DROP INDEX")
	}

	// Check if table exists
	if !e.engine.TableExists(stmt.TableName) {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	// Get the table
	tbl, err := e.engine.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Drop the index
	if err := tbl.DropIndex(stmt.IndexName); err != nil {
		return nil, fmt.Errorf("drop index error: %w", err)
	}

	return &Result{Message: "OK"}, nil
}

// executeAlterTable executes an ALTER TABLE statement.
func (e *Executor) executeAlterTable(stmt *sql.AlterTableStmt) (*Result, error) {
	// Check if table exists
	if !e.engine.TableExists(stmt.TableName) {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	// Get the table
	tbl, err := e.engine.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	tableName := stmt.TableName

	// Process each action
	for _, action := range stmt.Actions {
		switch a := action.(type) {
		case *sql.AddColumnAction:
			if err := e.executeAddColumn(tableName, a); err != nil {
				return nil, err
			}
		case *sql.DropColumnAction:
			if err := e.executeDropColumn(tableName, a); err != nil {
				return nil, err
			}
		case *sql.ModifyColumnAction:
			if err := e.executeModifyColumn(tableName, a); err != nil {
				return nil, err
			}
		case *sql.RenameColumnAction:
			if err := tbl.RenameColumn(a.OldName, a.NewName); err != nil {
				return nil, err
			}
		case *sql.RenameTableAction:
			if err := e.engine.RenameTable(tableName, a.NewName); err != nil {
				return nil, err
			}
			// Update table name for subsequent actions
			tableName = a.NewName
		case *sql.AddConstraintAction:
			if err := e.executeAddConstraint(tableName, a); err != nil {
				return nil, err
			}
		case *sql.DropConstraintAction:
			if err := e.executeDropConstraint(tableName, a); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported alter action: %T", action)
		}
	}

	return &Result{Message: "OK"}, nil
}

// executeAddColumn adds a column to a table.
func (e *Executor) executeAddColumn(tableName string, action *sql.AddColumnAction) error {
	tbl, err := e.engine.GetTable(tableName)
	if err != nil {
		return err
	}

	col := action.Column
	colType := types.ParseTypeID(col.Type.Name)
	if colType == types.TypeNull {
		return fmt.Errorf("unknown type: %s", col.Type.Name)
	}

	newCol := &types.ColumnInfo{
		Name:       col.Name,
		Type:       colType,
		Size:       col.Type.Size,
		Nullable:   col.Nullable,
		PrimaryKey: col.PrimaryKey,
		AutoIncr:   col.AutoIncr,
	}

	return tbl.AddColumn(newCol)
}

// executeDropColumn drops a column from a table.
func (e *Executor) executeDropColumn(tableName string, action *sql.DropColumnAction) error {
	tbl, err := e.engine.GetTable(tableName)
	if err != nil {
		return err
	}
	return tbl.DropColumn(action.ColumnName)
}

// executeModifyColumn modifies a column in a table.
func (e *Executor) executeModifyColumn(tableName string, action *sql.ModifyColumnAction) error {
	tbl, err := e.engine.GetTable(tableName)
	if err != nil {
		return err
	}

	col := action.Column
	colType := types.ParseTypeID(col.Type.Name)
	if colType == types.TypeNull {
		return fmt.Errorf("unknown type: %s", col.Type.Name)
	}

	newCol := &types.ColumnInfo{
		Name:       col.Name,
		Type:       colType,
		Size:       col.Type.Size,
		Nullable:   col.Nullable,
		PrimaryKey: col.PrimaryKey,
		AutoIncr:   col.AutoIncr,
	}

	return tbl.ModifyColumn(newCol)
}

// executeUse executes a USE DATABASE statement.
func (e *Executor) executeUse(stmt *sql.UseStmt) (*Result, error) {
	e.database = stmt.Database
	return &Result{Message: "Database changed"}, nil
}

// executeShow executes a SHOW statement.
func (e *Executor) executeShow(stmt *sql.ShowStmt) (*Result, error) {
	switch strings.ToUpper(stmt.Type) {
	case "TABLES":
		tables := e.engine.ListTables()
		result := &Result{
			Columns: []ColumnInfo{{Name: "Tables_in_" + e.database, Type: "VARCHAR"}},
			Rows:    make([][]interface{}, len(tables)),
		}
		for i, t := range tables {
			result.Rows[i] = []interface{}{t}
		}
		result.RowCount = len(tables)
		return result, nil

	case "DATABASES":
		return &Result{
			Columns: []ColumnInfo{{Name: "Database", Type: "VARCHAR"}},
			Rows:    [][]interface{}{{e.database}},
			RowCount: 1,
		}, nil

	case "COLUMNS", "FIELDS":
		if stmt.From == "" {
			return nil, fmt.Errorf("table name required for SHOW COLUMNS")
		}
		tbl, err := e.engine.GetTable(stmt.From)
		if err != nil {
			return nil, fmt.Errorf("table %s does not exist", stmt.From)
		}
		info := tbl.GetInfo()
		result := &Result{
			Columns: []ColumnInfo{
				{Name: "Field", Type: "VARCHAR"},
				{Name: "Type", Type: "VARCHAR"},
				{Name: "Null", Type: "VARCHAR"},
				{Name: "Key", Type: "VARCHAR"},
				{Name: "Default", Type: "VARCHAR"},
				{Name: "Extra", Type: "VARCHAR"},
			},
			Rows: make([][]interface{}, len(info.Columns)),
		}
		for i, col := range info.Columns {
			key := ""
			if col.PrimaryKey {
				key = "PRI"
			}
			extra := ""
			if col.AutoIncr {
				extra = "auto_increment"
			}
			null := "YES"
			if !col.Nullable {
				null = "NO"
			}
			result.Rows[i] = []interface{}{
				col.Name,
				col.Type.String(),
				null,
				key,
				nil,
				extra,
			}
		}
		result.RowCount = len(info.Columns)
		return result, nil

	default:
		return nil, fmt.Errorf("unsupported SHOW type: %s", stmt.Type)
	}
}

// executeDescribe executes a DESCRIBE/DESC statement.
func (e *Executor) executeDescribe(stmt *sql.DescribeStmt) (*Result, error) {
	// Check if table exists
	tbl, err := e.engine.GetTable(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	info := tbl.GetInfo()
	result := &Result{
		Columns: []ColumnInfo{
			{Name: "Field", Type: "VARCHAR"},
			{Name: "Type", Type: "VARCHAR"},
			{Name: "Null", Type: "VARCHAR"},
			{Name: "Key", Type: "VARCHAR"},
			{Name: "Default", Type: "VARCHAR"},
			{Name: "Extra", Type: "VARCHAR"},
		},
		Rows: make([][]interface{}, len(info.Columns)),
	}

	for i, col := range info.Columns {
		key := ""
		if col.PrimaryKey {
			key = "PRI"
		} else if col.Unique {
			key = "UNI"
		}
		extra := ""
		if col.AutoIncr {
			extra = "auto_increment"
		}
		null := "YES"
		if !col.Nullable {
			null = "NO"
		}
		var defaultVal interface{}
		if col.Default.Type != types.TypeNull {
			defaultVal = col.Default.String()
		}
		result.Rows[i] = []interface{}{
			col.Name,
			col.Type.String(),
			null,
			key,
			defaultVal,
			extra,
		}
	}
	result.RowCount = len(info.Columns)
	return result, nil
}

// executeShowCreateTable executes a SHOW CREATE TABLE statement.
func (e *Executor) executeShowCreateTable(stmt *sql.ShowCreateTableStmt) (*Result, error) {
	// Check if table exists
	tbl, err := e.engine.GetTable(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	info := tbl.GetInfo()

	// Build CREATE TABLE statement
	var sb strings.Builder
	sb.WriteString("CREATE TABLE `")
	sb.WriteString(stmt.TableName)
	sb.WriteString("` (\n")

	// Columns
	for i, col := range info.Columns {
		if i > 0 {
			sb.WriteString(",\n")
		}
		sb.WriteString("  `")
		sb.WriteString(col.Name)
		sb.WriteString("` ")
		sb.WriteString(col.Type.String())
		if col.Size > 0 && (col.Type == types.TypeChar || col.Type == types.TypeVarchar) {
			sb.WriteString("(")
			sb.WriteString(strconv.Itoa(col.Size))
			sb.WriteString(")")
		}
		if !col.Nullable {
			sb.WriteString(" NOT NULL")
		}
		if col.Default.Type != types.TypeNull {
			sb.WriteString(" DEFAULT ")
			sb.WriteString(col.Default.String())
		}
		if col.AutoIncr {
			sb.WriteString(" AUTO_INCREMENT")
		}
		if col.PrimaryKey {
			sb.WriteString(" PRIMARY KEY")
		}
		if col.Unique {
			sb.WriteString(" UNIQUE")
		}
	}

	// Table constraints (CHECK, FOREIGN KEY)
	for _, ck := range info.CheckConstraints {
		sb.WriteString(",\n  CHECK (")
		sb.WriteString(ck.Expression)
		sb.WriteString(")")
	}

	for _, fk := range info.ForeignKeys {
		sb.WriteString(",\n  FOREIGN KEY (")
		for i, col := range fk.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("`")
			sb.WriteString(col)
			sb.WriteString("`")
		}
		sb.WriteString(") REFERENCES `")
		sb.WriteString(fk.RefTable)
		sb.WriteString("` (")
		for i, col := range fk.RefColumns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("`")
			sb.WriteString(col)
			sb.WriteString("`")
		}
		sb.WriteString(")")
		if fk.OnDelete != "" {
			sb.WriteString(" ON DELETE ")
			sb.WriteString(fk.OnDelete)
		}
		if fk.OnUpdate != "" {
			sb.WriteString(" ON UPDATE ")
			sb.WriteString(fk.OnUpdate)
		}
	}

	sb.WriteString("\n)")

	result := &Result{
		Columns: []ColumnInfo{
			{Name: "Table", Type: "VARCHAR"},
			{Name: "Create Table", Type: "VARCHAR"},
		},
		Rows:    [][]interface{}{{stmt.TableName, sb.String()}},
		RowCount: 1,
	}
	return result, nil
}

// executeAddConstraint adds a constraint to a table.
func (e *Executor) executeAddConstraint(tableName string, action *sql.AddConstraintAction) error {
	tbl, err := e.engine.GetTable(tableName)
	if err != nil {
		return err
	}

	constraint := action.Constraint
	switch constraint.Type {
	case sql.ConstraintPrimaryKey:
		// Create primary key index
		for _, colName := range constraint.Columns {
			if err := tbl.SetPrimaryKey(colName); err != nil {
				return err
			}
		}

	case sql.ConstraintUnique:
		// Create unique index
		indexName := constraint.Name
		if indexName == "" {
			indexName = tableName + "_" + strings.Join(constraint.Columns, "_") + "_unique"
		}
		for _, colName := range constraint.Columns {
			if err := tbl.AddUniqueConstraint(colName, indexName); err != nil {
				return err
			}
		}

	case sql.ConstraintCheck:
		// Add CHECK constraint
		exprStr := ""
		if constraint.CheckExpr != nil {
			exprStr = constraint.CheckExpr.String()
		}
		ckInfo := &types.CheckConstraintInfo{
			Name:       constraint.Name,
			Expression: exprStr,
		}
		if err := tbl.AddCheckConstraint(ckInfo); err != nil {
			return err
		}

	case sql.ConstraintForeignKey:
		// Add FOREIGN KEY constraint
		fkInfo := &types.ForeignKeyInfo{
			Name:       constraint.Name,
			Columns:    constraint.Columns,
			RefTable:   constraint.RefTable,
			RefColumns: constraint.RefColumns,
			OnDelete:   constraint.OnDelete,
			OnUpdate:   constraint.OnUpdate,
		}
		if err := tbl.AddForeignKey(fkInfo); err != nil {
			return err
		}
	}

	return nil
}

// executeDropConstraint drops a constraint from a table.
func (e *Executor) executeDropConstraint(tableName string, action *sql.DropConstraintAction) error {
	tbl, err := e.engine.GetTable(tableName)
	if err != nil {
		return err
	}

	constraintName := action.ConstraintName

	// Try to drop as CHECK constraint
	if err := tbl.DropCheckConstraint(constraintName); err == nil {
		return nil
	}

	// Try to drop as FOREIGN KEY
	if err := tbl.DropForeignKey(constraintName); err == nil {
		return nil
	}

	// Try to drop as unique constraint
	if err := tbl.DropUniqueConstraint(constraintName); err == nil {
		return nil
	}

	return fmt.Errorf("constraint %s not found", constraintName)
}

// BackupManagerWrapper wraps the backup.Manager for use with storage.Engine.
type BackupManagerWrapper struct {
	*backup.Manager
}

// NewBackupManager creates a backup manager for the storage engine.
func NewBackupManager(engine *storage.Engine) *BackupManagerWrapper {
	return &BackupManagerWrapper{
		Manager: backup.NewManager(engine.GetDataDir()),
	}
}

// executeBackup executes a BACKUP DATABASE statement.
func (e *Executor) executeBackup(stmt *sql.BackupStmt) (*Result, error) {
	// Import backup package
	backupMgr := NewBackupManager(e.engine)

	opts := backup.BackupOptions{
		Path:     stmt.Path,
		Compress: stmt.Compress,
		Database: e.database,
	}

	manifest, err := backupMgr.Backup(opts)
	if err != nil {
		return nil, fmt.Errorf("backup failed: %w", err)
	}

	// Calculate the actual backup path (backup.Backup adds .xbak extension for compressed backups)
	backupPath := stmt.Path
	if stmt.Compress {
		backupPath = stmt.Path + backup.BackupExt
	}

	return &Result{
		Message: fmt.Sprintf("Backup completed: %s (%d tables, %s)", backupPath, manifest.TableCount, manifest.Timestamp),
	}, nil
}

// executeRestore executes a RESTORE DATABASE statement.
func (e *Executor) executeRestore(stmt *sql.RestoreStmt) (*Result, error) {
	backupMgr := NewBackupManager(e.engine)

	opts := backup.RestoreOptions{
		Path: stmt.Path,
	}

	manifest, err := backupMgr.Restore(opts)
	if err != nil {
		return nil, fmt.Errorf("restore failed: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("Restore completed: %s (%d tables, %s)", stmt.Path, manifest.TableCount, manifest.Timestamp),
	}, nil
}

// executeCreateFunction executes a CREATE FUNCTION statement.
func (e *Executor) executeCreateFunction(stmt *sql.CreateFunctionStmt) (*Result, error) {
	if e.udfManager == nil {
		return nil, fmt.Errorf("UDF manager not initialized")
	}

	fn := &sql.UserFunction{
		Name:       strings.ToUpper(stmt.Name),
		Parameters: stmt.Parameters,
		ReturnType: stmt.ReturnType,
		Body:       stmt.Body,
	}

	if err := e.udfManager.CreateFunction(fn, stmt.Replace); err != nil {
		return nil, err
	}

	// Save to disk
	if err := e.udfManager.Save(); err != nil {
		return nil, fmt.Errorf("failed to save function: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("Function %s created", stmt.Name),
	}, nil
}

// executeDropFunction executes a DROP FUNCTION statement.
func (e *Executor) executeDropFunction(stmt *sql.DropFunctionStmt) (*Result, error) {
	if e.udfManager == nil {
		return nil, fmt.Errorf("UDF manager not initialized")
	}

	name := strings.ToUpper(stmt.Name)
	if err := e.udfManager.DropFunction(name); err != nil {
		if stmt.IfExists {
			return &Result{Message: "OK"}, nil
		}
		return nil, err
	}

	// Save to disk
	if err := e.udfManager.Save(); err != nil {
		return nil, fmt.Errorf("failed to save functions: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("Function %s dropped", stmt.Name),
	}, nil
}

// executeTruncate executes a TRUNCATE TABLE statement.
func (e *Executor) executeTruncate(stmt *sql.TruncateTableStmt) (*Result, error) {
	// Check if table exists
	if !e.engine.TableExists(stmt.TableName) {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	// Drop and recreate the table (simple implementation)
	tbl, err := e.engine.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}
	info := tbl.GetInfo()

	if err := e.engine.DropTable(stmt.TableName); err != nil {
		return nil, err
	}

	if err := e.engine.CreateTable(stmt.TableName, info.Columns); err != nil {
		return nil, err
	}

	return &Result{Message: "OK"}, nil
}

// evaluateWhere evaluates a WHERE expression against a row.
func (e *Executor) evaluateWhere(expr sql.Expression, r *row.Row, columnMap map[string]*types.ColumnInfo, columnOrder []*types.ColumnInfo) (bool, error) {
	switch ex := expr.(type) {
	case *sql.BinaryExpr:
		// Handle logical operators
		if ex.Op == sql.OpAnd {
			left, err := e.evaluateWhere(ex.Left, r, columnMap, columnOrder)
			if err != nil {
				return false, err
			}
			if !left {
				return false, nil // Short-circuit
			}
			return e.evaluateWhere(ex.Right, r, columnMap, columnOrder)
		}
		if ex.Op == sql.OpOr {
			left, err := e.evaluateWhere(ex.Left, r, columnMap, columnOrder)
			if err != nil {
				return false, err
			}
			if left {
				return true, nil // Short-circuit
			}
			return e.evaluateWhere(ex.Right, r, columnMap, columnOrder)
		}

		// Handle IN operator specially (right side could be a subquery)
		if ex.Op == sql.OpIn {
			left, err := e.evaluateExpression(ex.Left, r, columnMap, columnOrder)
			if err != nil {
				return false, err
			}

			// Check if right side is a subquery
			if subq, ok := ex.Right.(*sql.SubqueryExpr); ok {
				result, err := e.executeStatement(subq.Select)
				if err != nil {
					return false, err
				}
				// Check if value is in subquery results
				for _, row := range result.Rows {
					if len(row) > 0 {
						if compareEqual(left, row[0]) {
							return true, nil
						}
					}
				}
				return false, nil
			}

			// Check if right side is a parenthesized expression with subquery
			if paren, ok := ex.Right.(*sql.ParenExpr); ok {
				if subq, ok := paren.Expr.(*sql.SubqueryExpr); ok {
					result, err := e.executeStatement(subq.Select)
					if err != nil {
						return false, err
					}
					// Check if value is in subquery results
					for _, row := range result.Rows {
						if len(row) > 0 {
							if compareEqual(left, row[0]) {
								return true, nil
							}
						}
					}
					return false, nil
				}
			}

			// For non-subquery IN, we'd need a list - not implemented yet
			return false, nil
		}

		left, err := e.evaluateExpression(ex.Left, r, columnMap, columnOrder)
		if err != nil {
			return false, err
		}
		right, err := e.evaluateExpression(ex.Right, r, columnMap, columnOrder)
		if err != nil {
			return false, err
		}
		return e.compareValues(left, ex.Op, right)

	case *sql.UnaryExpr:
		if ex.Op == sql.OpNot {
			val, err := e.evaluateWhere(ex.Right, r, columnMap, columnOrder)
			if err != nil {
				return false, err
			}
			return !val, nil
		}

	case *sql.IsNullExpr:
		val, err := e.evaluateExpression(ex.Expr, r, columnMap, columnOrder)
		if err != nil {
			return false, err
		}
		if ex.Not {
			return val != nil, nil
		}
		return val == nil, nil

	case *sql.InExpr:
		// Evaluate the expression
		val, err := e.evaluateExpression(ex.Expr, r, columnMap, columnOrder)
		if err != nil {
			return false, err
		}

		// Handle subquery
		if ex.Select != nil {
			result, err := e.executeStatement(ex.Select)
			if err != nil {
				return false, err
			}
			// Check if value is in subquery results
			for _, row := range result.Rows {
				if len(row) > 0 {
					if compareEqual(val, row[0]) {
						return !ex.Not, nil
					}
				}
			}
			return ex.Not, nil
		}

		// Handle value list
		for _, listExpr := range ex.List {
			listVal, err := e.evaluateExpression(listExpr, r, columnMap, columnOrder)
			if err != nil {
				continue
			}
			if compareEqual(val, listVal) {
				return !ex.Not, nil
			}
		}
		return ex.Not, nil

	case *sql.ExistsExpr:
		// Build outer context from the current row for correlated subqueries
		outerCtx := make(map[string]interface{})
		tablePrefix := ""
		if e.currentTable != "" {
			tablePrefix = e.currentTable + "."
		}
		for i, col := range columnOrder {
			if i < len(r.Values) {
				val := e.valueToInterface(r.Values[i])
				colName := strings.ToLower(col.Name)
				// Store without table prefix
				outerCtx[colName] = val
				// Store with table prefix (e.g., users.id)
				if tablePrefix != "" {
					outerCtx[tablePrefix+colName] = val
				}
			}
		}

		// Save current outer context and set new one
		oldOuterCtx := e.outerContext
		e.outerContext = outerCtx

		// Execute the subquery
		result, err := e.executeStatement(ex.Subquery.Select)

		// Restore old outer context
		e.outerContext = oldOuterCtx

		if err != nil {
			return false, err
		}
		// EXISTS returns true if the subquery returns any rows
		return len(result.Rows) > 0, nil

	case *sql.AnyAllExpr:
		// Evaluate the left expression
		left, err := e.evaluateExpression(ex.Left, r, columnMap, columnOrder)
		if err != nil {
			return false, err
		}

		// Build outer context from the current row for correlated subqueries
		outerCtx := make(map[string]interface{})
		tablePrefix := ""
		if e.currentTable != "" {
			tablePrefix = e.currentTable + "."
		}
		for i, col := range columnOrder {
			if i < len(r.Values) {
				val := e.valueToInterface(r.Values[i])
				colName := strings.ToLower(col.Name)
				outerCtx[colName] = val
				if tablePrefix != "" {
					outerCtx[tablePrefix+colName] = val
				}
			}
		}

		// Save current outer context and set new one
		oldOuterCtx := e.outerContext
		e.outerContext = outerCtx

		// Execute the subquery
		subqResult, err := e.executeStatement(ex.Subquery.Select)

		// Restore old outer context
		e.outerContext = oldOuterCtx

		if err != nil {
			return false, err
		}

		// Evaluate ANY/ALL
		if ex.IsAny {
			// ANY: returns true if comparison is true for at least one value
			for _, row := range subqResult.Rows {
				if len(row) > 0 {
					cmp, err := e.compareValues(left, ex.Op, row[0])
					if err == nil && cmp {
						return true, nil
					}
				}
			}
			return false, nil
		} else {
			// ALL: returns true if comparison is true for all values
			if len(subqResult.Rows) == 0 {
				return true, nil // ALL on empty set is true
			}
			for _, row := range subqResult.Rows {
				if len(row) > 0 {
					cmp, err := e.compareValues(left, ex.Op, row[0])
					if err != nil || !cmp {
						return false, nil
					}
				}
			}
			return true, nil
		}

	case *sql.ScalarSubquery:
		// Execute the scalar subquery
		oldOuterCtx := e.outerContext
		result, err := e.executeStatement(ex.Subquery.Select)
		e.outerContext = oldOuterCtx

		if err != nil {
			return false, err
		}

		// Scalar subquery must return exactly one row and one column
		if len(result.Rows) == 0 {
			return false, nil // No rows = NULL, which is false
		}
		if len(result.Rows) > 1 {
			return false, fmt.Errorf("scalar subquery returned more than one row")
		}
		if len(result.Rows[0]) == 0 {
			return false, nil
		}

		// Check if the result is truthy
		val := result.Rows[0][0]
		if val == nil {
			return false, nil
		}
		switch v := val.(type) {
		case bool:
			return v, nil
		case int, int64, float64:
			return v != 0, nil
		case string:
			return v != "", nil
		default:
			return val != nil, nil
		}

	case *sql.Literal:
		if ex.Type == sql.LiteralBool {
			if b, ok := ex.Value.(bool); ok {
				return b, nil
			}
		}
	}

	return false, nil
}

// compareEqual checks if two values are equal.
func compareEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// evaluateExpression evaluates an expression against a row.
func (e *Executor) evaluateExpression(expr sql.Expression, r *row.Row, columnMap map[string]*types.ColumnInfo, columnOrder []*types.ColumnInfo) (interface{}, error) {
	switch ex := expr.(type) {
	case *sql.Literal:
		return ex.Value, nil

	case *sql.ColumnRef:
		colName := strings.ToLower(ex.Name)

		// If column has a table prefix, check if it matches the current table
		if ex.Table != "" {
			tableName := strings.ToLower(ex.Table)
			// Check if the table prefix matches the current table
			if e.currentTable != "" && tableName != e.currentTable {
				// Column is from a different table - check outer context
				if e.outerContext != nil {
					qualifiedName := tableName + "." + colName
					if val, ok := e.outerContext[qualifiedName]; ok {
						return val, nil
					}
					// Also try without table prefix
					if val, ok := e.outerContext[colName]; ok {
						return val, nil
					}
				}
				return nil, fmt.Errorf("unknown column: %s.%s", ex.Table, ex.Name)
			}
			// Table prefix matches current table, look up the column
		}

		colInfo, ok := columnMap[colName]
		if !ok {
			// Check outer context for correlated subqueries
			if e.outerContext != nil {
				if val, ok := e.outerContext[colName]; ok {
					return val, nil
				}
			}
			return nil, fmt.Errorf("unknown column: %s", ex.Name)
		}
		// Find column index
		for i, c := range columnOrder {
			if c == colInfo {
				if i < len(r.Values) {
					return e.valueToInterface(r.Values[i]), nil
				}
			}
		}
		return nil, nil

	case *sql.CastExpr:
		// Evaluate the inner expression
		val, err := e.evaluateExpression(ex.Expr, r, columnMap, columnOrder)
		if err != nil {
			return nil, err
		}
		// Cast to the target type
		return e.castValue(val, ex.Type.Name)

	case *sql.BinaryExpr:
		left, err := e.evaluateExpression(ex.Left, r, columnMap, columnOrder)
		if err != nil {
			return nil, err
		}
		right, err := e.evaluateExpression(ex.Right, r, columnMap, columnOrder)
		if err != nil {
			return nil, err
		}
		return e.evaluateBinaryOp(left, ex.Op, right)

	case *sql.UnaryExpr:
		val, err := e.evaluateExpression(ex.Right, r, columnMap, columnOrder)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		if ex.Op == sql.OpNeg {
			switch v := val.(type) {
			case int:
				return -v, nil
			case int64:
				return -v, nil
			case float64:
				return -v, nil
			}
		}
		return val, nil

	case *sql.FunctionCall:
		return e.evaluateFunction(ex, r, columnMap, columnOrder)

	case *sql.ScalarSubquery:
		// Build outer context for correlated subqueries
		outerCtx := make(map[string]interface{})
		tablePrefix := ""
		if e.currentTable != "" {
			tablePrefix = e.currentTable + "."
		}
		if r != nil && columnOrder != nil {
			for i, col := range columnOrder {
				if i < len(r.Values) {
					val := e.valueToInterface(r.Values[i])
					colName := strings.ToLower(col.Name)
					outerCtx[colName] = val
					if tablePrefix != "" {
						outerCtx[tablePrefix+colName] = val
					}
				}
			}
		}

		// Save and set outer context
		oldOuterCtx := e.outerContext
		e.outerContext = outerCtx

		// Execute the scalar subquery
		result, err := e.executeStatement(ex.Subquery.Select)

		// Restore outer context
		e.outerContext = oldOuterCtx

		if err != nil {
			return nil, err
		}
		// Scalar subquery must return exactly one row and one column
		if len(result.Rows) == 0 {
			return nil, nil // No rows = NULL
		}
		if len(result.Rows) > 1 {
			return nil, fmt.Errorf("scalar subquery returned more than one row")
		}
		if len(result.Rows[0]) == 0 {
			return nil, nil
		}
		return result.Rows[0][0], nil

	case *sql.SubqueryExpr:
		// Build outer context for correlated subqueries
		outerCtx := make(map[string]interface{})
		tablePrefix := ""
		if e.currentTable != "" {
			tablePrefix = e.currentTable + "."
		}
		if r != nil && columnOrder != nil {
			for i, col := range columnOrder {
				if i < len(r.Values) {
					val := e.valueToInterface(r.Values[i])
					colName := strings.ToLower(col.Name)
					outerCtx[colName] = val
					if tablePrefix != "" {
						outerCtx[tablePrefix+colName] = val
					}
				}
			}
		}

		// Save and set outer context
		oldOuterCtx := e.outerContext
		e.outerContext = outerCtx

		// Treat as scalar subquery in expression context
		result, err := e.executeStatement(ex.Select)

		// Restore outer context
		e.outerContext = oldOuterCtx

		if err != nil {
			return nil, err
		}
		if len(result.Rows) == 0 {
			return nil, nil
		}
		if len(result.Rows) > 1 {
			return nil, fmt.Errorf("scalar subquery returned more than one row")
		}
		if len(result.Rows[0]) == 0 {
			return nil, nil
		}
		return result.Rows[0][0], nil

	case *sql.AnyAllExpr:
		// Evaluate the left expression
		left, err := e.evaluateExpression(ex.Left, r, columnMap, columnOrder)
		if err != nil {
			return nil, err
		}

		// Execute the subquery
		result, err := e.executeStatement(ex.Subquery.Select)
		if err != nil {
			return nil, err
		}

		// Evaluate ANY/ALL
		if ex.IsAny {
			// ANY: returns true if comparison is true for at least one value
			for _, row := range result.Rows {
				if len(row) > 0 {
					cmp, err := e.compareValues(left, ex.Op, row[0])
					if err == nil && cmp {
						return true, nil
					}
				}
			}
			return false, nil
		} else {
			// ALL: returns true if comparison is true for all values
			if len(result.Rows) == 0 {
				return true, nil // ALL on empty set is true
			}
			for _, row := range result.Rows {
				if len(row) > 0 {
					cmp, err := e.compareValues(left, ex.Op, row[0])
					if err != nil || !cmp {
						return false, nil
					}
				}
			}
			return true, nil
		}
	}
	return nil, nil
}

// castValue casts a value to the target type.
func (e *Executor) castValue(val interface{}, targetType string) (interface{}, error) {
	if val == nil {
		return nil, nil
	}

	targetType = strings.ToUpper(targetType)

	switch targetType {
	case "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT":
		switch v := val.(type) {
		case int, int32, int64:
			return v, nil
		case float64:
			return int64(v), nil
		case string:
			i, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot cast '%s' to INT", v)
			}
			return i, nil
		case []byte:
			// Try to interpret as string first
			i, err := strconv.ParseInt(string(v), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot cast BLOB to INT")
			}
			return i, nil
		case bool:
			if v {
				return int64(1), nil
			}
			return int64(0), nil
		}

	case "FLOAT", "DOUBLE":
		switch v := val.(type) {
		case float64:
			return v, nil
		case int, int32, int64:
			return float64(v.(int64)), nil
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot cast '%s' to FLOAT", v)
			}
			return f, nil
		case []byte:
			f, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				return nil, fmt.Errorf("cannot cast BLOB to FLOAT")
			}
			return f, nil
		}

	case "VARCHAR", "CHAR", "TEXT":
		switch v := val.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		case int, int32, int64, float64, bool:
			return fmt.Sprintf("%v", v), nil
		default:
			return fmt.Sprintf("%v", v), nil
		}

	case "BLOB":
		switch v := val.(type) {
		case []byte:
			return v, nil
		case string:
			// Check if it's a hex string
			if len(v) >= 2 && (v[0:2] == "0x" || v[0:2] == "0X") {
				blob, err := types.HexToBlob(v)
				if err != nil {
					return nil, err
				}
				return blob.Data, nil
			}
			// Otherwise convert string to bytes
			return []byte(v), nil
		case int, int32, int64:
			// Convert integer to bytes
			i := v.(int64)
			return []byte(strconv.FormatInt(i, 10)), nil
		case float64:
			return []byte(strconv.FormatFloat(v, 'f', -1, 64)), nil
		case bool:
			if v {
				return []byte("1"), nil
			}
			return []byte("0"), nil
		default:
			return nil, fmt.Errorf("cannot cast to BLOB")
		}

	case "BOOL", "BOOLEAN":
		switch v := val.(type) {
		case bool:
			return v, nil
		case int, int32, int64:
			return v.(int64) != 0, nil
		case float64:
			return v != 0, nil
		case string:
			lower := strings.ToLower(v)
			return lower == "true" || lower == "1" || lower == "t", nil
		case []byte:
			lower := strings.ToLower(string(v))
			return lower == "true" || lower == "1" || lower == "t", nil
		}
	}

	// Default: return as-is
	return val, nil
}

// evaluateFunction evaluates a function call.
func (e *Executor) evaluateFunction(fc *sql.FunctionCall, r *row.Row, columnMap map[string]*types.ColumnInfo, columnOrder []*types.ColumnInfo) (interface{}, error) {
	funcName := strings.ToUpper(fc.Name)

	// Helper to evaluate expression when row might be nil
	var evalExpr func(expr sql.Expression) (interface{}, error)
	evalExpr = func(expr sql.Expression) (interface{}, error) {
		if r == nil {
			// No row context, evaluate directly
			switch ex := expr.(type) {
			case *sql.Literal:
				return ex.Value, nil
			case *sql.ColumnRef:
				return nil, nil
			case *sql.CastExpr:
				return e.castValueFromExpr(ex)
			case *sql.FunctionCall:
				return e.evaluateFunction(ex, nil, nil, nil)
			case *sql.UnaryExpr:
				val, err := evalExpr(ex.Right)
				if err != nil {
					return nil, err
				}
				return e.evaluateUnaryExpr(ex.Op, val)
			case *sql.BinaryExpr:
				left, err := evalExpr(ex.Left)
				if err != nil {
					return nil, err
				}
				right, err := evalExpr(ex.Right)
				if err != nil {
					return nil, err
				}
				return e.evaluateBinaryOp(left, ex.Op, right)
			default:
				return nil, nil
			}
		}
		return e.evaluateExpression(expr, r, columnMap, columnOrder)
	}

	switch funcName {
	case "HEX":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("HEX requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		switch v := arg.(type) {
		case []byte:
			return fmt.Sprintf("%x", v), nil
		case string:
			return fmt.Sprintf("%x", v), nil
		case int:
			return fmt.Sprintf("%x", v), nil
		case int32:
			return fmt.Sprintf("%x", v), nil
		case int64:
			return fmt.Sprintf("%x", v), nil
		default:
			return fmt.Sprintf("%x", v), nil
		}

	case "UNHEX":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("UNHEX requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		strVal, ok := arg.(string)
		if !ok {
			strVal = fmt.Sprintf("%v", arg)
		}
		blob, err := types.HexToBlob(strVal)
		if err != nil {
			return nil, err
		}
		return blob.Data, nil

	case "LENGTH", "OCTET_LENGTH":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("%s requires 1 argument", funcName)
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		switch v := arg.(type) {
		case []byte:
			return int64(len(v)), nil
		case string:
			return int64(len(v)), nil
		default:
			return int64(len(fmt.Sprintf("%v", v))), nil
		}

	case "UPPER", "UCASE":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("%s requires 1 argument", funcName)
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		return strings.ToUpper(fmt.Sprintf("%v", arg)), nil

	case "LOWER", "LCASE":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("%s requires 1 argument", funcName)
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		return strings.ToLower(fmt.Sprintf("%v", arg)), nil

	case "CONCAT":
		var result strings.Builder
		for _, argExpr := range fc.Args {
			arg, err := evalExpr(argExpr)
			if err != nil {
				return nil, err
			}
			if arg == nil {
				return nil, nil // CONCAT with NULL returns NULL
			}
			result.WriteString(fmt.Sprintf("%v", arg))
		}
		return result.String(), nil

	case "SUBSTRING", "SUBSTR":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("%s requires at least 2 arguments", funcName)
		}
		str, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if str == nil {
			return nil, nil
		}
		strVal := fmt.Sprintf("%v", str)

		start, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		startIdx := 0
		switch v := start.(type) {
		case int:
			startIdx = v - 1 // SQL is 1-indexed
		case int64:
			startIdx = int(v) - 1
		case float64:
			startIdx = int(v) - 1
		}
		if startIdx < 0 {
			startIdx = 0
		}

		if len(fc.Args) >= 3 {
			length, err := evalExpr(fc.Args[2])
			if err != nil {
				return nil, err
			}
			var lengthVal int
			switch v := length.(type) {
			case int:
				lengthVal = v
			case int64:
				lengthVal = int(v)
			case float64:
				lengthVal = int(v)
			}
			if startIdx+lengthVal > len(strVal) {
				return strVal[startIdx:], nil
			}
			return strVal[startIdx : startIdx+lengthVal], nil
		}
		return strVal[startIdx:], nil

	case "NOW", "CURRENT_TIMESTAMP":
		return time.Now().Format("2006-01-02 15:04:05"), nil

	case "DATABASE":
		if e.database != "" {
			return e.database, nil
		}
		return "", nil

	default:
		// Check for user-defined function
		if e.udfManager != nil {
			if udf, exists := e.udfManager.GetFunction(funcName); exists {
				return e.evaluateUDF(udf, fc.Args, evalExpr)
			}
		}
		// Unknown function - return nil (NULL)
		return nil, nil
	}
}

// evaluateUDF evaluates a user-defined function.
func (e *Executor) evaluateUDF(udf *sql.UserFunction, args []sql.Expression, evalExpr func(sql.Expression) (interface{}, error)) (interface{}, error) {
	// Create parameter value map
	paramValues := make(map[string]interface{})

	for i, param := range udf.Parameters {
		if i < len(args) {
			val, err := evalExpr(args[i])
			if err != nil {
				return nil, err
			}
			paramValues[strings.ToLower(param.Name)] = val
		} else if param.DefaultValue != nil {
			// Use default value
			val, err := e.evaluateExpressionWithParams(param.DefaultValue, paramValues)
			if err != nil {
				return nil, err
			}
			paramValues[strings.ToLower(param.Name)] = val
		}
	}

	// Evaluate body with parameter substitution
	return e.evaluateExpressionWithParams(udf.Body, paramValues)
}

// evaluateExpressionWithParams evaluates an expression with parameter substitution.
func (e *Executor) evaluateExpressionWithParams(expr sql.Expression, params map[string]interface{}) (interface{}, error) {
	switch ex := expr.(type) {
	case *sql.Literal:
		return ex.Value, nil
	case *sql.ColumnRef:
		// Check if it's a parameter
		if val, ok := params[strings.ToLower(ex.Name)]; ok {
			return val, nil
		}
		return nil, nil
	case *sql.BinaryExpr:
		left, err := e.evaluateExpressionWithParams(ex.Left, params)
		if err != nil {
			return nil, err
		}
		right, err := e.evaluateExpressionWithParams(ex.Right, params)
		if err != nil {
			return nil, err
		}
		return e.evaluateBinaryOp(left, ex.Op, right)
	case *sql.FunctionCall:
		// Nested function call - need to evaluate with params
		return e.evaluateUDFFunctionCall(ex, params)
	case *sql.UnaryExpr:
		val, err := e.evaluateExpressionWithParams(ex.Right, params)
		if err != nil {
			return nil, err
		}
		return e.evaluateUnaryExpr(ex.Op, val)
	case *sql.IfExpr:
		return e.evaluateIfExpr(ex, params)
	case *sql.LetExpr:
		return e.evaluateLetExpr(ex, params)
	case *sql.BlockExpr:
		return e.evaluateBlockExpr(ex, params)
	case *sql.CastExpr:
		return e.evaluateCastExprWithParams(ex, params)
	default:
		return nil, fmt.Errorf("unsupported expression type in UDF: %T", expr)
	}
}

// evaluateIfExpr evaluates an IF expression.
func (e *Executor) evaluateIfExpr(expr *sql.IfExpr, params map[string]interface{}) (interface{}, error) {
	cond, err := e.evaluateExpressionWithParams(expr.Condition, params)
	if err != nil {
		return nil, err
	}

	// Check if condition is true
	isTrue := false
	switch v := cond.(type) {
	case bool:
		isTrue = v
	case int, int64, float64:
		isTrue = v != 0
	case string:
		isTrue = v != ""
	default:
		isTrue = cond != nil
	}

	if isTrue {
		return e.evaluateExpressionWithParams(expr.ThenExpr, params)
	} else if expr.ElseExpr != nil {
		return e.evaluateExpressionWithParams(expr.ElseExpr, params)
	}

	return nil, nil
}

// evaluateLetExpr evaluates a LET expression.
func (e *Executor) evaluateLetExpr(expr *sql.LetExpr, params map[string]interface{}) (interface{}, error) {
	val, err := e.evaluateExpressionWithParams(expr.Value, params)
	if err != nil {
		return nil, err
	}

	// Store the value in params
	params[strings.ToLower(expr.Name)] = val
	return val, nil
}

// evaluateBlockExpr evaluates a BEGIN ... END block.
func (e *Executor) evaluateBlockExpr(expr *sql.BlockExpr, params map[string]interface{}) (interface{}, error) {
	var result interface{}
	var err error

	for _, subExpr := range expr.Expressions {
		result, err = e.evaluateExpressionWithParams(subExpr, params)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// evaluateCastExprWithParams evaluates a CAST expression with parameter support.
func (e *Executor) evaluateCastExprWithParams(expr *sql.CastExpr, params map[string]interface{}) (interface{}, error) {
	val, err := e.evaluateExpressionWithParams(expr.Expr, params)
	if err != nil {
		return nil, err
	}
	if expr.Type != nil {
		return e.castValue(val, expr.Type.Name)
	}
	return val, nil
}

// evaluateUDFFunctionCall evaluates a function call within a UDF body.
func (e *Executor) evaluateUDFFunctionCall(fc *sql.FunctionCall, params map[string]interface{}) (interface{}, error) {
	// Evaluate arguments
	args := make([]interface{}, len(fc.Args))
	for i, arg := range fc.Args {
		val, err := e.evaluateExpressionWithParams(arg, params)
		if err != nil {
			return nil, err
		}
		args[i] = val
	}

	funcName := strings.ToUpper(fc.Name)

	// Built-in functions
	switch funcName {
	case "UPPER", "UCASE":
		if len(args) == 0 || args[0] == nil {
			return nil, nil
		}
		return strings.ToUpper(fmt.Sprintf("%v", args[0])), nil
	case "LOWER", "LCASE":
		if len(args) == 0 || args[0] == nil {
			return nil, nil
		}
		return strings.ToLower(fmt.Sprintf("%v", args[0])), nil
	case "LENGTH", "OCTET_LENGTH":
		if len(args) == 0 || args[0] == nil {
			return nil, nil
		}
		switch v := args[0].(type) {
		case string:
			return int64(len(v)), nil
		case []byte:
			return int64(len(v)), nil
		default:
			return int64(len(fmt.Sprintf("%v", v))), nil
		}
	case "CONCAT":
		var result strings.Builder
		for _, arg := range args {
			if arg == nil {
				return nil, nil
			}
			result.WriteString(fmt.Sprintf("%v", arg))
		}
		return result.String(), nil
	case "NOW", "CURRENT_TIMESTAMP":
		return time.Now().Format("2006-01-02 15:04:05"), nil
	default:
		// Check for nested UDF
		if e.udfManager != nil {
			if udf, exists := e.udfManager.GetFunction(funcName); exists {
				// Convert args back to expressions for nested call
				exprArgs := make([]sql.Expression, len(fc.Args))
				copy(exprArgs, fc.Args)
				return e.evaluateUDF(udf, exprArgs, func(expr sql.Expression) (interface{}, error) {
					return e.evaluateExpressionWithParams(expr, params)
				})
			}
		}
		return nil, nil
	}
}

// compareValues compares two values with an operator.
func (e *Executor) compareValues(left interface{}, op sql.BinaryOp, right interface{}) (bool, error) {
	// Handle NULL comparisons
	if left == nil || right == nil {
		if op == sql.OpEq {
			return left == nil && right == nil, nil
		}
		if op == sql.OpNe {
			return !(left == nil && right == nil), nil
		}
		return false, nil
	}

	// Handle BLOB comparisons
	leftBytes, leftIsBytes := left.([]byte)
	rightBytes, rightIsBytes := right.([]byte)

	if leftIsBytes || rightIsBytes {
		// At least one is a byte slice
		// Try to convert string to bytes if needed
		if leftIsBytes && !rightIsBytes {
			rightStr := fmt.Sprintf("%v", right)
			// Check if it's a hex string (0x...)
			if len(rightStr) >= 2 && (rightStr[0:2] == "0x" || rightStr[0:2] == "0X") {
				blob, err := types.HexToBlob(rightStr)
				if err == nil {
					rightBytes = blob.Data
					rightIsBytes = true
				}
			} else {
				rightBytes = []byte(rightStr)
				rightIsBytes = true
			}
		} else if !leftIsBytes && rightIsBytes {
			leftStr := fmt.Sprintf("%v", left)
			if len(leftStr) >= 2 && (leftStr[0:2] == "0x" || leftStr[0:2] == "0X") {
				blob, err := types.HexToBlob(leftStr)
				if err == nil {
					leftBytes = blob.Data
					leftIsBytes = true
				}
			} else {
				leftBytes = []byte(leftStr)
				leftIsBytes = true
			}
		}

		if leftIsBytes && rightIsBytes {
			// Compare as byte slices
			switch op {
			case sql.OpEq:
				return e.bytesEqual(leftBytes, rightBytes), nil
			case sql.OpNe:
				return !e.bytesEqual(leftBytes, rightBytes), nil
			case sql.OpLt:
				return e.bytesCompare(leftBytes, rightBytes) < 0, nil
			case sql.OpLe:
				return e.bytesCompare(leftBytes, rightBytes) <= 0, nil
			case sql.OpGt:
				return e.bytesCompare(leftBytes, rightBytes) > 0, nil
			case sql.OpGe:
				return e.bytesCompare(leftBytes, rightBytes) >= 0, nil
			}
		}
	}

	// Convert to comparable values
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)

	switch op {
	case sql.OpEq:
		return leftStr == rightStr, nil
	case sql.OpNe:
		return leftStr != rightStr, nil
	case sql.OpLt:
		return leftStr < rightStr, nil
	case sql.OpLe:
		return leftStr <= rightStr, nil
	case sql.OpGt:
		return leftStr > rightStr, nil
	case sql.OpGe:
		return leftStr >= rightStr, nil
	case sql.OpLike:
		// Simple LIKE implementation
		pattern := strings.ReplaceAll(rightStr, "%", ".*")
		pattern = strings.ReplaceAll(pattern, "_", ".")
		pattern = "^" + pattern + "$"
		matched, _ := regexp.MatchString(pattern, leftStr)
		return matched, nil
	default:
		return false, nil
	}
}

// bytesEqual compares two byte slices for equality.
func (e *Executor) bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// bytesCompare compares two byte slices (like strings.Compare).
func (e *Executor) bytesCompare(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// expressionToValue converts an expression to a storage value.
func (e *Executor) expressionToValue(expr sql.Expression, col *types.ColumnInfo) (types.Value, error) {
	switch ex := expr.(type) {
	case *sql.Literal:
		if ex.Type == sql.LiteralNull {
			return types.Value{Null: true}, nil
		}
		if ex.Type == sql.LiteralNumber {
			// Check target column type first
			if col != nil && col.Type == types.TypeBlob {
				// Convert integer to BLOB - store as big-endian bytes
				var i int64
				switch v := ex.Value.(type) {
				case int:
					i = int64(v)
				case int64:
					i = v
				case float64:
					i = int64(v)
				}
				// Convert to variable-length big-endian bytes
				data := make([]byte, 0)
				for i > 0 {
					data = append([]byte{byte(i & 0xff)}, data...)
					i >>= 8
				}
				if len(data) == 0 {
					data = []byte{0}
				}
				return types.NewBlobValue(data), nil
			}
			if col != nil && col.Type == types.TypeDecimal {
				// Store as DECIMAL
				return types.NewDecimalFromString(fmt.Sprintf("%v", ex.Value))
			}
			if col != nil && col.Type == types.TypeFloat {
				// Store as float for FLOAT columns
				if f, err := strconv.ParseFloat(fmt.Sprintf("%v", ex.Value), 64); err == nil {
					return types.NewFloatValue(f), nil
				}
			}
			// Try to parse as int first
			if i, err := strconv.ParseInt(fmt.Sprintf("%v", ex.Value), 10, 64); err == nil {
				return types.NewIntValue(i), nil
			}
			// Try as float
			if f, err := strconv.ParseFloat(fmt.Sprintf("%v", ex.Value), 64); err == nil {
				return types.NewFloatValue(f), nil
			}
		}
		if ex.Type == sql.LiteralString {
			// Check if target column is DECIMAL
			if col != nil && col.Type == types.TypeDecimal {
				return types.NewDecimalFromString(fmt.Sprintf("%v", ex.Value))
			}
			// Check if target column is BLOB
			if col != nil && col.Type == types.TypeBlob {
				// Try to parse as hex string if it starts with 0x or X'...'
				strVal := fmt.Sprintf("%v", ex.Value)
				if len(strVal) >= 2 && (strVal[0:2] == "0x" || strVal[0:2] == "0X") {
					return types.HexToBlob(strVal)
				}
				if len(strVal) >= 3 && (strVal[0] == 'x' || strVal[0] == 'X') && strVal[1] == '\'' {
					return types.HexToBlob(strVal)
				}
				// Otherwise store as raw bytes
				return types.NewBlobValue([]byte(strVal)), nil
			}
			return types.NewStringValue(fmt.Sprintf("%v", ex.Value), col.Type), nil
		}
		if ex.Type == sql.LiteralBool {
			return types.NewBoolValue(ex.Value.(bool)), nil
		}

	case *sql.UnaryExpr:
		if ex.Op == sql.OpNeg {
			// Handle negative numbers
			rightVal, err := e.expressionToValue(ex.Right, col)
			if err != nil {
				return types.Value{Null: true}, err
			}

			// Negate the value
			switch rightVal.Type {
			case types.TypeInt:
				return types.NewIntValue(-rightVal.AsInt()), nil
			case types.TypeFloat:
				return types.NewFloatValue(-rightVal.AsFloat()), nil
			case types.TypeDecimal:
				unscaled, scale := rightVal.AsDecimal()
				return types.NewDecimalValue(-unscaled, scale), nil
			}
		}

	case *sql.ColumnRef:
		// Column reference in VALUES clause - this would be for INSERT ... SELECT
		return types.Value{Null: true}, nil
	}

	return types.Value{Null: true}, nil
}

// valueToInterface converts a storage value to an interface{}.
func (e *Executor) valueToInterface(v types.Value) interface{} {
	if v.Null {
		return nil
	}
	switch v.Type {
	case types.TypeInt, types.TypeSeq:
		return v.AsInt()
	case types.TypeFloat:
		return v.AsFloat()
	case types.TypeDecimal:
		return v.AsDecimalString()
	case types.TypeBool:
		return v.AsBool()
	case types.TypeChar, types.TypeVarchar, types.TypeText:
		return v.AsString()
	case types.TypeBlob:
		return v.AsBytes()
	default:
		return v.AsString()
	}
}

// valueExistsInColumn checks if a value already exists in a column (for UNIQUE constraint).
func (e *Executor) valueExistsInColumn(tbl *table.Table, colIdx int, value types.Value) bool {
	// Get the index manager
	idxMgr := tbl.GetIndexManager()
	if idxMgr == nil {
		return false
	}

	// Get column info
	cols := tbl.Columns()
	if colIdx >= len(cols) {
		return false
	}
	col := cols[colIdx]

	// Check if there's a unique index on this column
	idx, err := idxMgr.GetIndex(col.Name)
	if err == nil && idx != nil {
		_, found := idx.Search(value)
		return found
	}

	// Fallback: scan all rows (slower)
	rows, err := tbl.Scan()
	if err != nil {
		return false
	}

	for _, r := range rows {
		if colIdx < len(r.Values) {
			if !r.Values[colIdx].Null && r.Values[colIdx].Compare(value) == 0 {
				return true
			}
		}
	}

	return false
}

// validateCheckConstraints validates CHECK constraints for a row.
func (e *Executor) validateCheckConstraints(tbl *table.Table, values []types.Value) error {
	checks := tbl.GetCheckConstraints()
	if len(checks) == 0 {
		return nil
	}

	cols := tbl.Columns()
	colMap := make(map[string]int)
	for i, col := range cols {
		colMap[strings.ToLower(col.Name)] = i
	}

	for _, check := range checks {
		// Parse the check expression using Parse function
		exprStr := check.Expression
		stmt, err := sql.Parse("SELECT " + exprStr)
		if err != nil {
			// If parsing fails, try direct expression
			continue
		}

		selectStmt, ok := stmt.(*sql.SelectStmt)
		if !ok || len(selectStmt.Columns) == 0 {
			continue
		}

		expr := selectStmt.Columns[0]

		// Evaluate the expression
		result, err := e.evaluateCheckExpression(expr, values, cols, colMap)
		if err != nil {
			continue // Skip on error
		}

		// Check constraint violated if result is false
		if !result {
			name := check.Name
			if name == "" {
				name = "unnamed"
			}
			return fmt.Errorf("check constraint '%s' violated", name)
		}
	}

	return nil
}

// evaluateCheckExpression evaluates a CHECK expression against row values.
func (e *Executor) evaluateCheckExpression(expr sql.Expression, values []types.Value, cols []*types.ColumnInfo, colMap map[string]int) (bool, error) {
	switch ex := expr.(type) {
	case *sql.BinaryExpr:
		// Handle comparison operators specially
		switch ex.Op {
		case sql.OpEq, sql.OpNe, sql.OpLt, sql.OpLe, sql.OpGt, sql.OpGe:
			return e.compareCheckValues(ex.Left, ex.Right, values, colMap, ex.Op)
		}

		// Handle logical operators
		left, err := e.evaluateCheckExpression(ex.Left, values, cols, colMap)
		if err != nil {
			return false, err
		}

		// Short-circuit for AND/OR
		if ex.Op == sql.OpAnd {
			if !left {
				return false, nil
			}
			return e.evaluateCheckExpression(ex.Right, values, cols, colMap)
		}
		if ex.Op == sql.OpOr {
			if left {
				return true, nil
			}
			return e.evaluateCheckExpression(ex.Right, values, cols, colMap)
		}

		return left, nil

	case *sql.Literal:
		if ex.Type == sql.LiteralBool {
			return ex.Value.(bool), nil
		}
		return true, nil

	case *sql.ColumnRef:
		idx, ok := colMap[strings.ToLower(ex.Name)]
		if !ok || idx >= len(values) {
			return false, fmt.Errorf("unknown column: %s", ex.Name)
		}
		// Return true if value is non-null
		return !values[idx].Null, nil

	case *sql.IsNullExpr:
		colRef, ok := ex.Expr.(*sql.ColumnRef)
		if !ok {
			return false, nil
		}
		idx, ok := colMap[strings.ToLower(colRef.Name)]
		if !ok {
			return false, nil
		}
		isNull := idx >= len(values) || values[idx].Null
		if ex.Not {
			return !isNull, nil
		}
		return isNull, nil

	case *sql.ParenExpr:
		// Unwrap parenthesized expressions
		return e.evaluateCheckExpression(ex.Expr, values, cols, colMap)
	}

	return true, nil
}

// compareCheckValues compares two values in a CHECK expression.
func (e *Executor) compareCheckValues(left, right sql.Expression, values []types.Value, colMap map[string]int, op sql.BinaryOp) (bool, error) {
	leftVal, err := e.getCheckValue(left, values, colMap)
	if err != nil {
		return false, err
	}
	rightVal, err := e.getCheckValue(right, values, colMap)
	if err != nil {
		return false, err
	}

	if leftVal == nil || rightVal == nil {
		return false, nil
	}

	// Compare based on type
	switch lv := leftVal.(type) {
	case int64:
		rv, ok := rightVal.(int64)
		if !ok {
			return false, nil
		}
		switch op {
		case sql.OpLt:
			return lv < rv, nil
		case sql.OpLe:
			return lv <= rv, nil
		case sql.OpGt:
			return lv > rv, nil
		case sql.OpGe:
			return lv >= rv, nil
		}
	case float64:
		rv, ok := rightVal.(float64)
		if !ok {
			// Try int to float
			if riv, ok := rightVal.(int64); ok {
				rv = float64(riv)
			} else {
				return false, nil
			}
		}
		switch op {
		case sql.OpLt:
			return lv < rv, nil
		case sql.OpLe:
			return lv <= rv, nil
		case sql.OpGt:
			return lv > rv, nil
		case sql.OpGe:
			return lv >= rv, nil
		}
	}

	return false, nil
}

// getCheckValue gets a value from an expression for CHECK comparison.
func (e *Executor) getCheckValue(expr sql.Expression, values []types.Value, colMap map[string]int) (interface{}, error) {
	switch ex := expr.(type) {
	case *sql.Literal:
		if ex.Type == sql.LiteralNumber {
			if i, err := strconv.ParseInt(fmt.Sprintf("%v", ex.Value), 10, 64); err == nil {
				return i, nil
			}
			if f, err := strconv.ParseFloat(fmt.Sprintf("%v", ex.Value), 64); err == nil {
				return f, nil
			}
		}
		return ex.Value, nil
	case *sql.ColumnRef:
		idx, ok := colMap[strings.ToLower(ex.Name)]
		if !ok || idx >= len(values) {
			return nil, fmt.Errorf("unknown column: %s", ex.Name)
		}
		v := values[idx]
		if v.Null {
			return nil, nil
		}
		switch v.Type {
		case types.TypeInt, types.TypeSeq:
			return v.AsInt(), nil
		case types.TypeFloat:
			return v.AsFloat(), nil
		case types.TypeChar, types.TypeVarchar, types.TypeText:
			return v.AsString(), nil
		case types.TypeBool:
			return v.AsBool(), nil
		}
	}
	return nil, fmt.Errorf("cannot get value")
}

// validateForeignKeys validates FOREIGN KEY constraints.
func (e *Executor) validateForeignKeys(tbl *table.Table, values []types.Value, isUpdate bool) error {
	fks := tbl.GetForeignKeys()
	if len(fks) == 0 {
		return nil
	}

	cols := tbl.Columns()
	colMap := make(map[string]int)
	for i, col := range cols {
		colMap[strings.ToLower(col.Name)] = i
	}

	for _, fk := range fks {
		// Check if referenced table exists
		if !e.engine.TableExists(fk.RefTable) {
			return fmt.Errorf("foreign key constraint fails: referenced table '%s' does not exist", fk.RefTable)
		}

		refTbl, err := e.engine.GetTable(fk.RefTable)
		if err != nil {
			return err
		}

		// Get the value for the FK column
		if len(fk.Columns) == 0 {
			continue
		}

		colIdx, ok := colMap[strings.ToLower(fk.Columns[0])]
		if !ok {
			continue
		}

		if colIdx >= len(values) {
			continue
		}

		fkValue := values[colIdx]

		// NULL values don't need FK validation
		if fkValue.Null {
			continue
		}

		// Check if referenced value exists
		refColIdx := -1
		refCols := refTbl.Columns()
		for i, c := range refCols {
			if strings.EqualFold(c.Name, fk.RefColumns[0]) {
				refColIdx = i
				break
			}
		}

		if refColIdx < 0 {
			return fmt.Errorf("foreign key constraint fails: referenced column '%s' not found", fk.RefColumns[0])
		}

		// Search for the referenced value
		rows, err := refTbl.Scan()
		if err != nil {
			return err
		}

		found := false
		for _, r := range rows {
			if refColIdx < len(r.Values) {
				if !r.Values[refColIdx].Null && r.Values[refColIdx].Compare(fkValue) == 0 {
					found = true
					break
				}
			}
		}

		if !found {
			return fmt.Errorf("foreign key constraint fails: key '%s' not found in table '%s'",
				fkValue.String(), fk.RefTable)
		}
	}

	return nil
}

// ============================================================================
// Auth Statement Execution
// ============================================================================

// executeCreateUser executes a CREATE USER statement.
func (e *Executor) executeCreateUser(stmt *sql.CreateUserStmt) (*Result, error) {
	if e.authMgr == nil {
		return nil, fmt.Errorf("auth manager not configured")
	}

	// Determine role
	role := 1 // default to user
	if strings.ToLower(stmt.Role) == "admin" {
		role = 0
	}

	_, err := e.authMgr.CreateUser(stmt.Username, stmt.Identified, role)
	if err != nil {
		return nil, err
	}

	return &Result{Message: "Query OK, 0 rows affected"}, nil
}

// executeDropUser executes a DROP USER statement.
func (e *Executor) executeDropUser(stmt *sql.DropUserStmt) (*Result, error) {
	if e.authMgr == nil {
		return nil, fmt.Errorf("auth manager not configured")
	}

	err := e.authMgr.DeleteUser(stmt.Username)
	if err != nil {
		return nil, err
	}

	return &Result{Message: "Query OK, 0 rows affected"}, nil
}

// executeAlterUser executes an ALTER USER statement.
func (e *Executor) executeAlterUser(stmt *sql.AlterUserStmt) (*Result, error) {
	if e.authMgr == nil {
		return nil, fmt.Errorf("auth manager not configured")
	}

	// ALTER USER mainly supports password changes
	if stmt.Identified != "" {
		err := e.authMgr.ChangePassword(stmt.Username, "", stmt.Identified)
		if err != nil {
			return nil, err
		}
	}

	return &Result{Message: "Query OK, 0 rows affected"}, nil
}

// executeSetPassword executes a SET PASSWORD statement.
func (e *Executor) executeSetPassword(stmt *sql.SetPasswordStmt) (*Result, error) {
	if e.authMgr == nil {
		return nil, fmt.Errorf("auth manager not configured")
	}

	username := stmt.ForUser
	if username == "" {
		// Set password for current user
		return nil, fmt.Errorf("SET PASSWORD requires FOR clause or session context")
	}

	err := e.authMgr.ChangePassword(username, "", stmt.Password)
	if err != nil {
		return nil, err
	}

	return &Result{Message: "Query OK, 0 rows affected"}, nil
}

// executeGrant executes a GRANT statement.
func (e *Executor) executeGrant(stmt *sql.GrantStmt) (*Result, error) {
	if e.authMgr == nil {
		return nil, fmt.Errorf("auth manager not configured")
	}

	// Convert privileges
	priv := convertPrivileges(stmt.Privileges)

	switch stmt.On {
	case sql.GrantOnAll:
		// Global grant
		globalPriv := &GlobalPrivilege{
			Select: priv.Select,
			Insert: priv.Insert,
			Update: priv.Update,
			Delete: priv.Delete,
			Create: priv.Create,
			Drop:   priv.Drop,
			Index:  priv.Index,
			Grant:  stmt.WithGrant,
		}
		if err := e.authMgr.GrantGlobal(stmt.To, globalPriv); err != nil {
			return nil, err
		}
	case sql.GrantOnDatabase:
		// Database grant
		dbPriv := &DatabasePrivilege{
			Select: priv.Select,
			Insert: priv.Insert,
			Update: priv.Update,
			Delete: priv.Delete,
			Create: priv.Create,
			Drop:   priv.Drop,
			Index:  priv.Index,
		}
		if err := e.authMgr.GrantDatabase(stmt.To, stmt.Database, dbPriv); err != nil {
			return nil, err
		}
	case sql.GrantOnTable:
		// Table grant
		tblPriv := &TablePrivilege{
			Select: priv.Select,
			Insert: priv.Insert,
			Update: priv.Update,
			Delete: priv.Delete,
			Create: priv.Create,
			Drop:   priv.Drop,
			Index:  priv.Index,
		}
		db := stmt.Database
		if db == "" {
			db = e.database
		}
		if err := e.authMgr.GrantTable(stmt.To, db, stmt.Table, tblPriv); err != nil {
			return nil, err
		}
	}

	return &Result{Message: "Query OK, 0 rows affected"}, nil
}

// executeRevoke executes a REVOKE statement.
func (e *Executor) executeRevoke(stmt *sql.RevokeStmt) (*Result, error) {
	if e.authMgr == nil {
		return nil, fmt.Errorf("auth manager not configured")
	}

	// Convert privileges
	priv := convertPrivileges(stmt.Privileges)

	switch stmt.On {
	case sql.GrantOnAll:
		// Global revoke
		globalPriv := &GlobalPrivilege{
			Select: priv.Select,
			Insert: priv.Insert,
			Update: priv.Update,
			Delete: priv.Delete,
			Create: priv.Create,
			Drop:   priv.Drop,
			Index:  priv.Index,
		}
		if err := e.authMgr.RevokeGlobal(stmt.From, globalPriv); err != nil {
			return nil, err
		}
	case sql.GrantOnDatabase:
		// Database revoke
		dbPriv := &DatabasePrivilege{
			Select: priv.Select,
			Insert: priv.Insert,
			Update: priv.Update,
			Delete: priv.Delete,
			Create: priv.Create,
			Drop:   priv.Drop,
			Index:  priv.Index,
		}
		if err := e.authMgr.RevokeDatabase(stmt.From, stmt.Database, dbPriv); err != nil {
			return nil, err
		}
	case sql.GrantOnTable:
		// Table revoke
		tblPriv := &TablePrivilege{
			Select: priv.Select,
			Insert: priv.Insert,
			Update: priv.Update,
			Delete: priv.Delete,
			Create: priv.Create,
			Drop:   priv.Drop,
			Index:  priv.Index,
		}
		db := stmt.Database
		if db == "" {
			db = e.database
		}
		if err := e.authMgr.RevokeTable(stmt.From, db, stmt.Table, tblPriv); err != nil {
			return nil, err
		}
	}

	return &Result{Message: "Query OK, 0 rows affected"}, nil
}

// executeShowGrants executes a SHOW GRANTS statement.
func (e *Executor) executeShowGrants(stmt *sql.ShowGrantsStmt) (*Result, error) {
	if e.authMgr == nil {
		return nil, fmt.Errorf("auth manager not configured")
	}

	username := stmt.ForUser
	if username == "" {
		// Show grants for current user - requires session context
		return nil, fmt.Errorf("SHOW GRANTS requires FOR clause or session context")
	}

	grants, err := e.authMgr.GetGrants(username)
	if err != nil {
		return nil, err
	}

	result := &Result{
		Columns: []ColumnInfo{
			{Name: "Grants for " + username + "@%", Type: "VARCHAR"},
		},
		Rows: make([][]interface{}, len(grants)),
	}

	for i, g := range grants {
		result.Rows[i] = []interface{}{g}
	}
	result.RowCount = len(grants)

	return result, nil
}

// privilegeSet represents a set of privileges.
type privilegeSet struct {
	Select bool
	Insert bool
	Update bool
	Delete bool
	Create bool
	Drop   bool
	Index  bool
	Alter  bool
}

// convertPrivileges converts AST privileges to privilege set.
func convertPrivileges(privs []*sql.Privilege) *privilegeSet {
	ps := &privilegeSet{}
	for _, p := range privs {
		switch p.Type {
		case sql.PrivAll:
			ps.Select = true
			ps.Insert = true
			ps.Update = true
			ps.Delete = true
			ps.Create = true
			ps.Drop = true
			ps.Index = true
			ps.Alter = true
		case sql.PrivSelect:
			ps.Select = true
		case sql.PrivInsert:
			ps.Insert = true
		case sql.PrivUpdate:
			ps.Update = true
		case sql.PrivDelete:
			ps.Delete = true
		case sql.PrivCreate:
			ps.Create = true
		case sql.PrivDrop:
			ps.Drop = true
		case sql.PrivIndex:
			ps.Index = true
		case sql.PrivAlter:
			ps.Alter = true
		}
	}
	return ps
}

// GlobalPrivilege represents global privileges for executor.
type GlobalPrivilege struct {
	Select bool
	Insert bool
	Update bool
	Delete bool
	Create bool
	Drop   bool
	Index  bool
	Grant  bool
}

// DatabasePrivilege represents database privileges for executor.
type DatabasePrivilege struct {
	Select bool
	Insert bool
	Update bool
	Delete bool
	Create bool
	Drop   bool
	Index  bool
}

// TablePrivilege represents table privileges for executor.
type TablePrivilege struct {
	Select bool
	Insert bool
	Update bool
	Delete bool
	Create bool
	Drop   bool
	Index  bool
}
