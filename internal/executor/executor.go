// Package executor provides SQL query execution for XxSql.
package executor

import (
	"bufio"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/topxeq/xxsql/internal/backup"
	"github.com/topxeq/xxsql/internal/optimizer"
	"github.com/topxeq/xxsql/internal/sql"
	"github.com/topxeq/xxsql/internal/storage"
	"github.com/topxeq/xxsql/internal/storage/btree"
	"github.com/topxeq/xxsql/internal/storage/catalog"
	"github.com/topxeq/xxsql/internal/storage/fts"
	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/table"
	"github.com/topxeq/xxsql/internal/storage/types"
	"github.com/topxeq/xxsql/internal/xxscript"
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
	udfManager    *UDFManager          // Old style UDF (SQL expressions)
	scriptUDFMgr  *ScriptUDFManager    // New style UDF (XxScript)
	outerContext  map[string]interface{} // For correlated subqueries
	currentTable  string                 // Current table being queried (for outer context)
	subqueryCache map[string]interface{} // Cache for non-correlated subquery results (optimization)
	cteResults    map[string]*Result     // CTE results for WITH clause support
	lastInsertID  int64                  // Last auto-generated insert ID
	lastRowCount  int64                  // Number of rows affected by last operation
	ftsManager    *fts.FTSManager        // Full-text search manager
	pragmaSettings map[string]interface{} // PRAGMA settings
	optimizer     *optimizer.Optimizer   // Query optimizer

	// Transaction state
	inTransaction bool
	txMode        string   // "DEFERRED", "IMMEDIATE", or "EXCLUSIVE"
	savepoints    []string // Stack of savepoint names
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
	var ftsMgr *fts.FTSManager
	if engine != nil {
		// Use the engine's shared FTS manager
		ftsMgr = engine.GetFTSManager()
	}
	return &Executor{
		engine:        engine,
		subqueryCache: make(map[string]interface{}),
		ftsManager:    ftsMgr,
	}
}

// SetUDFManager sets the UDF manager for the executor.
func (e *Executor) SetUDFManager(m *UDFManager) {
	e.udfManager = m
}

// SetScriptUDFManager sets the script-based UDF manager for the executor.
func (e *Executor) SetScriptUDFManager(m *ScriptUDFManager) {
	e.scriptUDFMgr = m
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

// ExecuteForScript executes a SQL statement and returns the result as interface{}.
// This method satisfies the xxscript.SQLExecutor interface.
func (e *Executor) ExecuteForScript(sqlStr string) (interface{}, error) {
	return e.Execute(sqlStr)
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
	case *sql.CreateViewStmt:
		if err := checkPerm(PermCreateTable); err != nil {
			return nil, err
		}
		return e.executeCreateView(s)
	case *sql.DropViewStmt:
		if err := checkPerm(PermDropTable); err != nil {
			return nil, err
		}
		return e.executeDropView(s)
	case *sql.ExplainStmt:
		return e.executeExplain(s)
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
	case *sql.VacuumStmt:
		return e.executeVacuum(s)
	case *sql.PragmaStmt:
		return e.executePragma(s)
	case *sql.AnalyzeStmt:
		return e.executeAnalyze(s)
	case *sql.CreateFunctionStmt:
		return e.executeCreateFunction(s)
	case *sql.DropFunctionStmt:
		return e.executeDropFunction(s)
	case *sql.CreateTriggerStmt:
		if err := checkPerm(PermCreateTable); err != nil {
			return nil, err
		}
		return e.executeCreateTrigger(s)
	case *sql.DropTriggerStmt:
		if err := checkPerm(PermDropTable); err != nil {
			return nil, err
		}
		return e.executeDropTrigger(s)
	case *sql.WithStmt:
		if err := checkPerm(PermSelect); err != nil {
			return nil, err
		}
		return e.executeWith(s)
	case *sql.BeginStmt:
		return e.executeBegin(s)
	case *sql.CommitStmt:
		return e.executeCommit(s)
	case *sql.RollbackStmt:
		return e.executeRollback(s)
	case *sql.SavepointStmt:
		return e.executeSavepoint(s)
	case *sql.ReleaseSavepointStmt:
		return e.executeReleaseSavepoint(s)
	case *sql.CopyStmt:
		if err := checkPerm(PermInsert); err != nil {
			return nil, err
		}
		return e.executeCopy(s)
	case *sql.LoadDataStmt:
		if err := checkPerm(PermInsert); err != nil {
			return nil, err
		}
		return e.executeLoadData(s)
	case *sql.CreateFTSStmt:
		if err := checkPerm(PermCreateIndex); err != nil {
			return nil, err
		}
		return e.executeCreateFTS(s)
	case *sql.DropFTSStmt:
		if err := checkPerm(PermDropIndex); err != nil {
			return nil, err
		}
		return e.executeDropFTS(s)
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
		// Check for LATERAL
		if stmt.From.Table.Lateral {
			return e.executeSelectFromLateral(stmt)
		}
		return e.executeSelectFromDerivedTable(stmt)
	}

	// Check if this is a VALUES table constructor
	if stmt.From.Table.Values != nil {
		return e.executeSelectFromValues(stmt)
	}

	tableName := stmt.From.Table.Name
	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	// Check if this is a CTE reference
	if cteResult, ok := e.cteResults[strings.ToLower(tableName)]; ok {
		return e.executeSelectFromCTE(stmt, cteResult)
	}

	// Check if this is a view reference
	if e.engine.ViewExists(tableName) {
		return e.executeSelectFromView(stmt, tableName)
	}

	// Set current table for correlated subqueries
	oldTable := e.currentTable
	e.currentTable = strings.ToLower(tableName)
	defer func() { e.currentTable = oldTable }()

	// Get the table
	tbl, _, err := e.engine.GetTableOrTemp(tableName)
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

	// Try to use index scan if possible
	var rows []*row.Row
	var usedIndex bool
	if stmt.Where != nil {
		rows, usedIndex, err = e.tryIndexScan(tableName, stmt.Where, tbl)
		if err != nil {
			// Index scan failed, fall back to table scan
			usedIndex = false
		}
	}

	// Fall back to full table scan if index wasn't used
	if !usedIndex {
		rows, err = e.engine.Scan(tableName)
		if err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
	}

	// Determine result columns
	var resultCols []ColumnInfo
	var colIndices []int
	var funcExprs []sql.Expression // Non-aggregate function expressions
	var aggregateFuncs []struct {
		name   string
		arg    string // column name for the aggregate argument
		index  int    // result column index
		filter sql.Expression // FILTER (WHERE ...) clause
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
			isAggregate := funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" || funcName == "GROUP_CONCAT" || funcName == "STRING_AGG" || funcName == "STDDEV" || funcName == "STDDEV_SAMP" || funcName == "VARIANCE" || funcName == "VAR_SAMP"

			// Determine result type
			resultType := "VARCHAR"
			if isAggregate {
				resultType = "INT"
				if funcName == "AVG" || funcName == "STDDEV" || funcName == "STDDEV_SAMP" || funcName == "VARIANCE" || funcName == "VAR_SAMP" {
					resultType = "FLOAT"
				} else if funcName == "GROUP_CONCAT" || funcName == "STRING_AGG" {
					resultType = "VARCHAR"
				}
			}

			resultCols = append(resultCols, ColumnInfo{
				Name: expr.Name + "()",
				Type: resultType,
			})
			if isAggregate {
				aggregateFuncs = append(aggregateFuncs, struct {
					name   string
					arg    string
					index  int
					filter sql.Expression
				}{funcName, colName, len(resultCols) - 1, expr.Filter})
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
		case *sql.WindowFuncCall:
			// Window function - will be evaluated after partitioning
			colIndices = append(colIndices, -1)
			funcExprs = append(funcExprs, expr)
			funcName := strings.ToUpper(expr.Func.Name)
			colName := expr.Func.Name + "()"
			if expr.Alias != "" {
				colName = expr.Alias
			}
			resultType := "INT"
			if funcName == "AVG" {
				resultType = "FLOAT"
			} else if funcName == "ROW_NUMBER" || funcName == "RANK" || funcName == "DENSE_RANK" {
				resultType = "INT"
			}
			resultCols = append(resultCols, ColumnInfo{
				Name: colName,
				Type: resultType,
			})
		}
	}

	// Check for window functions
	hasWindowFuncs := false
	for _, expr := range funcExprs {
		if _, ok := expr.(*sql.WindowFuncCall); ok {
			hasWindowFuncs = true
			break
		}
	}

	// If there are aggregate functions, compute aggregates
	if len(aggregateFuncs) > 0 || len(stmt.GroupBy) > 0 {
		// Handle GROUP BY with aggregates
		return e.executeGroupBy(stmt, rows, resultCols, colIndices, funcExprs, aggregateFuncs, tblInfo, columnMap, columnOrder)
	}

	// If there are window functions, handle them
	if hasWindowFuncs {
		return e.executeWindowFunctions(stmt, rows, resultCols, colIndices, funcExprs, tblInfo, columnMap, columnOrder)
	}

	// Build result rows
	result := &Result{
		Columns: resultCols,
		Rows:    make([][]interface{}, 0),
	}

	for _, r := range rows {
		// Evaluate virtual generated columns first
		if err := e.evaluateGeneratedColumns(r, columnOrder); err != nil {
			return nil, err
		}

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
		keyType    sortKeyType
		index      int
		expr       sql.Expression
		ascending  bool
		nullsFirst bool
		nullsLast  bool
		collation  string
	}
	sortKeys := make([]sortKeyData, 0, len(orderBy))

	for _, item := range orderBy {
		switch expr := item.Expr.(type) {
		case *sql.ColumnRef:
			colName := strings.ToLower(expr.Name)
			idx, ok := colIndexMap[colName]
			if ok {
				sortKeys = append(sortKeys, sortKeyData{
					keyType:    sortKeyIndex,
					index:      idx,
					ascending:  item.Ascending,
					nullsFirst: item.NullsFirst,
					nullsLast:  item.NullsLast,
					collation:  item.Collate,
				})
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
					sortKeys = append(sortKeys, sortKeyData{
						keyType:    sortKeyIndex,
						index:      idx,
						ascending:  item.Ascending,
						nullsFirst: item.NullsFirst,
						nullsLast:  item.NullsLast,
						collation:  item.Collate,
					})
				}
			}
		case *sql.BinaryExpr, *sql.UnaryExpr:
			// Handle expression-based ORDER BY (e.g., ORDER BY amount*2)
			sortKeys = append(sortKeys, sortKeyData{
				keyType:    sortKeyExpr,
				expr:       expr,
				ascending:  item.Ascending,
				nullsFirst: item.NullsFirst,
				nullsLast:  item.NullsLast,
				collation:  item.Collate,
			})
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

			// Handle NULL ordering with NULLS FIRST/LAST
			viIsNull := vi == nil
			vjIsNull := vj == nil

			if viIsNull && vjIsNull {
				continue // Both NULL, check next key
			}
			if viIsNull || vjIsNull {
				// One is NULL, handle based on NULLS FIRST/LAST
				if key.nullsFirst {
					// NULLS FIRST: NULL comes before non-NULL
					if viIsNull {
						return true
					}
					return false
				} else if key.nullsLast {
					// NULLS LAST: NULL comes after non-NULL
					if viIsNull {
						return false
					}
					return true
				}
				// Default: NULL sorts first (ascending) or last (descending)
				if key.ascending {
					return viIsNull // NULL first for ASC
				}
				return vjIsNull // NULL last for DESC (non-NULL first)
			}

			// Both non-NULL, use normal comparison
			var cmp int
			if key.collation != "" {
				// Use collation-aware comparison
				cmp = compareValuesWithCollation(vi, vj, key.collation)
			} else {
				cmp = compareValues(vi, vj)
			}
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
	case sql.OpConcat:
		// String concatenation
		leftStr := fmt.Sprintf("%v", left)
		rightStr := fmt.Sprintf("%v", right)
		return leftStr + rightStr, nil
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

// executeWindowFunctions handles window function execution.
func (e *Executor) executeWindowFunctions(stmt *sql.SelectStmt, rows []*row.Row, resultCols []ColumnInfo, colIndices []int, funcExprs []sql.Expression, tblInfo *table.TableInfo, columnMap map[string]*types.ColumnInfo, columnOrder []*types.ColumnInfo) (*Result, error) {
	// Filter rows by WHERE clause first
	filteredRows := make([]*row.Row, 0)
	for _, r := range rows {
		if stmt.Where != nil {
			match, err := e.evaluateWhere(stmt.Where, r, columnMap, columnOrder)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		filteredRows = append(filteredRows, r)
	}

	// Find window function expressions
	var windowFuncs []struct {
		index int
		expr  *sql.WindowFuncCall
	}
	for i, expr := range funcExprs {
		if wf, ok := expr.(*sql.WindowFuncCall); ok {
			windowFuncs = append(windowFuncs, struct {
				index int
				expr  *sql.WindowFuncCall
			}{i, wf})
		}
	}

	// Compute window function values
	windowValues := make([]map[int]interface{}, len(filteredRows))
	for i := range filteredRows {
		windowValues[i] = make(map[int]interface{})
	}

	// Build column index map for ORDER BY in window functions
	colIdxMap := make(map[string]int)
	for i, col := range columnOrder {
		colIdxMap[strings.ToLower(col.Name)] = i
	}

	// Process each window function
	for _, wf := range windowFuncs {
		// Group rows by partition
		partitions := e.partitionRows(filteredRows, wf.expr.Window.PartitionBy, columnMap, columnOrder)

		// Process each partition
		for _, partition := range partitions {
			// Sort partition by ORDER BY if present
			if len(wf.expr.Window.OrderBy) > 0 {
				e.sortRowsForWindowPartition(partition, filteredRows, wf.expr.Window.OrderBy, colIdxMap)
			}

			// Compute window function values for each row in partition
			e.computeWindowFunction(wf.expr, partition, filteredRows, windowValues, wf.index, columnMap, columnOrder, colIdxMap)
		}
	}

	// Build result rows
	result := &Result{
		Columns: resultCols,
		Rows:    make([][]interface{}, 0),
	}

	for rowIdx, r := range filteredRows {
		resultRow := make([]interface{}, len(colIndices))
		for i, idx := range colIndices {
			if funcExprs[i] != nil {
				// Check if it's a window function
				if _, ok := funcExprs[i].(*sql.WindowFuncCall); ok {
					resultRow[i] = windowValues[rowIdx][i]
				} else {
					// Evaluate regular expression
					val, err := e.evaluateExpression(funcExprs[i], r, columnMap, columnOrder)
					if err != nil {
						return nil, err
					}
					resultRow[i] = val
				}
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

// partitionRows groups rows by PARTITION BY expressions, returning indices.
func (e *Executor) partitionRows(rows []*row.Row, partitionBy []sql.Expression, columnMap map[string]*types.ColumnInfo, columnOrder []*types.ColumnInfo) [][]int {
	if len(partitionBy) == 0 {
		// No partition - all rows are in one partition
		indices := make([]int, len(rows))
		for i := range rows {
			indices[i] = i
		}
		return [][]int{indices}
	}

	// Group by partition key
	partitions := make(map[string][]int)
	for i, r := range rows {
		key := e.getPartitionKey(r, partitionBy, columnMap, columnOrder)
		partitions[key] = append(partitions[key], i)
	}

	result := make([][]int, 0, len(partitions))
	for _, indices := range partitions {
		result = append(result, indices)
	}
	return result
}

// getPartitionKey computes a string key for partitioning.
func (e *Executor) getPartitionKey(r *row.Row, partitionBy []sql.Expression, columnMap map[string]*types.ColumnInfo, columnOrder []*types.ColumnInfo) string {
	var parts []string
	for _, expr := range partitionBy {
		val, err := e.evaluateExpression(expr, r, columnMap, columnOrder)
		if err != nil {
			parts = append(parts, "NULL")
		} else {
			parts = append(parts, fmt.Sprintf("%v", val))
		}
	}
	return strings.Join(parts, "|")
}

// sortRowsForWindowPartition sorts row indices within a partition by ORDER BY.
func (e *Executor) sortRowsForWindowPartition(indices []int, rows []*row.Row, orderBy []*sql.OrderByItem, colIdxMap map[string]int) {
	sort.Slice(indices, func(i, j int) bool {
		for _, item := range orderBy {
			// Get column name from expression
			var colIdx int
			var found bool
			switch expr := item.Expr.(type) {
			case *sql.ColumnRef:
				colIdx, found = colIdxMap[strings.ToLower(expr.Name)]
				if !found {
					continue
				}
			default:
				continue
			}

			// Get values from rows
			rowI := rows[indices[i]]
			rowJ := rows[indices[j]]
			if colIdx >= len(rowI.Values) || colIdx >= len(rowJ.Values) {
				continue
			}
			vi := e.valueToInterface(rowI.Values[colIdx])
			vj := e.valueToInterface(rowJ.Values[colIdx])

			cmp := compareValues(vi, vj)
			if cmp != 0 {
				if item.Ascending {
					return cmp < 0
				}
				return cmp > 0
			}
		}
		return false
	})
}

// computeWindowFunction computes window function values for a partition.
func (e *Executor) computeWindowFunction(wf *sql.WindowFuncCall, partition []int, rows []*row.Row, windowValues []map[int]interface{}, colIndex int, columnMap map[string]*types.ColumnInfo, columnOrder []*types.ColumnInfo, colIdxMap map[string]int) {
	funcName := strings.ToUpper(wf.Func.Name)

	switch funcName {
	case "ROW_NUMBER":
		for rank, rowIdx := range partition {
			windowValues[rowIdx][colIndex] = int64(rank + 1)
		}

	case "RANK":
		e.computeRank(partition, rows, wf, windowValues, colIndex, colIdxMap, false)

	case "DENSE_RANK":
		e.computeRank(partition, rows, wf, windowValues, colIndex, colIdxMap, true)

	case "COUNT":
		e.computeCountWindow(partition, rows, wf, windowValues, colIndex, colIdxMap)

	case "SUM":
		e.computeSumWindow(partition, rows, wf, windowValues, colIndex, colIdxMap)

	case "AVG":
		e.computeAvgWindow(partition, rows, wf, windowValues, colIndex, colIdxMap)

	case "MIN":
		e.computeMinWindow(partition, rows, wf, windowValues, colIndex, colIdxMap)

	case "MAX":
		e.computeMaxWindow(partition, rows, wf, windowValues, colIndex, colIdxMap)

	case "LEAD":
		e.computeLeadLag(partition, rows, wf, windowValues, colIndex, colIdxMap, true)

	case "LAG":
		e.computeLeadLag(partition, rows, wf, windowValues, colIndex, colIdxMap, false)

	case "FIRST_VALUE":
		e.computeFirstLastValue(partition, rows, wf, windowValues, colIndex, colIdxMap, true)

	case "LAST_VALUE":
		e.computeFirstLastValue(partition, rows, wf, windowValues, colIndex, colIdxMap, false)

	case "NTILE":
		e.computeNtile(partition, wf, windowValues, colIndex)

	case "NTH_VALUE":
		e.computeNthValue(partition, rows, wf, windowValues, colIndex, colIdxMap)

	case "PERCENT_RANK":
		e.computePercentRank(partition, rows, wf, windowValues, colIndex, colIdxMap)

	case "CUME_DIST":
		e.computeCumeDist(partition, rows, wf, windowValues, colIndex, colIdxMap)
	}
}

// computeRank computes RANK or DENSE_RANK window function.
func (e *Executor) computeRank(partition []int, rows []*row.Row, wf *sql.WindowFuncCall, windowValues []map[int]interface{}, colIndex int, colIdxMap map[string]int, dense bool) {
	if len(wf.Window.OrderBy) == 0 {
		// Without ORDER BY, all rows have rank 1
		for _, rowIdx := range partition {
			windowValues[rowIdx][colIndex] = int64(1)
		}
		return
	}

	// Get ORDER BY column index
	var orderColIdx int
	if colRef, ok := wf.Window.OrderBy[0].Expr.(*sql.ColumnRef); ok {
		orderColIdx = colIdxMap[strings.ToLower(colRef.Name)]
	}

	rank := 1
	denseRank := 1
	var prevValue interface{}

	for i, rowIdx := range partition {
		r := rows[rowIdx]
		if orderColIdx >= len(r.Values) {
			continue
		}
		currentValue := e.valueToInterface(r.Values[orderColIdx])

		if i > 0 && !valuesEqual(currentValue, prevValue) {
			if dense {
				denseRank++
			} else {
				rank = i + 1
			}
		}

		if dense {
			windowValues[rowIdx][colIndex] = int64(denseRank)
		} else {
			windowValues[rowIdx][colIndex] = int64(rank)
		}
		prevValue = currentValue
	}
}

// computeSumWindow computes SUM window function with frame support.
func (e *Executor) computeSumWindow(partition []int, rows []*row.Row, wf *sql.WindowFuncCall, windowValues []map[int]interface{}, colIndex int, colIdxMap map[string]int) {
	partitionLen := len(partition)
	frame := wf.Window.Frame

	for i, rowIdx := range partition {
		start, end := e.getFrameBounds(frame, partitionLen, i)
		var sum float64
		for j := start; j <= end && j < partitionLen; j++ {
			val := e.getWindowFuncArgValue(wf, rows[partition[j]], colIdxMap)
			if val != nil {
				switch v := val.(type) {
				case int:
					sum += float64(v)
				case int64:
					sum += float64(v)
				case float64:
					sum += v
				}
			}
		}
		windowValues[rowIdx][colIndex] = sum
	}
}

// computeAvgWindow computes AVG window function with frame support.
func (e *Executor) computeAvgWindow(partition []int, rows []*row.Row, wf *sql.WindowFuncCall, windowValues []map[int]interface{}, colIndex int, colIdxMap map[string]int) {
	partitionLen := len(partition)
	frame := wf.Window.Frame

	for i, rowIdx := range partition {
		start, end := e.getFrameBounds(frame, partitionLen, i)
		var sum float64
		var count int64
		for j := start; j <= end && j < partitionLen; j++ {
			val := e.getWindowFuncArgValue(wf, rows[partition[j]], colIdxMap)
			if val != nil {
				switch v := val.(type) {
				case int:
					sum += float64(v)
					count++
				case int64:
					sum += float64(v)
					count++
				case float64:
					sum += v
					count++
				}
			}
		}
		var avg float64
		if count > 0 {
			avg = sum / float64(count)
		}
		windowValues[rowIdx][colIndex] = avg
	}
}

// getFrameBounds returns the start and end indices within the partition for the given row position.
// The returned indices are relative to the partition slice.
func (e *Executor) getFrameBounds(frame *sql.FrameSpec, partitionLen int, rowPos int) (start, end int) {
	if frame == nil {
		// No frame specified, default is entire partition for aggregates
		return 0, partitionLen - 1
	}

	// Calculate start bound
	switch frame.Start.Type {
	case "UNBOUNDED PRECEDING":
		start = 0
	case "CURRENT ROW":
		start = rowPos
	case "PRECEDING":
		start = rowPos - frame.Start.Offset
		if start < 0 {
			start = 0
		}
	case "FOLLOWING":
		start = rowPos + frame.Start.Offset
		if start >= partitionLen {
			start = partitionLen - 1
		}
	case "UNBOUNDED FOLLOWING":
		start = partitionLen - 1
	default:
		start = 0
	}

	// Calculate end bound
	switch frame.End.Type {
	case "UNBOUNDED PRECEDING":
		end = 0
	case "CURRENT ROW":
		end = rowPos
	case "PRECEDING":
		end = rowPos - frame.End.Offset
		if end < 0 {
			end = 0
		}
	case "FOLLOWING":
		end = rowPos + frame.End.Offset
		if end >= partitionLen {
			end = partitionLen - 1
		}
	case "UNBOUNDED FOLLOWING":
		end = partitionLen - 1
	default:
		end = partitionLen - 1
	}

	// Ensure start <= end
	if start > end {
		start = end
	}

	return start, end
}

// computeCountWindow computes COUNT window function with frame support.
func (e *Executor) computeCountWindow(partition []int, rows []*row.Row, wf *sql.WindowFuncCall, windowValues []map[int]interface{}, colIndex int, colIdxMap map[string]int) {
	partitionLen := len(partition)
	frame := wf.Window.Frame

	for i, rowIdx := range partition {
		start, end := e.getFrameBounds(frame, partitionLen, i)
		count := int64(end - start + 1)
		windowValues[rowIdx][colIndex] = count
	}
}

// computeMinWindow computes MIN window function with frame support.
func (e *Executor) computeMinWindow(partition []int, rows []*row.Row, wf *sql.WindowFuncCall, windowValues []map[int]interface{}, colIndex int, colIdxMap map[string]int) {
	partitionLen := len(partition)
	frame := wf.Window.Frame

	for i, rowIdx := range partition {
		start, end := e.getFrameBounds(frame, partitionLen, i)
		var minVal interface{}
		for j := start; j <= end && j < partitionLen; j++ {
			val := e.getWindowFuncArgValue(wf, rows[partition[j]], colIdxMap)
			if val != nil {
				if minVal == nil || compareValues(val, minVal) < 0 {
					minVal = val
				}
			}
		}
		windowValues[rowIdx][colIndex] = minVal
	}
}

// computeMaxWindow computes MAX window function with frame support.
func (e *Executor) computeMaxWindow(partition []int, rows []*row.Row, wf *sql.WindowFuncCall, windowValues []map[int]interface{}, colIndex int, colIdxMap map[string]int) {
	partitionLen := len(partition)
	frame := wf.Window.Frame

	for i, rowIdx := range partition {
		start, end := e.getFrameBounds(frame, partitionLen, i)
		var maxVal interface{}
		for j := start; j <= end && j < partitionLen; j++ {
			val := e.getWindowFuncArgValue(wf, rows[partition[j]], colIdxMap)
			if val != nil {
				if maxVal == nil || compareValues(val, maxVal) > 0 {
					maxVal = val
				}
			}
		}
		windowValues[rowIdx][colIndex] = maxVal
	}
}

// computeNtile computes NTILE window function.
// NTILE(n) divides the partition into n roughly equal groups and returns the group number.
func (e *Executor) computeNtile(partition []int, wf *sql.WindowFuncCall, windowValues []map[int]interface{}, colIndex int) {
	// Get the number of buckets (n)
	numBuckets := 1
	if len(wf.Func.Args) >= 1 {
		if lit, ok := wf.Func.Args[0].(*sql.Literal); ok {
			switch v := lit.Value.(type) {
			case int64:
				numBuckets = int(v)
			case string:
				if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
					numBuckets = int(n)
				}
			}
		}
	}

	if numBuckets <= 0 {
		numBuckets = 1
	}

	partitionSize := len(partition)
	if partitionSize == 0 {
		return
	}

	// Calculate base bucket size and remainder
	// Each bucket should have either bucketSize or bucketSize+1 rows
	bucketSize := partitionSize / numBuckets
	remainder := partitionSize % numBuckets

	// Distribute rows to buckets
	// First 'remainder' buckets have bucketSize+1 rows
	// Remaining buckets have bucketSize rows
	currentPos := 0
	for bucket := 1; bucket <= numBuckets && currentPos < partitionSize; bucket++ {
		// Calculate how many rows in this bucket
		rowsInBucket := bucketSize
		if bucket <= remainder {
			rowsInBucket++
		}

		// Assign bucket number to each row in this bucket
		for i := 0; i < rowsInBucket && currentPos < partitionSize; i++ {
			rowIdx := partition[currentPos]
			windowValues[rowIdx][colIndex] = int64(bucket)
			currentPos++
		}
	}
}

// computeNthValue computes NTH_VALUE window function.
// NTH_VALUE(col, n) returns the nth value in the window frame.
func (e *Executor) computeNthValue(partition []int, rows []*row.Row, wf *sql.WindowFuncCall, windowValues []map[int]interface{}, colIndex int, colIdxMap map[string]int) {
	// Get the n parameter (which row to get)
	n := 1
	if len(wf.Func.Args) >= 2 {
		if lit, ok := wf.Func.Args[1].(*sql.Literal); ok {
			switch v := lit.Value.(type) {
			case int64:
				n = int(v)
			case string:
				if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed > 0 {
					n = int(parsed)
				}
			}
		}
	}

	if n <= 0 {
		n = 1
	}

	// Get column index for the first argument
	if len(wf.Func.Args) == 0 {
		return
	}

	var colIdx int
	if colRef, ok := wf.Func.Args[0].(*sql.ColumnRef); ok {
		var found bool
		colIdx, found = colIdxMap[strings.ToLower(colRef.Name)]
		if !found {
			return
		}
	} else {
		return
	}

	partitionLen := len(partition)
	frame := wf.Window.Frame

	// Check for FROM LAST (search from end instead of beginning)
	fromLast := false
	// Note: FROM LAST parsing would need additional support

	for i, rowIdx := range partition {
		start, end := e.getFrameBounds(frame, partitionLen, i)

		// Calculate the position within the frame
		var targetPos int
		if fromLast {
			// Count from the end
			targetPos = end - n + 1
		} else {
			// Count from the beginning
			targetPos = start + n - 1
		}

		// Check if position is valid
		if targetPos >= start && targetPos <= end && targetPos < partitionLen {
			targetRowIdx := partition[targetPos]
			r := rows[targetRowIdx]
			if colIdx < len(r.Values) && !r.Values[colIdx].Null {
				windowValues[rowIdx][colIndex] = e.valueToInterface(r.Values[colIdx])
			} else {
				windowValues[rowIdx][colIndex] = nil
			}
		} else {
			windowValues[rowIdx][colIndex] = nil
		}
	}
}

// computePercentRank computes PERCENT_RANK window function.
// PERCENT_RANK = (rank - 1) / (total_rows - 1)
func (e *Executor) computePercentRank(partition []int, rows []*row.Row, wf *sql.WindowFuncCall, windowValues []map[int]interface{}, colIndex int, colIdxMap map[string]int) {
	partitionLen := len(partition)
	if partitionLen == 0 {
		return
	}

	// If no ORDER BY, all rows have the same rank
	if len(wf.Window.OrderBy) == 0 {
		for _, rowIdx := range partition {
			windowValues[rowIdx][colIndex] = 0.0
		}
		return
	}

	// Get ORDER BY column index
	var orderColIdx int
	if colRef, ok := wf.Window.OrderBy[0].Expr.(*sql.ColumnRef); ok {
		orderColIdx = colIdxMap[strings.ToLower(colRef.Name)]
	}

	// Calculate rank for each row
	rank := 0
	var prevValue interface{}
	for i, rowIdx := range partition {
		r := rows[rowIdx]
		var currentValue interface{}
		if orderColIdx < len(r.Values) && !r.Values[orderColIdx].Null {
			currentValue = e.valueToInterface(r.Values[orderColIdx])
		}

		// Update rank if value changed
		if i > 0 && !valuesEqual(currentValue, prevValue) {
			rank = i
		}

		// Calculate percent rank: (rank - 1) / (total - 1)
		var percentRank float64
		if partitionLen > 1 {
			percentRank = float64(rank) / float64(partitionLen-1)
		} else {
			percentRank = 0.0
		}

		windowValues[rowIdx][colIndex] = percentRank
		prevValue = currentValue
	}
}

// computeCumeDist computes CUME_DIST window function.
// CUME_DIST = number of rows with value <= current / total rows
func (e *Executor) computeCumeDist(partition []int, rows []*row.Row, wf *sql.WindowFuncCall, windowValues []map[int]interface{}, colIndex int, colIdxMap map[string]int) {
	partitionLen := len(partition)
	if partitionLen == 0 {
		return
	}

	// If no ORDER BY, all rows have the same cume_dist
	if len(wf.Window.OrderBy) == 0 {
		cumeDist := 1.0
		for _, rowIdx := range partition {
			windowValues[rowIdx][colIndex] = cumeDist
		}
		return
	}

	// Get ORDER BY column index
	var orderColIdx int
	if colRef, ok := wf.Window.OrderBy[0].Expr.(*sql.ColumnRef); ok {
		orderColIdx = colIdxMap[strings.ToLower(colRef.Name)]
	}

	// For each row, count how many rows have value <= current
	for _, rowIdx := range partition {
		r := rows[rowIdx]
		var currentValue interface{}
		if orderColIdx < len(r.Values) && !r.Values[orderColIdx].Null {
			currentValue = e.valueToInterface(r.Values[orderColIdx])
		}

		// Count rows with value <= current (including NULL handling)
		count := 0
		for _, otherRowIdx := range partition {
			otherRow := rows[otherRowIdx]
			var otherValue interface{}
			if orderColIdx < len(otherRow.Values) && !otherRow.Values[orderColIdx].Null {
				otherValue = e.valueToInterface(otherRow.Values[orderColIdx])
			}

			// Compare values
			if currentValue == nil && otherValue == nil {
				count++
			} else if currentValue != nil && otherValue != nil {
				if compareValues(otherValue, currentValue) <= 0 {
					count++
				}
			}
			// Note: NULL values are typically considered less than non-NULL values
			// but for CUME_DIST we consider NULLs equal to NULLs
		}

		// Calculate cumulative distribution
		cumeDist := float64(count) / float64(partitionLen)
		windowValues[rowIdx][colIndex] = cumeDist
	}
}

// computeLeadLag computes LEAD or LAG window function.
// LEAD(col, offset, default) - access value from a following row
// LAG(col, offset, default) - access value from a preceding row
func (e *Executor) computeLeadLag(partition []int, rows []*row.Row, wf *sql.WindowFuncCall, windowValues []map[int]interface{}, colIndex int, colIdxMap map[string]int, isLead bool) {
	// Get offset (default is 1)
	offset := 1
	if len(wf.Func.Args) >= 2 {
		if lit, ok := wf.Func.Args[1].(*sql.Literal); ok {
			if strVal, ok := lit.Value.(string); ok {
				if n, err := strconv.ParseInt(strVal, 10, 64); err == nil && n > 0 {
					offset = int(n)
				}
			}
		}
	}

	// Get default value (default is NULL)
	var defaultVal interface{}
	if len(wf.Func.Args) >= 3 {
		if lit, ok := wf.Func.Args[2].(*sql.Literal); ok {
			defaultVal = lit.Value
		}
	}

	// Get column index for the first argument
	if len(wf.Func.Args) == 0 {
		return
	}

	var colIdx int
	if colRef, ok := wf.Func.Args[0].(*sql.ColumnRef); ok {
		var found bool
		colIdx, found = colIdxMap[strings.ToLower(colRef.Name)]
		if !found {
			return
		}
	} else {
		return
	}

	// Compute LEAD/LAG for each row
	for i, rowIdx := range partition {
		var targetIdx int
		if isLead {
			targetIdx = i + offset
		} else {
			targetIdx = i - offset
		}

		if targetIdx >= 0 && targetIdx < len(partition) {
			targetRowIdx := partition[targetIdx]
			r := rows[targetRowIdx]
			if colIdx < len(r.Values) && !r.Values[colIdx].Null {
				val := e.valueToInterface(r.Values[colIdx])
				// Handle IGNORE NULLS
				if wf.IgnoreNulls && val == nil {
					// Skip NULL values, look for next non-NULL
					found := false
					for j := targetIdx + 1; j < len(partition) && !found; j++ {
						checkRow := rows[partition[j]]
						if colIdx < len(checkRow.Values) && !checkRow.Values[colIdx].Null {
							val = e.valueToInterface(checkRow.Values[colIdx])
							found = true
						}
					}
					if !found {
						windowValues[rowIdx][colIndex] = defaultVal
						continue
					}
				}
				windowValues[rowIdx][colIndex] = val
			} else {
				windowValues[rowIdx][colIndex] = defaultVal
			}
		} else {
			windowValues[rowIdx][colIndex] = defaultVal
		}
	}
}

// computeFirstLastValue computes FIRST_VALUE or LAST_VALUE window function.
func (e *Executor) computeFirstLastValue(partition []int, rows []*row.Row, wf *sql.WindowFuncCall, windowValues []map[int]interface{}, colIndex int, colIdxMap map[string]int, isFirst bool) {
	if len(wf.Func.Args) == 0 {
		return
	}

	var colIdx int
	if colRef, ok := wf.Func.Args[0].(*sql.ColumnRef); ok {
		var found bool
		colIdx, found = colIdxMap[strings.ToLower(colRef.Name)]
		if !found {
			return
		}
	} else {
		return
	}

	// For FIRST_VALUE, use the first row in the partition
	// For LAST_VALUE, use the last row in the partition
	var targetRowIdx int
	if isFirst {
		targetRowIdx = partition[0]
	} else {
		targetRowIdx = partition[len(partition)-1]
	}

	r := rows[targetRowIdx]
	var val interface{}
	if colIdx < len(r.Values) && !r.Values[colIdx].Null {
		val = e.valueToInterface(r.Values[colIdx])
	}

	// Handle IGNORE NULLS
	if wf.IgnoreNulls && val == nil {
		// Find first non-NULL value
		for _, rowIdx := range partition {
			checkRow := rows[rowIdx]
			if colIdx < len(checkRow.Values) && !checkRow.Values[colIdx].Null {
				val = e.valueToInterface(checkRow.Values[colIdx])
				break
			}
		}
	}

	for _, rowIdx := range partition {
		windowValues[rowIdx][colIndex] = val
	}
}

// getWindowFuncArgValue gets the argument value for a window function from a row.
func (e *Executor) getWindowFuncArgValue(wf *sql.WindowFuncCall, r *row.Row, colIdxMap map[string]int) interface{} {
	if len(wf.Func.Args) == 0 {
		return nil
	}
	if _, ok := wf.Func.Args[0].(*sql.StarExpr); ok {
		return 1 // COUNT(*) returns 1 for each row
	}
	if colRef, ok := wf.Func.Args[0].(*sql.ColumnRef); ok {
		colIdx, found := colIdxMap[strings.ToLower(colRef.Name)]
		if !found || colIdx >= len(r.Values) {
			return nil
		}
		return e.valueToInterface(r.Values[colIdx])
	}
	return nil
}

// valuesEqual checks if two values are equal.
func valuesEqual(a, b interface{}) bool {
	return compareValues(a, b) == 0
}

// executeGroupBy handles GROUP BY and aggregate functions.
func (e *Executor) executeGroupBy(stmt *sql.SelectStmt, rows []*row.Row, resultCols []ColumnInfo, colIndices []int, funcExprs []sql.Expression, aggregateFuncs []struct {
	name   string
	arg    string
	index  int
	filter sql.Expression
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
			// Apply FILTER clause if present
			filteredRows := groupRows
			if agg.filter != nil {
				filteredRows = make([]*row.Row, 0)
				for _, r := range groupRows {
					match, err := e.evaluateWhere(agg.filter, r, columnMap, columnOrder)
					if err != nil {
						return nil, fmt.Errorf("FILTER clause error: %w", err)
					}
					if match {
						filteredRows = append(filteredRows, r)
					}
				}
			}

			switch agg.name {
			case "COUNT":
				resultRow[agg.index] = len(filteredRows)
			case "SUM":
				var sum int64
				for _, r := range filteredRows {
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
				for _, r := range filteredRows {
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
				for _, r := range filteredRows {
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
				for _, r := range filteredRows {
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
			case "GROUP_CONCAT":
				// GROUP_CONCAT with optional SEPARATOR and ORDER BY
				// Syntax: GROUP_CONCAT(expr [SEPARATOR str] [ORDER BY col])
				var parts []string
				for _, r := range filteredRows {
					if agg.arg != "" {
						for j, col := range tblInfo.Columns {
							if strings.ToLower(col.Name) == agg.arg {
								if j < len(r.Values) && !r.Values[j].Null {
									parts = append(parts, fmt.Sprintf("%v", e.valueToInterface(r.Values[j])))
								}
							}
						}
					}
				}
				// Default separator is comma
				resultRow[agg.index] = strings.Join(parts, ",")
			case "STRING_AGG":
				// STRING_AGG(expr, separator) - PostgreSQL-style string aggregation
				// Syntax: STRING_AGG(expr, separator)
				var parts []string
				separator := "," // default separator
				// The separator is typically the second argument
				// For simplicity, we use comma as default
				for _, r := range filteredRows {
					if agg.arg != "" {
						for j, col := range tblInfo.Columns {
							if strings.ToLower(col.Name) == agg.arg {
								if j < len(r.Values) && !r.Values[j].Null {
									parts = append(parts, fmt.Sprintf("%v", e.valueToInterface(r.Values[j])))
								}
							}
						}
					}
				}
				resultRow[agg.index] = strings.Join(parts, separator)
			case "STDDEV", "STDDEV_SAMP":
				// Standard deviation (sample)
				var values []float64
				for _, r := range filteredRows {
					if agg.arg != "" {
						for j, col := range tblInfo.Columns {
							if strings.ToLower(col.Name) == agg.arg {
								if j < len(r.Values) && !r.Values[j].Null {
									// Convert to float64 - handle both int and float types
									var val float64
									if r.Values[j].Type == types.TypeFloat {
										val = r.Values[j].AsFloat()
									} else {
										val = float64(r.Values[j].AsInt())
									}
									values = append(values, val)
								}
							}
						}
					}
				}
				if len(values) < 2 {
					resultRow[agg.index] = nil
				} else {
					// Calculate mean
					var sum float64
					for _, v := range values {
						sum += v
					}
					mean := sum / float64(len(values))
					// Calculate variance
					var varianceSum float64
					for _, v := range values {
						diff := v - mean
						varianceSum += diff * diff
					}
					variance := varianceSum / float64(len(values)-1) // Sample variance
					resultRow[agg.index] = math.Sqrt(variance)
				}
			case "VARIANCE", "VAR_SAMP":
				// Variance (sample)
				var values []float64
				for _, r := range filteredRows {
					if agg.arg != "" {
						for j, col := range tblInfo.Columns {
							if strings.ToLower(col.Name) == agg.arg {
								if j < len(r.Values) && !r.Values[j].Null {
									// Convert to float64 - handle both int and float types
									var val float64
									if r.Values[j].Type == types.TypeFloat {
										val = r.Values[j].AsFloat()
									} else {
										val = float64(r.Values[j].AsInt())
									}
									values = append(values, val)
								}
							}
						}
					}
				}
				if len(values) < 2 {
					resultRow[agg.index] = nil
				} else {
					// Calculate mean
					var sum float64
					for _, v := range values {
						sum += v
					}
					mean := sum / float64(len(values))
					// Calculate variance
					var varianceSum float64
					for _, v := range values {
						diff := v - mean
						varianceSum += diff * diff
					}
					resultRow[agg.index] = varianceSum / float64(len(values)-1) // Sample variance
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
	name   string
	arg    string
	index  int
	filter sql.Expression
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
		return e.compareValues(left, ex.Op, right, ex.EscapeChar)

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
	name   string
	arg    string
	index  int
	filter sql.Expression
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
		if stmt.All {
			result.Rows = e.intersectAllRows(leftResult.Rows, rightResult.Rows)
		} else {
			result.Rows = e.intersectRows(leftResult.Rows, rightResult.Rows)
		}

	case sql.SetExcept:
		// EXCEPT: rows in left but not in right
		if stmt.All {
			result.Rows = e.exceptAllRows(leftResult.Rows, rightResult.Rows)
		} else {
			result.Rows = e.exceptRows(leftResult.Rows, rightResult.Rows)
		}
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

// intersectAllRows returns rows that exist in both left and right, preserving duplicates.
// Each row appears min(count_in_left, count_in_right) times.
func (e *Executor) intersectAllRows(left, right [][]interface{}) [][]interface{} {
	// Count occurrences in right
	rightCounts := make(map[string]int)
	for _, row := range right {
		key := e.rowKey(row)
		rightCounts[key]++
	}

	// Track how many times we've used each key from right
	usedCounts := make(map[string]int)

	var result [][]interface{}
	for _, row := range left {
		key := e.rowKey(row)
		// Check if we can still use this row from right
		if usedCounts[key] < rightCounts[key] {
			result = append(result, row)
			usedCounts[key]++
		}
	}
	return result
}

// exceptAllRows returns rows in left but not in right, preserving duplicates.
// Each row appears max(0, count_in_left - count_in_right) times.
func (e *Executor) exceptAllRows(left, right [][]interface{}) [][]interface{} {
	// Count occurrences in right
	rightCounts := make(map[string]int)
	for _, row := range right {
		key := e.rowKey(row)
		rightCounts[key]++
	}

	// Track how many times we've seen each key from left
	leftCounts := make(map[string]int)

	var result [][]interface{}
	for _, row := range left {
		key := e.rowKey(row)
		leftCounts[key]++
		// Include row if we haven't exhausted the right-side occurrences
		// count_left_so_far > count_right means we should include this row
		if leftCounts[key] > rightCounts[key] {
			result = append(result, row)
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

// executeWith executes a WITH clause (CTE) statement.
func (e *Executor) executeWith(stmt *sql.WithStmt) (*Result, error) {
	// Initialize CTE results map if needed
	if e.cteResults == nil {
		e.cteResults = make(map[string]*Result)
	}

	// Save current CTE results to restore later (for nested CTEs)
	oldCTEResults := e.cteResults
	e.cteResults = make(map[string]*Result)
	for k, v := range oldCTEResults {
		e.cteResults[k] = v
	}

	// Execute each CTE and store the results
	for _, cte := range stmt.CTEs {
		var cteResult *Result
		var err error

		if cte.Recursive {
			// Handle recursive CTE
			cteResult, err = e.executeRecursiveCTE(cte)
		} else {
			// Handle non-recursive CTE
			cteResult, err = e.executeStatementForCTE(cte.Query)
		}

		if err != nil {
			return nil, fmt.Errorf("CTE '%s' error: %w", cte.Name, err)
		}

		// Apply column aliases if specified
		if len(cte.Columns) > 0 {
			for i, colName := range cte.Columns {
				if i < len(cteResult.Columns) {
					cteResult.Columns[i].Name = colName
				}
			}
		}

		// Store the CTE result
		e.cteResults[strings.ToLower(cte.Name)] = cteResult
	}

	// Execute the main query
	result, err := e.executeStatementForCTE(stmt.MainQuery)
	if err != nil {
		return nil, err
	}

	// Restore original CTE results
	e.cteResults = oldCTEResults

	return result, nil
}

// executeRecursiveCTE executes a recursive CTE.
// Recursive CTEs must have the form: base_query UNION ALL recursive_query
func (e *Executor) executeRecursiveCTE(cte sql.CTEDefinition) (*Result, error) {
	cteName := strings.ToLower(cte.Name)

	// The CTE query must be a UNION ALL
	unionStmt, ok := cte.Query.(*sql.UnionStmt)
	if !ok {
		return nil, fmt.Errorf("recursive CTE '%s' must be a UNION ALL of base and recursive queries", cte.Name)
	}

	if unionStmt.Op != sql.SetUnion || !unionStmt.All {
		return nil, fmt.Errorf("recursive CTE '%s' must use UNION ALL (not UNION)", cte.Name)
	}

	// Execute the base query (left side of UNION ALL)
	baseResult, err := e.executeStatementForCTE(unionStmt.Left)
	if err != nil {
		return nil, fmt.Errorf("recursive CTE '%s' base query error: %w", cte.Name, err)
	}

	// Initialize the working result with base query results
	workingResult := &Result{
		Columns: baseResult.Columns,
		Rows:    make([][]interface{}, len(baseResult.Rows)),
	}
	copy(workingResult.Rows, baseResult.Rows)

	// Store initial result so recursive query can reference it
	e.cteResults[cteName] = workingResult

	// Maximum iterations to prevent infinite loops
	const maxIterations = 10000

	// Iterate until no new rows are produced
	for iteration := 0; iteration < maxIterations; iteration++ {
		// Execute the recursive query (right side of UNION ALL)
		recursiveResult, err := e.executeStatementForCTE(unionStmt.Right)
		if err != nil {
			return nil, fmt.Errorf("recursive CTE '%s' recursive query error: %w", cte.Name, err)
		}

		// Check if any new rows were produced
		if len(recursiveResult.Rows) == 0 {
			break // No more rows, we're done
		}

		// Track rows seen for cycle detection
		seenRows := make(map[string]bool)
		for _, row := range workingResult.Rows {
			seenRows[e.rowKey(row)] = true
		}

		// Add new rows to working result
		newRowsAdded := 0
		for _, row := range recursiveResult.Rows {
			key := e.rowKey(row)
			if !seenRows[key] {
				workingResult.Rows = append(workingResult.Rows, row)
				seenRows[key] = true
				newRowsAdded++
			}
		}

		// If no new rows were added (all were duplicates), we're done
		if newRowsAdded == 0 {
			break
		}

		// Update the CTE result for the next iteration
		e.cteResults[cteName] = workingResult
	}

	workingResult.RowCount = len(workingResult.Rows)
	return workingResult, nil
}

// executeStatementForCTE executes a statement within a CTE context.
func (e *Executor) executeStatementForCTE(stmt sql.Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *sql.SelectStmt:
		return e.executeSelect(s)
	case *sql.UnionStmt:
		return e.executeUnion(s)
	case *sql.WithStmt:
		return e.executeWith(s)
	default:
		return nil, fmt.Errorf("unsupported statement in CTE: %T", stmt)
	}
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

// executeSelectFromValues handles SELECT from a VALUES table constructor.
func (e *Executor) executeSelectFromValues(stmt *sql.SelectStmt) (*Result, error) {
	valuesExpr := stmt.From.Table.Values
	tableAlias := stmt.From.Table.Alias
	if tableAlias == "" {
		tableAlias = "values_table"
	}

	// Build column info
	numCols := 0
	if len(valuesExpr.Rows) > 0 {
		numCols = len(valuesExpr.Rows[0])
	}

	// Create column names (use provided names or generate default names)
	colNames := valuesExpr.Columns
	if len(colNames) == 0 {
		for i := 0; i < numCols; i++ {
			colNames = append(colNames, fmt.Sprintf("column%d", i+1))
		}
	}

	// Build column order for expression evaluation
	columnOrder := make([]*types.ColumnInfo, numCols)
	for i := 0; i < numCols; i++ {
		columnOrder[i] = &types.ColumnInfo{
			Name: colNames[i],
			Type: types.TypeVarchar,
		}
	}

	// Build column index map
	colIdxMap := make(map[string]int)
	for i, name := range colNames {
		colIdxMap[strings.ToLower(name)] = i
	}

	// Convert VALUES rows to result rows
	var rows []*row.Row
	for _, valueRow := range valuesExpr.Rows {
		r := &row.Row{Values: make([]types.Value, numCols)}
		for i, expr := range valueRow {
			if i < numCols {
				// Evaluate the expression
				switch ex := expr.(type) {
				case *sql.Literal:
					switch ex.Type {
					case sql.LiteralNumber:
						// Number can be stored as int64, float64, or string
						switch v := ex.Value.(type) {
						case int64:
							r.Values[i] = types.NewIntValue(v)
						case float64:
							r.Values[i] = types.NewFloatValue(v)
						case string:
							if n, err := strconv.ParseInt(v, 10, 64); err == nil {
								r.Values[i] = types.NewIntValue(n)
							} else if f, err := strconv.ParseFloat(v, 64); err == nil {
								r.Values[i] = types.NewFloatValue(f)
							} else {
								r.Values[i] = types.NewStringValue(v, types.TypeVarchar)
							}
						}
					case sql.LiteralString:
						if strVal, ok := ex.Value.(string); ok {
							r.Values[i] = types.NewStringValue(strVal, types.TypeVarchar)
						}
					case sql.LiteralBool:
						r.Values[i] = types.NewIntValue(0)
						if b, ok := ex.Value.(bool); ok && b {
							r.Values[i] = types.NewIntValue(1)
						}
					case sql.LiteralNull:
						r.Values[i] = types.Value{Null: true}
					default:
						r.Values[i] = types.NewStringValue(fmt.Sprintf("%v", ex.Value), types.TypeVarchar)
					}
				default:
					// Default to string representation
					r.Values[i] = types.NewStringValue(expr.String(), types.TypeVarchar)
				}
			}
		}
		rows = append(rows, r)
	}

	// Build result columns
	result := &Result{
		Columns: make([]ColumnInfo, numCols),
		Rows:    make([][]interface{}, 0),
	}
	for i, name := range colNames {
		result.Columns[i] = ColumnInfo{
			Name: name,
			Type: "VARCHAR",
		}
	}

	// Process each row
	for _, r := range rows {
		resultRow := make([]interface{}, numCols)
		for i := 0; i < numCols; i++ {
			if i < len(r.Values) && !r.Values[i].Null {
				resultRow[i] = e.valueToInterface(r.Values[i])
			}
		}
		result.Rows = append(result.Rows, resultRow)
	}

	// Apply WHERE clause if present
	if stmt.Where != nil {
		filteredRows := make([][]interface{}, 0)
		for i, resultRow := range result.Rows {
			r := rows[i]
			match, err := e.evaluateWhereForRow(stmt.Where, r, columnOrder, colIdxMap)
			if err != nil {
				return nil, err
			}
			if match {
				filteredRows = append(filteredRows, resultRow)
			}
		}
		result.Rows = filteredRows
	}

	// Apply ORDER BY if present
	if len(stmt.OrderBy) > 0 {
		result.Rows = e.sortRows(stmt.OrderBy, result.Columns, result.Rows)
	}

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

// executeSelectFromLateral handles SELECT from a LATERAL subquery.
func (e *Executor) executeSelectFromLateral(stmt *sql.SelectStmt) (*Result, error) {
	// LATERAL allows the subquery to reference columns from tables
	// that appear earlier in the FROM clause

	// For now, execute the subquery with outer context
	// In a full implementation, this would be joined with preceding tables
	subquery := stmt.From.Table.Subquery

	// Execute the lateral subquery
	derivedResult, err := e.executeStatement(subquery.Select)
	if err != nil {
		return nil, fmt.Errorf("lateral subquery error: %w", err)
	}

	// Use the alias if provided
	tableAlias := stmt.From.Table.Alias
	if tableAlias == "" {
		tableAlias = "lateral_table"
	}

	// Build column index map for the lateral result
	colIdxMap := make(map[string]int)
	for i, col := range derivedResult.Columns {
		colIdxMap[strings.ToLower(col.Name)] = i
	}

	// Build result columns
	result := &Result{
		Columns: make([]ColumnInfo, 0),
		Rows:    make([][]interface{}, 0),
	}

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
			ci := ColumnInfo{
				Name: fmt.Sprintf("expr_%d", len(result.Columns)+1),
				Type: "VARCHAR",
			}
			result.Columns = append(result.Columns, ci)
		}
	}

	// Process each row
	for _, srcRow := range derivedResult.Rows {
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
				// For complex expressions, just use the first column
				if len(srcRow) > 0 {
					resultRow[colIdx] = srcRow[0]
				}
				colIdx++
			}
		}
		result.Rows = append(result.Rows, resultRow)
	}

	// Apply ORDER BY if present
	if len(stmt.OrderBy) > 0 {
		result.Rows = e.sortRows(stmt.OrderBy, result.Columns, result.Rows)
	}

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

// executeSelectFromCTE handles SELECT from a CTE (Common Table Expression).
func (e *Executor) executeSelectFromCTE(stmt *sql.SelectStmt, cteResult *Result) (*Result, error) {
	// Set current table for correlated subqueries
	oldTable := e.currentTable
	tableName := strings.ToLower(stmt.From.Table.Name)
	e.currentTable = tableName
	defer func() { e.currentTable = oldTable }()

	// Build column index map for the CTE
	colIdxMap := make(map[string]int)
	for i, col := range cteResult.Columns {
		colIdxMap[strings.ToLower(col.Name)] = i
	}

	// Build column info for the CTE
	columnOrder := make([]*types.ColumnInfo, len(cteResult.Columns))
	for i, col := range cteResult.Columns {
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
			for _, col := range cteResult.Columns {
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
				ci.Name = expr.Alias
			}
			result.Columns = append(result.Columns, ci)
		case *sql.BinaryExpr:
			// Binary expressions can have aliases
			colName := fmt.Sprintf("expr_%d", len(result.Columns)+1)
			if expr.Alias != "" {
				colName = expr.Alias
			}
			result.Columns = append(result.Columns, ColumnInfo{
				Name: colName,
				Type: "VARCHAR",
			})
		case *sql.Literal:
			// Literals can have aliases
			colName := fmt.Sprintf("expr_%d", len(result.Columns)+1)
			if expr.Alias != "" {
				colName = expr.Alias
			}
			result.Columns = append(result.Columns, ColumnInfo{
				Name: colName,
				Type: "VARCHAR",
			})
		case *sql.FunctionCall:
			// Function calls - use function name as column name
			colName := expr.Name + "()"
			result.Columns = append(result.Columns, ColumnInfo{
				Name: colName,
				Type: "VARCHAR",
			})
		default:
			// Handle other expressions
			ci := ColumnInfo{
				Name: fmt.Sprintf("expr_%d", len(result.Columns)+1),
				Type: "VARCHAR",
			}
			result.Columns = append(result.Columns, ci)
		}
	}

	// Process each row from the CTE
	for _, srcRow := range cteResult.Rows {
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

		// Apply WHERE filter if present
		if stmt.Where != nil {
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
				// Evaluate other expressions (like n + 1)
				val, err := e.evaluateExprForRow(expr, pseudoRow, columnOrder, colIdxMap)
				if err != nil {
					return nil, err
				}
				resultRow[colIdx] = val
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

// executeSelectFromView handles SELECT from a view.
func (e *Executor) executeSelectFromView(stmt *sql.SelectStmt, viewName string) (*Result, error) {
	// Get the view definition
	viewInfo, err := e.engine.GetView(viewName)
	if err != nil {
		return nil, fmt.Errorf("view %s does not exist", viewName)
	}

	// Debug: log the view query
	// fmt.Printf("DEBUG: View query: %s\n", viewInfo.Query)

	// Parse and execute the view's query
	parser := sql.NewParser(viewInfo.Query)
	viewStmt, err := parser.Parse()
	if err != nil {
		return nil, fmt.Errorf("view query parse error: %w", err)
	}

	viewSelect, ok := viewStmt.(*sql.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("view query is not a SELECT statement")
	}

	// Execute the view's query
	viewResult, err := e.executeSelect(viewSelect)
	if err != nil {
		return nil, err
	}

	// Debug: log the view result
	// fmt.Printf("DEBUG: View result: %d rows, %d columns\n", len(viewResult.Rows), len(viewResult.Columns))

	// Now apply the outer SELECT's filters/columns to the view result
	// Build column index map for the view result
	colIdxMap := make(map[string]int)
	for i, col := range viewResult.Columns {
		colIdxMap[strings.ToLower(col.Name)] = i
	}

	// Build column info for the view
	columnOrder := make([]*types.ColumnInfo, len(viewResult.Columns))
	columnMap := make(map[string]*types.ColumnInfo)
	for i, col := range viewResult.Columns {
		columnOrder[i] = &types.ColumnInfo{
			Name: col.Name,
			Type: types.TypeVarchar,
		}
		columnMap[strings.ToLower(col.Name)] = columnOrder[i]
	}

	result := &Result{
		Columns: make([]ColumnInfo, 0),
		Rows:    make([][]interface{}, 0),
	}

	// Convert view result rows to Row format for processing
	rows := make([]*row.Row, len(viewResult.Rows))
	for i, r := range viewResult.Rows {
		values := make([]types.Value, len(r))
		for j, v := range r {
			if v == nil {
				values[j] = types.NewNullValue()
			} else {
				switch val := v.(type) {
				case int:
					values[j] = types.NewIntValue(int64(val))
				case int64:
					values[j] = types.NewIntValue(val)
				case float64:
					values[j] = types.NewFloatValue(val)
				case string:
					values[j] = types.NewStringValue(val, types.TypeVarchar)
				case bool:
					if val {
						values[j] = types.NewIntValue(1)
					} else {
						values[j] = types.NewIntValue(0)
					}
				default:
					values[j] = types.NewStringValue(fmt.Sprintf("%v", val), types.TypeVarchar)
				}
			}
		}
		rows[i] = &row.Row{Values: values}
	}

	// Process SELECT columns
	resultCols := make([]ColumnInfo, 0)
	colIndices := make([]int, 0)
	var funcExprs []sql.Expression

	for _, colExpr := range stmt.Columns {
		switch expr := colExpr.(type) {
		case *sql.StarExpr:
			// Add all view columns
			for i, col := range viewResult.Columns {
				resultCols = append(resultCols, col)
				colIndices = append(colIndices, i)
				funcExprs = append(funcExprs, nil)
			}
		case *sql.ColumnRef:
			colName := expr.Name
			if expr.Alias != "" {
				resultCols = append(resultCols, ColumnInfo{Name: expr.Alias, Type: "VARCHAR"})
			} else {
				resultCols = append(resultCols, ColumnInfo{Name: colName, Type: "VARCHAR"})
			}
			idx := colIdxMap[strings.ToLower(colName)]
			colIndices = append(colIndices, idx)
			funcExprs = append(funcExprs, nil)
		case *sql.FunctionCall:
			colName := expr.Name
			resultCols = append(resultCols, ColumnInfo{Name: colName + "()", Type: "VARCHAR"})
			colIndices = append(colIndices, -1)
			funcExprs = append(funcExprs, expr)
		default:
			resultCols = append(resultCols, ColumnInfo{Name: "expr", Type: "VARCHAR"})
			colIndices = append(colIndices, -1)
			funcExprs = append(funcExprs, expr)
		}
	}

	result.Columns = resultCols

	// Filter and project rows
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

		// Build result row
		resultRow := make([]interface{}, len(colIndices))
		for i, idx := range colIndices {
			if idx >= 0 && idx < len(r.Values) {
				resultRow[i] = e.valueToInterface(r.Values[idx])
			} else if funcExprs[i] != nil {
				val, err := e.evaluateExpression(funcExprs[i], r, columnMap, columnOrder)
				if err != nil {
					return nil, err
				}
				resultRow[i] = val
			}
		}
		result.Rows = append(result.Rows, resultRow)
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		result.Rows = e.sortRows(stmt.OrderBy, result.Columns, result.Rows)
	}

	// Apply LIMIT
	if stmt.Limit != nil {
		limit := *stmt.Limit
		if limit < len(result.Rows) {
			result.Rows = result.Rows[:limit]
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
			colName := fmt.Sprintf("%d", len(result.Columns)+1)
			if expr.Alias != "" {
				colName = expr.Alias
			}
			result.Columns = append(result.Columns, ColumnInfo{
				Name: colName,
				Type: "INT",
			})
			row = append(row, expr.Value)
		case *sql.ColumnRef:
			colName := expr.Name
			if expr.Alias != "" {
				colName = expr.Alias
			}
			result.Columns = append(result.Columns, ColumnInfo{
				Name: colName,
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
			colName := "expr"
			if expr.Alias != "" {
				colName = expr.Alias
			}
			result.Columns = append(result.Columns, ColumnInfo{
				Name: colName,
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

// evaluateExpressionWithoutRow evaluates an expression without row context.
// Used for evaluating default values, check constraints, etc.
func (e *Executor) evaluateExpressionWithoutRow(expr sql.Expression) (interface{}, error) {
	switch ex := expr.(type) {
	case *sql.Literal:
		return ex.Value, nil
	case *sql.BinaryExpr:
		return e.evaluateBinaryExprWithoutRow(ex)
	case *sql.UnaryExpr:
		val, err := e.evaluateExpressionWithoutRow(ex.Right)
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
	case *sql.FunctionCall:
		// Handle simple functions that don't require row data
		return e.evaluateFunctionWithoutRow(ex)
	case *sql.CastExpr:
		val, err := e.evaluateExpressionWithoutRow(ex.Expr)
		if err != nil {
			return nil, err
		}
		return e.castValue(val, ex.Type.Name)
	default:
		return nil, fmt.Errorf("unsupported expression type for evaluation: %T", expr)
	}
}

// evaluateFunctionWithoutRow evaluates a function without row context.
func (e *Executor) evaluateFunctionWithoutRow(expr *sql.FunctionCall) (interface{}, error) {
	// Handle simple functions that don't require row data
	switch strings.ToUpper(expr.Name) {
	case "CURRENT_TIMESTAMP", "NOW":
		return time.Now().Format("2006-01-02 15:04:05"), nil
	case "CURRENT_DATE":
		return time.Now().Format("2006-01-02"), nil
	case "CURRENT_TIME":
		return time.Now().Format("15:04:05"), nil
	case "NULL":
		return nil, nil
	case "UPPER":
		if len(expr.Args) > 0 {
			val, err := e.evaluateExpressionWithoutRow(expr.Args[0])
			if err != nil {
				return nil, err
			}
			return strings.ToUpper(fmt.Sprintf("%v", val)), nil
		}
		return nil, nil
	case "LOWER":
		if len(expr.Args) > 0 {
			val, err := e.evaluateExpressionWithoutRow(expr.Args[0])
			if err != nil {
				return nil, err
			}
			return strings.ToLower(fmt.Sprintf("%v", val)), nil
		}
		return nil, nil
	case "COALESCE":
		for _, arg := range expr.Args {
			val, err := e.evaluateExpressionWithoutRow(arg)
			if err != nil {
				return nil, err
			}
			if val != nil {
				return val, nil
			}
		}
		return nil, nil
	case "IFNULL":
		if len(expr.Args) >= 2 {
			val, err := e.evaluateExpressionWithoutRow(expr.Args[0])
			if err != nil {
				return nil, err
			}
			if val != nil {
				return val, nil
			}
			return e.evaluateExpressionWithoutRow(expr.Args[1])
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("function %s cannot be evaluated without row context", expr.Name)
	}
}

// executeInsert executes an INSERT statement.
func (e *Executor) executeInsert(stmt *sql.InsertStmt) (*Result, error) {
	// Handle WITH clause if present
	if stmt.WithClause != nil {
		// Initialize CTE results map if needed
		if e.cteResults == nil {
			e.cteResults = make(map[string]*Result)
		}

		// Save current CTE results to restore later
		oldCTEResults := e.cteResults
		e.cteResults = make(map[string]*Result)
		for k, v := range oldCTEResults {
			e.cteResults[k] = v
		}

		// Execute each CTE and store the results
		for _, cte := range stmt.WithClause.CTEs {
			var cteResult *Result
			var err error
			if cte.Recursive {
				cteResult, err = e.executeRecursiveCTE(cte)
			} else {
				cteResult, err = e.executeStatementForCTE(cte.Query)
			}
			if err != nil {
				e.cteResults = oldCTEResults
				return nil, fmt.Errorf("CTE '%s' error: %w", cte.Name, err)
			}

			// Validate CTE result
			if cteResult == nil {
				e.cteResults = oldCTEResults
				return nil, fmt.Errorf("CTE '%s' returned nil result", cte.Name)
			}

			// Store the CTE result
			e.cteResults[strings.ToLower(cte.Name)] = cteResult
		}

		// Execute the INSERT statement (can reference CTEs now)
		result, err := e.executeInsertInternal(stmt)

		// Restore original CTE results
		e.cteResults = oldCTEResults

		return result, err
	}

	return e.executeInsertInternal(stmt)
}

// executeInsertInternal performs the actual INSERT operation.
func (e *Executor) executeInsertInternal(stmt *sql.InsertStmt) (*Result, error) {
	tableName := stmt.Table
	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	// Check if this is a view and get updatable view info
	var viewInfo *UpdatableViewInfo
	var baseTableName string = tableName

	viewInfo, err := e.getUpdatableViewInfo(tableName)
	if err != nil {
		return nil, fmt.Errorf("view check error: %w", err)
	}

	if viewInfo != nil {
		// This is an updatable view
		baseTableName = viewInfo.BaseTableName
	} else if e.engine.ViewExists(tableName) {
		// This is a non-updatable view
		return nil, fmt.Errorf("view '%s' is not updatable", tableName)
	}

	// Check if base table exists
	if !e.engine.TableOrTempExists(baseTableName) {
		return nil, fmt.Errorf("table %s does not exist", baseTableName)
	}

	// Get table info for column mapping
	tbl, _, err := e.engine.GetTableOrTemp(baseTableName)
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

	// If inserting through a view, map view columns to base table columns
	if viewInfo != nil {
		mappedColOrder := make([]string, len(colOrder))
		for i, colName := range colOrder {
			found := false
			for j, viewCol := range viewInfo.ViewColumns {
				if strings.EqualFold(viewCol, colName) {
					if viewInfo.BaseTableCols[j] != "" {
						mappedColOrder[i] = viewInfo.BaseTableCols[j]
						found = true
					}
					break
				}
			}
			if !found {
				// Use the original column name (might be a base table column)
				mappedColOrder[i] = colName
			}
		}
		colOrder = mappedColOrder
	}

	// Build column index map
	colIdxMap := make(map[string]int)
	for i, col := range tblInfo.Columns {
		colIdxMap[strings.ToLower(col.Name)] = i
	}

	var totalAffected int
	var lastInsertID uint64
	var returningRows [][]interface{}
	var returningCols []ColumnInfo

	// Setup returning columns if needed
	if stmt.Returning != nil {
		if stmt.Returning.All || len(stmt.Returning.Columns) == 0 {
			// RETURNING *
			for _, col := range tblInfo.Columns {
				returningCols = append(returningCols, ColumnInfo{
					Name: col.Name,
					Type: col.Type.String(),
				})
			}
		} else {
			// Specific columns
			for _, expr := range stmt.Returning.Columns {
				if colRef, ok := expr.(*sql.ColumnRef); ok {
					colName := colRef.Name
					for _, col := range tblInfo.Columns {
						if strings.EqualFold(col.Name, colName) {
							returningCols = append(returningCols, ColumnInfo{
								Name: col.Name,
								Type: col.Type.String(),
							})
							break
						}
					}
				}
			}
		}
	}

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

		// Compute STORED generated columns
		for i, col := range tblInfo.Columns {
			if col.GeneratedExpr != "" && col.GeneratedStored {
				// Parse and evaluate the expression
				p := sql.NewParser(col.GeneratedExpr)
				expr := p.ParseExpression()
				if p.Error() != nil {
					return nil, fmt.Errorf("invalid generated column expression: %w", p.Error())
				}

				// Build a temporary row for evaluation
				tempRow := &row.Row{Values: values}
				columnMap := make(map[string]*types.ColumnInfo)
				for j, c := range tblInfo.Columns {
					columnMap[strings.ToLower(c.Name)] = tblInfo.Columns[j]
				}

				// Evaluate the expression
				val, err := e.evaluateExpression(expr, tempRow, columnMap, tblInfo.Columns)
				if err != nil {
					return nil, fmt.Errorf("error evaluating generated column %s: %w", col.Name, err)
				}

				values[i] = e.interfaceToValue(val, col)
			}
		}

		// Handle auto-increment columns - mark as NULL so table layer can generate value
		for i, col := range tblInfo.Columns {
			if col.AutoIncr && values[i].Null {
				// Keep as NULL - table layer will generate the sequence value
			}
		}

		// Validate NOT NULL constraints
		for i, col := range tblInfo.Columns {
			if !col.Nullable && values[i].Null {
				return nil, fmt.Errorf("column '%s' cannot be null", col.Name)
			}
		}

		// Check for conflict (UNIQUE constraint or PRIMARY KEY violation)
		var conflictIndex int = -1
		for i, col := range tblInfo.Columns {
			if (col.Unique || col.PrimaryKey) && !values[i].Null {
				if e.valueExistsInColumn(tbl, i, values[i]) {
					conflictIndex = i
					break
				}
			}
		}

		// Handle UPSERT (ON CONFLICT)
		if conflictIndex >= 0 && stmt.OnConflict != nil {
			if stmt.OnConflict.DoNothing {
				// ON CONFLICT DO NOTHING - skip this row
				continue
			} else if stmt.OnConflict.DoUpdate {
				// ON CONFLICT DO UPDATE - perform update instead
				updatedRow, err := e.performUpsertUpdate(tbl, tblInfo, values, stmt.OnConflict, colIdxMap)
				if err != nil {
					return nil, err
				}
				totalAffected++
				if stmt.Returning != nil {
					returningRows = append(returningRows, updatedRow)
				}
				continue
			}
		}

		// If conflict but no ON CONFLICT clause, return error
		if conflictIndex >= 0 {
			col := tblInfo.Columns[conflictIndex]
			return nil, fmt.Errorf("duplicate entry '%s' for key '%s'", values[conflictIndex].String(), col.Name)
		}

		// Validate CHECK constraints
		if err := e.validateCheckConstraints(tbl, values); err != nil {
			return nil, err
		}

		// Validate CHECK OPTION for view inserts
		if viewInfo != nil && viewInfo.CheckOption != "" {
			// Build values map for CHECK OPTION validation
			valuesMap := make(map[string]interface{})
			for i, col := range tblInfo.Columns {
				if !values[i].Null {
					valuesMap[strings.ToLower(col.Name)] = e.valueToInterface(values[i])
				}
			}
			if err := e.validateCheckOption(viewInfo, valuesMap, "INSERT"); err != nil {
				return nil, err
			}
		}

		// Validate FOREIGN KEY constraints
		if err := e.validateForeignKeys(tbl, values, false); err != nil {
			return nil, err
		}

		// Fire BEFORE INSERT triggers
		if err := e.fireTriggers(tableName, 0, 0, nil); err != nil {
			return nil, err
		}

		// Insert the row (use tbl directly to support temp tables)
		rowID, err := tbl.Insert(values)
		if err != nil {
			return nil, fmt.Errorf("insert error: %w", err)
		}

		// Fire AFTER INSERT triggers
		if err := e.fireTriggers(tableName, 1, 0, nil); err != nil {
			return nil, err
		}

		// Update FTS indexes
		if e.ftsManager != nil {
			ftsIndexes := e.engine.GetCatalog().GetFTSIndexesForTable(tableName)
			if len(ftsIndexes) > 0 {
				valuesMap := make(map[string]interface{})
				for i, col := range tblInfo.Columns {
					valuesMap[strings.ToLower(col.Name)] = e.valueToInterface(values[i])
				}
				for _, ftsInfo := range ftsIndexes {
					if idx, err := e.ftsManager.GetIndex(ftsInfo.Name); err == nil {
						idx.IndexDocument(uint64(rowID), valuesMap)
					}
				}
				// Save FTS index to disk
				e.ftsManager.SaveAll()
			}
		}

		totalAffected++
		lastInsertID = uint64(rowID)

		// Collect returning data
		if stmt.Returning != nil {
			row := make([]interface{}, len(returningCols))
			for i, col := range returningCols {
				for j, tc := range tblInfo.Columns {
					if tc.Name == col.Name {
						row[i] = e.valueToInterface(values[j])
						break
					}
				}
			}
			// If auto-increment column, use the generated rowID
			for _, col := range tblInfo.Columns {
				if col.AutoIncr {
					for j, rc := range returningCols {
						if rc.Name == col.Name {
							row[j] = int64(rowID)
							break
						}
					}
				}
			}
			returningRows = append(returningRows, row)
		}
	}

	e.lastInsertID = int64(lastInsertID)
	e.lastRowCount = int64(totalAffected)

	// Return result with optional RETURNING data
	result := &Result{
		Affected:   totalAffected,
		LastInsert: lastInsertID,
		Message:    "OK",
	}
	if stmt.Returning != nil && len(returningRows) > 0 {
		result.Columns = returningCols
		result.Rows = returningRows
		result.RowCount = len(returningRows)
	}

	return result, nil
}

// performUpsertUpdate performs the DO UPDATE part of an UPSERT
func (e *Executor) performUpsertUpdate(tbl *table.Table, tblInfo *table.TableInfo, values []types.Value, upsert *sql.UpsertClause, colIdxMap map[string]int) ([]interface{}, error) {
	// Find the conflicting row (we need to update it)
	// For simplicity, we'll find the row by the unique value
	var existingRow []types.Value
	var found bool
	var foundRowID int64

	for _, col := range tblInfo.Columns {
		if col.Unique || col.PrimaryKey {
			// Scan table to find matching row
			rows, err := tbl.Scan()
			if err != nil {
				return nil, err
			}

			for _, r := range rows {
				rowVals := r.Values
				for i, v := range values {
					if !v.Null && len(rowVals) > i && e.valuesEqual(rowVals[i], v) {
						existingRow = make([]types.Value, len(rowVals))
						copy(existingRow, rowVals)
						foundRowID = int64(r.ID)
						found = true
						break
					}
				}
				if found {
					break
				}
			}
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("conflicting row not found for upsert")
	}

	// Build update values from DO UPDATE SET assignments
	updatedValues := make(map[int]types.Value)

	for _, assign := range upsert.Assignments {
		idx, ok := colIdxMap[strings.ToLower(assign.Column)]
		if !ok {
			return nil, fmt.Errorf("unknown column in upsert: %s", assign.Column)
		}

		// Evaluate the assignment expression
		// Support excluded.* references for the new values
		switch expr := assign.Value.(type) {
		case *sql.ColumnRef:
			// Support referencing "excluded" table (the new values being inserted)
			if strings.EqualFold(expr.Name, "excluded") {
				// This would need excluded.column syntax - for now use the new value
				updatedValues[idx] = values[idx]
			} else {
				// Reference to existing column value (for expressions like counter = counter + 1)
				// For now, just keep existing value
				updatedValues[idx] = existingRow[idx]
			}
		default:
			// Evaluate expression using the standard method
			val, err := e.expressionToValue(assign.Value, tblInfo.Columns[idx])
			if err != nil {
				return nil, err
			}
			updatedValues[idx] = val
		}
	}

	// Build predicate to match the specific row
	predicate := func(r *row.Row) bool {
		return int64(r.ID) == foundRowID
	}

	// Perform the update
	_, err := tbl.Update(predicate, updatedValues)
	if err != nil {
		return nil, err
	}

	// Build the result row with updated values
	resultRow := make([]interface{}, len(existingRow))
	for i, v := range existingRow {
		if newVal, ok := updatedValues[i]; ok {
			resultRow[i] = e.valueToInterface(newVal)
		} else {
			resultRow[i] = e.valueToInterface(v)
		}
	}

	return resultRow, nil
}

// valuesEqual compares two values for equality
func (e *Executor) valuesEqual(a, b types.Value) bool {
	if a.Null != b.Null {
		return false
	}
	if a.Null {
		return true
	}
	return fmt.Sprintf("%v", a.Data) == fmt.Sprintf("%v", b.Data)
}

// executeUpdate executes an UPDATE statement.
func (e *Executor) executeUpdate(stmt *sql.UpdateStmt) (*Result, error) {
	// Handle WITH clause if present
	if stmt.WithClause != nil {
		// Initialize CTE results map if needed
		if e.cteResults == nil {
			e.cteResults = make(map[string]*Result)
		}

		// Save current CTE results to restore later
		oldCTEResults := e.cteResults
		e.cteResults = make(map[string]*Result)
		for k, v := range oldCTEResults {
			e.cteResults[k] = v
		}

		// Execute each CTE and store the results
		for _, cte := range stmt.WithClause.CTEs {
			var cteResult *Result
			var err error
			if cte.Recursive {
				cteResult, err = e.executeRecursiveCTE(cte)
			} else {
				cteResult, err = e.executeStatementForCTE(cte.Query)
			}
			if err != nil {
				e.cteResults = oldCTEResults
				return nil, fmt.Errorf("CTE '%s' error: %w", cte.Name, err)
			}

			// Validate CTE result
			if cteResult == nil {
				e.cteResults = oldCTEResults
				return nil, fmt.Errorf("CTE '%s' returned nil result", cte.Name)
			}

			// Store the CTE result
			e.cteResults[strings.ToLower(cte.Name)] = cteResult
		}

		// Execute the UPDATE statement (can reference CTEs now)
		result, err := e.executeUpdateInternal(stmt)

		// Restore original CTE results
		e.cteResults = oldCTEResults

		return result, err
	}

	return e.executeUpdateInternal(stmt)
}

// executeUpdateInternal performs the actual UPDATE operation.
func (e *Executor) executeUpdateInternal(stmt *sql.UpdateStmt) (*Result, error) {
	tableName := stmt.Table
	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	// Check if this is a view and get updatable view info
	var viewInfo *UpdatableViewInfo
	var baseTableName string = tableName

	viewInfo, err := e.getUpdatableViewInfo(tableName)
	if err != nil {
		return nil, fmt.Errorf("view check error: %w", err)
	}

	if viewInfo != nil {
		// This is an updatable view
		baseTableName = viewInfo.BaseTableName
	} else if e.engine.ViewExists(tableName) {
		// This is a non-updatable view
		return nil, fmt.Errorf("view '%s' is not updatable", tableName)
	}

	// Check if base table exists
	if !e.engine.TableOrTempExists(baseTableName) {
		return nil, fmt.Errorf("table %s does not exist", baseTableName)
	}

	// Get table info
	tbl, _, err := e.engine.GetTableOrTemp(baseTableName)
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

	// Collect returning data before update
	var returningRows [][]interface{}
	var returningCols []ColumnInfo

	if stmt.Returning != nil {
		// Setup returning columns
		if stmt.Returning.All || len(stmt.Returning.Columns) == 0 {
			for _, col := range tblInfo.Columns {
				returningCols = append(returningCols, ColumnInfo{
					Name: col.Name,
					Type: col.Type.String(),
				})
			}
		} else {
			for _, expr := range stmt.Returning.Columns {
				if colRef, ok := expr.(*sql.ColumnRef); ok {
					colName := colRef.Name
					for _, col := range tblInfo.Columns {
						if strings.EqualFold(col.Name, colName) {
							returningCols = append(returningCols, ColumnInfo{
								Name: col.Name,
								Type: col.Type.String(),
							})
							break
						}
					}
				}
			}
		}

		// Collect rows that will be updated (with new values)
		rows, err := tbl.Scan()
		if err == nil {
			for _, r := range rows {
				if predicate(r) {
					oldVals := r.Values
					newVals := make([]types.Value, len(oldVals))
					copy(newVals, oldVals)

					// Apply updates
					for idx, val := range updates {
						newVals[idx] = val
					}

					// Build returning row
					row := make([]interface{}, len(returningCols))
					for i, col := range returningCols {
						for j, tc := range tblInfo.Columns {
							if tc.Name == col.Name {
								row[i] = e.valueToInterface(newVals[j])
								break
							}
						}
					}
					returningRows = append(returningRows, row)
				}
			}
		}
	}

	// Fire BEFORE UPDATE triggers
	if err := e.fireTriggers(tableName, 0, 1, nil); err != nil {
		return nil, err
	}

	// Handle foreign key ON UPDATE cascade
	// Find primary key column
	pkColIdx := -1
	pkColName := ""
	for i, col := range tblInfo.Columns {
		if col.PrimaryKey {
			pkColIdx = i
			pkColName = col.Name
			break
		}
	}

	// If updating primary key, handle cascade
	if pkColIdx >= 0 && pkColName != "" {
		if oldVal, hasOld := updates[pkColIdx]; hasOld {
			// Get current rows to find old values
			currentRows, err := tbl.Scan()
			if err == nil {
				for _, r := range currentRows {
					if predicate(r) && pkColIdx < len(r.Values) {
						// Handle FK cascade for this row's old value -> new value
						if err := e.handleForeignKeyOnUpdate(tableName, r.Values[pkColIdx], oldVal, pkColName); err != nil {
							return nil, err
						}
					}
				}
			}
		}
	}

	// Execute update
	var affected int

	// If updating through a view with CHECK OPTION, validate each row
	if viewInfo != nil && viewInfo.CheckOption != "" {
		// Get all rows and validate CHECK OPTION
		rows, err := tbl.Scan()
		if err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}

		for _, r := range rows {
			if !predicate(r) {
				continue
			}

			// Build new values after update
			newVals := make([]types.Value, len(r.Values))
			copy(newVals, r.Values)
			for idx, val := range updates {
				newVals[idx] = val
			}

			// Build values map for CHECK OPTION validation
			valuesMap := make(map[string]interface{})
			for i, col := range tblInfo.Columns {
				if !newVals[i].Null {
					valuesMap[strings.ToLower(col.Name)] = e.valueToInterface(newVals[i])
				}
			}

			// Validate CHECK OPTION
			if err := e.validateCheckOption(viewInfo, valuesMap, "UPDATE"); err != nil {
				return nil, err
			}
		}
	}

	affected, err = tbl.Update(predicate, updates)
	if err != nil {
		return nil, fmt.Errorf("update error: %w", err)
	}

	// Fire AFTER UPDATE triggers
	if err := e.fireTriggers(tableName, 1, 1, nil); err != nil {
		return nil, err
	}

	// Update FTS indexes
	if e.ftsManager != nil && affected > 0 {
		ftsIndexes := e.engine.GetCatalog().GetFTSIndexesForTable(tableName)
		if len(ftsIndexes) > 0 {
			// Re-scan and update FTS for affected rows
			rows, err := tbl.Scan()
			if err == nil {
				for _, r := range rows {
					if predicate(r) {
						valuesMap := make(map[string]interface{})
						for i, col := range tblInfo.Columns {
							valuesMap[strings.ToLower(col.Name)] = e.valueToInterface(r.Values[i])
						}
						for _, ftsInfo := range ftsIndexes {
							if idx, err := e.ftsManager.GetIndex(ftsInfo.Name); err == nil {
								idx.UpdateDocument(uint64(r.ID), valuesMap)
							}
						}
					}
				}
			}
		}
	}

	e.lastRowCount = int64(affected)

	result := &Result{
		Affected: affected,
		Message:  fmt.Sprintf("Query OK, %d rows affected", affected),
	}

	if stmt.Returning != nil && len(returningRows) > 0 {
		result.Columns = returningCols
		result.Rows = returningRows
		result.RowCount = len(returningRows)
	}

	return result, nil
}

// executeDelete executes a DELETE statement.
func (e *Executor) executeDelete(stmt *sql.DeleteStmt) (*Result, error) {
	// Handle WITH clause if present
	if stmt.WithClause != nil {
		// Initialize CTE results map if needed
		if e.cteResults == nil {
			e.cteResults = make(map[string]*Result)
		}

		// Save current CTE results to restore later
		oldCTEResults := e.cteResults
		e.cteResults = make(map[string]*Result)
		for k, v := range oldCTEResults {
			e.cteResults[k] = v
		}

		// Execute each CTE and store the results
		for _, cte := range stmt.WithClause.CTEs {
			var cteResult *Result
			var err error
			if cte.Recursive {
				cteResult, err = e.executeRecursiveCTE(cte)
			} else {
				cteResult, err = e.executeStatementForCTE(cte.Query)
			}
			if err != nil {
				e.cteResults = oldCTEResults
				return nil, fmt.Errorf("CTE '%s' error: %w", cte.Name, err)
			}

			// Validate CTE result
			if cteResult == nil {
				e.cteResults = oldCTEResults
				return nil, fmt.Errorf("CTE '%s' returned nil result", cte.Name)
			}

			// Store the CTE result
			e.cteResults[strings.ToLower(cte.Name)] = cteResult
		}

		// Execute the DELETE statement (can reference CTEs now)
		result, err := e.executeDeleteInternal(stmt)

		// Restore original CTE results
		e.cteResults = oldCTEResults

		return result, err
	}

	return e.executeDeleteInternal(stmt)
}

// executeDeleteInternal performs the actual DELETE operation.
func (e *Executor) executeDeleteInternal(stmt *sql.DeleteStmt) (*Result, error) {
	tableName := stmt.Table
	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	// Check if table exists
	if !e.engine.TableOrTempExists(tableName) {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Get table
	tbl, _, err := e.engine.GetTableOrTemp(tableName)
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

		// Collect returning data before truncate
		var returningRows [][]interface{}
		var returningCols []ColumnInfo

		if stmt.Returning != nil {
			// Setup returning columns
			if stmt.Returning.All || len(stmt.Returning.Columns) == 0 {
				for _, col := range tblInfo.Columns {
					returningCols = append(returningCols, ColumnInfo{
						Name: col.Name,
						Type: col.Type.String(),
					})
				}
			} else {
				for _, expr := range stmt.Returning.Columns {
					if colRef, ok := expr.(*sql.ColumnRef); ok {
						colName := colRef.Name
						for _, col := range tblInfo.Columns {
							if strings.EqualFold(col.Name, colName) {
								returningCols = append(returningCols, ColumnInfo{
									Name: col.Name,
									Type: col.Type.String(),
								})
								break
							}
						}
					}
				}
			}

			// Collect all rows
			rows, err := tbl.Scan()
			if err == nil {
				for _, r := range rows {
					rowVals := r.Values
					row := make([]interface{}, len(returningCols))
					for i, col := range returningCols {
						for j, tc := range tblInfo.Columns {
							if tc.Name == col.Name {
								row[i] = e.valueToInterface(rowVals[j])
								break
							}
						}
					}
					returningRows = append(returningRows, row)
				}
			}
		}

		// Fire BEFORE DELETE triggers
		if err := e.fireTriggers(tableName, 0, 2, nil); err != nil {
			return nil, err
		}

		if err := tbl.Truncate(); err != nil {
			return nil, fmt.Errorf("delete error: %w", err)
		}

		// Fire AFTER DELETE triggers
		if err := e.fireTriggers(tableName, 1, 2, nil); err != nil {
			return nil, err
		}

		// Clear FTS indexes for the table
		if e.ftsManager != nil {
			e.ftsManager.DropIndexForTable(tableName)
		}

		e.lastRowCount = int64(rowCount)
		result := &Result{
			Affected: int(rowCount),
			Message:  "Query OK, all rows deleted",
		}
		if stmt.Returning != nil && len(returningRows) > 0 {
			result.Columns = returningCols
			result.Rows = returningRows
			result.RowCount = len(returningRows)
		}
		return result, nil
	}

	// Collect returning data before delete
	var returningRows [][]interface{}
	var returningCols []ColumnInfo

	if stmt.Returning != nil {
		// Setup returning columns
		if stmt.Returning.All || len(stmt.Returning.Columns) == 0 {
			for _, col := range tblInfo.Columns {
				returningCols = append(returningCols, ColumnInfo{
					Name: col.Name,
					Type: col.Type.String(),
				})
			}
		} else {
			for _, expr := range stmt.Returning.Columns {
				if colRef, ok := expr.(*sql.ColumnRef); ok {
					colName := colRef.Name
					for _, col := range tblInfo.Columns {
						if strings.EqualFold(col.Name, colName) {
							returningCols = append(returningCols, ColumnInfo{
								Name: col.Name,
								Type: col.Type.String(),
							})
							break
						}
					}
				}
			}
		}

		// Collect rows that will be deleted
		rows, err := tbl.Scan()
		if err == nil {
			for _, r := range rows {
				if predicate(r) {
					rowVals := r.Values
					row := make([]interface{}, len(returningCols))
					for i, col := range returningCols {
						for j, tc := range tblInfo.Columns {
							if tc.Name == col.Name {
								row[i] = e.valueToInterface(rowVals[j])
								break
							}
						}
					}
					returningRows = append(returningRows, row)
				}
			}
		}
	}

	// Collect row IDs for FTS index cleanup
	var rowIDsToDelete []uint64
	if e.ftsManager != nil {
		ftsIndexes := e.engine.GetCatalog().GetFTSIndexesForTable(tableName)
		if len(ftsIndexes) > 0 {
			rows, err := tbl.Scan()
			if err == nil {
				for _, r := range rows {
					if predicate(r) {
						rowIDsToDelete = append(rowIDsToDelete, uint64(r.ID))
					}
				}
			}
		}
	}

	// Collect rows to delete and handle foreign key cascade
	rows, err := tbl.Scan()
	if err != nil {
		return nil, err
	}

	// Build column index map for FK reference columns
	// Find primary key columns or use first column
	pkColIdx := 0
	pkColName := ""
	for i, col := range tblInfo.Columns {
		if col.PrimaryKey {
			pkColIdx = i
			pkColName = col.Name
			break
		}
	}
	if pkColName == "" && len(tblInfo.Columns) > 0 {
		pkColName = tblInfo.Columns[0].Name
	}

	// Handle foreign key ON DELETE actions for each row
	for _, r := range rows {
		if predicate(r) {
			if pkColIdx < len(r.Values) {
				if err := e.handleForeignKeyOnDelete(tableName, r.Values[pkColIdx], pkColName); err != nil {
					return nil, err
				}
			}
		}
	}

	// Fire BEFORE DELETE triggers
	if err := e.fireTriggers(tableName, 0, 2, nil); err != nil {
		return nil, err
	}

	// Execute delete
	affected, err := tbl.Delete(predicate)
	if err != nil {
		return nil, fmt.Errorf("delete error: %w", err)
	}

	// Fire AFTER DELETE triggers
	if err := e.fireTriggers(tableName, 1, 2, nil); err != nil {
		return nil, err
	}

	// Remove deleted rows from FTS indexes
	if e.ftsManager != nil && len(rowIDsToDelete) > 0 {
		for _, docID := range rowIDsToDelete {
			e.ftsManager.RemoveDocument(tableName, docID)
		}
	}

	e.lastRowCount = int64(affected)

	result := &Result{
		Affected: affected,
		Message:  fmt.Sprintf("Query OK, %d rows affected", affected),
	}

	if stmt.Returning != nil && len(returningRows) > 0 {
		result.Columns = returningCols
		result.Rows = returningRows
		result.RowCount = len(returningRows)
	}

	return result, nil
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
		return e.compareValues(left, ex.Op, right, ex.EscapeChar)

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

	case *sql.MatchExpr:
		// For FTS matching, check if the row ID matches FTS results
		if e.ftsManager == nil {
			fmt.Printf("FTS: ftsManager is nil\n")
			return false, nil
		}

		// Get FTS indexes for the table
		ftsIndexes := e.engine.GetCatalog().GetFTSIndexesForTable(ex.Table)
		fmt.Printf("FTS: table='%s', found %d indexes for table\n", ex.Table, len(ftsIndexes))
		if len(ftsIndexes) == 0 {
			// Try listing all FTS indexes for debugging
			allIndexes := e.engine.GetCatalog().ListFTSIndexes()
			fmt.Printf("FTS: all available indexes: %v\n", allIndexes)
			return false, nil
		}

		// Search using the first matching index
		for _, ftsInfo := range ftsIndexes {
			fmt.Printf("FTS: searching index %s for query '%s'\n", ftsInfo.Name, ex.Query)
			results, err := e.ftsManager.Search(ftsInfo.Name, ex.Query)
			if err != nil {
				fmt.Printf("FTS: search error: %v\n", err)
				continue
			}
			fmt.Printf("FTS: found %d results\n", len(results))
			// Check if this row's ID is in the results
			rowID := uint64(r.ID)
			for _, result := range results {
				if result.DocID == rowID {
					return true, nil
				}
			}
		}
		return false, nil
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
	// For temp tables, check only temp table existence
	if stmt.Temp {
		if e.engine.TempTableExists(stmt.TableName) {
			if stmt.IfNotExists {
				return &Result{Message: "OK"}, nil
			}
			return nil, fmt.Errorf("temp table %s already exists", stmt.TableName)
		}
	} else {
		// Check if table already exists (both regular and temp)
		if e.engine.TableOrTempExists(stmt.TableName) {
			if stmt.IfNotExists {
				return &Result{Message: "OK"}, nil
			}
			return nil, fmt.Errorf("table %s already exists", stmt.TableName)
		}
	}

	// Convert column definitions
	columns := make([]*types.ColumnInfo, len(stmt.Columns))
	for i, colDef := range stmt.Columns {
		colType := types.ParseTypeID(colDef.Type.Name)
		if colType == types.TypeNull {
			return nil, fmt.Errorf("unknown type: %s", colDef.Type.Name)
		}

		col := &types.ColumnInfo{
			Name:            colDef.Name,
			Type:            colType,
			Size:            colDef.Type.Size,
			Precision:       colDef.Type.Precision,
			Scale:           colDef.Type.Scale,
			Nullable:        colDef.Nullable,
			PrimaryKey:      colDef.PrimaryKey,
			AutoIncr:        colDef.AutoIncr || colType == types.TypeSeq, // SEQ type is auto-increment
			Unique:          colDef.Unique,
			GeneratedStored: colDef.GeneratedStored,
		}

		// Store generated column expression as string
		if colDef.GeneratedExpr != nil {
			col.GeneratedExpr = colDef.GeneratedExpr.String()
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
	if stmt.Temp {
		if err := e.engine.CreateTempTable(stmt.TableName, columns); err != nil {
			return nil, fmt.Errorf("create temp table error: %w", err)
		}
	} else {
		if err := e.engine.CreateTable(stmt.TableName, columns); err != nil {
			return nil, fmt.Errorf("create table error: %w", err)
		}
	}

	// Get the table and add constraints
	tbl, _, err := e.engine.GetTableOrTemp(stmt.TableName)
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
	// Check if table exists (both regular and temp)
	if !e.engine.TableOrTempExists(stmt.TableName) {
		if stmt.IfExists {
			return &Result{Message: "OK"}, nil
		}
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	if err := e.engine.DropTableOrTemp(stmt.TableName); err != nil {
		return nil, fmt.Errorf("drop table error: %w", err)
	}

	return &Result{Message: "OK"}, nil
}

// executeCreateIndex executes a CREATE INDEX statement.
func (e *Executor) executeCreateIndex(stmt *sql.CreateIndexStmt) (*Result, error) {
	// Check if table exists
	if !e.engine.TableOrTempExists(stmt.TableName) {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	// Get the table
	tbl, _, err := e.engine.GetTableOrTemp(stmt.TableName)
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
	if !e.engine.TableOrTempExists(stmt.TableName) {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	// Get the table
	tbl, _, err := e.engine.GetTableOrTemp(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Drop the index
	if err := tbl.DropIndex(stmt.IndexName); err != nil {
		return nil, fmt.Errorf("drop index error: %w", err)
	}

	return &Result{Message: "OK"}, nil
}

// executeCreateFTS executes a CREATE FTS INDEX statement.
func (e *Executor) executeCreateFTS(stmt *sql.CreateFTSStmt) (*Result, error) {
	tableName := strings.ToLower(stmt.TableName)

	// Check if table exists
	if !e.engine.TableOrTempExists(tableName) {
		return nil, fmt.Errorf("table '%s' does not exist", stmt.TableName)
	}

	// Check if FTS index already exists
	if e.engine.GetCatalog().FTSIndexExists(stmt.IndexName) {
		if stmt.IfNotExists {
			return &Result{Message: "OK"}, nil
		}
		return nil, fmt.Errorf("FTS index '%s' already exists", stmt.IndexName)
	}

	// Set default tokenizer
	tokenizer := stmt.Tokenizer
	if tokenizer == "" {
		tokenizer = "simple"
	}

	// Create FTS index in catalog
	if err := e.engine.GetCatalog().CreateFTSIndex(stmt.IndexName, tableName, stmt.Columns, tokenizer); err != nil {
		return nil, fmt.Errorf("create FTS index error: %w", err)
	}

	// Create the actual FTS index
	if e.ftsManager != nil {
		idx, err := e.ftsManager.CreateIndex(stmt.IndexName, tableName, stmt.Columns, tokenizer)
		if err != nil {
			return nil, fmt.Errorf("create FTS index error: %w", err)
		}

		// Index existing data in the table
		tbl, _, err := e.engine.GetTableOrTemp(tableName)
		if err != nil {
			return nil, err
		}

		rows, err := tbl.Scan()
		if err != nil {
			return nil, err
		}

		for _, row := range rows {
			values := make(map[string]interface{})
			for i, col := range tbl.Columns() {
				if i < len(row.Values) {
					values[strings.ToLower(col.Name)] = e.valueToInterface(row.Values[i])
				}
			}
			if err := idx.IndexDocument(uint64(row.ID), values); err != nil {
				return nil, err
			}
		}
	}

	return &Result{Message: "OK"}, nil
}

// executeDropFTS executes a DROP FTS INDEX statement.
func (e *Executor) executeDropFTS(stmt *sql.DropFTSStmt) (*Result, error) {
	// Check if FTS index exists
	if !e.engine.GetCatalog().FTSIndexExists(stmt.IndexName) {
		if stmt.IfExists {
			return &Result{Message: "OK"}, nil
		}
		return nil, fmt.Errorf("FTS index '%s' does not exist", stmt.IndexName)
	}

	// Drop from FTS manager
	if e.ftsManager != nil {
		if err := e.ftsManager.DropIndex(stmt.IndexName); err != nil {
			return nil, fmt.Errorf("drop FTS index error: %w", err)
		}
	}

	// Drop from catalog
	if err := e.engine.GetCatalog().DropFTSIndex(stmt.IndexName); err != nil {
		return nil, fmt.Errorf("drop FTS index error: %w", err)
	}

	return &Result{Message: "OK"}, nil
}

// GetFTSManager returns the FTS manager for external use.
func (e *Executor) GetFTSManager() *fts.FTSManager {
	return e.ftsManager
}

// UpdatableViewInfo contains information about an updatable view.
type UpdatableViewInfo struct {
	BaseTableName  string
	BaseTableCols  []string // Column names in the base table
	ViewColumns    []string // Column names in the view (mapped to base)
	WhereClause    sql.Expression
	CheckOption    string // "CASCADED", "LOCAL", or ""
	UnderlyingView *UpdatableViewInfo // For nested views
}

// getUpdatableViewInfo checks if a name refers to an updatable view
// and returns information needed for INSERT/UPDATE/DELETE operations.
func (e *Executor) getUpdatableViewInfo(name string) (*UpdatableViewInfo, error) {
	// Check if it's a view
	if !e.engine.ViewExists(name) {
		return nil, nil // Not a view
	}

	viewInfo, err := e.engine.GetView(name)
	if err != nil {
		return nil, err
	}

	// Parse the view's query
	parser := sql.NewParser(viewInfo.Query)
	selectStmt, err := parser.Parse()
	if err != nil {
		return nil, fmt.Errorf("view query parse error: %w", err)
	}

	selectNode, ok := selectStmt.(*sql.SelectStmt)
	if !ok {
		return nil, nil // Not a simple SELECT
	}

	// Check if it's a simple updatable view (single table, no GROUP BY, no DISTINCT, no aggregates)
	if selectNode.From == nil || selectNode.From.Table == nil {
		return nil, nil // No FROM clause
	}

	// Check for JOINs
	if len(selectNode.From.Joins) > 0 {
		return nil, nil // Has joins
	}

	// Check for subquery in FROM
	if selectNode.From.Table.Subquery != nil {
		return nil, nil // Subquery in FROM
	}

	baseTableName := selectNode.From.Table.Name
	if baseTableName == "" {
		return nil, nil
	}

	// Check if base table exists (could be another view)
	var underlyingView *UpdatableViewInfo
	if e.engine.ViewExists(baseTableName) {
		// Recursively get underlying view info
		underlyingView, err = e.getUpdatableViewInfo(baseTableName)
		if err != nil {
			return nil, err
		}
		if underlyingView == nil {
			return nil, nil // Underlying view is not updatable
		}
		baseTableName = underlyingView.BaseTableName
	} else if !e.engine.TableOrTempExists(selectNode.From.Table.Name) {
		return nil, nil // Base doesn't exist
	}

	// Check for GROUP BY, HAVING, aggregates
	if len(selectNode.GroupBy) > 0 || selectNode.Having != nil {
		return nil, nil
	}
	if selectNode.Distinct {
		return nil, nil
	}

	// Check for aggregates in SELECT
	for _, col := range selectNode.Columns {
		if hasAggregate(col) {
			return nil, nil
		}
	}

	// Get column mappings
	viewColumns := make([]string, 0, len(selectNode.Columns))
	baseTableCols := make([]string, 0, len(selectNode.Columns))

	for _, col := range selectNode.Columns {
		// Check for alias - use column string representation
		viewColumns = append(viewColumns, col.String())

		// Get the base column name
		if colRef, ok := col.(*sql.ColumnRef); ok {
			baseTableCols = append(baseTableCols, colRef.Name)
		} else {
			// Complex expression - not directly updatable
			baseTableCols = append(baseTableCols, "")
		}
	}

	// Determine effective check option
	checkOption := viewInfo.CheckOption

	return &UpdatableViewInfo{
		BaseTableName:  baseTableName,
		BaseTableCols:  baseTableCols,
		ViewColumns:    viewColumns,
		WhereClause:    selectNode.Where,
		CheckOption:    checkOption,
		UnderlyingView: underlyingView,
	}, nil
}

// hasAggregate checks if an expression contains an aggregate function.
func hasAggregate(expr sql.Expression) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *sql.FunctionCall:
		switch strings.ToUpper(e.Name) {
		case "COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT":
			return true
		}
		for _, arg := range e.Args {
			if hasAggregate(arg) {
				return true
			}
		}
	case *sql.BinaryExpr:
		return hasAggregate(e.Left) || hasAggregate(e.Right)
	case *sql.UnaryExpr:
		return hasAggregate(e.Right)
	case *sql.CaseExpr:
		if hasAggregate(e.Expr) {
			return true
		}
		for _, when := range e.Whens {
			if hasAggregate(when.Condition) || hasAggregate(when.Result) {
				return true
			}
		}
		if hasAggregate(e.Else) {
			return true
		}
	}

	return false
}

// validateCheckOption validates that a row satisfies the view's WHERE clause.
// Returns an error if CHECK OPTION is set and the row doesn't match.
func (e *Executor) validateCheckOption(viewInfo *UpdatableViewInfo, rowValues map[string]interface{}, operation string) error {
	if viewInfo.CheckOption == "" {
		return nil
	}

	// For CASCADED, check all underlying views regardless of their CHECK OPTION
	// For LOCAL, only check underlying views that have their own CHECK OPTION
	if viewInfo.UnderlyingView != nil {
		if viewInfo.CheckOption == "CASCADED" {
			// CASCADED: Check all underlying views
			if err := e.validateCheckOptionRecursive(viewInfo.UnderlyingView, rowValues, operation); err != nil {
				return err
			}
		} else if viewInfo.UnderlyingView.CheckOption != "" {
			// LOCAL: Only check underlying views that have their own CHECK OPTION
			if err := e.validateCheckOption(viewInfo.UnderlyingView, rowValues, operation); err != nil {
				return err
			}
		}
	}

	// Check current view's WHERE clause
	if viewInfo.WhereClause != nil {
		// Evaluate the WHERE clause with the row values
		result, err := e.evaluateConditionWithValues(viewInfo.WhereClause, rowValues)
		if err != nil {
			return fmt.Errorf("check option evaluation error: %w", err)
		}

		if !result {
			return fmt.Errorf("CHECK OPTION violation: %s would not be visible through the view", operation)
		}
	}

	return nil
}

// validateCheckOptionRecursive checks all views in the hierarchy regardless of their CHECK OPTION settings.
// This is used for CASCADED check option.
func (e *Executor) validateCheckOptionRecursive(viewInfo *UpdatableViewInfo, rowValues map[string]interface{}, operation string) error {
	// Check underlying views first
	if viewInfo.UnderlyingView != nil {
		if err := e.validateCheckOptionRecursive(viewInfo.UnderlyingView, rowValues, operation); err != nil {
			return err
		}
	}

	// Check current view's WHERE clause
	if viewInfo.WhereClause != nil {
		result, err := e.evaluateConditionWithValues(viewInfo.WhereClause, rowValues)
		if err != nil {
			return fmt.Errorf("check option evaluation error: %w", err)
		}

		if !result {
			return fmt.Errorf("CHECK OPTION violation: %s would not be visible through the view", operation)
		}
	}

	return nil
}

// evaluateConditionWithValues evaluates a condition with a map of values.
func (e *Executor) evaluateConditionWithValues(expr sql.Expression, values map[string]interface{}) (bool, error) {
	// Evaluate the expression
	result, err := e.evaluateExprWithValues(expr, values)
	if err != nil {
		return false, err
	}

	if result == nil {
		return false, nil
	}

	switch v := result.(type) {
	case bool:
		return v, nil
	case int64:
		return v != 0, nil
	case float64:
		return v != 0, nil
	default:
		return false, fmt.Errorf("condition did not evaluate to boolean")
	}
}

// evaluateExprWithValues evaluates an expression with a map of values.
func (e *Executor) evaluateExprWithValues(expr sql.Expression, values map[string]interface{}) (interface{}, error) {
	if expr == nil {
		return nil, nil
	}

	switch ex := expr.(type) {
	case *sql.ColumnRef:
		colName := strings.ToLower(ex.Name)
		if val, ok := values[colName]; ok {
			return val, nil
		}
		return nil, fmt.Errorf("column %s not found in values", ex.Name)

	case *sql.Literal:
		switch ex.Type {
		case sql.LiteralString:
			if s, ok := ex.Value.(string); ok {
				return s, nil
			}
			return fmt.Sprintf("%v", ex.Value), nil
		case sql.LiteralNumber:
			switch v := ex.Value.(type) {
			case int64:
				return v, nil
			case float64:
				return v, nil
			case int:
				return int64(v), nil
			case string:
				if i, err := strconv.ParseInt(v, 10, 64); err == nil {
					return i, nil
				}
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					return f, nil
				}
			}
			return ex.Value, nil
		case sql.LiteralBool:
			if b, ok := ex.Value.(bool); ok {
				return b, nil
			}
			return false, nil
		case sql.LiteralNull:
			return nil, nil
		}
		return ex.Value, nil

	case *sql.BinaryExpr:
		left, err := e.evaluateExprWithValues(ex.Left, values)
		if err != nil {
			return nil, err
		}
		right, err := e.evaluateExprWithValues(ex.Right, values)
		if err != nil {
			return nil, err
		}
		return e.compareValues(left, ex.Op, right, ex.EscapeChar)

	case *sql.UnaryExpr:
		operand, err := e.evaluateExprWithValues(ex.Right, values)
		if err != nil {
			return nil, err
		}
		switch ex.Op {
		case sql.OpNeg:
			switch v := operand.(type) {
			case int64:
				return -v, nil
			case float64:
				return -v, nil
			}
		case sql.OpNot:
			if b, ok := operand.(bool); ok {
				return !b, nil
			}
		}
		return operand, nil

	case *sql.IsNullExpr:
		val, err := e.evaluateExprWithValues(ex.Expr, values)
		if err != nil {
			return nil, err
		}
		if ex.Not {
			return val != nil, nil
		}
		return val == nil, nil

	case *sql.InExpr:
		val, err := e.evaluateExprWithValues(ex.Expr, values)
		if err != nil {
			return nil, err
		}
		for _, listExpr := range ex.List {
			listVal, err := e.evaluateExprWithValues(listExpr, values)
			if err != nil {
				return nil, err
			}
			if compareEqual(val, listVal) {
				return true, nil
			}
		}
		return false, nil

	case *sql.ParenExpr:
		return e.evaluateExprWithValues(ex.Expr, values)
	}

	return nil, fmt.Errorf("unsupported expression type for CHECK OPTION evaluation: %T", expr)
}

// executeCreateView executes a CREATE VIEW statement.
func (e *Executor) executeCreateView(stmt *sql.CreateViewStmt) (*Result, error) {
	// Check if table or view already exists
	if e.engine.TableOrTempExists(stmt.ViewName) {
		return nil, fmt.Errorf("table '%s' already exists", stmt.ViewName)
	}
	if e.engine.ViewExists(stmt.ViewName) {
		if stmt.OrReplace {
			// Drop existing view first
			if err := e.engine.DropView(stmt.ViewName); err != nil {
				return nil, fmt.Errorf("drop existing view error: %w", err)
			}
		} else {
			return nil, fmt.Errorf("view '%s' already exists", stmt.ViewName)
		}
	}

	// Get the query string from the SELECT statement
	query := stmt.SelectStmt.String()

	// Create the view
	if err := e.engine.CreateView(stmt.ViewName, query, stmt.Columns, stmt.CheckOption); err != nil {
		return nil, fmt.Errorf("create view error: %w", err)
	}

	return &Result{Message: "OK"}, nil
}

// executeDropView executes a DROP VIEW statement.
func (e *Executor) executeDropView(stmt *sql.DropViewStmt) (*Result, error) {
	// Check if view exists
	if !e.engine.ViewExists(stmt.ViewName) {
		if stmt.IfExists {
			return &Result{Message: "OK"}, nil
		}
		return nil, fmt.Errorf("view '%s' does not exist", stmt.ViewName)
	}

	// Drop the view
	if err := e.engine.DropView(stmt.ViewName); err != nil {
		return nil, fmt.Errorf("drop view error: %w", err)
	}

	return &Result{Message: "OK"}, nil
}

// executeExplain executes an EXPLAIN statement.
func (e *Executor) executeExplain(stmt *sql.ExplainStmt) (*Result, error) {
	// Generate query plan
	plan := e.generateQueryPlan(stmt.Statement)

	result := &Result{
		Columns: []ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "parent", Type: "INT"},
			{Name: "notused", Type: "INT"},
			{Name: "detail", Type: "TEXT"},
		},
		Rows:    plan,
		Message: "OK",
	}

	return result, nil
}

// generateQueryPlan generates a query plan for a statement
func (e *Executor) generateQueryPlan(s sql.Statement) [][]interface{} {
	var rows [][]interface{}
	var id int

	switch stmt := s.(type) {
	case *sql.SelectStmt:
		rows = e.generateSelectPlan(stmt, &id, 0)

	case *sql.InsertStmt:
		id++
		detail := "INSERT INTO " + stmt.Table
		rows = append(rows, []interface{}{id, 0, 0, detail})

		// Show values being inserted
		if len(stmt.Values) > 0 {
			id++
			rows = append(rows, []interface{}{id, 1, 0, fmt.Sprintf("VALUES (%d rows)", len(stmt.Values))})
		}

		// Check for column list
		if len(stmt.Columns) > 0 {
			id++
			rows = append(rows, []interface{}{id, 1, 0, fmt.Sprintf("COLUMNS: %s", strings.Join(stmt.Columns, ", "))})
		}

		// Check for UPSERT
		if stmt.OnConflict != nil {
			id++
			if stmt.OnConflict.DoNothing {
				rows = append(rows, []interface{}{id, 1, 0, "ON CONFLICT DO NOTHING"})
			} else {
				rows = append(rows, []interface{}{id, 1, 0, "ON CONFLICT DO UPDATE"})
			}
		}

		// Check for RETURNING
		if stmt.Returning != nil {
			id++
			rows = append(rows, []interface{}{id, 1, 0, "RETURNING"})
		}

	case *sql.UpdateStmt:
		id++
		detail := "UPDATE TABLE " + stmt.Table

		// Add table info if exists
		if e.engine.TableOrTempExists(stmt.Table) {
			if tbl, _, err := e.engine.GetTableOrTemp(stmt.Table); err == nil {
				info := tbl.GetInfo()
				detail += fmt.Sprintf(" (~%d rows)", info.RowCount)
			}
		}
		rows = append(rows, []interface{}{id, 0, 0, detail})

		// Show columns being updated
		if len(stmt.Assignments) > 0 {
			var cols []string
			for _, a := range stmt.Assignments {
				cols = append(cols, a.Column)
			}
			id++
			rows = append(rows, []interface{}{id, 1, 0, fmt.Sprintf("SET: %s", strings.Join(cols, ", "))})
		}

		// Check for WHERE
		if stmt.Where != nil {
			id++
			whereDetail := "FILTER"
			// Check if index can be used
			if idxName := e.findIndexForWhere(stmt.Table, stmt.Where); idxName != "" {
				whereDetail += fmt.Sprintf(" (USING INDEX %s)", idxName)
			}
			rows = append(rows, []interface{}{id, 1, 0, whereDetail})
		}

		// Check for RETURNING
		if stmt.Returning != nil {
			id++
			rows = append(rows, []interface{}{id, 1, 0, "RETURNING"})
		}

	case *sql.DeleteStmt:
		id++
		detail := "DELETE FROM " + stmt.Table

		// Add table info if exists
		if e.engine.TableOrTempExists(stmt.Table) {
			if tbl, _, err := e.engine.GetTableOrTemp(stmt.Table); err == nil {
				info := tbl.GetInfo()
				detail += fmt.Sprintf(" (~%d rows)", info.RowCount)
			}
		}
		rows = append(rows, []interface{}{id, 0, 0, detail})

		// Check for WHERE
		if stmt.Where != nil {
			id++
			whereDetail := "FILTER"
			// Check if index can be used
			if idxName := e.findIndexForWhere(stmt.Table, stmt.Where); idxName != "" {
				whereDetail += fmt.Sprintf(" (USING INDEX %s)", idxName)
			}
			rows = append(rows, []interface{}{id, 1, 0, whereDetail})
		}

		// Check for RETURNING
		if stmt.Returning != nil {
			id++
			rows = append(rows, []interface{}{id, 1, 0, "RETURNING"})
		}

	case *sql.CreateTableStmt:
		id++
		detail := "CREATE TABLE " + stmt.TableName
		rows = append(rows, []interface{}{id, 0, 0, detail})

		// Show column count
		id++
		rows = append(rows, []interface{}{id, 1, 0, fmt.Sprintf("COLUMNS: %d", len(stmt.Columns))})

		// Show constraints
		if len(stmt.Constraints) > 0 {
			id++
			rows = append(rows, []interface{}{id, 1, 0, fmt.Sprintf("CONSTRAINTS: %d", len(stmt.Constraints))})
		}

	case *sql.DropTableStmt:
		id++
		rows = append(rows, []interface{}{id, 0, 0, "DROP TABLE " + stmt.TableName})

	case *sql.DropViewStmt:
		id++
		rows = append(rows, []interface{}{id, 0, 0, "DROP VIEW " + stmt.ViewName})

	case *sql.DropIndexStmt:
		id++
		rows = append(rows, []interface{}{id, 0, 0, "DROP INDEX " + stmt.IndexName})

	case *sql.CreateIndexStmt:
		id++
		detail := "CREATE INDEX " + stmt.IndexName + " ON " + stmt.TableName
		rows = append(rows, []interface{}{id, 0, 0, detail})

		// Show columns
		if len(stmt.Columns) > 0 {
			id++
			rows = append(rows, []interface{}{id, 1, 0, fmt.Sprintf("COLUMNS: %s", strings.Join(stmt.Columns, ", "))})
		}

	case *sql.WithStmt:
		// Handle CTE
		id++
		rows = append(rows, []interface{}{id, 0, 0, "WITH CLAUSE"})

		// Show CTE names
		for _, cte := range stmt.CTEs {
			id++
			cteDetail := fmt.Sprintf("CTE %s", cte.Name)
			if cte.Recursive {
				cteDetail += " (RECURSIVE)"
			}
			rows = append(rows, []interface{}{id, 1, 0, cteDetail})
		}

		// Show main query
		id++
		rows = append(rows, []interface{}{id, 0, 0, "MAIN QUERY"})

	case *sql.UnionStmt:
		id++
		rows = append(rows, []interface{}{id, 0, 0, "UNION"})

		// Show left side
		id++
		rows = append(rows, []interface{}{id, 1, 0, "LEFT QUERY"})
		leftRows := e.generateQueryPlan(stmt.Left)
		for _, r := range leftRows {
			r[1] = r[1].(int) + id // Adjust parent
			rows = append(rows, r)
		}

		// Show right side
		id++
		rows = append(rows, []interface{}{id, 1, 0, "RIGHT QUERY"})
		rightRows := e.generateQueryPlan(stmt.Right)
		for _, r := range rightRows {
			r[1] = r[1].(int) + id // Adjust parent
			rows = append(rows, r)
		}

	default:
		id++
		rows = append(rows, []interface{}{id, 0, 0, "EXECUTE " + s.String()})
	}

	return rows
}

// generateSelectPlan generates a query plan for SELECT statement
func (e *Executor) generateSelectPlan(stmt *sql.SelectStmt, id *int, parent int) [][]interface{} {
	var rows [][]interface{}

	// Check if FROM clause exists
	if stmt.From == nil || stmt.From.Table == nil {
		*id++
		rows = append(rows, []interface{}{*id, parent, 0, "SCAN (no table)"})
		return rows
	}

	tableName := stmt.From.Table.Name

	// Check for derived table (subquery in FROM)
	if stmt.From.Table.Subquery != nil {
		*id++
		alias := stmt.From.Table.Alias
		if alias == "" {
			alias = "derived_table"
		}
		rows = append(rows, []interface{}{*id, parent, 0, fmt.Sprintf("DERIVED TABLE %s", alias)})

		// Add subquery plan
		subRows := e.generateQueryPlan(stmt.From.Table.Subquery.Select)
		for _, r := range subRows {
			r[1] = r[1].(int) + *id
			rows = append(rows, r)
		}
		return rows
	}

	// Check for CTE reference
	if cteResult, ok := e.cteResults[strings.ToLower(tableName)]; ok {
		*id++
		rows = append(rows, []interface{}{*id, parent, 0, fmt.Sprintf("SCAN CTE %s (~%d rows)", tableName, cteResult.RowCount)})
	} else if e.engine.TableOrTempExists(tableName) {
		// Regular table scan
		tbl, _, err := e.engine.GetTableOrTemp(tableName)
		if err == nil {
			info := tbl.GetInfo()

			// Check if we can use an index
			scanType := "FULL TABLE SCAN"
			var usedIndex string

			if stmt.Where != nil {
				if idx := e.findIndexForWhere(tableName, stmt.Where); idx != "" {
					scanType = "INDEX SCAN"
					usedIndex = idx
				}
			}

			*id++
			detail := fmt.Sprintf("%s %s (~%d rows)", scanType, tableName, info.RowCount)
			if usedIndex != "" {
				detail += fmt.Sprintf(" USING INDEX %s", usedIndex)
			}
			rows = append(rows, []interface{}{*id, parent, 0, detail})
		} else {
			*id++
			rows = append(rows, []interface{}{*id, parent, 0, "SCAN TABLE " + tableName})
		}
	} else {
		*id++
		rows = append(rows, []interface{}{*id, parent, 0, "SCAN TABLE " + tableName + " (not found)"})
	}

	// Check for JOINs
	if stmt.From != nil && len(stmt.From.Joins) > 0 {
		for _, join := range stmt.From.Joins {
			*id++
			joinType := "INNER JOIN"
			if join.Type == sql.JoinLeft {
				joinType = "LEFT JOIN"
			} else if join.Type == sql.JoinRight {
				joinType = "RIGHT JOIN"
			} else if join.Type == sql.JoinCross {
				joinType = "CROSS JOIN"
			} else if join.Type == sql.JoinFull {
				joinType = "FULL OUTER JOIN"
			}

			joinTable := join.Table.Name

			// Get table info
			if e.engine.TableOrTempExists(joinTable) {
				if tbl, _, err := e.engine.GetTableOrTemp(joinTable); err == nil {
					info := tbl.GetInfo()
					*id++
					rows = append(rows, []interface{}{*id, parent, 0, fmt.Sprintf("%s %s (~%d rows)", joinType, joinTable, info.RowCount)})
				}
			} else {
				*id++
				rows = append(rows, []interface{}{*id, parent, 0, joinType + " TABLE " + joinTable})
			}

			// Check for join condition
			if join.On != nil {
				*id++
				rows = append(rows, []interface{}{*id, *id - 1, 0, "JOIN CONDITION"})
			}
		}
	}

	// Check for WHERE clause
	if stmt.Where != nil {
		*id++
		whereDetail := "FILTER (WHERE)"
		rows = append(rows, []interface{}{*id, parent, 0, whereDetail})
	}

	// Check for GROUP BY
	if len(stmt.GroupBy) > 0 {
		*id++
		groupCols := make([]string, len(stmt.GroupBy))
		for i, g := range stmt.GroupBy {
			if colRef, ok := g.(*sql.ColumnRef); ok {
				groupCols[i] = colRef.Name
			} else {
				groupCols[i] = g.String()
			}
		}
		rows = append(rows, []interface{}{*id, parent, 0, fmt.Sprintf("GROUP BY (%s)", strings.Join(groupCols, ", "))})
	}

	// Check for HAVING
	if stmt.Having != nil {
		*id++
		rows = append(rows, []interface{}{*id, parent, 0, "FILTER (HAVING)"})
	}

	// Check for aggregate functions
	if hasAggregateFunctions(stmt.Columns) {
		*id++
		rows = append(rows, []interface{}{*id, parent, 0, "AGGREGATE"})
	}

	// Check for DISTINCT
	if stmt.Distinct {
		*id++
		rows = append(rows, []interface{}{*id, parent, 0, "DISTINCT"})
	}

	// Check for ORDER BY
	if len(stmt.OrderBy) > 0 {
		*id++
		orderCols := make([]string, len(stmt.OrderBy))
		for i, o := range stmt.OrderBy {
			if colRef, ok := o.Expr.(*sql.ColumnRef); ok {
				orderCols[i] = colRef.Name
				if !o.Ascending {
					orderCols[i] += " DESC"
				}
			}
		}
		rows = append(rows, []interface{}{*id, parent, 0, fmt.Sprintf("ORDER BY (%s)", strings.Join(orderCols, ", "))})
	}

	// Check for LIMIT
	if stmt.Limit != nil {
		*id++
		detail := fmt.Sprintf("LIMIT %d", *stmt.Limit)
		if stmt.Offset != nil {
			detail += fmt.Sprintf(" OFFSET %d", *stmt.Offset)
		}
		rows = append(rows, []interface{}{*id, parent, 0, detail})
	}

	return rows
}

// findIndexForWhere tries to find an index that can be used for the WHERE clause
func (e *Executor) findIndexForWhere(tableName string, where sql.Expression) string {
	// Get the table
	tbl, _, err := e.engine.GetTableOrTemp(tableName)
	if err != nil || tbl == nil {
		return ""
	}

	// Get index manager
	idxMgr := tbl.GetIndexManager()
	if idxMgr == nil {
		return ""
	}

	// Extract column references from WHERE clause
	whereCols := e.extractColumnsFromExpr(where)
	if len(whereCols) == 0 {
		return ""
	}

	// Get all indexes and try to find one that matches
	indexNames := idxMgr.ListIndexes()
	for _, name := range indexNames {
		idx, err := idxMgr.GetIndex(name)
		if err != nil || idx == nil || idx.Info == nil {
			continue
		}
		for _, idxCol := range idx.Info.Columns {
			for _, whereCol := range whereCols {
				if strings.EqualFold(idxCol, whereCol) {
					return name
				}
			}
		}
	}

	// Check primary key
	primary := idxMgr.GetPrimary()
	if primary != nil && primary.Info != nil {
		for _, idxCol := range primary.Info.Columns {
			for _, whereCol := range whereCols {
				if strings.EqualFold(idxCol, whereCol) {
					return "PRIMARY"
				}
			}
		}
	}

	return ""
}

// extractColumnsFromExpr extracts column names from an expression
func (e *Executor) extractColumnsFromExpr(expr sql.Expression) []string {
	var cols []string

	switch ex := expr.(type) {
	case *sql.ColumnRef:
		cols = append(cols, ex.Name)
	case *sql.BinaryExpr:
		cols = append(cols, e.extractColumnsFromExpr(ex.Left)...)
		cols = append(cols, e.extractColumnsFromExpr(ex.Right)...)
	case *sql.UnaryExpr:
		cols = append(cols, e.extractColumnsFromExpr(ex.Right)...)
	case *sql.ParenExpr:
		cols = append(cols, e.extractColumnsFromExpr(ex.Expr)...)
	case *sql.FunctionCall:
		for _, arg := range ex.Args {
			cols = append(cols, e.extractColumnsFromExpr(arg)...)
		}
	case *sql.InExpr:
		cols = append(cols, e.extractColumnsFromExpr(ex.Expr)...)
	}

	return cols
}

// IndexCondition represents a condition that can use an index.
type IndexCondition struct {
	ColumnName  string
	Op          sql.BinaryOp
	Value       interface{}
	IsRange     bool  // true for <, <=, >, >=
	IsEquality  bool  // true for =
}

// extractIndexConditions extracts conditions from a WHERE clause that can use an index.
// Returns conditions that match the given index columns (prefix match).
func (e *Executor) extractIndexConditions(where sql.Expression, indexColumns []string) []IndexCondition {
	if where == nil || len(indexColumns) == 0 {
		return nil
	}

	var conditions []IndexCondition

	// Recursively extract conditions
	e.extractConditionsRecursive(where, indexColumns, &conditions)

	return conditions
}

// extractConditionsRecursive recursively extracts index conditions from an expression.
func (e *Executor) extractConditionsRecursive(expr sql.Expression, indexColumns []string, conditions *[]IndexCondition) {
	switch ex := expr.(type) {
	case *sql.BinaryExpr:
		// Check if this is an AND - recursively process both sides
		if ex.Op == sql.OpAnd {
			e.extractConditionsRecursive(ex.Left, indexColumns, conditions)
			e.extractConditionsRecursive(ex.Right, indexColumns, conditions)
			return
		}

		// Check for comparison operators
		if ex.Op == sql.OpEq || ex.Op == sql.OpLt || ex.Op == sql.OpLe ||
			ex.Op == sql.OpGt || ex.Op == sql.OpGe {
			// Check if left side is a column reference that matches the first index column
			if colRef, ok := ex.Left.(*sql.ColumnRef); ok {
				colName := strings.ToLower(colRef.Name)
				// Check if this column is the first index column (for index usage)
				if len(indexColumns) > 0 && strings.ToLower(indexColumns[0]) == colName {
					// Extract the value from the right side
					value := e.extractLiteralValue(ex.Right)
					if value != nil {
						*conditions = append(*conditions, IndexCondition{
							ColumnName:  colName,
							Op:          ex.Op,
							Value:       value,
							IsRange:     ex.Op != sql.OpEq,
							IsEquality:  ex.Op == sql.OpEq,
						})
					}
				}
			}
		}

	case *sql.ParenExpr:
		e.extractConditionsRecursive(ex.Expr, indexColumns, conditions)
	}
}

// extractLiteralValue extracts a literal value from an expression.
func (e *Executor) extractLiteralValue(expr sql.Expression) interface{} {
	switch ex := expr.(type) {
	case *sql.Literal:
		return ex.Value
	case *sql.ColumnRef:
		// Could be a parameter reference
		if ex.Table == "" && ex.Name == "" {
			return nil
		}
		return nil
	default:
		return nil
	}
}

// shouldUseIndex decides whether to use an index based on cost estimation.
// Returns true if index scan is cheaper than table scan.
func (e *Executor) shouldUseIndex(tableName, indexName string, conditions []IndexCondition, tableRowCount int) bool {
	if len(conditions) == 0 {
		return false // No usable conditions
	}

	// Get selectivity estimate
	selectivity := e.engine.EstimateSelectivity(tableName, indexName)

	// If we have an equality condition on the first index column, use the index
	for _, cond := range conditions {
		if cond.IsEquality {
			return true
		}
	}

	// For range conditions, use index if estimated rows < 30% of table
	if selectivity > 0 && tableRowCount > 0 {
		ratio := float64(selectivity) / float64(tableRowCount)
		if ratio < 0.3 {
			return true
		}
	}

	// If table is small (< 100 rows), table scan is fine
	if tableRowCount < 100 {
		return false
	}

	// Default: use index for unique/primary keys
	return selectivity == 1
}

// executeIndexScan performs an index scan and returns matching rows.
func (e *Executor) executeIndexScan(tableName, indexName string, conditions []IndexCondition, tbl *table.Table) ([]*row.Row, error) {
	if len(conditions) == 0 {
		return nil, fmt.Errorf("no conditions for index scan")
	}

	// Convert condition value to types.Value
	cond := conditions[0]
	var value types.Value
	switch v := cond.Value.(type) {
	case int:
		value = types.NewIntValue(int64(v))
	case int64:
		value = types.NewIntValue(v)
	case float64:
		value = types.NewFloatValue(v)
	case string:
		value = types.NewStringValue(v, types.TypeVarchar)
	case bool:
		value = types.NewBoolValue(v)
	default:
		// Try as string
		value = types.NewStringValue(fmt.Sprintf("%v", v), types.TypeVarchar)
	}

	// Determine the operator string for IndexConditionScan
	var opStr string
	switch cond.Op {
	case sql.OpEq:
		opStr = "="
	case sql.OpLt:
		opStr = "<"
	case sql.OpLe:
		opStr = "<="
	case sql.OpGt:
		opStr = ">"
	case sql.OpGe:
		opStr = ">="
	default:
		opStr = "="
	}

	// Use IndexConditionScan for all condition types
	rowIDs, err := tbl.IndexConditionScan(indexName, opStr, value)
	if err != nil {
		return nil, err
	}

	if len(rowIDs) == 0 {
		return []*row.Row{}, nil
	}

	// Fetch rows by IDs
	rowsMap, err := tbl.GetRowsByRowIDs(rowIDs)
	if err != nil {
		return nil, err
	}

	// Convert map to slice
	rows := make([]*row.Row, 0, len(rowsMap))
	for _, r := range rowsMap {
		rows = append(rows, r)
	}

	return rows, nil
}

// tryIndexScan attempts to use an index for the query.
// Returns the rows if index scan was used, or nil if no suitable index found.
func (e *Executor) tryIndexScan(tableName string, where sql.Expression, tbl *table.Table) ([]*row.Row, bool, error) {
	if where == nil {
		return nil, false, nil
	}

	tblInfo := tbl.GetInfo()
	tableRowCount := int(tblInfo.RowCount)

	// Extract columns from WHERE clause
	whereCols := e.extractColumnsFromExpr(where)
	if len(whereCols) == 0 {
		return nil, false, nil
	}

	// Find the best index for these columns
	indexName, hasIndex, matchCount := tbl.GetIndexForColumns(whereCols)
	if !hasIndex || matchCount == 0 {
		return nil, false, nil
	}

	// Get index columns
	var indexColumns []string
	if indexName == "PRIMARY" {
		indexColumns = tblInfo.PrimaryKey
	} else {
		idxMgr := tbl.GetIndexManager()
		if idxMgr != nil {
			idx, err := idxMgr.GetIndex(indexName)
			if err != nil || idx == nil {
				return nil, false, nil
			}
			indexColumns = idx.Info.Columns
		}
	}

	if len(indexColumns) == 0 {
		return nil, false, nil
	}

	// Extract conditions that can use this index
	conditions := e.extractIndexConditions(where, indexColumns)
	if len(conditions) == 0 {
		return nil, false, nil
	}

	// Decide whether to use index
	if !e.shouldUseIndex(tableName, indexName, conditions, tableRowCount) {
		return nil, false, nil
	}

	// Perform index scan
	rows, err := e.executeIndexScan(tableName, indexName, conditions, tbl)
	if err != nil {
		return nil, false, err
	}

	return rows, true, nil
}

// hasAggregateFunctions checks if the columns contain aggregate functions
func hasAggregateFunctions(columns []sql.Expression) bool {
	aggregateFuncs := map[string]bool{
		"COUNT": true, "SUM": true, "AVG": true,
		"MIN": true, "MAX": true, "GROUP_CONCAT": true,
	}

	for _, col := range columns {
		if fc, ok := col.(*sql.FunctionCall); ok {
			if aggregateFuncs[strings.ToUpper(fc.Name)] {
				return true
			}
		}
		// Check window functions
		if wfc, ok := col.(*sql.WindowFuncCall); ok {
			if aggregateFuncs[strings.ToUpper(wfc.Func.Name)] {
				return true
			}
		}
	}
	return false
}

// executeAlterTable executes an ALTER TABLE statement.
func (e *Executor) executeAlterTable(stmt *sql.AlterTableStmt) (*Result, error) {
	// Check if table exists
	if !e.engine.TableOrTempExists(stmt.TableName) {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	// Get the table
	tbl, _, err := e.engine.GetTableOrTemp(stmt.TableName)
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
	tbl, _, err := e.engine.GetTableOrTemp(tableName)
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

	// Handle default value
	if col.Default != nil {
		defaultVal, err := e.evaluateExpressionWithoutRow(col.Default)
		if err != nil {
			return fmt.Errorf("invalid default value: %v", err)
		}
		// Convert to types.Value
		newCol.Default = e.interfaceToValue(defaultVal, newCol)
	}

	return tbl.AddColumn(newCol)
}

// executeDropColumn drops a column from a table.
func (e *Executor) executeDropColumn(tableName string, action *sql.DropColumnAction) error {
	tbl, _, err := e.engine.GetTableOrTemp(tableName)
	if err != nil {
		return err
	}
	return tbl.DropColumn(action.ColumnName)
}

// executeModifyColumn modifies a column in a table.
func (e *Executor) executeModifyColumn(tableName string, action *sql.ModifyColumnAction) error {
	tbl, _, err := e.engine.GetTableOrTemp(tableName)
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
		tbl, _, err := e.engine.GetTableOrTemp(stmt.From)
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
	tbl, _, err := e.engine.GetTableOrTemp(stmt.TableName)
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
	tbl, _, err := e.engine.GetTableOrTemp(stmt.TableName)
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
	tbl, _, err := e.engine.GetTableOrTemp(tableName)
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
	tbl, _, err := e.engine.GetTableOrTemp(tableName)
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

// executeVacuum executes a VACUUM statement.
// VACUUM rebuilds the database file, reclaiming unused space and defragmenting the data.
func (e *Executor) executeVacuum(stmt *sql.VacuumStmt) (*Result, error) {
	catalog := e.engine.GetCatalog()
	if catalog == nil {
		return nil, fmt.Errorf("catalog not available")
	}

	tables := catalog.ListTables()

	// Check for specific non-existent table first
	if stmt.Table != "" {
		found := false
		for _, tableName := range tables {
			if strings.EqualFold(tableName, stmt.Table) {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("table %s not found", stmt.Table)
		}
	}

	if len(tables) == 0 {
		return &Result{Message: "VACUUM completed: no tables to vacuum"}, nil
	}

	var totalRowsBefore, totalRowsAfter uint64
	var tablesVacuumed int

	for _, tableName := range tables {
		// Skip if specific table requested and this isn't it
		if stmt.Table != "" && !strings.EqualFold(tableName, stmt.Table) {
			continue
		}

		tbl, err := catalog.GetTable(tableName)
		if err != nil {
			continue
		}

		info := tbl.GetInfo()
		rowsBefore := info.RowCount
		totalRowsBefore += rowsBefore

		// Vacuum the table - compact and rebuild
		if err := e.vacuumTable(tbl); err != nil {
			return nil, fmt.Errorf("failed to vacuum table %s: %w", tableName, err)
		}

		// Flush the table to disk
		if err := tbl.Flush(); err != nil {
			return nil, fmt.Errorf("failed to flush table %s: %w", tableName, err)
		}

		// Get updated row count
		info = tbl.GetInfo()
		rowsAfter := info.RowCount
		totalRowsAfter += rowsAfter

		tablesVacuumed++
	}

	if tablesVacuumed == 0 && stmt.Table != "" {
		return nil, fmt.Errorf("table %s not found", stmt.Table)
	}

	// Handle VACUUM INTO - export to a different file
	if stmt.IntoPath != "" {
		backupMgr := NewBackupManager(e.engine)
		opts := backup.BackupOptions{
			Path:     stmt.IntoPath,
			Compress: false,
			Database: e.database,
		}
		_, err := backupMgr.Backup(opts)
		if err != nil {
			return nil, fmt.Errorf("VACUUM INTO failed: %w", err)
		}
		return &Result{
			Message: fmt.Sprintf("VACUUM INTO completed: exported to %s (%d tables)", stmt.IntoPath, tablesVacuumed),
		}, nil
	}

	return &Result{
		Message: fmt.Sprintf("VACUUM completed: %d tables processed, %d rows", tablesVacuumed, totalRowsAfter),
	}, nil
}

// vacuumTable compacts a single table by rebuilding it.
func (e *Executor) vacuumTable(tbl *table.Table) error {
	// Get all rows
	rows, err := tbl.GetAllRows()
	if err != nil {
		return err
	}

	// Get table info
	info := tbl.GetInfo()
	columns := info.Columns

	// Clear the table's data pages
	if err := tbl.Truncate(); err != nil {
		return err
	}

	// Re-insert all rows
	for _, r := range rows {
		if _, err := tbl.Insert(r.Values); err != nil {
			return err
		}
	}

	// Rebuild indexes
	indexMgr := tbl.GetIndexManager()
	if indexMgr != nil {
		// Get all index names
		indexNames := indexMgr.ListIndexes()

		for _, idxName := range indexNames {
			idx, err := indexMgr.GetIndex(idxName)
			if err != nil {
				continue
			}

			// Clear and rebuild the index
			idx.Clear()

			// Re-insert all rows into the index
			colMap := make(map[string]int)
			for i, col := range columns {
				colMap[strings.ToLower(col.Name)] = i
			}

			for rowIdx, r := range rows {
				if len(idx.Info.Columns) == 0 {
					continue
				}
				colIdx, ok := colMap[strings.ToLower(idx.Info.Columns[0])]
				if !ok || colIdx >= len(r.Values) {
					continue
				}
				key := r.Values[colIdx]
				if err := idx.Insert(key, row.RowID(rowIdx)); err != nil {
					return err
				}
			}
		}

		// Rebuild primary key index
		primary := indexMgr.GetPrimary()
		if primary != nil && len(info.PrimaryKey) > 0 {
			primary.Clear()
			colMap := make(map[string]int)
			for i, col := range columns {
				colMap[strings.ToLower(col.Name)] = i
			}

			pkCol := info.PrimaryKey[0]
			colIdx, ok := colMap[strings.ToLower(pkCol)]
			if ok {
				for rowIdx, r := range rows {
					if colIdx < len(r.Values) {
						key := r.Values[colIdx]
						if err := primary.Insert(key, row.RowID(rowIdx)); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}

// executePragma executes a PRAGMA statement.
// PRAGMA is used to query or set runtime configuration options.
func (e *Executor) executePragma(stmt *sql.PragmaStmt) (*Result, error) {
	name := strings.ToLower(stmt.Name)

	// Initialize pragma settings if needed
	if e.pragmaSettings == nil {
		e.pragmaSettings = make(map[string]interface{})
		// Set defaults
		e.pragmaSettings["cache_size"] = int64(2000)
		e.pragmaSettings["foreign_keys"] = true
		e.pragmaSettings["synchronous"] = int64(1)
		e.pragmaSettings["journal_mode"] = "WAL"
		e.pragmaSettings["auto_vacuum"] = int64(0)
		e.pragmaSettings["temp_store"] = "MEMORY"
		e.pragmaSettings["busy_timeout"] = int64(5000)
		e.pragmaSettings["locking_mode"] = "NORMAL"
		e.pragmaSettings["recursive_triggers"] = true
		e.pragmaSettings["ignore_check_constraints"] = false
	}

	// Handle function-style pragmas with argument
	if stmt.Argument != "" {
		return e.executePragmaWithArg(name, stmt.Argument, stmt.Value)
	}

	// Handle function-style pragmas without argument (e.g., database_list, integrity_check)
	switch name {
	case "database_list":
		return e.pragmaDatabaseList()
	case "compile_options":
		return e.pragmaCompileOptions()
	case "integrity_check":
		return e.pragmaIntegrityCheck("")
	case "quick_check":
		return e.pragmaQuickCheck("")
	case "page_count":
		return e.pragmaPageCount("")
	case "page_size":
		return e.pragmaPageSize("")
	}

	// If value is provided, set the pragma
	if stmt.Value != nil {
		return e.setPragma(name, stmt.Value)
	}

	// Otherwise, return the current value
	return e.getPragma(name)
}

// setPragma sets a pragma value.
func (e *Executor) setPragma(name string, value interface{}) (*Result, error) {
	switch name {
	case "cache_size":
		val, ok := toInt64(value)
		if !ok {
			return nil, fmt.Errorf("cache_size must be an integer")
		}
		e.pragmaSettings["cache_size"] = val
		return &Result{Message: fmt.Sprintf("cache_size = %d", val)}, nil

	case "foreign_keys":
		val, ok := toBool(value)
		if !ok {
			return nil, fmt.Errorf("foreign_keys must be a boolean")
		}
		e.pragmaSettings["foreign_keys"] = val
		return &Result{Message: fmt.Sprintf("foreign_keys = %v", val)}, nil

	case "synchronous":
		val, ok := toInt64(value)
		if !ok {
			return nil, fmt.Errorf("synchronous must be an integer (0=OFF, 1=NORMAL, 2=FULL)")
		}
		if val < 0 || val > 2 {
			return nil, fmt.Errorf("synchronous must be 0, 1, or 2")
		}
		e.pragmaSettings["synchronous"] = val
		return &Result{Message: fmt.Sprintf("synchronous = %d", val)}, nil

	case "journal_mode":
		val := strings.ToUpper(fmt.Sprintf("%v", value))
		switch val {
		case "DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF":
			e.pragmaSettings["journal_mode"] = val
			return &Result{Message: fmt.Sprintf("journal_mode = %s", val)}, nil
		default:
			return nil, fmt.Errorf("invalid journal_mode: %s (valid: DELETE, TRUNCATE, PERSIST, MEMORY, WAL, OFF)", val)
		}

	case "auto_vacuum":
		val, ok := toInt64(value)
		if !ok {
			return nil, fmt.Errorf("auto_vacuum must be an integer (0=NONE, 1=FULL, 2=INCREMENTAL)")
		}
		if val < 0 || val > 2 {
			return nil, fmt.Errorf("auto_vacuum must be 0, 1, or 2")
		}
		e.pragmaSettings["auto_vacuum"] = val
		return &Result{Message: fmt.Sprintf("auto_vacuum = %d", val)}, nil

	case "temp_store":
		val := strings.ToUpper(fmt.Sprintf("%v", value))
		switch val {
		case "DEFAULT", "FILE", "MEMORY":
			e.pragmaSettings["temp_store"] = val
			return &Result{Message: fmt.Sprintf("temp_store = %s", val)}, nil
		default:
			return nil, fmt.Errorf("invalid temp_store: %s (valid: DEFAULT, FILE, MEMORY)", val)
		}

	case "busy_timeout":
		val, ok := toInt64(value)
		if !ok {
			return nil, fmt.Errorf("busy_timeout must be an integer (milliseconds)")
		}
		e.pragmaSettings["busy_timeout"] = val
		return &Result{Message: fmt.Sprintf("busy_timeout = %d", val)}, nil

	case "locking_mode":
		val := strings.ToUpper(fmt.Sprintf("%v", value))
		switch val {
		case "NORMAL", "EXCLUSIVE":
			e.pragmaSettings["locking_mode"] = val
			return &Result{Message: fmt.Sprintf("locking_mode = %s", val)}, nil
		default:
			return nil, fmt.Errorf("invalid locking_mode: %s (valid: NORMAL, EXCLUSIVE)", val)
		}

	case "recursive_triggers":
		val, ok := toBool(value)
		if !ok {
			return nil, fmt.Errorf("recursive_triggers must be a boolean")
		}
		e.pragmaSettings["recursive_triggers"] = val
		return &Result{Message: fmt.Sprintf("recursive_triggers = %v", val)}, nil

	case "ignore_check_constraints":
		val, ok := toBool(value)
		if !ok {
			return nil, fmt.Errorf("ignore_check_constraints must be a boolean")
		}
		e.pragmaSettings["ignore_check_constraints"] = val
		return &Result{Message: fmt.Sprintf("ignore_check_constraints = %v", val)}, nil

	case "database_version":
		e.pragmaSettings["database_version"] = fmt.Sprintf("%v", value)
		return &Result{Message: fmt.Sprintf("database_version = %v", value)}, nil

	case "user_version":
		val, ok := toInt64(value)
		if !ok {
			// Allow string values too
			e.pragmaSettings["user_version"] = fmt.Sprintf("%v", value)
			return &Result{Message: fmt.Sprintf("user_version = %v", value)}, nil
		}
		e.pragmaSettings["user_version"] = val
		return &Result{Message: fmt.Sprintf("user_version = %d", val)}, nil

	default:
		// Store custom pragma
		e.pragmaSettings[name] = value
		return &Result{Message: fmt.Sprintf("%s = %v", name, value)}, nil
	}
}

// getPragma returns a pragma value.
func (e *Executor) getPragma(name string) (*Result, error) {
	val, exists := e.pragmaSettings[name]
	if !exists {
		return nil, fmt.Errorf("unknown pragma: %s", name)
	}

	return &Result{
		Columns: []ColumnInfo{
			{Name: name, Type: "TEXT"},
		},
		Rows:    [][]interface{}{{val}},
		Message: fmt.Sprintf("%v", val),
	}, nil
}

// GetPragmaValue returns the current value of a pragma (for internal use).
func (e *Executor) GetPragmaValue(name string) interface{} {
	if e.pragmaSettings == nil {
		return nil
	}
	return e.pragmaSettings[strings.ToLower(name)]
}

// executePragmaWithArg executes a function-style PRAGMA with an argument.
// Examples: PRAGMA table_info(users), PRAGMA index_list(users)
func (e *Executor) executePragmaWithArg(name, arg string, value interface{}) (*Result, error) {
	switch name {
	case "table_info":
		return e.pragmaTableInfo(arg)
	case "index_list":
		return e.pragmaIndexList(arg)
	case "index_info":
		return e.pragmaIndexInfo(arg)
	case "foreign_key_list":
		return e.pragmaForeignKeyList(arg)
	case "database_list":
		return e.pragmaDatabaseList()
	case "compile_options":
		return e.pragmaCompileOptions()
	case "integrity_check":
		return e.pragmaIntegrityCheck(arg)
	case "quick_check":
		return e.pragmaQuickCheck(arg)
	case "page_count":
		return e.pragmaPageCount(arg)
	case "page_size":
		return e.pragmaPageSize(arg)
	default:
		return nil, fmt.Errorf("unknown pragma function: %s", name)
	}
}

// pragmaTableInfo returns information about columns in a table.
// Output: cid, name, type, notnull, dflt_value, pk
func (e *Executor) pragmaTableInfo(tableName string) (*Result, error) {
	table, err := e.engine.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	info := table.GetInfo()
	rows := make([][]interface{}, 0, len(info.Columns))

	for i, col := range info.Columns {
		pk := 0
		if col.PrimaryKey {
			// Find position in primary key
			for j, pkCol := range info.PrimaryKey {
				if pkCol == col.Name {
					pk = j + 1
					break
				}
			}
		}

		dfltValue := interface{}(nil)
		if !col.Default.Null {
			dfltValue = col.Default.String()
		}

		notNull := 0
		if !col.Nullable {
			notNull = 1
		}

		rows = append(rows, []interface{}{
			i,              // cid
			col.Name,       // name
			col.Type,       // type
			notNull,        // notnull (1 = NOT NULL, 0 = nullable)
			dfltValue,      // dflt_value
			pk,             // pk (0 = not PK, >0 = position in PK)
		})
	}

	return &Result{
		Columns: []ColumnInfo{
			{Name: "cid", Type: "INT"},
			{Name: "name", Type: "TEXT"},
			{Name: "type", Type: "TEXT"},
			{Name: "notnull", Type: "INT"},
			{Name: "dflt_value", Type: "TEXT"},
			{Name: "pk", Type: "INT"},
		},
		Rows: rows,
	}, nil
}

// pragmaIndexList returns list of indexes for a table.
// Output: seq, name, unique, origin, partial
func (e *Executor) pragmaIndexList(tableName string) (*Result, error) {
	table, err := e.engine.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	indexMgr := table.GetIndexManager()
	if indexMgr == nil {
		return &Result{
			Columns: []ColumnInfo{
				{Name: "seq", Type: "INT"},
				{Name: "name", Type: "TEXT"},
				{Name: "unique", Type: "INT"},
				{Name: "origin", Type: "TEXT"},
				{Name: "partial", Type: "INT"},
			},
			Rows: [][]interface{}{},
		}, nil
	}

	rows := make([][]interface{}, 0)
	seq := 0

	// Add primary key index first
	if indexMgr.HasPrimary() {
		rows = append(rows, []interface{}{
			seq,    // seq
			"PRIMARY", // name
			1,      // unique
			"pk",   // origin (pk = primary key)
			0,      // partial (0 = not partial index)
		})
		seq++
	}

	// Add other indexes
	for _, idxName := range indexMgr.ListIndexes() {
		idx, err := indexMgr.GetIndex(idxName)
		if err != nil {
			continue
		}
		unique := 0
		if idx.Info.Type == btree.IndexTypeUnique {
			unique = 1
		}
		rows = append(rows, []interface{}{
			seq,       // seq
			idxName,   // name
			unique,    // unique
			"c",       // origin (c = CREATE INDEX)
			0,         // partial (0 = not partial index)
		})
		seq++
	}

	return &Result{
		Columns: []ColumnInfo{
			{Name: "seq", Type: "INT"},
			{Name: "name", Type: "TEXT"},
			{Name: "unique", Type: "INT"},
			{Name: "origin", Type: "TEXT"},
			{Name: "partial", Type: "INT"},
		},
		Rows: rows,
	}, nil
}

// pragmaIndexInfo returns information about columns in an index.
// Output: seqno, cid, name
func (e *Executor) pragmaIndexInfo(indexName string) (*Result, error) {
	// Find the index across all tables
	tables := e.engine.ListTables()

	for _, tableName := range tables {
		table, err := e.engine.GetTable(tableName)
		if err != nil {
			continue
		}

		info := table.GetInfo()
		indexMgr := table.GetIndexManager()
		if indexMgr == nil {
			continue
		}

		// Check if it's the primary key index
		if strings.ToUpper(indexName) == "PRIMARY" && indexMgr.HasPrimary() {
			pk := indexMgr.GetPrimary()
			if pk != nil {
				rows := make([][]interface{}, 0, len(pk.Info.Columns))
				for i, colName := range pk.Info.Columns {
					// Find column id
					cid := -1
					for j, col := range info.Columns {
						if col.Name == colName {
							cid = j
							break
						}
					}
					rows = append(rows, []interface{}{
						i,       // seqno
						cid,     // cid
						colName, // name
					})
				}
				return &Result{
					Columns: []ColumnInfo{
						{Name: "seqno", Type: "INT"},
						{Name: "cid", Type: "INT"},
						{Name: "name", Type: "TEXT"},
					},
					Rows: rows,
				}, nil
			}
		}

		// Check other indexes
		idx, err := indexMgr.GetIndex(indexName)
		if err == nil && idx != nil {
			rows := make([][]interface{}, 0, len(idx.Info.Columns))
			for i, colName := range idx.Info.Columns {
				// Find column id
				cid := -1
				for j, col := range info.Columns {
					if col.Name == colName {
						cid = j
						break
					}
				}
				rows = append(rows, []interface{}{
					i,       // seqno
					cid,     // cid
					colName, // name
				})
			}
			return &Result{
				Columns: []ColumnInfo{
					{Name: "seqno", Type: "INT"},
					{Name: "cid", Type: "INT"},
					{Name: "name", Type: "TEXT"},
				},
				Rows: rows,
			}, nil
		}
	}

	return nil, fmt.Errorf("index '%s' does not exist", indexName)
}

// pragmaForeignKeyList returns list of foreign keys for a table.
// Output: id, seq, table, from, to, on_update, on_delete, match
func (e *Executor) pragmaForeignKeyList(tableName string) (*Result, error) {
	table, err := e.engine.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	info := table.GetInfo()
	rows := make([][]interface{}, 0)

	fkID := 0
	for _, fk := range info.ForeignKeys {
		for i, fromCol := range fk.Columns {
			toCol := ""
			if i < len(fk.RefColumns) {
				toCol = fk.RefColumns[i]
			}
			rows = append(rows, []interface{}{
				fkID,        // id
				i,           // seq
				fk.RefTable, // table
				fromCol,     // from
				toCol,       // to
				fk.OnUpdate, // on_update
				fk.OnDelete, // on_delete
				"NONE",      // match
			})
		}
		fkID++
	}

	return &Result{
		Columns: []ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "seq", Type: "INT"},
			{Name: "table", Type: "TEXT"},
			{Name: "from", Type: "TEXT"},
			{Name: "to", Type: "TEXT"},
			{Name: "on_update", Type: "TEXT"},
			{Name: "on_delete", Type: "TEXT"},
			{Name: "match", Type: "TEXT"},
		},
		Rows: rows,
	}, nil
}

// pragmaDatabaseList returns list of databases.
// Output: seq, name, file
func (e *Executor) pragmaDatabaseList() (*Result, error) {
	return &Result{
		Columns: []ColumnInfo{
			{Name: "seq", Type: "INT"},
			{Name: "name", Type: "TEXT"},
			{Name: "file", Type: "TEXT"},
		},
		Rows: [][]interface{}{
			{0, "main", e.engine.GetDataDir()},
		},
	}, nil
}

// pragmaCompileOptions returns compile-time options.
func (e *Executor) pragmaCompileOptions() (*Result, error) {
	options := []string{
		"ENABLE_FTS",
		"ENABLE_JSON",
		"ENABLE_WINDOW_FUNCTIONS",
		"ENABLE_GENERATED_COLUMNS",
		"ENABLE_UPSERT",
		"ENABLE_RETURNING",
	}
	rows := make([][]interface{}, 0, len(options))
	for i, opt := range options {
		rows = append(rows, []interface{}{i, opt})
	}
	return &Result{
		Columns: []ColumnInfo{
			{Name: "seq", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
		Rows: rows,
	}, nil
}

// pragmaIntegrityCheck performs database integrity check.
func (e *Executor) pragmaIntegrityCheck(tableName string) (*Result, error) {
	// Simple integrity check - verify all tables can be read
	tables := e.engine.ListTables()

	errors := make([]string, 0)

	for _, tbl := range tables {
		table, err := e.engine.GetTable(tbl)
		if err != nil {
			errors = append(errors, fmt.Sprintf("table %s: %v", tbl, err))
			continue
		}

		// Try to scan all rows
		_, err = table.Scan()
		if err != nil {
			errors = append(errors, fmt.Sprintf("scan %s: %v", tbl, err))
		}
	}

	if len(errors) == 0 {
		return &Result{
			Columns: []ColumnInfo{{Name: "integrity_check", Type: "TEXT"}},
			Rows:    [][]interface{}{{"ok"}},
		}, nil
	}

	rows := make([][]interface{}, 0, len(errors))
	for _, e := range errors {
		rows = append(rows, []interface{}{e})
	}
	return &Result{
		Columns: []ColumnInfo{{Name: "integrity_check", Type: "TEXT"}},
		Rows:    rows,
	}, nil
}

// pragmaQuickCheck performs quick integrity check.
func (e *Executor) pragmaQuickCheck(tableName string) (*Result, error) {
	// Quick check is similar to integrity_check but faster
	return e.pragmaIntegrityCheck(tableName)
}

// pragmaPageCount returns the number of pages in the database.
func (e *Executor) pragmaPageCount(tableName string) (*Result, error) {
	// Calculate total pages across all tables
	tables := e.engine.ListTables()

	totalPages := 0
	for _, tbl := range tables {
		table, err := e.engine.GetTable(tbl)
		if err != nil {
			continue
		}
		info := table.GetInfo()
		totalPages += int(info.NextPageID - 1)
	}

	return &Result{
		Columns: []ColumnInfo{{Name: "page_count", Type: "INT"}},
		Rows:    [][]interface{}{{totalPages}},
	}, nil
}

// pragmaPageSize returns the page size.
func (e *Executor) pragmaPageSize(tableName string) (*Result, error) {
	// Return default page size (4096 is common)
	pageSize := 4096
	return &Result{
		Columns: []ColumnInfo{{Name: "page_size", Type: "INT"}},
		Rows:    [][]interface{}{{pageSize}},
	}, nil
}

// boolToInt converts bool to int (true=1, false=0)
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// toInt64 converts various types to int64.
func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int64:
		return val, true
	case int32:
		return int64(val), true
	case float64:
		return int64(val), true
	case float32:
		return int64(val), true
	case string:
		// Try parsing as number
		var i int64
		_, err := fmt.Sscanf(val, "%d", &i)
		return i, err == nil
	case bool:
		if val {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}

// toBool converts various types to bool.
func toBool(v interface{}) (bool, bool) {
	switch val := v.(type) {
	case bool:
		return val, true
	case int:
		return val != 0, true
	case int64:
		return val != 0, true
	case int32:
		return val != 0, true
	case string:
		switch strings.ToUpper(val) {
		case "TRUE", "ON", "YES", "1":
			return true, true
		case "FALSE", "OFF", "NO", "0":
			return false, true
		default:
			return false, false
		}
	default:
		return false, false
	}
}

// executeAnalyze executes an ANALYZE statement.
// ANALYZE [TABLE table_name] - collects statistics for query optimization.
func (e *Executor) executeAnalyze(stmt *sql.AnalyzeStmt) (*Result, error) {
	result := &Result{
		Columns: []ColumnInfo{
			{Name: "table", Type: "VARCHAR"},
			{Name: "rows_analyzed", Type: "INT"},
		},
		Rows: make([][]interface{}, 0),
	}

	if stmt.TableName != "" {
		// Analyze a specific table
		count, err := e.analyzeTable(stmt.TableName)
		if err != nil {
			return nil, err
		}
		result.Rows = append(result.Rows, []interface{}{stmt.TableName, count})
	} else {
		// Analyze all tables
		tables := e.engine.ListTables()
		for _, tableName := range tables {
			count, err := e.analyzeTable(tableName)
			if err != nil {
				// Log error but continue with other tables
				continue
			}
			result.Rows = append(result.Rows, []interface{}{tableName, count})
		}
	}

	result.RowCount = len(result.Rows)
	return result, nil
}

// analyzeTable analyzes a single table and returns the number of rows analyzed.
func (e *Executor) analyzeTable(tableName string) (int, error) {
	tbl, err := e.engine.GetTable(tableName)
	if err != nil {
		return 0, fmt.Errorf("table '%s' does not exist", tableName)
	}

	// Scan all rows to collect statistics
	rows, err := e.engine.Scan(tableName)
	if err != nil {
		return 0, err
	}

	info := tbl.GetInfo()
	analyzer := NewAnalyzeTable()

	// Analyze each row
	for _, r := range rows {
		analyzer.AddRow(getColumnNames(info.Columns), r.Values)
	}

	// Finalize statistics
	stats := analyzer.Finalize()

	// Analyze indexes
	idxMgr := tbl.GetIndexManager()
	if idxMgr != nil {
		// Primary key
		if pk := idxMgr.GetPrimary(); pk != nil {
			height := pk.Tree.Height()
			count := pk.Tree.Count()
			analyzer.SetIndexStatistics("PRIMARY", pk.Info.Columns, uint64(count), height, true)
		}

		// Secondary indexes
		for _, idxName := range idxMgr.ListIndexes() {
			idx, err := idxMgr.GetIndex(idxName)
			if err != nil {
				continue
			}
			height := idx.Tree.Height()
			count := idx.Tree.Count()
			analyzer.SetIndexStatistics(idxName, idx.Info.Columns, uint64(count), height, false)
		}
	}

	// Store statistics in the optimizer
	if e.optimizer == nil {
		e.optimizer = NewOptimizer()
	}
	e.optimizer.UpdateStatistics(strings.ToLower(tableName), stats)

	return len(rows), nil
}

// NewAnalyzeTable creates a new table analyzer.
// This is a helper that wraps the optimizer's AnalyzeTable.
func NewAnalyzeTable() *optimizer.AnalyzeTable {
	return optimizer.NewAnalyzeTable()
}

// NewOptimizer creates a new query optimizer.
func NewOptimizer() *optimizer.Optimizer {
	return optimizer.NewOptimizer()
}

// getColumnNames extracts column names from column info.
func getColumnNames(columns []*types.ColumnInfo) []string {
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.Name
	}
	return names
}

// executeCreateFunction executes a CREATE FUNCTION statement.
func (e *Executor) executeCreateFunction(stmt *sql.CreateFunctionStmt) (*Result, error) {
	// Check if it's a script-based function (new style)
	if stmt.Script != "" {
		if e.scriptUDFMgr == nil {
			return nil, fmt.Errorf("script UDF manager not initialized")
		}

		// Extract parameter names
		params := make([]string, len(stmt.Parameters))
		for i, p := range stmt.Parameters {
			params[i] = p.Name
		}

		retType := ""
		if stmt.ReturnType != nil {
			retType = stmt.ReturnType.Name
		}

		fn := &ScriptFunction{
			Name:       strings.ToUpper(stmt.Name),
			Params:     params,
			ReturnType: retType,
			Script:     stmt.Script,
		}

		if err := e.scriptUDFMgr.CreateFunction(fn, stmt.Replace); err != nil {
			return nil, err
		}

		// Save to disk
		if err := e.scriptUDFMgr.Save(); err != nil {
			return nil, fmt.Errorf("failed to save function: %w", err)
		}

		return &Result{
			Message: fmt.Sprintf("Function %s created", stmt.Name),
		}, nil
	}

	// Old style: SQL expression body
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
	name := strings.ToUpper(stmt.Name)

	// Try script UDF first
	if e.scriptUDFMgr != nil {
		if _, exists := e.scriptUDFMgr.GetFunction(name); exists {
			if err := e.scriptUDFMgr.DropFunction(name); err != nil {
				if stmt.IfExists {
					return &Result{Message: "OK"}, nil
				}
				return nil, err
			}
			if err := e.scriptUDFMgr.Save(); err != nil {
				return nil, fmt.Errorf("failed to save functions: %w", err)
			}
			return &Result{
				Message: fmt.Sprintf("Function %s dropped", stmt.Name),
			}, nil
		}
	}

	// Try old style UDF
	if e.udfManager != nil {
		if err := e.udfManager.DropFunction(name); err != nil {
			if stmt.IfExists {
				return &Result{Message: "OK"}, nil
			}
			return nil, err
		}
		if err := e.udfManager.Save(); err != nil {
			return nil, fmt.Errorf("failed to save functions: %w", err)
		}
		return &Result{
			Message: fmt.Sprintf("Function %s dropped", stmt.Name),
		}, nil
	}

	if stmt.IfExists {
		return &Result{Message: "OK"}, nil
	}
	return nil, fmt.Errorf("function %s does not exist", stmt.Name)
}

// executeCreateTrigger executes a CREATE TRIGGER statement.
func (e *Executor) executeCreateTrigger(stmt *sql.CreateTriggerStmt) (*Result, error) {
	// Check if trigger already exists
	if e.engine.TriggerExists(stmt.TriggerName) {
		if stmt.IfNotExists {
			return &Result{Message: "OK"}, nil
		}
		return nil, fmt.Errorf("trigger already exists: %s", stmt.TriggerName)
	}

	// Check if table exists
	if !e.engine.TableOrTempExists(stmt.TableName) {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	// Serialize the WHEN clause if present
	whenClause := ""
	if stmt.WhenClause != nil {
		whenClause = stmt.WhenClause.String()
	}

	// Serialize the body statements
	var bodyParts []string
	for _, s := range stmt.Body {
		bodyParts = append(bodyParts, s.String())
	}
	body := strings.Join(bodyParts, "; ")

	// Create trigger in catalog
	if err := e.engine.CreateTrigger(
		stmt.TriggerName,
		int(stmt.Timing),
		int(stmt.Event),
		stmt.TableName,
		int(stmt.Granularity),
		whenClause,
		body,
	); err != nil {
		return nil, err
	}

	return &Result{Message: "OK"}, nil
}

// executeDropTrigger executes a DROP TRIGGER statement.
func (e *Executor) executeDropTrigger(stmt *sql.DropTriggerStmt) (*Result, error) {
	if !e.engine.TriggerExists(stmt.TriggerName) {
		if stmt.IfExists {
			return &Result{Message: "OK"}, nil
		}
		return nil, fmt.Errorf("trigger not found: %s", stmt.TriggerName)
	}

	if err := e.engine.DropTrigger(stmt.TriggerName); err != nil {
		return nil, err
	}

	return &Result{Message: "OK"}, nil
}

// fireTriggers executes triggers for a given table and event.
// timing: 0=BEFORE, 1=AFTER
// event: 0=INSERT, 1=UPDATE, 2=DELETE
// rowData contains OLD and NEW row data for the trigger context
func (e *Executor) fireTriggers(tableName string, timing, event int, rowData map[string]interface{}) error {
	triggers := e.engine.GetTriggersForTable(tableName, event)
	if len(triggers) == 0 {
		return nil
	}

	// Filter triggers by timing
	var matchingTriggers []*catalog.TriggerInfo
	for _, t := range triggers {
		if t.Timing == timing {
			matchingTriggers = append(matchingTriggers, t)
		}
	}

	// Execute each matching trigger
	for _, t := range matchingTriggers {
		// Set up context for trigger execution (OLD and NEW references)
		oldOuter := e.outerContext
		e.outerContext = make(map[string]interface{})
		for k, v := range rowData {
			e.outerContext[k] = v
		}

		// Execute each statement in the trigger body
		// We need to execute via Execute() which handles all statement types
		_, err := e.Execute(t.Body)
		if err != nil {
			e.outerContext = oldOuter
			return fmt.Errorf("trigger %s: %w", t.Name, err)
		}

		// Restore outer context
		e.outerContext = oldOuter
	}

	return nil
}

// executeTruncate executes a TRUNCATE TABLE statement.
func (e *Executor) executeTruncate(stmt *sql.TruncateTableStmt) (*Result, error) {
	// Check if table exists
	if !e.engine.TableOrTempExists(stmt.TableName) {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	// Drop and recreate the table (simple implementation)
	tbl, _, err := e.engine.GetTableOrTemp(stmt.TableName)
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
	case *sql.CollateExpr:
		// Handle COLLATE in WHERE - evaluate inner expression with collation
		return e.evaluateWhereWithCollation(ex.Expr, ex.Collate, r, columnMap, columnOrder)

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

		// Extract collation from either left or right expression
		collation := e.extractCollation(ex.Left)
		if collation == "" {
			collation = e.extractCollation(ex.Right)
		}

		return e.compareValuesWithCollation(left, ex.Op, right, collation, ex.EscapeChar)

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

	case *sql.ParenExpr:
		// Evaluate the inner expression
		return e.evaluateWhere(ex.Expr, r, columnMap, columnOrder)

	case *sql.MatchExpr:
		// For FTS matching, we need to check if the row ID matches FTS results
		// This requires the FTS manager to have performed a search beforehand
		// For simplicity, we check if this row's ID is in the matched results
		if e.ftsManager == nil {
			return false, nil
		}

		// Get FTS indexes for the table
		ftsIndexes := e.engine.GetCatalog().GetFTSIndexesForTable(ex.Table)
		if len(ftsIndexes) == 0 {
			return false, nil
		}

		// Search using the first matching index
		for _, ftsInfo := range ftsIndexes {
			results, err := e.ftsManager.Search(ftsInfo.Name, ex.Query)
			if err != nil {
				continue
			}
			// Check if this row's ID is in the results
			rowID := uint64(r.ID)
			for _, result := range results {
				if result.DocID == rowID {
					return true, nil
				}
			}
		}
		return false, nil
	}

	return false, nil
}

// extractCollation extracts the collation name from an expression if it's a CollateExpr.
func (e *Executor) extractCollation(expr sql.Expression) string {
	if collate, ok := expr.(*sql.CollateExpr); ok {
		return collate.Collate
	}
	return ""
}

// evaluateWhereWithCollation evaluates a WHERE expression with a specific collation.
func (e *Executor) evaluateWhereWithCollation(expr sql.Expression, collation string, r *row.Row, columnMap map[string]*types.ColumnInfo, columnOrder []*types.ColumnInfo) (bool, error) {
	switch ex := expr.(type) {
	case *sql.BinaryExpr:
		// Handle logical operators
		if ex.Op == sql.OpAnd {
			left, err := e.evaluateWhereWithCollation(ex.Left, collation, r, columnMap, columnOrder)
			if err != nil {
				return false, err
			}
			if !left {
				return false, nil
			}
			return e.evaluateWhereWithCollation(ex.Right, collation, r, columnMap, columnOrder)
		}
		if ex.Op == sql.OpOr {
			left, err := e.evaluateWhereWithCollation(ex.Left, collation, r, columnMap, columnOrder)
			if err != nil {
				return false, err
			}
			if left {
				return true, nil
			}
			return e.evaluateWhereWithCollation(ex.Right, collation, r, columnMap, columnOrder)
		}

		// Comparison operators
		left, err := e.evaluateExpression(ex.Left, r, columnMap, columnOrder)
		if err != nil {
			return false, err
		}
		right, err := e.evaluateExpression(ex.Right, r, columnMap, columnOrder)
		if err != nil {
			return false, err
		}
		return e.compareValuesWithCollation(left, ex.Op, right, collation, ex.EscapeChar)

	case *sql.ParenExpr:
		return e.evaluateWhereWithCollation(ex.Expr, collation, r, columnMap, columnOrder)

	default:
		return e.evaluateWhere(expr, r, columnMap, columnOrder)
	}
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

	case *sql.CollateExpr:
		// COLLATE expression - evaluate inner expression
		// The collation is used in comparisons, not in value evaluation
		return e.evaluateExpression(ex.Expr, r, columnMap, columnOrder)

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

	case *sql.ParenExpr:
		// Evaluate the inner expression
		return e.evaluateExpression(ex.Expr, r, columnMap, columnOrder)

	case *sql.RankExpr:
		// Return the FTS rank score for the current row
		// This is a placeholder - actual rank should be computed during FTS search
		// and stored in a context variable
		if e.ftsManager == nil {
			return 0.0, nil
		}

		// Get FTS rank from context if available
		if e.outerContext != nil {
			if rank, ok := e.outerContext["__fts_rank"].(float64); ok {
				return rank, nil
			}
		}

		return 0.0, nil
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
				// Return the column name as a string (for cases like TIMESTAMPDIFF(DAY, ...))
				return ex.Name, nil
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

	case "MD5":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("MD5 requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		strVal := fmt.Sprintf("%v", arg)
		hash := md5.Sum([]byte(strVal))
		return hex.EncodeToString(hash[:]), nil

	case "SHA1":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("SHA1 requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		strVal := fmt.Sprintf("%v", arg)
		hash := sha1.Sum([]byte(strVal))
		return hex.EncodeToString(hash[:]), nil

	case "SHA256":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("SHA256 requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		strVal := fmt.Sprintf("%v", arg)
		hash := sha256.Sum256([]byte(strVal))
		return hex.EncodeToString(hash[:]), nil

	case "SHA512":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("SHA512 requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		strVal := fmt.Sprintf("%v", arg)
		hash := sha512.Sum512([]byte(strVal))
		return hex.EncodeToString(hash[:]), nil

	case "BASE64_ENCODE", "BASE64ENCODE":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("BASE64_ENCODE requires 1 argument")
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
			return base64.StdEncoding.EncodeToString(v), nil
		case string:
			return base64.StdEncoding.EncodeToString([]byte(v)), nil
		default:
			return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%v", v))), nil
		}

	case "BASE64_DECODE", "BASE64DECODE":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("BASE64_DECODE requires 1 argument")
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
		decoded, err := base64.StdEncoding.DecodeString(strVal)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 string: %v", err)
		}
		return decoded, nil

	case "HEX_ENCODE", "HEXENCODE":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("HEX_ENCODE requires 1 argument")
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
			return hex.EncodeToString(v), nil
		case string:
			return hex.EncodeToString([]byte(v)), nil
		default:
			return hex.EncodeToString([]byte(fmt.Sprintf("%v", v))), nil
		}

	case "HEX_DECODE", "HEXDECODE":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("HEX_DECODE requires 1 argument")
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
		decoded, err := hex.DecodeString(strVal)
		if err != nil {
			return nil, fmt.Errorf("invalid hex string: %v", err)
		}
		return decoded, nil

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

	// Math functions
	case "ABS":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("ABS requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		switch v := arg.(type) {
		case int:
			if v < 0 {
				return -v, nil
			}
			return v, nil
		case int64:
			if v < 0 {
				return -v, nil
			}
			return v, nil
		case float64:
			if v < 0 {
				return -v, nil
			}
			return v, nil
		default:
			return nil, fmt.Errorf("ABS requires numeric argument")
		}

	case "ROUND":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("ROUND requires at least 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}

		// Get the value to round
		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return nil, fmt.Errorf("ROUND requires numeric argument")
		}

		// Get precision (default 0)
		precision := 0
		if len(fc.Args) >= 2 {
			precArg, err := evalExpr(fc.Args[1])
			if err != nil {
				return nil, err
			}
			switch v := precArg.(type) {
			case int:
				precision = v
			case int64:
				precision = int(v)
			case float64:
				precision = int(v)
			}
		}

		// Round using math.Round with precision
		multiplier := math.Pow(10, float64(precision))
		result := math.Round(val*multiplier) / multiplier

		// Return int64 if precision is 0, otherwise float64
		if precision == 0 {
			return int64(result), nil
		}
		return result, nil

	case "CEIL", "CEILING":
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
		case int:
			return int64(v), nil
		case int64:
			return v, nil
		case float64:
			return int64(math.Ceil(v)), nil
		default:
			return nil, fmt.Errorf("%s requires numeric argument", funcName)
		}

	case "FLOOR":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("FLOOR requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		switch v := arg.(type) {
		case int:
			return int64(v), nil
		case int64:
			return v, nil
		case float64:
			return int64(math.Floor(v)), nil
		default:
			return nil, fmt.Errorf("FLOOR requires numeric argument")
		}

	case "MOD":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("MOD requires 2 arguments")
		}
		arg1, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		arg2, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if arg1 == nil || arg2 == nil {
			return nil, nil
		}

		var dividend, divisor float64
		switch v := arg1.(type) {
		case int:
			dividend = float64(v)
		case int64:
			dividend = float64(v)
		case float64:
			dividend = v
		}
		switch v := arg2.(type) {
		case int:
			divisor = float64(v)
		case int64:
			divisor = float64(v)
		case float64:
			divisor = v
		}

		if divisor == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return int64(int(dividend) % int(divisor)), nil

	case "POWER", "POW":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("%s requires 2 arguments", funcName)
		}
		base, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		exp, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if base == nil || exp == nil {
			return nil, nil
		}

		var baseVal, expVal float64
		switch v := base.(type) {
		case int:
			baseVal = float64(v)
		case int64:
			baseVal = float64(v)
		case float64:
			baseVal = v
		}
		switch v := exp.(type) {
		case int:
			expVal = float64(v)
		case int64:
			expVal = float64(v)
		case float64:
			expVal = v
		}

		return math.Pow(baseVal, expVal), nil

	case "SQRT":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("SQRT requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}

		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return nil, fmt.Errorf("SQRT requires numeric argument")
		}

		if val < 0 {
			return nil, fmt.Errorf("SQRT of negative number")
		}
		return math.Sqrt(val), nil

	// ========== Date/Time Functions ==========
	case "DATE":
		if len(fc.Args) == 0 {
			return time.Now().Format("2006-01-02"), nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		// Parse the input and extract date part
		switch v := arg.(type) {
		case string:
			// Try various date formats
			formats := []string{
				"2006-01-02 15:04:05",
				"2006-01-02T15:04:05",
				"2006-01-02",
				time.RFC3339,
			}
			for _, fmt := range formats {
				if t, err := time.Parse(fmt, v); err == nil {
					return t.Format("2006-01-02"), nil
				}
			}
			return v, nil // Return as-is if can't parse
		case time.Time:
			return v.Format("2006-01-02"), nil
		default:
			return fmt.Sprintf("%v", v), nil
		}

	case "TIME":
		if len(fc.Args) == 0 {
			return time.Now().Format("15:04:05"), nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		switch v := arg.(type) {
		case string:
			formats := []string{
				"2006-01-02 15:04:05",
				"15:04:05",
				time.RFC3339,
			}
			for _, fmt := range formats {
				if t, err := time.Parse(fmt, v); err == nil {
					return t.Format("15:04:05"), nil
				}
			}
			return v, nil
		case time.Time:
			return v.Format("15:04:05"), nil
		default:
			return fmt.Sprintf("%v", v), nil
		}

	case "DATETIME":
		if len(fc.Args) == 0 {
			return time.Now().Format("2006-01-02 15:04:05"), nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		switch v := arg.(type) {
		case string:
			formats := []string{
				"2006-01-02 15:04:05",
				"2006-01-02T15:04:05",
				"2006-01-02",
				time.RFC3339,
			}
			for _, fmt := range formats {
				if t, err := time.Parse(fmt, v); err == nil {
					return t.Format("2006-01-02 15:04:05"), nil
				}
			}
			return v, nil
		case time.Time:
			return v.Format("2006-01-02 15:04:05"), nil
		default:
			return fmt.Sprintf("%v", v), nil
		}

	case "YEAR":
		if len(fc.Args) == 0 {
			return time.Now().Year(), nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		switch v := arg.(type) {
		case string:
			if t, err := time.Parse("2006-01-02", v); err == nil {
				return t.Year(), nil
			}
			if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
				return t.Year(), nil
			}
			return nil, fmt.Errorf("YEAR: invalid date format")
		case time.Time:
			return v.Year(), nil
		default:
			return nil, fmt.Errorf("YEAR requires date argument")
		}

	case "MONTH":
		if len(fc.Args) == 0 {
			return int(time.Now().Month()), nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		switch v := arg.(type) {
		case string:
			if t, err := time.Parse("2006-01-02", v); err == nil {
				return int(t.Month()), nil
			}
			if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
				return int(t.Month()), nil
			}
			return nil, fmt.Errorf("MONTH: invalid date format")
		case time.Time:
			return int(v.Month()), nil
		default:
			return nil, fmt.Errorf("MONTH requires date argument")
		}

	case "DAY":
		if len(fc.Args) == 0 {
			return time.Now().Day(), nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		switch v := arg.(type) {
		case string:
			if t, err := time.Parse("2006-01-02", v); err == nil {
				return t.Day(), nil
			}
			if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
				return t.Day(), nil
			}
			return nil, fmt.Errorf("DAY: invalid date format")
		case time.Time:
			return v.Day(), nil
		default:
			return nil, fmt.Errorf("DAY requires date argument")
		}

	case "HOUR":
		if len(fc.Args) == 0 {
			return time.Now().Hour(), nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		switch v := arg.(type) {
		case string:
			if t, err := time.Parse("15:04:05", v); err == nil {
				return t.Hour(), nil
			}
			if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
				return t.Hour(), nil
			}
			return nil, fmt.Errorf("HOUR: invalid time format")
		case time.Time:
			return v.Hour(), nil
		default:
			return nil, fmt.Errorf("HOUR requires time argument")
		}

	case "MINUTE":
		if len(fc.Args) == 0 {
			return time.Now().Minute(), nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		switch v := arg.(type) {
		case string:
			if t, err := time.Parse("15:04:05", v); err == nil {
				return t.Minute(), nil
			}
			if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
				return t.Minute(), nil
			}
			return nil, fmt.Errorf("MINUTE: invalid time format")
		case time.Time:
			return v.Minute(), nil
		default:
			return nil, fmt.Errorf("MINUTE requires time argument")
		}

	case "SECOND":
		if len(fc.Args) == 0 {
			return time.Now().Second(), nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		switch v := arg.(type) {
		case string:
			if t, err := time.Parse("15:04:05", v); err == nil {
				return t.Second(), nil
			}
			if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
				return t.Second(), nil
			}
			return nil, fmt.Errorf("SECOND: invalid time format")
		case time.Time:
			return v.Second(), nil
		default:
			return nil, fmt.Errorf("SECOND requires time argument")
		}

	case "DATE_ADD":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("DATE_ADD requires 2 arguments")
		}
		dateArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		intervalArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if dateArg == nil {
			return nil, nil
		}

		// Parse date
		var t time.Time
		switch v := dateArg.(type) {
		case string:
			if parsed, err := time.Parse("2006-01-02", v); err == nil {
				t = parsed
			} else if parsed, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
				t = parsed
			} else {
				return nil, fmt.Errorf("DATE_ADD: invalid date format")
			}
		case time.Time:
			t = v
		default:
			return nil, fmt.Errorf("DATE_ADD requires date argument")
		}

		// Get interval value
		var interval int
		switch v := intervalArg.(type) {
		case int:
			interval = v
		case int64:
			interval = int(v)
		case float64:
			interval = int(v)
		default:
			return nil, fmt.Errorf("DATE_ADD: interval must be numeric")
		}

		// Add days (default unit)
		return t.AddDate(0, 0, interval).Format("2006-01-02"), nil

	case "DATE_SUB":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("DATE_SUB requires 2 arguments")
		}
		dateArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		intervalArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if dateArg == nil {
			return nil, nil
		}

		var t time.Time
		switch v := dateArg.(type) {
		case string:
			if parsed, err := time.Parse("2006-01-02", v); err == nil {
				t = parsed
			} else if parsed, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
				t = parsed
			} else {
				return nil, fmt.Errorf("DATE_SUB: invalid date format")
			}
		case time.Time:
			t = v
		default:
			return nil, fmt.Errorf("DATE_SUB requires date argument")
		}

		var interval int
		switch v := intervalArg.(type) {
		case int:
			interval = v
		case int64:
			interval = int(v)
		case float64:
			interval = int(v)
		default:
			return nil, fmt.Errorf("DATE_SUB: interval must be numeric")
		}

		return t.AddDate(0, 0, -interval).Format("2006-01-02"), nil

	case "DATEDIFF", "DATE_DIFF":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("%s requires 2 arguments", funcName)
		}
		date1Arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		date2Arg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if date1Arg == nil || date2Arg == nil {
			return nil, nil
		}

		var t1, t2 time.Time
		switch v := date1Arg.(type) {
		case string:
			if parsed, err := time.Parse("2006-01-02", v); err == nil {
				t1 = parsed
			} else {
				return nil, fmt.Errorf("%s: invalid date format", funcName)
			}
		case time.Time:
			t1 = v
		}
		switch v := date2Arg.(type) {
		case string:
			if parsed, err := time.Parse("2006-01-02", v); err == nil {
				t2 = parsed
			} else {
				return nil, fmt.Errorf("%s: invalid date format", funcName)
			}
		case time.Time:
			t2 = v
		}

		diff := t1.Sub(t2)
		return int(diff.Hours() / 24), nil

	case "STRFTIME", "DATE_FORMAT":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("%s requires 2 arguments", funcName)
		}
		dateArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		formatArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if dateArg == nil {
			return nil, nil
		}

		var t time.Time
		switch v := dateArg.(type) {
		case string:
			if parsed, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
				t = parsed
			} else if parsed, err := time.Parse("2006-01-02", v); err == nil {
				t = parsed
			} else {
				return nil, fmt.Errorf("%s: invalid date format", funcName)
			}
		case time.Time:
			t = v
		default:
			return nil, fmt.Errorf("%s requires date argument", funcName)
		}

		format, ok := formatArg.(string)
		if !ok {
			return nil, fmt.Errorf("%s: format must be string", funcName)
		}

		// Convert SQLite-style format to Go format
		format = strings.ReplaceAll(format, "%Y", "2006")
		format = strings.ReplaceAll(format, "%m", "01")
		format = strings.ReplaceAll(format, "%d", "02")
		format = strings.ReplaceAll(format, "%H", "15")
		format = strings.ReplaceAll(format, "%M", "04")
		format = strings.ReplaceAll(format, "%S", "05")
		format = strings.ReplaceAll(format, "%s", "05")

		return t.Format(format), nil

	case "JULIANDAY":
		// Convert a date/time string to Julian day number
		// Julian day is the number of days since noon UTC on January 1, 4713 BCE
		if len(fc.Args) == 0 {
			// Return Julian day for current time
			return timeToJulianDay(time.Now()), nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}

		switch v := arg.(type) {
		case string:
			// Handle 'now' special case
			if strings.ToLower(v) == "now" {
				return timeToJulianDay(time.Now()), nil
			}
			// Try various date formats
			formats := []string{
				"2006-01-02 15:04:05",
				"2006-01-02T15:04:05",
				"2006-01-02",
				"15:04:05",
				time.RFC3339,
			}
			for _, fmt := range formats {
				if t, err := time.Parse(fmt, v); err == nil {
					return timeToJulianDay(t), nil
				}
			}
			return nil, fmt.Errorf("JULIANDAY: invalid date format: %s", v)
		case time.Time:
			return timeToJulianDay(v), nil
		case float64:
			// Already a Julian day number
			return v, nil
		case int:
			return float64(v), nil
		case int64:
			return float64(v), nil
		default:
			return nil, fmt.Errorf("JULIANDAY: unsupported argument type")
		}

	case "DATE_MODIFY":
		// SQLite-style date modification
		// Syntax: date_modify(date, '+N days', '-N months', etc.)
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("DATE_MODIFY requires at least 2 arguments")
		}

		dateArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if dateArg == nil {
			return nil, nil
		}

		// Parse the base date
		var t time.Time
		switch v := dateArg.(type) {
		case string:
			if strings.ToLower(v) == "now" {
				t = time.Now()
			} else if parsed, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
				t = parsed
			} else if parsed, err := time.Parse("2006-01-02", v); err == nil {
				t = parsed
			} else {
				return nil, fmt.Errorf("DATE_MODIFY: invalid date format")
			}
		case time.Time:
			t = v
		default:
			return nil, fmt.Errorf("DATE_MODIFY: requires date argument")
		}

		// Apply modifiers
		for i := 1; i < len(fc.Args); i++ {
			modArg, err := evalExpr(fc.Args[i])
			if err != nil {
				return nil, err
			}
			modStr, ok := modArg.(string)
			if !ok {
				modStr = fmt.Sprintf("%v", modArg)
			}
			t = applyDateModifier(t, modStr)
		}

		return t.Format("2006-01-02 15:04:05"), nil

	case "TIMESTAMPDIFF":
		// MySQL-style timestamp difference
		// Syntax: TIMESTAMPDIFF(unit, datetime1, datetime2)
		if len(fc.Args) < 3 {
			return nil, fmt.Errorf("TIMESTAMPDIFF requires 3 arguments")
		}

		unitArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		date1Arg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		date2Arg, err := evalExpr(fc.Args[2])
		if err != nil {
			return nil, err
		}

		if date1Arg == nil || date2Arg == nil {
			return nil, nil
		}

		// Get unit
		unit := "DAY"
		if u, ok := unitArg.(string); ok {
			unit = strings.ToUpper(u)
		}

		// Parse dates
		var t1, t2 time.Time
		switch v := date1Arg.(type) {
		case string:
			t1, err = parseDateTime(v)
			if err != nil {
				return nil, err
			}
		case time.Time:
			t1 = v
		default:
			return nil, fmt.Errorf("TIMESTAMPDIFF: invalid datetime1 type")
		}
		switch v := date2Arg.(type) {
		case string:
			t2, err = parseDateTime(v)
			if err != nil {
				return nil, err
			}
		case time.Time:
			t2 = v
		default:
			return nil, fmt.Errorf("TIMESTAMPDIFF: invalid datetime2 type")
		}

		return timestampDiff(unit, t1, t2), nil

	case "FROM_UNIXTIME":
		// Convert Unix timestamp to datetime string
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("FROM_UNIXTIME requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}

		var timestamp int64
		switch v := arg.(type) {
		case int:
			timestamp = int64(v)
		case int64:
			timestamp = v
		case float64:
			timestamp = int64(v)
		default:
			return nil, fmt.Errorf("FROM_UNIXTIME requires numeric argument")
		}

		t := time.Unix(timestamp, 0)
		return t.Format("2006-01-02 15:04:05"), nil

	case "UNIX_TIMESTAMP":
		// Convert datetime to Unix timestamp
		if len(fc.Args) == 0 {
			return time.Now().Unix(), nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}

		switch v := arg.(type) {
		case string:
			t, err := parseDateTime(v)
			if err != nil {
				return nil, err
			}
			return t.Unix(), nil
		case time.Time:
			return v.Unix(), nil
		default:
			return nil, fmt.Errorf("UNIX_TIMESTAMP: invalid argument type")
		}

	case "QUARTER":
		// Return quarter of year (1-4)
		if len(fc.Args) == 0 {
			return (int(time.Now().Month())-1)/3 + 1, nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}

		switch v := arg.(type) {
		case string:
			t, err := parseDateTime(v)
			if err != nil {
				return nil, err
			}
			return (int(t.Month())-1)/3 + 1, nil
		case time.Time:
			return (int(v.Month())-1)/3 + 1, nil
		default:
			return nil, fmt.Errorf("QUARTER: invalid argument type")
		}

	case "WEEK":
		// Return week of year (1-53)
		if len(fc.Args) == 0 {
			_, week := time.Now().ISOWeek()
			return week, nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}

		switch v := arg.(type) {
		case string:
			t, err := parseDateTime(v)
			if err != nil {
				return nil, err
			}
			_, week := t.ISOWeek()
			return week, nil
		case time.Time:
			_, week := v.ISOWeek()
			return week, nil
		default:
			return nil, fmt.Errorf("WEEK: invalid argument type")
		}

	case "WEEKDAY":
		// Return day of week (0=Monday, 6=Sunday)
		if len(fc.Args) == 0 {
			return int(time.Now().Weekday()), nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}

		switch v := arg.(type) {
		case string:
			t, err := parseDateTime(v)
			if err != nil {
				return nil, err
			}
			return int(t.Weekday()), nil
		case time.Time:
			return int(v.Weekday()), nil
		default:
			return nil, fmt.Errorf("WEEKDAY: invalid argument type")
		}

	case "DAYOFYEAR":
		// Return day of year (1-366)
		if len(fc.Args) == 0 {
			return time.Now().YearDay(), nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}

		switch v := arg.(type) {
		case string:
			t, err := parseDateTime(v)
			if err != nil {
				return nil, err
			}
			return t.YearDay(), nil
		case time.Time:
			return v.YearDay(), nil
		default:
			return nil, fmt.Errorf("DAYOFYEAR: invalid argument type")
		}

	case "DAYOFMONTH":
		// Return day of month (1-31)
		if len(fc.Args) == 0 {
			return time.Now().Day(), nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}

		switch v := arg.(type) {
		case string:
			t, err := parseDateTime(v)
			if err != nil {
				return nil, err
			}
			return t.Day(), nil
		case time.Time:
			return v.Day(), nil
		default:
			return nil, fmt.Errorf("DAYOFMONTH: invalid argument type")
		}

	case "DAYOFWEEK":
		// Return day of week (1=Sunday, 7=Saturday) - MySQL style
		if len(fc.Args) == 0 {
			return int(time.Now().Weekday()) + 1, nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}

		switch v := arg.(type) {
		case string:
			t, err := parseDateTime(v)
			if err != nil {
				return nil, err
			}
			return int(t.Weekday()) + 1, nil
		case time.Time:
			return int(v.Weekday()) + 1, nil
		default:
			return nil, fmt.Errorf("DAYOFWEEK: invalid argument type")
		}

	case "LAST_DAY":
		// Return the last day of the month for the given date
		if len(fc.Args) == 0 {
			now := time.Now()
			return time.Date(now.Year(), now.Month()+1, 0, 0, 0, 0, 0, time.Local).Format("2006-01-02"), nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}

		switch v := arg.(type) {
		case string:
			t, err := parseDateTime(v)
			if err != nil {
				return nil, err
			}
			// Last day = day 0 of next month
			lastDay := time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, t.Location())
			return lastDay.Format("2006-01-02"), nil
		case time.Time:
			lastDay := time.Date(v.Year(), v.Month()+1, 0, 0, 0, 0, 0, v.Location())
			return lastDay.Format("2006-01-02"), nil
		default:
			return nil, fmt.Errorf("LAST_DAY: invalid argument type")
		}

	// ========== String Functions ==========
	case "TRIM":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("TRIM requires at least 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		str, ok := arg.(string)
		if !ok {
			str = fmt.Sprintf("%v", arg)
		}
		return strings.TrimSpace(str), nil

	case "LTRIM":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("LTRIM requires at least 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		str, ok := arg.(string)
		if !ok {
			str = fmt.Sprintf("%v", arg)
		}
		return strings.TrimLeft(str, " \t\n\r"), nil

	case "RTRIM":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("RTRIM requires at least 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		str, ok := arg.(string)
		if !ok {
			str = fmt.Sprintf("%v", arg)
		}
		return strings.TrimRight(str, " \t\n\r"), nil

	case "INSTR", "POSITION":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("%s requires 2 arguments", funcName)
		}
		strArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		subArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if strArg == nil || subArg == nil {
			return nil, nil
		}
		str, ok := strArg.(string)
		if !ok {
			str = fmt.Sprintf("%v", strArg)
		}
		sub, ok := subArg.(string)
		if !ok {
			sub = fmt.Sprintf("%v", subArg)
		}
		return strings.Index(str, sub) + 1, nil // 1-indexed, 0 if not found

	case "LPAD":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("LPAD requires at least 2 arguments")
		}
		strArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		lenArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if strArg == nil {
			return nil, nil
		}
		str, ok := strArg.(string)
		if !ok {
			str = fmt.Sprintf("%v", strArg)
		}
		targetLen := 0
		switch v := lenArg.(type) {
		case int:
			targetLen = v
		case int64:
			targetLen = int(v)
		case float64:
			targetLen = int(v)
		}

		padStr := " "
		if len(fc.Args) >= 3 {
			padArg, err := evalExpr(fc.Args[2])
			if err != nil {
				return nil, err
			}
			if padArg != nil {
				if ps, ok := padArg.(string); ok {
					padStr = ps
				}
			}
		}

		if len(str) >= targetLen {
			return str[:targetLen], nil
		}
		padLen := targetLen - len(str)
		padding := strings.Repeat(padStr, (padLen+len(padStr)-1)/len(padStr))
		return padding[:padLen] + str, nil

	case "RPAD":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("RPAD requires at least 2 arguments")
		}
		strArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		lenArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if strArg == nil {
			return nil, nil
		}
		str, ok := strArg.(string)
		if !ok {
			str = fmt.Sprintf("%v", strArg)
		}
		targetLen := 0
		switch v := lenArg.(type) {
		case int:
			targetLen = v
		case int64:
			targetLen = int(v)
		case float64:
			targetLen = int(v)
		}

		padStr := " "
		if len(fc.Args) >= 3 {
			padArg, err := evalExpr(fc.Args[2])
			if err != nil {
				return nil, err
			}
			if padArg != nil {
				if ps, ok := padArg.(string); ok {
					padStr = ps
				}
			}
		}

		if len(str) >= targetLen {
			return str[:targetLen], nil
		}
		padLen := targetLen - len(str)
		padding := strings.Repeat(padStr, (padLen+len(padStr)-1)/len(padStr))
		return str + padding[:padLen], nil

	case "REVERSE":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("REVERSE requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		str, ok := arg.(string)
		if !ok {
			str = fmt.Sprintf("%v", arg)
		}
		runes := []rune(str)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes), nil

	case "LEFT":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("LEFT requires 2 arguments")
		}
		strArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		nArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if strArg == nil {
			return nil, nil
		}
		str, ok := strArg.(string)
		if !ok {
			str = fmt.Sprintf("%v", strArg)
		}
		n := 0
		switch v := nArg.(type) {
		case int:
			n = v
		case int64:
			n = int(v)
		case float64:
			n = int(v)
		}
		runes := []rune(str)
		if n > len(runes) {
			n = len(runes)
		}
		if n < 0 {
			n = 0
		}
		return string(runes[:n]), nil

	case "RIGHT":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("RIGHT requires 2 arguments")
		}
		strArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		nArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if strArg == nil {
			return nil, nil
		}
		str, ok := strArg.(string)
		if !ok {
			str = fmt.Sprintf("%v", strArg)
		}
		n := 0
		switch v := nArg.(type) {
		case int:
			n = v
		case int64:
			n = int(v)
		case float64:
			n = int(v)
		}
		runes := []rune(str)
		if n > len(runes) {
			n = len(runes)
		}
		if n < 0 {
			n = 0
		}
		return string(runes[len(runes)-n:]), nil

	// ========== Utility Functions ==========
	case "TYPEOF":
		if len(fc.Args) == 0 {
			return "null", nil
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return "null", nil
		}
		switch arg.(type) {
		case int, int64:
			return "integer", nil
		case float64:
			return "real", nil
		case string:
			return "text", nil
		case bool:
			return "boolean", nil
		case []byte:
			return "blob", nil
		case time.Time:
			return "datetime", nil
		default:
			return "unknown", nil
		}

	case "IIF":
		if len(fc.Args) < 3 {
			return nil, fmt.Errorf("IIF requires 3 arguments")
		}
		condArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		cond, ok := condArg.(bool)
		if !ok {
			cond = condArg != nil && condArg != false && condArg != 0 && condArg != ""
		}
		if cond {
			return evalExpr(fc.Args[1])
		}
		return evalExpr(fc.Args[2])

	case "GREATEST":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("GREATEST requires at least 1 argument")
		}
		var maxVal interface{}
		for i, arg := range fc.Args {
			val, err := evalExpr(arg)
			if err != nil {
				return nil, err
			}
			if i == 0 {
				maxVal = val
				continue
			}
			if val == nil {
				continue
			}
			if maxVal == nil {
				maxVal = val
				continue
			}
			cmp := compareValues(maxVal, val)
			if cmp < 0 {
				maxVal = val
			}
		}
		return maxVal, nil

	case "LEAST":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("LEAST requires at least 1 argument")
		}
		var minVal interface{}
		for i, arg := range fc.Args {
			val, err := evalExpr(arg)
			if err != nil {
				return nil, err
			}
			if i == 0 {
				minVal = val
				continue
			}
			if val == nil {
				continue
			}
			if minVal == nil {
				minVal = val
				continue
			}
			cmp := compareValues(minVal, val)
			if cmp > 0 {
				minVal = val
			}
		}
		return minVal, nil

	// ========== More Math Functions ==========
	case "SIGN":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("SIGN requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		switch v := arg.(type) {
		case int:
			if v > 0 {
				return 1, nil
			} else if v < 0 {
				return -1, nil
			}
			return 0, nil
		case int64:
			if v > 0 {
				return 1, nil
			} else if v < 0 {
				return -1, nil
			}
			return 0, nil
		case float64:
			if v > 0 {
				return 1, nil
			} else if v < 0 {
				return -1, nil
			}
			return 0, nil
		default:
			return nil, fmt.Errorf("SIGN requires numeric argument")
		}

	case "RANDOM":
		return time.Now().UnixNano(), nil

	case "LOG":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("LOG requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return nil, fmt.Errorf("LOG requires numeric argument")
		}
		if val <= 0 {
			return nil, fmt.Errorf("LOG of non-positive number")
		}
		return math.Log(val), nil

	case "LOG10":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("LOG10 requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return nil, fmt.Errorf("LOG10 requires numeric argument")
		}
		if val <= 0 {
			return nil, fmt.Errorf("LOG10 of non-positive number")
		}
		return math.Log10(val), nil

	case "EXP":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("EXP requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return nil, fmt.Errorf("EXP requires numeric argument")
		}
		return math.Exp(val), nil

	case "PI":
		return math.Pi, nil

	// ========== More String Functions ==========
	case "REPLACE":
		if len(fc.Args) < 3 {
			return nil, fmt.Errorf("REPLACE requires 3 arguments")
		}
		strArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		fromArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		toArg, err := evalExpr(fc.Args[2])
		if err != nil {
			return nil, err
		}
		if strArg == nil {
			return nil, nil
		}
		str, ok := strArg.(string)
		if !ok {
			str = fmt.Sprintf("%v", strArg)
		}
		from := ""
		if fromArg != nil {
			if s, ok := fromArg.(string); ok {
				from = s
			} else {
				from = fmt.Sprintf("%v", fromArg)
			}
		}
		to := ""
		if toArg != nil {
			if s, ok := toArg.(string); ok {
				to = s
			} else {
				to = fmt.Sprintf("%v", toArg)
			}
		}
		return strings.ReplaceAll(str, from, to), nil

	case "CHAR":
		var chars []rune
		for _, arg := range fc.Args {
			val, err := evalExpr(arg)
			if err != nil {
				return nil, err
			}
			if val == nil {
				continue
			}
			var code int
			switch v := val.(type) {
			case int:
				code = v
			case int64:
				code = int(v)
			case float64:
				code = int(v)
			}
			if code >= 0 && code <= 0x10FFFF {
				chars = append(chars, rune(code))
			}
		}
		return string(chars), nil

	case "UNICODE":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("UNICODE requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		str, ok := arg.(string)
		if !ok {
			str = fmt.Sprintf("%v", arg)
		}
		if len(str) == 0 {
			return nil, nil
		}
		return int(rune(str[0])), nil

	case "ASCII":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("ASCII requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		str, ok := arg.(string)
		if !ok {
			str = fmt.Sprintf("%v", arg)
		}
		if len(str) == 0 {
			return nil, nil
		}
		return int(str[0]), nil

	// ========== Utility Functions ==========
	case "LAST_INSERT_ID":
		// Return the last inserted ID from the session
		if e.lastInsertID > 0 {
			return e.lastInsertID, nil
		}
		return int64(0), nil

	case "ROW_COUNT":
		// Return the number of affected rows from last operation
		return e.lastRowCount, nil

	case "UUID":
		// Generate a random UUID v4
		uuid := make([]byte, 16)
		// Use current time for some randomness
		nano := time.Now().UnixNano()
		for i := 0; i < 8; i++ {
			uuid[i] = byte(nano >> (i * 8))
		}
		// Add more pseudo-random data
		nano2 := nano ^ (nano >> 32)
		for i := 8; i < 16; i++ {
			uuid[i] = byte(nano2 >> ((i - 8) * 8))
		}
		// Set version (4) and variant bits
		uuid[6] = (uuid[6] & 0x0f) | 0x40
		uuid[8] = (uuid[8] & 0x3f) | 0x80
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16]), nil

	case "REPEAT":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("REPEAT requires 2 arguments")
		}
		strArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		nArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if strArg == nil {
			return nil, nil
		}
		str, ok := strArg.(string)
		if !ok {
			str = fmt.Sprintf("%v", strArg)
		}
		n := 0
		switch v := nArg.(type) {
		case int:
			n = v
		case int64:
			n = int(v)
		case float64:
			n = int(v)
		}
		if n < 0 {
			n = 0
		}
		return strings.Repeat(str, n), nil

	case "SPACE":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("SPACE requires 1 argument")
		}
		nArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		n := 0
		switch v := nArg.(type) {
		case int:
			n = v
		case int64:
			n = int(v)
		case float64:
			n = int(v)
		}
		if n < 0 {
			n = 0
		}
		return strings.Repeat(" ", n), nil

	case "CONCAT_WS":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("CONCAT_WS requires at least 2 arguments")
		}
		sepArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		sep := ","
		if sepArg != nil {
			if s, ok := sepArg.(string); ok {
				sep = s
			}
		}

		var parts []string
		for i := 1; i < len(fc.Args); i++ {
			arg, err := evalExpr(fc.Args[i])
			if err != nil {
				return nil, err
			}
			if arg != nil {
				if s, ok := arg.(string); ok {
					parts = append(parts, s)
				} else {
					parts = append(parts, fmt.Sprintf("%v", arg))
				}
			}
		}
		return strings.Join(parts, sep), nil

	// ========== Utility Functions ==========
	case "IFNULL":
		if len(fc.Args) < 1 {
			return nil, fmt.Errorf("IFNULL requires at least 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg != nil {
			return arg, nil
		}
		if len(fc.Args) >= 2 {
			return evalExpr(fc.Args[1])
		}
		return nil, nil

	case "COALESCE":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("COALESCE requires at least 1 argument")
		}
		for _, argExpr := range fc.Args {
			arg, err := evalExpr(argExpr)
			if err != nil {
				return nil, err
			}
			if arg != nil {
				return arg, nil
			}
		}
		return nil, nil

	case "NULLIF":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("NULLIF requires 2 arguments")
		}
		arg1, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		arg2, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		// If arg1 equals arg2, return NULL
		if arg1 != nil && arg2 != nil {
			if fmt.Sprintf("%v", arg1) == fmt.Sprintf("%v", arg2) {
				return nil, nil
			}
		}
		return arg1, nil

	case "REGEXP":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("REGEXP requires 2 arguments")
		}
		strArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		patternArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if strArg == nil || patternArg == nil {
			return false, nil
		}
		str, ok := strArg.(string)
		if !ok {
			str = fmt.Sprintf("%v", strArg)
		}
		pattern, ok := patternArg.(string)
		if !ok {
			pattern = fmt.Sprintf("%v", patternArg)
		}
		matched, err := regexp.MatchString(pattern, str)
		if err != nil {
			return nil, fmt.Errorf("REGEXP: invalid pattern: %v", err)
		}
		return matched, nil

	// ========== JSON Functions ==========
	case "JSON_EXTRACT":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("JSON_EXTRACT requires at least 2 arguments")
		}
		jsonArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if jsonArg == nil {
			return nil, nil
		}
		jsonStr, ok := jsonArg.(string)
		if !ok {
			jsonStr = fmt.Sprintf("%v", jsonArg)
		}
		// Parse JSON
		var jsonVal interface{}
		if err := json.Unmarshal([]byte(jsonStr), &jsonVal); err != nil {
			return nil, fmt.Errorf("JSON_EXTRACT: invalid JSON: %v", err)
		}
		// Extract path (simplified - supports $.key and $[index])
		pathArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		path, ok := pathArg.(string)
		if !ok {
			path = fmt.Sprintf("%v", pathArg)
		}
		result := extractJSONPath(jsonVal, path)
		return result, nil

	case "JSON_ARRAY":
		arr := make([]interface{}, len(fc.Args))
		for i, arg := range fc.Args {
			val, err := evalExpr(arg)
			if err != nil {
				return nil, err
			}
			arr[i] = val
		}
		bytes, err := json.Marshal(arr)
		if err != nil {
			return nil, err
		}
		return string(bytes), nil

	case "JSON_OBJECT":
		if len(fc.Args)%2 != 0 {
			return nil, fmt.Errorf("JSON_OBJECT requires even number of arguments")
		}
		obj := make(map[string]interface{})
		for i := 0; i < len(fc.Args); i += 2 {
			keyArg, err := evalExpr(fc.Args[i])
			if err != nil {
				return nil, err
			}
			key, ok := keyArg.(string)
			if !ok {
				key = fmt.Sprintf("%v", keyArg)
			}
			val, err := evalExpr(fc.Args[i+1])
			if err != nil {
				return nil, err
			}
			obj[key] = val
		}
		bytes, err := json.Marshal(obj)
		if err != nil {
			return nil, err
		}
		return string(bytes), nil

	case "JSON_TYPE":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("JSON_TYPE requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return "NULL", nil
		}
		jsonStr, ok := arg.(string)
		if !ok {
			jsonStr = fmt.Sprintf("%v", arg)
		}
		var jsonVal interface{}
		if err := json.Unmarshal([]byte(jsonStr), &jsonVal); err != nil {
			return nil, fmt.Errorf("JSON_TYPE: invalid JSON: %v", err)
		}
		return getJSONType(jsonVal), nil

	case "JSON_UNQUOTE":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("JSON_UNQUOTE requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		jsonStr, ok := arg.(string)
		if !ok {
			jsonStr = fmt.Sprintf("%v", arg)
		}
		var jsonVal interface{}
		if err := json.Unmarshal([]byte(jsonStr), &jsonVal); err != nil {
			// Not valid JSON, return as-is
			return jsonStr, nil
		}
		if str, ok := jsonVal.(string); ok {
			return str, nil
		}
		bytes, _ := json.Marshal(jsonVal)
		return string(bytes), nil

	case "JSON_QUOTE":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("JSON_QUOTE requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(arg)
		if err != nil {
			return nil, err
		}
		return string(data), nil

	case "JSON_CONTAINS":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("JSON_CONTAINS requires at least 2 arguments")
		}
		targetArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		candidateArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if targetArg == nil || candidateArg == nil {
			return nil, nil
		}
		targetStr, _ := targetArg.(string)
		candidateStr, _ := candidateArg.(string)
		if targetStr == "" {
			targetStr = fmt.Sprintf("%v", targetArg)
		}
		if candidateStr == "" {
			candidateStr = fmt.Sprintf("%v", candidateArg)
		}
		// Parse both JSON values
		var target, candidate interface{}
		if err := json.Unmarshal([]byte(targetStr), &target); err != nil {
			return false, nil
		}
		if err := json.Unmarshal([]byte(candidateStr), &candidate); err != nil {
			return false, nil
		}
		return jsonContainsValue(target, candidate), nil

	case "JSON_VALID":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("JSON_VALID requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return false, nil
		}
		jsonStr, ok := arg.(string)
		if !ok {
			jsonStr = fmt.Sprintf("%v", arg)
		}
		var jsonVal interface{}
		err = json.Unmarshal([]byte(jsonStr), &jsonVal)
		return err == nil, nil

	case "JSON_KEYS":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("JSON_KEYS requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		jsonStr, ok := arg.(string)
		if !ok {
			jsonStr = fmt.Sprintf("%v", arg)
		}
		var jsonVal interface{}
		if err := json.Unmarshal([]byte(jsonStr), &jsonVal); err != nil {
			return nil, fmt.Errorf("JSON_KEYS: invalid JSON: %v", err)
		}
		obj, ok := jsonVal.(map[string]interface{})
		if !ok {
			return nil, nil
		}
		keys := make([]interface{}, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		// Return as JSON array
		bytes, _ := json.Marshal(keys)
		return string(bytes), nil

	case "JSON_LENGTH":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("JSON_LENGTH requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		jsonStr, ok := arg.(string)
		if !ok {
			jsonStr = fmt.Sprintf("%v", arg)
		}
		var jsonVal interface{}
		if err := json.Unmarshal([]byte(jsonStr), &jsonVal); err != nil {
			return nil, fmt.Errorf("JSON_LENGTH: invalid JSON: %v", err)
		}
		switch v := jsonVal.(type) {
		case []interface{}:
			return int64(len(v)), nil
		case map[string]interface{}:
			return int64(len(v)), nil
		default:
			return int64(1), nil
		}

	case "JSON_SET":
		if len(fc.Args) < 3 {
			return nil, fmt.Errorf("JSON_SET requires at least 3 arguments")
		}
		jsonArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if jsonArg == nil {
			return nil, nil
		}
		jsonStr, ok := jsonArg.(string)
		if !ok {
			jsonStr = fmt.Sprintf("%v", jsonArg)
		}
		var jsonVal interface{}
		if err := json.Unmarshal([]byte(jsonStr), &jsonVal); err != nil {
			return nil, fmt.Errorf("JSON_SET: invalid JSON: %v", err)
		}
		// Process path-value pairs
		for i := 1; i < len(fc.Args); i += 2 {
			if i+1 >= len(fc.Args) {
				break
			}
			pathArg, _ := evalExpr(fc.Args[i])
			valArg, _ := evalExpr(fc.Args[i+1])
			path, _ := pathArg.(string)
			jsonVal = jsonSetPath(jsonVal, path, valArg)
		}
		bytes, err := json.Marshal(jsonVal)
		if err != nil {
			return nil, err
		}
		return string(bytes), nil

	case "JSON_REPLACE":
		if len(fc.Args) < 3 {
			return nil, fmt.Errorf("JSON_REPLACE requires at least 3 arguments")
		}
		jsonArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if jsonArg == nil {
			return nil, nil
		}
		jsonStr, ok := jsonArg.(string)
		if !ok {
			jsonStr = fmt.Sprintf("%v", jsonArg)
		}
		var jsonVal interface{}
		if err := json.Unmarshal([]byte(jsonStr), &jsonVal); err != nil {
			return nil, fmt.Errorf("JSON_REPLACE: invalid JSON: %v", err)
		}
		// Process path-value pairs (only replace existing paths)
		for i := 1; i < len(fc.Args); i += 2 {
			if i+1 >= len(fc.Args) {
				break
			}
			pathArg, _ := evalExpr(fc.Args[i])
			valArg, _ := evalExpr(fc.Args[i+1])
			path, _ := pathArg.(string)
			jsonVal = jsonReplacePath(jsonVal, path, valArg)
		}
		bytes, err := json.Marshal(jsonVal)
		if err != nil {
			return nil, err
		}
		return string(bytes), nil

	case "JSON_REMOVE":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("JSON_REMOVE requires at least 2 arguments")
		}
		jsonArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if jsonArg == nil {
			return nil, nil
		}
		jsonStr, ok := jsonArg.(string)
		if !ok {
			jsonStr = fmt.Sprintf("%v", jsonArg)
		}
		var jsonVal interface{}
		if err := json.Unmarshal([]byte(jsonStr), &jsonVal); err != nil {
			return nil, fmt.Errorf("JSON_REMOVE: invalid JSON: %v", err)
		}
		// Remove each specified path
		for i := 1; i < len(fc.Args); i++ {
			pathArg, _ := evalExpr(fc.Args[i])
			path, _ := pathArg.(string)
			jsonVal = jsonRemovePath(jsonVal, path)
		}
		bytes, err := json.Marshal(jsonVal)
		if err != nil {
			return nil, err
		}
		return string(bytes), nil

	case "JSON_MERGE_PATCH":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("JSON_MERGE_PATCH requires at least 2 arguments")
		}
		var result map[string]interface{}
		for _, arg := range fc.Args {
			val, err := evalExpr(arg)
			if err != nil {
				return nil, err
			}
			if val == nil {
				continue
			}
			jsonStr, ok := val.(string)
			if !ok {
				jsonStr = fmt.Sprintf("%v", val)
			}
			var jsonObj map[string]interface{}
			if err := json.Unmarshal([]byte(jsonStr), &jsonObj); err != nil {
				return nil, fmt.Errorf("JSON_MERGE_PATCH: invalid JSON: %v", err)
			}
			if result == nil {
				result = jsonObj
			} else {
				result = jsonMergePatch(result, jsonObj)
			}
		}
		bytes, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		return string(bytes), nil

	// ========== Additional Math Functions ==========
	case "TRUNCATE":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("TRUNCATE requires 2 arguments")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return nil, fmt.Errorf("TRUNCATE requires numeric argument")
		}
		precArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		var precision int
		switch v := precArg.(type) {
		case int:
			precision = v
		case int64:
			precision = int(v)
		case float64:
			precision = int(v)
		}
		multiplier := math.Pow(10, float64(precision))
		result := math.Trunc(val*multiplier) / multiplier
		if precision == 0 {
			return int64(result), nil
		}
		return result, nil

	case "COS":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("COS requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return nil, fmt.Errorf("COS requires numeric argument")
		}
		return math.Cos(val), nil

	case "SIN":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("SIN requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return nil, fmt.Errorf("SIN requires numeric argument")
		}
		return math.Sin(val), nil

	case "TAN":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("TAN requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return nil, fmt.Errorf("TAN requires numeric argument")
		}
		return math.Tan(val), nil

	case "ACOS":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("ACOS requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return nil, fmt.Errorf("ACOS requires numeric argument")
		}
		if val < -1 || val > 1 {
			return nil, nil
		}
		return math.Acos(val), nil

	case "ASIN":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("ASIN requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return nil, fmt.Errorf("ASIN requires numeric argument")
		}
		if val < -1 || val > 1 {
			return nil, nil
		}
		return math.Asin(val), nil

	case "ATAN":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("ATAN requires at least 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return nil, fmt.Errorf("ATAN requires numeric argument")
		}
		if len(fc.Args) >= 2 {
			arg2, err := evalExpr(fc.Args[1])
			if err != nil {
				return nil, err
			}
			var val2 float64
			switch v := arg2.(type) {
			case int:
				val2 = float64(v)
			case int64:
				val2 = float64(v)
			case float64:
				val2 = v
			default:
				return nil, fmt.Errorf("ATAN requires numeric argument")
			}
			return math.Atan2(val, val2), nil
		}
		return math.Atan(val), nil

	case "ATAN2":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("ATAN2 requires 2 arguments")
		}
		arg1, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		arg2, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if arg1 == nil || arg2 == nil {
			return nil, nil
		}
		var y, x float64
		switch v := arg1.(type) {
		case int:
			y = float64(v)
		case int64:
			y = float64(v)
		case float64:
			y = v
		}
		switch v := arg2.(type) {
		case int:
			x = float64(v)
		case int64:
			x = float64(v)
		case float64:
			x = v
		}
		return math.Atan2(y, x), nil

	case "COT":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("COT requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return nil, fmt.Errorf("COT requires numeric argument")
		}
		sinVal := math.Sin(val)
		if sinVal == 0 {
			return nil, nil
		}
		return math.Cos(val) / sinVal, nil

	case "DEGREES":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("DEGREES requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return nil, fmt.Errorf("DEGREES requires numeric argument")
		}
		return val * 180 / math.Pi, nil

	case "RADIANS":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("RADIANS requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return nil, fmt.Errorf("RADIANS requires numeric argument")
		}
		return val * math.Pi / 180, nil

	case "RAND":
		if len(fc.Args) == 0 {
			return rand.Float64(), nil
		}
		seedArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		var seed int64
		switch v := seedArg.(type) {
		case int:
			seed = int64(v)
		case int64:
			seed = v
		case float64:
			seed = int64(v)
		default:
			return rand.Float64(), nil
		}
		r := rand.New(rand.NewSource(seed))
		return r.Float64(), nil

	// ========== Additional String Functions ==========
	case "FORMAT":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("FORMAT requires at least 2 arguments")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		precisionArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		var precision int
		switch v := precisionArg.(type) {
		case int:
			precision = v
		case int64:
			precision = int(v)
		case float64:
			precision = int(v)
		}
		var val float64
		switch v := arg.(type) {
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case float64:
			val = v
		default:
			return fmt.Sprintf("%v", arg), nil
		}
		return fmt.Sprintf(fmt.Sprintf("%%.%df", precision), val), nil

	case "SOUNDEX":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("SOUNDEX requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		str, ok := arg.(string)
		if !ok {
			str = fmt.Sprintf("%v", arg)
		}
		return soundex(str), nil

	case "DIFFERENCE":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("DIFFERENCE requires 2 arguments")
		}
		arg1, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		arg2, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		if arg1 == nil || arg2 == nil {
			return nil, nil
		}
		str1, ok := arg1.(string)
		if !ok {
			str1 = fmt.Sprintf("%v", arg1)
		}
		str2, ok := arg2.(string)
		if !ok {
			str2 = fmt.Sprintf("%v", arg2)
		}
		return soundexDifference(str1, str2), nil

	case "MAKEDATE":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("MAKEDATE requires 2 arguments")
		}
		yearArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		dayArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		var year, day int
		switch v := yearArg.(type) {
		case int:
			year = v
		case int64:
			year = int(v)
		case float64:
			year = int(v)
		}
		switch v := dayArg.(type) {
		case int:
			day = v
		case int64:
			day = int(v)
		case float64:
			day = int(v)
		}
		t := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
		t = t.AddDate(0, 0, day-1)
		return t.Format("2006-01-02"), nil

	case "MAKETIME":
		if len(fc.Args) < 3 {
			return nil, fmt.Errorf("MAKETIME requires 3 arguments")
		}
		hourArg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		minArg, err := evalExpr(fc.Args[1])
		if err != nil {
			return nil, err
		}
		secArg, err := evalExpr(fc.Args[2])
		if err != nil {
			return nil, err
		}
		var hour, min, sec int
		switch v := hourArg.(type) {
		case int:
			hour = v
		case int64:
			hour = int(v)
		case float64:
			hour = int(v)
		}
		switch v := minArg.(type) {
		case int:
			min = v
		case int64:
			min = int(v)
		case float64:
			min = int(v)
		}
		switch v := secArg.(type) {
		case int:
			sec = v
		case int64:
			sec = int(v)
		case float64:
			sec = int(v)
		}
		return fmt.Sprintf("%02d:%02d:%02d", hour, min, sec), nil

	case "SEC_TO_TIME":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("SEC_TO_TIME requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		var secs int
		switch v := arg.(type) {
		case int:
			secs = v
		case int64:
			secs = int(v)
		case float64:
			secs = int(v)
		}
		hours := secs / 3600
		secs %= 3600
		mins := secs / 60
		secs %= 60
		return fmt.Sprintf("%02d:%02d:%02d", hours, mins, secs), nil

	case "TIME_TO_SEC":
		if len(fc.Args) == 0 {
			return nil, fmt.Errorf("TIME_TO_SEC requires 1 argument")
		}
		arg, err := evalExpr(fc.Args[0])
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		timeStr, ok := arg.(string)
		if !ok {
			timeStr = fmt.Sprintf("%v", arg)
		}
		parts := strings.Split(timeStr, ":")
		if len(parts) >= 2 {
			hours, _ := strconv.Atoi(parts[0])
			mins, _ := strconv.Atoi(parts[1])
			secs := 0
			if len(parts) >= 3 {
				secs, _ = strconv.Atoi(parts[2])
			}
			return int64(hours*3600 + mins*60 + secs), nil
		}
		return int64(0), nil

	// ========== System Functions ==========
	case "USER", "CURRENT_USER":
		return "admin", nil // Default user for now

	case "VERSION":
		return "XxSQL 0.0.5", nil

	case "CONNECTION_ID":
		return int64(1), nil // Single connection for now

	// ========== More Aggregate Functions ==========
	// STDDEV and VARIANCE are handled in executeGroupBy

	default:
		// Check for user-defined function
		// Check for script-based UDF first (new style)
		if e.scriptUDFMgr != nil {
			if fn, exists := e.scriptUDFMgr.GetFunction(funcName); exists {
				// Evaluate arguments
				argValues := make([]interface{}, len(fc.Args))
				for i, arg := range fc.Args {
					val, err := evalExpr(arg)
					if err != nil {
						return nil, err
					}
					argValues[i] = val
				}
				return e.callScriptFunction(fn, argValues)
			}
		}
		// Check for old style UDF
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
// Optional escapeChar is used for LIKE operations.
func (e *Executor) compareValues(left interface{}, op sql.BinaryOp, right interface{}, escapeChar ...string) (bool, error) {
	esc := ""
	if len(escapeChar) > 0 {
		esc = escapeChar[0]
	}

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
		// LIKE with optional escape character
		return matchLikePattern(leftStr, rightStr, esc), nil
	case sql.OpGlob:
		// GLOB uses Unix-style wildcards: * and ?
		// Convert GLOB pattern to regex
		pattern := globToRegex(rightStr)
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

// Collation functions for string comparison

// collationCompare compares two strings using the specified collation.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func collationCompare(a, b string, collation string) int {
	switch strings.ToUpper(collation) {
	case "NOCASE":
		return collationCompareNOCASE(a, b)
	case "RTRIM":
		return collationCompareRTRIM(a, b)
	case "BINARY":
		fallthrough
	default:
		// BINARY is the default - exact byte comparison
		return strings.Compare(a, b)
	}
}

// collationCompareNOCASE compares strings case-insensitively.
func collationCompareNOCASE(a, b string) int {
	// Compare character by character, case-insensitively
	aUpper := strings.ToUpper(a)
	bUpper := strings.ToUpper(b)
	return strings.Compare(aUpper, bUpper)
}

// collationCompareRTRIM compares strings with trailing spaces ignored.
func collationCompareRTRIM(a, b string) int {
	// Trim trailing spaces and compare
	aTrimmed := strings.TrimRight(a, " ")
	bTrimmed := strings.TrimRight(b, " ")
	return strings.Compare(aTrimmed, bTrimmed)
}

// collationEqual checks if two strings are equal using the specified collation.
func collationEqual(a, b string, collation string) bool {
	return collationCompare(a, b, collation) == 0
}

// compareValuesWithCollation compares values with optional collation support.
// escapeChar is used for LIKE operations.
func (e *Executor) compareValuesWithCollation(left interface{}, op sql.BinaryOp, right interface{}, collation string, escapeChar ...string) (bool, error) {
	esc := ""
	if len(escapeChar) > 0 {
		esc = escapeChar[0]
	}

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

	// Try numeric comparison first
	leftFloat, leftIsFloat := toFloat64(left)
	rightFloat, rightIsFloat := toFloat64(right)
	if leftIsFloat && rightIsFloat {
		switch op {
		case sql.OpEq:
			return leftFloat == rightFloat, nil
		case sql.OpNe:
			return leftFloat != rightFloat, nil
		case sql.OpLt:
			return leftFloat < rightFloat, nil
		case sql.OpLe:
			return leftFloat <= rightFloat, nil
		case sql.OpGt:
			return leftFloat > rightFloat, nil
		case sql.OpGe:
			return leftFloat >= rightFloat, nil
		}
	}

	// Convert to comparable values
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)

	// Use collation-aware comparison if specified
	if collation != "" {
		cmp := collationCompare(leftStr, rightStr, collation)
		switch op {
		case sql.OpEq:
			return cmp == 0, nil
		case sql.OpNe:
			return cmp != 0, nil
		case sql.OpLt:
			return cmp < 0, nil
		case sql.OpLe:
			return cmp <= 0, nil
		case sql.OpGt:
			return cmp > 0, nil
		case sql.OpGe:
			return cmp >= 0, nil
		case sql.OpLike:
			// For LIKE with collation, use case-insensitive matching for NOCASE
			if strings.ToUpper(collation) == "NOCASE" {
				return matchLikePattern(strings.ToLower(leftStr), strings.ToLower(rightStr), esc), nil
			}
			// Fall through for other collations
		case sql.OpGlob:
			// GLOB uses Unix-style wildcards: * and ?
			pattern := globToRegex(rightStr)
			matched, _ := regexp.MatchString(pattern, leftStr)
			return matched, nil
		}
	}

	// Standard comparison (BINARY collation default)
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
		// LIKE with optional escape character
		return matchLikePattern(leftStr, rightStr, esc), nil
	case sql.OpGlob:
		// GLOB uses Unix-style wildcards: * and ?
		pattern := globToRegex(rightStr)
		matched, _ := regexp.MatchString(pattern, leftStr)
		return matched, nil
	default:
		return false, nil
	}
}

// matchLikePattern matches a string against a LIKE pattern with optional escape character.
// If escapeChar is empty, no escaping is done (standard LIKE behavior).
// With escapeChar, the character following the escape char is treated literally.
func matchLikePattern(str, pattern, escapeChar string) bool {
	// Build a regex pattern from the LIKE pattern
	var regexPattern strings.Builder
	regexPattern.WriteString("^")

	escape := false
	if escapeChar != "" && len(escapeChar) == 1 {
		escape = true
	}

	escapeRune := rune(0)
	if escape {
		escapeRune = []rune(escapeChar)[0]
	}

	for i, ch := range pattern {
		if escape && ch == escapeRune {
			// Skip the escape character and treat the next character literally
			// This is handled by moving to the next character in the next iteration
			continue
		}

		// Check if the previous character was an escape character
		if escape && i > 0 && rune(pattern[i-1]) == escapeRune {
			// This character was escaped, treat literally
			regexPattern.WriteString(regexp.QuoteMeta(string(ch)))
			continue
		}

		switch ch {
		case '%':
			regexPattern.WriteString(".*")
		case '_':
			regexPattern.WriteString(".")
		default:
			regexPattern.WriteString(regexp.QuoteMeta(string(ch)))
		}
	}

	regexPattern.WriteString("$")

	matched, _ := regexp.MatchString(regexPattern.String(), str)
	return matched
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
		if !e.engine.TableOrTempExists(fk.RefTable) {
			return fmt.Errorf("foreign key constraint fails: referenced table '%s' does not exist", fk.RefTable)
		}

		refTbl, _, err := e.engine.GetTableOrTemp(fk.RefTable)
		if err != nil {
			return err
		}

		// Get the values for all FK columns
		if len(fk.Columns) == 0 {
			continue
		}

		// Build list of column indices and values for composite FK
		fkColIndices := make([]int, len(fk.Columns))
		fkValues := make([]types.Value, len(fk.Columns))
		allNull := true

		for i, colName := range fk.Columns {
			colIdx, ok := colMap[strings.ToLower(colName)]
			if !ok {
				return fmt.Errorf("foreign key constraint fails: column '%s' not found", colName)
			}
			fkColIndices[i] = colIdx
			if colIdx < len(values) {
				fkValues[i] = values[colIdx]
				if !values[colIdx].Null {
					allNull = false
				}
			} else {
				fkValues[i] = types.Value{Null: true}
			}
		}

		// If all FK values are NULL, skip validation (SQL standard behavior)
		if allNull {
			continue
		}

		// Build list of referenced column indices
		refColIndices := make([]int, len(fk.RefColumns))
		refCols := refTbl.Columns()
		for i, refColName := range fk.RefColumns {
			found := false
			for j, c := range refCols {
				if strings.EqualFold(c.Name, refColName) {
					refColIndices[i] = j
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("foreign key constraint fails: referenced column '%s' not found", refColName)
			}
		}

		// Search for the referenced values (composite key match)
		rows, err := refTbl.Scan()
		if err != nil {
			return err
		}

		found := false
		for _, r := range rows {
			match := true
			for i := 0; i < len(fkValues); i++ {
				if fkValues[i].Null {
					// NULL in FK column means no match needed for this component
					// But we already checked allNull above, so at least one is non-null
					match = false
					break
				}
				if refColIndices[i] >= len(r.Values) {
					match = false
					break
				}
				if r.Values[refColIndices[i]].Null {
					match = false
					break
				}
				if r.Values[refColIndices[i]].Compare(fkValues[i]) != 0 {
					match = false
					break
				}
			}
			if match {
				found = true
				break
			}
		}

		if !found {
			// Build key string for error message
			keyStr := ""
			for i, v := range fkValues {
				if i > 0 {
					keyStr += ", "
				}
				if v.Null {
					keyStr += "NULL"
				} else {
					keyStr += v.String()
				}
			}
			return fmt.Errorf("foreign key constraint fails: key (%s) not found in table '%s'",
				keyStr, fk.RefTable)
		}
	}

	return nil
}

// getReferencingTables finds all tables that have foreign keys referencing the given table.
func (e *Executor) getReferencingTables(tableName string) []struct {
	table *table.Table
	fk    *types.ForeignKeyInfo
} {
	var result []struct {
		table *table.Table
		fk    *types.ForeignKeyInfo
	}

	tables := e.engine.ListTables()
	for _, tName := range tables {
		tbl, _, err := e.engine.GetTableOrTemp(tName)
		if err != nil {
			continue
		}

		for _, fk := range tbl.GetForeignKeys() {
			if strings.EqualFold(fk.RefTable, tableName) {
				result = append(result, struct {
					table *table.Table
					fk    *types.ForeignKeyInfo
				}{tbl, fk})
			}
		}
	}

	return result
}

// handleForeignKeyOnDelete handles ON DELETE actions for foreign keys.
func (e *Executor) handleForeignKeyOnDelete(tableName string, deletedValue types.Value, refColName string) error {
	referencing := e.getReferencingTables(tableName)

	for _, ref := range referencing {
		fk := ref.fk
		tbl := ref.table
		tblInfo := tbl.GetInfo()

		// Check if this FK references the specific column that was deleted
		if len(fk.RefColumns) == 0 || !strings.EqualFold(fk.RefColumns[0], refColName) {
			continue
		}

		// Get the FK column index in the child table
		childColIdx := -1
		for i, col := range tblInfo.Columns {
			if strings.EqualFold(col.Name, fk.Columns[0]) {
				childColIdx = i
				break
			}
		}
		if childColIdx < 0 {
			continue
		}

		// NULL values don't need cascade handling
		if deletedValue.Null {
			continue
		}

		// Build predicate to find matching rows in child table
		finalChildColIdx := childColIdx
		predicate := func(r *row.Row) bool {
			if finalChildColIdx >= len(r.Values) || r.Values[finalChildColIdx].Null {
				return false
			}
			return r.Values[finalChildColIdx].Compare(deletedValue) == 0
		}

		switch strings.ToUpper(fk.OnDelete) {
		case "CASCADE":
			// Delete matching rows
			_, err := tbl.Delete(predicate)
			if err != nil {
				return err
			}
		case "SET NULL":
			// Set FK column to NULL
			updates := map[int]types.Value{finalChildColIdx: types.NewNullValue()}
			_, err := tbl.Update(predicate, updates)
			if err != nil {
				return err
			}
		case "RESTRICT", "NO ACTION", "":
			// Check if there are any matching rows - if so, error
			rows, err := tbl.Scan()
			if err != nil {
				return err
			}
			for _, r := range rows {
				if predicate(r) {
					return fmt.Errorf("foreign key constraint fails: cannot delete row from table '%s' because it is referenced by table '%s'",
						tableName, tblInfo.Name)
				}
			}
		}
	}

	return nil
}

// handleForeignKeyOnUpdate handles ON UPDATE actions for foreign keys.
func (e *Executor) handleForeignKeyOnUpdate(tableName string, oldValue, newValue types.Value, refColName string) error {
	// If value didn't change, nothing to do
	if oldValue.Compare(newValue) == 0 {
		return nil
	}

	referencing := e.getReferencingTables(tableName)

	for _, ref := range referencing {
		fk := ref.fk
		tbl := ref.table
		tblInfo := tbl.GetInfo()

		// Check if this FK references the specific column that was updated
		if len(fk.RefColumns) == 0 || !strings.EqualFold(fk.RefColumns[0], refColName) {
			continue
		}

		// Get the FK column index in the child table
		childColIdx := -1
		for i, col := range tblInfo.Columns {
			if strings.EqualFold(col.Name, fk.Columns[0]) {
				childColIdx = i
				break
			}
		}
		if childColIdx < 0 {
			continue
		}

		// NULL values don't need cascade handling
		if oldValue.Null {
			continue
		}

		// Build predicate to find matching rows in child table
		finalChildColIdx := childColIdx
		predicate := func(r *row.Row) bool {
			if finalChildColIdx >= len(r.Values) || r.Values[finalChildColIdx].Null {
				return false
			}
			return r.Values[finalChildColIdx].Compare(oldValue) == 0
		}

		switch strings.ToUpper(fk.OnUpdate) {
		case "CASCADE":
			// Update matching rows to new value
			updates := map[int]types.Value{finalChildColIdx: newValue}
			_, err := tbl.Update(predicate, updates)
			if err != nil {
				return err
			}
		case "SET NULL":
			// Set FK column to NULL
			updates := map[int]types.Value{finalChildColIdx: types.NewNullValue()}
			_, err := tbl.Update(predicate, updates)
			if err != nil {
				return err
			}
		case "RESTRICT", "NO ACTION", "":
			// Check if there are any matching rows - if so, error
			rows, err := tbl.Scan()
			if err != nil {
				return err
			}
			for _, r := range rows {
				if predicate(r) {
					return fmt.Errorf("foreign key constraint fails: cannot update row from table '%s' because it is referenced by table '%s'",
						tableName, tblInfo.Name)
				}
			}
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

// ========== JSON Helper Functions ==========

// extractJSONPath extracts a value from JSON using a path like $.key or $[0]
func extractJSONPath(jsonVal interface{}, path string) interface{} {
	// Remove leading $ if present
	if len(path) > 0 && path[0] == '$' {
		path = path[1:]
	}
	if path == "" {
		return jsonVal
	}

	current := jsonVal
	i := 0
	for i < len(path) {
		if path[i] == '.' {
			// Object key access
			i++
			start := i
			for i < len(path) && path[i] != '.' && path[i] != '[' {
				i++
			}
			key := path[start:i]
			obj, ok := current.(map[string]interface{})
			if !ok {
				return nil
			}
			current = obj[key]
		} else if path[i] == '[' {
			// Array index access
			i++
			start := i
			for i < len(path) && path[i] != ']' {
				i++
			}
			if i >= len(path) {
				return nil
			}
			indexStr := path[start:i]
			var index int
			fmt.Sscanf(indexStr, "%d", &index)
			i++ // skip ]
			arr, ok := current.([]interface{})
			if !ok {
				return nil
			}
			if index < 0 || index >= len(arr) {
				return nil
			}
			current = arr[index]
		} else {
			i++
		}
	}

	// Convert result to string if it's a JSON string
	if str, ok := current.(string); ok {
		return str
	}
	// For objects/arrays, return JSON string
	bytes, err := json.Marshal(current)
	if err != nil {
		return current
	}
	return string(bytes)
}

// getJSONType returns the JSON type of a value
func getJSONType(jsonVal interface{}) string {
	switch jsonVal.(type) {
	case nil:
		return "NULL"
	case bool:
		return "BOOLEAN"
	case float64:
		return "NUMBER"
	case string:
		return "STRING"
	case []interface{}:
		return "ARRAY"
	case map[string]interface{}:
		return "OBJECT"
	default:
		return "UNKNOWN"
	}
}

// ========== Soundex Helper Functions ==========

// soundex returns the soundex code for a string
func soundex(s string) string {
	if len(s) == 0 {
		return "0000"
	}

	// Convert to uppercase and remove non-alpha
	s = strings.ToUpper(s)
	var cleaned strings.Builder
	for _, c := range s {
		if c >= 'A' && c <= 'Z' {
			cleaned.WriteRune(c)
		}
	}
	s = cleaned.String()
	if len(s) == 0 {
		return "0000"
	}

	// Soundex mappings
	mapping := map[byte]byte{
		'B': '1', 'F': '1', 'P': '1', 'V': '1',
		'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
		'D': '3', 'T': '3',
		'L': '4',
		'M': '5', 'N': '5',
		'R': '6',
	}

	var result strings.Builder
	result.WriteByte(s[0]) // First letter

	lastCode := byte('0')
	if code, ok := mapping[s[0]]; ok {
		lastCode = code
	}

	for i := 1; i < len(s) && result.Len() < 4; i++ {
		code, ok := mapping[s[i]]
		if ok && code != lastCode {
			result.WriteByte(code)
			lastCode = code
		} else if !ok {
			// Vowels and H, W don't have codes
			lastCode = '0'
		}
	}

	// Pad with zeros
	for result.Len() < 4 {
		result.WriteByte('0')
	}

	return result.String()
}

// soundexDifference returns the difference between two soundex codes (0-4)
func soundexDifference(str1, str2 string) int {
	s1 := soundex(str1)
	s2 := soundex(str2)

	diff := 0
	for i := 0; i < 4; i++ {
		if s1[i] == s2[i] {
			diff++
		}
	}
	return diff
}

// ========== DateTime Helper Functions ==========

// parseDateTime parses a datetime string in various formats
func parseDateTime(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05.999",
		"15:04:05",
		time.RFC3339,
		time.RFC3339Nano,
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse datetime: %s", s)
}

// timestampDiff returns the difference between two timestamps in the specified unit
func timestampDiff(unit string, t1, t2 time.Time) int64 {
	if t1.IsZero() || t2.IsZero() {
		return 0
	}

	d := t2.Sub(t1)
	switch strings.ToUpper(unit) {
	case "MICROSECOND":
		return int64(d / time.Microsecond)
	case "SECOND":
		return int64(d / time.Second)
	case "MINUTE":
		return int64(d / time.Minute)
	case "HOUR":
		return int64(d / time.Hour)
	case "DAY":
		return int64(d / (24 * time.Hour))
	case "WEEK":
		return int64(d / (7 * 24 * time.Hour))
	case "MONTH":
		// Approximate
		return int64(int(t2.Month()) - int(t1.Month()) + 12*(t2.Year()-t1.Year()))
	case "QUARTER":
		months := int64(int(t2.Month())-int(t1.Month()) + 12*(t2.Year()-t1.Year()))
		return months / 3
	case "YEAR":
		return int64(t2.Year() - t1.Year())
	default:
		return int64(d / time.Second)
	}
}

// globToRegex converts a GLOB pattern to a regex pattern.
// GLOB uses: * (any sequence), ? (any single char), [abc] (character set), [a-z] (range)
// SQLite GLOB is case-sensitive and uses Unix-style wildcards.
func globToRegex(pattern string) string {
	var result strings.Builder
	result.WriteString("^")

	i := 0
	n := len(pattern)
	for i < n {
		c := pattern[i]
		switch c {
		case '*':
			result.WriteString(".*")
		case '?':
			result.WriteString(".")
		case '[':
			// Handle character set [abc] or [a-z] or [!abc] or [^abc]
			result.WriteByte('[')
			i++
			if i < n {
				// Handle negation: [!abc] or [^abc]
				if pattern[i] == '!' || pattern[i] == '^' {
					result.WriteByte('^')
					i++
				}
				// Copy everything until closing ]
				for i < n && pattern[i] != ']' {
					if pattern[i] == '\\' {
						// Escape backslash in regex
						result.WriteString("\\\\")
					} else {
						result.WriteByte(pattern[i])
					}
					i++
				}
				if i < n {
					result.WriteByte(']')
				}
			}
		case ']':
			// Unmatched ] - escape it
			result.WriteString("\\]")
		case '(', ')', '{', '}', '.', '+', '^', '$', '|', '\\':
			// Escape other regex special characters
			result.WriteByte('\\')
			result.WriteByte(c)
		default:
			result.WriteByte(c)
		}
		i++
	}
	result.WriteString("$")
	return result.String()
}

// ========== JSON Helper Functions for Set/Replace/Remove ==========

// jsonSetPath sets a value at a path, creating intermediate objects/arrays as needed
func jsonSetPath(jsonVal interface{}, path string, value interface{}) interface{} {
	if path == "" || path == "$" {
		return value
	}

	// Parse path
	path = strings.TrimPrefix(path, "$")
	parts := parseJSONPathParts(path)

	if len(parts) == 0 {
		return value
	}

	// Navigate and set
	return jsonSetPathRecursive(jsonVal, parts, value)
}

func jsonSetPathRecursive(current interface{}, parts []jsonPathPart, value interface{}) interface{} {
	if len(parts) == 0 {
		return value
	}

	part := parts[0]
	remaining := parts[1:]

	switch part.typ {
	case pathTypeKey:
		obj, ok := current.(map[string]interface{})
		if !ok {
			obj = make(map[string]interface{})
		}
		obj[part.key] = jsonSetPathRecursive(obj[part.key], remaining, value)
		return obj
	case pathTypeIndex:
		arr, ok := current.([]interface{})
		if !ok {
			arr = make([]interface{}, part.index+1)
		}
		// Extend array if needed
		for len(arr) <= part.index {
			arr = append(arr, nil)
		}
		arr[part.index] = jsonSetPathRecursive(arr[part.index], remaining, value)
		return arr
	}
	return current
}

// jsonReplacePath replaces a value at a path only if it exists
func jsonReplacePath(jsonVal interface{}, path string, value interface{}) interface{} {
	if path == "" || path == "$" {
		return value
	}

	path = strings.TrimPrefix(path, "$")
	parts := parseJSONPathParts(path)

	if len(parts) == 0 {
		return value
	}

	return jsonReplacePathRecursive(jsonVal, parts, value)
}

func jsonReplacePathRecursive(current interface{}, parts []jsonPathPart, value interface{}) interface{} {
	if len(parts) == 0 {
		return value
	}

	part := parts[0]
	remaining := parts[1:]

	switch part.typ {
	case pathTypeKey:
		obj, ok := current.(map[string]interface{})
		if !ok {
			return current
		}
		if _, exists := obj[part.key]; exists {
			obj[part.key] = jsonReplacePathRecursive(obj[part.key], remaining, value)
		}
		return obj
	case pathTypeIndex:
		arr, ok := current.([]interface{})
		if !ok || part.index >= len(arr) {
			return current
		}
		arr[part.index] = jsonReplacePathRecursive(arr[part.index], remaining, value)
		return arr
	}
	return current
}

// jsonRemovePath removes a value at a path
func jsonRemovePath(jsonVal interface{}, path string) interface{} {
	if path == "" || path == "$" {
		return nil
	}

	path = strings.TrimPrefix(path, "$")
	parts := parseJSONPathParts(path)

	if len(parts) == 0 {
		return jsonVal
	}

	return jsonRemovePathRecursive(jsonVal, parts)
}

func jsonRemovePathRecursive(current interface{}, parts []jsonPathPart) interface{} {
	if len(parts) == 0 {
		return nil
	}

	part := parts[0]
	remaining := parts[1:]

	switch part.typ {
	case pathTypeKey:
		obj, ok := current.(map[string]interface{})
		if !ok {
			return current
		}
		if len(remaining) == 0 {
			delete(obj, part.key)
		} else {
			obj[part.key] = jsonRemovePathRecursive(obj[part.key], remaining)
		}
		return obj
	case pathTypeIndex:
		arr, ok := current.([]interface{})
		if !ok || part.index >= len(arr) {
			return current
		}
		if len(remaining) == 0 {
			// Remove element at index
			arr = append(arr[:part.index], arr[part.index+1:]...)
		} else {
			arr[part.index] = jsonRemovePathRecursive(arr[part.index], remaining)
		}
		return arr
	}
	return current
}

// jsonMergePatch merges two JSON objects per RFC 7396
func jsonMergePatch(target, patch map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range target {
		result[k] = v
	}

	for key, value := range patch {
		if value == nil {
			delete(result, key)
		} else if patchObj, ok := value.(map[string]interface{}); ok {
			if targetObj, ok := result[key].(map[string]interface{}); ok {
				result[key] = jsonMergePatch(targetObj, patchObj)
			} else {
				result[key] = patchObj
			}
		} else {
			result[key] = value
		}
	}

	return result
}

type jsonPathPart struct {
	typ   pathPartType
	key   string
	index int
}

type pathPartType int

const (
	pathTypeKey   pathPartType = iota
	pathTypeIndex pathPartType = iota
)

func parseJSONPathParts(path string) []jsonPathPart {
	var parts []jsonPathPart
	i := 0
	for i < len(path) {
		if path[i] == '.' {
			i++
			start := i
			for i < len(path) && path[i] != '.' && path[i] != '[' {
				i++
			}
			if i > start {
				parts = append(parts, jsonPathPart{typ: pathTypeKey, key: path[start:i]})
			}
		} else if path[i] == '[' {
			i++
			start := i
			for i < len(path) && path[i] != ']' {
				i++
			}
			var idx int
			fmt.Sscanf(path[start:i], "%d", &idx)
			parts = append(parts, jsonPathPart{typ: pathTypeIndex, index: idx})
			i++ // skip ]
		} else {
			i++
		}
	}
	return parts
}

// evaluateGeneratedColumns evaluates virtual generated columns for a row
func (e *Executor) evaluateGeneratedColumns(r *row.Row, columns []*types.ColumnInfo) error {
	for i, col := range columns {
		// Skip stored generated columns (already computed)
		if col.GeneratedExpr != "" && !col.GeneratedStored {
			// Virtual generated column - compute value
			// Parse the expression
			p := sql.NewParser(col.GeneratedExpr)
			expr := p.ParseExpression()
			if p.Error() != nil {
				return fmt.Errorf("invalid generated column expression: %w", p.Error())
			}

			// Build column map for evaluation
			columnMap := make(map[string]*types.ColumnInfo)
			for j, c := range columns {
				columnMap[strings.ToLower(c.Name)] = columns[j]
			}

			// Evaluate the expression
			val, err := e.evaluateExpression(expr, r, columnMap, columns)
			if err != nil {
				return fmt.Errorf("error evaluating generated column %s: %w", col.Name, err)
			}

			// Store the computed value in the row
			if i < len(r.Values) {
				r.Values[i] = e.interfaceToValue(val, col)
			}
		}
	}
	return nil
}

// interfaceToValue converts an interface{} to a types.Value
func (e *Executor) interfaceToValue(val interface{}, col *types.ColumnInfo) types.Value {
	if val == nil {
		return types.Value{Null: true}
	}

	switch v := val.(type) {
	case int:
		return types.NewIntValue(int64(v))
	case int64:
		return types.NewIntValue(v)
	case float64:
		return types.NewFloatValue(v)
	case string:
		return types.NewStringValue(v, col.Type)
	case bool:
		if v {
			return types.NewIntValue(1)
		}
		return types.NewIntValue(0)
	case []byte:
		return types.Value{Type: types.TypeBlob, Data: v, Null: false}
	default:
		// Try to convert to string
		return types.NewStringValue(fmt.Sprintf("%v", val), col.Type)
	}
}

// ========== Date/Time Helper Functions ==========

// timeToJulianDay converts a time.Time to Julian day number
// Julian day is the number of days since noon UTC on January 1, 4713 BCE
func timeToJulianDay(t time.Time) float64 {
	// Use UTC for consistency
	t = t.UTC()

	// Get Unix timestamp
	unix := t.Unix()

	// Days from Unix epoch (1970-01-01) to Julian day reference
	// Julian day 2440587.5 = Unix epoch (1970-01-01 00:00:00 UTC)
	// The 0.5 accounts for Julian day starting at noon
	const unixEpochJD = 2440587.5

	// Calculate Julian day
	jd := unixEpochJD + float64(unix)/86400.0

	return jd
}

// applyDateModifier applies a SQLite-style date modifier
// Examples: '+1 day', '-7 days', '+1 month', '+1 year', 'start of month'
func applyDateModifier(t time.Time, modifier string) time.Time {
	modifier = strings.TrimSpace(modifier)
	modifier = strings.ToLower(modifier)

	// Handle special modifiers
	switch modifier {
	case "start of month":
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case "start of year":
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	case "start of day":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case "end of month":
		return time.Date(t.Year(), t.Month()+1, 0, 23, 59, 59, 0, t.Location())
	case "end of year":
		return time.Date(t.Year(), 12, 31, 23, 59, 59, 0, t.Location())
	}

	// Parse numeric modifiers: [+/-]N unit
	re := regexp.MustCompile(`^([+-]?)\s*(\d+)\s+(day|days|month|months|year|years|hour|hours|minute|minutes|second|seconds)$`)
	matches := re.FindStringSubmatch(modifier)
	if matches == nil {
		return t
	}

	sign := 1
	if matches[1] == "-" {
		sign = -1
	}

	value, _ := strconv.Atoi(matches[2])
	value *= sign
	unit := matches[3]

	switch unit {
	case "day", "days":
		return t.AddDate(0, 0, value)
	case "month", "months":
		return t.AddDate(0, value, 0)
	case "year", "years":
		return t.AddDate(value, 0, 0)
	case "hour", "hours":
		return t.Add(time.Duration(value) * time.Hour)
	case "minute", "minutes":
		return t.Add(time.Duration(value) * time.Minute)
	case "second", "seconds":
		return t.Add(time.Duration(value) * time.Second)
	}

	return t
}

// ============================================================================
// Transaction Functions
// ============================================================================

// executeBegin executes a BEGIN [TRANSACTION] statement.
func (e *Executor) executeBegin(stmt *sql.BeginStmt) (*Result, error) {
	if e.inTransaction {
		return nil, fmt.Errorf("already in transaction")
	}

	// Set transaction mode
	// SQLite semantics:
	// - DEFERRED (default): Don't acquire any locks until first read/write
	// - IMMEDIATE: Acquire RESERVED lock immediately (prevent other writers)
	// - EXCLUSIVE: Acquire EXCLUSIVE lock immediately (prevent all other access)
	//
	// For XxSQL's simplified model:
	// - We track the mode but don't implement full locking
	// - The storage engine handles basic transaction state

	e.txMode = stmt.TransactionType
	if e.txMode == "" {
		e.txMode = "DEFERRED" // default
	}

	// Begin transaction in storage engine
	if err := e.engine.BeginTransaction(); err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	e.inTransaction = true
	e.savepoints = nil

	modeStr := ""
	if stmt.TransactionType != "" {
		modeStr = " " + stmt.TransactionType
	}

	return &Result{
		Columns:  []ColumnInfo{{Name: "Result"}},
		Rows:     [][]interface{}{{fmt.Sprintf("Transaction started%s", modeStr)}},
		RowCount: 1,
		Message:  "BEGIN",
	}, nil
}

// executeCommit executes a COMMIT [TRANSACTION] statement.
func (e *Executor) executeCommit(stmt *sql.CommitStmt) (*Result, error) {
	if !e.inTransaction {
		return nil, fmt.Errorf("no transaction in progress")
	}

	// Commit transaction in storage engine
	if err := e.engine.CommitTransaction(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	e.inTransaction = false
	e.savepoints = nil

	return &Result{
		Columns:  []ColumnInfo{{Name: "Result"}},
		Rows:     [][]interface{}{{"Transaction committed"}},
		RowCount: 1,
		Message:  "COMMIT",
	}, nil
}

// executeRollback executes a ROLLBACK [TRANSACTION] [TO SAVEPOINT name] statement.
func (e *Executor) executeRollback(stmt *sql.RollbackStmt) (*Result, error) {
	if !e.inTransaction {
		return nil, fmt.Errorf("no transaction in progress")
	}

	if stmt.ToSavepoint != "" {
		// Rollback to savepoint
		found := false
		for i, sp := range e.savepoints {
			if sp == stmt.ToSavepoint {
				// Remove all savepoints after this one
				e.savepoints = e.savepoints[:i]
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("savepoint '%s' not found", stmt.ToSavepoint)
		}

		// Rollback to savepoint in storage engine
		if err := e.engine.RollbackToSavepoint(stmt.ToSavepoint); err != nil {
			return nil, fmt.Errorf("failed to rollback to savepoint: %w", err)
		}

		return &Result{
			Columns:  []ColumnInfo{{Name: "Result"}},
			Rows:     [][]interface{}{{fmt.Sprintf("Rolled back to savepoint '%s'", stmt.ToSavepoint)}},
			RowCount: 1,
			Message:  "ROLLBACK TO SAVEPOINT",
		}, nil
	}

	// Full rollback
	if err := e.engine.RollbackTransaction(); err != nil {
		return nil, fmt.Errorf("failed to rollback transaction: %w", err)
	}

	e.inTransaction = false
	e.savepoints = nil

	return &Result{
		Columns:  []ColumnInfo{{Name: "Result"}},
		Rows:     [][]interface{}{{"Transaction rolled back"}},
		RowCount: 1,
		Message:  "ROLLBACK",
	}, nil
}

// executeSavepoint executes a SAVEPOINT name statement.
func (e *Executor) executeSavepoint(stmt *sql.SavepointStmt) (*Result, error) {
	if !e.inTransaction {
		return nil, fmt.Errorf("no transaction in progress")
	}

	// Create savepoint in storage engine
	if err := e.engine.CreateSavepoint(stmt.Name); err != nil {
		return nil, fmt.Errorf("failed to create savepoint: %w", err)
	}

	// Add to savepoint stack
	e.savepoints = append(e.savepoints, stmt.Name)

	return &Result{
		Columns:  []ColumnInfo{{Name: "Result"}},
		Rows:     [][]interface{}{{fmt.Sprintf("Savepoint '%s' created", stmt.Name)}},
		RowCount: 1,
		Message:  "SAVEPOINT",
	}, nil
}

// executeReleaseSavepoint executes a RELEASE SAVEPOINT name statement.
func (e *Executor) executeReleaseSavepoint(stmt *sql.ReleaseSavepointStmt) (*Result, error) {
	if !e.inTransaction {
		return nil, fmt.Errorf("no transaction in progress")
	}

	// Find and remove savepoint
	found := false
	for i, sp := range e.savepoints {
		if sp == stmt.Name {
			// Remove this savepoint (and all after it, per SQL spec)
			e.savepoints = append(e.savepoints[:i], e.savepoints[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("savepoint '%s' not found", stmt.Name)
	}

	// Release savepoint in storage engine
	if err := e.engine.ReleaseSavepoint(stmt.Name); err != nil {
		return nil, fmt.Errorf("failed to release savepoint: %w", err)
	}

	return &Result{
		Columns:  []ColumnInfo{{Name: "Result"}},
		Rows:     [][]interface{}{{fmt.Sprintf("Savepoint '%s' released", stmt.Name)}},
		RowCount: 1,
		Message:  "RELEASE SAVEPOINT",
	}, nil
}

// InTransaction returns true if a transaction is currently in progress.
func (e *Executor) InTransaction() bool {
	return e.inTransaction
}

// ============================================================================
// Bulk Import/Export Functions
// ============================================================================

// executeCopy executes a COPY statement for bulk import/export.
func (e *Executor) executeCopy(stmt *sql.CopyStmt) (*Result, error) {
	if stmt.Direction == "FROM" {
		return e.executeCopyFrom(stmt)
	}
	return e.executeCopyTo(stmt)
}

// executeCopyFrom executes COPY table FROM 'file.csv'
func (e *Executor) executeCopyFrom(stmt *sql.CopyStmt) (*Result, error) {
	// Open the file
	file, err := os.Open(stmt.FileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open file '%s': %w", stmt.FileName, err)
	}
	defer file.Close()

	// Get table schema
	table, _, err := e.engine.GetTableOrTemp(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table '%s' not found", stmt.TableName)
	}
	columns := table.Columns()

	// Create CSV reader
	reader := csv.NewReader(file)
	reader.Comma = []rune(stmt.Delimiter)[0]
	reader.LazyQuotes = true

	// Skip header if specified
	if stmt.Header {
		_, err = reader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read header: %w", err)
		}
	}

	// Read and insert rows
	rowCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read record: %w", err)
		}

		// Convert record to values
		values := make([]types.Value, len(columns))
		for i, col := range columns {
			if i >= len(record) {
				values[i] = types.Value{Null: true}
				continue
			}

			strVal := record[i]
			// Handle NULL
			if stmt.NullString != "" && strVal == stmt.NullString {
				values[i] = types.Value{Null: true}
				continue
			}

			// Convert based on column type
			val, err := e.parseValueForColumn(strVal, col)
			if err != nil {
				return nil, fmt.Errorf("row %d, column %s: %w", rowCount+1, col.Name, err)
			}
			values[i] = val
		}

		// Insert row
		_, err = table.Insert(values)
		if err != nil {
			return nil, fmt.Errorf("failed to insert row %d: %w", rowCount+1, err)
		}
		rowCount++
	}

	return &Result{
		Columns:  []ColumnInfo{{Name: "Rows Imported"}},
		Rows:     [][]interface{}{{rowCount}},
		RowCount: 1,
		Message:  fmt.Sprintf("COPY FROM: %d rows imported", rowCount),
	}, nil
}

// executeCopyTo executes COPY (SELECT ...) TO 'file.csv'
func (e *Executor) executeCopyTo(stmt *sql.CopyStmt) (*Result, error) {
	// Execute the query
	var result *Result
	var err error

	if stmt.Query != nil {
		// Execute the query statement
		result, err = e.executeStatementForExport(stmt.Query)
		if err != nil {
			return nil, fmt.Errorf("query execution failed: %w", err)
		}
	} else {
		// Select all from table
		result, err = e.Execute(fmt.Sprintf("SELECT * FROM %s", stmt.TableName))
		if err != nil {
			return nil, fmt.Errorf("failed to select from table: %w", err)
		}
	}

	// Create the file
	file, err := os.Create(stmt.FileName)
	if err != nil {
		return nil, fmt.Errorf("failed to create file '%s': %w", stmt.FileName, err)
	}
	defer file.Close()

	// Create CSV writer
	writer := csv.NewWriter(file)
	writer.Comma = []rune(stmt.Delimiter)[0]

	// Write header if specified
	if stmt.Header {
		header := make([]string, len(result.Columns))
		for i, col := range result.Columns {
			header[i] = col.Name
		}
		if err := writer.Write(header); err != nil {
			return nil, fmt.Errorf("failed to write header: %w", err)
		}
	}

	// Write rows
	for _, row := range result.Rows {
		record := make([]string, len(row))
		for i, val := range row {
			if val == nil {
				if stmt.NullString != "" {
					record[i] = stmt.NullString
				} else {
					record[i] = ""
				}
			} else {
				record[i] = fmt.Sprintf("%v", val)
			}
		}
		if err := writer.Write(record); err != nil {
			return nil, fmt.Errorf("failed to write record: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, fmt.Errorf("failed to flush writer: %w", err)
	}

	return &Result{
		Columns:  []ColumnInfo{{Name: "Rows Exported"}},
		Rows:     [][]interface{}{{result.RowCount}},
		RowCount: 1,
		Message:  fmt.Sprintf("COPY TO: %d rows exported to %s", result.RowCount, stmt.FileName),
	}, nil
}

// executeLoadData executes a LOAD DATA INFILE statement (MySQL style).
func (e *Executor) executeLoadData(stmt *sql.LoadDataStmt) (*Result, error) {
	// Open the file
	file, err := os.Open(stmt.FileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open file '%s': %w", stmt.FileName, err)
	}
	defer file.Close()

	// Get table schema
	table, _, err := e.engine.GetTableOrTemp(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table '%s' not found", stmt.TableName)
	}
	columns := table.Columns()

	// Create scanner for line-by-line reading
	scanner := bufio.NewScanner(file)

	// Set custom line separator
	if stmt.LinesTerminated != "" {
		// For custom line terminators, we need to read the whole file and split
		content, err := io.ReadAll(file)
		if err != nil {
			return nil, fmt.Errorf("failed to read file: %w", err)
		}
		lines := strings.Split(string(content), stmt.LinesTerminated)
		return e.processLoadDataLines(lines, stmt, columns, table)
	}

	// Process line by line
	lines := []string{}
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	return e.processLoadDataLines(lines, stmt, columns, table)
}

// processLoadDataLines processes lines from a LOAD DATA file
func (e *Executor) processLoadDataLines(lines []string, stmt *sql.LoadDataStmt, columns []*types.ColumnInfo, table *table.Table) (*Result, error) {
	// Get target columns
	targetColumns := columns
	if len(stmt.ColumnList) > 0 {
		// Map column names to indices
		targetColumns = make([]*types.ColumnInfo, len(stmt.ColumnList))
		colMap := make(map[string]*types.ColumnInfo)
		for _, col := range columns {
			colMap[strings.ToLower(col.Name)] = col
		}
		for i, colName := range stmt.ColumnList {
			col, ok := colMap[strings.ToLower(colName)]
			if !ok {
				return nil, fmt.Errorf("column '%s' not found", colName)
			}
			targetColumns[i] = col
		}
	}

	// Skip lines if specified
	startLine := stmt.IgnoreRows
	if startLine > len(lines) {
		startLine = len(lines)
	}

	// Process rows
	rowCount := 0
	for i := startLine; i < len(lines); i++ {
		line := lines[i]
		if line == "" {
			continue
		}

		// Parse the line
		fields := e.parseLoadDataLine(line, stmt)

		// Build values array
		values := make([]types.Value, len(columns))
		for j := range columns {
			values[j] = types.Value{Null: true}
		}

		for j, field := range fields {
			if j >= len(targetColumns) {
				break
			}

			col := targetColumns[j]
			colIdx := -1
			for k, c := range columns {
				if c.Name == col.Name {
					colIdx = k
					break
				}
			}

			if colIdx == -1 {
				continue
			}

			// Handle empty field
			if field == "" {
				values[colIdx] = types.Value{Null: true}
				continue
			}

			// Convert value
			val, err := e.parseValueForColumn(field, col)
			if err != nil {
				return nil, fmt.Errorf("line %d, column %s: %w", i+1, col.Name, err)
			}
			values[colIdx] = val
		}

		// Insert row
		_, err := table.Insert(values)
		if err != nil {
			return nil, fmt.Errorf("failed to insert row %d: %w", i+1, err)
		}
		rowCount++
	}

	return &Result{
		Columns:  []ColumnInfo{{Name: "Rows Imported"}},
		Rows:     [][]interface{}{{rowCount}},
		RowCount: 1,
		Message:  fmt.Sprintf("LOAD DATA: %d rows imported", rowCount),
	}, nil
}

// parseLoadDataLine parses a single line according to LOAD DATA syntax
func (e *Executor) parseLoadDataLine(line string, stmt *sql.LoadDataStmt) []string {
	// Remove line starting prefix if specified
	if stmt.LinesStarting != "" {
		line = strings.TrimPrefix(line, stmt.LinesStarting)
	}

	// Parse fields
	separator := stmt.FieldsTerminated
	if separator == "" {
		separator = "\t"
	}

	// Simple split for now (doesn't handle quoted fields with separators inside)
	// A more sophisticated parser would be needed for production
	fields := strings.Split(line, separator)

	// Handle enclosed fields
	if stmt.FieldsEnclosed != "" {
		for i, field := range fields {
			fields[i] = strings.Trim(field, stmt.FieldsEnclosed)
		}
	}

	return fields
}

// parseValueForColumn parses a string value according to column type
func (e *Executor) parseValueForColumn(strVal string, col *types.ColumnInfo) (types.Value, error) {
	if strVal == "" || strVal == "NULL" {
		return types.Value{Null: true}, nil
	}

	switch col.Type {
	case types.TypeInt, types.TypeSeq:
		var val int64
		_, err := fmt.Sscanf(strVal, "%d", &val)
		if err != nil {
			return types.Value{}, fmt.Errorf("invalid integer: %s", strVal)
		}
		return types.NewIntValue(val), nil

	case types.TypeFloat:
		var val float64
		_, err := fmt.Sscanf(strVal, "%f", &val)
		if err != nil {
			return types.Value{}, fmt.Errorf("invalid float: %s", strVal)
		}
		return types.NewFloatValue(val), nil

	case types.TypeBool:
		lower := strings.ToLower(strVal)
		if lower == "true" || lower == "1" || lower == "yes" {
			return types.NewBoolValue(true), nil
		}
		if lower == "false" || lower == "0" || lower == "no" {
			return types.NewBoolValue(false), nil
		}
		return types.Value{}, fmt.Errorf("invalid boolean: %s", strVal)

	case types.TypeDate, types.TypeTime, types.TypeDatetime:
		return types.NewStringValue(strVal, col.Type), nil

	case types.TypeChar, types.TypeVarchar, types.TypeText:
		return types.NewStringValue(strVal, col.Type), nil

	case types.TypeBlob:
		// Assume hex encoding for blobs
		return types.NewStringValue(strVal, col.Type), nil

	case types.TypeDecimal:
		return types.NewStringValue(strVal, col.Type), nil

	default:
		return types.NewStringValue(strVal, col.Type), nil
	}
}

// executeStatementForExport executes a statement for export purposes
func (e *Executor) executeStatementForExport(stmt sql.Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *sql.SelectStmt:
		return e.executeSelect(s)
	case *sql.UnionStmt:
		return e.executeUnion(s)
	default:
		return nil, fmt.Errorf("unsupported statement for export: %T", stmt)
	}
}

// ========== JSON Helper Functions ==========

// jsonExtract extracts a value from JSON at the given path
func jsonExtract(jsonStr, path string) (interface{}, error) {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil, fmt.Errorf("invalid JSON: %v", err)
	}

	// Parse path: $.field or $[0] or $.field[0].subfield
	path = strings.TrimSpace(path)
	if path == "" || path == "$" {
		return data, nil
	}

	if !strings.HasPrefix(path, "$") {
		return nil, fmt.Errorf("JSON path must start with $")
	}
	path = path[1:] // Remove leading $

	return jsonExtractPath(data, path)
}

// jsonExtractPath navigates JSON data using a path
func jsonExtractPath(data interface{}, path string) (interface{}, error) {
	if path == "" {
		return data, nil
	}

	// Check for array index [n]
	if strings.HasPrefix(path, "[") {
		end := strings.Index(path, "]")
		if end == -1 {
			return nil, fmt.Errorf("invalid path: missing ]")
		}
		indexStr := path[1:end]
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			return nil, fmt.Errorf("invalid array index: %s", indexStr)
		}
		arr, ok := data.([]interface{})
		if !ok {
			return nil, nil
		}
		if index < 0 || index >= len(arr) {
			return nil, nil
		}
		return jsonExtractPath(arr[index], path[end+1:])
	}

	// Check for object field .field
	if strings.HasPrefix(path, ".") {
		// Find the field name
		rest := path[1:]
		i := 0
		for i < len(rest) && rest[i] != '.' && rest[i] != '[' {
			i++
		}
		fieldName := rest[:i]
		obj, ok := data.(map[string]interface{})
		if !ok {
			return nil, nil
		}
		val, exists := obj[fieldName]
		if !exists {
			return nil, nil
		}
		return jsonExtractPath(val, rest[i:])
	}

	return nil, fmt.Errorf("invalid path syntax: %s", path)
}

// jsonType returns the JSON type of a value
func jsonType(jsonStr string) string {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return "INVALID"
	}

	switch data.(type) {
	case nil:
		return "NULL"
	case bool:
		return "BOOLEAN"
	case float64:
		return "INTEGER"
		// Note: JSON numbers are always float64 in Go, but we check if it's an integer
		if f, ok := data.(float64); ok && f == float64(int64(f)) {
			return "INTEGER"
		}
		return "DOUBLE"
	case string:
		return "STRING"
	case []interface{}:
		return "ARRAY"
	case map[string]interface{}:
		return "OBJECT"
	default:
		return "UNKNOWN"
	}
}

// jsonContains checks if target JSON contains candidate JSON
func jsonContains(targetStr, candidateStr string) bool {
	var target, candidate interface{}
	if err := json.Unmarshal([]byte(targetStr), &target); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(candidateStr), &candidate); err != nil {
		return false
	}
	return jsonContainsValue(target, candidate)
}

// jsonContainsValue recursively checks if target contains candidate
func jsonContainsValue(target, candidate interface{}) bool {
	// Direct equality
	if jsonEqual(target, candidate) {
		return true
	}

	// If target is an array, check each element
	if arr, ok := target.([]interface{}); ok {
		for _, elem := range arr {
			if jsonContainsValue(elem, candidate) {
				return true
			}
		}
	}

	// If target is an object and candidate is an object,
	// check if target has all of candidate's keys with matching values
	if obj, ok := target.(map[string]interface{}); ok {
		if cObj, ok := candidate.(map[string]interface{}); ok {
			for key, cVal := range cObj {
				tVal, exists := obj[key]
				if !exists || !jsonContainsValue(tVal, cVal) {
					return false
				}
			}
			return true
		}
	}

	return false
}

// jsonEqual checks if two JSON values are equal
func jsonEqual(a, b interface{}) bool {
	aJSON, _ := json.Marshal(a)
	bJSON, _ := json.Marshal(b)
	return string(aJSON) == string(bJSON)
}

// jsonKeys returns the keys of a JSON object
func jsonKeys(jsonStr string) ([]string, error) {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil, fmt.Errorf("invalid JSON: %v", err)
	}

	obj, ok := data.(map[string]interface{})
	if !ok {
		return nil, nil
	}

	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}

// jsonLength returns the length of a JSON array or object
func jsonLength(jsonStr string) int64 {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return 0
	}

	switch v := data.(type) {
	case []interface{}:
		return int64(len(v))
	case map[string]interface{}:
		return int64(len(v))
	case string:
		return int64(len(v))
	default:
		return 1
	}
}

// callScriptFunction calls a XxScript-based UDF with the given arguments.
func (e *Executor) callScriptFunction(fn *ScriptFunction, args []interface{}) (interface{}, error) {
	// Build parameter assignments
	var scriptBuilder strings.Builder
	for i, param := range fn.Params {
		if i < len(args) {
			scriptBuilder.WriteString(fmt.Sprintf("var %s = ", param))
			val := args[i]
			switch v := val.(type) {
			case string:
				// Escape quotes in string
				escaped := strings.ReplaceAll(v, "\\", "\\\\")
				escaped = strings.ReplaceAll(escaped, "\"", "\\\"")
				scriptBuilder.WriteString(fmt.Sprintf("\"%s\"", escaped))
			case nil:
				scriptBuilder.WriteString("null")
			case bool:
				if v {
					scriptBuilder.WriteString("true")
				} else {
					scriptBuilder.WriteString("false")
				}
			case int:
				scriptBuilder.WriteString(fmt.Sprintf("%d", v))
			case int64:
				scriptBuilder.WriteString(fmt.Sprintf("%d", v))
			case float64:
				scriptBuilder.WriteString(fmt.Sprintf("%v", v))
			default:
				// Try to convert to string
				scriptBuilder.WriteString(fmt.Sprintf("\"%v\"", v))
			}
			scriptBuilder.WriteString("\n")
		}
	}

	// Append the function script
	scriptBuilder.WriteString(fn.Script)

	// Execute the script
	result, err := xxscript.Run(scriptBuilder.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("script error in function %s: %w", fn.Name, err)
	}

	return result, nil
}
