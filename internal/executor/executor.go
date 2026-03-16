// Package executor provides SQL query execution for XxSql.
package executor

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/topxeq/xxsql/internal/sql"
	"github.com/topxeq/xxsql/internal/storage"
	"github.com/topxeq/xxsql/internal/storage/row"
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
	Name string
	Type string
}

// Executor executes SQL queries against the storage engine.
type Executor struct {
	engine   *storage.Engine
	database string
	perms    PermissionChecker
	authMgr  AuthManager
}

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
		engine: engine,
	}
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

	tableName := stmt.From.Table.Name
	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

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
			}
		case *sql.ColumnRef:
			colName := strings.ToLower(expr.Name)
			idx := -1
			for i, col := range tblInfo.Columns {
				if strings.ToLower(col.Name) == colName {
					idx = i
					resultCols = append(resultCols, ColumnInfo{
						Name: col.Name,
						Type: col.Type.String(),
					})
					break
				}
			}
			if idx == -1 {
				return nil, fmt.Errorf("unknown column: %s", expr.Name)
			}
			colIndices = append(colIndices, idx)
		}
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
			if idx < len(r.Values) {
				resultRow[i] = e.valueToInterface(r.Values[idx])
			} else {
				resultRow[i] = nil
			}
		}
		result.Rows = append(result.Rows, resultRow)
	}

	result.RowCount = len(result.Rows)
	return result, nil
}

// executeUnion executes a UNION statement.
func (e *Executor) executeUnion(stmt *sql.UnionStmt) (*Result, error) {
	// Execute left side
	leftResult, err := e.executeStatement(stmt.Left)
	if err != nil {
		return nil, fmt.Errorf("left side of UNION error: %w", err)
	}

	// Execute right side
	rightResult, err := e.executeStatement(stmt.Right)
	if err != nil {
		return nil, fmt.Errorf("right side of UNION error: %w", err)
	}

	// Validate column count match
	if len(leftResult.Columns) != len(rightResult.Columns) {
		return nil, fmt.Errorf("UNION: column count mismatch (%d vs %d)",
			len(leftResult.Columns), len(rightResult.Columns))
	}

	// Build combined result
	result := &Result{
		Columns: leftResult.Columns,
		Rows:    make([][]interface{}, 0, len(leftResult.Rows)+len(rightResult.Rows)),
	}

	// Add left rows
	result.Rows = append(result.Rows, leftResult.Rows...)

	// Add right rows
	result.Rows = append(result.Rows, rightResult.Rows...)

	// For UNION (not UNION ALL), remove duplicates
	if !stmt.All {
		result.Rows = e.removeDuplicateRows(result.Rows)
	}

	result.RowCount = len(result.Rows)
	return result, nil
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
func (e *Executor) rowKey(row []interface{}) string {
	parts := make([]string, len(row))
	for i, v := range row {
		if v == nil {
			parts[i] = "NULL"
		} else {
			parts[i] = fmt.Sprintf("%v", v)
		}
	}
	return strings.Join(parts, "\x00")
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
				row = append(row, "2024-01-01 00:00:00")
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
				result.Columns = append(result.Columns, ColumnInfo{
					Name: colName + "()",
					Type: "INT",
				})
				row = append(row, nil)
			}
		case *sql.StarExpr:
			// SELECT * without FROM is invalid, return 1
			result.Columns = append(result.Columns, ColumnInfo{
				Name: "1",
				Type: "INT",
			})
			row = append(row, 1)
		}
	}

	result.Rows = append(result.Rows, row)
	result.RowCount = 1
	return result, nil
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

		// Handle auto-increment columns
		for i, col := range tblInfo.Columns {
			if col.AutoIncr && values[i].Null {
				values[i] = types.Value{Null: false, Type: types.TypeSeq}
			}
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
		idx, ok := colIdxMap[colName]
		if !ok {
			return nil, fmt.Errorf("unknown column: %s", ex.Name)
		}
		if idx < len(r.Values) {
			return e.valueToInterface(r.Values[idx]), nil
		}
		return nil, nil
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

		columns[i] = &types.ColumnInfo{
			Name:       colDef.Name,
			Type:       colType,
			Size:       colDef.Type.Size,
			Precision:  colDef.Type.Precision,
			Scale:      colDef.Type.Scale,
			Nullable:   colDef.Nullable,
			PrimaryKey: colDef.PrimaryKey,
			AutoIncr:   colDef.AutoIncr,
		}
	}

	// Check for primary key in constraints
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
	}

	// Create the table
	if err := e.engine.CreateTable(stmt.TableName, columns); err != nil {
		return nil, fmt.Errorf("create table error: %w", err)
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

	case *sql.Literal:
		if ex.Type == sql.LiteralBool {
			if b, ok := ex.Value.(bool); ok {
				return b, nil
			}
		}
	}

	return false, nil
}

// evaluateExpression evaluates an expression against a row.
func (e *Executor) evaluateExpression(expr sql.Expression, r *row.Row, columnMap map[string]*types.ColumnInfo, columnOrder []*types.ColumnInfo) (interface{}, error) {
	switch ex := expr.(type) {
	case *sql.Literal:
		return ex.Value, nil

	case *sql.ColumnRef:
		colName := strings.ToLower(ex.Name)
		colInfo, ok := columnMap[colName]
		if !ok {
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
	}
	return nil, nil
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

// expressionToValue converts an expression to a storage value.
func (e *Executor) expressionToValue(expr sql.Expression, col *types.ColumnInfo) (types.Value, error) {
	switch ex := expr.(type) {
	case *sql.Literal:
		if ex.Type == sql.LiteralNull {
			return types.Value{Null: true}, nil
		}
		if ex.Type == sql.LiteralNumber {
			// Check target column type first
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
	default:
		return v.AsString()
	}
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
