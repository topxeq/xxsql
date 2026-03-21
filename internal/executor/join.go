// Package executor provides SQL query execution for XxSql.
package executor

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/topxeq/xxsql/internal/sql"
	"github.com/topxeq/xxsql/internal/storage"
	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// ============================================================================
// JOIN Data Structures
// ============================================================================

// joinTable represents a table in the join context.
type joinTable struct {
	name     string            // table name
	alias    string            // optional alias (takes precedence for lookup)
	columns  []*types.ColumnInfo
	rows     []*row.Row
	colIndex map[string]int // column name (lowercase) -> index
	startIdx int            // start index in flattened joinedRow.values
}

// lookupKey returns the key for tableMap lookup.
func (t *joinTable) lookupKey() string {
	if t.alias != "" {
		return t.alias
	}
	return t.name
}

// hasColumn checks if a column exists in the table.
func (t *joinTable) hasColumn(name string) bool {
	_, ok := t.colIndex[strings.ToLower(name)]
	return ok
}

// joinContext tracks state during join execution.
type joinContext struct {
	tables    []*joinTable          // ordered list: base table first, then joins
	tableMap  map[string]*joinTable // alias (if present) OR name -> table
	totalCols int                   // total column count across all tables
}

// joinedRow represents a row across all joined tables.
// Uses flattened structure for efficiency.
type joinedRow struct {
	values    []interface{} // flattened: all columns from all tables
	nullFlags []bool        // true if value is NULL (for outer joins)
}

// getTableValues extracts values for a specific table from joinedRow.
func (r *joinedRow) getTableValues(tbl *joinTable) []interface{} {
	return r.values[tbl.startIdx : tbl.startIdx+len(tbl.columns)]
}

// createNullRow creates a joinedRow with NULL values.
func createNullRow(colCount int) *joinedRow {
	return &joinedRow{
		values:    make([]interface{}, colCount),
		nullFlags: make([]bool, colCount),
	}
}

// combineRows combines a joinedRow with a single table row.
func combineRows(left *joinedRow, rightRowValues []interface{}, rightTbl *joinTable, totalCols int) *joinedRow {
	result := &joinedRow{
		values:    make([]interface{}, totalCols),
		nullFlags: make([]bool, totalCols),
	}

	// Copy left values
	leftColCount := len(left.values)
	copy(result.values[:leftColCount], left.values)
	copy(result.nullFlags[:leftColCount], left.nullFlags)

	// Copy right values at correct position
	for i, v := range rightRowValues {
		result.values[rightTbl.startIdx+i] = v
	}

	return result
}

// ============================================================================
// JOIN Context Building
// ============================================================================

// buildJoinContext creates the initial join context from base table.
func (e *Executor) buildJoinContext(tableRef *sql.TableRef) (*joinContext, error) {
	tableName := tableRef.Name
	if !e.engine.TableExists(tableName) {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	tbl, err := e.engine.GetTable(tableName)
	if err != nil {
		return nil, err
	}
	tblInfo := tbl.GetInfo()

	// Scan all rows
	rows, err := e.engine.Scan(tableName)
	if err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}

	// Build column index map
	colIndex := make(map[string]int)
	for i, col := range tblInfo.Columns {
		colIndex[strings.ToLower(col.Name)] = i
	}

	jt := &joinTable{
		name:     tableName,
		alias:    tableRef.Alias,
		columns:  tblInfo.Columns,
		rows:     rows,
		colIndex: colIndex,
		startIdx: 0,
	}

	ctx := &joinContext{
		tables:    []*joinTable{jt},
		tableMap:  make(map[string]*joinTable),
		totalCols: len(tblInfo.Columns),
	}
	ctx.tableMap[jt.lookupKey()] = jt

	return ctx, nil
}

// loadJoinTable loads a table for joining.
func (e *Executor) loadJoinTable(tableRef *sql.TableRef, startIdx int) (*joinTable, error) {
	tableName := tableRef.Name
	tableNameLower := strings.ToLower(tableName)

	// Check if this is a CTE table
	if cteResult, ok := e.cteResults[tableNameLower]; ok {
		return e.loadJoinTableFromCTE(tableName, tableRef.Alias, cteResult, startIdx)
	}

	if !e.engine.TableExists(tableName) {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	tbl, err := e.engine.GetTable(tableName)
	if err != nil {
		return nil, err
	}
	tblInfo := tbl.GetInfo()

	rows, err := e.engine.Scan(tableName)
	if err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}

	colIndex := make(map[string]int)
	for i, col := range tblInfo.Columns {
		colIndex[strings.ToLower(col.Name)] = i
	}

	return &joinTable{
		name:     tableName,
		alias:    tableRef.Alias,
		columns:  tblInfo.Columns,
		rows:     rows,
		colIndex: colIndex,
		startIdx: startIdx,
	}, nil
}

// loadJoinTableFromCTE creates a joinTable from a CTE result.
func (e *Executor) loadJoinTableFromCTE(tableName, alias string, cteResult *Result, startIdx int) (*joinTable, error) {
	// Build column info for the CTE
	columns := make([]*types.ColumnInfo, len(cteResult.Columns))
	colIndex := make(map[string]int)
	for i, col := range cteResult.Columns {
		columns[i] = &types.ColumnInfo{
			Name: col.Name,
			Type: types.TypeVarchar,
		}
		colIndex[strings.ToLower(col.Name)] = i
	}

	// Convert CTE rows to row.Row format
	rows := make([]*row.Row, len(cteResult.Rows))
	for i, srcRow := range cteResult.Rows {
		values := make([]types.Value, len(srcRow))
		for j, v := range srcRow {
			switch val := v.(type) {
			case int:
				values[j] = types.NewIntValue(int64(val))
			case int64:
				values[j] = types.NewIntValue(val)
			case float64:
				values[j] = types.NewFloatValue(val)
			case string:
				values[j] = types.NewStringValue(val, types.TypeVarchar)
			case []byte:
				values[j] = types.NewBlobValue(val)
			default:
				values[j] = types.NewStringValue(fmt.Sprintf("%v", val), types.TypeVarchar)
			}
		}
		rows[i] = &row.Row{Values: values}
	}

	return &joinTable{
		name:     tableName,
		alias:    alias,
		columns:  columns,
		rows:     rows,
		colIndex: colIndex,
		startIdx: startIdx,
	}, nil
}

// initJoinedRows converts base table rows to initial joinedRows.
func (e *Executor) initJoinedRows(ctx *joinContext) []*joinedRow {
	baseTbl := ctx.tables[0]
	rows := make([]*joinedRow, len(baseTbl.rows))

	for i, r := range baseTbl.rows {
		jr := &joinedRow{
			values:    make([]interface{}, ctx.totalCols),
			nullFlags: make([]bool, ctx.totalCols),
		}
		for j, v := range r.Values {
			jr.values[j] = e.valueToInterface(v)
			jr.nullFlags[j] = v.Null
		}
		rows[i] = jr
	}
	return rows
}

// ============================================================================
// Column Resolution
// ============================================================================

// resolveJoinColumn resolves a column reference in JOIN context.
func (e *Executor) resolveJoinColumn(ref *sql.ColumnRef, row *joinedRow, ctx *joinContext) (interface{}, error) {
	colName := strings.ToLower(ref.Name)

	if ref.Table != "" {
		// Qualified: look up specific table
		tbl := ctx.tableMap[ref.Table]
		if tbl == nil {
			return nil, fmt.Errorf("unknown table or alias: %s", ref.Table)
		}
		idx, ok := tbl.colIndex[colName]
		if !ok {
			return nil, fmt.Errorf("unknown column: %s", ref.Name)
		}
		return row.values[tbl.startIdx+idx], nil
	}

	// Unqualified: search all tables
	var found *joinTable
	for _, tbl := range ctx.tables {
		if tbl.hasColumn(colName) {
			if found != nil {
				return nil, fmt.Errorf("column %s is ambiguous", ref.Name)
			}
			found = tbl
		}
	}
	if found == nil {
		return nil, fmt.Errorf("unknown column: %s", ref.Name)
	}

	idx := found.colIndex[colName]
	return row.values[found.startIdx+idx], nil
}

// ============================================================================
// Expression Evaluation for JOINs
// ============================================================================

// evaluateJoinExpression evaluates an expression in JOIN context.
// Returns (value, error) where value may be nil for NULL.
func (e *Executor) evaluateJoinExpression(expr sql.Expression, row *joinedRow, ctx *joinContext, rightRow *row.Row, rightTbl *joinTable) (interface{}, error) {
	switch ex := expr.(type) {
	case *sql.Literal:
		return ex.Value, nil

	case *sql.ColumnRef:
		colName := strings.ToLower(ex.Name)

		// Check if this is a column from the right table being joined
		if rightTbl != nil && ex.Table != "" {
			if ex.Table == rightTbl.lookupKey() {
				idx, ok := rightTbl.colIndex[colName]
				if !ok {
					return nil, fmt.Errorf("unknown column: %s", ex.Name)
				}
				if rightRow == nil {
					return nil, nil // NULL for unmatched
				}
				return e.valueToInterface(rightRow.Values[idx]), nil
			}
		}

		// Resolve from existing context (left side)
		return e.resolveJoinColumn(ex, row, ctx)

	case *sql.BinaryExpr:
		left, err := e.evaluateJoinExpression(ex.Left, row, ctx, rightRow, rightTbl)
		if err != nil {
			return nil, err
		}
		right, err := e.evaluateJoinExpression(ex.Right, row, ctx, rightRow, rightTbl)
		if err != nil {
			return nil, err
		}
		// NULL handling: any NULL operand returns NULL for comparison
		if left == nil || right == nil {
			return nil, nil
		}
		// Use numeric-aware comparison
		result, err := compareValuesNumeric(left, ex.Op, right)
		if err != nil {
			return nil, err
		}
		return result, nil

	case *sql.ParenExpr:
		return e.evaluateJoinExpression(ex.Expr, row, ctx, rightRow, rightTbl)

	case *sql.UnaryExpr:
		if ex.Op == sql.OpNot {
			val, err := e.evaluateJoinExpression(ex.Right, row, ctx, rightRow, rightTbl)
			if err != nil {
				return nil, err
			}
			if val == nil {
				return nil, nil
			}
			if b, ok := val.(bool); ok {
				return !b, nil
			}
		}

	case *sql.IsNullExpr:
		val, err := e.evaluateJoinExpression(ex.Expr, row, ctx, rightRow, rightTbl)
		if err != nil {
			return nil, err
		}
		if ex.Not {
			return val != nil, nil // IS NOT NULL
		}
		return val == nil, nil // IS NULL
	}

	return nil, nil
}

// evaluateOnClause evaluates ON condition, treating NULL as no match.
func (e *Executor) evaluateOnClause(on sql.Expression, leftRow *joinedRow, rightRow *row.Row, ctx *joinContext, rightTbl *joinTable) (bool, error) {
	result, err := e.evaluateJoinExpression(on, leftRow, ctx, rightRow, rightTbl)
	if err != nil {
		return false, err
	}
	// NULL means no match
	if result == nil {
		return false, nil
	}
	if b, ok := result.(bool); ok {
		return b, nil
	}
	return false, nil
}

// evaluateJoinWhere evaluates WHERE clause for joined rows.
func (e *Executor) evaluateJoinWhere(expr sql.Expression, row *joinedRow, ctx *joinContext) (bool, error) {
	result, err := e.evaluateJoinExpression(expr, row, ctx, nil, nil)
	if err != nil {
		return false, err
	}
	if result == nil {
		return false, nil
	}
	if b, ok := result.(bool); ok {
		return b, nil
	}
	return false, fmt.Errorf("WHERE clause did not evaluate to boolean, got %T", result)
}

// compareValuesNumeric compares two values with proper numeric handling.
func compareValuesNumeric(left interface{}, op sql.BinaryOp, right interface{}) (bool, error) {
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

	// Try numeric comparison first
	leftNum, leftIsNum := toFloat64(left)
	rightNum, rightIsNum := toFloat64(right)

	if leftIsNum && rightIsNum {
		switch op {
		case sql.OpEq:
			return leftNum == rightNum, nil
		case sql.OpNe:
			return leftNum != rightNum, nil
		case sql.OpLt:
			return leftNum < rightNum, nil
		case sql.OpLe:
			return leftNum <= rightNum, nil
		case sql.OpGt:
			return leftNum > rightNum, nil
		case sql.OpGe:
			return leftNum >= rightNum, nil
		}
	}

	// Fallback to string comparison
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
		pattern := strings.ReplaceAll(rightStr, "%", ".*")
		pattern = strings.ReplaceAll(pattern, "_", ".")
		pattern = "^" + pattern + "$"
		matched, _ := regexp.MatchString(pattern, leftStr)
		return matched, nil
	default:
		return false, nil
	}
}

// toFloat64 attempts to convert a value to float64.
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

// ============================================================================
// JOIN Execution
// ============================================================================

// executeJoin executes a single JOIN operation.
func (e *Executor) executeJoin(ctx *joinContext, currentRows []*joinedRow, join *sql.JoinClause) ([]*joinedRow, error) {
	switch join.Type {
	case sql.JoinInner:
		return e.executeInnerJoin(ctx, currentRows, join)
	case sql.JoinLeft:
		return e.executeLeftJoin(ctx, currentRows, join)
	case sql.JoinRight:
		return e.executeRightJoin(ctx, currentRows, join)
	case sql.JoinCross:
		return e.executeCrossJoin(ctx, currentRows, join)
	case sql.JoinFull:
		return e.executeFullJoin(ctx, currentRows, join)
	default:
		return nil, fmt.Errorf("unsupported join type: %v", join.Type)
	}
}

// executeInnerJoin performs INNER JOIN.
func (e *Executor) executeInnerJoin(ctx *joinContext, currentRows []*joinedRow, join *sql.JoinClause) ([]*joinedRow, error) {
	// Load right table
	rightTbl, err := e.loadJoinTable(join.Table, ctx.totalCols)
	if err != nil {
		return nil, err
	}
	ctx.tables = append(ctx.tables, rightTbl)
	ctx.tableMap[rightTbl.lookupKey()] = rightTbl
	oldTotal := ctx.totalCols
	ctx.totalCols += len(rightTbl.columns)

	var newRows []*joinedRow

	for _, leftRow := range currentRows {
		for _, rightRow := range rightTbl.rows {
			match, err := e.evaluateOnClause(join.On, leftRow, rightRow, ctx, rightTbl)
			if err != nil {
				return nil, err
			}
			if match {
				rightValues := make([]interface{}, len(rightTbl.columns))
				for i, v := range rightRow.Values {
					rightValues[i] = e.valueToInterface(v)
				}
				combined := combineRows(leftRow, rightValues, rightTbl, ctx.totalCols)
				newRows = append(newRows, combined)
			}
		}
	}

	// Reset if no matches (context was updated but rows empty)
	if len(newRows) == 0 {
		ctx.tables = ctx.tables[:len(ctx.tables)-1]
		delete(ctx.tableMap, rightTbl.lookupKey())
		ctx.totalCols = oldTotal
	}

	return newRows, nil
}

// executeLeftJoin performs LEFT JOIN.
func (e *Executor) executeLeftJoin(ctx *joinContext, currentRows []*joinedRow, join *sql.JoinClause) ([]*joinedRow, error) {
	rightTbl, err := e.loadJoinTable(join.Table, ctx.totalCols)
	if err != nil {
		return nil, err
	}
	ctx.tables = append(ctx.tables, rightTbl)
	ctx.tableMap[rightTbl.lookupKey()] = rightTbl
	ctx.totalCols += len(rightTbl.columns)

	var newRows []*joinedRow

	for _, leftRow := range currentRows {
		found := false
		for _, rightRow := range rightTbl.rows {
			match, err := e.evaluateOnClause(join.On, leftRow, rightRow, ctx, rightTbl)
			if err != nil {
				return nil, err
			}
			if match {
				found = true
				rightValues := make([]interface{}, len(rightTbl.columns))
				for i, v := range rightRow.Values {
					rightValues[i] = e.valueToInterface(v)
				}
				combined := combineRows(leftRow, rightValues, rightTbl, ctx.totalCols)
				newRows = append(newRows, combined)
			}
		}
		if !found {
			// No match: emit left row with NULL right columns
			combined := combineRows(leftRow, make([]interface{}, len(rightTbl.columns)), rightTbl, ctx.totalCols)
			newRows = append(newRows, combined)
		}
	}

	return newRows, nil
}

// executeRightJoin performs RIGHT JOIN.
func (e *Executor) executeRightJoin(ctx *joinContext, currentRows []*joinedRow, join *sql.JoinClause) ([]*joinedRow, error) {
	rightTbl, err := e.loadJoinTable(join.Table, ctx.totalCols)
	if err != nil {
		return nil, err
	}
	ctx.tables = append(ctx.tables, rightTbl)
	ctx.tableMap[rightTbl.lookupKey()] = rightTbl
	leftColCount := ctx.totalCols
	ctx.totalCols += len(rightTbl.columns)

	// Track which right rows matched
	matched := make(map[row.RowID]bool)
	var newRows []*joinedRow

	// Pass 1: Find matches
	for _, leftRow := range currentRows {
		for _, rightRow := range rightTbl.rows {
			match, err := e.evaluateOnClause(join.On, leftRow, rightRow, ctx, rightTbl)
			if err != nil {
				return nil, err
			}
			if match {
				matched[rightRow.ID] = true
				rightValues := make([]interface{}, len(rightTbl.columns))
				for i, v := range rightRow.Values {
					rightValues[i] = e.valueToInterface(v)
				}
				combined := combineRows(leftRow, rightValues, rightTbl, ctx.totalCols)
				newRows = append(newRows, combined)
			}
		}
	}

	// Pass 2: Add unmatched right rows with NULL left
	for _, rightRow := range rightTbl.rows {
		if !matched[rightRow.ID] {
			combined := &joinedRow{
				values:    make([]interface{}, ctx.totalCols),
				nullFlags: make([]bool, ctx.totalCols),
			}
			// Left side: NULL
			for i := 0; i < leftColCount; i++ {
				combined.values[i] = nil
				combined.nullFlags[i] = true
			}
			// Right side: actual values
			for i, v := range rightRow.Values {
				combined.values[rightTbl.startIdx+i] = e.valueToInterface(v)
				combined.nullFlags[rightTbl.startIdx+i] = v.Null
			}
			newRows = append(newRows, combined)
		}
	}

	return newRows, nil
}

// executeCrossJoin performs CROSS JOIN (cartesian product).
func (e *Executor) executeCrossJoin(ctx *joinContext, currentRows []*joinedRow, join *sql.JoinClause) ([]*joinedRow, error) {
	rightTbl, err := e.loadJoinTable(join.Table, ctx.totalCols)
	if err != nil {
		return nil, err
	}
	ctx.tables = append(ctx.tables, rightTbl)
	ctx.tableMap[rightTbl.lookupKey()] = rightTbl
	ctx.totalCols += len(rightTbl.columns)

	var newRows []*joinedRow

	for _, leftRow := range currentRows {
		for _, rightRow := range rightTbl.rows {
			rightValues := make([]interface{}, len(rightTbl.columns))
			for i, v := range rightRow.Values {
				rightValues[i] = e.valueToInterface(v)
			}
			combined := combineRows(leftRow, rightValues, rightTbl, ctx.totalCols)
			newRows = append(newRows, combined)
		}
	}

	return newRows, nil
}

// executeFullJoin performs FULL OUTER JOIN.
func (e *Executor) executeFullJoin(ctx *joinContext, currentRows []*joinedRow, join *sql.JoinClause) ([]*joinedRow, error) {
	rightTbl, err := e.loadJoinTable(join.Table, ctx.totalCols)
	if err != nil {
		return nil, err
	}
	ctx.tables = append(ctx.tables, rightTbl)
	ctx.tableMap[rightTbl.lookupKey()] = rightTbl
	leftColCount := ctx.totalCols
	ctx.totalCols += len(rightTbl.columns)

	// Track which right rows matched
	matched := make(map[row.RowID]bool)
	var newRows []*joinedRow

	// Pass 1: Find matches and emit matched rows + unmatched left rows
	for _, leftRow := range currentRows {
		found := false
		for _, rightRow := range rightTbl.rows {
			match, err := e.evaluateOnClause(join.On, leftRow, rightRow, ctx, rightTbl)
			if err != nil {
				return nil, err
			}
			if match {
				found = true
				matched[rightRow.ID] = true
				rightValues := make([]interface{}, len(rightTbl.columns))
				for i, v := range rightRow.Values {
					rightValues[i] = e.valueToInterface(v)
				}
				combined := combineRows(leftRow, rightValues, rightTbl, ctx.totalCols)
				newRows = append(newRows, combined)
			}
		}
		if !found {
			// No match: emit left row with NULL right columns
			combined := combineRows(leftRow, make([]interface{}, len(rightTbl.columns)), rightTbl, ctx.totalCols)
			// Mark right columns as NULL
			for i := 0; i < len(rightTbl.columns); i++ {
				combined.nullFlags[rightTbl.startIdx+i] = true
			}
			newRows = append(newRows, combined)
		}
	}

	// Pass 2: Add unmatched right rows with NULL left
	for _, rightRow := range rightTbl.rows {
		if !matched[rightRow.ID] {
			combined := &joinedRow{
				values:    make([]interface{}, ctx.totalCols),
				nullFlags: make([]bool, ctx.totalCols),
			}
			// Left side: NULL
			for i := 0; i < leftColCount; i++ {
				combined.values[i] = nil
				combined.nullFlags[i] = true
			}
			// Right side: actual values
			for i, v := range rightRow.Values {
				combined.values[rightTbl.startIdx+i] = e.valueToInterface(v)
				combined.nullFlags[rightTbl.startIdx+i] = v.Null
			}
			newRows = append(newRows, combined)
		}
	}

	return newRows, nil
}

// ============================================================================
// Column Projection
// ============================================================================

// projectJoinColumns projects columns from joined result.
func (e *Executor) projectJoinColumns(columns []sql.Expression, rows []*joinedRow, ctx *joinContext) ([]ColumnInfo, [][]interface{}, error) {
	var resultCols []ColumnInfo
	var colProjections []func(*joinedRow) interface{}

	for _, colExpr := range columns {
		switch expr := colExpr.(type) {
		case *sql.StarExpr:
			if expr.Table != "" {
				// table.*: expand specific table
				tbl := ctx.tableMap[expr.Table]
				if tbl == nil {
					return nil, nil, fmt.Errorf("unknown table: %s", expr.Table)
				}
				for _, col := range tbl.columns {
					col := col // capture for closure
					resultCols = append(resultCols, ColumnInfo{
						Name: col.Name,
						Type: col.Type.String(),
					})
					idx := tbl.colIndex[strings.ToLower(col.Name)]
					startIdx := tbl.startIdx
					colProjections = append(colProjections, func(row *joinedRow) interface{} {
						return row.values[startIdx+idx]
					})
				}
			} else {
				// *: expand all tables
				for _, tbl := range ctx.tables {
					tbl := tbl // capture for closure
					for _, col := range tbl.columns {
						col := col // capture for closure
						resultCols = append(resultCols, ColumnInfo{
							Name: col.Name,
							Type: col.Type.String(),
						})
						idx := tbl.colIndex[strings.ToLower(col.Name)]
						startIdx := tbl.startIdx
						colProjections = append(colProjections, func(row *joinedRow) interface{} {
							return row.values[startIdx+idx]
						})
					}
				}
			}

		case *sql.ColumnRef:
			colName := expr.Name
			colType := "VARCHAR" // default
			var projector func(*joinedRow) interface{}

			if expr.Table != "" {
				tbl := ctx.tableMap[expr.Table]
				if tbl == nil {
					return nil, nil, fmt.Errorf("unknown table: %s", expr.Table)
				}
				idx, ok := tbl.colIndex[strings.ToLower(colName)]
				if !ok {
					return nil, nil, fmt.Errorf("unknown column: %s", colName)
				}
				colType = tbl.columns[idx].Type.String()
				startIdx := tbl.startIdx
				projector = func(row *joinedRow) interface{} {
					return row.values[startIdx+idx]
				}
			} else {
				// Unqualified: find column
				var found *joinTable
				var foundIdx int
				for _, tbl := range ctx.tables {
					if idx, ok := tbl.colIndex[strings.ToLower(colName)]; ok {
						if found != nil {
							return nil, nil, fmt.Errorf("column %s is ambiguous", colName)
						}
						found = tbl
						foundIdx = idx
					}
				}
				if found == nil {
					return nil, nil, fmt.Errorf("unknown column: %s", colName)
				}
				colType = found.columns[foundIdx].Type.String()
				startIdx := found.startIdx
				projector = func(row *joinedRow) interface{} {
					return row.values[startIdx+foundIdx]
				}
			}

			colNameForResult := colName
			if expr.Alias != "" {
				colNameForResult = expr.Alias
			}
			resultCols = append(resultCols, ColumnInfo{
				Name: colNameForResult,
				Type: colType,
			})
			colProjections = append(colProjections, projector)
		}
	}

	// Build result rows
	resultRows := make([][]interface{}, len(rows))
	for i, row := range rows {
		resultRows[i] = make([]interface{}, len(colProjections))
		for j, proj := range colProjections {
			resultRows[i][j] = proj(row)
		}
	}

	return resultCols, resultRows, nil
}

// ============================================================================
// Main JOIN Query Entry Point
// ============================================================================

// executeSelectWithJoin handles SELECT with JOINs.
func (e *Executor) executeSelectWithJoin(stmt *sql.SelectStmt) (*Result, error) {
	// Build context from base table
	ctx, err := e.buildJoinContext(stmt.From.Table)
	if err != nil {
		return nil, err
	}

	// Initialize rows from base table
	rows := e.initJoinedRows(ctx)

	// Execute each JOIN in order
	for _, join := range stmt.From.Joins {
		rows, err = e.executeJoin(ctx, rows, join)
		if err != nil {
			return nil, err
		}
	}

	// Apply WHERE clause
	if stmt.Where != nil {
		var filtered []*joinedRow
		for _, row := range rows {
			match, err := e.evaluateJoinWhere(stmt.Where, row, ctx)
			if err != nil {
				return nil, err
			}
			if match {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}

	// Project columns
	resultCols, resultRows, err := e.projectJoinColumns(stmt.Columns, rows, ctx)
	if err != nil {
		return nil, err
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		resultRows, err = e.sortJoinedRows(stmt.OrderBy, resultCols, resultRows, ctx)
		if err != nil {
			return nil, err
		}
	}

	// Apply LIMIT
	if stmt.Limit != nil && *stmt.Limit < len(resultRows) {
		resultRows = resultRows[:*stmt.Limit]
	}

	// Apply OFFSET
	if stmt.Offset != nil && *stmt.Offset < len(resultRows) {
		resultRows = resultRows[*stmt.Offset:]
	}

	return &Result{
		Columns:  resultCols,
		Rows:     resultRows,
		RowCount: len(resultRows),
	}, nil
}

// Engine returns the storage engine (for testing).
func (e *Executor) Engine() *storage.Engine {
	return e.engine
}

// sortJoinedRows sorts the result rows according to ORDER BY clause.
func (e *Executor) sortJoinedRows(orderBy []*sql.OrderByItem, cols []ColumnInfo, rows [][]interface{}, ctx *joinContext) ([][]interface{}, error) {
	// Build column index map
	colIndexMap := make(map[string]int)
	for i, col := range cols {
		colIndexMap[strings.ToLower(col.Name)] = i
	}

	// Determine sort indices and directions
	type sortKey struct {
		index     int
		ascending bool
	}
	sortKeys := make([]sortKey, 0, len(orderBy))

	for _, item := range orderBy {
		var colName string
		switch expr := item.Expr.(type) {
		case *sql.ColumnRef:
			colName = strings.ToLower(expr.Name)
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
					sortKeys = append(sortKeys, sortKey{index: idx, ascending: item.Ascending})
				}
			}
			continue
		default:
			continue
		}

		// Check if column name has table prefix (e.g., "t1.id")
		if parts := strings.SplitN(colName, ".", 2); len(parts) == 2 {
			colName = parts[1]
		}

		idx, ok := colIndexMap[colName]
		if !ok {
			// Try to find column in context tables
			for _, tbl := range ctx.tables {
				if tbl.hasColumn(colName) {
					idx = tbl.startIdx + tbl.colIndex[colName]
					if idx < len(cols) {
						sortKeys = append(sortKeys, sortKey{index: idx, ascending: item.Ascending})
					}
					break
				}
			}
			continue
		}
		sortKeys = append(sortKeys, sortKey{index: idx, ascending: item.Ascending})
	}

	if len(sortKeys) == 0 {
		return rows, nil
	}

	// Sort rows
	sort.Slice(rows, func(i, j int) bool {
		for _, key := range sortKeys {
			if key.index >= len(rows[i]) || key.index >= len(rows[j]) {
				continue
			}

			vi := rows[i][key.index]
			vj := rows[j][key.index]

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

	return rows, nil
}

// compareValues compares two values for sorting.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func compareValues(a, b interface{}) int {
	// Handle NULL values
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1 // NULLs sort first
	}
	if b == nil {
		return 1
	}

	switch va := a.(type) {
	case int:
		vb, ok := b.(int)
		if !ok {
			vb2, ok2 := b.(int64)
			if ok2 {
				return compareInts(int64(va), vb2)
			}
			return 0
		}
		return compareInts(int64(va), int64(vb))
	case int64:
		vb, ok := b.(int64)
		if !ok {
			vb2, ok2 := b.(int)
			if ok2 {
				return compareInts(va, int64(vb2))
			}
			return 0
		}
		return compareInts(va, vb)
	case float64:
		vb, ok := b.(float64)
		if !ok {
			return 0
		}
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case string:
		vb, ok := b.(string)
		if !ok {
			return 0
		}
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case bool:
		vb, ok := b.(bool)
		if !ok {
			return 0
		}
		if !va && vb {
			return -1
		} else if va && !vb {
			return 1
		}
		return 0
	default:
		// Fallback to string comparison
		as := fmt.Sprintf("%v", a)
		bs := fmt.Sprintf("%v", b)
		if as < bs {
			return -1
		} else if as > bs {
			return 1
		}
		return 0
	}
}

func compareInts(a, b int64) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// compareValuesWithCollation compares two values with optional collation support for strings.
func compareValuesWithCollation(a, b interface{}, collation string) int {
	// Handle NULL values
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Handle string comparison with collation
	if sa, ok := a.(string); ok {
		if sb, ok := b.(string); ok {
			return collationCompare(sa, sb, collation)
		}
	}

	// Fall back to normal comparison for non-strings
	return compareValues(a, b)
}
