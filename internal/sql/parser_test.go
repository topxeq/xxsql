package sql

import (
	"testing"
)

// ============================================================================
// Lexer Tests
// ============================================================================

func TestLexerKeywords(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"SELECT", TokSelect},
		{"FROM", TokFrom},
		{"WHERE", TokWhere},
		{"INSERT", TokInsert},
		{"UPDATE", TokUpdate},
		{"DELETE", TokDelete},
		{"CREATE", TokCreate},
		{"DROP", TokDrop},
		{"ALTER", TokAlter},
		{"TRUNCATE", TokTruncate},
		{"TABLE", TokTable},
		{"INDEX", TokIndex},
		{"PRIMARY", TokPrimary},
		{"KEY", TokKey},
		{"UNIQUE", TokUnique},
		{"JOIN", TokJoin},
		{"INNER", TokInner},
		{"LEFT", TokLeft},
		{"RIGHT", TokRight},
		{"CROSS", TokCross},
		{"UNION", TokUnion},
		{"SEQ", TokSeq},
		{"INT", TokInt},
		{"FLOAT", TokFloat},
		{"VARCHAR", TokVarchar},
		{"TEXT", TokText},
		{"DATE", TokDate},
		{"TIME", TokTime},
		{"DATETIME", TokDateTime},
	}

	for _, tt := range tests {
		l := NewLexer(tt.input)
		tok := l.NextToken()
		if tok.Type != tt.expected {
			t.Errorf("Lexer keyword %q: expected %v, got %v", tt.input, tt.expected, tok.Type)
		}
	}
}

func TestLexerOperators(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"=", TokEq},
		{"!=", TokNe},
		{"<>", TokNe},
		{"<", TokLt},
		{"<=", TokLe},
		{">", TokGt},
		{">=", TokGe},
		{"+", TokPlus},
		{"-", TokMinus},
		{"*", TokStar},
		{"/", TokSlash},
		{"%", TokPercent},
		{"||", TokConcat},
	}

	for _, tt := range tests {
		l := NewLexer(tt.input)
		tok := l.NextToken()
		if tok.Type != tt.expected {
			t.Errorf("Lexer operator %q: expected %v, got %v", tt.input, tt.expected, tok.Type)
		}
	}
}

func TestLexerNumbers(t *testing.T) {
	tests := []string{
		"123",
		"123.456",
		"0.5",
		"1e10",
		"1E10",
		"1.5e-10",
		"0xFF",
	}

	for _, input := range tests {
		l := NewLexer(input)
		tok := l.NextToken()
		if tok.Type != TokNumber {
			t.Errorf("Lexer number %q: expected TokNumber, got %v", input, tok.Type)
		}
		if tok.Value != input {
			t.Errorf("Lexer number %q: value mismatch, got %q", input, tok.Value)
		}
	}
}

func TestLexerStrings(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"'hello'", "hello"},
		{"'hello world'", "hello world"},
		{`'hello\'world'`, "hello'world"},
		{`'hello\nworld'`, "hello\nworld"},
		{`'hello\tworld'`, "hello\tworld"},
		{`"double"`, "double"},
	}

	for _, tt := range tests {
		l := NewLexer(tt.input)
		tok := l.NextToken()
		if tok.Type != TokString {
			t.Errorf("Lexer string %q: expected TokString, got %v", tt.input, tok.Type)
		}
		if tok.Value != tt.expected {
			t.Errorf("Lexer string %q: expected %q, got %q", tt.input, tt.expected, tok.Value)
		}
	}
}

func TestLexerIdentifiers(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"mytable", "mytable"},
		{"_underscore", "_underscore"},
		{"table123", "table123"},
		{"`backtick`", "backtick"},
		{"`table name`", "table name"},
	}

	for _, tt := range tests {
		l := NewLexer(tt.input)
		tok := l.NextToken()
		if tok.Type != TokIdent {
			t.Errorf("Lexer identifier %q: expected TokIdent, got %v", tt.input, tok.Type)
		}
		if tok.Value != tt.expected {
			t.Errorf("Lexer identifier %q: expected %q, got %q", tt.input, tt.expected, tok.Value)
		}
	}
}

func TestLexerComments(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"-- this is a comment\nSELECT", TokSelect},
		{"/* multi\nline\ncomment */ SELECT", TokSelect},
	}

	for _, tt := range tests {
		l := NewLexer(tt.input)
		tok := l.NextToken()
		if tok.Type != tt.expected {
			t.Errorf("Lexer comment %q: expected %v, got %v", tt.input, tt.expected, tok.Type)
		}
	}
}

// ============================================================================
// Parser Tests - SELECT
// ============================================================================

func TestParseSelectSimple(t *testing.T) {
	input := "SELECT * FROM users"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected *SelectStmt, got %T", stmt)
	}

	if len(selectStmt.Columns) != 1 {
		t.Errorf("Expected 1 column, got %d", len(selectStmt.Columns))
	}

	if selectStmt.From == nil {
		t.Fatal("Expected FROM clause")
	}

	if selectStmt.From.Table.Name != "users" {
		t.Errorf("Expected table 'users', got %q", selectStmt.From.Table.Name)
	}
}

func TestParseSelectColumns(t *testing.T) {
	input := "SELECT id, name, email FROM users"
	stmt, _ := Parse(input)

	selectStmt := stmt.(*SelectStmt)
	if len(selectStmt.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(selectStmt.Columns))
	}
}

func TestParseSelectWhere(t *testing.T) {
	input := "SELECT * FROM users WHERE id = 1"
	stmt, _ := Parse(input)

	selectStmt := stmt.(*SelectStmt)
	if selectStmt.Where == nil {
		t.Fatal("Expected WHERE clause")
	}
}

func TestParseSelectOrderBy(t *testing.T) {
	input := "SELECT * FROM users ORDER BY name ASC, id DESC"
	stmt, _ := Parse(input)

	selectStmt := stmt.(*SelectStmt)
	if len(selectStmt.OrderBy) != 2 {
		t.Fatalf("Expected 2 ORDER BY items, got %d", len(selectStmt.OrderBy))
	}

	if !selectStmt.OrderBy[0].Ascending {
		t.Error("Expected first ORDER BY to be ascending")
	}

	if selectStmt.OrderBy[1].Ascending {
		t.Error("Expected second ORDER BY to be descending")
	}
}

func TestParseSelectLimit(t *testing.T) {
	input := "SELECT * FROM users LIMIT 10 OFFSET 5"
	stmt, _ := Parse(input)

	selectStmt := stmt.(*SelectStmt)
	if selectStmt.Limit == nil || *selectStmt.Limit != 10 {
		t.Errorf("Expected LIMIT 10, got %v", selectStmt.Limit)
	}

	if selectStmt.Offset == nil || *selectStmt.Offset != 5 {
		t.Errorf("Expected OFFSET 5, got %v", selectStmt.Offset)
	}
}

func TestParseSelectDistinct(t *testing.T) {
	input := "SELECT DISTINCT name FROM users"
	stmt, _ := Parse(input)

	selectStmt := stmt.(*SelectStmt)
	if !selectStmt.Distinct {
		t.Error("Expected DISTINCT to be true")
	}
}

func TestParseSelectGroupBy(t *testing.T) {
	input := "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected *SelectStmt, got %T", stmt)
	}

	if len(selectStmt.GroupBy) != 1 {
		t.Errorf("Expected 1 GROUP BY column, got %d", len(selectStmt.GroupBy))
	}

	if selectStmt.Having == nil {
		t.Error("Expected HAVING clause")
	}
}

// ============================================================================
// Parser Tests - INSERT
// ============================================================================

func TestParseInsertSimple(t *testing.T) {
	input := "INSERT INTO users (id, name) VALUES (1, 'Alice')"
	stmt, _ := Parse(input)

	insertStmt, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("Expected *InsertStmt, got %T", stmt)
	}

	if insertStmt.Table != "users" {
		t.Errorf("Expected table 'users', got %q", insertStmt.Table)
	}

	if len(insertStmt.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(insertStmt.Columns))
	}

	if len(insertStmt.Values) != 1 {
		t.Errorf("Expected 1 value row, got %d", len(insertStmt.Values))
	}
}

func TestParseInsertMultiRow(t *testing.T) {
	input := "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
	stmt, _ := Parse(input)

	insertStmt := stmt.(*InsertStmt)
	if len(insertStmt.Values) != 3 {
		t.Errorf("Expected 3 value rows, got %d", len(insertStmt.Values))
	}
}

func TestParseInsertNoColumns(t *testing.T) {
	input := "INSERT INTO users VALUES (1, 'Alice')"
	stmt, _ := Parse(input)

	insertStmt := stmt.(*InsertStmt)
	if len(insertStmt.Columns) != 0 {
		t.Errorf("Expected 0 columns, got %d", len(insertStmt.Columns))
	}
}

// ============================================================================
// Parser Tests - UPDATE
// ============================================================================

func TestParseUpdate(t *testing.T) {
	input := "UPDATE users SET name = 'Bob', email = 'bob@example.com' WHERE id = 1"
	stmt, _ := Parse(input)

	updateStmt, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("Expected *UpdateStmt, got %T", stmt)
	}

	if updateStmt.Table != "users" {
		t.Errorf("Expected table 'users', got %q", updateStmt.Table)
	}

	if len(updateStmt.Assignments) != 2 {
		t.Errorf("Expected 2 assignments, got %d", len(updateStmt.Assignments))
	}

	if updateStmt.Where == nil {
		t.Error("Expected WHERE clause")
	}
}

// ============================================================================
// Parser Tests - DELETE
// ============================================================================

func TestParseDelete(t *testing.T) {
	input := "DELETE FROM users WHERE id = 1"
	stmt, _ := Parse(input)

	deleteStmt, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("Expected *DeleteStmt, got %T", stmt)
	}

	if deleteStmt.Table != "users" {
		t.Errorf("Expected table 'users', got %q", deleteStmt.Table)
	}

	if deleteStmt.Where == nil {
		t.Error("Expected WHERE clause")
	}
}

// ============================================================================
// Parser Tests - DDL
// ============================================================================

func TestParseCreateTable(t *testing.T) {
	input := `CREATE TABLE users (
		id SEQ PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		email VARCHAR(255) UNIQUE,
		age INT DEFAULT 0
	)`
	stmt, _ := Parse(input)

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("Expected *CreateTableStmt, got %T", stmt)
	}

	if createStmt.TableName != "users" {
		t.Errorf("Expected table 'users', got %q", createStmt.TableName)
	}

	if len(createStmt.Columns) != 4 {
		t.Errorf("Expected 4 columns, got %d", len(createStmt.Columns))
	}

	// Check first column (SEQ PRIMARY KEY)
	col := createStmt.Columns[0]
	if col.Name != "id" {
		t.Errorf("Expected column 'id', got %q", col.Name)
	}
	if col.Type.Name != "SEQ" {
		t.Errorf("Expected type 'SEQ', got %q", col.Type.Name)
	}
	if !col.PrimaryKey {
		t.Error("Expected PRIMARY KEY")
	}
}

func TestParseCreateTableIfNotExists(t *testing.T) {
	input := "CREATE TABLE IF NOT EXISTS users (id SEQ)"
	stmt, _ := Parse(input)

	createStmt := stmt.(*CreateTableStmt)
	if !createStmt.IfNotExists {
		t.Error("Expected IF NOT EXISTS to be true")
	}
}

func TestParseDropTable(t *testing.T) {
	input := "DROP TABLE IF EXISTS users"
	stmt, _ := Parse(input)

	dropStmt, ok := stmt.(*DropTableStmt)
	if !ok {
		t.Fatalf("Expected *DropTableStmt, got %T", stmt)
	}

	if dropStmt.TableName != "users" {
		t.Errorf("Expected table 'users', got %q", dropStmt.TableName)
	}

	if !dropStmt.IfExists {
		t.Error("Expected IF EXISTS to be true")
	}
}

func TestParseCreateIndex(t *testing.T) {
	input := "CREATE INDEX idx_name ON users (name)"
	stmt, _ := Parse(input)

	createStmt, ok := stmt.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("Expected *CreateIndexStmt, got %T", stmt)
	}

	if createStmt.IndexName != "idx_name" {
		t.Errorf("Expected index 'idx_name', got %q", createStmt.IndexName)
	}

	if createStmt.TableName != "users" {
		t.Errorf("Expected table 'users', got %q", createStmt.TableName)
	}

	if len(createStmt.Columns) != 1 {
		t.Errorf("Expected 1 column, got %d", len(createStmt.Columns))
	}
}

func TestParseCreateUniqueIndex(t *testing.T) {
	input := "CREATE UNIQUE INDEX idx_email ON users (email)"
	stmt, _ := Parse(input)

	createStmt := stmt.(*CreateIndexStmt)
	if !createStmt.Unique {
		t.Error("Expected UNIQUE index")
	}
}

// ============================================================================
// Parser Tests - ALTER TABLE
// ============================================================================

func TestParseAlterTableAddColumn(t *testing.T) {
	input := "ALTER TABLE users ADD COLUMN age INT DEFAULT 0"
	stmt, _ := Parse(input)

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if alterStmt.TableName != "users" {
		t.Errorf("Expected table 'users', got %q", alterStmt.TableName)
	}

	if len(alterStmt.Actions) != 1 {
		t.Fatalf("Expected 1 action, got %d", len(alterStmt.Actions))
	}

	action, ok := alterStmt.Actions[0].(*AddColumnAction)
	if !ok {
		t.Fatalf("Expected *AddColumnAction, got %T", alterStmt.Actions[0])
	}

	if action.Column.Name != "age" {
		t.Errorf("Expected column 'age', got %q", action.Column.Name)
	}
}

func TestParseAlterTableDropColumn(t *testing.T) {
	input := "ALTER TABLE users DROP COLUMN age"
	stmt, _ := Parse(input)

	alterStmt := stmt.(*AlterTableStmt)
	action, ok := alterStmt.Actions[0].(*DropColumnAction)
	if !ok {
		t.Fatalf("Expected *DropColumnAction, got %T", alterStmt.Actions[0])
	}

	if action.ColumnName != "age" {
		t.Errorf("Expected column 'age', got %q", action.ColumnName)
	}
}

func TestParseAlterTableModifyColumn(t *testing.T) {
	input := "ALTER TABLE users MODIFY COLUMN name VARCHAR(200) NOT NULL"
	stmt, _ := Parse(input)

	alterStmt := stmt.(*AlterTableStmt)
	action, ok := alterStmt.Actions[0].(*ModifyColumnAction)
	if !ok {
		t.Fatalf("Expected *ModifyColumnAction, got %T", alterStmt.Actions[0])
	}

	if action.Column.Name != "name" {
		t.Errorf("Expected column 'name', got %q", action.Column.Name)
	}

	if action.Column.Type.Size != 200 {
		t.Errorf("Expected size 200, got %d", action.Column.Type.Size)
	}
}

func TestParseAlterTableRenameColumn(t *testing.T) {
	input := "ALTER TABLE users RENAME COLUMN old_name TO new_name"
	stmt, _ := Parse(input)

	alterStmt := stmt.(*AlterTableStmt)
	action, ok := alterStmt.Actions[0].(*RenameColumnAction)
	if !ok {
		t.Fatalf("Expected *RenameColumnAction, got %T", alterStmt.Actions[0])
	}

	if action.OldName != "old_name" {
		t.Errorf("Expected old name 'old_name', got %q", action.OldName)
	}

	if action.NewName != "new_name" {
		t.Errorf("Expected new name 'new_name', got %q", action.NewName)
	}
}

func TestParseAlterTableRenameTable(t *testing.T) {
	input := "ALTER TABLE users RENAME TO customers"
	stmt, _ := Parse(input)

	alterStmt := stmt.(*AlterTableStmt)
	action, ok := alterStmt.Actions[0].(*RenameTableAction)
	if !ok {
		t.Fatalf("Expected *RenameTableAction, got %T", alterStmt.Actions[0])
	}

	if action.NewName != "customers" {
		t.Errorf("Expected new name 'customers', got %q", action.NewName)
	}
}

func TestParseAlterTableMultipleActions(t *testing.T) {
	input := "ALTER TABLE users ADD COLUMN age INT, DROP COLUMN old_field"
	stmt, _ := Parse(input)

	alterStmt := stmt.(*AlterTableStmt)
	if len(alterStmt.Actions) != 2 {
		t.Errorf("Expected 2 actions, got %d", len(alterStmt.Actions))
	}
}

// ============================================================================
// Parser Tests - TRUNCATE
// ============================================================================

func TestParseTruncateTable(t *testing.T) {
	input := "TRUNCATE TABLE users"
	stmt, _ := Parse(input)

	truncateStmt, ok := stmt.(*TruncateTableStmt)
	if !ok {
		t.Fatalf("Expected *TruncateTableStmt, got %T", stmt)
	}

	if truncateStmt.TableName != "users" {
		t.Errorf("Expected table 'users', got %q", truncateStmt.TableName)
	}
}

// ============================================================================
// Parser Tests - JOIN
// ============================================================================

func TestParseInnerJoin(t *testing.T) {
	input := "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
	stmt, _ := Parse(input)

	selectStmt := stmt.(*SelectStmt)
	if len(selectStmt.From.Joins) != 1 {
		t.Fatalf("Expected 1 JOIN, got %d", len(selectStmt.From.Joins))
	}

	join := selectStmt.From.Joins[0]
	if join.Type != JoinInner {
		t.Errorf("Expected INNER JOIN, got %v", join.Type)
	}

	if join.Table.Name != "orders" {
		t.Errorf("Expected table 'orders', got %q", join.Table.Name)
	}
}

func TestParseLeftJoin(t *testing.T) {
	input := "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id"
	stmt, _ := Parse(input)

	selectStmt := stmt.(*SelectStmt)
	join := selectStmt.From.Joins[0]
	if join.Type != JoinLeft {
		t.Errorf("Expected LEFT JOIN, got %v", join.Type)
	}
}

func TestParseMultipleJoins(t *testing.T) {
	input := `SELECT * FROM users
		INNER JOIN orders ON users.id = orders.user_id
		LEFT JOIN items ON orders.id = items.order_id`
	stmt, _ := Parse(input)

	selectStmt := stmt.(*SelectStmt)
	if len(selectStmt.From.Joins) != 2 {
		t.Errorf("Expected 2 JOINs, got %d", len(selectStmt.From.Joins))
	}
}

// ============================================================================
// Parser Tests - UNION
// ============================================================================

func TestParseUnion(t *testing.T) {
	input := "SELECT name FROM users UNION SELECT name FROM customers"
	stmt, _ := Parse(input)

	unionStmt, ok := stmt.(*UnionStmt)
	if !ok {
		t.Fatalf("Expected *UnionStmt, got %T", stmt)
	}

	if unionStmt.All {
		t.Error("Expected UNION ALL to be false")
	}
}

func TestParseUnionAll(t *testing.T) {
	input := "SELECT name FROM users UNION ALL SELECT name FROM customers"
	stmt, _ := Parse(input)

	unionStmt := stmt.(*UnionStmt)
	if !unionStmt.All {
		t.Error("Expected UNION ALL to be true")
	}
}

// ============================================================================
// Parser Tests - Expressions
// ============================================================================

func TestParseBinaryExpression(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"SELECT * FROM t WHERE a = 1", "(a = 1)"},
		{"SELECT * FROM t WHERE a <> 1", "(a <> 1)"},
		{"SELECT * FROM t WHERE a < 1", "(a < 1)"},
		{"SELECT * FROM t WHERE a <= 1", "(a <= 1)"},
		{"SELECT * FROM t WHERE a > 1", "(a > 1)"},
		{"SELECT * FROM t WHERE a >= 1", "(a >= 1)"},
		{"SELECT * FROM t WHERE a AND b", "(a AND b)"},
		{"SELECT * FROM t WHERE a OR b", "(a OR b)"},
	}

	for _, tt := range tests {
		stmt, _ := Parse(tt.input)
		selectStmt := stmt.(*SelectStmt)
		if selectStmt.Where.String() != tt.expected {
			t.Errorf("Expression %q: expected %q, got %q", tt.input, tt.expected, selectStmt.Where.String())
		}
	}
}

func TestParseFunctionCall(t *testing.T) {
	input := "SELECT COUNT(*), MAX(age), SUM(salary) FROM employees"
	stmt, _ := Parse(input)

	selectStmt := stmt.(*SelectStmt)
	if len(selectStmt.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(selectStmt.Columns))
	}

	// Check COUNT(*)
	countFunc, ok := selectStmt.Columns[0].(*FunctionCall)
	if !ok {
		t.Fatalf("Expected *FunctionCall, got %T", selectStmt.Columns[0])
	}
	if countFunc.Name != "COUNT" {
		t.Errorf("Expected COUNT, got %q", countFunc.Name)
	}
}

func TestParseCaseExpression(t *testing.T) {
	input := `SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END FROM students`
	stmt, _ := Parse(input)

	selectStmt := stmt.(*SelectStmt)
	caseExpr, ok := selectStmt.Columns[0].(*CaseExpr)
	if !ok {
		t.Fatalf("Expected *CaseExpr, got %T", selectStmt.Columns[0])
	}

	if len(caseExpr.Whens) != 2 {
		t.Errorf("Expected 2 WHEN clauses, got %d", len(caseExpr.Whens))
	}

	if caseExpr.Else == nil {
		t.Error("Expected ELSE clause")
	}
}

func TestParseCastExpression(t *testing.T) {
	input := "SELECT CAST(age AS VARCHAR(10)) FROM users"
	stmt, _ := Parse(input)

	selectStmt := stmt.(*SelectStmt)
	castExpr, ok := selectStmt.Columns[0].(*CastExpr)
	if !ok {
		t.Fatalf("Expected *CastExpr, got %T", selectStmt.Columns[0])
	}

	if castExpr.Type.Name != "VARCHAR" {
		t.Errorf("Expected VARCHAR, got %q", castExpr.Type.Name)
	}
}

// ============================================================================
// Parser Tests - Data Types
// ============================================================================

func TestParseDataTypes(t *testing.T) {
	tests := []struct {
		input       string
		expected    string
		expectedLen int
	}{
		{"CREATE TABLE t (c SEQ)", "SEQ", 0},
		{"CREATE TABLE t (c INT)", "INT", 0},
		{"CREATE TABLE t (c FLOAT)", "FLOAT", 0},
		{"CREATE TABLE t (c CHAR(10))", "CHAR", 10},
		{"CREATE TABLE t (c VARCHAR(255))", "VARCHAR", 255},
		{"CREATE TABLE t (c TEXT)", "TEXT", 0},
		{"CREATE TABLE t (c DATE)", "DATE", 0},
		{"CREATE TABLE t (c TIME)", "TIME", 0},
		{"CREATE TABLE t (c DATETIME)", "DATETIME", 0},
	}

	for _, tt := range tests {
		stmt, _ := Parse(tt.input)
		createStmt := stmt.(*CreateTableStmt)
		col := createStmt.Columns[0]

		if col.Type.Name != tt.expected {
			t.Errorf("Data type %q: expected %q, got %q", tt.input, tt.expected, col.Type.Name)
		}

		if col.Type.Size != tt.expectedLen {
			t.Errorf("Data type %q: expected size %d, got %d", tt.input, tt.expectedLen, col.Type.Size)
		}
	}
}

// ============================================================================
// Parser Tests - Multiple Statements
// ============================================================================

func TestParseMultipleStatements(t *testing.T) {
	input := "SELECT * FROM users; INSERT INTO users (id) VALUES (1);"
	stmts, err := ParseAll(input)
	if err != nil {
		t.Fatalf("ParseAll error: %v", err)
	}

	if len(stmts) != 2 {
		t.Errorf("Expected 2 statements, got %d", len(stmts))
	}
}

// ============================================================================
// Parser Tests - Error Handling
// ============================================================================

func TestParseErrors(t *testing.T) {
	tests := []string{
		"SELECT",
		"SELECT * FROM",
		"INSERT INTO users VALUES",
		"UPDATE users SET",
		"DELETE FROM users WHERE",
		"CREATE TABLE",
		"DROP TABLE",
	}

	for _, input := range tests {
		_, err := Parse(input)
		if err == nil {
			t.Errorf("Expected error for input: %q", input)
		}
	}
}

// ============================================================================
// Parser Tests - Table Alias
// ============================================================================

func TestParseTableAlias(t *testing.T) {
	input := "SELECT u.name FROM users AS u"
	stmt, _ := Parse(input)

	selectStmt := stmt.(*SelectStmt)
	if selectStmt.From.Table.Alias != "u" {
		t.Errorf("Expected alias 'u', got %q", selectStmt.From.Table.Alias)
	}
}

func TestParseTableAliasImplicit(t *testing.T) {
	input := "SELECT u.name FROM users u"
	stmt, _ := Parse(input)

	selectStmt := stmt.(*SelectStmt)
	if selectStmt.From.Table.Alias != "u" {
		t.Errorf("Expected alias 'u', got %q", selectStmt.From.Table.Alias)
	}
}

// ============================================================================
// Parser Tests - Column Qualifiers
// ============================================================================

func TestParseColumnQualifier(t *testing.T) {
	input := "SELECT users.name, users.email FROM users"
	stmt, _ := Parse(input)

	selectStmt := stmt.(*SelectStmt)
	colRef, ok := selectStmt.Columns[0].(*ColumnRef)
	if !ok {
		t.Fatalf("Expected *ColumnRef, got %T", selectStmt.Columns[0])
	}

	if colRef.Table != "users" {
		t.Errorf("Expected table 'users', got %q", colRef.Table)
	}

	if colRef.Name != "name" {
		t.Errorf("Expected column 'name', got %q", colRef.Name)
	}
}

// ============================================================================
// Parser Tests - USE and SHOW
// ============================================================================

func TestParseUse(t *testing.T) {
	input := "USE mydatabase"
	stmt, _ := Parse(input)

	useStmt, ok := stmt.(*UseStmt)
	if !ok {
		t.Fatalf("Expected *UseStmt, got %T", stmt)
	}

	if useStmt.Database != "mydatabase" {
		t.Errorf("Expected database 'mydatabase', got %q", useStmt.Database)
	}
}

func TestParseShow(t *testing.T) {
	tests := []struct {
		input string
		typ   string
		from  string
		like  string
	}{
		{"SHOW TABLES", "TABLES", "", ""},
		{"SHOW DATABASES", "DATABASES", "", ""},
		{"SHOW COLUMNS FROM users", "COLUMNS", "users", ""},
		{"SHOW TABLES LIKE 'user%'", "TABLES", "", "user%"},
	}

	for _, tt := range tests {
		stmt, _ := Parse(tt.input)
		showStmt := stmt.(*ShowStmt)

		if showStmt.Type != tt.typ {
			t.Errorf("SHOW %q: expected type %q, got %q", tt.input, tt.typ, showStmt.Type)
		}

		if showStmt.From != tt.from {
			t.Errorf("SHOW %q: expected from %q, got %q", tt.input, tt.from, showStmt.From)
		}
	}
}

// ============================================================================
// Parser Tests - Auth Statements
// ============================================================================

func TestParseCreateUser(t *testing.T) {
	input := "CREATE USER testuser IDENTIFIED BY 'password123'"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	createUser, ok := stmt.(*CreateUserStmt)
	if !ok {
		t.Fatalf("Expected *CreateUserStmt, got %T", stmt)
	}

	if createUser.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got %q", createUser.Username)
	}

	if createUser.Identified != "password123" {
		t.Errorf("Expected password 'password123', got %q", createUser.Identified)
	}
}

func TestParseCreateUserWithHost(t *testing.T) {
	input := "CREATE USER 'admin'@'localhost' IDENTIFIED BY 'secret'"
	stmt, _ := Parse(input)

	createUser := stmt.(*CreateUserStmt)
	if createUser.Username != "admin" {
		t.Errorf("Expected username 'admin', got %q", createUser.Username)
	}
	// Note: The parser handles @ but host parsing depends on lexer tokenizing
}

func TestParseCreateUserIfNotExists(t *testing.T) {
	input := "CREATE USER IF NOT EXISTS newuser IDENTIFIED BY 'pass'"
	stmt, _ := Parse(input)

	createUser := stmt.(*CreateUserStmt)
	if !createUser.IfNotExists {
		t.Error("Expected IF NOT EXISTS to be true")
	}
}

func TestParseDropUser(t *testing.T) {
	input := "DROP USER testuser"
	stmt, _ := Parse(input)

	dropUser, ok := stmt.(*DropUserStmt)
	if !ok {
		t.Fatalf("Expected *DropUserStmt, got %T", stmt)
	}

	if dropUser.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got %q", dropUser.Username)
	}
}

func TestParseDropUserIfExists(t *testing.T) {
	input := "DROP USER IF EXISTS testuser"
	stmt, _ := Parse(input)

	dropUser := stmt.(*DropUserStmt)
	if !dropUser.IfExists {
		t.Error("Expected IF EXISTS to be true")
	}
}

func TestParseAlterUser(t *testing.T) {
	input := "ALTER USER testuser IDENTIFIED BY 'newpassword'"
	stmt, _ := Parse(input)

	alterUser, ok := stmt.(*AlterUserStmt)
	if !ok {
		t.Fatalf("Expected *AlterUserStmt, got %T", stmt)
	}

	if alterUser.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got %q", alterUser.Username)
	}

	if alterUser.Identified != "newpassword" {
		t.Errorf("Expected password 'newpassword', got %q", alterUser.Identified)
	}
}

func TestParseSetPassword(t *testing.T) {
	input := "SET PASSWORD FOR testuser = 'newsecret'"
	stmt, _ := Parse(input)

	setPwd, ok := stmt.(*SetPasswordStmt)
	if !ok {
		t.Fatalf("Expected *SetPasswordStmt, got %T", stmt)
	}

	if setPwd.ForUser != "testuser" {
		t.Errorf("Expected user 'testuser', got %q", setPwd.ForUser)
	}

	if setPwd.Password != "newsecret" {
		t.Errorf("Expected password 'newsecret', got %q", setPwd.Password)
	}
}

func TestParseGrantGlobal(t *testing.T) {
	input := "GRANT ALL ON *.* TO testuser"
	stmt, _ := Parse(input)

	grant, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("Expected *GrantStmt, got %T", stmt)
	}

	if grant.To != "testuser" {
		t.Errorf("Expected user 'testuser', got %q", grant.To)
	}

	if grant.On != GrantOnAll {
		t.Errorf("Expected GrantOnAll, got %d", grant.On)
	}

	if len(grant.Privileges) != 1 || grant.Privileges[0].Type != PrivAll {
		t.Error("Expected ALL privilege")
	}
}

func TestParseGrantDatabase(t *testing.T) {
	input := "GRANT SELECT, INSERT ON mydb.* TO testuser"
	stmt, _ := Parse(input)

	grant := stmt.(*GrantStmt)
	if grant.On != GrantOnDatabase {
		t.Errorf("Expected GrantOnDatabase, got %d", grant.On)
	}

	if grant.Database != "mydb" {
		t.Errorf("Expected database 'mydb', got %q", grant.Database)
	}

	if len(grant.Privileges) != 2 {
		t.Errorf("Expected 2 privileges, got %d", len(grant.Privileges))
	}
}

func TestGrantTable(t *testing.T) {
	input := "GRANT SELECT, UPDATE ON mydb.users TO testuser"
	stmt, _ := Parse(input)

	grant := stmt.(*GrantStmt)
	if grant.On != GrantOnTable {
		t.Errorf("Expected GrantOnTable, got %d", grant.On)
	}

	if grant.Table != "users" {
		t.Errorf("Expected table 'users', got %q", grant.Table)
	}
}

func TestParseGrantWithGrantOption(t *testing.T) {
	input := "GRANT ALL ON *.* TO admin WITH GRANT OPTION"
	stmt, _ := Parse(input)

	grant := stmt.(*GrantStmt)
	if !grant.WithGrant {
		t.Error("Expected WITH GRANT OPTION to be true")
	}
}

func TestParseRevoke(t *testing.T) {
	input := "REVOKE SELECT ON mydb.* FROM testuser"
	stmt, _ := Parse(input)

	revoke := stmt.(*RevokeStmt)
	if revoke.From != "testuser" {
		t.Errorf("Expected user 'testuser', got %q", revoke.From)
	}
}

func TestParseShowGrants(t *testing.T) {
	input := "SHOW GRANTS FOR testuser"
	stmt, _ := Parse(input)

	showGrants, ok := stmt.(*ShowGrantsStmt)
	if !ok {
		t.Fatalf("Expected *ShowGrantsStmt, got %T", stmt)
	}

	if showGrants.ForUser != "testuser" {
		t.Errorf("Expected user 'testuser', got %q", showGrants.ForUser)
	}
}

func TestLexerPos(t *testing.T) {
	l := NewLexer("SELECT")
	l.NextToken()
	pos := l.Pos()
	if pos != 6 {
		t.Errorf("Pos: got %d, want 6", pos)
	}
}

func TestLexerLine(t *testing.T) {
	l := NewLexer("SELECT\nFROM")
	l.NextToken()
	l.NextToken()
	line := l.Line()
	if line != 2 {
		t.Errorf("Line: got %d, want 2", line)
	}
}

func TestLexerColumn(t *testing.T) {
	l := NewLexer("SELECT")
	l.NextToken()
	col := l.Column()
	if col < 0 {
		t.Errorf("Column: got %d, want >= 0", col)
	}
}

func TestLexerTokenize(t *testing.T) {
	tokens, err := Tokenize("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Tokenize error: %v", err)
	}
	if len(tokens) != 4 {
		t.Errorf("Tokenize: got %d tokens, want 4", len(tokens))
	}
}

func TestLexerParameter(t *testing.T) {
	input := "SELECT * FROM users WHERE id = ?"
	tokens, err := Tokenize(input)
	if err != nil {
		t.Fatalf("Tokenize error: %v", err)
	}

	found := false
	for _, tok := range tokens {
		if tok.Type == TokParameter {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected to find parameter token")
	}
}

func TestLexerParameterNamed(t *testing.T) {
	input := "SELECT * FROM users WHERE id = :id"
	tokens, err := Tokenize(input)
	if err != nil {
		t.Fatalf("Tokenize error: %v", err)
	}

	if len(tokens) == 0 {
		t.Error("Expected tokens")
	}
}

func TestParserParseBool(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"SELECT TRUE", true},
		{"SELECT FALSE", false},
	}

	for _, tt := range tests {
		stmt, err := Parse(tt.input)
		if err != nil {
			t.Errorf("Parse error for %q: %v", tt.input, err)
			continue
		}

		selectStmt, ok := stmt.(*SelectStmt)
		if !ok {
			t.Errorf("Expected *SelectStmt for %q", tt.input)
			continue
		}

		literal, ok := selectStmt.Columns[0].(*Literal)
		if !ok {
			t.Errorf("Expected *Literal for %q", tt.input)
			continue
		}

		if literal.Value.(bool) != tt.expected {
			t.Errorf("Bool value for %q: got %v, want %v", tt.input, literal.Value, tt.expected)
		}
	}
}

func TestParseDescribe(t *testing.T) {
	input := "DESCRIBE users"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	describeStmt, ok := stmt.(*DescribeStmt)
	if !ok {
		t.Fatalf("Expected *DescribeStmt, got %T", stmt)
	}

	if describeStmt.TableName != "users" {
		t.Errorf("Expected table 'users', got %q", describeStmt.TableName)
	}
}

func TestParseBackup(t *testing.T) {
	input := "BACKUP DATABASE TO '/backup/path'"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	backupStmt, ok := stmt.(*BackupStmt)
	if !ok {
		t.Fatalf("Expected *BackupStmt, got %T", stmt)
	}

	if backupStmt.Path != "/backup/path" {
		t.Errorf("Expected path '/backup/path', got %q", backupStmt.Path)
	}
}

func TestParseRestore(t *testing.T) {
	input := "RESTORE DATABASE FROM '/backup/path'"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	restoreStmt, ok := stmt.(*RestoreStmt)
	if !ok {
		t.Fatalf("Expected *RestoreStmt, got %T", stmt)
	}

	if restoreStmt.Path != "/backup/path" {
		t.Errorf("Expected path '/backup/path', got %q", restoreStmt.Path)
	}
}

func TestParseDropIndex(t *testing.T) {
	input := "DROP INDEX idx_name ON users"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	dropIndex, ok := stmt.(*DropIndexStmt)
	if !ok {
		t.Fatalf("Expected *DropIndexStmt, got %T", stmt)
	}

	if dropIndex.IndexName != "idx_name" {
		t.Errorf("Expected index 'idx_name', got %q", dropIndex.IndexName)
	}

	if dropIndex.TableName != "users" {
		t.Errorf("Expected table 'users', got %q", dropIndex.TableName)
	}
}

func TestSelectStmtString(t *testing.T) {
	stmt := &SelectStmt{
		Distinct: true,
		Columns:  []Expression{&StarExpr{}},
		From: &FromClause{
			Table: &TableRef{Name: "users"},
		},
		Where: &BinaryExpr{
			Left:  &ColumnRef{Name: "id"},
			Op:    OpEq,
			Right: &Literal{Value: 1},
		},
	}

	result := stmt.String()
	if result == "" {
		t.Error("SelectStmt.String() returned empty string")
	}
}

func TestInsertStmtString(t *testing.T) {
	stmt := &InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name"},
		Values: [][]Expression{
			{&Literal{Value: 1}, &Literal{Value: "Alice"}},
		},
	}

	result := stmt.String()
	if result == "" {
		t.Error("InsertStmt.String() returned empty string")
	}
}

func TestUpdateStmtString(t *testing.T) {
	stmt := &UpdateStmt{
		Table: "users",
		Assignments: []*Assignment{
			{Column: "name", Value: &Literal{Value: "Bob"}},
		},
		Where: &BinaryExpr{
			Left:  &ColumnRef{Name: "id"},
			Op:    OpEq,
			Right: &Literal{Value: 1},
		},
	}

	result := stmt.String()
	if result == "" {
		t.Error("UpdateStmt.String() returned empty string")
	}
}

func TestDeleteStmtString(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "users",
		Where: &BinaryExpr{
			Left:  &ColumnRef{Name: "id"},
			Op:    OpEq,
			Right: &Literal{Value: 1},
		},
	}

	result := stmt.String()
	if result == "" {
		t.Error("DeleteStmt.String() returned empty string")
	}
}

func TestCreateTableStmtString(t *testing.T) {
	stmt := &CreateTableStmt{
		TableName:   "users",
		IfNotExists: true,
		Columns: []*ColumnDef{
			{Name: "id", Type: &DataType{Name: "SEQ"}, PrimaryKey: true},
		},
	}

	result := stmt.String()
	if result == "" {
		t.Error("CreateTableStmt.String() returned empty string")
	}
}

func TestDropTableStmtString(t *testing.T) {
	stmt := &DropTableStmt{
		TableName: "users",
		IfExists:  true,
	}

	result := stmt.String()
	if result == "" {
		t.Error("DropTableStmt.String() returned empty string")
	}
}

func TestCreateIndexStmtString(t *testing.T) {
	stmt := &CreateIndexStmt{
		IndexName: "idx_name",
		TableName: "users",
		Columns:   []string{"name"},
		Unique:    true,
	}

	result := stmt.String()
	if result == "" {
		t.Error("CreateIndexStmt.String() returned empty string")
	}
}

func TestAlterTableStmtString(t *testing.T) {
	stmt := &AlterTableStmt{
		TableName: "users",
		Actions: []AlterAction{
			&AddColumnAction{Column: &ColumnDef{Name: "age", Type: &DataType{Name: "INT"}}},
		},
	}

	result := stmt.String()
	if result == "" {
		t.Error("AlterTableStmt.String() returned empty string")
	}
}

func TestAddColumnActionString(t *testing.T) {
	action := &AddColumnAction{
		Column: &ColumnDef{Name: "age", Type: &DataType{Name: "INT"}},
	}

	result := action.String()
	if result == "" {
		t.Error("AddColumnAction.String() returned empty string")
	}
}

func TestDropColumnActionString(t *testing.T) {
	action := &DropColumnAction{
		ColumnName: "age",
	}

	result := action.String()
	if result == "" {
		t.Error("DropColumnAction.String() returned empty string")
	}
}

func TestModifyColumnActionString(t *testing.T) {
	action := &ModifyColumnAction{
		Column: &ColumnDef{Name: "name", Type: &DataType{Name: "VARCHAR", Size: 200}},
	}

	result := action.String()
	if result == "" {
		t.Error("ModifyColumnAction.String() returned empty string")
	}
}

func TestRenameColumnActionString(t *testing.T) {
	action := &RenameColumnAction{
		OldName: "old_name",
		NewName: "new_name",
	}

	result := action.String()
	if result == "" {
		t.Error("RenameColumnAction.String() returned empty string")
	}
}

func TestRenameTableActionString(t *testing.T) {
	action := &RenameTableAction{
		NewName: "customers",
	}

	result := action.String()
	if result == "" {
		t.Error("RenameTableAction.String() returned empty string")
	}
}

func TestTruncateTableStmtString(t *testing.T) {
	stmt := &TruncateTableStmt{
		TableName: "users",
	}

	result := stmt.String()
	if result == "" {
		t.Error("TruncateTableStmt.String() returned empty string")
	}
}

func TestUseStmtString(t *testing.T) {
	stmt := &UseStmt{
		Database: "mydb",
	}

	result := stmt.String()
	if result == "" {
		t.Error("UseStmt.String() returned empty string")
	}
}

func TestShowStmtString(t *testing.T) {
	stmt := &ShowStmt{
		Type: "TABLES",
	}

	result := stmt.String()
	if result == "" {
		t.Error("ShowStmt.String() returned empty string")
	}
}

func TestCreateUserStmtString(t *testing.T) {
	stmt := &CreateUserStmt{
		Username:    "testuser",
		Identified:  "password",
		IfNotExists: true,
	}

	result := stmt.String()
	if result == "" {
		t.Error("CreateUserStmt.String() returned empty string")
	}
}

func TestDropUserStmtString(t *testing.T) {
	stmt := &DropUserStmt{
		Username: "testuser",
		IfExists: true,
	}

	result := stmt.String()
	if result == "" {
		t.Error("DropUserStmt.String() returned empty string")
	}
}

func TestAlterUserStmtString(t *testing.T) {
	stmt := &AlterUserStmt{
		Username:   "testuser",
		Identified: "newpassword",
	}

	result := stmt.String()
	if result == "" {
		t.Error("AlterUserStmt.String() returned empty string")
	}
}

func TestSetPasswordStmtString(t *testing.T) {
	stmt := &SetPasswordStmt{
		ForUser:  "testuser",
		Password: "secret",
	}

	result := stmt.String()
	if result == "" {
		t.Error("SetPasswordStmt.String() returned empty string")
	}
}

func TestGrantStmtString(t *testing.T) {
	stmt := &GrantStmt{
		Privileges: []*Privilege{{Type: PrivAll}},
		On:         GrantOnAll,
		To:         "testuser",
		WithGrant:  true,
	}

	result := stmt.String()
	if result == "" {
		t.Error("GrantStmt.String() returned empty string")
	}
}

func TestRevokeStmtString(t *testing.T) {
	stmt := &RevokeStmt{
		Privileges: []*Privilege{{Type: PrivSelect}},
		On:         GrantOnDatabase,
		Database:   "mydb",
		From:       "testuser",
	}

	result := stmt.String()
	if result == "" {
		t.Error("RevokeStmt.String() returned empty string")
	}
}

func TestShowGrantsStmtString(t *testing.T) {
	stmt := &ShowGrantsStmt{
		ForUser: "testuser",
	}

	result := stmt.String()
	if result == "" {
		t.Error("ShowGrantsStmt.String() returned empty string")
	}
}

func TestBackupStmtString(t *testing.T) {
	stmt := &BackupStmt{
		Path: "/backup/path",
	}

	result := stmt.String()
	if result == "" {
		t.Error("BackupStmt.String() returned empty string")
	}
}

func TestRestoreStmtString(t *testing.T) {
	stmt := &RestoreStmt{
		Path: "/backup/path",
	}

	result := stmt.String()
	if result == "" {
		t.Error("RestoreStmt.String() returned empty string")
	}
}

func TestDropIndexStmtString(t *testing.T) {
	stmt := &DropIndexStmt{
		IndexName: "idx_name",
		TableName: "users",
	}

	result := stmt.String()
	if result == "" {
		t.Error("DropIndexStmt.String() returned empty string")
	}
}

func TestDescribeStmtString(t *testing.T) {
	stmt := &DescribeStmt{
		TableName: "users",
	}

	result := stmt.String()
	if result == "" {
		t.Error("DescribeStmt.String() returned empty string")
	}
}

func TestBinaryExprString(t *testing.T) {
	expr := &BinaryExpr{
		Left:  &ColumnRef{Name: "id"},
		Op:    OpEq,
		Right: &Literal{Value: 1},
	}

	result := expr.String()
	if result == "" {
		t.Error("BinaryExpr.String() returned empty string")
	}
}

func TestUnaryExprString(t *testing.T) {
	expr := &UnaryExpr{
		Op:    OpNeg,
		Right: &Literal{Value: 5},
	}

	result := expr.String()
	if result == "" {
		t.Error("UnaryExpr.String() returned empty string")
	}
}

func TestFunctionCallString(t *testing.T) {
	expr := &FunctionCall{
		Name: "COUNT",
		Args: []Expression{&StarExpr{}},
	}

	result := expr.String()
	if result == "" {
		t.Error("FunctionCall.String() returned empty string")
	}
}

func TestCaseExprString(t *testing.T) {
	expr := &CaseExpr{
		Whens: []*CaseWhen{
			{
				Condition: &BinaryExpr{
					Left:  &ColumnRef{Name: "score"},
					Op:    OpGe,
					Right: &Literal{Value: 90},
				},
				Result: &Literal{Value: "A"},
			},
		},
		Else: &Literal{Value: "B"},
	}

	result := expr.String()
	if result == "" {
		t.Error("CaseExpr.String() returned empty string")
	}
}

func TestCastExprString(t *testing.T) {
	expr := &CastExpr{
		Expr: &ColumnRef{Name: "age"},
		Type: &DataType{Name: "VARCHAR", Size: 10},
	}

	result := expr.String()
	if result == "" {
		t.Error("CastExpr.String() returned empty string")
	}
}

func TestColumnRefString(t *testing.T) {
	expr := &ColumnRef{
		Table: "users",
		Name:  "id",
	}

	result := expr.String()
	if result == "" {
		t.Error("ColumnRef.String() returned empty string")
	}
}

func TestLiteralString(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{"int", 42},
		{"float", 3.14},
		{"string", "hello"},
		{"bool", true},
		{"nil", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lit := &Literal{Value: tt.value}
			result := lit.String()
			if result == "" {
				t.Errorf("Literal.String() for %v returned empty string", tt.value)
			}
		})
	}
}

func TestStarExprString(t *testing.T) {
	expr := &StarExpr{}

	result := expr.String()
	if result == "" {
		t.Error("StarExpr.String() returned empty string")
	}
}

func TestBetweenExprString(t *testing.T) {
	expr := &BetweenExpr{
		Expr:  &ColumnRef{Name: "age"},
		Left:  &Literal{Value: 18},
		Right: &Literal{Value: 65},
	}

	result := expr.String()
	if result == "" {
		t.Error("BetweenExpr.String() returned empty string")
	}
}

func TestInExprString(t *testing.T) {
	expr := &InExpr{
		Expr: &ColumnRef{Name: "id"},
		List: []Expression{&Literal{Value: 1}, &Literal{Value: 2}, &Literal{Value: 3}},
		Not:  false,
	}

	result := expr.String()
	if result == "" {
		t.Error("InExpr.String() returned empty string")
	}
}

func TestIsNullExprString(t *testing.T) {
	expr := &IsNullExpr{
		Expr: &ColumnRef{Name: "name"},
		Not:  false,
	}

	result := expr.String()
	if result == "" {
		t.Error("IsNullExpr.String() returned empty string")
	}
}

func TestOrderByItemString(t *testing.T) {
	item := &OrderByItem{
		Expr:      &ColumnRef{Name: "name"},
		Ascending: true,
	}

	result := item.String()
	if result == "" {
		t.Error("OrderByItem.String() returned empty string")
	}
}

func TestFromClauseString(t *testing.T) {
	clause := &FromClause{
		Table: &TableRef{Name: "users", Alias: "u"},
		Joins: []*JoinClause{
			{
				Type:  JoinInner,
				Table: &TableRef{Name: "orders"},
				On: &BinaryExpr{
					Left:  &ColumnRef{Table: "u", Name: "id"},
					Op:    OpEq,
					Right: &ColumnRef{Table: "orders", Name: "user_id"},
				},
			},
		},
	}

	result := clause.String()
	if result == "" {
		t.Error("FromClause.String() returned empty string")
	}
}

func TestJoinClauseString(t *testing.T) {
	clause := &JoinClause{
		Type:  JoinLeft,
		Table: &TableRef{Name: "orders"},
		On: &BinaryExpr{
			Left:  &ColumnRef{Name: "user_id"},
			Op:    OpEq,
			Right: &ColumnRef{Name: "id"},
		},
	}

	result := clause.String()
	if result == "" {
		t.Error("JoinClause.String() returned empty string")
	}
}

func TestTableRefString(t *testing.T) {
	ref := &TableRef{
		Name:  "users",
		Alias: "u",
	}

	result := ref.String()
	if result == "" {
		t.Error("TableRef.String() returned empty string")
	}
}

func TestColumnDefString(t *testing.T) {
	col := &ColumnDef{
		Name:       "id",
		Type:       &DataType{Name: "SEQ"},
		PrimaryKey: true,
		Nullable:   false,
	}

	result := col.String()
	if result == "" {
		t.Error("ColumnDef.String() returned empty string")
	}
}

func TestDataTypeString(t *testing.T) {
	tests := []struct {
		name string
		dt   *DataType
	}{
		{"SEQ", &DataType{Name: "SEQ"}},
		{"INT", &DataType{Name: "INT"}},
		{"VARCHAR with size", &DataType{Name: "VARCHAR", Size: 255}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.dt.String()
			if result == "" {
				t.Error("DataType.String() returned empty string")
			}
		})
	}
}

func TestAssignmentString(t *testing.T) {
	assign := &Assignment{
		Column: "name",
		Value:  &Literal{Value: "Alice"},
	}

	result := assign.String()
	if result == "" {
		t.Error("Assignment.String() returned empty string")
	}
}

func TestPrivilegeString(t *testing.T) {
	tests := []struct {
		name string
		priv PrivilegeType
	}{
		{"ALL", PrivAll},
		{"SELECT", PrivSelect},
		{"INSERT", PrivInsert},
		{"UPDATE", PrivUpdate},
		{"DELETE", PrivDelete},
		{"CREATE", PrivCreate},
		{"DROP", PrivDrop},
		{"ALTER", PrivAlter},
		{"INDEX", PrivIndex},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Privilege{Type: tt.priv}
			result := p.String()
			if result == "" {
				t.Errorf("Privilege.String() for %v returned empty string", tt.priv)
			}
		})
	}
}

func TestUnionStmtString(t *testing.T) {
	stmt := &UnionStmt{
		Left: &SelectStmt{
			Columns: []Expression{&ColumnRef{Name: "name"}},
			From:    &FromClause{Table: &TableRef{Name: "users"}},
		},
		Right: &SelectStmt{
			Columns: []Expression{&ColumnRef{Name: "name"}},
			From:    &FromClause{Table: &TableRef{Name: "customers"}},
		},
		All: true,
	}

	result := stmt.String()
	if result == "" {
		t.Error("UnionStmt.String() returned empty string")
	}
}
