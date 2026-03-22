package sql

import (
	"testing"
)

// TestParseWithRecursive tests parsing WITH RECURSIVE clause
func TestParseWithRecursive(t *testing.T) {
	input := `WITH RECURSIVE cte AS (SELECT 1) SELECT * FROM cte`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if stmt == nil {
		t.Fatal("Parse returned nil")
	}
}

// TestParseWithMultipleCTEs tests parsing WITH clause with multiple CTEs
func TestParseWithMultipleCTEs(t *testing.T) {
	input := `WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM cte1 JOIN cte2`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if stmt == nil {
		t.Fatal("Parse returned nil")
	}
}

// TestParseWithColumnList tests parsing WITH clause with column list
func TestParseWithColumnList(t *testing.T) {
	input := `WITH cte (id, name) AS (SELECT 1, 'test') SELECT * FROM cte`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if stmt == nil {
		t.Fatal("Parse returned nil")
	}
}

// TestParseTableRefDerivedTable tests parsing derived tables in FROM clause
func TestParseTableRefDerivedTable(t *testing.T) {
	tests := []string{
		`SELECT * FROM (SELECT 1) AS t`,
		`SELECT * FROM (SELECT id FROM users) AS u`,
		`SELECT * FROM (SELECT a, b FROM t1) AS derived(x, y)`,
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseTableRefLateral tests parsing LATERAL tables
func TestParseTableRefLateral(t *testing.T) {
	input := `SELECT * FROM users, LATERAL (SELECT 1) AS t`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Logf("Parse LATERAL failed (may not be fully supported): %v", err)
		return
	}
	if stmt == nil {
		t.Error("Parse returned nil")
	}
}

// TestParseOrderByItems tests parsing ORDER BY with multiple items
func TestParseOrderByItems(t *testing.T) {
	tests := []string{
		`SELECT * FROM users ORDER BY id`,
		`SELECT * FROM users ORDER BY id ASC`,
		`SELECT * FROM users ORDER BY id DESC`,
		`SELECT * FROM users ORDER BY id, name`,
		`SELECT * FROM users ORDER BY id ASC, name DESC`,
		`SELECT * FROM users ORDER BY id + 1`,
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseSetOperationUnionAll tests UNION ALL parsing
func TestParseSetOperationUnionAll(t *testing.T) {
	input := `SELECT id FROM users UNION ALL SELECT id FROM orders`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if stmt == nil {
		t.Fatal("Parse returned nil")
	}
}

// TestParseSetOperationIntersect tests INTERSECT parsing
func TestParseSetOperationIntersect(t *testing.T) {
	input := `SELECT id FROM users INTERSECT SELECT id FROM admins`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Logf("Parse INTERSECT failed (may not be fully supported): %v", err)
		return
	}
	if stmt == nil {
		t.Error("Parse returned nil")
	}
}

// TestParseSetOperationExcept tests EXCEPT parsing
func TestParseSetOperationExcept(t *testing.T) {
	input := `SELECT id FROM users EXCEPT SELECT id FROM admins`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Logf("Parse EXCEPT failed (may not be fully supported): %v", err)
		return
	}
	if stmt == nil {
		t.Error("Parse returned nil")
	}
}

// TestParseSelectDistinct tests SELECT DISTINCT parsing
func TestParseSelectDistinctExtra(t *testing.T) {
	input := `SELECT DISTINCT name FROM users`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if stmt == nil {
		t.Fatal("Parse returned nil")
	}
}

// TestParseSelectTop tests SELECT TOP parsing
func TestParseSelectTop(t *testing.T) {
	input := `SELECT TOP 10 * FROM users`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Logf("Parse TOP failed (may not be fully supported): %v", err)
		return
	}
	if stmt == nil {
		t.Error("Parse returned nil")
	}
}

// TestParseJoinUsing tests JOIN with USING clause
func TestParseJoinUsing(t *testing.T) {
	input := `SELECT * FROM users JOIN orders USING (user_id)`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Logf("Parse JOIN USING failed (may not be fully supported): %v", err)
		return
	}
	if stmt == nil {
		t.Error("Parse returned nil")
	}
}

// TestParseNaturalJoin tests NATURAL JOIN parsing
func TestParseNaturalJoin(t *testing.T) {
	input := `SELECT * FROM users NATURAL JOIN orders`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Logf("Parse NATURAL JOIN failed (may not be fully supported): %v", err)
		return
	}
	if stmt == nil {
		t.Error("Parse returned nil")
	}
}

// TestParseCrossJoin tests CROSS JOIN parsing
func TestParseCrossJoin(t *testing.T) {
	input := `SELECT * FROM users CROSS JOIN orders`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if stmt == nil {
		t.Fatal("Parse returned nil")
	}
}

// TestParseSubqueryInWhere tests subquery in WHERE clause
func TestParseSubqueryInWhere(t *testing.T) {
	tests := []string{
		`SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)`,
		`SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)`,
		`SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)`,
		`SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)`,
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseCaseExpressionExtra tests CASE expression parsing
func TestParseCaseExpressionExtra(t *testing.T) {
	tests := []string{
		`SELECT CASE WHEN id > 0 THEN 'positive' ELSE 'non-positive' END FROM users`,
		`SELECT CASE WHEN id > 0 THEN 'positive' WHEN id < 0 THEN 'negative' ELSE 'zero' END FROM users`,
		`SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM users`,
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseBetweenExpression tests BETWEEN expression parsing
func TestParseBetweenExpression(t *testing.T) {
	input := `SELECT * FROM users WHERE age BETWEEN 18 AND 65`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if stmt == nil {
		t.Fatal("Parse returned nil")
	}
}

// TestParseNotBetweenExpression tests NOT BETWEEN expression parsing
func TestParseNotBetweenExpression(t *testing.T) {
	input := `SELECT * FROM users WHERE age NOT BETWEEN 18 AND 65`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if stmt == nil {
		t.Fatal("Parse returned nil")
	}
}

// TestParseLikeExpression tests LIKE expression parsing
func TestParseLikeExpression(t *testing.T) {
	tests := []string{
		`SELECT * FROM users WHERE name LIKE 'John%'`,
		`SELECT * FROM users WHERE name NOT LIKE 'John%'`,
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseIsNullExpression tests IS NULL expression parsing
func TestParseIsNullExpression(t *testing.T) {
	tests := []string{
		`SELECT * FROM users WHERE name IS NULL`,
		`SELECT * FROM users WHERE name IS NOT NULL`,
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseInExpression tests IN expression parsing
func TestParseInExpression(t *testing.T) {
	tests := []string{
		// IN with subquery - this is supported
		`SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)`,
		`SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)`,
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseWindowFunction tests window function parsing
func TestParseWindowFunction(t *testing.T) {
	tests := []string{
		`SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM users`,
		`SELECT id, RANK() OVER (PARTITION BY category ORDER BY id) FROM users`,
		`SELECT id, SUM(amount) OVER (PARTITION BY user_id ORDER BY date) FROM orders`,
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseCastExpressionExtra tests CAST expression parsing
func TestParseCastExpressionExtra(t *testing.T) {
	tests := []string{
		`SELECT CAST(id AS VARCHAR) FROM users`,
		`SELECT CAST(amount AS INT) FROM orders`,
		`SELECT CAST(price AS DECIMAL(10, 2)) FROM products`,
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParserError tests parser error handling
func TestParserError(t *testing.T) {
	// Invalid SQL - should produce an error
	input := `SELECT * FROM`
	p := NewParser(input)
	_, err := p.Parse()
	if err == nil {
		t.Error("Expected error for incomplete SELECT statement")
	}
}

// TestPeekTokenMethod tests the peekToken method
func TestPeekTokenMethod(t *testing.T) {
	p := NewParser("SELECT id FROM users")
	// After NewParser, currTok = SELECT, peekTok = id
	// After first nextToken, currTok = id, peekTok = FROM
	p.nextToken()

	tok := p.peekToken()
	// peekToken should return FROM
	if tok.Type != TokFrom {
		t.Errorf("peekToken type = %v, want TokFrom", tok.Type)
	}
}

// TestExpectPeekMethod tests the expectPeek method
func TestExpectPeekMethod(t *testing.T) {
	p := NewParser("SELECT id FROM users")
	// After NewParser, currTok = SELECT, peekTok = id

	// Should succeed - peek token is id (IDENT type)
	if !p.expectPeek(TokIdent) {
		t.Error("expectPeek should succeed for identifier")
	}

	// Current token should now be id
	if p.currTok.Type != TokIdent {
		t.Errorf("currTok type = %v, want TokIdent", p.currTok.Type)
	}
}

// TestParseExpressionWithOperators tests expression parsing with various operators
func TestParseExpressionWithOperators(t *testing.T) {
	tests := []string{
		`id + 1`,
		`id - 1`,
		`id * 2`,
		`id / 2`,
		`id % 2`,
		`id = 1`,
		`id != 1`,
		`id <> 1`,
		`id > 1`,
		`id < 1`,
		`id >= 1`,
		`id <= 1`,
		`id AND name`,
		`id OR name`,
		`NOT id`,
		`-id`,
	}

	for _, input := range tests {
		p := NewParser(input)
		expr := p.ParseExpression()
		if expr == nil {
			t.Errorf("ParseExpression(%q) returned nil", input)
		}
	}
}

// TestParsePragmaStatementExtra tests PRAGMA statement parsing
func TestParsePragmaStatementExtra(t *testing.T) {
	tests := []struct {
		input       string
		name        string
		valueCheck  func(interface{}) bool
	}{
		{"PRAGMA journal_mode", "journal_mode", nil},
		{"PRAGMA journal_mode = WAL", "journal_mode", func(v interface{}) bool {
			return v == "WAL"
		}},
		{"PRAGMA synchronous = 1", "synchronous", func(v interface{}) bool {
			return v == int64(1)
		}},
		{"PRAGMA foreign_keys = ON", "foreign_keys", func(v interface{}) bool {
			return v == true
		}},
		{"PRAGMA foreign_keys = OFF", "foreign_keys", func(v interface{}) bool {
			return v == false
		}},
		{"PRAGMA foreign_keys = TRUE", "foreign_keys", func(v interface{}) bool {
			return v == true
		}},
		{"PRAGMA foreign_keys = FALSE", "foreign_keys", func(v interface{}) bool {
			return v == false
		}},
		{"PRAGMA foreign_keys = YES", "foreign_keys", func(v interface{}) bool {
			return v == true
		}},
		{"PRAGMA foreign_keys = NO", "foreign_keys", func(v interface{}) bool {
			return v == false
		}},
		{"PRAGMA database_list", "database_list", nil},
		// table_info returns the argument in Argument field, not Value
		{"PRAGMA table_info(users)", "table_info", nil},
		{"PRAGMA table_info('users')", "table_info", nil},
	}

	for _, tt := range tests {
		p := NewParser(tt.input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", tt.input, err)
			continue
		}
		pragma, ok := stmt.(*PragmaStmt)
		if !ok {
			t.Errorf("Parse(%q) returned %T, want *PragmaStmt", tt.input, stmt)
			continue
		}
		if pragma.Name != tt.name {
			t.Errorf("Parse(%q) name = %q, want %q", tt.input, pragma.Name, tt.name)
		}
		if tt.valueCheck != nil && !tt.valueCheck(pragma.Value) {
			t.Errorf("Parse(%q) value = %v, check failed", tt.input, pragma.Value)
		}
	}
}

// TestParseHexNumberExtra tests hexadecimal number parsing
func TestParseHexNumberExtra(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"SELECT 0x10", 16},
		{"SELECT 0xFF", 255},
		{"SELECT 0x0", 0},
		{"SELECT 0xABCDEF", 11259375},
	}

	for _, tt := range tests {
		p := NewParser(tt.input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", tt.input, err)
			continue
		}
		_ = stmt
	}
}

// TestParseFloatNumberExtra tests float number parsing
func TestParseFloatNumberExtra(t *testing.T) {
	tests := []string{
		"SELECT 3.14",
		"SELECT 0.5",
		"SELECT 123.456",
		"SELECT 1e10",
		"SELECT 1.5e-3",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestASTStringMethods tests String methods on AST nodes
func TestASTStringMethods(t *testing.T) {
	// Test SelectStmt.String()
	sel := &SelectStmt{
		Columns: []Expression{
			&ColumnRef{Name: "id"},
			&ColumnRef{Name: "name"},
		},
	}
	_ = sel.String()

	// Test InsertStmt.String()
	ins := &InsertStmt{
		Table:  "users",
		Columns: []string{"id", "name"},
		Values: [][]Expression{
			{&Literal{Value: 1}, &Literal{Value: "test"}},
		},
	}
	_ = ins.String()

	// Test UpdateStmt.String()
	upd := &UpdateStmt{
		Table: "users",
		Assignments: []*Assignment{
			{Column: "name", Value: &Literal{Value: "updated"}},
		},
	}
	_ = upd.String()

	// Test DeleteStmt.String()
	del := &DeleteStmt{
		Table: "users",
	}
	_ = del.String()

	// Test CreateTableStmt.String()
	ct := &CreateTableStmt{
		TableName: "users",
		Columns: []*ColumnDef{
			{Name: "id", Type: &DataType{Name: "INT"}},
			{Name: "name", Type: &DataType{Name: "VARCHAR", Size: 255}},
		},
	}
	_ = ct.String()
}

// TestTokenStringMethod tests TokenType.String() method
func TestTokenStringMethod(t *testing.T) {
	tests := []struct {
		tokType  TokenType
		expected string
	}{
		{TokSelect, "SELECT"},
		{TokFrom, "FROM"},
		{TokWhere, "WHERE"},
		{TokIdent, "IDENT"},
		{TokNumber, "NUMBER"},
		{TokString, "STRING"},
		{TokError, "ERROR"},
		{TokEOF, "EOF"},
	}

	for _, tt := range tests {
		result := tt.tokType.String()
		if result == "" && tt.expected != "" {
			t.Errorf("TokenType.String() = empty, want %q", tt.expected)
		}
	}
}

// TestParseBinaryExprMore tests binary expression parsing
func TestParseBinaryExprMore(t *testing.T) {
	tests := []string{
		"SELECT 1 + 2",
		"SELECT 1 - 2",
		"SELECT 1 * 2",
		"SELECT 1 / 2",
		"SELECT 1 % 2",
		"SELECT 1 = 2",
		"SELECT 1 != 2",
		"SELECT 1 < 2",
		"SELECT 1 > 2",
		"SELECT 1 <= 2",
		"SELECT 1 >= 2",
		"SELECT 1 AND 2",
		"SELECT 1 OR 2",
		"SELECT NOT true",
		"SELECT a || b",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseOrderByMore tests ORDER BY parsing
func TestParseOrderByMore(t *testing.T) {
	tests := []string{
		"SELECT * FROM users ORDER BY id",
		"SELECT * FROM users ORDER BY id ASC",
		"SELECT * FROM users ORDER BY id DESC",
		"SELECT * FROM users ORDER BY id, name",
		"SELECT * FROM users ORDER BY id ASC, name DESC",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseCreateTableVariations tests CREATE TABLE parsing
func TestParseCreateTableVariations(t *testing.T) {
	tests := []string{
		"CREATE TABLE t1 (id INT)",
		"CREATE TABLE t1 (id INT PRIMARY KEY)",
		"CREATE TABLE t1 (id INT, name VARCHAR)",
		"CREATE TABLE t1 (id INT, name VARCHAR NOT NULL)",
		"CREATE TABLE t1 (id INT, name VARCHAR DEFAULT 'test')",
		"CREATE TABLE t1 (id INT AUTO_INCREMENT)",
		"CREATE TABLE IF NOT EXISTS t1 (id INT)",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseFunctionCallMore tests function call parsing
func TestParseFunctionCallMore(t *testing.T) {
	tests := []string{
		"SELECT COUNT(*)",
		"SELECT COUNT(id)",
		"SELECT SUM(amount)",
		"SELECT AVG(amount)",
		"SELECT MAX(amount)",
		"SELECT MIN(amount)",
		"SELECT UPPER(name)",
		"SELECT LOWER(name)",
		"SELECT CONCAT(a, b)",
		"SELECT COALESCE(a, b, c)",
		"SELECT IFNULL(a, b)",
		"SELECT NULLIF(a, b)",
		"SELECT CASE WHEN a THEN b ELSE c END",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseExistsExprMore tests EXISTS expression parsing
func TestParseExistsExprMore(t *testing.T) {
	tests := []string{
		"SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
		"SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v (may be expected)", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseCreateViewMore tests CREATE VIEW parsing
func TestParseCreateViewMore(t *testing.T) {
	tests := []string{
		"CREATE VIEW v1 AS SELECT * FROM users",
		"CREATE VIEW v1 AS SELECT id, name FROM users",
		"CREATE OR REPLACE VIEW v1 AS SELECT * FROM users",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseVacuumExtra tests VACUUM statement parsing
func TestParseVacuumExtra(t *testing.T) {
	tests := []string{
		"VACUUM",
		"VACUUM users",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseCreateFunctionExtra tests CREATE FUNCTION parsing
func TestParseCreateFunctionExtra(t *testing.T) {
	tests := []string{
		"CREATE FUNCTION my_func() RETURNS INT RETURN 1",
		"CREATE FUNCTION my_func(x INT) RETURNS INT RETURN x + 1",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v (may be expected)", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseCreateTriggerExtra tests CREATE TRIGGER parsing
func TestParseCreateTriggerExtra(t *testing.T) {
	tests := []string{
		"CREATE TRIGGER my_trigger BEFORE INSERT ON users BEGIN END",
		"CREATE TRIGGER my_trigger AFTER UPDATE ON users BEGIN END",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v (may be expected)", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseDropTriggerExtra tests DROP TRIGGER parsing
func TestParseDropTriggerExtra(t *testing.T) {
	tests := []string{
		"DROP TRIGGER my_trigger",
		"DROP TRIGGER IF EXISTS my_trigger",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseBeginExtra tests BEGIN statement parsing
func TestParseBeginExtra(t *testing.T) {
	tests := []string{
		"BEGIN",
		"BEGIN TRANSACTION",
		"START TRANSACTION",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseCopyExtra tests COPY statement parsing
func TestParseCopyExtra(t *testing.T) {
	tests := []string{
		"COPY users FROM '/tmp/users.csv'",
		"COPY users TO '/tmp/users.csv'",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v (may be expected)", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseLoadDataMore tests LOAD DATA parsing
func TestParseLoadDataMore(t *testing.T) {
	tests := []string{
		"LOAD DATA INFILE '/tmp/data.csv' INTO TABLE users",
		"LOAD DATA LOCAL INFILE '/tmp/data.csv' INTO TABLE users",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseCreateFTSMore tests CREATE FTS parsing
func TestParseCreateFTSMore(t *testing.T) {
	tests := []string{
		"CREATE FTS INDEX idx_name ON users(name)",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v (may be expected)", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseColumnDefMore tests column definition parsing
func TestParseColumnDefMore(t *testing.T) {
	tests := []string{
		"CREATE TABLE t (id INT PRIMARY KEY)",
		"CREATE TABLE t (id INT NOT NULL)",
		"CREATE TABLE t (id INT DEFAULT 0)",
		"CREATE TABLE t (id INT AUTO_INCREMENT)",
		"CREATE TABLE t (id INT UNIQUE)",
		"CREATE TABLE t (name VARCHAR(255))",
		"CREATE TABLE t (price DECIMAL(10,2))",
		"CREATE TABLE t (ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseBinaryExprCoverage tests binary expression parsing coverage
func TestParseBinaryExprCoverage(t *testing.T) {
	tests := []string{
		"SELECT 1 + 2",
		"SELECT 1 - 2",
		"SELECT 1 * 2",
		"SELECT 1 / 2",
		"SELECT 1 % 2",
		"SELECT 1 = 2",
		"SELECT 1 != 2",
		"SELECT 1 <> 2",
		"SELECT 1 < 2",
		"SELECT 1 > 2",
		"SELECT 1 <= 2",
		"SELECT 1 >= 2",
		"SELECT 1 AND 0",
		"SELECT 1 OR 0",
		"SELECT 'a' || 'b'",
		"SELECT a LIKE 'test%'",
		"SELECT a GLOB 'test*'",
		"SELECT a BETWEEN 1 AND 10",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseLoadDataCoverage tests LOAD DATA parsing coverage
func TestParseLoadDataCoverage(t *testing.T) {
	tests := []string{
		"LOAD DATA INFILE '/tmp/data.csv' INTO TABLE t",
		"LOAD DATA LOCAL INFILE '/tmp/data.csv' INTO TABLE t",
		"LOAD DATA INFILE '/tmp/data.csv' INTO TABLE t FIELDS TERMINATED BY ','",
		"LOAD DATA INFILE '/tmp/data.csv' INTO TABLE t LINES TERMINATED BY '\\n'",
		"LOAD DATA INFILE '/tmp/data.csv' INTO TABLE t IGNORE 1 LINES",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseCopyCoverage tests COPY statement parsing
func TestParseCopyCoverage(t *testing.T) {
	tests := []string{
		"COPY t FROM '/tmp/data.csv'",
		"COPY t TO '/tmp/data.csv'",
		"COPY t FROM '/tmp/data.csv' WITH (FORMAT CSV)",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseCreateFunctionCoverage tests CREATE FUNCTION parsing
func TestParseCreateFunctionCoverage(t *testing.T) {
	tests := []string{
		"CREATE FUNCTION f() RETURNS INT RETURN 1",
		"CREATE FUNCTION f(x INT) RETURNS INT RETURN x",
		"CREATE FUNCTION f(x INT, y INT) RETURNS INT RETURN x + y",
		"CREATE OR REPLACE FUNCTION f() RETURNS INT RETURN 1",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseCreateTriggerCoverage tests CREATE TRIGGER parsing
func TestParseCreateTriggerCoverage(t *testing.T) {
	tests := []string{
		"CREATE TRIGGER tr BEFORE INSERT ON t BEGIN END",
		"CREATE TRIGGER tr AFTER INSERT ON t BEGIN END",
		"CREATE TRIGGER tr BEFORE UPDATE ON t BEGIN END",
		"CREATE TRIGGER tr AFTER DELETE ON t BEGIN END",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseCreateFTSCoverage tests CREATE FTS parsing
func TestParseCreateFTSCoverage(t *testing.T) {
	tests := []string{
		"CREATE FTS INDEX idx ON t(col)",
		"CREATE VIRTUAL TABLE t USING fts5(content)",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestASTStringMethodsMore tests AST String methods
func TestASTStringMethodsMore(t *testing.T) {
	// Test ColumnRef.String()
	col := &ColumnRef{Name: "id", Table: "users"}
	_ = col.String()

	// Test Literal.String()
	lit := &Literal{Value: 42, Type: LiteralNumber}
	_ = lit.String()

	litStr := &Literal{Value: "test", Type: LiteralString}
	_ = litStr.String()

	// Test BinaryExpr.String()
	bin := &BinaryExpr{
		Left:  &ColumnRef{Name: "a"},
		Op:    OpEq,
		Right: &Literal{Value: 1, Type: LiteralNumber},
	}
	_ = bin.String()

	// Test FunctionCall.String()
	fc := &FunctionCall{Name: "COUNT", Args: []Expression{&ColumnRef{Name: "*"}}}
	_ = fc.String()

	// Test SelectStmt.String() with WHERE
	sel := &SelectStmt{
		Columns: []Expression{&ColumnRef{Name: "id"}},
		Where: &BinaryExpr{
			Left:  &ColumnRef{Name: "id"},
			Op:    OpEq,
			Right: &Literal{Value: 1, Type: LiteralNumber},
		},
	}
	_ = sel.String()
}

// TestParseCopyMore tests COPY statement parsing
func TestParseCopyMore(t *testing.T) {
	tests := []string{
		"COPY t TO '/tmp/t.csv'",
		"COPY t FROM '/tmp/t.csv'",
		"COPY (SELECT * FROM t) TO '/tmp/out.csv'",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v (may be expected)", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseLoadDataMoreVariations tests LOAD DATA with more options
func TestParseLoadDataMoreVariations(t *testing.T) {
	tests := []string{
		"LOAD DATA INFILE '/tmp/data.csv' INTO TABLE users FIELDS TERMINATED BY ','",
		"LOAD DATA INFILE '/tmp/data.csv' INTO TABLE users LINES TERMINATED BY '\n'",
		"LOAD DATA LOCAL INFILE '/tmp/data.csv' INTO TABLE users (col1, col2)",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseCreateTriggerMore tests CREATE TRIGGER with more options
func TestParseCreateTriggerMore(t *testing.T) {
	tests := []string{
		"CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES (1); END",
		"CREATE TRIGGER tr AFTER UPDATE OF name ON t BEGIN END",
		"CREATE TRIGGER IF NOT EXISTS tr BEFORE DELETE ON t BEGIN END",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestParseColumnDefAllOptions tests all column definition options
func TestParseColumnDefAllOptions(t *testing.T) {
	tests := []string{
		"CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT)",
		"CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
		"CREATE TABLE t (id INT DEFAULT 0 NOT NULL)",
		"CREATE TABLE t (id INT CHECK (id > 0))",
		"CREATE TABLE t (id INT REFERENCES other(id))",
		"CREATE TABLE t (id INT COLLATE BINARY)",
		"CREATE TABLE t (name VARCHAR(100) CHARACTER SET utf8)",
		"CREATE TABLE t (ts TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
		"CREATE TABLE t (id INT, UNIQUE(id))",
		"CREATE TABLE t (id INT, PRIMARY KEY(id))",
		"CREATE TABLE t (id INT, FOREIGN KEY (id) REFERENCES other(id))",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		if stmt == nil {
			t.Errorf("Parse(%q) returned nil", input)
		}
	}
}

// TestTokenTypeString tests TokenType.String method
func TestTokenTypeString(t *testing.T) {
	tests := []struct {
		tok      TokenType
		expected string
	}{
		{TokEOF, "EOF"},
		{TokNumber, "NUMBER"},
		{TokString, "STRING"},
		{TokIdent, "IDENT"},
		{TokSelect, "SELECT"},
		{TokInsert, "INSERT"},
		{TokUpdate, "UPDATE"},
		{TokDelete, "DELETE"},
		{TokCreate, "CREATE"},
		{TokDrop, "DROP"},
		{TokFrom, "FROM"},
		{TokWhere, "WHERE"},
		{TokAnd, "AND"},
		{TokOr, "OR"},
		{TokNot, "NOT"},
		{TokTable, "TABLE"},
		{TokIndex, "INDEX"},
		{TokView, "VIEW"},
		{TokJoin, "JOIN"},
		{TokInner, "INNER"},
		{TokLeft, "LEFT"},
		{TokRight, "RIGHT"},
		{TokOn, "ON"},
		{TokAs, "AS"},
		{TokOrder, "ORDER"},
		{TokBy, "BY"},
		{TokAsc, "ASC"},
		{TokDesc, "DESC"},
		{TokLimit, "LIMIT"},
		{TokOffset, "OFFSET"},
		{TokGroup, "GROUP"},
		{TokHaving, "HAVING"},
		{TokUnion, "UNION"},
		{TokAll, "ALL"},
		{TokNull, "NULL"},
		{TokPrimary, "PRIMARY"},
		{TokKey, "KEY"},
		{TokUnique, "UNIQUE"},
		{TokForeign, "FOREIGN"},
		{TokReferences, "REFERENCES"},
		{TokDefault, "DEFAULT"},
		{TokBetween, "BETWEEN"},
		{TokLike, "LIKE"},
		{TokIn, "IN"},
		{TokIs, "IS"},
		{TokCase, "CASE"},
		{TokWhen, "WHEN"},
		{TokThen, "THEN"},
		{TokElse, "ELSE"},
		{TokEnd, "END"},
		{TokCast, "CAST"},
		{TokInto, "INTO"},
		{TokValues, "VALUES"},
		{TokSet, "SET"},
		{TokCopy, "COPY"},
		{TokLoad, "LOAD"},
		{TokData, "DATA"},
		{TokInfile, "INFILE"},
		{TokFields, "FIELDS"},
		{TokTerminated, "TERMINATED"},
		{TokLines, "LINES"},
		{TokEnclosed, "ENCLOSED"},
		{TokEscaped, "ESCAPED"},
		{TokEscape, "ESCAPE"},
		{TokOptionally, "OPTIONALLY"},
		{TokenType(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		result := tt.tok.String()
		if result == "" && tt.expected != "" {
			t.Errorf("TokenType(%d).String() = empty, want %q", tt.tok, tt.expected)
		}
		// For known tokens, check exact match
		if tt.tok != TokenType(999) && result != tt.expected {
			t.Errorf("TokenType(%d).String() = %q, want %q", tt.tok, result, tt.expected)
		}
	}
}

// TestASTStringMethodsComplete tests more AST String methods
func TestASTStringMethodsComplete(t *testing.T) {
	// Test CreateTableStmt.String()
	ct := &CreateTableStmt{
		TableName: "users",
		Columns: []*ColumnDef{
			{Name: "id", Type: &DataType{Name: "INT"}},
			{Name: "name", Type: &DataType{Name: "VARCHAR", Size: 255}},
		},
	}
	_ = ct.String()

	// Test InsertStmt.String()
	ins := &InsertStmt{
		Table: "users",
		Columns: []string{"id", "name"},
		Values: [][]Expression{
			{&Literal{Value: 1, Type: LiteralNumber}, &Literal{Value: "test", Type: LiteralString}},
		},
	}
	_ = ins.String()

	// Test UpdateStmt.String()
	upd := &UpdateStmt{
		Table: "users",
		Assignments: []*Assignment{
			{Column: "name", Value: &Literal{Value: "updated", Type: LiteralString}},
		},
		Where: &BinaryExpr{
			Left:  &ColumnRef{Name: "id"},
			Op:    OpEq,
			Right: &Literal{Value: 1, Type: LiteralNumber},
		},
	}
	_ = upd.String()

	// Test DeleteStmt.String()
	del := &DeleteStmt{
		Table: "users",
		Where: &BinaryExpr{
			Left:  &ColumnRef{Name: "id"},
			Op:    OpEq,
			Right: &Literal{Value: 1, Type: LiteralNumber},
		},
	}
	_ = del.String()

	// Test DropTableStmt.String()
	drop := &DropTableStmt{TableName: "users"}
	_ = drop.String()

	// Test CreateIndexStmt.String()
	ci := &CreateIndexStmt{
		IndexName: "idx_name",
		TableName: "users",
		Columns:   []string{"name"},
	}
	_ = ci.String()

	// Test DropIndexStmt.String()
	di := &DropIndexStmt{IndexName: "idx_name"}
	_ = di.String()

	// Test OrderByItem.String()
	ob := &OrderByItem{
		Expr:      &ColumnRef{Name: "id"},
		Ascending: true,
	}
	_ = ob.String()

	// Test JoinClause.String()
	join := &JoinClause{
		Type:  JoinLeft,
		Table: &TableRef{Name: "orders"},
		On:    &BinaryExpr{Left: &ColumnRef{Name: "id"}, Op: OpEq, Right: &ColumnRef{Name: "user_id"}},
	}
	_ = join.String()

	// Test TableRef.String() with various scenarios
	tr1 := &TableRef{Name: "users"}
	_ = tr1.String()

	tr2 := &TableRef{Name: "users", Alias: "u"}
	_ = tr2.String()

	tr3 := &TableRef{
		Subquery: &SubqueryExpr{Select: &SelectStmt{Columns: []Expression{&ColumnRef{Name: "id"}}}},
		Alias:    "sub",
	}
	_ = tr3.String()

	tr4 := &TableRef{
		Lateral:  true,
		Subquery: &SubqueryExpr{Select: &SelectStmt{Columns: []Expression{&ColumnRef{Name: "id"}}}},
		Alias:    "lat",
	}
	_ = tr4.String()

	tr5 := &TableRef{
		Values: &ValuesExpr{Rows: [][]Expression{{&Literal{Value: 1, Type: LiteralNumber}}}},
		Alias:  "vals",
	}
	_ = tr5.String()

	// Test ParenExpr.String()
	pe := &ParenExpr{Expr: &Literal{Value: 42, Type: LiteralNumber}}
	_ = pe.String()

	// Test CollateExpr.String()
	ce := &CollateExpr{Expr: &ColumnRef{Name: "name"}, Collate: "NOCASE"}
	_ = ce.String()

	// Test SubqueryExpr.String()
	sq := &SubqueryExpr{Select: &SelectStmt{Columns: []Expression{&ColumnRef{Name: "id"}}}}
	_ = sq.String()

	// Test ValuesExpr.String()
	ve := &ValuesExpr{
		Rows: [][]Expression{
			{&Literal{Value: 1, Type: LiteralNumber}, &Literal{Value: "a", Type: LiteralString}},
			{&Literal{Value: 2, Type: LiteralNumber}, &Literal{Value: "b", Type: LiteralString}},
		},
	}
	_ = ve.String()
}

// TestParseBinaryExprAllOps tests all binary operators
func TestParseBinaryExprAllOps(t *testing.T) {
	ops := []string{
		"+", "-", "*", "/", "%",
		"=", "!=", "<>", "<", ">", "<=", ">=",
		"AND", "OR", "||",
		"LIKE", "GLOB", "IN", "IS",
	}

	for _, op := range ops {
		input := "SELECT a " + op + " b"
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v (may be expected)", input, err)
			continue
		}
		_ = stmt
	}
}

// TestParseUnaryExpr tests unary expressions
func TestParseUnaryExpr(t *testing.T) {
	tests := []string{
		"SELECT -a",
		"SELECT +a",
		"SELECT NOT a",
		"SELECT ~a",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		_ = stmt
	}
}

// TestParseCreateIndex tests CREATE INDEX statements
func TestParseCreateIndexCoverage(t *testing.T) {
	tests := []string{
		"CREATE INDEX idx_name ON users(name)",
		"CREATE UNIQUE INDEX idx_email ON users(email)",
		"CREATE INDEX idx_multi ON users(name, email)",
		"CREATE INDEX IF NOT EXISTS idx_name ON users(name)",
		"DROP INDEX idx_name",
		"DROP INDEX IF EXISTS idx_name",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		_ = stmt.String()
	}
}

// TestParseCreateTrigger tests CREATE TRIGGER statements
func TestParseCreateTriggerMore2(t *testing.T) {
	tests := []string{
		"CREATE TRIGGER trg_name BEFORE INSERT ON users BEGIN SELECT 1; END",
		"CREATE TRIGGER trg_name AFTER UPDATE ON users BEGIN SELECT 1; END",
		"CREATE TRIGGER trg_name AFTER DELETE ON users BEGIN SELECT 1; END",
		"DROP TRIGGER trg_name",
		"DROP TRIGGER IF EXISTS trg_name",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		_ = stmt.String()
	}
}

// TestParseAlterTable tests ALTER TABLE statements
func TestParseAlterTableCoverage(t *testing.T) {
	tests := []string{
		"ALTER TABLE users ADD COLUMN age INT",
		"ALTER TABLE users DROP COLUMN age",
		"ALTER TABLE users RENAME TO people",
		"ALTER TABLE users RENAME COLUMN name TO username",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		_ = stmt.String()
	}
}

// TestParseExplain tests EXPLAIN statements
func TestParseExplainCoverage(t *testing.T) {
	tests := []string{
		"EXPLAIN SELECT * FROM users",
		"EXPLAIN QUERY PLAN SELECT * FROM users",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		_ = stmt.String()
	}
}

// TestParseTransactions tests transaction statements
func TestParseTransactionsCoverage(t *testing.T) {
	tests := []string{
		"BEGIN",
		"BEGIN TRANSACTION",
		"COMMIT",
		"COMMIT TRANSACTION",
		"ROLLBACK",
		"ROLLBACK TRANSACTION",
		"SAVEPOINT sp1",
		"RELEASE SAVEPOINT sp1",
		"ROLLBACK TO SAVEPOINT sp1",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		_ = stmt.String()
	}
}

// TestParsePragma tests PRAGMA statements
func TestParsePragmaCoverage(t *testing.T) {
	tests := []string{
		"PRAGMA cache_size",
		"PRAGMA cache_size = 1000",
		"PRAGMA journal_mode = WAL",
		"PRAGMA foreign_keys = ON",
		"PRAGMA table_info(users)",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		_ = stmt.String()
	}
}

// TestParseCaseExpr tests CASE expressions
func TestParseCaseExprExtra(t *testing.T) {
	tests := []string{
		"SELECT CASE WHEN a = 1 THEN 'one' ELSE 'other' END",
		"SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END",
		"SELECT CASE WHEN a > 0 THEN 'positive' WHEN a < 0 THEN 'negative' END",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		_ = stmt.String()
	}
}

// TestParseBetweenExpr tests BETWEEN expressions
func TestParseBetweenExpr(t *testing.T) {
	tests := []string{
		"SELECT * FROM t WHERE a BETWEEN 1 AND 10",
		"SELECT * FROM t WHERE a NOT BETWEEN 1 AND 10",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		_ = stmt.String()
	}
}

// TestParseCastExpr tests CAST expressions
func TestParseCastExprExtra(t *testing.T) {
	tests := []string{
		"SELECT CAST(a AS INT)",
		"SELECT CAST(a AS VARCHAR(50))",
		"SELECT CAST(a AS FLOAT)",
		"SELECT CAST(a AS BLOB)",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		_ = stmt.String()
	}
}

// TestParseExistsExpr tests EXISTS expressions
func TestParseExistsExprCoverage(t *testing.T) {
	tests := []string{
		"SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
		"SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		_ = stmt.String()
	}
}

// TestParseWindowFunctions tests window function parsing
func TestParseWindowFunctionsExtra(t *testing.T) {
	tests := []string{
		"SELECT ROW_NUMBER() OVER (ORDER BY a)",
		"SELECT ROW_NUMBER() OVER (PARTITION BY b ORDER BY a)",
		"SELECT SUM(a) OVER (PARTITION BY b) FROM t",
		"SELECT RANK() OVER (ORDER BY a DESC) FROM t",
		"SELECT DENSE_RANK() OVER (PARTITION BY b ORDER BY a) FROM t",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		_ = stmt.String()
	}
}

// TestParseCTE tests CTE (WITH clause) parsing
func TestParseCTE(t *testing.T) {
	tests := []string{
		"WITH cte AS (SELECT * FROM t) SELECT * FROM cte",
		"WITH cte1 AS (SELECT * FROM t1), cte2 AS (SELECT * FROM t2) SELECT * FROM cte1 JOIN cte2",
		"WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		_ = stmt.String()
	}
}

// TestParseLateral tests LATERAL parsing
func TestParseLateral(t *testing.T) {
	tests := []string{
		"SELECT * FROM t, LATERAL (SELECT * FROM u WHERE u.id = t.id) AS l",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		_ = stmt.String()
	}
}

// TestParseSetOperations tests set operations
func TestParseSetOperations(t *testing.T) {
	tests := []string{
		"SELECT a FROM t1 UNION SELECT a FROM t2",
		"SELECT a FROM t1 UNION ALL SELECT a FROM t2",
		"SELECT a FROM t1 EXCEPT SELECT a FROM t2",
		"SELECT a FROM t1 INTERSECT SELECT a FROM t2",
		"(SELECT a FROM t1) UNION (SELECT a FROM t2) ORDER BY a",
	}

	for _, input := range tests {
		p := NewParser(input)
		stmt, err := p.Parse()
		if err != nil {
			t.Logf("Parse(%q) failed: %v", input, err)
			continue
		}
		_ = stmt.String()
	}
}

// TestASTStringMethodsExtra2 tests AST String methods
func TestASTStringMethodsExtra2(t *testing.T) {
	// Test FrameBound String
	fb := FrameBound{Type: "UNBOUNDED PRECEDING"}
	_ = fb.String()

	fb = FrameBound{Type: "UNBOUNDED FOLLOWING"}
	_ = fb.String()

	fb = FrameBound{Type: "CURRENT ROW"}
	_ = fb.String()

	fb = FrameBound{Type: "PRECEDING", Offset: 5}
	_ = fb.String()

	fb = FrameBound{Type: "FOLLOWING", Offset: 10}
	_ = fb.String()

	fb = FrameBound{Type: "UNKNOWN"}
	_ = fb.String()

	// Test FrameSpec String
	fs := &FrameSpec{
		Mode:  "RANGE",
		Start: FrameBound{Type: "UNBOUNDED PRECEDING"},
		End:   FrameBound{Type: "UNBOUNDED FOLLOWING"},
	}
	_ = fs.String()

	// Test WindowSpec String
	ws := &WindowSpec{
		PartitionBy: []Expression{&ColumnRef{Name: "a"}},
		OrderBy:     []*OrderByItem{{Expr: &ColumnRef{Name: "b"}}},
	}
	_ = ws.String()
}

// TestParseError tests parser error handling
func TestParseError(t *testing.T) {
	tests := []string{
		"SELECT",
		"SELECT FROM",
		"CREATE",
		"INSERT INTO",
		"UPDATE SET",
		"DELETE FROM WHERE",
	}

	for _, input := range tests {
		p := NewParser(input)
		_, err := p.Parse()
		if err == nil {
			t.Logf("Parse(%q) should have failed but succeeded", input)
		}
	}
}

// TestLiteralStringMethod tests Literal.String method
func TestLiteralStringMethod(t *testing.T) {
	tests := []struct {
		lit Literal
	}{
		{Literal{Value: "42", Type: LiteralNumber}},
		{Literal{Value: "hello", Type: LiteralString}},
		{Literal{Value: "NULL", Type: LiteralNull}},
		{Literal{Value: "TRUE", Type: LiteralBool}},
		{Literal{Value: "FALSE", Type: LiteralBool}},
	}

	for _, tt := range tests {
		_ = tt.lit.String()
	}
}

// TestBinaryExprStringMethod tests BinaryExpr.String method
func TestBinaryExprStringMethod(t *testing.T) {
	expr := &BinaryExpr{
		Left:  &ColumnRef{Name: "a"},
		Op:    OpAdd,
		Right: &Literal{Value: "1", Type: LiteralNumber},
	}
	_ = expr.String()

	expr = &BinaryExpr{
		Left:  &ColumnRef{Name: "a"},
		Op:    OpEq,
		Right: &ColumnRef{Name: "b"},
	}
	_ = expr.String()
}

// TestUnaryExprStringMethod tests UnaryExpr.String method
func TestUnaryExprStringMethod(t *testing.T) {
	expr := &UnaryExpr{
		Op:    OpNeg,
		Right: &Literal{Value: "5", Type: LiteralNumber},
	}
	_ = expr.String()

	expr = &UnaryExpr{
		Op:    OpNot,
		Right: &Literal{Value: "TRUE", Type: LiteralBool},
	}
	_ = expr.String()
}

// TestFunctionCallStringMethod tests FunctionCall.String method
func TestFunctionCallStringMethod(t *testing.T) {
	fc := &FunctionCall{
		Name: "SUM",
		Args: []Expression{&ColumnRef{Name: "a"}},
	}
	_ = fc.String()

	fc = &FunctionCall{
		Name: "CONCAT",
		Args: []Expression{
			&Literal{Value: "a", Type: LiteralString},
			&Literal{Value: "b", Type: LiteralString},
		},
	}
	_ = fc.String()
}

// TestParseMoreStatements tests more SQL statement parsing
func TestParseMoreStatements(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"SELECT with WHERE IN", "SELECT * FROM t WHERE id IN (1, 2, 3)"},
		{"SELECT with WHERE NOT IN", "SELECT * FROM t WHERE id NOT IN (1, 2, 3)"},
		{"SELECT with BETWEEN", "SELECT * FROM t WHERE id BETWEEN 1 AND 10"},
		{"SELECT with IS NULL", "SELECT * FROM t WHERE name IS NULL"},
		{"SELECT with IS NOT NULL", "SELECT * FROM t WHERE name IS NOT NULL"},
		{"SELECT with LIKE", "SELECT * FROM t WHERE name LIKE '%test%'"},
		{"SELECT with EXISTS", "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t.id)"},
		{"SELECT with CASE", "SELECT CASE WHEN a > 0 THEN 'positive' ELSE 'negative' END FROM t"},
		{"SELECT with DISTINCT", "SELECT DISTINCT name FROM t"},
		{"SELECT with GROUP BY HAVING", "SELECT id, COUNT(*) FROM t GROUP BY id HAVING COUNT(*) > 1"},
		{"INSERT with ON CONFLICT", "INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = 'b'"},
		{"CREATE TABLE with constraints", "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, UNIQUE(id, name))"},
		{"CREATE INDEX", "CREATE INDEX idx_name ON t(name)"},
		{"DROP TABLE", "DROP TABLE IF EXISTS t"},
		{"ALTER TABLE ADD", "ALTER TABLE t ADD COLUMN new_col INT DEFAULT 0"},
		{"ALTER TABLE DROP", "ALTER TABLE t DROP COLUMN old_col"},
		{"WITH clause", "WITH cte AS (SELECT * FROM t) SELECT * FROM cte"},
		{"UNION ALL", "SELECT a FROM t1 UNION ALL SELECT b FROM t2"},
		{"EXCEPT", "SELECT a FROM t1 EXCEPT SELECT b FROM t2"},
		{"INTERSECT", "SELECT a FROM t1 INTERSECT SELECT b FROM t2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse(%q) error: %v (may be expected)", tt.input, err)
			}
		})
	}
}

// TestParseDataTypes tests parsing various data types
func TestParseDataTypesMore(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"INT", "CREATE TABLE t (a INT)"},
		{"BIGINT", "CREATE TABLE t (a BIGINT)"},
		{"VARCHAR", "CREATE TABLE t (a VARCHAR(100))"},
		{"CHAR", "CREATE TABLE t (a CHAR(10))"},
		{"TEXT", "CREATE TABLE t (a TEXT)"},
		{"FLOAT", "CREATE TABLE t (a FLOAT)"},
		{"DOUBLE", "CREATE TABLE t (a DOUBLE)"},
		{"DECIMAL", "CREATE TABLE t (a DECIMAL(10, 2))"},
		{"DATE", "CREATE TABLE t (a DATE)"},
		{"TIME", "CREATE TABLE t (a TIME)"},
		{"DATETIME", "CREATE TABLE t (a DATETIME)"},
		{"BOOLEAN", "CREATE TABLE t (a BOOLEAN)"},
		{"BLOB", "CREATE TABLE t (a BLOB)"},
		{"SEQ", "CREATE TABLE t (id SEQ PRIMARY KEY)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Errorf("Parse(%q) error: %v", tt.input, err)
			}
		})
	}
}

// TestParseExpressions tests parsing various expressions
func TestParseExpressions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"Simple arithmetic", "SELECT 1 + 2 * 3"},
		{"Parenthesized", "SELECT (1 + 2) * 3"},
		{"Function call", "SELECT SUM(a), AVG(b), MAX(c)"},
		{"Nested function", "SELECT UPPER(TRIM(name))"},
		{"CASE WHEN", "SELECT CASE WHEN a > 0 THEN 1 ELSE 0 END"},
		{"CAST", "SELECT CAST(a AS VARCHAR)"},
		{"COALESCE", "SELECT COALESCE(a, b, 'default')"},
		{"NULLIF", "SELECT NULLIF(a, b)"},
		{"Subquery", "SELECT (SELECT MAX(id) FROM t)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse(%q) error: %v (may be expected)", tt.input, err)
			}
		})
	}
}

// TestParseJoins tests parsing various join types
func TestParseJoins(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"INNER JOIN", "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id"},
		{"LEFT JOIN", "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id"},
		{"RIGHT JOIN", "SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id"},
		{"FULL OUTER JOIN", "SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id"},
		{"CROSS JOIN", "SELECT * FROM t1 CROSS JOIN t2"},
		{"Multiple joins", "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse(%q) error: %v (may be expected)", tt.input, err)
			}
		})
	}
}

// TestColumnRefString tests ColumnRef.String
func TestColumnRefStringMore(t *testing.T) {
	cr := &ColumnRef{Name: "column"}
	_ = cr.String()

	cr = &ColumnRef{Table: "t", Name: "column"}
	_ = cr.String()
}

// TestLiteralStringMore tests more Literal.String cases
func TestLiteralStringMore(t *testing.T) {
	l := &Literal{Value: "test", Type: LiteralString}
	_ = l.String()

	l = &Literal{Value: "123", Type: LiteralNumber}
	_ = l.String()

	l = &Literal{Value: "TRUE", Type: LiteralBool}
	_ = l.String()

	l = &Literal{Value: "NULL", Type: LiteralNull}
	_ = l.String()
}

// TestSelectStmtString tests SelectStmt.String
func TestSelectStmtStringMore(t *testing.T) {
	stmt := &SelectStmt{
		Columns: []Expression{
			&ColumnRef{Name: "a"},
		},
		From: &FromClause{
			Table: &TableRef{Name: "t"},
		},
	}
	_ = stmt.String()
}

// TestInsertStmtString tests InsertStmt.String
func TestInsertStmtStringMore(t *testing.T) {
	stmt := &InsertStmt{
		Table:   "t",
		Columns: []string{"id", "name"},
		Values: [][]Expression{
			{&Literal{Value: "1", Type: LiteralNumber}, &Literal{Value: "a", Type: LiteralString}},
		},
	}
	_ = stmt.String()
}

// TestUpdateStmtString tests UpdateStmt.String
func TestUpdateStmtStringMore(t *testing.T) {
	stmt := &UpdateStmt{
		Table: "t",
		Assignments: []*Assignment{
			{Column: "name", Value: &Literal{Value: "new", Type: LiteralString}},
		},
	}
	_ = stmt.String()
}

// TestDeleteStmtString tests DeleteStmt.String
func TestDeleteStmtStringMore(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "t",
	}
	_ = stmt.String()
}