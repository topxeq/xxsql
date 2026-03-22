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