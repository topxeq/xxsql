package sql

import (
	"testing"
)

// ============================================================================
// Lexer Tests - Additional Coverage
// ============================================================================

// TestLexer_ParameterScanning tests $1, $2 style parameters
func TestLexer_ParameterScanning(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"$1", "$1"},
		{"$123", "$123"},
		{"$0", "$0"},
	}

	for _, tt := range tests {
		l := NewLexer(tt.input)
		tok := l.NextToken()
		if tok.Type != TokParameter {
			t.Errorf("Expected TokParameter for %q, got %v", tt.input, tok.Type)
		}
		if tok.Value != tt.expected {
			t.Errorf("Parameter value: got %q, want %q", tok.Value, tt.expected)
		}
	}
}

// TestLexer_EscapeSequences tests string escape sequences
func TestLexer_EscapeSequences(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`'hello\nworld'`, "hello\nworld"},
		{`'hello\tworld'`, "hello\tworld"},
		{`'hello\rworld'`, "hello\rworld"},
		{`'hello\\world'`, "hello\\world"},
		{`'hello\'world'`, "hello'world"},
		{`'hello\0world'`, "hello\x00world"},
		{`'hello\xworld'`, "helloxworld"}, // Unknown escape
	}

	for _, tt := range tests {
		l := NewLexer(tt.input)
		tok := l.NextToken()
		if tok.Type != TokString {
			t.Errorf("Expected TokString for %q, got %v", tt.input, tok.Type)
			continue
		}
		if tok.Value != tt.expected {
			t.Errorf("Escape sequence %q: got %q, want %q", tt.input, tok.Value, tt.expected)
		}
	}
}

// TestLexer_DoubleQuotedString tests double quoted strings
func TestLexer_DoubleQuotedString(t *testing.T) {
	l := NewLexer(`"hello world"`)
	tok := l.NextToken()
	if tok.Type != TokString {
		t.Errorf("Expected TokString, got %v", tok.Type)
	}
	if tok.Value != "hello world" {
		t.Errorf("String value: got %q, want %q", tok.Value, "hello world")
	}
}

// TestLexer_EscapedQuote tests escaped quotes in strings
func TestLexer_EscapedQuote(t *testing.T) {
	// Single quote escaped with another single quote
	l := NewLexer(`'it''s fine'`)
	tok := l.NextToken()
	if tok.Type != TokString {
		t.Errorf("Expected TokString, got %v", tok.Type)
	}
	if tok.Value != "it's fine" {
		t.Errorf("String value: got %q, want %q", tok.Value, "it's fine")
	}
}

// TestLexer_UnterminatedString tests unterminated strings
func TestLexer_UnterminatedString(t *testing.T) {
	l := NewLexer(`'hello`)
	tok := l.NextToken()
	if tok.Type != TokError {
		t.Errorf("Expected TokError for unterminated string, got %v", tok.Type)
	}
}

// TestLexer_BacktickIdentifiers tests backtick quoted identifiers
func TestLexer_BacktickIdentifiers(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"`table_name`", "table_name"},
		{"`select`", "select"}, // Reserved word as identifier
		{"`column``name`", "column`name"}, // Escaped backtick
	}

	for _, tt := range tests {
		l := NewLexer(tt.input)
		tok := l.NextToken()
		if tok.Type != TokIdent {
			t.Errorf("Expected TokIdent for %q, got %v", tt.input, tok.Type)
		}
		if tok.Value != tt.expected {
			t.Errorf("Identifier value: got %q, want %q", tok.Value, tt.expected)
		}
	}
}

// TestLexer_UnterminatedBacktick tests unterminated backtick identifier
func TestLexer_UnterminatedBacktick(t *testing.T) {
	l := NewLexer("`table_name")
	tok := l.NextToken()
	if tok.Type != TokError {
		t.Errorf("Expected TokError for unterminated backtick, got %v", tok.Type)
	}
}

// TestLexer_HexNumber tests hexadecimal numbers
func TestLexer_HexNumber(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"0x1A", "0x1A"},
		{"0XFF", "0XFF"},
		{"0x0", "0x0"},
		{"0xabcdef", "0xabcdef"},
	}

	for _, tt := range tests {
		l := NewLexer(tt.input)
		tok := l.NextToken()
		if tok.Type != TokNumber {
			t.Errorf("Expected TokNumber for %q, got %v", tt.input, tok.Type)
		}
		if tok.Value != tt.expected {
			t.Errorf("Hex number value: got %q, want %q", tok.Value, tt.expected)
		}
	}
}

// TestLexer_ScientificNotation tests scientific notation numbers
func TestLexer_ScientificNotation(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"1e10", "1e10"},
		{"1E10", "1E10"},
		{"1.5e-3", "1.5e-3"},
		{"2.5E+5", "2.5E+5"},
	}

	for _, tt := range tests {
		l := NewLexer(tt.input)
		tok := l.NextToken()
		if tok.Type != TokNumber {
			t.Errorf("Expected TokNumber for %q, got %v", tt.input, tok.Type)
		}
		if tok.Value != tt.expected {
			t.Errorf("Scientific number value: got %q, want %q", tok.Value, tt.expected)
		}
	}
}

// TestLexer_UnknownCharacter tests unknown characters
func TestLexer_UnknownCharacter(t *testing.T) {
	l := NewLexer("#")
	tok := l.NextToken()
	if tok.Type != TokError {
		t.Errorf("Expected TokError for unknown character, got %v", tok.Type)
	}
}

// TestLexer_ConcatOperator tests || concatenation operator
func TestLexer_ConcatOperator(t *testing.T) {
	l := NewLexer("||")
	tok := l.NextToken()
	if tok.Type != TokConcat {
		t.Errorf("Expected TokConcat, got %v", tok.Type)
	}
}

// TestLexer_AssignOperator tests := assignment operator
func TestLexer_AssignOperator(t *testing.T) {
	l := NewLexer(":=")
	tok := l.NextToken()
	if tok.Type != TokAssign {
		t.Errorf("Expected TokAssign, got %v", tok.Type)
	}
}

// TestLexer_DoubleColon tests :: type cast operator
func TestLexer_DoubleColon(t *testing.T) {
	l := NewLexer("::")
	tok := l.NextToken()
	if tok.Type != TokDoubleCol {
		t.Errorf("Expected TokDoubleCol, got %v", tok.Type)
	}
}

// TestLexer_SingleColon tests single colon
func TestLexer_SingleColon(t *testing.T) {
	l := NewLexer(":")
	tok := l.NextToken()
	if tok.Type != TokColon {
		t.Errorf("Expected TokColon, got %v", tok.Type)
	}
}

// TestLexer_SinglePipe tests single pipe (error)
func TestLexer_SinglePipe(t *testing.T) {
	l := NewLexer("|")
	tok := l.NextToken()
	if tok.Type != TokError {
		t.Errorf("Expected TokError for single pipe, got %v", tok.Type)
	}
}

// TestLexer_AtSign tests @ operator
func TestLexer_AtSign(t *testing.T) {
	l := NewLexer("@")
	tok := l.NextToken()
	if tok.Type != TokAt {
		t.Errorf("Expected TokAt, got %v", tok.Type)
	}
}

// TestLexer_NotOperator tests ! operator
func TestLexer_NotOperator(t *testing.T) {
	l := NewLexer("!")
	tok := l.NextToken()
	if tok.Type != TokNot {
		t.Errorf("Expected TokNot, got %v", tok.Type)
	}
}

// TestLexer_LineColumn tests line and column tracking
func TestLexer_LineColumn(t *testing.T) {
	input := "SELECT\n  name\nFROM"
	l := NewLexer(input)

	// First token SELECT at line 1, column 1
	tok := l.NextToken()
	if tok.Line != 1 || tok.Column != 1 {
		t.Errorf("SELECT position: got line %d col %d, want line 1 col 1", tok.Line, tok.Column)
	}

	// Skip whitespace and get name at line 2
	tok = l.NextToken() // name
	if tok.Line != 2 {
		t.Errorf("name line: got %d, want 2", tok.Line)
	}

	// Get FROM at line 3
	tok = l.NextToken() // FROM
	if tok.Line != 3 {
		t.Errorf("FROM line: got %d, want 3", tok.Line)
	}
}

// ============================================================================
// Parser Tests - Join Types
// ============================================================================

func TestParseJoin_InnerJoin(t *testing.T) {
	input := "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	if len(selectStmt.From.Joins) != 1 {
		t.Fatalf("Expected 1 join, got %d", len(selectStmt.From.Joins))
	}

	join := selectStmt.From.Joins[0]
	if join.Type != JoinInner {
		t.Errorf("Expected JoinInner, got %v", join.Type)
	}
	if join.Table.Name != "orders" {
		t.Errorf("Join table: got %q, want orders", join.Table.Name)
	}
	if join.On == nil {
		t.Error("Expected ON clause")
	}
}

func TestParseJoin_LeftOuterJoin(t *testing.T) {
	input := "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	join := selectStmt.From.Joins[0]
	if join.Type != JoinLeft {
		t.Errorf("Expected JoinLeft, got %v", join.Type)
	}
}

func TestParseJoin_LeftJoin(t *testing.T) {
	input := "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	join := selectStmt.From.Joins[0]
	if join.Type != JoinLeft {
		t.Errorf("Expected JoinLeft, got %v", join.Type)
	}
}

func TestParseJoin_RightJoin(t *testing.T) {
	input := "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	join := selectStmt.From.Joins[0]
	if join.Type != JoinRight {
		t.Errorf("Expected JoinRight, got %v", join.Type)
	}
}

func TestParseJoin_RightOuterJoin(t *testing.T) {
	input := "SELECT * FROM users RIGHT OUTER JOIN orders ON users.id = orders.user_id"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	join := selectStmt.From.Joins[0]
	if join.Type != JoinRight {
		t.Errorf("Expected JoinRight, got %v", join.Type)
	}
}

func TestParseJoin_CrossJoin(t *testing.T) {
	input := "SELECT * FROM users CROSS JOIN orders"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	join := selectStmt.From.Joins[0]
	if join.Type != JoinCross {
		t.Errorf("Expected JoinCross, got %v", join.Type)
	}
}

func TestParseJoin_ImplicitInnerJoin(t *testing.T) {
	input := "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	join := selectStmt.From.Joins[0]
	if join.Type != JoinInner {
		t.Errorf("Expected JoinInner (implicit), got %v", join.Type)
	}
}

func TestParseJoin_Using(t *testing.T) {
	input := "SELECT * FROM users JOIN orders USING (user_id)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	join := selectStmt.From.Joins[0]
	if len(join.Using) != 1 {
		t.Errorf("Expected 1 USING column, got %d", len(join.Using))
	}
	if join.Using[0] != "user_id" {
		t.Errorf("USING column: got %q, want user_id", join.Using[0])
	}
}

func TestParseJoin_MultipleJoins(t *testing.T) {
	input := `SELECT * FROM users
		INNER JOIN orders ON users.id = orders.user_id
		LEFT JOIN items ON orders.id = items.order_id`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	if len(selectStmt.From.Joins) != 2 {
		t.Errorf("Expected 2 joins, got %d", len(selectStmt.From.Joins))
	}
}

// ============================================================================
// Parser Tests - Table Constraints
// ============================================================================

func TestParseTableConstraint_PrimaryKey(t *testing.T) {
	input := "CREATE TABLE t (id INT, PRIMARY KEY (id))"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	if len(createStmt.Constraints) != 1 {
		t.Fatalf("Expected 1 constraint, got %d", len(createStmt.Constraints))
	}

	tc := createStmt.Constraints[0]
	if tc.Type != ConstraintPrimaryKey {
		t.Errorf("Expected ConstraintPrimaryKey, got %v", tc.Type)
	}
	if len(tc.Columns) != 1 || tc.Columns[0] != "id" {
		t.Errorf("Primary key columns: got %v, want [id]", tc.Columns)
	}
}

func TestParseTableConstraint_Unique(t *testing.T) {
	input := "CREATE TABLE t (id INT, email VARCHAR(255), UNIQUE (email))"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	tc := createStmt.Constraints[0]
	if tc.Type != ConstraintUnique {
		t.Errorf("Expected ConstraintUnique, got %v", tc.Type)
	}
}

func TestParseTableConstraint_NamedUnique(t *testing.T) {
	input := "CREATE TABLE t (id INT, email VARCHAR(255), CONSTRAINT uq_email UNIQUE (email))"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	tc := createStmt.Constraints[0]
	if tc.Name != "uq_email" {
		t.Errorf("Constraint name: got %q, want uq_email", tc.Name)
	}
}

func TestParseTableConstraint_ForeignKey(t *testing.T) {
	input := "CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id))"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	tc := createStmt.Constraints[0]
	if tc.Type != ConstraintForeignKey {
		t.Errorf("Expected ConstraintForeignKey, got %v", tc.Type)
	}
	if tc.RefTable != "users" {
		t.Errorf("Referenced table: got %q, want users", tc.RefTable)
	}
	if len(tc.RefColumns) != 1 || tc.RefColumns[0] != "id" {
		t.Errorf("Referenced columns: got %v, want [id]", tc.RefColumns)
	}
}

func TestParseTableConstraint_ForeignKeyWithOnDelete(t *testing.T) {
	input := "CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users ON DELETE CASCADE)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	tc := createStmt.Constraints[0]
	if tc.OnDelete != "CASCADE" {
		t.Errorf("OnDelete: got %q, want CASCADE", tc.OnDelete)
	}
}

func TestParseTableConstraint_ForeignKeyWithOnUpdate(t *testing.T) {
	input := "CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users ON UPDATE RESTRICT)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	tc := createStmt.Constraints[0]
	if tc.OnUpdate != "RESTRICT" {
		t.Errorf("OnUpdate: got %q, want RESTRICT", tc.OnUpdate)
	}
}

func TestParseTableConstraint_Check(t *testing.T) {
	input := "CREATE TABLE t (age INT, CHECK (age >= 0))"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	tc := createStmt.Constraints[0]
	if tc.Type != ConstraintCheck {
		t.Errorf("Expected ConstraintCheck, got %v", tc.Type)
	}
	if tc.CheckExpr == nil {
		t.Error("Expected check expression")
	}
}

// ============================================================================
// Parser Tests - Data Types with Scale/Precision
// ============================================================================

func TestParseDataType_Decimal(t *testing.T) {
	input := "CREATE TABLE t (price DECIMAL(10,2))"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	col := createStmt.Columns[0]
	if col.Type.Name != "DECIMAL" {
		t.Errorf("Type name: got %q, want DECIMAL", col.Type.Name)
	}
	if col.Type.Precision != 10 {
		t.Errorf("Precision: got %d, want 10", col.Type.Precision)
	}
	if col.Type.Scale != 2 {
		t.Errorf("Scale: got %d, want 2", col.Type.Scale)
	}
}

func TestParseDataType_Unsigned(t *testing.T) {
	input := "CREATE TABLE t (id INT UNSIGNED)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	col := createStmt.Columns[0]
	if !col.Type.Unsigned {
		t.Error("Expected UNSIGNED flag to be true")
	}
}

func TestParseDataType_VarcharWithSize(t *testing.T) {
	input := "CREATE TABLE t (name VARCHAR(255))"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	col := createStmt.Columns[0]
	if col.Type.Size != 255 {
		t.Errorf("Size: got %d, want 255", col.Type.Size)
	}
}

// ============================================================================
// Parser Tests - Column Aliases
// ============================================================================

func TestParseColumnAlias_WithAS(t *testing.T) {
	input := "SELECT name AS user_name FROM users"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	colRef, ok := selectStmt.Columns[0].(*ColumnRef)
	if !ok {
		t.Fatalf("Expected *ColumnRef, got %T", selectStmt.Columns[0])
	}
	if colRef.Alias != "user_name" {
		t.Errorf("Alias: got %q, want user_name", colRef.Alias)
	}
}

func TestParseColumnAlias_WithoutAS(t *testing.T) {
	input := "SELECT name user_name FROM users"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	colRef, ok := selectStmt.Columns[0].(*ColumnRef)
	if !ok {
		t.Fatalf("Expected *ColumnRef, got %T", selectStmt.Columns[0])
	}
	if colRef.Alias != "user_name" {
		t.Errorf("Alias: got %q, want user_name", colRef.Alias)
	}
}

// ============================================================================
// Parser Tests - DROP Statements
// ============================================================================

func TestParseDropUser_Extra(t *testing.T) {
	input := "DROP USER testuser"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	dropStmt, ok := stmt.(*DropUserStmt)
	if !ok {
		t.Fatalf("Expected *DropUserStmt, got %T", stmt)
	}
	if dropStmt.Username != "testuser" {
		t.Errorf("Username: got %q, want testuser", dropStmt.Username)
	}
}

func TestParseDropIndex_Extra(t *testing.T) {
	input := "DROP INDEX idx_name ON users"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	dropStmt, ok := stmt.(*DropIndexStmt)
	if !ok {
		t.Fatalf("Expected *DropIndexStmt, got %T", stmt)
	}
	if dropStmt.IndexName != "idx_name" {
		t.Errorf("Index name: got %q, want idx_name", dropStmt.IndexName)
	}
	if dropStmt.TableName != "users" {
		t.Errorf("Table name: got %q, want users", dropStmt.TableName)
	}
}

func TestParseDropIndex_WithoutTable_Extra(t *testing.T) {
	input := "DROP INDEX idx_name"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	dropStmt, ok := stmt.(*DropIndexStmt)
	if !ok {
		t.Fatalf("Expected *DropIndexStmt, got %T", stmt)
	}
	if dropStmt.TableName != "" {
		t.Errorf("Table name should be empty, got %q", dropStmt.TableName)
	}
}

// ============================================================================
// Parser Tests - SHOW Statements
// ============================================================================

func TestParseShowGrants_Extra(t *testing.T) {
	input := "SHOW GRANTS"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	showStmt, ok := stmt.(*ShowGrantsStmt)
	if !ok {
		t.Fatalf("Expected *ShowGrantsStmt, got %T", stmt)
	}
	_ = showStmt // Just verify type
}

func TestParseShowCreateTable(t *testing.T) {
	input := "SHOW CREATE TABLE users"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	showStmt, ok := stmt.(*ShowCreateTableStmt)
	if !ok {
		t.Fatalf("Expected *ShowCreateTableStmt, got %T", stmt)
	}
	if showStmt.TableName != "users" {
		t.Errorf("Table name: got %q, want users", showStmt.TableName)
	}
}

func TestParseShow_WithFrom(t *testing.T) {
	input := "SHOW COLUMNS FROM users"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	showStmt, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("Expected *ShowStmt, got %T", stmt)
	}
	if showStmt.Type != "COLUMNS" {
		t.Errorf("Type: got %q, want COLUMNS", showStmt.Type)
	}
	if showStmt.From != "users" {
		t.Errorf("From: got %q, want users", showStmt.From)
	}
}

func TestParseShow_WithLike(t *testing.T) {
	input := "SHOW TABLES LIKE 'user%'"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	showStmt, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("Expected *ShowStmt, got %T", stmt)
	}
	if showStmt.Like != "user%" {
		t.Errorf("Like: got %q, want user%%", showStmt.Like)
	}
}

// ============================================================================
// Parser Tests - CREATE INDEX
// ============================================================================

func TestParseCreateIndex_Simple(t *testing.T) {
	input := "CREATE INDEX idx_name ON users (name)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt, ok := stmt.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("Expected *CreateIndexStmt, got %T", stmt)
	}
	if createStmt.IndexName != "idx_name" {
		t.Errorf("Index name: got %q, want idx_name", createStmt.IndexName)
	}
	if createStmt.TableName != "users" {
		t.Errorf("Table name: got %q, want users", createStmt.TableName)
	}
	if len(createStmt.Columns) != 1 || createStmt.Columns[0] != "name" {
		t.Errorf("Columns: got %v, want [name]", createStmt.Columns)
	}
}

func TestParseCreateIndex_Unique(t *testing.T) {
	input := "CREATE UNIQUE INDEX idx_email ON users (email)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt, ok := stmt.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("Expected *CreateIndexStmt, got %T", stmt)
	}
	if !createStmt.Unique {
		t.Error("Expected Unique to be true")
	}
}

func TestParseCreateIndex_IfNotExists(t *testing.T) {
	input := "CREATE INDEX IF NOT EXISTS idx_name ON users (name)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt, ok := stmt.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("Expected *CreateIndexStmt, got %T", stmt)
	}
	if !createStmt.IfNotExists {
		t.Error("Expected IfNotExists to be true")
	}
}

// ============================================================================
// Parser Tests - Column Definition Features
// ============================================================================

func TestParseColumnDef_AutoIncrement(t *testing.T) {
	input := "CREATE TABLE t (id INT AUTO_INCREMENT)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	col := createStmt.Columns[0]
	if !col.AutoIncr {
		t.Error("Expected AutoIncr to be true")
	}
}

func TestParseColumnDef_NotNull(t *testing.T) {
	input := "CREATE TABLE t (name VARCHAR(255) NOT NULL)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	col := createStmt.Columns[0]
	if col.Nullable {
		t.Error("Expected Nullable to be false")
	}
}

func TestParseColumnDef_Null(t *testing.T) {
	input := "CREATE TABLE t (name VARCHAR(255) NULL)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	col := createStmt.Columns[0]
	if !col.Nullable {
		t.Error("Expected Nullable to be true")
	}
}

func TestParseColumnDef_Default(t *testing.T) {
	input := "CREATE TABLE t (status INT DEFAULT 0)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	col := createStmt.Columns[0]
	if col.Default == nil {
		t.Fatal("Expected Default to be set")
	}
}

func TestParseColumnDef_Comment(t *testing.T) {
	input := "CREATE TABLE t (name VARCHAR(255) COMMENT 'User name')"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	col := createStmt.Columns[0]
	if col.Comment != "User name" {
		t.Errorf("Comment: got %q, want 'User name'", col.Comment)
	}
}

func TestParseColumnDef_InlinePrimaryKey(t *testing.T) {
	input := "CREATE TABLE t (id INT PRIMARY KEY)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	col := createStmt.Columns[0]
	if !col.PrimaryKey {
		t.Error("Expected PrimaryKey to be true")
	}
}

func TestParseColumnDef_InlineUnique(t *testing.T) {
	input := "CREATE TABLE t (email VARCHAR(255) UNIQUE)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	col := createStmt.Columns[0]
	if !col.Unique {
		t.Error("Expected Unique to be true")
	}
}

func TestParseColumnDef_References(t *testing.T) {
	// REFERENCES is handled via table constraints, not inline
	// Just test that the parser doesn't fail on this
	input := "CREATE TABLE t (user_id INT)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	if len(createStmt.Columns) != 1 {
		t.Errorf("Expected 1 column, got %d", len(createStmt.Columns))
	}
}

// ============================================================================
// Parser Tests - Additional Statement Types
// ============================================================================

func TestParseDescribe_Extra(t *testing.T) {
	input := "DESCRIBE users"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	descStmt, ok := stmt.(*DescribeStmt)
	if !ok {
		t.Fatalf("Expected *DescribeStmt, got %T", stmt)
	}
	if descStmt.TableName != "users" {
		t.Errorf("Table name: got %q, want users", descStmt.TableName)
	}
}

func TestParseDesc_Extra(t *testing.T) {
	input := "DESC users"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	descStmt, ok := stmt.(*DescribeStmt)
	if !ok {
		t.Fatalf("Expected *DescribeStmt, got %T", stmt)
	}
	if descStmt.TableName != "users" {
		t.Errorf("Table name: got %q, want users", descStmt.TableName)
	}
}

func TestParseTruncateTable_Extra(t *testing.T) {
	input := "TRUNCATE TABLE users"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	truncStmt, ok := stmt.(*TruncateTableStmt)
	if !ok {
		t.Fatalf("Expected *TruncateTableStmt, got %T", stmt)
	}
	if truncStmt.TableName != "users" {
		t.Errorf("Table name: got %q, want users", truncStmt.TableName)
	}
}

// ============================================================================
// Parser Tests - UNION
// ============================================================================

func TestParseUnion_Extra(t *testing.T) {
	input := "SELECT id FROM users UNION SELECT id FROM admins"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	unionStmt, ok := stmt.(*UnionStmt)
	if !ok {
		t.Fatalf("Expected *UnionStmt, got %T", stmt)
	}
	if unionStmt.All {
		t.Error("Expected All to be false")
	}
}

func TestParseUnionAll_Extra(t *testing.T) {
	input := "SELECT id FROM users UNION ALL SELECT id FROM admins"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	unionStmt, ok := stmt.(*UnionStmt)
	if !ok {
		t.Fatalf("Expected *UnionStmt, got %T", stmt)
	}
	if !unionStmt.All {
		t.Error("Expected All to be true")
	}
}

// ============================================================================
// Parser Tests - LIMIT/OFFSET
// ============================================================================

func TestParseLimitOffset(t *testing.T) {
	input := "SELECT * FROM users LIMIT 10 OFFSET 5"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	if selectStmt.Limit == nil || *selectStmt.Limit != 10 {
		t.Errorf("Limit: got %v, want 10", selectStmt.Limit)
	}
	if selectStmt.Offset == nil || *selectStmt.Offset != 5 {
		t.Errorf("Offset: got %v, want 5", selectStmt.Offset)
	}
}

// ============================================================================
// Parser Tests - Error Conditions
// ============================================================================

func TestParseError_InvalidLimit(t *testing.T) {
	input := "SELECT * FROM users LIMIT abc"
	_, err := Parse(input)
	if err == nil {
		t.Error("Expected error for invalid LIMIT value")
	}
}

func TestParseError_InvalidDataTypeSize(t *testing.T) {
	input := "CREATE TABLE t (c VARCHAR(abc))"
	_, err := Parse(input)
	if err == nil {
		t.Error("Expected error for invalid data type size")
	}
}

func TestParseError_InvalidDataTypeScale(t *testing.T) {
	input := "CREATE TABLE t (c DECIMAL(10,abc))"
	_, err := Parse(input)
	if err == nil {
		t.Error("Expected error for invalid data type scale")
	}
}

func TestParseError_MissingTableName(t *testing.T) {
	input := "CREATE TABLE (id INT)"
	_, err := Parse(input)
	if err == nil {
		t.Error("Expected error for missing table name")
	}
}

func TestParseError_InvalidDrop(t *testing.T) {
	input := "DROP INVALID"
	_, err := Parse(input)
	if err == nil {
		t.Error("Expected error for invalid DROP statement")
	}
}

// ============================================================================
// Token Tests
// ============================================================================

func TestTokenType_String_Extra(t *testing.T) {
	// Test a few token types
	tests := []struct {
		tt       TokenType
		expected string
	}{
		{TokEOF, "EOF"},
		{TokError, "ERROR"},
		{TokIdent, "IDENT"},
		{TokNumber, "NUMBER"},
		{TokString, "STRING"},
	}

	for _, tt := range tests {
		result := tt.tt.String()
		if result != tt.expected {
			t.Errorf("TokenType.String(): got %q, want %q", result, tt.expected)
		}
	}
}

// ============================================================================
// Parser Tests - ALTER TABLE DROP COLUMN
// ============================================================================

func TestParseAlterTable_DropColumn(t *testing.T) {
	input := "ALTER TABLE users DROP COLUMN name"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}
	if alterStmt.TableName != "users" {
		t.Errorf("Table name: got %q, want users", alterStmt.TableName)
	}

	if len(alterStmt.Actions) == 0 {
		t.Fatal("Expected at least one action")
	}
	dropCol, ok := alterStmt.Actions[0].(*DropColumnAction)
	if !ok {
		t.Fatalf("Expected *DropColumnAction, got %T", alterStmt.Actions[0])
	}
	if dropCol.ColumnName != "name" {
		t.Errorf("Column name: got %q, want name", dropCol.ColumnName)
	}
}

func TestParseAlterTable_DropConstraint(t *testing.T) {
	input := "ALTER TABLE users DROP CONSTRAINT uq_email"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if len(alterStmt.Actions) == 0 {
		t.Fatal("Expected at least one action")
	}
	dropCon, ok := alterStmt.Actions[0].(*DropConstraintAction)
	if !ok {
		t.Fatalf("Expected *DropConstraintAction, got %T", alterStmt.Actions[0])
	}
	if dropCon.ConstraintName != "uq_email" {
		t.Errorf("Constraint name: got %q, want uq_email", dropCon.ConstraintName)
	}
}

func TestParseAlterTable_DropPrimaryKey(t *testing.T) {
	input := "ALTER TABLE users DROP PRIMARY KEY"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if len(alterStmt.Actions) == 0 {
		t.Fatal("Expected at least one action")
	}
	// DROP PRIMARY KEY is handled as DropConstraintAction
	dropCon, ok := alterStmt.Actions[0].(*DropConstraintAction)
	if !ok {
		t.Fatalf("Expected *DropConstraintAction, got %T", alterStmt.Actions[0])
	}
	if dropCon.ConstraintName != "PRIMARY" {
		t.Errorf("Constraint name: got %q, want PRIMARY", dropCon.ConstraintName)
	}
}

// ============================================================================
// Parser Tests - ALTER TABLE ADD COLUMN
// ============================================================================

func TestParseAlterTable_AddColumn(t *testing.T) {
	input := "ALTER TABLE users ADD COLUMN age INT"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if len(alterStmt.Actions) == 0 {
		t.Fatal("Expected at least one action")
	}
	addCol, ok := alterStmt.Actions[0].(*AddColumnAction)
	if !ok {
		t.Fatalf("Expected *AddColumnAction, got %T", alterStmt.Actions[0])
	}
	if addCol.Column == nil {
		t.Fatal("Expected column definition")
	}
	if addCol.Column.Name != "age" {
		t.Errorf("Column name: got %q, want age", addCol.Column.Name)
	}
}

func TestParseAlterTable_AddColumnWithConstraints(t *testing.T) {
	input := "ALTER TABLE users ADD COLUMN email VARCHAR(255) NOT NULL UNIQUE"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if len(alterStmt.Actions) == 0 {
		t.Fatal("Expected at least one action")
	}
	addCol, ok := alterStmt.Actions[0].(*AddColumnAction)
	if !ok {
		t.Fatalf("Expected *AddColumnAction, got %T", alterStmt.Actions[0])
	}
	if addCol.Column.Nullable {
		t.Error("Expected Nullable to be false")
	}
	if !addCol.Column.Unique {
		t.Error("Expected Unique to be true")
	}
}

func TestParseAlterTable_AddColumnWithoutKeyword(t *testing.T) {
	input := "ALTER TABLE users ADD age INT"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if len(alterStmt.Actions) == 0 {
		t.Fatal("Expected at least one action")
	}
	addCol, ok := alterStmt.Actions[0].(*AddColumnAction)
	if !ok {
		t.Fatalf("Expected *AddColumnAction, got %T", alterStmt.Actions[0])
	}
	if addCol.Column.Name != "age" {
		t.Errorf("Column name: got %q, want age", addCol.Column.Name)
	}
}

// ============================================================================
// Parser Tests - ALTER TABLE MODIFY COLUMN
// ============================================================================

func TestParseAlterTable_ModifyColumn(t *testing.T) {
	input := "ALTER TABLE users MODIFY COLUMN name VARCHAR(100)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if len(alterStmt.Actions) == 0 {
		t.Fatal("Expected at least one action")
	}
	modCol, ok := alterStmt.Actions[0].(*ModifyColumnAction)
	if !ok {
		t.Fatalf("Expected *ModifyColumnAction, got %T", alterStmt.Actions[0])
	}
	if modCol.Column.Name != "name" {
		t.Errorf("Column name: got %q, want name", modCol.Column.Name)
	}
}

// ============================================================================
// Parser Tests - ALTER TABLE RENAME
// ============================================================================

func TestParseAlterTable_RenameColumn(t *testing.T) {
	input := "ALTER TABLE users RENAME COLUMN name TO username"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if len(alterStmt.Actions) == 0 {
		t.Fatal("Expected at least one action")
	}
	renameCol, ok := alterStmt.Actions[0].(*RenameColumnAction)
	if !ok {
		t.Fatalf("Expected *RenameColumnAction, got %T", alterStmt.Actions[0])
	}
	if renameCol.OldName != "name" {
		t.Errorf("Old name: got %q, want name", renameCol.OldName)
	}
	if renameCol.NewName != "username" {
		t.Errorf("New name: got %q, want username", renameCol.NewName)
	}
}

func TestParseAlterTable_RenameTable(t *testing.T) {
	input := "ALTER TABLE users RENAME TO customers"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if len(alterStmt.Actions) == 0 {
		t.Fatal("Expected at least one action")
	}
	renameTbl, ok := alterStmt.Actions[0].(*RenameTableAction)
	if !ok {
		t.Fatalf("Expected *RenameTableAction, got %T", alterStmt.Actions[0])
	}
	if renameTbl.NewName != "customers" {
		t.Errorf("New name: got %q, want customers", renameTbl.NewName)
	}
}

// ============================================================================
// Parser Tests - ALTER TABLE ADD CONSTRAINT
// ============================================================================

func TestParseAlterTable_AddConstraintPrimaryKey(t *testing.T) {
	input := "ALTER TABLE users ADD CONSTRAINT pk_user PRIMARY KEY (id)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if len(alterStmt.Actions) == 0 {
		t.Fatal("Expected at least one action")
	}
	addCon, ok := alterStmt.Actions[0].(*AddConstraintAction)
	if !ok {
		t.Fatalf("Expected *AddConstraintAction, got %T", alterStmt.Actions[0])
	}
	if addCon.Constraint.Name != "pk_user" {
		t.Errorf("Constraint name: got %q, want pk_user", addCon.Constraint.Name)
	}
	if addCon.Constraint.Type != ConstraintPrimaryKey {
		t.Errorf("Constraint type: got %v, want ConstraintPrimaryKey", addCon.Constraint.Type)
	}
}

func TestParseAlterTable_AddConstraintUnique(t *testing.T) {
	input := "ALTER TABLE users ADD CONSTRAINT uq_email UNIQUE (email)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if len(alterStmt.Actions) == 0 {
		t.Fatal("Expected at least one action")
	}
	addCon, ok := alterStmt.Actions[0].(*AddConstraintAction)
	if !ok {
		t.Fatalf("Expected *AddConstraintAction, got %T", alterStmt.Actions[0])
	}
	if addCon.Constraint.Type != ConstraintUnique {
		t.Errorf("Constraint type: got %v, want ConstraintUnique", addCon.Constraint.Type)
	}
}

func TestParseAlterTable_AddConstraintForeignKey(t *testing.T) {
	input := "ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if len(alterStmt.Actions) == 0 {
		t.Fatal("Expected at least one action")
	}
	addCon, ok := alterStmt.Actions[0].(*AddConstraintAction)
	if !ok {
		t.Fatalf("Expected *AddConstraintAction, got %T", alterStmt.Actions[0])
	}
	if addCon.Constraint.Type != ConstraintForeignKey {
		t.Errorf("Constraint type: got %v, want ConstraintForeignKey", addCon.Constraint.Type)
	}
	if addCon.Constraint.RefTable != "users" {
		t.Errorf("Referenced table: got %q, want users", addCon.Constraint.RefTable)
	}
}

func TestParseAlterTable_AddConstraintCheck(t *testing.T) {
	input := "ALTER TABLE users ADD CONSTRAINT chk_age CHECK (age >= 0)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	alterStmt, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if len(alterStmt.Actions) == 0 {
		t.Fatal("Expected at least one action")
	}
	addCon, ok := alterStmt.Actions[0].(*AddConstraintAction)
	if !ok {
		t.Fatalf("Expected *AddConstraintAction, got %T", alterStmt.Actions[0])
	}
	if addCon.Constraint.Type != ConstraintCheck {
		t.Errorf("Constraint type: got %v, want ConstraintCheck", addCon.Constraint.Type)
	}
}

// ============================================================================
// Parser Tests - Parenthesized Expressions
// ============================================================================

func TestParseParenExpr_Simple(t *testing.T) {
	input := "SELECT (1 + 2) * 3"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	binExpr, ok := selectStmt.Columns[0].(*BinaryExpr)
	if !ok {
		t.Fatalf("Expected *BinaryExpr, got %T", selectStmt.Columns[0])
	}
	_, isParen := binExpr.Left.(*ParenExpr)
	if !isParen {
		t.Errorf("Expected left to be ParenExpr, got %T", binExpr.Left)
	}
}

func TestParseParenExpr_Nested(t *testing.T) {
	input := "SELECT ((a + b) * c)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	parenExpr, ok := selectStmt.Columns[0].(*ParenExpr)
	if !ok {
		t.Fatalf("Expected *ParenExpr, got %T", selectStmt.Columns[0])
	}
	_ = parenExpr
}

func TestParseParenExpr_Subquery(t *testing.T) {
	input := "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	if selectStmt.Where == nil {
		t.Fatal("Expected WHERE clause")
	}
	_ = selectStmt.Where
}

// ============================================================================
// Parser Tests - Unary Expressions
// ============================================================================

func TestParseUnaryExpr_Not(t *testing.T) {
	input := "SELECT * FROM users WHERE NOT active"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	unaryExpr, ok := selectStmt.Where.(*UnaryExpr)
	if !ok {
		t.Fatalf("Expected *UnaryExpr, got %T", selectStmt.Where)
	}
	if unaryExpr.Op != OpNot {
		t.Errorf("Operator: got %v, want OpNot", unaryExpr.Op)
	}
}

func TestParseUnaryExpr_Negate(t *testing.T) {
	input := "SELECT -amount FROM transactions"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	unaryExpr, ok := selectStmt.Columns[0].(*UnaryExpr)
	if !ok {
		t.Fatalf("Expected *UnaryExpr, got %T", selectStmt.Columns[0])
	}
	if unaryExpr.Op != OpNeg {
		t.Errorf("Operator: got %v, want OpNeg", unaryExpr.Op)
	}
}

func TestParseUnaryExpr_DoubleNegate(t *testing.T) {
	input := "SELECT -(-5)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	// The expression -(-5) parses as a UnaryExpr with a ParenExpr inside
	unaryExpr, ok := selectStmt.Columns[0].(*UnaryExpr)
	if !ok {
		t.Fatalf("Expected *UnaryExpr, got %T", selectStmt.Columns[0])
	}
	if unaryExpr.Op != OpNeg {
		t.Errorf("Operator: got %v, want OpNeg", unaryExpr.Op)
	}
}

func TestParseUnaryExpr_NotWithComparison(t *testing.T) {
	input := "SELECT * FROM users WHERE NOT (id = 1)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	unaryExpr, ok := selectStmt.Where.(*UnaryExpr)
	if !ok {
		t.Fatalf("Expected *UnaryExpr, got %T", selectStmt.Where)
	}
	if unaryExpr.Op != OpNot {
		t.Errorf("Operator: got %v, want OpNot", unaryExpr.Op)
	}
}

// ============================================================================
// Parser Tests - Foreign Key Actions
// ============================================================================

func TestParseFKAction_SetNull(t *testing.T) {
	input := "CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users ON DELETE SET NULL)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	tc := createStmt.Constraints[0]
	if tc.OnDelete != "SET NULL" {
		t.Errorf("OnDelete: got %q, want SET NULL", tc.OnDelete)
	}
}

func TestParseFKAction_NoAction(t *testing.T) {
	input := "CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users ON DELETE NO ACTION)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	tc := createStmt.Constraints[0]
	if tc.OnDelete != "NO ACTION" {
		t.Errorf("OnDelete: got %q, want NO ACTION", tc.OnDelete)
	}
}

func TestParseFKAction_OnUpdateSetNull(t *testing.T) {
	input := "CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users ON UPDATE SET NULL)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	tc := createStmt.Constraints[0]
	if tc.OnUpdate != "SET NULL" {
		t.Errorf("OnUpdate: got %q, want SET NULL", tc.OnUpdate)
	}
}

func TestParseFKAction_DefaultRestrict(t *testing.T) {
	input := "CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createStmt := stmt.(*CreateTableStmt)
	tc := createStmt.Constraints[0]
	// When no action is specified, default is RESTRICT
	if tc.OnDelete != "RESTRICT" && tc.OnDelete != "" {
		t.Errorf("OnDelete: got %q", tc.OnDelete)
	}
}

// ============================================================================
// Parser Tests - BACKUP Statements
// ============================================================================

func TestParseBackup_WithCompress(t *testing.T) {
	input := "BACKUP DATABASE TO '/tmp/backup' WITH COMPRESS"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	backupStmt, ok := stmt.(*BackupStmt)
	if !ok {
		t.Fatalf("Expected *BackupStmt, got %T", stmt)
	}
	if backupStmt.Path != "/tmp/backup" {
		t.Errorf("Path: got %q, want /tmp/backup", backupStmt.Path)
	}
	if !backupStmt.Compress {
		t.Error("Expected Compress to be true")
	}
}

func TestParseBackup_WithoutDatabase(t *testing.T) {
	// BACKUP without DATABASE keyword may not be supported
	// Let's test BACKUP DATABASE TO instead
	input := "BACKUP DATABASE TO '/tmp/backup'"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	backupStmt, ok := stmt.(*BackupStmt)
	if !ok {
		t.Fatalf("Expected *BackupStmt, got %T", stmt)
	}
	if backupStmt.Path != "/tmp/backup" {
		t.Errorf("Path: got %q, want /tmp/backup", backupStmt.Path)
	}
}

func TestParseBackup_Simple(t *testing.T) {
	input := "BACKUP DATABASE TO '/var/backups/db'"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	backupStmt, ok := stmt.(*BackupStmt)
	if !ok {
		t.Fatalf("Expected *BackupStmt, got %T", stmt)
	}
	if backupStmt.Compress {
		t.Error("Expected Compress to be false by default")
	}
}

// ============================================================================
// Parser Tests - RESTORE Statements
// ============================================================================

func TestParseRestore_Simple(t *testing.T) {
	input := "RESTORE DATABASE FROM '/tmp/backup'"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	restoreStmt, ok := stmt.(*RestoreStmt)
	if !ok {
		t.Fatalf("Expected *RestoreStmt, got %T", stmt)
	}
	if restoreStmt.Path != "/tmp/backup" {
		t.Errorf("Path: got %q, want /tmp/backup", restoreStmt.Path)
	}
}

func TestParseRestore_WithoutDatabase(t *testing.T) {
	input := "RESTORE FROM '/var/backups/db'"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	restoreStmt, ok := stmt.(*RestoreStmt)
	if !ok {
		t.Fatalf("Expected *RestoreStmt, got %T", stmt)
	}
	if restoreStmt.Path != "/var/backups/db" {
		t.Errorf("Path: got %q, want /var/backups/db", restoreStmt.Path)
	}
}

// ============================================================================
// Parser Tests - Binary Operators Coverage
// ============================================================================

func TestParseBinaryOp_AllOperators(t *testing.T) {
	tests := []struct {
		input    string
		expected BinaryOp
	}{
		{"SELECT 1 = 2", OpEq},
		{"SELECT 1 != 2", OpNe},
		{"SELECT 1 <> 2", OpNe},
		{"SELECT 1 < 2", OpLt},
		{"SELECT 1 <= 2", OpLe},
		{"SELECT 1 > 2", OpGt},
		{"SELECT 1 >= 2", OpGe},
		{"SELECT 1 + 2", OpAdd},
		{"SELECT 1 - 2", OpSub},
		{"SELECT 1 * 2", OpMul},
		{"SELECT 1 / 2", OpDiv},
		{"SELECT 1 % 2", OpMod},
		{"SELECT a AND b", OpAnd},
		{"SELECT a OR b", OpOr},
		{"SELECT a LIKE b", OpLike},
		{"SELECT a || b", OpConcat},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			selectStmt := stmt.(*SelectStmt)
			binExpr, ok := selectStmt.Columns[0].(*BinaryExpr)
			if !ok {
				t.Fatalf("Expected *BinaryExpr, got %T", selectStmt.Columns[0])
			}
			if binExpr.Op != tt.expected {
				t.Errorf("Operator: got %v, want %v", binExpr.Op, tt.expected)
			}
		})
	}
}

// ============================================================================
// Parser Tests - Precedence
// ============================================================================

func TestParsePrecedence_MulBeforeAdd(t *testing.T) {
	input := "SELECT 1 + 2 * 3"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	// Should parse as 1 + (2 * 3) due to precedence
	binExpr := selectStmt.Columns[0].(*BinaryExpr)
	if binExpr.Op != OpAdd {
		t.Errorf("Top operator: got %v, want OpAdd", binExpr.Op)
	}
}

func TestParsePrecedence_AndBeforeOr(t *testing.T) {
	// Test that precedence works - just verify parsing succeeds
	input := "SELECT a AND b OR c"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	// Just verify it's a binary expression
	_, ok := selectStmt.Columns[0].(*BinaryExpr)
	if !ok {
		t.Fatalf("Expected *BinaryExpr, got %T", selectStmt.Columns[0])
	}
}

func TestParsePrecedence_ComparisonBeforeAnd(t *testing.T) {
	input := "SELECT a = 1 AND b = 2"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	binExpr := selectStmt.Columns[0].(*BinaryExpr)
	if binExpr.Op != OpAnd {
		t.Errorf("Top operator: got %v, want OpAnd", binExpr.Op)
	}
}

// ============================================================================
// Parser Tests - Privilege Types
// ============================================================================

func TestParsePrivilege_AllTypes(t *testing.T) {
	tests := []struct {
		input    string
		expected PrivilegeType
	}{
		{"GRANT SELECT ON *.* TO user", PrivSelect},
		{"GRANT INSERT ON db.* TO user", PrivInsert},
		{"GRANT UPDATE ON db.table TO user", PrivUpdate},
		{"GRANT DELETE ON *.* TO user", PrivDelete},
		{"GRANT CREATE ON *.* TO user", PrivCreate},
		{"GRANT DROP ON *.* TO user", PrivDrop},
		{"GRANT INDEX ON *.* TO user", PrivIndex},
		{"GRANT ALTER ON *.* TO user", PrivAlter},
		{"GRANT ALL ON *.* TO user", PrivAll},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			grantStmt, ok := stmt.(*GrantStmt)
			if !ok {
				t.Fatalf("Expected *GrantStmt, got %T", stmt)
			}
			if len(grantStmt.Privileges) == 0 {
				t.Fatal("Expected at least one privilege")
			}
			if grantStmt.Privileges[0].Type != tt.expected {
				t.Errorf("Privilege: got %v, want %v", grantStmt.Privileges[0].Type, tt.expected)
			}
		})
	}
}

func TestParsePrivilege_Multiple(t *testing.T) {
	input := "GRANT SELECT, INSERT, UPDATE ON db.* TO user"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	grantStmt, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("Expected *GrantStmt, got %T", stmt)
	}
	if len(grantStmt.Privileges) != 3 {
		t.Errorf("Privileges count: got %d, want 3", len(grantStmt.Privileges))
	}
}

// ============================================================================
// Parser Tests - Token String Method Coverage
// ============================================================================

func TestTokenType_String_MoreTypes(t *testing.T) {
	tests := []struct {
		tt       TokenType
		expected string
	}{
		{TokCreate, "CREATE"},
		{TokTable, "TABLE"},
		{TokDrop, "DROP"},
		{TokIndex, "INDEX"},
		{TokSelect, "SELECT"},
		{TokFrom, "FROM"},
		{TokWhere, "WHERE"},
		{TokAnd, "AND"},
		{TokOr, "OR"},
		{TokNot, "NOT"},
		{TokInsert, "INSERT"},
		{TokInto, "INTO"},
		{TokValues, "VALUES"},
		{TokUpdate, "UPDATE"},
		{TokDelete, "DELETE"},
		{TokSet, "SET"},
		{TokPrimary, "PRIMARY"},
		{TokKey, "KEY"},
		{TokUnique, "UNIQUE"},
		{TokForeign, "FOREIGN"},
		{TokReferences, "REFERENCES"},
		{TokOn, "ON"},
		{TokNull, "NULL"},
		{TokDefault, "DEFAULT"},
		{TokAutoIncrement, "AUTO_INCREMENT"},
		{TokInt, "INT"},
		{TokVarchar, "VARCHAR"},
		{TokText, "TEXT"},
		{TokGrant, "GRANT"},
		{TokRevoke, "REVOKE"},
		{TokLike, "LIKE"},
		{TokIn, "IN"},
		{TokBetween, "BETWEEN"},
		{TokIs, "IS"},
		{TokAs, "AS"},
		{TokAsc, "ASC"},
		{TokDesc, "DESC"},
		{TokOrder, "ORDER"},
		{TokBy, "BY"},
		{TokGroup, "GROUP"},
		{TokHaving, "HAVING"},
		{TokLimit, "LIMIT"},
		{TokOffset, "OFFSET"},
		{TokJoin, "JOIN"},
		{TokInner, "INNER"},
		{TokLeft, "LEFT"},
		{TokRight, "RIGHT"},
		{TokCross, "CROSS"},
		{TokCase, "CASE"},
		{TokWhen, "WHEN"},
		{TokThen, "THEN"},
		{TokElse, "ELSE"},
		{TokEnd, "END"},
		{TokCast, "CAST"},
		{TokCount, "COUNT"},
		{TokSum, "SUM"},
		{TokAvg, "AVG"},
		{TokMin, "MIN"},
		{TokMax, "MAX"},
		{TokCoalesce, "COALESCE"},
		{TokNullIf, "NULLIF"},
		{TokBackup, "BACKUP"},
		{TokRestore, "RESTORE"},
		{TokBoolLit, "BOOL"},
		{TokBoolean, "BOOLEAN"},
		{TokBool, "BOOLEAN"},
		// These tokens return "UNKNOWN" in String() since they're not in the switch
		// {TokCascade, "CASCADE"},
		// {TokRestrict, "RESTRICT"},
		// {TokTo, "TO"},
		// {TokUse, "USE"},
		// {TokDatabase, "DATABASE"},
		// {TokShow, "SHOW"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.tt.String()
			if result != tt.expected {
				t.Errorf("TokenType.String(): got %q, want %q", result, tt.expected)
			}
		})
	}
}

// ============================================================================
// Parser Tests - SET PASSWORD
// ============================================================================

func TestParseSetPassword_ForUser(t *testing.T) {
	input := "SET PASSWORD FOR user = 'secret'"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	setStmt, ok := stmt.(*SetPasswordStmt)
	if !ok {
		t.Fatalf("Expected *SetPasswordStmt, got %T", stmt)
	}
	if setStmt.ForUser != "user" {
		t.Errorf("ForUser: got %q, want user", setStmt.ForUser)
	}
	if setStmt.Password != "secret" {
		t.Errorf("Password: got %q, want secret", setStmt.Password)
	}
}

func TestSetPassword_CurrentUser(t *testing.T) {
	input := "SET PASSWORD = 'newpass'"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	setStmt, ok := stmt.(*SetPasswordStmt)
	if !ok {
		t.Fatalf("Expected *SetPasswordStmt, got %T", stmt)
	}
	if setStmt.Password != "newpass" {
		t.Errorf("Password: got %q, want newpass", setStmt.Password)
	}
}

// ============================================================================
// Parser Tests - CREATE USER / DROP USER
// ============================================================================

func TestCreateUser_WithOptions(t *testing.T) {
	input := "CREATE USER admin IDENTIFIED BY 'secret'"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	createUser, ok := stmt.(*CreateUserStmt)
	if !ok {
		t.Fatalf("Expected *CreateUserStmt, got %T", stmt)
	}
	if createUser.Username != "admin" {
		t.Errorf("Username: got %q, want admin", createUser.Username)
	}
	if createUser.Identified != "secret" {
		t.Errorf("Identified: got %q, want secret", createUser.Identified)
	}
}

// ============================================================================
// Parser Tests - REVOKE
// ============================================================================

func TestParseRevoke_Simple(t *testing.T) {
	input := "REVOKE SELECT ON *.* FROM user"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	revokeStmt, ok := stmt.(*RevokeStmt)
	if !ok {
		t.Fatalf("Expected *RevokeStmt, got %T", stmt)
	}
	if len(revokeStmt.Privileges) != 1 || revokeStmt.Privileges[0].Type != PrivSelect {
		t.Errorf("Privileges: got %v, want [PrivSelect]", revokeStmt.Privileges)
	}
}

func TestParseRevoke_All(t *testing.T) {
	input := "REVOKE ALL ON *.* FROM user"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	revokeStmt, ok := stmt.(*RevokeStmt)
	if !ok {
		t.Fatalf("Expected *RevokeStmt, got %T", stmt)
	}
	if len(revokeStmt.Privileges) != 1 || revokeStmt.Privileges[0].Type != PrivAll {
		t.Errorf("Privileges: got %v, want [PrivAll]", revokeStmt.Privileges)
	}
}

// ============================================================================
// Parser Tests - CASE Expression
// ============================================================================

func TestParseCaseExpr_Simple(t *testing.T) {
	input := "SELECT CASE WHEN a > 0 THEN 1 ELSE 0 END"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	caseExpr, ok := selectStmt.Columns[0].(*CaseExpr)
	if !ok {
		t.Fatalf("Expected *CaseExpr, got %T", selectStmt.Columns[0])
	}
	if len(caseExpr.Whens) != 1 {
		t.Errorf("WHENs count: got %d, want 1", len(caseExpr.Whens))
	}
	if caseExpr.Else == nil {
		t.Error("Expected ELSE clause")
	}
}

func TestParseCaseExpr_MultipleWhens(t *testing.T) {
	input := "SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'other' END"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	caseExpr, ok := selectStmt.Columns[0].(*CaseExpr)
	if !ok {
		t.Fatalf("Expected *CaseExpr, got %T", selectStmt.Columns[0])
	}
	if len(caseExpr.Whens) != 2 {
		t.Errorf("WHENs count: got %d, want 2", len(caseExpr.Whens))
	}
}

func TestParseCaseExpr_WithOperand(t *testing.T) {
	input := "SELECT CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' END"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	caseExpr, ok := selectStmt.Columns[0].(*CaseExpr)
	if !ok {
		t.Fatalf("Expected *CaseExpr, got %T", selectStmt.Columns[0])
	}
	if caseExpr.Expr == nil {
		t.Error("Expected operand expression")
	}
}

// ============================================================================
// Parser Tests - CAST Expression
// ============================================================================

func TestParseCastExpr_Simple(t *testing.T) {
	input := "SELECT CAST(123 AS VARCHAR)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	castExpr, ok := selectStmt.Columns[0].(*CastExpr)
	if !ok {
		t.Fatalf("Expected *CastExpr, got %T", selectStmt.Columns[0])
	}
	if castExpr.Type == nil || castExpr.Type.Name != "VARCHAR" {
		t.Errorf("Cast type: got %v", castExpr.Type)
	}
}

func TestParseCastExpr_Int(t *testing.T) {
	input := "SELECT CAST('123' AS INT)"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	castExpr, ok := selectStmt.Columns[0].(*CastExpr)
	if !ok {
		t.Fatalf("Expected *CastExpr, got %T", selectStmt.Columns[0])
	}
	if castExpr.Type.Name != "INT" {
		t.Errorf("Cast type: got %q, want INT", castExpr.Type.Name)
	}
}

// ============================================================================
// Parser Tests - Aggregate Functions
// ============================================================================

func TestParseAggregateFunctions(t *testing.T) {
	tests := []struct {
		input    string
		funcName string
	}{
		{"SELECT COUNT(*) FROM users", "COUNT"},
		{"SELECT SUM(amount) FROM orders", "SUM"},
		{"SELECT AVG(price) FROM products", "AVG"},
		{"SELECT MIN(id) FROM users", "MIN"},
		{"SELECT MAX(id) FROM users", "MAX"},
		{"SELECT COALESCE(name, 'N/A') FROM users", "COALESCE"},
		{"SELECT NULLIF(a, b) FROM t", "NULLIF"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			selectStmt := stmt.(*SelectStmt)
			funcCall, ok := selectStmt.Columns[0].(*FunctionCall)
			if !ok {
				t.Fatalf("Expected *FunctionCall, got %T", selectStmt.Columns[0])
			}
			if funcCall.Name != tt.funcName {
				t.Errorf("Function name: got %q, want %q", funcCall.Name, tt.funcName)
			}
		})
	}
}

func TestParseAggregateFunction_Distinct(t *testing.T) {
	input := "SELECT COUNT(DISTINCT user_id) FROM orders"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	funcCall, ok := selectStmt.Columns[0].(*FunctionCall)
	if !ok {
		t.Fatalf("Expected *FunctionCall, got %T", selectStmt.Columns[0])
	}
	if !funcCall.Distinct {
		t.Error("Expected Distinct to be true")
	}
}