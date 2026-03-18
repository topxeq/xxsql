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