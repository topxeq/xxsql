package sql

import (
	"fmt"
	"strings"
)

// Node represents an AST node.
type Node interface {
	node()
	String() string
}

// Statement represents a SQL statement.
type Statement interface {
	Node
	statement()
}

// Expression represents an expression.
type Expression interface {
	Node
	expression()
}

// ============================================================================
// Statements
// ============================================================================

// SelectStmt represents a SELECT statement.
type SelectStmt struct {
	Distinct   bool
	Columns    []Expression // SELECT columns
	From       *FromClause  // FROM clause
	Where      Expression   // WHERE condition
	GroupBy    []Expression // GROUP BY columns
	Having     Expression   // HAVING condition
	OrderBy    []*OrderByItem
	Limit      *int
	Offset     *int
}

func (s *SelectStmt) node()      {}
func (s *SelectStmt) statement() {}
func (s *SelectStmt) String() string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	if s.Distinct {
		sb.WriteString("DISTINCT ")
	}
	for i, col := range s.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col.String())
	}
	if s.From != nil {
		sb.WriteString(" ")
		sb.WriteString(s.From.String())
	}
	if s.Where != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(s.Where.String())
	}
	if len(s.GroupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		for i, col := range s.GroupBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col.String())
		}
	}
	if s.Having != nil {
		sb.WriteString(" HAVING ")
		sb.WriteString(s.Having.String())
	}
	if len(s.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, item := range s.OrderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(item.String())
		}
	}
	if s.Limit != nil {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", *s.Limit))
	}
	if s.Offset != nil {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", *s.Offset))
	}
	return sb.String()
}

// InsertStmt represents an INSERT statement.
type InsertStmt struct {
	Table      string
	Columns    []string
	Values     [][]Expression
	OnDuplicateKeyUpdate []*Assignment // MySQL-style ON DUPLICATE KEY UPDATE
}

func (s *InsertStmt) node()      {}
func (s *InsertStmt) statement() {}
func (s *InsertStmt) String() string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(s.Table)
	if len(s.Columns) > 0 {
		sb.WriteString(" (")
		for i, col := range s.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col)
		}
		sb.WriteString(")")
	}
	sb.WriteString(" VALUES ")
	for i, row := range s.Values {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		for j, val := range row {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(val.String())
		}
		sb.WriteString(")")
	}
	return sb.String()
}

// UpdateStmt represents an UPDATE statement.
type UpdateStmt struct {
	Table       string
	Assignments []*Assignment
	Where       Expression
	OrderBy     []*OrderByItem
	Limit       *int
}

func (s *UpdateStmt) node()      {}
func (s *UpdateStmt) statement() {}
func (s *UpdateStmt) String() string {
	var sb strings.Builder
	sb.WriteString("UPDATE ")
	sb.WriteString(s.Table)
	sb.WriteString(" SET ")
	for i, a := range s.Assignments {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(a.String())
	}
	if s.Where != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(s.Where.String())
	}
	return sb.String()
}

// DeleteStmt represents a DELETE statement.
type DeleteStmt struct {
	Table   string
	Where   Expression
	OrderBy []*OrderByItem
	Limit   *int
}

func (s *DeleteStmt) node()      {}
func (s *DeleteStmt) statement() {}
func (s *DeleteStmt) String() string {
	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(s.Table)
	if s.Where != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(s.Where.String())
	}
	return sb.String()
}

// CreateTableStmt represents a CREATE TABLE statement.
type CreateTableStmt struct {
	IfNotExists bool
	TableName   string
	Columns     []*ColumnDef
	Constraints []*TableConstraint
	Options     map[string]string
}

func (s *CreateTableStmt) node()      {}
func (s *CreateTableStmt) statement() {}
func (s *CreateTableStmt) String() string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	if s.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	sb.WriteString(s.TableName)
	sb.WriteString(" (")
	for i, col := range s.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col.String())
	}
	for _, c := range s.Constraints {
		sb.WriteString(", ")
		sb.WriteString(c.String())
	}
	sb.WriteString(")")
	return sb.String()
}

// DropTableStmt represents a DROP TABLE statement.
type DropTableStmt struct {
	IfExists  bool
	TableName string
}

func (s *DropTableStmt) node()      {}
func (s *DropTableStmt) statement() {}
func (s *DropTableStmt) String() string {
	var sb strings.Builder
	sb.WriteString("DROP TABLE ")
	if s.IfExists {
		sb.WriteString("IF EXISTS ")
	}
	sb.WriteString(s.TableName)
	return sb.String()
}

// CreateIndexStmt represents a CREATE INDEX statement.
type CreateIndexStmt struct {
	Unique      bool
	IndexName   string
	TableName   string
	Columns     []string
	IfNotExists bool
}

func (s *CreateIndexStmt) node()      {}
func (s *CreateIndexStmt) statement() {}
func (s *CreateIndexStmt) String() string {
	var sb strings.Builder
	sb.WriteString("CREATE ")
	if s.Unique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX ")
	if s.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	sb.WriteString(s.IndexName)
	sb.WriteString(" ON ")
	sb.WriteString(s.TableName)
	sb.WriteString(" (")
	for i, col := range s.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col)
	}
	sb.WriteString(")")
	return sb.String()
}

// DropIndexStmt represents a DROP INDEX statement.
type DropIndexStmt struct {
	IndexName string
	TableName string
	IfExists  bool
}

func (s *DropIndexStmt) node()      {}
func (s *DropIndexStmt) statement() {}
func (s *DropIndexStmt) String() string {
	return fmt.Sprintf("DROP INDEX %s ON %s", s.IndexName, s.TableName)
}

// AlterTableStmt represents an ALTER TABLE statement.
type AlterTableStmt struct {
	TableName string
	Actions   []AlterAction
}

func (s *AlterTableStmt) node()      {}
func (s *AlterTableStmt) statement() {}
func (s *AlterTableStmt) String() string {
	var sb strings.Builder
	sb.WriteString("ALTER TABLE ")
	sb.WriteString(s.TableName)
	sb.WriteString(" ")
	for i, action := range s.Actions {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(action.String())
	}
	return sb.String()
}

// AlterAction represents an action in ALTER TABLE.
type AlterAction interface {
	Node
	alterAction()
}

// AddColumnAction represents ADD COLUMN action.
type AddColumnAction struct {
	Column *ColumnDef
}

func (a *AddColumnAction) node()       {}
func (a *AddColumnAction) alterAction() {}
func (a *AddColumnAction) String() string {
	return fmt.Sprintf("ADD COLUMN %s", a.Column.String())
}

// DropColumnAction represents DROP COLUMN action.
type DropColumnAction struct {
	ColumnName string
}

func (a *DropColumnAction) node()       {}
func (a *DropColumnAction) alterAction() {}
func (a *DropColumnAction) String() string {
	return fmt.Sprintf("DROP COLUMN %s", a.ColumnName)
}

// ModifyColumnAction represents MODIFY COLUMN action.
type ModifyColumnAction struct {
	Column *ColumnDef
}

func (a *ModifyColumnAction) node()       {}
func (a *ModifyColumnAction) alterAction() {}
func (a *ModifyColumnAction) String() string {
	return fmt.Sprintf("MODIFY COLUMN %s", a.Column.String())
}

// RenameColumnAction represents RENAME COLUMN action.
type RenameColumnAction struct {
	OldName string
	NewName string
}

func (a *RenameColumnAction) node()       {}
func (a *RenameColumnAction) alterAction() {}
func (a *RenameColumnAction) String() string {
	return fmt.Sprintf("RENAME COLUMN %s TO %s", a.OldName, a.NewName)
}

// RenameTableAction represents RENAME TO action.
type RenameTableAction struct {
	NewName string
}

func (a *RenameTableAction) node()       {}
func (a *RenameTableAction) alterAction() {}
func (a *RenameTableAction) String() string {
	return fmt.Sprintf("RENAME TO %s", a.NewName)
}

// AddConstraintAction represents ADD CONSTRAINT action.
type AddConstraintAction struct {
	Constraint *TableConstraint
}

func (a *AddConstraintAction) node()       {}
func (a *AddConstraintAction) alterAction() {}
func (a *AddConstraintAction) String() string {
	return fmt.Sprintf("ADD %s", a.Constraint.String())
}

// DropConstraintAction represents DROP CONSTRAINT action.
type DropConstraintAction struct {
	ConstraintName string
}

func (a *DropConstraintAction) node()       {}
func (a *DropConstraintAction) alterAction() {}
func (a *DropConstraintAction) String() string {
	return fmt.Sprintf("DROP CONSTRAINT %s", a.ConstraintName)
}

// TruncateTableStmt represents a TRUNCATE TABLE statement.
type TruncateTableStmt struct {
	TableName string
}

func (s *TruncateTableStmt) node()      {}
func (s *TruncateTableStmt) statement() {}
func (s *TruncateTableStmt) String() string {
	return fmt.Sprintf("TRUNCATE TABLE %s", s.TableName)
}

// UseStmt represents a USE DATABASE statement.
type UseStmt struct {
	Database string
}

func (s *UseStmt) node()      {}
func (s *UseStmt) statement() {}
func (s *UseStmt) String() string {
	return fmt.Sprintf("USE %s", s.Database)
}

// ShowStmt represents a SHOW statement.
type ShowStmt struct {
	Type    string // TABLES, DATABASES, COLUMNS, etc.
	Like    string
	From    string // table name for SHOW COLUMNS
}

func (s *ShowStmt) node()      {}
func (s *ShowStmt) statement() {}
func (s *ShowStmt) String() string {
	var sb strings.Builder
	sb.WriteString("SHOW ")
	sb.WriteString(s.Type)
	if s.From != "" {
		sb.WriteString(" FROM ")
		sb.WriteString(s.From)
	}
	if s.Like != "" {
		sb.WriteString(" LIKE ")
		sb.WriteString(fmt.Sprintf("'%s'", s.Like))
	}
	return sb.String()
}

// DescribeStmt represents a DESCRIBE/DESC table statement.
type DescribeStmt struct {
	TableName string
}

func (s *DescribeStmt) node()      {}
func (s *DescribeStmt) statement() {}
func (s *DescribeStmt) String() string {
	return fmt.Sprintf("DESCRIBE %s", s.TableName)
}

// ShowCreateTableStmt represents a SHOW CREATE TABLE statement.
type ShowCreateTableStmt struct {
	TableName string
}

func (s *ShowCreateTableStmt) node()      {}
func (s *ShowCreateTableStmt) statement() {}
func (s *ShowCreateTableStmt) String() string {
	return fmt.Sprintf("SHOW CREATE TABLE %s", s.TableName)
}

// UnionStmt represents a UNION statement.
type UnionStmt struct {
	Left     Statement
	Right    Statement
	All      bool
}

func (s *UnionStmt) node()      {}
func (s *UnionStmt) statement() {}
func (s *UnionStmt) String() string {
	var sb strings.Builder
	sb.WriteString(s.Left.String())
	sb.WriteString(" UNION ")
	if s.All {
		sb.WriteString("ALL ")
	}
	sb.WriteString(s.Right.String())
	return sb.String()
}

// ============================================================================
// Clauses
// ============================================================================

// FromClause represents a FROM clause.
type FromClause struct {
	Table *TableRef
	Joins []*JoinClause
}

func (c *FromClause) node() {}
func (c *FromClause) String() string {
	var sb strings.Builder
	sb.WriteString("FROM ")
	sb.WriteString(c.Table.String())
	for _, join := range c.Joins {
		sb.WriteString(" ")
		sb.WriteString(join.String())
	}
	return sb.String()
}

// JoinClause represents a JOIN clause.
type JoinClause struct {
	Type   JoinType
	Table  *TableRef
	On     Expression
	Using  []string
}

func (c *JoinClause) node() {}
func (c *JoinClause) String() string {
	var sb strings.Builder
	switch c.Type {
	case JoinInner:
		sb.WriteString("INNER JOIN ")
	case JoinLeft:
		sb.WriteString("LEFT JOIN ")
	case JoinRight:
		sb.WriteString("RIGHT JOIN ")
	case JoinCross:
		sb.WriteString("CROSS JOIN ")
	}
	sb.WriteString(c.Table.String())
	if c.On != nil {
		sb.WriteString(" ON ")
		sb.WriteString(c.On.String())
	}
	if len(c.Using) > 0 {
		sb.WriteString(" USING (")
		for i, col := range c.Using {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col)
		}
		sb.WriteString(")")
	}
	return sb.String()
}

// JoinType represents the type of JOIN.
type JoinType int

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
	JoinCross
	JoinFull
)

// OrderByItem represents an ORDER BY item.
type OrderByItem struct {
	Expr      Expression
	Ascending bool
}

func (o *OrderByItem) node() {}
func (o *OrderByItem) String() string {
	s := o.Expr.String()
	if o.Ascending {
		return s + " ASC"
	}
	return s + " DESC"
}

// Assignment represents a SET assignment.
type Assignment struct {
	Column string
	Value  Expression
}

func (a *Assignment) node() {}
func (a *Assignment) String() string {
	return fmt.Sprintf("%s = %s", a.Column, a.Value.String())
}

// ============================================================================
// Column Definitions
// ============================================================================

// ColumnDef represents a column definition.
type ColumnDef struct {
	Name       string
	Type       *DataType
	Nullable   bool  // true if NULL allowed (default true)
	Default    Expression
	AutoIncr   bool
	PrimaryKey bool
	Unique     bool
	Comment    string
}

func (c *ColumnDef) node() {}
func (c *ColumnDef) String() string {
	var sb strings.Builder
	sb.WriteString(c.Name)
	sb.WriteString(" ")
	sb.WriteString(c.Type.String())
	if !c.Nullable {
		sb.WriteString(" NOT NULL")
	}
	if c.Default != nil {
		sb.WriteString(" DEFAULT ")
		sb.WriteString(c.Default.String())
	}
	if c.AutoIncr {
		sb.WriteString(" AUTO_INCREMENT")
	}
	if c.PrimaryKey {
		sb.WriteString(" PRIMARY KEY")
	}
	if c.Unique {
		sb.WriteString(" UNIQUE")
	}
	return sb.String()
}

// DataType represents a data type.
type DataType struct {
	Name     string
	Size     int    // for CHAR, VARCHAR
	Precision int   // for DECIMAL, NUMERIC
	Scale    int    // for DECIMAL, NUMERIC
	Unsigned bool
}

func (d *DataType) node() {}
func (d *DataType) String() string {
	var sb strings.Builder
	sb.WriteString(d.Name)
	if d.Size > 0 {
		sb.WriteString(fmt.Sprintf("(%d)", d.Size))
	} else if d.Precision > 0 {
		if d.Scale > 0 {
			sb.WriteString(fmt.Sprintf("(%d,%d)", d.Precision, d.Scale))
		} else {
			sb.WriteString(fmt.Sprintf("(%d)", d.Precision))
		}
	}
	if d.Unsigned {
		sb.WriteString(" UNSIGNED")
	}
	return sb.String()
}

// TableConstraint represents a table-level constraint.
type TableConstraint struct {
	Name        string
	Type        ConstraintType
	Columns     []string
	RefTable    string
	RefColumns  []string
	CheckExpr   Expression // For CHECK constraint
	OnDelete    string     // For FK: CASCADE, SET NULL, RESTRICT, NO ACTION
	OnUpdate    string     // For FK: CASCADE, SET NULL, RESTRICT, NO ACTION
}

func (c *TableConstraint) node() {}
func (c *TableConstraint) String() string {
	var sb strings.Builder
	if c.Name != "" {
		sb.WriteString("CONSTRAINT ")
		sb.WriteString(c.Name)
		sb.WriteString(" ")
	}
	switch c.Type {
	case ConstraintPrimaryKey:
		sb.WriteString("PRIMARY KEY (")
	case ConstraintUnique:
		sb.WriteString("UNIQUE (")
	case ConstraintForeignKey:
		sb.WriteString("FOREIGN KEY (")
	case ConstraintCheck:
		sb.WriteString("CHECK (")
		if c.CheckExpr != nil {
			sb.WriteString(c.CheckExpr.String())
		}
		sb.WriteString(")")
		return sb.String()
	}
	for i, col := range c.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col)
	}
	sb.WriteString(")")
	if c.Type == ConstraintForeignKey && c.RefTable != "" {
		sb.WriteString(" REFERENCES ")
		sb.WriteString(c.RefTable)
		if len(c.RefColumns) > 0 {
			sb.WriteString(" (")
			for i, col := range c.RefColumns {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(col)
			}
			sb.WriteString(")")
		}
		if c.OnDelete != "" {
			sb.WriteString(" ON DELETE ")
			sb.WriteString(c.OnDelete)
		}
		if c.OnUpdate != "" {
			sb.WriteString(" ON UPDATE ")
			sb.WriteString(c.OnUpdate)
		}
	}
	return sb.String()
}

// ConstraintType represents the type of constraint.
type ConstraintType int

const (
	ConstraintPrimaryKey ConstraintType = iota
	ConstraintUnique
	ConstraintForeignKey
	ConstraintCheck
)

// ============================================================================
// Expressions
// ============================================================================

// Literal represents a literal value.
type Literal struct {
	Value interface{}
	Type  LiteralType
}

type LiteralType int

const (
	LiteralNull LiteralType = iota
	LiteralString
	LiteralNumber
	LiteralBool
)

func (l *Literal) node()       {}
func (l *Literal) expression() {}
func (l *Literal) String() string {
	switch l.Type {
	case LiteralNull:
		return "NULL"
	case LiteralString:
		return fmt.Sprintf("'%s'", l.Value)
	case LiteralNumber:
		return fmt.Sprintf("%v", l.Value)
	case LiteralBool:
		return fmt.Sprintf("%v", l.Value)
	}
	return fmt.Sprintf("%v", l.Value)
}

// ColumnRef represents a column reference.
type ColumnRef struct {
	Table  string // optional table qualifier
	Name   string
	Alias  string // optional alias
}

func (c *ColumnRef) node()       {}
func (c *ColumnRef) expression() {}
func (c *ColumnRef) String() string {
	if c.Table != "" {
		return fmt.Sprintf("%s.%s", c.Table, c.Name)
	}
	return c.Name
}

// StarExpr represents a * or table.* expression.
type StarExpr struct {
	Table string // optional table qualifier
}

func (s *StarExpr) node()       {}
func (s *StarExpr) expression() {}
func (s *StarExpr) String() string {
	if s.Table != "" {
		return fmt.Sprintf("%s.*", s.Table)
	}
	return "*"
}

// BinaryExpr represents a binary expression.
type BinaryExpr struct {
	Left  Expression
	Op    BinaryOp
	Right Expression
}

type BinaryOp int

const (
	OpEq BinaryOp = iota
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpAdd
	OpSub
	OpMul
	OpDiv
	OpMod
	OpAnd
	OpOr
	OpLike
	OpNotLike
	OpIn
	OpNotIn
	OpConcat
)

func (e *BinaryExpr) node()       {}
func (e *BinaryExpr) expression() {}
func (e *BinaryExpr) String() string {
	var op string
	switch e.Op {
	case OpEq:
		op = "="
	case OpNe:
		op = "<>"
	case OpLt:
		op = "<"
	case OpLe:
		op = "<="
	case OpGt:
		op = ">"
	case OpGe:
		op = ">="
	case OpAdd:
		op = "+"
	case OpSub:
		op = "-"
	case OpMul:
		op = "*"
	case OpDiv:
		op = "/"
	case OpMod:
		op = "%"
	case OpAnd:
		op = "AND"
	case OpOr:
		op = "OR"
	case OpLike:
		op = "LIKE"
	case OpNotLike:
		op = "NOT LIKE"
	case OpConcat:
		op = "||"
	}
	return fmt.Sprintf("(%s %s %s)", e.Left.String(), op, e.Right.String())
}

// UnaryExpr represents a unary expression.
type UnaryExpr struct {
	Op    UnaryOp
	Right Expression
}

type UnaryOp int

const (
	OpNot UnaryOp = iota
	OpNeg
)

func (e *UnaryExpr) node()       {}
func (e *UnaryExpr) expression() {}
func (e *UnaryExpr) String() string {
	if e.Op == OpNot {
		return fmt.Sprintf("NOT %s", e.Right.String())
	}
	return fmt.Sprintf("-%s", e.Right.String())
}

// FunctionCall represents a function call.
type FunctionCall struct {
	Name      string
	Args      []Expression
	Distinct  bool
}

func (f *FunctionCall) node()       {}
func (f *FunctionCall) expression() {}
func (f *FunctionCall) String() string {
	var sb strings.Builder
	sb.WriteString(f.Name)
	sb.WriteString("(")
	if f.Distinct {
		sb.WriteString("DISTINCT ")
	}
	for i, arg := range f.Args {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(arg.String())
	}
	sb.WriteString(")")
	return sb.String()
}

// BetweenExpr represents a BETWEEN expression.
type BetweenExpr struct {
	Expr   Expression
	Left   Expression
	Right  Expression
	Not    bool
}

func (e *BetweenExpr) node()       {}
func (e *BetweenExpr) expression() {}
func (e *BetweenExpr) String() string {
	if e.Not {
		return fmt.Sprintf("%s NOT BETWEEN %s AND %s", e.Expr.String(), e.Left.String(), e.Right.String())
	}
	return fmt.Sprintf("%s BETWEEN %s AND %s", e.Expr.String(), e.Left.String(), e.Right.String())
}

// InExpr represents an IN expression.
type InExpr struct {
	Expr   Expression
	List   []Expression
	Select Statement
	Not    bool
}

func (e *InExpr) node()       {}
func (e *InExpr) expression() {}
func (e *InExpr) String() string {
	var sb strings.Builder
	sb.WriteString(e.Expr.String())
	if e.Not {
		sb.WriteString(" NOT")
	}
	sb.WriteString(" IN (")
	if e.Select != nil {
		sb.WriteString(e.Select.String())
	} else {
		for i, v := range e.List {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(v.String())
		}
	}
	sb.WriteString(")")
	return sb.String()
}

// IsNullExpr represents an IS NULL expression.
type IsNullExpr struct {
	Expr Expression
	Not  bool
}

func (e *IsNullExpr) node()       {}
func (e *IsNullExpr) expression() {}
func (e *IsNullExpr) String() string {
	if e.Not {
		return fmt.Sprintf("%s IS NOT NULL", e.Expr.String())
	}
	return fmt.Sprintf("%s IS NULL", e.Expr.String())
}

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Expr   Expression      // optional operand
	Whens  []*CaseWhen
	Else   Expression
}

func (e *CaseExpr) node()       {}
func (e *CaseExpr) expression() {}
func (e *CaseExpr) String() string {
	var sb strings.Builder
	sb.WriteString("CASE")
	if e.Expr != nil {
		sb.WriteString(" ")
		sb.WriteString(e.Expr.String())
	}
	for _, w := range e.Whens {
		sb.WriteString(" ")
		sb.WriteString(w.String())
	}
	if e.Else != nil {
		sb.WriteString(" ELSE ")
		sb.WriteString(e.Else.String())
	}
	sb.WriteString(" END")
	return sb.String()
}

// CaseWhen represents a WHEN clause in a CASE expression.
type CaseWhen struct {
	Condition Expression
	Result    Expression
}

func (w *CaseWhen) String() string {
	return fmt.Sprintf("WHEN %s THEN %s", w.Condition.String(), w.Result.String())
}

// TableRef represents a table reference.
type TableRef struct {
	Name  string
	Alias string
}

func (t *TableRef) node() {}
func (t *TableRef) String() string {
	if t.Alias != "" {
		return fmt.Sprintf("%s AS %s", t.Name, t.Alias)
	}
	return t.Name
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expression
}

func (e *ParenExpr) node()       {}
func (e *ParenExpr) expression() {}
func (e *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", e.Expr.String())
}

// SubqueryExpr represents a subquery expression (e.g., in IN clause).
type SubqueryExpr struct {
	Select *SelectStmt
}

func (e *SubqueryExpr) node()       {}
func (e *SubqueryExpr) expression() {}
func (e *SubqueryExpr) String() string {
	return fmt.Sprintf("(%s)", e.Select.String())
}

// CastExpr represents a CAST expression.
type CastExpr struct {
	Expr Expression
	Type *DataType
}

func (e *CastExpr) node()       {}
func (e *CastExpr) expression() {}
func (e *CastExpr) String() string {
	return fmt.Sprintf("CAST(%s AS %s)", e.Expr.String(), e.Type.String())
}

// ============================================================================
// Authentication and Authorization Statements
// ============================================================================

// PrivilegeType represents a SQL privilege type.
type PrivilegeType int

const (
	PrivAll PrivilegeType = iota
	PrivSelect
	PrivInsert
	PrivUpdate
	PrivDelete
	PrivCreate
	PrivDrop
	PrivIndex
	PrivAlter
	PrivUsage
)

// String returns the string representation of the privilege.
func (p PrivilegeType) String() string {
	switch p {
	case PrivAll:
		return "ALL"
	case PrivSelect:
		return "SELECT"
	case PrivInsert:
		return "INSERT"
	case PrivUpdate:
		return "UPDATE"
	case PrivDelete:
		return "DELETE"
	case PrivCreate:
		return "CREATE"
	case PrivDrop:
		return "DROP"
	case PrivIndex:
		return "INDEX"
	case PrivAlter:
		return "ALTER"
	case PrivUsage:
		return "USAGE"
	default:
		return "UNKNOWN"
	}
}

// Privilege represents a privilege with optional columns.
type Privilege struct {
	Type    PrivilegeType
	Columns []string // For column-level privileges
}

func (p *Privilege) String() string {
	var sb strings.Builder
	sb.WriteString(p.Type.String())
	if len(p.Columns) > 0 {
		sb.WriteString("(")
		for i, col := range p.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col)
		}
		sb.WriteString(")")
	}
	return sb.String()
}

// GrantOn represents the object type for GRANT/REVOKE.
type GrantOn int

const (
	GrantOnAll GrantOn = iota
	GrantOnDatabase
	GrantOnTable
)

// CreateUserStmt represents a CREATE USER statement.
type CreateUserStmt struct {
	IfNotExists bool
	Username    string
	Host        string // default: '%'
	Identified  string // password or auth string
	Role        string // optional: admin, user
}

func (s *CreateUserStmt) node()      {}
func (s *CreateUserStmt) statement() {}
func (s *CreateUserStmt) String() string {
	var sb strings.Builder
	sb.WriteString("CREATE USER ")
	if s.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	sb.WriteString(s.Username)
	if s.Host != "" {
		sb.WriteString("@")
		sb.WriteString(s.Host)
	}
	if s.Identified != "" {
		sb.WriteString(" IDENTIFIED BY '")
		sb.WriteString(s.Identified)
		sb.WriteString("'")
	}
	return sb.String()
}

// DropUserStmt represents a DROP USER statement.
type DropUserStmt struct {
	IfExists bool
	Username string
	Host     string
}

func (s *DropUserStmt) node()      {}
func (s *DropUserStmt) statement() {}
func (s *DropUserStmt) String() string {
	var sb strings.Builder
	sb.WriteString("DROP USER ")
	if s.IfExists {
		sb.WriteString("IF EXISTS ")
	}
	sb.WriteString(s.Username)
	if s.Host != "" {
		sb.WriteString("@")
		sb.WriteString(s.Host)
	}
	return sb.String()
}

// AlterUserStmt represents an ALTER USER statement.
type AlterUserStmt struct {
	Username   string
	Host       string
	Identified string // new password
}

func (s *AlterUserStmt) node()      {}
func (s *AlterUserStmt) statement() {}
func (s *AlterUserStmt) String() string {
	var sb strings.Builder
	sb.WriteString("ALTER USER ")
	sb.WriteString(s.Username)
	if s.Host != "" {
		sb.WriteString("@")
		sb.WriteString(s.Host)
	}
	if s.Identified != "" {
		sb.WriteString(" IDENTIFIED BY '")
		sb.WriteString(s.Identified)
		sb.WriteString("'")
	}
	return sb.String()
}

// GrantStmt represents a GRANT statement.
type GrantStmt struct {
	Privileges []*Privilege
	On         GrantOn
	Database   string
	Table      string
	To         string // username
	Host       string
	WithGrant  bool   // WITH GRANT OPTION
}

func (s *GrantStmt) node()      {}
func (s *GrantStmt) statement() {}
func (s *GrantStmt) String() string {
	var sb strings.Builder
	sb.WriteString("GRANT ")
	for i, p := range s.Privileges {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(p.String())
	}
	if s.On == GrantOnAll {
		sb.WriteString(" ON *.*")
	} else if s.On == GrantOnDatabase {
		sb.WriteString(" ON ")
		sb.WriteString(s.Database)
		sb.WriteString(".*")
	} else {
		sb.WriteString(" ON ")
		if s.Database != "" {
			sb.WriteString(s.Database)
			sb.WriteString(".")
		}
		sb.WriteString(s.Table)
	}
	sb.WriteString(" TO ")
	sb.WriteString(s.To)
	if s.Host != "" {
		sb.WriteString("@")
		sb.WriteString(s.Host)
	}
	if s.WithGrant {
		sb.WriteString(" WITH GRANT OPTION")
	}
	return sb.String()
}

// RevokeStmt represents a REVOKE statement.
type RevokeStmt struct {
	Privileges []*Privilege
	On         GrantOn
	Database   string
	Table      string
	From       string // username
	Host       string
}

func (s *RevokeStmt) node()      {}
func (s *RevokeStmt) statement() {}
func (s *RevokeStmt) String() string {
	var sb strings.Builder
	sb.WriteString("REVOKE ")
	for i, p := range s.Privileges {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(p.String())
	}
	if s.On == GrantOnAll {
		sb.WriteString(" ON *.*")
	} else if s.On == GrantOnDatabase {
		sb.WriteString(" ON ")
		sb.WriteString(s.Database)
		sb.WriteString(".*")
	} else {
		sb.WriteString(" ON ")
		if s.Database != "" {
			sb.WriteString(s.Database)
			sb.WriteString(".")
		}
		sb.WriteString(s.Table)
	}
	sb.WriteString(" FROM ")
	sb.WriteString(s.From)
	if s.Host != "" {
		sb.WriteString("@")
		sb.WriteString(s.Host)
	}
	return sb.String()
}

// ShowGrantsStmt represents a SHOW GRANTS statement.
type ShowGrantsStmt struct {
	ForUser string // optional: SHOW GRANTS FOR user
	ForHost string
}

func (s *ShowGrantsStmt) node()      {}
func (s *ShowGrantsStmt) statement() {}
func (s *ShowGrantsStmt) String() string {
	if s.ForUser != "" {
		var sb strings.Builder
		sb.WriteString("SHOW GRANTS FOR ")
		sb.WriteString(s.ForUser)
		if s.ForHost != "" {
			sb.WriteString("@")
			sb.WriteString(s.ForHost)
		}
		return sb.String()
	}
	return "SHOW GRANTS"
}

// SetPasswordStmt represents a SET PASSWORD statement.
type SetPasswordStmt struct {
	ForUser string // optional: FOR user
	ForHost string
	Password string
}

func (s *SetPasswordStmt) node()      {}
func (s *SetPasswordStmt) statement() {}
func (s *SetPasswordStmt) String() string {
	var sb strings.Builder
	sb.WriteString("SET PASSWORD")
	if s.ForUser != "" {
		sb.WriteString(" FOR ")
		sb.WriteString(s.ForUser)
		if s.ForHost != "" {
			sb.WriteString("@")
			sb.WriteString(s.ForHost)
		}
	}
	sb.WriteString(" = '")
	sb.WriteString(s.Password)
	sb.WriteString("'")
	return sb.String()
}

// ============================================================================
// Backup and Restore Statements
// ============================================================================

// BackupStmt represents a BACKUP DATABASE statement.
type BackupStmt struct {
	Path      string // backup file path
	Compress  bool   // WITH COMPRESS option
	Incremental bool  // incremental backup
}

func (s *BackupStmt) node()      {}
func (s *BackupStmt) statement() {}
func (s *BackupStmt) String() string {
	var sb strings.Builder
	sb.WriteString("BACKUP DATABASE TO '")
	sb.WriteString(s.Path)
	sb.WriteString("'")
	if s.Compress {
		sb.WriteString(" WITH COMPRESS")
	}
	return sb.String()
}

// RestoreStmt represents a RESTORE DATABASE statement.
type RestoreStmt struct {
	Path string // backup file path
}

func (s *RestoreStmt) node()      {}
func (s *RestoreStmt) statement() {}
func (s *RestoreStmt) String() string {
	return fmt.Sprintf("RESTORE DATABASE FROM '%s'", s.Path)
}