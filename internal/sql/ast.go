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

// CTEDefinition represents a single CTE (Common Table Expression) definition.
type CTEDefinition struct {
	Name      string      // CTE name
	Columns   []string    // Optional column names
	Query     Statement   // The subquery defining the CTE
	Recursive bool        // Whether this is a recursive CTE
}

func (c *CTEDefinition) String() string {
	var sb strings.Builder
	sb.WriteString(c.Name)
	if len(c.Columns) > 0 {
		sb.WriteString("(")
		for i, col := range c.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col)
		}
		sb.WriteString(")")
	}
	sb.WriteString(" AS (")
	sb.WriteString(c.Query.String())
	sb.WriteString(")")
	return sb.String()
}

// WithStmt represents a WITH clause (CTE) statement.
// WITH cte_name AS (SELECT ...) SELECT * FROM cte_name
type WithStmt struct {
	CTEs      []CTEDefinition // List of CTE definitions
	MainQuery Statement        // The main query that uses the CTEs
}

func (s *WithStmt) node()      {}
func (s *WithStmt) statement() {}
func (s *WithStmt) String() string {
	var sb strings.Builder
	sb.WriteString("WITH ")
	for i, cte := range s.CTEs {
		if i > 0 {
			sb.WriteString(", ")
		}
		if cte.Recursive {
			sb.WriteString("RECURSIVE ")
		}
		sb.WriteString(cte.String())
	}
	sb.WriteString(" ")
	sb.WriteString(s.MainQuery.String())
	return sb.String()
}

// WithClause represents a WITH clause that can be attached to DML statements.
// It allows CTEs to be used with INSERT, UPDATE, DELETE.
type WithClause struct {
	CTEs      []CTEDefinition
	Recursive bool
}

func (w *WithClause) String() string {
	var sb strings.Builder
	sb.WriteString("WITH ")
	if w.Recursive {
		sb.WriteString("RECURSIVE ")
	}
	for i, cte := range w.CTEs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(cte.String())
	}
	return sb.String()
}

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
	Table                string
	Columns              []string
	Values               [][]Expression
	OnDuplicateKeyUpdate []*Assignment    // MySQL-style ON DUPLICATE KEY UPDATE
	OnConflict           *UpsertClause    // SQLite-style ON CONFLICT
	Returning            *ReturningClause // RETURNING clause
	WithClause           *WithClause      // Optional WITH clause
}

// UpsertClause represents an ON CONFLICT clause (SQLite-style UPSERT).
type UpsertClause struct {
	ConflictColumns []string      // ON CONFLICT(column1, column2)
	DoNothing       bool          // DO NOTHING
	DoUpdate        bool          // DO UPDATE
	Assignments     []*Assignment // SET assignments for DO UPDATE
	Where           Expression    // Optional WHERE for DO UPDATE
}

func (u *UpsertClause) String() string {
	var sb strings.Builder
	sb.WriteString("ON CONFLICT")
	if len(u.ConflictColumns) > 0 {
		sb.WriteString("(")
		for i, col := range u.ConflictColumns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col)
		}
		sb.WriteString(")")
	}
	if u.DoNothing {
		sb.WriteString(" DO NOTHING")
	} else if u.DoUpdate {
		sb.WriteString(" DO UPDATE SET ")
		for i, a := range u.Assignments {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(a.String())
		}
		if u.Where != nil {
			sb.WriteString(" WHERE ")
			sb.WriteString(u.Where.String())
		}
	}
	return sb.String()
}

// ReturningClause represents a RETURNING clause.
type ReturningClause struct {
	Columns []Expression // Columns to return, nil or empty means *
	All     bool         // RETURNING *
}

func (r *ReturningClause) String() string {
	var sb strings.Builder
	sb.WriteString("RETURNING ")
	if r.All || len(r.Columns) == 0 {
		sb.WriteString("*")
	} else {
		for i, col := range r.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col.String())
		}
	}
	return sb.String()
}

func (s *InsertStmt) node()      {}
func (s *InsertStmt) statement() {}
func (s *InsertStmt) String() string {
	var sb strings.Builder
	if s.WithClause != nil {
		sb.WriteString(s.WithClause.String())
		sb.WriteString(" ")
	}
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
	if s.OnConflict != nil {
		sb.WriteString(" ")
		sb.WriteString(s.OnConflict.String())
	}
	if s.Returning != nil {
		sb.WriteString(" ")
		sb.WriteString(s.Returning.String())
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
	Returning   *ReturningClause // RETURNING clause
	WithClause  *WithClause      // Optional WITH clause
}

func (s *UpdateStmt) node()      {}
func (s *UpdateStmt) statement() {}
func (s *UpdateStmt) String() string {
	var sb strings.Builder
	if s.WithClause != nil {
		sb.WriteString(s.WithClause.String())
		sb.WriteString(" ")
	}
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
	if s.Returning != nil {
		sb.WriteString(" ")
		sb.WriteString(s.Returning.String())
	}
	return sb.String()
}

// DeleteStmt represents a DELETE statement.
type DeleteStmt struct {
	Table      string
	Where      Expression
	OrderBy    []*OrderByItem
	Limit      *int
	Returning  *ReturningClause // RETURNING clause
	WithClause *WithClause      // Optional WITH clause
}

func (s *DeleteStmt) node()      {}
func (s *DeleteStmt) statement() {}
func (s *DeleteStmt) String() string {
	var sb strings.Builder
	if s.WithClause != nil {
		sb.WriteString(s.WithClause.String())
		sb.WriteString(" ")
	}
	sb.WriteString("DELETE FROM ")
	sb.WriteString(s.Table)
	if s.Where != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(s.Where.String())
	}
	if s.Returning != nil {
		sb.WriteString(" ")
		sb.WriteString(s.Returning.String())
	}
	return sb.String()
}

// CreateTableStmt represents a CREATE TABLE statement.
type CreateTableStmt struct {
	IfNotExists bool
	Temp        bool              // TEMP or TEMPORARY keyword
	TableName   string
	Columns     []*ColumnDef
	Constraints []*TableConstraint
	Options     map[string]string
}

func (s *CreateTableStmt) node()      {}
func (s *CreateTableStmt) statement() {}
func (s *CreateTableStmt) String() string {
	var sb strings.Builder
	sb.WriteString("CREATE ")
	if s.Temp {
		sb.WriteString("TEMP ")
	}
	sb.WriteString("TABLE ")
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

// CreateViewStmt represents a CREATE VIEW statement.
type CreateViewStmt struct {
	ViewName    string
	Columns     []string // Optional column names
	SelectStmt  Statement
	OrReplace   bool
	CheckOption string // "CASCADED", "LOCAL", or "" (empty for no check)
}

func (s *CreateViewStmt) node()      {}
func (s *CreateViewStmt) statement() {}
func (s *CreateViewStmt) String() string {
	var sb strings.Builder
	if s.OrReplace {
		sb.WriteString("CREATE OR REPLACE VIEW ")
	} else {
		sb.WriteString("CREATE VIEW ")
	}
	sb.WriteString(s.ViewName)
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
	sb.WriteString(" AS ")
	sb.WriteString(s.SelectStmt.String())
	if s.CheckOption != "" {
		sb.WriteString(" WITH ")
		sb.WriteString(s.CheckOption)
		sb.WriteString(" CHECK OPTION")
	}
	return sb.String()
}

// DropViewStmt represents a DROP VIEW statement.
type DropViewStmt struct {
	ViewName string
	IfExists bool
}

func (s *DropViewStmt) node()      {}
func (s *DropViewStmt) statement() {}
func (s *DropViewStmt) String() string {
	if s.IfExists {
		return fmt.Sprintf("DROP VIEW IF EXISTS %s", s.ViewName)
	}
	return fmt.Sprintf("DROP VIEW %s", s.ViewName)
}

// ExplainStmt represents an EXPLAIN statement.
type ExplainStmt struct {
	Statement Statement // The statement to explain
	QueryPlan bool      // EXPLAIN QUERY PLAN
}

func (s *ExplainStmt) node()      {}
func (s *ExplainStmt) statement() {}
func (s *ExplainStmt) String() string {
	if s.QueryPlan {
		return fmt.Sprintf("EXPLAIN QUERY PLAN %s", s.Statement.String())
	}
	return fmt.Sprintf("EXPLAIN %s", s.Statement.String())
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

// AnalyzeStmt represents an ANALYZE TABLE statement.
type AnalyzeStmt struct {
	TableName string // Empty means analyze all tables
}

func (s *AnalyzeStmt) node()      {}
func (s *AnalyzeStmt) statement() {}
func (s *AnalyzeStmt) String() string {
	if s.TableName == "" {
		return "ANALYZE"
	}
	return fmt.Sprintf("ANALYZE TABLE %s", s.TableName)
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
	Op       SetOperation // UNION, INTERSECT, or EXCEPT
}

// SetOperation represents a set operation type.
type SetOperation int

const (
	SetUnion SetOperation = iota
	SetIntersect
	SetExcept
)

func (s *SetOperation) String() string {
	switch *s {
	case SetUnion:
		return "UNION"
	case SetIntersect:
		return "INTERSECT"
	case SetExcept:
		return "EXCEPT"
	}
	return "UNKNOWN"
}

func (s *UnionStmt) node()      {}
func (s *UnionStmt) statement() {}
func (s *UnionStmt) String() string {
	var sb strings.Builder
	sb.WriteString(s.Left.String())
	sb.WriteString(" ")
	sb.WriteString(s.Op.String())
	sb.WriteString(" ")
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
	case JoinFull:
		sb.WriteString("FULL JOIN ")
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
	Expr       Expression
	Ascending  bool
	NullsFirst bool // NULLS FIRST specified
	NullsLast  bool // NULLS LAST specified
	Collate    string // COLLATE collation name
}

func (o *OrderByItem) node() {}
func (o *OrderByItem) String() string {
	s := o.Expr.String()
	if o.Collate != "" {
		s += " COLLATE " + o.Collate
	}
	if o.Ascending {
		s += " ASC"
	} else {
		s += " DESC"
	}
	if o.NullsFirst {
		s += " NULLS FIRST"
	} else if o.NullsLast {
		s += " NULLS LAST"
	}
	return s
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
	Name            string
	Type            *DataType
	Nullable        bool  // true if NULL allowed (default true)
	Default         Expression
	AutoIncr        bool
	PrimaryKey      bool
	Unique          bool
	Comment         string
	Collate         string    // COLLATE collation name
	GeneratedExpr   Expression // GENERATED ALWAYS AS expression
	GeneratedStored bool       // true if STORED, false if VIRTUAL
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
	if c.GeneratedExpr != nil {
		sb.WriteString(" GENERATED ALWAYS AS (")
		sb.WriteString(c.GeneratedExpr.String())
		sb.WriteString(")")
		if c.GeneratedStored {
			sb.WriteString(" STORED")
		} else {
			sb.WriteString(" VIRTUAL")
		}
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
	if c.Collate != "" {
		sb.WriteString(" COLLATE ")
		sb.WriteString(c.Collate)
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
	Alias string // optional alias
}

type LiteralType int

const (
	LiteralNull LiteralType = iota
	LiteralString
	LiteralNumber
	LiteralBool
	LiteralBlob
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
	case LiteralBlob:
		return fmt.Sprintf("X'%x'", l.Value)
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
	Left       Expression
	Op         BinaryOp
	Right      Expression
	Alias      string // optional alias
	EscapeChar string // optional escape character for LIKE/NOT LIKE (default is '\')
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
	OpGlob
	OpNotGlob
	OpIn
	OpNotIn
	OpConcat
)

func (op BinaryOp) String() string {
	switch op {
	case OpEq:
		return "="
	case OpNe:
		return "<>"
	case OpLt:
		return "<"
	case OpLe:
		return "<="
	case OpGt:
		return ">"
	case OpGe:
		return ">="
	case OpAdd:
		return "+"
	case OpSub:
		return "-"
	case OpMul:
		return "*"
	case OpDiv:
		return "/"
	case OpMod:
		return "%"
	case OpAnd:
		return "AND"
	case OpOr:
		return "OR"
	case OpLike:
		return "LIKE"
	case OpNotLike:
		return "NOT LIKE"
	case OpIn:
		return "IN"
	case OpNotIn:
		return "NOT IN"
	case OpConcat:
		return "||"
	default:
		return "?"
	}
}

func (e *BinaryExpr) node()       {}
func (e *BinaryExpr) expression() {}
func (e *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left.String(), e.Op.String(), e.Right.String())
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
	Name     string
	Args     []Expression
	Distinct bool
	Filter   Expression // FILTER (WHERE ...) clause for aggregates
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
	if f.Filter != nil {
		sb.WriteString(" FILTER (WHERE ")
		sb.WriteString(f.Filter.String())
		sb.WriteString(")")
	}
	return sb.String()
}

// WindowSpec represents the window specification for window functions.
type WindowSpec struct {
	PartitionBy []Expression  // PARTITION BY expressions
	OrderBy     []*OrderByItem // ORDER BY items
	Name        string        // Named window reference (optional)
	Frame       *FrameSpec    // Window frame clause (optional)
}

// FrameSpec represents a window frame clause (ROWS/RANGE BETWEEN ... AND ...).
type FrameSpec struct {
	Mode      string     // "ROWS" or "RANGE"
	Start     FrameBound // Start bound
	End       FrameBound // End bound (optional, defaults to CURRENT ROW)
}

// FrameBound represents one side of a window frame.
type FrameBound struct {
	Type      string // "UNBOUNDED PRECEDING", "PRECEDING", "CURRENT ROW", "FOLLOWING", "UNBOUNDED FOLLOWING"
	Offset    int    // Offset for PRECEDING/FOLLOWING (0 for CURRENT ROW, UNBOUNDED)
}

func (f *FrameSpec) String() string {
	var sb strings.Builder
	sb.WriteString(f.Mode)
	sb.WriteString(" BETWEEN ")
	sb.WriteString(f.Start.String())
	sb.WriteString(" AND ")
	sb.WriteString(f.End.String())
	return sb.String()
}

func (f FrameBound) String() string {
	switch f.Type {
	case "UNBOUNDED PRECEDING":
		return "UNBOUNDED PRECEDING"
	case "UNBOUNDED FOLLOWING":
		return "UNBOUNDED FOLLOWING"
	case "CURRENT ROW":
		return "CURRENT ROW"
	case "PRECEDING":
		return fmt.Sprintf("%d PRECEDING", f.Offset)
	case "FOLLOWING":
		return fmt.Sprintf("%d FOLLOWING", f.Offset)
	default:
		return f.Type
	}
}

func (w *WindowSpec) String() string {
	var sb strings.Builder
	sb.WriteString("OVER (")
	if len(w.PartitionBy) > 0 {
		sb.WriteString("PARTITION BY ")
		for i, expr := range w.PartitionBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(expr.String())
		}
	}
	if len(w.OrderBy) > 0 {
		if len(w.PartitionBy) > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString("ORDER BY ")
		for i, item := range w.OrderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(item.String())
		}
	}
	if w.Frame != nil {
		if len(w.PartitionBy) > 0 || len(w.OrderBy) > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(w.Frame.String())
	}
	sb.WriteString(")")
	return sb.String()
}

// WindowFuncCall represents a window function call with OVER clause.
type WindowFuncCall struct {
	Func        *FunctionCall // The function being called
	Window      *WindowSpec   // The window specification
	Alias       string        // optional alias
	IgnoreNulls bool          // IGNORE NULLS (for LEAD/LAG/FIRST_VALUE/LAST_VALUE)
	RespectNulls bool         // RESPECT NULLS (default behavior, explicitly stated)
}

func (w *WindowFuncCall) node()       {}
func (w *WindowFuncCall) expression() {}
func (w *WindowFuncCall) String() string {
	var sb strings.Builder
	sb.WriteString(w.Func.String())
	if w.IgnoreNulls {
		sb.WriteString(" IGNORE NULLS")
	} else if w.RespectNulls {
		sb.WriteString(" RESPECT NULLS")
	}
	sb.WriteString(" ")
	sb.WriteString(w.Window.String())
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

// TableRef represents a table reference (either a named table or a subquery).
type TableRef struct {
	Name     string        // Table name (if referencing a real table)
	Alias    string        // Optional alias
	Subquery *SubqueryExpr // Subquery (if this is a derived table)
	Lateral  bool          // LATERAL keyword for correlated subqueries
	Values   *ValuesExpr   // VALUES constructor (if this is a values table)
}

func (t *TableRef) node() {}
func (t *TableRef) String() string {
	if t.Values != nil {
		if t.Alias != "" {
			return fmt.Sprintf("%s AS %s", t.Values.String(), t.Alias)
		}
		return t.Values.String()
	}
	if t.Subquery != nil {
		prefix := ""
		if t.Lateral {
			prefix = "LATERAL "
		}
		if t.Alias != "" {
			return fmt.Sprintf("%s(%s) AS %s", prefix, t.Subquery.String(), t.Alias)
		}
		return fmt.Sprintf("%s(%s)", prefix, t.Subquery.String())
	}
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

// CollateExpr represents a COLLATE expression.
// Syntax: expr COLLATE collation_name
type CollateExpr struct {
	Expr     Expression
	Collate  string
}

func (e *CollateExpr) node()       {}
func (e *CollateExpr) expression() {}
func (e *CollateExpr) String() string {
	return fmt.Sprintf("%s COLLATE %s", e.Expr.String(), e.Collate)
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

// ValuesExpr represents a VALUES table constructor.
// Example: VALUES (1, 'a'), (2, 'b')
type ValuesExpr struct {
	Rows   [][]Expression // Each row is a list of expressions
	Alias  string         // Optional table alias
	Columns []string      // Optional column aliases
}

func (e *ValuesExpr) node()       {}
func (e *ValuesExpr) expression() {}
func (e *ValuesExpr) String() string {
	var sb strings.Builder
	sb.WriteString("VALUES ")
	for i, row := range e.Rows {
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

// ExistsExpr represents an EXISTS subquery expression.
type ExistsExpr struct {
	Subquery *SubqueryExpr
	Not      bool // true for NOT EXISTS
}

func (e *ExistsExpr) node()       {}
func (e *ExistsExpr) expression() {}
func (e *ExistsExpr) String() string {
	if e.Not {
		return fmt.Sprintf("NOT EXISTS %s", e.Subquery.String())
	}
	return fmt.Sprintf("EXISTS %s", e.Subquery.String())
}

// AnyAllExpr represents an ANY or ALL expression with a subquery.
// Examples: x > ANY (SELECT ...), x = ALL (SELECT ...)
type AnyAllExpr struct {
	Left     Expression  // Left operand
	Op       BinaryOp    // Comparison operator (=, >, <, >=, <=, !=, <>)
	IsAny    bool        // true for ANY, false for ALL
	Subquery *SubqueryExpr
}

func (e *AnyAllExpr) node()       {}
func (e *AnyAllExpr) expression() {}
func (e *AnyAllExpr) String() string {
	keyword := "ALL"
	if e.IsAny {
		keyword = "ANY"
	}
	return fmt.Sprintf("%s %s %s (%s)", e.Left.String(), e.Op.String(), keyword, e.Subquery.String())
}

// ScalarSubquery represents a scalar subquery that returns a single value.
// Example: SELECT (SELECT COUNT(*) FROM users)
type ScalarSubquery struct {
	Subquery *SubqueryExpr
}

func (e *ScalarSubquery) node()       {}
func (e *ScalarSubquery) expression() {}
func (e *ScalarSubquery) String() string {
	return fmt.Sprintf("(%s)", e.Subquery.String())
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

// VacuumStmt represents a VACUUM statement.
// Syntax: VACUUM [table_name] [INTO 'filename']
type VacuumStmt struct {
	Table    string // optional table name, empty means vacuum entire database
	IntoPath string // optional INTO path for vacuum into a different file
}

func (s *VacuumStmt) node()      {}
func (s *VacuumStmt) statement() {}
func (s *VacuumStmt) String() string {
	var sb strings.Builder
	sb.WriteString("VACUUM")
	if s.Table != "" {
		sb.WriteString(" ")
		sb.WriteString(s.Table)
	}
	if s.IntoPath != "" {
		sb.WriteString(" INTO '")
		sb.WriteString(s.IntoPath)
		sb.WriteString("'")
	}
	return sb.String()
}

// PragmaStmt represents a PRAGMA statement.
// Syntax: PRAGMA name [= value] or PRAGMA name(argument)
type PragmaStmt struct {
	Name     string      // pragma name
	Value    interface{} // optional value (can be string, int, bool, or nil for query)
	Argument string      // optional argument for function-style pragmas like table_info(table)
}

func (s *PragmaStmt) node()      {}
func (s *PragmaStmt) statement() {}
func (s *PragmaStmt) String() string {
	if s.Argument != "" {
		if s.Value != nil {
			return fmt.Sprintf("PRAGMA %s(%s) = %v", s.Name, s.Argument, s.Value)
		}
		return fmt.Sprintf("PRAGMA %s(%s)", s.Name, s.Argument)
	}
	if s.Value == nil {
		return "PRAGMA " + s.Name
	}
	return fmt.Sprintf("PRAGMA %s = %v", s.Name, s.Value)
}

// ============================================================================
// UDF Expression Types
// ============================================================================

// IfExpr represents an IF expression.
// Syntax: IF condition THEN expr [ELSE expr] END
type IfExpr struct {
	Condition Expression
	ThenExpr  Expression
	ElseExpr  Expression // may be nil
}

func (e *IfExpr) node()       {}
func (e *IfExpr) expression() {}
func (e *IfExpr) String() string {
	var sb strings.Builder
	sb.WriteString("IF ")
	sb.WriteString(e.Condition.String())
	sb.WriteString(" THEN ")
	sb.WriteString(e.ThenExpr.String())
	if e.ElseExpr != nil {
		sb.WriteString(" ELSE ")
		sb.WriteString(e.ElseExpr.String())
	}
	sb.WriteString(" END")
	return sb.String()
}

// LetExpr represents a LET variable assignment.
// Syntax: LET name = expr
type LetExpr struct {
	Name  string
	Value Expression
}

func (e *LetExpr) node()       {}
func (e *LetExpr) expression() {}
func (e *LetExpr) String() string {
	return fmt.Sprintf("LET %s = %s", e.Name, e.Value.String())
}

// BlockExpr represents a block of expressions.
// Syntax: BEGIN expr; expr; ... END or (expr, expr, ...)
// The result is the value of the last expression.
type BlockExpr struct {
	Expressions []Expression
}

func (e *BlockExpr) node()       {}
func (e *BlockExpr) expression() {}
func (e *BlockExpr) String() string {
	if len(e.Expressions) == 0 {
		return "BEGIN END"
	}
	var sb strings.Builder
	sb.WriteString("BEGIN ")
	for i, expr := range e.Expressions {
		if i > 0 {
			sb.WriteString("; ")
		}
		sb.WriteString(expr.String())
	}
	sb.WriteString(" END")
	return sb.String()
}

// ============================================================================
// User Defined Function Statements
// ============================================================================

// FunctionParameter represents a parameter in a UDF.
type FunctionParameter struct {
	Name         string
	Type         *DataType
	DefaultValue Expression // optional default value
}

func (p *FunctionParameter) String() string {
	var sb strings.Builder
	sb.WriteString(p.Name)
	if p.Type != nil {
		sb.WriteString(" ")
		sb.WriteString(p.Type.String())
	}
	if p.DefaultValue != nil {
		sb.WriteString(" DEFAULT ")
		sb.WriteString(p.DefaultValue.String())
	}
	return sb.String()
}

// CreateFunctionStmt represents a CREATE FUNCTION statement.
type CreateFunctionStmt struct {
	Name       string
	Parameters []*FunctionParameter
	ReturnType *DataType
	Body       Expression // Old style: SQL expression body
	Script     string     // New style: XxScript body
	Replace    bool       // CREATE OR REPLACE
}

func (s *CreateFunctionStmt) node()      {}
func (s *CreateFunctionStmt) statement() {}
func (s *CreateFunctionStmt) String() string {
	var sb strings.Builder
	if s.Replace {
		sb.WriteString("CREATE OR REPLACE ")
	} else {
		sb.WriteString("CREATE ")
	}
	sb.WriteString("FUNCTION ")
	sb.WriteString(s.Name)
	sb.WriteString("(")
	for i, p := range s.Parameters {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(p.Name)
		if p.Type != nil {
			sb.WriteString(" ")
			sb.WriteString(p.Type.String())
		}
	}
	sb.WriteString(") RETURNS ")
	if s.ReturnType != nil {
		sb.WriteString(s.ReturnType.String())
	}
	if s.Script != "" {
		sb.WriteString(" AS $$ ")
		sb.WriteString(s.Script)
		sb.WriteString(" $$")
	} else if s.Body != nil {
		sb.WriteString(" RETURN ")
		sb.WriteString(s.Body.String())
	}
	return sb.String()
}

// DropFunctionStmt represents a DROP FUNCTION statement.
type DropFunctionStmt struct {
	Name     string
	IfExists bool
}

func (s *DropFunctionStmt) node()      {}
func (s *DropFunctionStmt) statement() {}
func (s *DropFunctionStmt) String() string {
	var sb strings.Builder
	sb.WriteString("DROP FUNCTION ")
	if s.IfExists {
		sb.WriteString("IF EXISTS ")
	}
	sb.WriteString(s.Name)
	return sb.String()
}

// UserFunction represents a stored user-defined function.
type UserFunction struct {
	Name       string
	Parameters []*FunctionParameter
	ReturnType *DataType
	Body       Expression
}

// ============================================================================
// Trigger Statements
// ============================================================================

// TriggerTiming represents when a trigger fires.
type TriggerTiming int

const (
	TriggerBefore TriggerTiming = iota
	TriggerAfter
	TriggerInsteadOf
)

func (t TriggerTiming) String() string {
	switch t {
	case TriggerBefore:
		return "BEFORE"
	case TriggerAfter:
		return "AFTER"
	case TriggerInsteadOf:
		return "INSTEAD OF"
	}
	return "UNKNOWN"
}

// TriggerEvent represents the event that fires a trigger.
type TriggerEvent int

const (
	TriggerInsert TriggerEvent = iota
	TriggerUpdate
	TriggerDelete
)

func (e TriggerEvent) String() string {
	switch e {
	case TriggerInsert:
		return "INSERT"
	case TriggerUpdate:
		return "UPDATE"
	case TriggerDelete:
		return "DELETE"
	}
	return "UNKNOWN"
}

// TriggerGranularity represents FOR EACH ROW or FOR EACH STATEMENT.
type TriggerGranularity int

const (
	TriggerForEachRow TriggerGranularity = iota
	TriggerForEachStatement
)

func (g TriggerGranularity) String() string {
	switch g {
	case TriggerForEachRow:
		return "FOR EACH ROW"
	case TriggerForEachStatement:
		return "FOR EACH STATEMENT"
	}
	return "UNKNOWN"
}

// CreateTriggerStmt represents a CREATE TRIGGER statement.
// Syntax: CREATE TRIGGER name {BEFORE|AFTER|INSTEAD OF} {INSERT|UPDATE|DELETE} ON table [FOR EACH ROW] BEGIN statements END
type CreateTriggerStmt struct {
	TriggerName  string
	Timing       TriggerTiming
	Event        TriggerEvent
	TableName    string
	Granularity  TriggerGranularity
	WhenClause   Expression    // optional WHEN condition
	Body         []Statement   // trigger body statements
	IfNotExists  bool
}

func (s *CreateTriggerStmt) node()      {}
func (s *CreateTriggerStmt) statement() {}
func (s *CreateTriggerStmt) String() string {
	var sb strings.Builder
	sb.WriteString("CREATE TRIGGER ")
	if s.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	sb.WriteString(s.TriggerName)
	sb.WriteString(" ")
	sb.WriteString(s.Timing.String())
	sb.WriteString(" ")
	sb.WriteString(s.Event.String())
	sb.WriteString(" ON ")
	sb.WriteString(s.TableName)
	sb.WriteString(" ")
	sb.WriteString(s.Granularity.String())
	if s.WhenClause != nil {
		sb.WriteString(" WHEN ")
		sb.WriteString(s.WhenClause.String())
	}
	sb.WriteString(" BEGIN ")
	for i, stmt := range s.Body {
		if i > 0 {
			sb.WriteString("; ")
		}
		sb.WriteString(stmt.String())
	}
	sb.WriteString(" END")
	return sb.String()
}

// DropTriggerStmt represents a DROP TRIGGER statement.
type DropTriggerStmt struct {
	TriggerName string
	TableName   string // optional, for MySQL compatibility
	IfExists    bool
}

func (s *DropTriggerStmt) node()      {}
func (s *DropTriggerStmt) statement() {}
func (s *DropTriggerStmt) String() string {
	var sb strings.Builder
	sb.WriteString("DROP TRIGGER ")
	if s.IfExists {
		sb.WriteString("IF EXISTS ")
	}
	sb.WriteString(s.TriggerName)
	if s.TableName != "" {
		sb.WriteString(" ON ")
		sb.WriteString(s.TableName)
	}
	return sb.String()
}

// TriggerInfo represents stored trigger information.
type TriggerInfo struct {
	Name        string
	Timing      TriggerTiming
	Event       TriggerEvent
	TableName   string
	Granularity TriggerGranularity
	WhenClause  string // serialized WHEN expression
	Body        string // serialized body statements
}

// ============================================================================
// Transaction Statements
// ============================================================================

// BeginStmt represents a BEGIN [TRANSACTION] statement.
type BeginStmt struct {
	// TransactionType: "", "DEFERRED", "IMMEDIATE", or "EXCLUSIVE"
	TransactionType string
}

func (s *BeginStmt) node()      {}
func (s *BeginStmt) statement() {}
func (s *BeginStmt) String() string {
	switch s.TransactionType {
	case "DEFERRED":
		return "BEGIN DEFERRED TRANSACTION"
	case "IMMEDIATE":
		return "BEGIN IMMEDIATE TRANSACTION"
	case "EXCLUSIVE":
		return "BEGIN EXCLUSIVE TRANSACTION"
	default:
		return "BEGIN TRANSACTION"
	}
}

// CommitStmt represents a COMMIT [TRANSACTION] statement.
type CommitStmt struct {
	// COMMIT saves all changes made in the current transaction
}

func (s *CommitStmt) node()      {}
func (s *CommitStmt) statement() {}
func (s *CommitStmt) String() string {
	return "COMMIT TRANSACTION"
}

// RollbackStmt represents a ROLLBACK [TRANSACTION] [TO SAVEPOINT name] statement.
type RollbackStmt struct {
	ToSavepoint string // if non-empty, rollback to this savepoint
}

func (s *RollbackStmt) node()      {}
func (s *RollbackStmt) statement() {}
func (s *RollbackStmt) String() string {
	if s.ToSavepoint != "" {
		return fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", s.ToSavepoint)
	}
	return "ROLLBACK TRANSACTION"
}

// SavepointStmt represents a SAVEPOINT name statement.
type SavepointStmt struct {
	Name string
}

func (s *SavepointStmt) node()      {}
func (s *SavepointStmt) statement() {}
func (s *SavepointStmt) String() string {
	return fmt.Sprintf("SAVEPOINT %s", s.Name)
}

// ReleaseSavepointStmt represents a RELEASE SAVEPOINT name statement.
type ReleaseSavepointStmt struct {
	Name string
}

func (s *ReleaseSavepointStmt) node()      {}
func (s *ReleaseSavepointStmt) statement() {}
func (s *ReleaseSavepointStmt) String() string {
	return fmt.Sprintf("RELEASE SAVEPOINT %s", s.Name)
}

// ============================================================================
// Bulk Import/Export Statements
// ============================================================================

// CopyStmt represents a COPY statement for bulk import/export.
// COPY table FROM 'file.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
// COPY (SELECT ...) TO 'file.csv' WITH (FORMAT csv, HEADER true)
type CopyStmt struct {
	TableName   string    // For COPY table FROM
	Query       Statement // For COPY (SELECT ...) TO
	FileName    string
	Direction   string // "FROM" or "TO"
	Format      string // csv, tsv, text (default: csv)
	Header      bool   // First row is header
	Delimiter   string // Field delimiter (default: comma)
	Quote       string // Quote character (default: ")
	NullString  string // String representing NULL (default: \N)
	Encoding    string // File encoding (default: utf-8)
}

func (s *CopyStmt) node()      {}
func (s *CopyStmt) statement() {}
func (s *CopyStmt) String() string {
	var sb strings.Builder
	sb.WriteString("COPY ")
	if s.Query != nil {
		sb.WriteString("(")
		sb.WriteString(s.Query.String())
		sb.WriteString(")")
	} else {
		sb.WriteString(s.TableName)
	}
	sb.WriteString(" ")
	sb.WriteString(s.Direction)
	sb.WriteString(" '")
	sb.WriteString(s.FileName)
	sb.WriteString("'")
	if s.Format != "" || s.Header || s.Delimiter != "" {
		sb.WriteString(" WITH (")
		first := true
		if s.Format != "" {
			if !first {
				sb.WriteString(", ")
			}
			sb.WriteString("FORMAT ")
			sb.WriteString(s.Format)
			first = false
		}
		if s.Header {
			if !first {
				sb.WriteString(", ")
			}
			sb.WriteString("HEADER true")
			first = false
		}
		if s.Delimiter != "" {
			if !first {
				sb.WriteString(", ")
			}
			sb.WriteString("DELIMITER '")
			sb.WriteString(s.Delimiter)
			sb.WriteString("'")
		}
		sb.WriteString(")")
	}
	return sb.String()
}

// LoadDataStmt represents a LOAD DATA INFILE statement (MySQL style).
// LOAD DATA INFILE 'file.csv' INTO TABLE table_name
//   FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
//   IGNORE 1 ROWS;
type LoadDataStmt struct {
	FileName        string
	TableName       string
	FieldsTerminated string // default: '\t'
	FieldsEnclosed   string // default: ''
	FieldsEscaped    string // default: '\\'
	LinesTerminated  string // default: '\n'
	LinesStarting    string // default: ''
	IgnoreRows       int
	ColumnList       []string // Optional column list
}

func (s *LoadDataStmt) node()      {}
func (s *LoadDataStmt) statement() {}
func (s *LoadDataStmt) String() string {
	var sb strings.Builder
	sb.WriteString("LOAD DATA INFILE '")
	sb.WriteString(s.FileName)
	sb.WriteString("' INTO TABLE ")
	sb.WriteString(s.TableName)
	if s.FieldsTerminated != "" || s.FieldsEnclosed != "" || s.FieldsEscaped != "" {
		sb.WriteString(" FIELDS")
		if s.FieldsTerminated != "" {
			sb.WriteString(" TERMINATED BY '")
			sb.WriteString(s.FieldsTerminated)
			sb.WriteString("'")
		}
		if s.FieldsEnclosed != "" {
			sb.WriteString(" ENCLOSED BY '")
			sb.WriteString(s.FieldsEnclosed)
			sb.WriteString("'")
		}
		if s.FieldsEscaped != "" {
			sb.WriteString(" ESCAPED BY '")
			sb.WriteString(s.FieldsEscaped)
			sb.WriteString("'")
		}
	}
	if s.LinesTerminated != "" || s.LinesStarting != "" {
		sb.WriteString(" LINES")
		if s.LinesStarting != "" {
			sb.WriteString(" STARTING BY '")
			sb.WriteString(s.LinesStarting)
			sb.WriteString("'")
		}
		if s.LinesTerminated != "" {
			sb.WriteString(" TERMINATED BY '")
			sb.WriteString(s.LinesTerminated)
			sb.WriteString("'")
		}
	}
	if s.IgnoreRows > 0 {
		sb.WriteString(" IGNORE ")
		sb.WriteString(fmt.Sprintf("%d", s.IgnoreRows))
		sb.WriteString(" ROWS")
	}
	if len(s.ColumnList) > 0 {
		sb.WriteString(" (")
		for i, col := range s.ColumnList {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col)
		}
		sb.WriteString(")")
	}
	return sb.String()
}

// ============================================================================
// Full-Text Search Statements
// ============================================================================

// CreateFTSStmt represents a CREATE FTS INDEX statement.
// Syntax: CREATE FTS INDEX name ON table(column1, column2, ...) [WITH TOKENIZER tokenizer]
type CreateFTSStmt struct {
	IndexName   string   // Name of the FTS index
	TableName   string   // Table to index
	Columns     []string // Columns to include in the index
	Tokenizer   string   // Tokenizer type: "simple" (default), "porter"
	IfNotExists bool     // IF NOT EXISTS clause
}

func (s *CreateFTSStmt) node()      {}
func (s *CreateFTSStmt) statement() {}
func (s *CreateFTSStmt) String() string {
	var sb strings.Builder
	sb.WriteString("CREATE FTS INDEX ")
	if s.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	sb.WriteString(s.IndexName)
	sb.WriteString(" ON ")
	sb.WriteString(s.TableName)
	sb.WriteString("(")
	for i, col := range s.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col)
	}
	sb.WriteString(")")
	if s.Tokenizer != "" && s.Tokenizer != "simple" {
		sb.WriteString(" WITH TOKENIZER ")
		sb.WriteString(s.Tokenizer)
	}
	return sb.String()
}

// DropFTSStmt represents a DROP FTS INDEX statement.
// Syntax: DROP FTS INDEX name
type DropFTSStmt struct {
	IndexName string
	IfExists  bool
}

func (s *DropFTSStmt) node()      {}
func (s *DropFTSStmt) statement() {}
func (s *DropFTSStmt) String() string {
	var sb strings.Builder
	sb.WriteString("DROP FTS INDEX ")
	if s.IfExists {
		sb.WriteString("IF EXISTS ")
	}
	sb.WriteString(s.IndexName)
	return sb.String()
}

// MatchExpr represents a MATCH expression for full-text search.
// Syntax: table_name MATCH 'search query'
// Used in WHERE clause for FTS queries.
type MatchExpr struct {
	Table   string   // Table name (or alias)
	Query   string   // Search query string
	Columns []string // Optional: specific columns to search
}

func (e *MatchExpr) node()       {}
func (e *MatchExpr) expression() {}
func (e *MatchExpr) String() string {
	var sb strings.Builder
	if len(e.Columns) > 0 {
		for i, col := range e.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col)
		}
		sb.WriteString(" ")
	}
	sb.WriteString("MATCH '")
	sb.WriteString(e.Query)
	sb.WriteString("'")
	return sb.String()
}

// RankExpr represents a RANK expression for FTS result ordering.
// Returns the relevance score of the FTS match.
type RankExpr struct {
	IndexName string // Optional: specific index name
}

func (e *RankExpr) node()       {}
func (e *RankExpr) expression() {}
func (e *RankExpr) String() string {
	if e.IndexName != "" {
		return fmt.Sprintf("RANK(%s)", e.IndexName)
	}
	return "RANK"
}