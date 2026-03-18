package sql

import (
	"testing"
)

// TestSelectStmt_String tests the String method for SelectStmt
func TestSelectStmt_String(t *testing.T) {
	limit := 10
	offset := 5

	tests := []struct {
		name string
		stmt *SelectStmt
	}{
		{
			name: "simple select",
			stmt: &SelectStmt{
				Columns: []Expression{&ColumnRef{Name: "id"}},
			},
		},
		{
			name: "select distinct",
			stmt: &SelectStmt{
				Distinct: true,
				Columns:  []Expression{&ColumnRef{Name: "name"}},
			},
		},
		{
			name: "select with where",
			stmt: &SelectStmt{
				Columns: []Expression{&StarExpr{}},
				Where: &BinaryExpr{
					Left:  &ColumnRef{Name: "id"},
					Op:    OpEq,
					Right: &Literal{Value: 1, Type: LiteralNumber},
				},
			},
		},
		{
			name: "select with group by",
			stmt: &SelectStmt{
				Columns: []Expression{&ColumnRef{Name: "name"}},
				GroupBy: []Expression{&ColumnRef{Name: "category"}},
			},
		},
		{
			name: "select with having",
			stmt: &SelectStmt{
				Columns: []Expression{&ColumnRef{Name: "name"}},
				GroupBy: []Expression{&ColumnRef{Name: "id"}},
				Having: &BinaryExpr{
					Left:  &ColumnRef{Name: "count"},
					Op:    OpGt,
					Right: &Literal{Value: 5, Type: LiteralNumber},
				},
			},
		},
		{
			name: "select with order by",
			stmt: &SelectStmt{
				Columns: []Expression{&ColumnRef{Name: "name"}},
				OrderBy: []*OrderByItem{{Expr: &ColumnRef{Name: "id"}, Ascending: true}},
			},
		},
		{
			name: "select with limit and offset",
			stmt: &SelectStmt{
				Columns: []Expression{&StarExpr{}},
				Limit:   &limit,
				Offset:  &offset,
			},
		},
		{
			name: "select with from",
			stmt: &SelectStmt{
				Columns: []Expression{&StarExpr{}},
				From:    &FromClause{Table: &TableRef{Name: "users"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.stmt.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestInsertStmt_String tests the String method for InsertStmt
func TestInsertStmt_String(t *testing.T) {
	stmt := &InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name"},
		Values: [][]Expression{
			{&Literal{Value: 1, Type: LiteralNumber}, &Literal{Value: "Alice", Type: LiteralString}},
		},
	}
	result := stmt.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestUpdateStmt_String tests the String method for UpdateStmt
func TestUpdateStmt_String(t *testing.T) {
	stmt := &UpdateStmt{
		Table: "users",
		Assignments: []*Assignment{
			{Column: "name", Value: &Literal{Value: "Bob", Type: LiteralString}},
		},
		Where: &BinaryExpr{
			Left:  &ColumnRef{Name: "id"},
			Op:    OpEq,
			Right: &Literal{Value: 1, Type: LiteralNumber},
		},
	}
	result := stmt.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestDeleteStmt_String tests the String method for DeleteStmt
func TestDeleteStmt_String(t *testing.T) {
	stmt := &DeleteStmt{
		Table: "users",
		Where: &BinaryExpr{
			Left:  &ColumnRef{Name: "id"},
			Op:    OpGt,
			Right: &Literal{Value: 100, Type: LiteralNumber},
		},
	}
	result := stmt.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestCreateTableStmt_String tests the String method for CreateTableStmt
func TestCreateTableStmt_String(t *testing.T) {
	stmt := &CreateTableStmt{
		TableName: "users",
		Columns: []*ColumnDef{
			{Name: "id", Type: &DataType{Name: "INT"}, PrimaryKey: true},
			{Name: "name", Type: &DataType{Name: "VARCHAR", Size: 100}, Nullable: true},
		},
	}
	result := stmt.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestDropTableStmt_String tests the String method for DropTableStmt
func TestDropTableStmt_String(t *testing.T) {
	stmt := &DropTableStmt{
		TableName: "users",
		IfExists:  true,
	}
	result := stmt.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestAlterTableStmt_String tests the String method for AlterTableStmt
func TestAlterTableStmt_String(t *testing.T) {
	stmt := &AlterTableStmt{
		TableName: "users",
		Actions: []AlterAction{
			&AddColumnAction{
				Column: &ColumnDef{Name: "email", Type: &DataType{Name: "VARCHAR", Size: 255}},
			},
		},
	}
	result := stmt.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestAlterAction_String tests the String method for alter actions
func TestAlterAction_String(t *testing.T) {
	tests := []struct {
		name   string
		action AlterAction
	}{
		{
			name:   "add column",
			action: &AddColumnAction{Column: &ColumnDef{Name: "age", Type: &DataType{Name: "INT"}}},
		},
		{
			name:   "drop column",
			action: &DropColumnAction{ColumnName: "old_column"},
		},
		{
			name:   "modify column",
			action: &ModifyColumnAction{Column: &ColumnDef{Name: "name", Type: &DataType{Name: "VARCHAR", Size: 200}}},
		},
		{
			name:   "rename column",
			action: &RenameColumnAction{OldName: "old_name", NewName: "new_name"},
		},
		{
			name:   "rename table",
			action: &RenameTableAction{NewName: "new_table"},
		},
		{
			name: "add constraint",
			action: &AddConstraintAction{
				Constraint: &TableConstraint{Type: ConstraintPrimaryKey, Columns: []string{"id"}},
			},
		},
		{
			name:   "drop constraint",
			action: &DropConstraintAction{ConstraintName: "fk_user"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.action.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestTruncateTableStmt_String tests the String method for TruncateTableStmt
func TestTruncateTableStmt_String(t *testing.T) {
	stmt := &TruncateTableStmt{TableName: "logs"}
	result := stmt.String()
	if result != "TRUNCATE TABLE logs" {
		t.Errorf("Expected 'TRUNCATE TABLE logs', got %q", result)
	}
}

// TestUseStmt_String tests the String method for UseStmt
func TestUseStmt_String(t *testing.T) {
	stmt := &UseStmt{Database: "mydb"}
	result := stmt.String()
	if result != "USE mydb" {
		t.Errorf("Expected 'USE mydb', got %q", result)
	}
}

// TestShowStmt_String tests the String method for ShowStmt
func TestShowStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *ShowStmt
	}{
		{
			name: "show tables",
			stmt: &ShowStmt{Type: "TABLES"},
		},
		{
			name: "show columns with from",
			stmt: &ShowStmt{Type: "COLUMNS", From: "users"},
		},
		{
			name: "show tables with like",
			stmt: &ShowStmt{Type: "TABLES", Like: "user%"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.stmt.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestDescribeStmt_String tests the String method for DescribeStmt
func TestDescribeStmt_String(t *testing.T) {
	stmt := &DescribeStmt{TableName: "users"}
	result := stmt.String()
	if result != "DESCRIBE users" {
		t.Errorf("Expected 'DESCRIBE users', got %q", result)
	}
}

// TestShowCreateTableStmt_String tests the String method for ShowCreateTableStmt
func TestShowCreateTableStmt_String(t *testing.T) {
	stmt := &ShowCreateTableStmt{TableName: "users"}
	result := stmt.String()
	if result != "SHOW CREATE TABLE users" {
		t.Errorf("Expected 'SHOW CREATE TABLE users', got %q", result)
	}
}

// TestCreateIndexStmt_String tests the String method for CreateIndexStmt
func TestCreateIndexStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *CreateIndexStmt
	}{
		{
			name: "simple index",
			stmt: &CreateIndexStmt{
				IndexName: "idx_name",
				TableName: "users",
				Columns:   []string{"name"},
			},
		},
		{
			name: "unique index",
			stmt: &CreateIndexStmt{
				IndexName: "idx_email",
				TableName: "users",
				Unique:    true,
				Columns:   []string{"email"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.stmt.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestDropIndexStmt_String tests the String method for DropIndexStmt
func TestDropIndexStmt_String(t *testing.T) {
	stmt := &DropIndexStmt{IndexName: "idx_name", TableName: "users"}
	result := stmt.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestColumnDef_String tests the String method for ColumnDef
func TestColumnDef_String(t *testing.T) {
	tests := []struct {
		name string
		col  *ColumnDef
	}{
		{
			name: "simple column",
			col:  &ColumnDef{Name: "id", Type: &DataType{Name: "INT"}},
		},
		{
			name: "primary key column",
			col:  &ColumnDef{Name: "id", Type: &DataType{Name: "INT"}, PrimaryKey: true},
		},
		{
			name: "not null column",
			col:  &ColumnDef{Name: "name", Type: &DataType{Name: "VARCHAR", Size: 100}, Nullable: false},
		},
		{
			name: "auto increment column",
			col:  &ColumnDef{Name: "id", Type: &DataType{Name: "INT"}, AutoIncr: true},
		},
		{
			name: "column with default",
			col:  &ColumnDef{Name: "status", Type: &DataType{Name: "INT"}, Default: &Literal{Value: 0, Type: LiteralNumber}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.col.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestTableConstraint_String tests the String method for TableConstraint
func TestTableConstraint_String(t *testing.T) {
	tests := []struct {
		name       string
		constraint *TableConstraint
	}{
		{
			name:       "primary key",
			constraint: &TableConstraint{Type: ConstraintPrimaryKey, Columns: []string{"id"}},
		},
		{
			name:       "unique constraint",
			constraint: &TableConstraint{Type: ConstraintUnique, Name: "uq_email", Columns: []string{"email"}},
		},
		{
			name: "foreign key",
			constraint: &TableConstraint{
				Type:       ConstraintForeignKey,
				Name:       "fk_user",
				Columns:    []string{"user_id"},
				RefTable:   "users",
				RefColumns: []string{"id"},
			},
		},
		{
			name: "check constraint",
			constraint: &TableConstraint{
				Type:      ConstraintCheck,
				Name:      "chk_age",
				CheckExpr: &BinaryExpr{Left: &ColumnRef{Name: "age"}, Op: OpGt, Right: &Literal{Value: 0, Type: LiteralNumber}},
			},
		},
		{
			name: "foreign key with on delete",
			constraint: &TableConstraint{
				Type:       ConstraintForeignKey,
				Name:       "fk_order",
				Columns:    []string{"order_id"},
				RefTable:   "orders",
				RefColumns: []string{"id"},
				OnDelete:   "CASCADE",
				OnUpdate:   "RESTRICT",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.constraint.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestExpression_String tests the String method for expressions
func TestExpression_String(t *testing.T) {
	tests := []struct {
		name string
		expr Expression
	}{
		{
			name: "literal null",
			expr: &Literal{Type: LiteralNull},
		},
		{
			name: "literal string",
			expr: &Literal{Value: "hello", Type: LiteralString},
		},
		{
			name: "literal number",
			expr: &Literal{Value: 123, Type: LiteralNumber},
		},
		{
			name: "literal bool",
			expr: &Literal{Value: true, Type: LiteralBool},
		},
		{
			name: "column ref",
			expr: &ColumnRef{Name: "id"},
		},
		{
			name: "column ref with table",
			expr: &ColumnRef{Table: "users", Name: "id"},
		},
		{
			name: "binary expr",
			expr: &BinaryExpr{
				Left:  &ColumnRef{Name: "age"},
				Op:    OpGt,
				Right: &Literal{Value: 18, Type: LiteralNumber},
			},
		},
		{
			name: "unary expr not",
			expr: &UnaryExpr{Op: OpNot, Right: &Literal{Value: true, Type: LiteralBool}},
		},
		{
			name: "unary expr neg",
			expr: &UnaryExpr{Op: OpNeg, Right: &Literal{Value: 5, Type: LiteralNumber}},
		},
		{
			name: "function call",
			expr: &FunctionCall{
				Name: "COUNT",
				Args: []Expression{&StarExpr{}},
			},
		},
		{
			name: "function call distinct",
			expr: &FunctionCall{
				Name:     "COUNT",
				Args:     []Expression{&ColumnRef{Name: "id"}},
				Distinct: true,
			},
		},
		{
			name: "star expr",
			expr: &StarExpr{},
		},
		{
			name: "star expr with table",
			expr: &StarExpr{Table: "users"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.expr.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestFromClause_String tests the String method for FromClause
func TestFromClause_String(t *testing.T) {
	tests := []struct {
		name string
		from *FromClause
	}{
		{
			name: "simple table",
			from: &FromClause{Table: &TableRef{Name: "users"}},
		},
		{
			name: "table with alias",
			from: &FromClause{Table: &TableRef{Name: "users", Alias: "u"}},
		},
		{
			name: "join",
			from: &FromClause{
				Table: &TableRef{Name: "users"},
				Joins: []*JoinClause{
					{
						Type:  JoinInner,
						Table: &TableRef{Name: "orders"},
						On: &BinaryExpr{
							Left:  &ColumnRef{Table: "users", Name: "id"},
							Op:    OpEq,
							Right: &ColumnRef{Table: "orders", Name: "user_id"},
						},
					},
				},
			},
		},
		{
			name: "join with using",
			from: &FromClause{
				Table: &TableRef{Name: "users"},
				Joins: []*JoinClause{
					{
						Type:  JoinLeft,
						Table: &TableRef{Name: "orders"},
						Using: []string{"user_id"},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.from.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestOrderByItem_String tests the String method for OrderByItem
func TestOrderByItem_String(t *testing.T) {
	tests := []struct {
		name string
		item *OrderByItem
		want string
	}{
		{
			name: "asc order",
			item: &OrderByItem{Expr: &ColumnRef{Name: "id"}, Ascending: true},
			want: "id ASC",
		},
		{
			name: "desc order",
			item: &OrderByItem{Expr: &ColumnRef{Name: "name"}, Ascending: false},
			want: "name DESC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.item.String()
			if result != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, result)
			}
		})
	}
}

// TestBetweenExpr_String tests the String method for BetweenExpr
func TestBetweenExpr_String(t *testing.T) {
	tests := []struct {
		name string
		expr *BetweenExpr
	}{
		{
			name: "between",
			expr: &BetweenExpr{
				Expr:  &ColumnRef{Name: "age"},
				Left:  &Literal{Value: 18, Type: LiteralNumber},
				Right: &Literal{Value: 65, Type: LiteralNumber},
				Not:   false,
			},
		},
		{
			name: "not between",
			expr: &BetweenExpr{
				Expr:  &ColumnRef{Name: "age"},
				Left:  &Literal{Value: 0, Type: LiteralNumber},
				Right: &Literal{Value: 17, Type: LiteralNumber},
				Not:   true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.expr.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestCaseExpr_String tests the String method for CaseExpr
func TestCaseExpr_String(t *testing.T) {
	expr := &CaseExpr{
		Expr: &ColumnRef{Name: "status"},
		Whens: []*CaseWhen{
			{Condition: &Literal{Value: 1, Type: LiteralNumber}, Result: &Literal{Value: "active", Type: LiteralString}},
		},
		Else: &Literal{Value: "unknown", Type: LiteralString},
	}
	result := expr.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestInExpr_String tests the String method for InExpr
func TestInExpr_String(t *testing.T) {
	tests := []struct {
		name string
		expr *InExpr
	}{
		{
			name: "in list",
			expr: &InExpr{
				Expr: &ColumnRef{Name: "id"},
				List: []Expression{
					&Literal{Value: 1, Type: LiteralNumber},
					&Literal{Value: 2, Type: LiteralNumber},
					&Literal{Value: 3, Type: LiteralNumber},
				},
				Not: false,
			},
		},
		{
			name: "not in list",
			expr: &InExpr{
				Expr: &ColumnRef{Name: "status"},
				List: []Expression{
					&Literal{Value: "deleted", Type: LiteralString},
				},
				Not: true,
			},
		},
		{
			name: "in subquery",
			expr: &InExpr{
				Expr: &ColumnRef{Name: "user_id"},
				Select: &SelectStmt{
					Columns: []Expression{&ColumnRef{Name: "id"}},
					From:    &FromClause{Table: &TableRef{Name: "admins"}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.expr.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestIsNullExpr_String tests the String method for IsNullExpr
func TestIsNullExpr_String(t *testing.T) {
	tests := []struct {
		name string
		expr *IsNullExpr
	}{
		{
			name: "is null",
			expr: &IsNullExpr{Expr: &ColumnRef{Name: "deleted_at"}, Not: false},
		},
		{
			name: "is not null",
			expr: &IsNullExpr{Expr: &ColumnRef{Name: "name"}, Not: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.expr.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestDataType_String tests the String method for DataType
func TestDataType_String(t *testing.T) {
	tests := []struct {
		name string
		dt   *DataType
		want string
	}{
		{
			name: "simple type",
			dt:   &DataType{Name: "INT"},
			want: "INT",
		},
		{
			name: "varchar with size",
			dt:   &DataType{Name: "VARCHAR", Size: 255},
			want: "VARCHAR(255)",
		},
		{
			name: "decimal with precision and scale",
			dt:   &DataType{Name: "DECIMAL", Precision: 10, Scale: 2},
			want: "DECIMAL(10,2)",
		},
		{
			name: "int unsigned",
			dt:   &DataType{Name: "INT", Unsigned: true},
			want: "INT UNSIGNED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.dt.String()
			if result != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, result)
			}
		})
	}
}

// TestTableRef_String tests the String method for TableRef
func TestTableRef_String(t *testing.T) {
	tests := []struct {
		name string
		ref  *TableRef
		want string
	}{
		{
			name: "simple table",
			ref:  &TableRef{Name: "users"},
			want: "users",
		},
		{
			name: "table with alias",
			ref:  &TableRef{Name: "users", Alias: "u"},
			want: "users AS u",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.ref.String()
			if result != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, result)
			}
		})
	}
}

// TestAssignment_String tests the String method for Assignment
func TestAssignment_String(t *testing.T) {
	assign := &Assignment{
		Column: "name",
		Value:  &Literal{Value: "test", Type: LiteralString},
	}
	result := assign.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestBinaryExpr_AllOperators tests all binary operators
func TestBinaryExpr_AllOperators(t *testing.T) {
	ops := []struct {
		op   BinaryOp
		name string
	}{
		{OpEq, "="},
		{OpNe, "<>"},
		{OpLt, "<"},
		{OpLe, "<="},
		{OpGt, ">"},
		{OpGe, ">="},
		{OpAdd, "+"},
		{OpSub, "-"},
		{OpMul, "*"},
		{OpDiv, "/"},
		{OpMod, "%"},
		{OpAnd, "AND"},
		{OpOr, "OR"},
		{OpLike, "LIKE"},
		{OpNotLike, "NOT LIKE"},
		{OpConcat, "||"},
	}

	for _, tt := range ops {
		t.Run(tt.name, func(t *testing.T) {
			expr := &BinaryExpr{
				Left:  &Literal{Value: 1, Type: LiteralNumber},
				Op:    tt.op,
				Right: &Literal{Value: 2, Type: LiteralNumber},
			}
			result := expr.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestJoinClause_AllTypes tests all join types
func TestJoinClause_AllTypes(t *testing.T) {
	types := []struct {
		jt   JoinType
		name string
	}{
		{JoinInner, "INNER"},
		{JoinLeft, "LEFT"},
		{JoinRight, "RIGHT"},
		{JoinCross, "CROSS"},
	}

	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			join := &JoinClause{
				Type:  tt.jt,
				Table: &TableRef{Name: "orders"},
			}
			result := join.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}