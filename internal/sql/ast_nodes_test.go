package sql

import (
	"testing"
)

// TestCTEDefinition_String tests CTE definition String output
func TestCTEDefinition_String(t *testing.T) {
	tests := []struct {
		name string
		cte  CTEDefinition
	}{
		{
			name: "simple cte",
			cte: CTEDefinition{
				Name:  "cte1",
				Query: &SelectStmt{Columns: []Expression{&StarExpr{}}},
			},
		},
		{
			name: "cte with columns",
			cte: CTEDefinition{
				Name:    "cte2",
				Columns: []string{"a", "b"},
				Query:   &SelectStmt{Columns: []Expression{&StarExpr{}}},
			},
		},
		{
			name: "recursive cte",
			cte: CTEDefinition{
				Name:      "recursive_cte",
				Recursive: true,
				Query:     &SelectStmt{Columns: []Expression{&StarExpr{}}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.cte.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestWithStmt_String tests WITH statement String output
func TestWithStmt_String(t *testing.T) {
	stmt := &WithStmt{
		CTEs: []CTEDefinition{
			{Name: "cte1", Query: &SelectStmt{Columns: []Expression{&StarExpr{}}}},
		},
		MainQuery: &SelectStmt{Columns: []Expression{&StarExpr{}}},
	}
	result := stmt.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestWithClause_String tests WITH clause String output
func TestWithClause_String(t *testing.T) {
	tests := []struct {
		name   string
		clause WithClause
	}{
		{
			name: "simple with clause",
			clause: WithClause{
				CTEs: []CTEDefinition{
					{Name: "cte1", Query: &SelectStmt{Columns: []Expression{&StarExpr{}}}},
				},
			},
		},
		{
			name: "recursive with clause",
			clause: WithClause{
				Recursive: true,
				CTEs: []CTEDefinition{
					{Name: "cte1", Query: &SelectStmt{Columns: []Expression{&StarExpr{}}}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.clause.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestUpsertClause_String tests upsert clause String output
func TestUpsertClause_String(t *testing.T) {
	tests := []struct {
		name   string
		clause UpsertClause
	}{
		{
			name: "do nothing",
			clause: UpsertClause{
				ConflictColumns: []string{"id"},
				DoNothing:       true,
			},
		},
		{
			name: "do update",
			clause: UpsertClause{
				ConflictColumns: []string{"id"},
				DoUpdate:        true,
				Assignments: []*Assignment{
					{Column: "name", Value: &Literal{Value: "test", Type: LiteralString}},
				},
			},
		},
		{
			name: "do update with where",
			clause: UpsertClause{
				ConflictColumns: []string{"id"},
				DoUpdate:        true,
				Assignments: []*Assignment{
					{Column: "name", Value: &Literal{Value: "test", Type: LiteralString}},
				},
				Where: &BinaryExpr{Left: &ColumnRef{Name: "status"}, Op: OpEq, Right: &Literal{Value: "active", Type: LiteralString}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.clause.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestReturningClause_String tests returning clause String output
func TestReturningClause_String(t *testing.T) {
	tests := []struct {
		name   string
		clause ReturningClause
	}{
		{
			name:   "returning all",
			clause: ReturningClause{All: true},
		},
		{
			name: "returning specific columns",
			clause: ReturningClause{
				Columns: []Expression{&ColumnRef{Name: "id"}, &ColumnRef{Name: "name"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.clause.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestCreateViewStmt_String tests CREATE VIEW statement
func TestCreateViewStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *CreateViewStmt
	}{
		{
			name: "simple view",
			stmt: &CreateViewStmt{
				ViewName:   "v_users",
				SelectStmt: &SelectStmt{Columns: []Expression{&StarExpr{}}},
			},
		},
		{
			name: "view with columns",
			stmt: &CreateViewStmt{
				ViewName:   "v_users",
				Columns:    []string{"id", "name"},
				SelectStmt: &SelectStmt{Columns: []Expression{&StarExpr{}}},
			},
		},
		{
			name: "create or replace view",
			stmt: &CreateViewStmt{
				ViewName:    "v_users",
				OrReplace:   true,
				SelectStmt:  &SelectStmt{Columns: []Expression{&StarExpr{}}},
				CheckOption: "CASCADED",
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

// TestDropViewStmt_String tests DROP VIEW statement
func TestDropViewStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *DropViewStmt
	}{
		{
			name: "drop view",
			stmt: &DropViewStmt{ViewName: "v_users"},
		},
		{
			name: "drop view if exists",
			stmt: &DropViewStmt{ViewName: "v_users", IfExists: true},
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

// TestExplainStmt_String tests EXPLAIN statement
func TestExplainStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *ExplainStmt
	}{
		{
			name: "explain",
			stmt: &ExplainStmt{
				Statement: &SelectStmt{Columns: []Expression{&StarExpr{}}},
			},
		},
		{
			name: "explain query plan",
			stmt: &ExplainStmt{
				Statement: &SelectStmt{Columns: []Expression{&StarExpr{}}},
				QueryPlan: true,
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

// TestUnionStmt_String tests UNION statement
func TestUnionStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *UnionStmt
	}{
		{
			name: "union",
			stmt: &UnionStmt{
				Left:  &SelectStmt{Columns: []Expression{&ColumnRef{Name: "id"}}},
				Right: &SelectStmt{Columns: []Expression{&ColumnRef{Name: "id"}}},
				Op:    SetUnion,
			},
		},
		{
			name: "union all",
			stmt: &UnionStmt{
				Left:  &SelectStmt{Columns: []Expression{&ColumnRef{Name: "id"}}},
				Right: &SelectStmt{Columns: []Expression{&ColumnRef{Name: "id"}}},
				Op:    SetUnion,
				All:   true,
			},
		},
		{
			name: "intersect",
			stmt: &UnionStmt{
				Left:  &SelectStmt{Columns: []Expression{&ColumnRef{Name: "id"}}},
				Right: &SelectStmt{Columns: []Expression{&ColumnRef{Name: "id"}}},
				Op:    SetIntersect,
			},
		},
		{
			name: "except",
			stmt: &UnionStmt{
				Left:  &SelectStmt{Columns: []Expression{&ColumnRef{Name: "id"}}},
				Right: &SelectStmt{Columns: []Expression{&ColumnRef{Name: "id"}}},
				Op:    SetExcept,
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

// TestSetOperation_String tests SetOperation enum String output
func TestSetOperation_String(t *testing.T) {
	tests := []struct {
		op   SetOperation
		want string
	}{
		{SetUnion, "UNION"},
		{SetIntersect, "INTERSECT"},
		{SetExcept, "EXCEPT"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			result := tt.op.String()
			if result != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, result)
			}
		})
	}
}

// TestAnalyzeStmt_String tests ANALYZE statement
func TestAnalyzeStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *AnalyzeStmt
		want string
	}{
		{
			name: "analyze all",
			stmt: &AnalyzeStmt{},
			want: "ANALYZE",
		},
		{
			name: "analyze table",
			stmt: &AnalyzeStmt{TableName: "users"},
			want: "ANALYZE TABLE users",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.stmt.String()
			if result != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, result)
			}
		})
	}
}

// TestWindowSpec_String tests window specification
func TestWindowSpec_String(t *testing.T) {
	tests := []struct {
		name string
		spec *WindowSpec
	}{
		{
			name: "empty window",
			spec: &WindowSpec{},
		},
		{
			name: "partition by",
			spec: &WindowSpec{
				PartitionBy: []Expression{&ColumnRef{Name: "dept"}},
			},
		},
		{
			name: "order by",
			spec: &WindowSpec{
				OrderBy: []*OrderByItem{{Expr: &ColumnRef{Name: "salary"}, Ascending: false}},
			},
		},
		{
			name: "partition and order by",
			spec: &WindowSpec{
				PartitionBy: []Expression{&ColumnRef{Name: "dept"}},
				OrderBy:     []*OrderByItem{{Expr: &ColumnRef{Name: "salary"}, Ascending: false}},
			},
		},
		{
			name: "with frame",
			spec: &WindowSpec{
				OrderBy: []*OrderByItem{{Expr: &ColumnRef{Name: "date"}, Ascending: true}},
				Frame: &FrameSpec{
					Mode:  "ROWS",
					Start: FrameBound{Type: "UNBOUNDED PRECEDING"},
					End:   FrameBound{Type: "CURRENT ROW"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.spec.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestFrameSpec_String tests frame specification
func TestFrameSpec_String(t *testing.T) {
	spec := &FrameSpec{
		Mode:  "ROWS",
		Start: FrameBound{Type: "UNBOUNDED PRECEDING"},
		End:   FrameBound{Type: "UNBOUNDED FOLLOWING"},
	}
	result := spec.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestFrameBound_String tests frame bound
func TestFrameBound_String(t *testing.T) {
	tests := []struct {
		name  string
		bound FrameBound
		want  string
	}{
		{"unbounded preceding", FrameBound{Type: "UNBOUNDED PRECEDING"}, "UNBOUNDED PRECEDING"},
		{"unbounded following", FrameBound{Type: "UNBOUNDED FOLLOWING"}, "UNBOUNDED FOLLOWING"},
		{"current row", FrameBound{Type: "CURRENT ROW"}, "CURRENT ROW"},
		{"preceding", FrameBound{Type: "PRECEDING", Offset: 5}, "5 PRECEDING"},
		{"following", FrameBound{Type: "FOLLOWING", Offset: 3}, "3 FOLLOWING"},
		{"unknown", FrameBound{Type: "UNKNOWN"}, "UNKNOWN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.bound.String()
			if result != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, result)
			}
		})
	}
}

// TestWindowFuncCall_String tests window function call
func TestWindowFuncCall_String(t *testing.T) {
	tests := []struct {
		name string
		call *WindowFuncCall
	}{
		{
			name: "simple window func",
			call: &WindowFuncCall{
				Func:   &FunctionCall{Name: "ROW_NUMBER", Args: []Expression{}},
				Window: &WindowSpec{},
			},
		},
		{
			name: "ignore nulls",
			call: &WindowFuncCall{
				Func:        &FunctionCall{Name: "FIRST_VALUE", Args: []Expression{&ColumnRef{Name: "val"}}},
				Window:      &WindowSpec{},
				IgnoreNulls: true,
			},
		},
		{
			name: "respect nulls",
			call: &WindowFuncCall{
				Func:         &FunctionCall{Name: "LAST_VALUE", Args: []Expression{&ColumnRef{Name: "val"}}},
				Window:       &WindowSpec{},
				RespectNulls: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.call.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestParenExpr_String tests parenthesized expression
func TestParenExpr_String(t *testing.T) {
	expr := &ParenExpr{Expr: &BinaryExpr{Left: &Literal{Value: 1, Type: LiteralNumber}, Op: OpAdd, Right: &Literal{Value: 2, Type: LiteralNumber}}}
	result := expr.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestCollateExpr_String tests COLLATE expression
func TestCollateExpr_String(t *testing.T) {
	expr := &CollateExpr{Expr: &ColumnRef{Name: "name"}, Collate: "utf8_general_ci"}
	result := expr.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestSubqueryExpr_String tests subquery expression
func TestSubqueryExpr_String(t *testing.T) {
	expr := &SubqueryExpr{
		Select: &SelectStmt{
			Columns: []Expression{&ColumnRef{Name: "id"}},
			From:    &FromClause{Table: &TableRef{Name: "users"}},
		},
	}
	result := expr.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestValuesExpr_String tests VALUES expression
func TestValuesExpr_String(t *testing.T) {
	expr := &ValuesExpr{
		Rows: [][]Expression{
			{&Literal{Value: 1, Type: LiteralNumber}, &Literal{Value: "a", Type: LiteralString}},
			{&Literal{Value: 2, Type: LiteralNumber}, &Literal{Value: "b", Type: LiteralString}},
		},
	}
	result := expr.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestExistsExpr_String tests EXISTS expression
func TestExistsExpr_String(t *testing.T) {
	tests := []struct {
		name string
		expr *ExistsExpr
	}{
		{
			name: "exists",
			expr: &ExistsExpr{
				Subquery: &SubqueryExpr{Select: &SelectStmt{Columns: []Expression{&StarExpr{}}}},
			},
		},
		{
			name: "not exists",
			expr: &ExistsExpr{
				Subquery: &SubqueryExpr{Select: &SelectStmt{Columns: []Expression{&StarExpr{}}}},
				Not:      true,
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

// TestAnyAllExpr_String tests ANY/ALL expression
func TestAnyAllExpr_String(t *testing.T) {
	tests := []struct {
		name string
		expr *AnyAllExpr
	}{
		{
			name: "any",
			expr: &AnyAllExpr{
				Left:     &ColumnRef{Name: "x"},
				Op:       OpGt,
				IsAny:    true,
				Subquery: &SubqueryExpr{Select: &SelectStmt{Columns: []Expression{&ColumnRef{Name: "val"}}}},
			},
		},
		{
			name: "all",
			expr: &AnyAllExpr{
				Left:     &ColumnRef{Name: "x"},
				Op:       OpEq,
				IsAny:    false,
				Subquery: &SubqueryExpr{Select: &SelectStmt{Columns: []Expression{&ColumnRef{Name: "val"}}}},
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

// TestScalarSubquery_String tests scalar subquery
func TestScalarSubquery_String(t *testing.T) {
	expr := &ScalarSubquery{
		Subquery: &SubqueryExpr{
			Select: &SelectStmt{Columns: []Expression{&FunctionCall{Name: "COUNT", Args: []Expression{&StarExpr{}}}}},
		},
	}
	result := expr.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestCastExpr_String tests CAST expression
func TestCastExpr_String(t *testing.T) {
	expr := &CastExpr{
		Expr: &ColumnRef{Name: "age"},
		Type: &DataType{Name: "VARCHAR", Size: 10},
	}
	result := expr.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestPrivilegeType_String tests all privilege types
func TestPrivilegeType_String(t *testing.T) {
	tests := []struct {
		pt   PrivilegeType
		want string
	}{
		{PrivAll, "ALL"},
		{PrivSelect, "SELECT"},
		{PrivInsert, "INSERT"},
		{PrivUpdate, "UPDATE"},
		{PrivDelete, "DELETE"},
		{PrivCreate, "CREATE"},
		{PrivDrop, "DROP"},
		{PrivIndex, "INDEX"},
		{PrivAlter, "ALTER"},
		{PrivUsage, "USAGE"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			result := tt.pt.String()
			if result != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, result)
			}
		})
	}
}

// TestPrivilege_String tests privilege with columns
func TestPrivilege_String(t *testing.T) {
	tests := []struct {
		name string
		priv *Privilege
	}{
		{
			name: "simple privilege",
			priv: &Privilege{Type: PrivSelect},
		},
		{
			name: "column privilege",
			priv: &Privilege{Type: PrivUpdate, Columns: []string{"name", "email"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.priv.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestCreateUserStmt_String tests CREATE USER statement
func TestCreateUserStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *CreateUserStmt
	}{
		{
			name: "simple user",
			stmt: &CreateUserStmt{Username: "alice", Identified: "secret123"},
		},
		{
			name: "user with host",
			stmt: &CreateUserStmt{Username: "alice", Host: "localhost", Identified: "secret123"},
		},
		{
			name: "user if not exists",
			stmt: &CreateUserStmt{IfNotExists: true, Username: "bob", Identified: "pass"},
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

// TestDropUserStmt_String tests DROP USER statement
func TestDropUserStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *DropUserStmt
	}{
		{
			name: "drop user",
			stmt: &DropUserStmt{Username: "alice"},
		},
		{
			name: "drop user with host",
			stmt: &DropUserStmt{Username: "alice", Host: "localhost"},
		},
		{
			name: "drop user if exists",
			stmt: &DropUserStmt{IfExists: true, Username: "bob"},
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

// TestAlterUserStmt_String tests ALTER USER statement
func TestAlterUserStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *AlterUserStmt
	}{
		{
			name: "alter user",
			stmt: &AlterUserStmt{Username: "alice", Identified: "newpass"},
		},
		{
			name: "alter user with host",
			stmt: &AlterUserStmt{Username: "alice", Host: "localhost", Identified: "newpass"},
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

// TestGrantStmt_String tests GRANT statement
func TestGrantStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *GrantStmt
	}{
		{
			name: "grant on all",
			stmt: &GrantStmt{
				Privileges: []*Privilege{{Type: PrivSelect}},
				On:         GrantOnAll,
				To:         "alice",
			},
		},
		{
			name: "grant on database",
			stmt: &GrantStmt{
				Privileges: []*Privilege{{Type: PrivSelect}, {Type: PrivInsert}},
				On:         GrantOnDatabase,
				Database:   "mydb",
				To:         "alice",
			},
		},
		{
			name: "grant on table",
			stmt: &GrantStmt{
				Privileges: []*Privilege{{Type: PrivSelect}},
				On:         GrantOnTable,
				Database:   "mydb",
				Table:      "users",
				To:         "alice",
			},
		},
		{
			name: "grant with grant option",
			stmt: &GrantStmt{
				Privileges: []*Privilege{{Type: PrivAll}},
				On:         GrantOnAll,
				To:         "admin",
				WithGrant:  true,
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

// TestRevokeStmt_String tests REVOKE statement
func TestRevokeStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *RevokeStmt
	}{
		{
			name: "revoke on all",
			stmt: &RevokeStmt{
				Privileges: []*Privilege{{Type: PrivSelect}},
				On:         GrantOnAll,
				From:       "alice",
			},
		},
		{
			name: "revoke on table",
			stmt: &RevokeStmt{
				Privileges: []*Privilege{{Type: PrivDelete}},
				On:         GrantOnTable,
				Table:      "users",
				From:       "bob",
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

// TestShowGrantsStmt_String tests SHOW GRANTS statement
func TestShowGrantsStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *ShowGrantsStmt
		want string
	}{
		{
			name: "show grants",
			stmt: &ShowGrantsStmt{},
			want: "SHOW GRANTS",
		},
		{
			name: "show grants for user",
			stmt: &ShowGrantsStmt{ForUser: "alice"},
			want: "SHOW GRANTS FOR alice",
		},
		{
			name: "show grants for user@host",
			stmt: &ShowGrantsStmt{ForUser: "alice", ForHost: "localhost"},
			want: "SHOW GRANTS FOR alice@localhost",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.stmt.String()
			if result != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, result)
			}
		})
	}
}

// TestSetPasswordStmt_String tests SET PASSWORD statement
func TestSetPasswordStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *SetPasswordStmt
	}{
		{
			name: "set password",
			stmt: &SetPasswordStmt{Password: "newpass"},
		},
		{
			name: "set password for user",
			stmt: &SetPasswordStmt{ForUser: "alice", Password: "newpass"},
		},
		{
			name: "set password for user@host",
			stmt: &SetPasswordStmt{ForUser: "alice", ForHost: "localhost", Password: "newpass"},
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

// TestBackupStmt_String tests BACKUP statement
func TestBackupStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *BackupStmt
	}{
		{
			name: "backup",
			stmt: &BackupStmt{Path: "/backup/db.bak"},
		},
		{
			name: "backup with compress",
			stmt: &BackupStmt{Path: "/backup/db.bak", Compress: true},
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

// TestRestoreStmt_String tests RESTORE statement
func TestRestoreStmt_String(t *testing.T) {
	stmt := &RestoreStmt{Path: "/backup/db.bak"}
	result := stmt.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestVacuumStmt_String tests VACUUM statement
func TestVacuumStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *VacuumStmt
	}{
		{
			name: "vacuum entire",
			stmt: &VacuumStmt{},
		},
		{
			name: "vacuum table",
			stmt: &VacuumStmt{Table: "users"},
		},
		{
			name: "vacuum into",
			stmt: &VacuumStmt{Table: "users", IntoPath: "/backup/vacuum.db"},
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

// TestPragmaStmt_String tests PRAGMA statement
func TestPragmaStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *PragmaStmt
	}{
		{
			name: "pragma query",
			stmt: &PragmaStmt{Name: "journal_mode"},
		},
		{
			name: "pragma set value",
			stmt: &PragmaStmt{Name: "journal_mode", Value: "wal"},
		},
		{
			name: "pragma with argument",
			stmt: &PragmaStmt{Name: "table_info", Argument: "users"},
		},
		{
			name: "pragma with argument and value",
			stmt: &PragmaStmt{Name: "user_version", Argument: "main", Value: 1},
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

// TestIfExpr_String tests IF expression
func TestIfExpr_String(t *testing.T) {
	tests := []struct {
		name string
		expr *IfExpr
	}{
		{
			name: "if then",
			expr: &IfExpr{
				Condition: &BinaryExpr{Left: &ColumnRef{Name: "x"}, Op: OpGt, Right: &Literal{Value: 0, Type: LiteralNumber}},
				ThenExpr:  &Literal{Value: "positive", Type: LiteralString},
			},
		},
		{
			name: "if then else",
			expr: &IfExpr{
				Condition: &BinaryExpr{Left: &ColumnRef{Name: "x"}, Op: OpGt, Right: &Literal{Value: 0, Type: LiteralNumber}},
				ThenExpr:  &Literal{Value: "positive", Type: LiteralString},
				ElseExpr:  &Literal{Value: "non-positive", Type: LiteralString},
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

// TestLetExpr_String tests LET expression
func TestLetExpr_String(t *testing.T) {
	expr := &LetExpr{
		Name:  "x",
		Value: &Literal{Value: 42, Type: LiteralNumber},
	}
	result := expr.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
}

// TestBlockExpr_String tests block expression
func TestBlockExpr_String(t *testing.T) {
	tests := []struct {
		name string
		expr *BlockExpr
	}{
		{
			name: "empty block",
			expr: &BlockExpr{},
		},
		{
			name: "block with expressions",
			expr: &BlockExpr{
				Expressions: []Expression{
					&LetExpr{Name: "x", Value: &Literal{Value: 1, Type: LiteralNumber}},
					&ColumnRef{Name: "x"},
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

// TestFunctionParameter_String tests function parameter
func TestFunctionParameter_String(t *testing.T) {
	tests := []struct {
		name  string
		param *FunctionParameter
	}{
		{
			name:  "simple param",
			param: &FunctionParameter{Name: "x", Type: &DataType{Name: "INT"}},
		},
		{
			name: "param with default",
			param: &FunctionParameter{
				Name:         "limit",
				Type:         &DataType{Name: "INT"},
				DefaultValue: &Literal{Value: 10, Type: LiteralNumber},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.param.String()
			if result == "" {
				t.Error("String() returned empty string")
			}
		})
	}
}

// TestCreateFunctionStmt_String tests CREATE FUNCTION statement
func TestCreateFunctionStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *CreateFunctionStmt
	}{
		{
			name: "simple function",
			stmt: &CreateFunctionStmt{
				Name:       "add_one",
				Parameters: []*FunctionParameter{{Name: "x", Type: &DataType{Name: "INT"}}},
				ReturnType: &DataType{Name: "INT"},
				Body:       &BinaryExpr{Left: &ColumnRef{Name: "x"}, Op: OpAdd, Right: &Literal{Value: 1, Type: LiteralNumber}},
			},
		},
		{
			name: "function with script",
			stmt: &CreateFunctionStmt{
				Name:       "hello",
				Parameters: []*FunctionParameter{{Name: "name", Type: &DataType{Name: "VARCHAR", Size: 100}}},
				ReturnType: &DataType{Name: "VARCHAR", Size: 100},
				Script:     "return 'Hello ' + name;",
			},
		},
		{
			name: "create or replace function",
			stmt: &CreateFunctionStmt{
				Name:       "add_one",
				Replace:    true,
				Parameters: []*FunctionParameter{{Name: "x", Type: &DataType{Name: "INT"}}},
				ReturnType: &DataType{Name: "INT"},
				Body:       &BinaryExpr{Left: &ColumnRef{Name: "x"}, Op: OpAdd, Right: &Literal{Value: 1, Type: LiteralNumber}},
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

// TestDropFunctionStmt_String tests DROP FUNCTION statement
func TestDropFunctionStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *DropFunctionStmt
	}{
		{
			name: "drop function",
			stmt: &DropFunctionStmt{Name: "add_one"},
		},
		{
			name: "drop function if exists",
			stmt: &DropFunctionStmt{Name: "add_one", IfExists: true},
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

// TestTriggerTiming_String tests trigger timing enum
func TestTriggerTiming_String(t *testing.T) {
	tests := []struct {
		timing TriggerTiming
		want   string
	}{
		{TriggerBefore, "BEFORE"},
		{TriggerAfter, "AFTER"},
		{TriggerInsteadOf, "INSTEAD OF"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			result := tt.timing.String()
			if result != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, result)
			}
		})
	}
}

// TestTriggerEvent_String tests trigger event enum
func TestTriggerEvent_String(t *testing.T) {
	tests := []struct {
		event TriggerEvent
		want  string
	}{
		{TriggerInsert, "INSERT"},
		{TriggerUpdate, "UPDATE"},
		{TriggerDelete, "DELETE"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			result := tt.event.String()
			if result != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, result)
			}
		})
	}
}

// TestTriggerGranularity_String tests trigger granularity enum
func TestTriggerGranularity_String(t *testing.T) {
	tests := []struct {
		gran TriggerGranularity
		want string
	}{
		{TriggerForEachRow, "FOR EACH ROW"},
		{TriggerForEachStatement, "FOR EACH STATEMENT"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			result := tt.gran.String()
			if result != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, result)
			}
		})
	}
}

// TestCreateTriggerStmt_String tests CREATE TRIGGER statement
func TestCreateTriggerStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *CreateTriggerStmt
	}{
		{
			name: "simple trigger",
			stmt: &CreateTriggerStmt{
				TriggerName: "trg_before_insert",
				Timing:      TriggerBefore,
				Event:       TriggerInsert,
				TableName:   "users",
				Granularity: TriggerForEachRow,
			},
		},
		{
			name: "trigger with when",
			stmt: &CreateTriggerStmt{
				TriggerName: "trg_check_age",
				Timing:      TriggerBefore,
				Event:       TriggerInsert,
				TableName:   "users",
				Granularity: TriggerForEachRow,
				WhenClause:  &BinaryExpr{Left: &ColumnRef{Name: "age"}, Op: OpLt, Right: &Literal{Value: 0, Type: LiteralNumber}},
			},
		},
		{
			name: "trigger if not exists",
			stmt: &CreateTriggerStmt{
				TriggerName: "trg_log",
				Timing:      TriggerAfter,
				Event:       TriggerUpdate,
				TableName:   "users",
				Granularity: TriggerForEachRow,
				IfNotExists: true,
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

// TestDropTriggerStmt_String tests DROP TRIGGER statement
func TestDropTriggerStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *DropTriggerStmt
	}{
		{
			name: "drop trigger",
			stmt: &DropTriggerStmt{TriggerName: "trg_before_insert"},
		},
		{
			name: "drop trigger with table",
			stmt: &DropTriggerStmt{TriggerName: "trg_before_insert", TableName: "users"},
		},
		{
			name: "drop trigger if exists",
			stmt: &DropTriggerStmt{TriggerName: "trg_before_insert", IfExists: true},
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

// TestBeginStmt_String tests BEGIN statement
func TestBeginStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *BeginStmt
		want string
	}{
		{
			name: "begin",
			stmt: &BeginStmt{},
			want: "BEGIN TRANSACTION",
		},
		{
			name: "begin deferred",
			stmt: &BeginStmt{TransactionType: "DEFERRED"},
			want: "BEGIN DEFERRED TRANSACTION",
		},
		{
			name: "begin immediate",
			stmt: &BeginStmt{TransactionType: "IMMEDIATE"},
			want: "BEGIN IMMEDIATE TRANSACTION",
		},
		{
			name: "begin exclusive",
			stmt: &BeginStmt{TransactionType: "EXCLUSIVE"},
			want: "BEGIN EXCLUSIVE TRANSACTION",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.stmt.String()
			if result != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, result)
			}
		})
	}
}

// TestCommitStmt_String tests COMMIT statement
func TestCommitStmt_String(t *testing.T) {
	stmt := &CommitStmt{}
	result := stmt.String()
	if result != "COMMIT TRANSACTION" {
		t.Errorf("Expected 'COMMIT TRANSACTION', got %q", result)
	}
}

// TestRollbackStmt_String tests ROLLBACK statement
func TestRollbackStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *RollbackStmt
		want string
	}{
		{
			name: "rollback",
			stmt: &RollbackStmt{},
			want: "ROLLBACK TRANSACTION",
		},
		{
			name: "rollback to savepoint",
			stmt: &RollbackStmt{ToSavepoint: "sp1"},
			want: "ROLLBACK TO SAVEPOINT sp1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.stmt.String()
			if result != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, result)
			}
		})
	}
}

// TestSavepointStmt_String tests SAVEPOINT statement
func TestSavepointStmt_String(t *testing.T) {
	stmt := &SavepointStmt{Name: "sp1"}
	result := stmt.String()
	if result != "SAVEPOINT sp1" {
		t.Errorf("Expected 'SAVEPOINT sp1', got %q", result)
	}
}

// TestReleaseSavepointStmt_String tests RELEASE SAVEPOINT statement
func TestReleaseSavepointStmt_String(t *testing.T) {
	stmt := &ReleaseSavepointStmt{Name: "sp1"}
	result := stmt.String()
	if result != "RELEASE SAVEPOINT sp1" {
		t.Errorf("Expected 'RELEASE SAVEPOINT sp1', got %q", result)
	}
}

// TestCopyStmt_String tests COPY statement
func TestCopyStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *CopyStmt
	}{
		{
			name: "copy from",
			stmt: &CopyStmt{
				TableName: "users",
				FileName:  "/data/users.csv",
				Direction: "FROM",
				Format:    "csv",
				Header:    true,
			},
		},
		{
			name: "copy query to",
			stmt: &CopyStmt{
				Query:     &SelectStmt{Columns: []Expression{&StarExpr{}}},
				FileName:  "/data/output.csv",
				Direction: "TO",
				Format:    "csv",
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

// TestLoadDataStmt_String tests LOAD DATA statement
func TestLoadDataStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *LoadDataStmt
	}{
		{
			name: "simple load data",
			stmt: &LoadDataStmt{
				FileName:  "/data/users.csv",
				TableName: "users",
			},
		},
		{
			name: "load data with options",
			stmt: &LoadDataStmt{
				FileName:         "/data/users.csv",
				TableName:        "users",
				FieldsTerminated: ",",
				FieldsEnclosed:   "\"",
				LinesTerminated:  "\n",
				IgnoreRows:       1,
			},
		},
		{
			name: "load data with columns",
			stmt: &LoadDataStmt{
				FileName:   "/data/users.csv",
				TableName:  "users",
				ColumnList: []string{"id", "name", "email"},
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

// TestCreateFTSStmt_String tests CREATE FTS INDEX statement
func TestCreateFTSStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *CreateFTSStmt
	}{
		{
			name: "simple fts index",
			stmt: &CreateFTSStmt{
				IndexName: "fts_users",
				TableName: "users",
				Columns:   []string{"name", "bio"},
			},
		},
		{
			name: "fts index with tokenizer",
			stmt: &CreateFTSStmt{
				IndexName: "fts_users",
				TableName: "users",
				Columns:   []string{"name", "bio"},
				Tokenizer: "porter",
			},
		},
		{
			name: "fts index if not exists",
			stmt: &CreateFTSStmt{
				IndexName:   "fts_users",
				TableName:   "users",
				Columns:     []string{"name"},
				IfNotExists: true,
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

// TestDropFTSStmt_String tests DROP FTS INDEX statement
func TestDropFTSStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *DropFTSStmt
	}{
		{
			name: "drop fts index",
			stmt: &DropFTSStmt{IndexName: "fts_users"},
		},
		{
			name: "drop fts index if exists",
			stmt: &DropFTSStmt{IndexName: "fts_users", IfExists: true},
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

// TestMatchExpr_String tests MATCH expression for full-text search
func TestMatchExpr_String(t *testing.T) {
	tests := []struct {
		name string
		expr *MatchExpr
	}{
		{
			name: "simple match",
			expr: &MatchExpr{Table: "users", Query: "john doe"},
		},
		{
			name: "match with columns",
			expr: &MatchExpr{Table: "users", Query: "john", Columns: []string{"name", "bio"}},
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

// TestRankExpr_String tests RANK expression
func TestRankExpr_String(t *testing.T) {
	tests := []struct {
		name string
		expr *RankExpr
		want string
	}{
		{
			name: "rank",
			expr: &RankExpr{},
			want: "RANK",
		},
		{
			name: "rank with index",
			expr: &RankExpr{IndexName: "fts_users"},
			want: "RANK(fts_users)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.expr.String()
			if result != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, result)
			}
		})
	}
}

// TestProcedureParamMode_String tests procedure parameter mode
func TestProcedureParamMode_String(t *testing.T) {
	tests := []struct {
		mode ProcedureParamMode
		want string
	}{
		{ParamModeIn, "IN"},
		{ParamModeOut, "OUT"},
		{ParamModeInout, "INOUT"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			result := tt.mode.String()
			if result != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, result)
			}
		})
	}
}

// TestCreateProcedureStmt_String tests CREATE PROCEDURE statement
func TestCreateProcedureStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *CreateProcedureStmt
	}{
		{
			name: "procedure with script",
			stmt: &CreateProcedureStmt{
				ProcedureName: "get_user",
				Params:        []ProcedureParam{{Name: "user_id", Type: "INT", Mode: ParamModeIn}},
				Body:          "return query('SELECT * FROM users WHERE id = ' + user_id);",
			},
		},
		{
			name: "procedure with sql body",
			stmt: &CreateProcedureStmt{
				ProcedureName: "log_action",
				Params:        []ProcedureParam{{Name: "msg", Type: "VARCHAR", Mode: ParamModeIn}},
				SQLBody:       []Statement{&InsertStmt{Table: "logs", Columns: []string{"message"}, Values: [][]Expression{{&ColumnRef{Name: "msg"}}}}},
			},
		},
		{
			name: "procedure if not exists",
			stmt: &CreateProcedureStmt{
				ProcedureName: "init_db",
				IfNotExists:   true,
				Body:          "print('initialized');",
			},
		},
		{
			name: "procedure with out param",
			stmt: &CreateProcedureStmt{
				ProcedureName: "get_count",
				Params:        []ProcedureParam{{Name: "cnt", Type: "INT", Mode: ParamModeOut}},
				Body:          "cnt = query('SELECT COUNT(*) FROM users');",
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

// TestDropProcedureStmt_String tests DROP PROCEDURE statement
func TestDropProcedureStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *DropProcedureStmt
	}{
		{
			name: "drop procedure",
			stmt: &DropProcedureStmt{ProcedureName: "get_user"},
		},
		{
			name: "drop procedure if exists",
			stmt: &DropProcedureStmt{ProcedureName: "get_user", IfExists: true},
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

// TestCallStmt_String tests CALL statement
func TestCallStmt_String(t *testing.T) {
	tests := []struct {
		name string
		stmt *CallStmt
	}{
		{
			name: "call no args",
			stmt: &CallStmt{ProcedureName: "init_db"},
		},
		{
			name: "call with args",
			stmt: &CallStmt{
				ProcedureName: "get_user",
				Args:          []Expression{&Literal{Value: 1, Type: LiteralNumber}},
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

// TestInterfaceMethods tests that all AST types properly implement their interfaces
func TestInterfaceMethods_Statement(t *testing.T) {
	stmts := []Statement{
		&SelectStmt{},
		&InsertStmt{},
		&UpdateStmt{},
		&DeleteStmt{},
		&CreateTableStmt{},
		&DropTableStmt{},
		&CreateIndexStmt{},
		&DropIndexStmt{},
		&DropViewStmt{},
		&AlterTableStmt{},
		&TruncateTableStmt{},
		&AnalyzeStmt{},
		&UseStmt{},
		&ShowStmt{},
		&DescribeStmt{},
		&ShowCreateTableStmt{},
		&CreateUserStmt{},
		&DropUserStmt{},
		&AlterUserStmt{},
		&ShowGrantsStmt{},
		&SetPasswordStmt{},
		&BackupStmt{},
		&RestoreStmt{},
		&VacuumStmt{},
		&PragmaStmt{},
		&DropFunctionStmt{},
		&DropTriggerStmt{},
		&BeginStmt{},
		&CommitStmt{},
		&RollbackStmt{},
		&SavepointStmt{},
		&ReleaseSavepointStmt{},
		&DropFTSStmt{},
		&DropProcedureStmt{},
	}
	for _, stmt := range stmts {
		stmt.node()
		stmt.statement()
		_ = stmt.String()
	}
}

func TestInterfaceMethods_Expression(t *testing.T) {
	exprs := []Expression{
		&Literal{Value: "test", Type: LiteralString},
		&ColumnRef{Name: "id"},
		&StarExpr{},
		&RankExpr{},
		&MatchExpr{Query: "test"},
	}
	for _, expr := range exprs {
		expr.node()
		expr.expression()
		_ = expr.String()
	}
}

func TestInterfaceMethods_AlterAction(t *testing.T) {
	actions := []AlterAction{
		&DropColumnAction{},
		&RenameColumnAction{},
		&RenameTableAction{},
		&DropConstraintAction{},
	}
	for _, action := range actions {
		action.node()
		action.alterAction()
		_ = action.String()
	}
}

func TestInterfaceMethods_NodeOnly(t *testing.T) {
	tests := []struct {
		node Node
	}{
		{&DataType{Name: "INT"}},
		{&OrderByItem{Expr: &ColumnRef{Name: "id"}, Ascending: true}},
		{&Assignment{Column: "x", Value: &Literal{Value: 1, Type: LiteralNumber}}},
		{&ColumnDef{Name: "id", Type: &DataType{Name: "INT"}}},
	}
	for _, tt := range tests {
		tt.node.node()
		_ = tt.node.String()
	}
}
