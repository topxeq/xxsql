package executor

import (
	"testing"

	"github.com/topxeq/xxsql/internal/sql"
	"github.com/topxeq/xxsql/internal/storage/row"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// TestExecuteSelectWithoutFrom tests SELECT without FROM clause
func TestExecuteSelectWithoutFrom(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		name           string
		stmt           *sql.SelectStmt
		expectedCols   int
		expectedRows   int
	}{
		{
			name: "select literal number",
			stmt: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.Literal{Value: 123, Type: sql.LiteralNumber}},
			},
			expectedCols: 1,
			expectedRows: 1,
		},
		{
			name: "select literal string",
			stmt: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.Literal{Value: "hello", Type: sql.LiteralString}},
			},
			expectedCols: 1,
			expectedRows: 1,
		},
		{
			name: "select column ref",
			stmt: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.ColumnRef{Name: "test"}},
			},
			expectedCols: 1,
			expectedRows: 1,
		},
		{
			name: "select NOW function",
			stmt: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.FunctionCall{Name: "NOW"}},
			},
			expectedCols: 1,
			expectedRows: 1,
		},
		{
			name: "select CURRENT_TIMESTAMP function",
			stmt: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.FunctionCall{Name: "CURRENT_TIMESTAMP"}},
			},
			expectedCols: 1,
			expectedRows: 1,
		},
		{
			name: "select DATABASE function",
			stmt: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.FunctionCall{Name: "DATABASE"}},
			},
			expectedCols: 1,
			expectedRows: 1,
		},
		{
			name: "select VERSION function",
			stmt: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.FunctionCall{Name: "VERSION"}},
			},
			expectedCols: 1,
			expectedRows: 1,
		},
		{
			name: "select unknown function",
			stmt: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.FunctionCall{Name: "UNKNOWN_FUNC"}},
			},
			expectedCols: 1,
			expectedRows: 1,
		},
		{
			name: "select star",
			stmt: &sql.SelectStmt{
				Columns: []sql.Expression{&sql.StarExpr{}},
			},
			expectedCols: 1,
			expectedRows: 1,
		},
		{
			name: "select multiple columns",
			stmt: &sql.SelectStmt{
				Columns: []sql.Expression{
					&sql.Literal{Value: 1, Type: sql.LiteralNumber},
					&sql.Literal{Value: "test", Type: sql.LiteralString},
					&sql.FunctionCall{Name: "NOW"},
				},
			},
			expectedCols: 3,
			expectedRows: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.executeSelectWithoutFrom(tt.stmt)
			if err != nil {
				t.Errorf("executeSelectWithoutFrom failed: %v", err)
				return
			}
			if len(result.Columns) != tt.expectedCols {
				t.Errorf("Expected %d columns, got %d", tt.expectedCols, len(result.Columns))
			}
			if result.RowCount != tt.expectedRows {
				t.Errorf("Expected %d rows, got %d", tt.expectedRows, result.RowCount)
			}
		})
	}
}

// TestEvaluateWhereForRow tests WHERE clause evaluation for rows
func TestEvaluateWhereForRow(t *testing.T) {
	exec := &Executor{}

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
		{Name: "name", Type: types.TypeVarchar},
		{Name: "active", Type: types.TypeBool},
	}
	colIdxMap := map[string]int{"id": 0, "name": 1, "active": 2}

	tests := []struct {
		name     string
		expr     sql.Expression
		row      *row.Row
		expected bool
	}{
		{
			name: "binary equals true",
			expr: &sql.BinaryExpr{
				Left:  &sql.ColumnRef{Name: "id"},
				Op:    sql.OpEq,
				Right: &sql.Literal{Value: int64(1), Type: sql.LiteralNumber},
			},
			row:      &row.Row{Values: []types.Value{types.NewIntValue(1)}},
			expected: true,
		},
		{
			name: "binary equals false",
			expr: &sql.BinaryExpr{
				Left:  &sql.ColumnRef{Name: "id"},
				Op:    sql.OpEq,
				Right: &sql.Literal{Value: int64(2), Type: sql.LiteralNumber},
			},
			row:      &row.Row{Values: []types.Value{types.NewIntValue(1)}},
			expected: false,
		},
		{
			name: "binary greater than true",
			expr: &sql.BinaryExpr{
				Left:  &sql.ColumnRef{Name: "id"},
				Op:    sql.OpGt,
				Right: &sql.Literal{Value: int64(0), Type: sql.LiteralNumber},
			},
			row:      &row.Row{Values: []types.Value{types.NewIntValue(1)}},
			expected: true,
		},
		{
			name: "binary less than true",
			expr: &sql.BinaryExpr{
				Left:  &sql.ColumnRef{Name: "id"},
				Op:    sql.OpLt,
				Right: &sql.Literal{Value: int64(10), Type: sql.LiteralNumber},
			},
			row:      &row.Row{Values: []types.Value{types.NewIntValue(1)}},
			expected: true,
		},
		{
			name: "is null true",
			expr: &sql.IsNullExpr{
				Expr: &sql.ColumnRef{Name: "name"},
				Not:  false,
			},
			row:      &row.Row{Values: []types.Value{types.NewIntValue(1), types.NewNullValue()}},
			expected: true,
		},
		{
			name: "is not null true",
			expr: &sql.IsNullExpr{
				Expr: &sql.ColumnRef{Name: "id"},
				Not:  true,
			},
			row:      &row.Row{Values: []types.Value{types.NewIntValue(1)}},
			expected: true,
		},
		{
			name: "not expression",
			expr: &sql.UnaryExpr{
				Op:    sql.OpNot,
				Right: &sql.Literal{Value: true, Type: sql.LiteralBool},
			},
			row:      &row.Row{Values: []types.Value{}},
			expected: false,
		},
		{
			name: "literal bool true",
			expr: &sql.Literal{
				Value: true,
				Type:  sql.LiteralBool,
			},
			row:      &row.Row{Values: []types.Value{}},
			expected: true,
		},
		{
			name: "literal bool false",
			expr: &sql.Literal{
				Value: false,
				Type:  sql.LiteralBool,
			},
			row:      &row.Row{Values: []types.Value{}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.evaluateWhereForRow(tt.expr, tt.row, columns, colIdxMap)
			if err != nil {
				t.Errorf("evaluateWhereForRow failed: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestCompareValues tests the compareValues function
func TestCompareValues(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		name     string
		left     interface{}
		op       sql.BinaryOp
		right    interface{}
		expected bool
		wantErr  bool
	}{
		{"int equal", int64(1), sql.OpEq, int64(1), true, false},
		{"int not equal", int64(1), sql.OpNe, int64(2), true, false},
		{"int less than", int64(1), sql.OpLt, int64(2), true, false},
		{"int less or equal", int64(1), sql.OpLe, int64(1), true, false},
		{"int greater than", int64(2), sql.OpGt, int64(1), true, false},
		{"int greater or equal", int64(2), sql.OpGe, int64(2), true, false},
		{"string equal", "a", sql.OpEq, "a", true, false},
		{"string not equal", "a", sql.OpNe, "b", true, false},
		{"float equal", 1.5, sql.OpEq, 1.5, true, false},
		{"float less than", 1.0, sql.OpLt, 2.0, true, false},
		{"nil values both nil eq", nil, sql.OpEq, nil, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.compareValues(tt.left, tt.op, tt.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("compareValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result != tt.expected {
				t.Errorf("compareValues() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestEvaluateCheckExpression tests CHECK constraint evaluation
func TestEvaluateCheckExpression(t *testing.T) {
	exec := &Executor{}

	colMap := map[string]int{"age": 0, "status": 1}

	tests := []struct {
		name     string
		expr     sql.Expression
		values   []types.Value
		expected bool
	}{
		{
			name: "age greater than zero",
			expr: &sql.BinaryExpr{
				Left:  &sql.ColumnRef{Name: "age"},
				Op:    sql.OpGt,
				Right: &sql.Literal{Value: int64(0), Type: sql.LiteralNumber},
			},
			values:   []types.Value{types.NewIntValue(25)},
			expected: true,
		},
		{
			name: "age less than 100",
			expr: &sql.BinaryExpr{
				Left:  &sql.ColumnRef{Name: "age"},
				Op:    sql.OpLt,
				Right: &sql.Literal{Value: int64(100), Type: sql.LiteralNumber},
			},
			values:   []types.Value{types.NewIntValue(25)},
			expected: true,
		},
		{
			name: "AND expression both true",
			expr: &sql.BinaryExpr{
				Left: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "age"},
					Op:    sql.OpGt,
					Right: &sql.Literal{Value: int64(0), Type: sql.LiteralNumber},
				},
				Op: sql.OpAnd,
				Right: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "age"},
					Op:    sql.OpLt,
					Right: &sql.Literal{Value: int64(100), Type: sql.LiteralNumber},
				},
			},
			values:   []types.Value{types.NewIntValue(50)},
			expected: true,
		},
		{
			name: "AND expression short-circuit",
			expr: &sql.BinaryExpr{
				Left: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "age"},
					Op:    sql.OpLt,
					Right: &sql.Literal{Value: int64(0), Type: sql.LiteralNumber},
				},
				Op: sql.OpAnd,
				Right: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "age"},
					Op:    sql.OpGt,
					Right: &sql.Literal{Value: int64(100), Type: sql.LiteralNumber},
				},
			},
			values:   []types.Value{types.NewIntValue(50)},
			expected: false,
		},
		{
			name: "OR expression first true",
			expr: &sql.BinaryExpr{
				Left: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "age"},
					Op:    sql.OpGt,
					Right: &sql.Literal{Value: int64(0), Type: sql.LiteralNumber},
				},
				Op: sql.OpOr,
				Right: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "age"},
					Op:    sql.OpLt,
					Right: &sql.Literal{Value: int64(0), Type: sql.LiteralNumber},
				},
			},
			values:   []types.Value{types.NewIntValue(50)},
			expected: true,
		},
		{
			name: "OR expression second true",
			expr: &sql.BinaryExpr{
				Left: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "age"},
					Op:    sql.OpLt,
					Right: &sql.Literal{Value: int64(0), Type: sql.LiteralNumber},
				},
				Op: sql.OpOr,
				Right: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "age"},
					Op:    sql.OpGt,
					Right: &sql.Literal{Value: int64(0), Type: sql.LiteralNumber},
				},
			},
			values:   []types.Value{types.NewIntValue(50)},
			expected: true,
		},
		{
			name: "literal bool true",
			expr: &sql.Literal{
				Value: true,
				Type:  sql.LiteralBool,
			},
			values:   []types.Value{},
			expected: true,
		},
		{
			name: "column ref non-null",
			expr: &sql.ColumnRef{Name: "age"},
			values:   []types.Value{types.NewIntValue(25)},
			expected: true,
		},
		{
			name: "is null check",
			expr: &sql.IsNullExpr{
				Expr: &sql.ColumnRef{Name: "age"},
				Not:  false,
			},
			values:   []types.Value{types.NewNullValue()},
			expected: true,
		},
		{
			name: "is not null check",
			expr: &sql.IsNullExpr{
				Expr: &sql.ColumnRef{Name: "age"},
				Not:  true,
			},
			values:   []types.Value{types.NewIntValue(25)},
			expected: true,
		},
		{
			name: "paren expression",
			expr: &sql.ParenExpr{
				Expr: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "age"},
					Op:    sql.OpGt,
					Right: &sql.Literal{Value: int64(0), Type: sql.LiteralNumber},
				},
			},
			values:   []types.Value{types.NewIntValue(25)},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.evaluateCheckExpression(tt.expr, tt.values, nil, colMap)
			if err != nil {
				t.Errorf("evaluateCheckExpression failed: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestGetCheckValue tests getCheckValue function
func TestGetCheckValue(t *testing.T) {
	exec := &Executor{}
	colMap := map[string]int{"age": 0}

	tests := []struct {
		name     string
		expr     sql.Expression
		values   []types.Value
		expected interface{}
	}{
		{
			name:     "literal int",
			expr:     &sql.Literal{Value: int64(42), Type: sql.LiteralNumber},
			values:   nil,
			expected: int64(42),
		},
		{
			name:     "literal float",
			expr:     &sql.Literal{Value: 3.14, Type: sql.LiteralNumber},
			values:   nil,
			expected: float64(3.14),
		},
		{
			name:     "column ref",
			expr:     &sql.ColumnRef{Name: "age"},
			values:   []types.Value{types.NewIntValue(25)},
			expected: int64(25),
		},
		{
			name:     "null column",
			expr:     &sql.ColumnRef{Name: "age"},
			values:   []types.Value{types.NewNullValue()},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.getCheckValue(tt.expr, tt.values, colMap)
			if err != nil {
				t.Errorf("getCheckValue failed: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestCompareCheckValues tests compareCheckValues function
func TestCompareCheckValues(t *testing.T) {
	exec := &Executor{}
	colMap := map[string]int{"a": 0, "b": 1}
	values := []types.Value{
		types.NewIntValue(10),
		types.NewIntValue(20),
	}

	tests := []struct {
		name     string
		left     sql.Expression
		right    sql.Expression
		op       sql.BinaryOp
		expected bool
	}{
		{
			name:     "int less than",
			left:     &sql.Literal{Value: int64(10), Type: sql.LiteralNumber},
			right:    &sql.Literal{Value: int64(20), Type: sql.LiteralNumber},
			op:       sql.OpLt,
			expected: true,
		},
		{
			name:     "int greater than",
			left:     &sql.Literal{Value: int64(20), Type: sql.LiteralNumber},
			right:    &sql.Literal{Value: int64(10), Type: sql.LiteralNumber},
			op:       sql.OpGt,
			expected: true,
		},
		{
			name:     "float comparison",
			left:     &sql.Literal{Value: 1.5, Type: sql.LiteralNumber},
			right:    &sql.Literal{Value: 2.5, Type: sql.LiteralNumber},
			op:       sql.OpLt,
			expected: true,
		},
		{
			name:     "column vs literal",
			left:     &sql.ColumnRef{Name: "a"},
			right:    &sql.Literal{Value: int64(20), Type: sql.LiteralNumber},
			op:       sql.OpLt,
			expected: true,
		},
		{
			name:     "column vs column",
			left:     &sql.ColumnRef{Name: "a"},
			right:    &sql.ColumnRef{Name: "b"},
			op:       sql.OpLt,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.compareCheckValues(tt.left, tt.right, values, colMap, tt.op)
			if err != nil {
				t.Errorf("compareCheckValues failed: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestExecuteWithAuthManager tests auth operations with auth manager
func TestExecuteWithAuthManager(t *testing.T) {
	exec := &Executor{authMgr: &fullMockAuthManager{}}

	t.Run("create user", func(t *testing.T) {
		stmt := &sql.CreateUserStmt{
			Username:  "testuser",
			Identified: "password",
			Role:      "user",
		}
		result, err := exec.executeCreateUser(stmt)
		if err != nil {
			t.Errorf("executeCreateUser failed: %v", err)
		}
		if result.Message == "" {
			t.Error("Expected message")
		}
	})

	t.Run("create admin user", func(t *testing.T) {
		stmt := &sql.CreateUserStmt{
			Username:  "adminuser",
			Identified: "password",
			Role:      "admin",
		}
		result, err := exec.executeCreateUser(stmt)
		if err != nil {
			t.Errorf("executeCreateUser failed: %v", err)
		}
		if result.Message == "" {
			t.Error("Expected message")
		}
	})

	t.Run("drop user", func(t *testing.T) {
		stmt := &sql.DropUserStmt{Username: "testuser"}
		result, err := exec.executeDropUser(stmt)
		if err != nil {
			t.Errorf("executeDropUser failed: %v", err)
		}
		if result.Message == "" {
			t.Error("Expected message")
		}
	})

	t.Run("alter user", func(t *testing.T) {
		stmt := &sql.AlterUserStmt{
			Username:  "testuser",
			Identified: "newpassword",
		}
		result, err := exec.executeAlterUser(stmt)
		if err != nil {
			t.Errorf("executeAlterUser failed: %v", err)
		}
		if result.Message == "" {
			t.Error("Expected message")
		}
	})

	t.Run("set password", func(t *testing.T) {
		stmt := &sql.SetPasswordStmt{
			ForUser:  "testuser",
			Password: "newpassword",
		}
		result, err := exec.executeSetPassword(stmt)
		if err != nil {
			t.Errorf("executeSetPassword failed: %v", err)
		}
		if result.Message == "" {
			t.Error("Expected message")
		}
	})

	t.Run("set password without user", func(t *testing.T) {
		stmt := &sql.SetPasswordStmt{
			Password: "newpassword",
		}
		_, err := exec.executeSetPassword(stmt)
		if err == nil {
			t.Error("Expected error for SET PASSWORD without FOR clause")
		}
	})

	t.Run("grant global", func(t *testing.T) {
		stmt := &sql.GrantStmt{
			Privileges: []*sql.Privilege{{Type: sql.PrivAll}},
			On:         sql.GrantOnAll,
			To:         "testuser",
			WithGrant:  true,
		}
		result, err := exec.executeGrant(stmt)
		if err != nil {
			t.Errorf("executeGrant failed: %v", err)
		}
		if result.Message == "" {
			t.Error("Expected message")
		}
	})

	t.Run("grant database", func(t *testing.T) {
		stmt := &sql.GrantStmt{
			Privileges: []*sql.Privilege{{Type: sql.PrivSelect}},
			On:         sql.GrantOnDatabase,
			Database:   "testdb",
			To:         "testuser",
		}
		result, err := exec.executeGrant(stmt)
		if err != nil {
			t.Errorf("executeGrant failed: %v", err)
		}
		if result.Message == "" {
			t.Error("Expected message")
		}
	})

	t.Run("grant table", func(t *testing.T) {
		stmt := &sql.GrantStmt{
			Privileges: []*sql.Privilege{{Type: sql.PrivSelect}},
			On:         sql.GrantOnTable,
			Database:   "testdb",
			Table:      "testtable",
			To:         "testuser",
		}
		result, err := exec.executeGrant(stmt)
		if err != nil {
			t.Errorf("executeGrant failed: %v", err)
		}
		if result.Message == "" {
			t.Error("Expected message")
		}
	})

	t.Run("revoke global", func(t *testing.T) {
		stmt := &sql.RevokeStmt{
			Privileges: []*sql.Privilege{{Type: sql.PrivAll}},
			On:         sql.GrantOnAll,
			From:       "testuser",
		}
		result, err := exec.executeRevoke(stmt)
		if err != nil {
			t.Errorf("executeRevoke failed: %v", err)
		}
		if result.Message == "" {
			t.Error("Expected message")
		}
	})

	t.Run("revoke database", func(t *testing.T) {
		stmt := &sql.RevokeStmt{
			Privileges: []*sql.Privilege{{Type: sql.PrivSelect}},
			On:         sql.GrantOnDatabase,
			Database:   "testdb",
			From:       "testuser",
		}
		result, err := exec.executeRevoke(stmt)
		if err != nil {
			t.Errorf("executeRevoke failed: %v", err)
		}
		if result.Message == "" {
			t.Error("Expected message")
		}
	})

	t.Run("revoke table", func(t *testing.T) {
		stmt := &sql.RevokeStmt{
			Privileges: []*sql.Privilege{{Type: sql.PrivSelect}},
			On:         sql.GrantOnTable,
			Database:   "testdb",
			Table:      "testtable",
			From:       "testuser",
		}
		result, err := exec.executeRevoke(stmt)
		if err != nil {
			t.Errorf("executeRevoke failed: %v", err)
		}
		if result.Message == "" {
			t.Error("Expected message")
		}
	})

	t.Run("show grants", func(t *testing.T) {
		stmt := &sql.ShowGrantsStmt{ForUser: "testuser"}
		result, err := exec.executeShowGrants(stmt)
		if err != nil {
			t.Errorf("executeShowGrants failed: %v", err)
		}
		if result == nil {
			t.Error("Expected result")
		}
	})
}

// fullMockAuthManager is a more complete mock for testing auth operations
type fullMockAuthManager struct{}

func (m *fullMockAuthManager) CreateUser(username, password string, role int) (interface{}, error) {
	return struct{ Name string }{Name: username}, nil
}

func (m *fullMockAuthManager) DeleteUser(username string) error {
	return nil
}

func (m *fullMockAuthManager) GetUser(username string) (interface{}, error) {
	return struct{ Name string }{Name: username}, nil
}

func (m *fullMockAuthManager) ChangePassword(username, oldPassword, newPassword string) error {
	return nil
}

func (m *fullMockAuthManager) GrantGlobal(username string, priv interface{}) error {
	return nil
}

func (m *fullMockAuthManager) GrantDatabase(username, database string, priv interface{}) error {
	return nil
}

func (m *fullMockAuthManager) GrantTable(username, database, table string, priv interface{}) error {
	return nil
}

func (m *fullMockAuthManager) RevokeGlobal(username string, priv interface{}) error {
	return nil
}

func (m *fullMockAuthManager) RevokeDatabase(username, database string, priv interface{}) error {
	return nil
}

func (m *fullMockAuthManager) RevokeTable(username, database, table string, priv interface{}) error {
	return nil
}

func (m *fullMockAuthManager) GetGrants(username string) ([]string, error) {
	return []string{"GRANT ALL ON *.* TO 'testuser'"}, nil
}

// TestEvaluateExprForRow tests evaluateExprForRow function
func TestEvaluateExprForRow(t *testing.T) {
	exec := &Executor{}

	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
		{Name: "name", Type: types.TypeVarchar},
	}
	colIdxMap := map[string]int{"id": 0, "name": 1}

	tests := []struct {
		name     string
		expr     sql.Expression
		row      *row.Row
		expected interface{}
	}{
		{
			name:     "literal",
			expr:     &sql.Literal{Value: 42, Type: sql.LiteralNumber},
			row:      &row.Row{},
			expected: 42,
		},
		{
			name:     "column ref",
			expr:     &sql.ColumnRef{Name: "id"},
			row:      &row.Row{Values: []types.Value{types.NewIntValue(123)}},
			expected: int64(123),
		},
		{
			name:     "unknown column",
			expr:     &sql.ColumnRef{Name: "unknown"},
			row:      &row.Row{Values: []types.Value{}},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.evaluateExprForRow(tt.expr, tt.row, columns, colIdxMap)
			if err != nil && tt.name != "unknown column" {
				t.Errorf("evaluateExprForRow failed: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestValueToInterface tests valueToInterface function
func TestValueToInterface(t *testing.T) {
	exec := &Executor{}

	tests := []struct {
		name     string
		value    types.Value
		expected interface{}
	}{
		{
			name:     "null value",
			value:    types.NewNullValue(),
			expected: nil,
		},
		{
			name:     "int value",
			value:    types.NewIntValue(42),
			expected: int64(42),
		},
		{
			name:     "float value",
			value:    types.NewFloatValue(3.14),
			expected: float64(3.14),
		},
		{
			name:     "string value",
			value:    types.NewStringValue("hello", types.TypeVarchar),
			expected: "hello",
		},
		{
			name:     "bool value true",
			value:    types.NewBoolValue(true),
			expected: true,
		},
		{
			name:     "bool value false",
			value:    types.NewBoolValue(false),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.valueToInterface(tt.value)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}