// Package optimizer provides query optimization for XxSql.
package optimizer

import (
	"testing"

	"github.com/topxeq/xxsql/internal/sql"
	"github.com/topxeq/xxsql/internal/storage/types"
)

func TestPlanTypeString(t *testing.T) {
	tests := []struct {
		planType PlanType
		expected string
	}{
		{PlanTypeTableScan, "TABLE SCAN"},
		{PlanTypeIndexScan, "INDEX SCAN"},
		{PlanTypeIndexRangeScan, "INDEX RANGE SCAN"},
		{PlanTypeIndexPointLookup, "INDEX POINT LOOKUP"},
		{PlanType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.planType.String(); got != tt.expected {
				t.Errorf("PlanType.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestNewOptimizer(t *testing.T) {
	opt := NewOptimizer()
	if opt == nil {
		t.Fatal("NewOptimizer returned nil")
	}
	if opt.stats == nil {
		t.Error("stats map not initialized")
	}
}

func TestUpdateAndGetStatistics(t *testing.T) {
	opt := NewOptimizer()

	// Test GetStatistics for non-existent table
	if stats := opt.GetStatistics("nonexistent"); stats != nil {
		t.Errorf("GetStatistics for nonexistent table should return nil, got %v", stats)
	}

	// Test UpdateStatistics and GetStatistics
	stats := &Statistics{
		RowCount: 100,
		ColumnStats: map[string]*ColumnStatistics{
			"id": {Name: "id", DistinctCount: 100},
		},
	}

	opt.UpdateStatistics("users", stats)

	got := opt.GetStatistics("users")
	if got == nil {
		t.Fatal("GetStatistics returned nil for existing table")
	}
	if got.RowCount != 100 {
		t.Errorf("RowCount = %d, want 100", got.RowCount)
	}
}

func TestEstimateSelectivity(t *testing.T) {
	opt := NewOptimizer()

	// Test with nil stats
	selectivity := opt.EstimateSelectivity("users", "id", sql.OpEq, types.Value{}, nil)
	if selectivity != 0.1 {
		t.Errorf("Selectivity with nil stats = %v, want 0.1", selectivity)
	}

	// Test with stats but missing column
	stats := &Statistics{
		ColumnStats: map[string]*ColumnStatistics{
			"name": {Name: "name", DistinctCount: 50},
		},
	}
	selectivity = opt.EstimateSelectivity("users", "id", sql.OpEq, types.Value{}, stats)
	if selectivity != 0.1 {
		t.Errorf("Selectivity with missing column = %v, want 0.1", selectivity)
	}

	// Test equality with statistics
	stats = &Statistics{
		ColumnStats: map[string]*ColumnStatistics{
			"id": {Name: "id", DistinctCount: 100},
		},
	}
	selectivity = opt.EstimateSelectivity("users", "id", sql.OpEq, types.Value{}, stats)
	if selectivity != 0.01 { // 1/100
		t.Errorf("Selectivity for equality = %v, want 0.01", selectivity)
	}

	// Test inequality
	selectivity = opt.EstimateSelectivity("users", "id", sql.OpNe, types.Value{}, stats)
	if selectivity != 0.99 { // 1 - 1/100
		t.Errorf("Selectivity for inequality = %v, want 0.99", selectivity)
	}

	// Test range operators with min/max
	stats = &Statistics{
		ColumnStats: map[string]*ColumnStatistics{
			"age": {
				Name:          "age",
				DistinctCount: 100,
				MinValue:      types.NewIntValue(0),
				MaxValue:      types.NewIntValue(100),
			},
		},
	}

	// Test OpLt with value below min
	selectivity = opt.EstimateSelectivity("users", "age", sql.OpLt, types.NewIntValue(-10), stats)
	if selectivity != 0.0 {
		t.Errorf("Selectivity for value < min = %v, want 0.0", selectivity)
	}

	// Test OpLt with value above max
	selectivity = opt.EstimateSelectivity("users", "age", sql.OpLt, types.NewIntValue(200), stats)
	if selectivity != 1.0 {
		t.Errorf("Selectivity for value > max = %v, want 1.0", selectivity)
	}

	// Test OpGt with value above max
	selectivity = opt.EstimateSelectivity("users", "age", sql.OpGt, types.NewIntValue(200), stats)
	if selectivity != 0.0 {
		t.Errorf("Selectivity for value > max = %v, want 0.0", selectivity)
	}

	// Test OpGt with value below min
	selectivity = opt.EstimateSelectivity("users", "age", sql.OpGt, types.NewIntValue(-10), stats)
	if selectivity != 1.0 {
		t.Errorf("Selectivity for value < min = %v, want 1.0", selectivity)
	}
}

func TestDefaultSelectivity(t *testing.T) {
	tests := []struct {
		op       sql.BinaryOp
		expected float64
	}{
		{sql.OpEq, 0.1},
		{sql.OpLt, 0.33},
		{sql.OpLe, 0.33},
		{sql.OpGt, 0.33},
		{sql.OpGe, 0.33},
		{sql.OpNe, 0.9},
		{sql.BinaryOp(99), 0.5},
	}

	for _, tt := range tests {
		t.Run(tt.op.String(), func(t *testing.T) {
			if got := defaultSelectivity(tt.op); got != tt.expected {
				t.Errorf("defaultSelectivity(%v) = %v, want %v", tt.op, got, tt.expected)
			}
		})
	}
}

func TestEstimateRangeSelectivity(t *testing.T) {
	colStats := &ColumnStatistics{
		Name:          "age",
		DistinctCount: 100,
		MinValue:      types.NewIntValue(0),
		MaxValue:      types.NewIntValue(100),
	}

	tests := []struct {
		op       sql.BinaryOp
		value    int64
		expected float64
	}{
		{sql.OpLt, -10, 0.0},   // value < min
		{sql.OpLt, 200, 1.0},   // value > max
		{sql.OpLt, 50, 0.33},   // in range
		{sql.OpLe, -10, 0.0},   // value < min
		{sql.OpLe, 200, 1.0},   // value > max
		{sql.OpLe, 50, 0.33},   // in range
		{sql.OpGt, 200, 0.0},   // value > max
		{sql.OpGt, -10, 1.0},   // value < min
		{sql.OpGt, 50, 0.33},   // in range
		{sql.OpGe, 200, 0.0},   // value > max
		{sql.OpGe, -10, 1.0},   // value < min
		{sql.OpGe, 50, 0.33},   // in range
	}

	for _, tt := range tests {
		t.Run(tt.op.String(), func(t *testing.T) {
			got := estimateRangeSelectivity(colStats, tt.op, types.NewIntValue(int64(tt.value)))
			if got != tt.expected {
				t.Errorf("estimateRangeSelectivity(%v, %d) = %v, want %v", tt.op, tt.value, got, tt.expected)
			}
		})
	}

	// Test with nil min/max
	colStatsNoRange := &ColumnStatistics{
		Name:          "age",
		DistinctCount: 100,
	}
	got := estimateRangeSelectivity(colStatsNoRange, sql.OpLt, types.NewIntValue(50))
	if got != 0.33 {
		t.Errorf("estimateRangeSelectivity with nil min/max = %v, want 0.33", got)
	}
}

func TestEstimateCost(t *testing.T) {
	opt := NewOptimizer()

	// Test without index stats
	cost := opt.EstimateCost("users", 1000, nil, 0.1)
	if cost.Cardinality == 0 {
		t.Error("Cardinality should not be 0")
	}
	if cost.TotalCost <= 0 {
		t.Error("TotalCost should be positive")
	}

	// Test with index stats
	indexStats := &IndexStatistics{
		Name:        "idx_users_id",
		Columns:     []string{"id"},
		Height:      3,
		Clustered:   false,
	}
	cost = opt.EstimateCost("users", 1000, indexStats, 0.1)
	if cost.Cardinality == 0 {
		t.Error("Cardinality should not be 0")
	}

	// Test with clustered index
	indexStats.Clustered = true
	costClustered := opt.EstimateCost("users", 1000, indexStats, 0.1)
	if costClustered.TotalCost >= cost.TotalCost {
		t.Error("Clustered index should be cheaper than non-clustered")
	}
}

func TestShouldUseIndex(t *testing.T) {
	opt := NewOptimizer()

	// Test with low selectivity - should use index
	if !opt.ShouldUseIndex("users", 1000, nil, 0.05) {
		t.Error("Should use index for selectivity <= 0.1")
	}

	// Test with high selectivity - should not use index
	if opt.ShouldUseIndex("users", 1000, nil, 0.95) {
		t.Error("Should not use index for selectivity >= 0.9")
	}

	// Test with medium selectivity and index stats
	indexStats := &IndexStatistics{
		Name:      "idx_users_id",
		Columns:   []string{"id"},
		Height:    3,
		Clustered: false,
	}
	// Medium selectivity, index cost should be lower for large tables
	opt.ShouldUseIndex("users", 1000, indexStats, 0.5)
}

func TestEstimateRows(t *testing.T) {
	opt := NewOptimizer()

	tests := []struct {
		rowCount    uint64
		selectivity float64
		expected    uint64
	}{
		{1000, 0.1, 100},
		{100, 0.5, 50},
		{10, 0.01, 1},     // Should return at least 1 if selectivity > 0
		{1000, 2.0, 1000}, // Should cap at rowCount
		{0, 0.5, 1},       // Returns min 1 if selectivity > 0
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			got := opt.EstimateRows("users", tt.rowCount, tt.selectivity)
			if got != tt.expected {
				t.Errorf("EstimateRows(%d, %v) = %d, want %d", tt.rowCount, tt.selectivity, got, tt.expected)
			}
		})
	}
}

func TestChooseIndex(t *testing.T) {
	opt := NewOptimizer()

	// Test with nil WHERE clause
	name, planType := opt.ChooseIndex("users", nil, nil)
	if name != "" || planType != PlanTypeTableScan {
		t.Errorf("ChooseIndex with nil WHERE should return table scan")
	}

	// Test with no available indexes
	where := &sql.BinaryExpr{
		Left:  &sql.ColumnRef{Name: "id"},
		Op:    sql.OpEq,
		Right: &sql.Literal{Value: "1", Type: sql.LiteralString},
	}
	name, planType = opt.ChooseIndex("users", where, nil)
	if name != "" || planType != PlanTypeTableScan {
		t.Errorf("ChooseIndex with no indexes should return table scan")
	}

	// Test with matching index
	indexes := map[string]*IndexStatistics{
		"idx_users_id": {
			Name:    "idx_users_id",
			Columns: []string{"id"},
		},
	}
	name, planType = opt.ChooseIndex("users", where, indexes)
	if name != "idx_users_id" {
		t.Errorf("ChooseIndex should select idx_users_id, got %s", name)
	}
	if planType != PlanTypeIndexPointLookup {
		t.Errorf("Plan type should be IndexPointLookup for equality, got %v", planType)
	}

	// Test with range condition
	whereRange := &sql.BinaryExpr{
		Left:  &sql.ColumnRef{Name: "id"},
		Op:    sql.OpLt,
		Right: &sql.Literal{Value: "100", Type: sql.LiteralString},
	}
	name, planType = opt.ChooseIndex("users", whereRange, indexes)
	if name != "idx_users_id" {
		t.Errorf("ChooseIndex should select idx_users_id for range, got %s", name)
	}
	if planType != PlanTypeIndexScan {
		t.Errorf("Plan type should be IndexScan for range, got %v", planType)
	}
}

func TestExtractColumnsFromExpr(t *testing.T) {
	tests := []struct {
		name     string
		expr     sql.Expression
		expected []string
	}{
		{
			name:     "column ref",
			expr:     &sql.ColumnRef{Name: "id"},
			expected: []string{"id"},
		},
		{
			name: "binary expression",
			expr: &sql.BinaryExpr{
				Left:  &sql.ColumnRef{Name: "id"},
				Op:    sql.OpEq,
				Right: &sql.ColumnRef{Name: "status"},
			},
			expected: []string{"id", "status"},
		},
		{
			name: "nested binary expression",
			expr: &sql.BinaryExpr{
				Left: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "a"},
					Op:    sql.OpEq,
					Right: &sql.ColumnRef{Name: "b"},
				},
				Op:    sql.OpAnd,
				Right: &sql.ColumnRef{Name: "c"},
			},
			expected: []string{"a", "b", "c"},
		},
		{
			name: "unary expression",
			expr: &sql.UnaryExpr{
				Op:    sql.OpNot,
				Right: &sql.ColumnRef{Name: "active"},
			},
			expected: []string{"active"},
		},
		{
			name: "paren expression",
			expr: &sql.ParenExpr{
				Expr: &sql.ColumnRef{Name: "id"},
			},
			expected: []string{"id"},
		},
		{
			name: "function call",
			expr: &sql.FunctionCall{
				Name: "UPPER",
				Args: []sql.Expression{&sql.ColumnRef{Name: "name"}},
			},
			expected: []string{"name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractColumnsFromExpr(tt.expr)
			if len(got) != len(tt.expected) {
				t.Errorf("extractColumnsFromExpr got %d columns, want %d", len(got), len(tt.expected))
				return
			}
			for i, col := range got {
				if col != tt.expected[i] {
					t.Errorf("Column[%d] = %s, want %s", i, col, tt.expected[i])
				}
			}
		})
	}
}

func TestCountPrefixMatch(t *testing.T) {
	tests := []struct {
		indexCols []string
		queryCols []string
		expected  int
	}{
		{[]string{"a", "b", "c"}, []string{"a", "b", "c"}, 3},
		{[]string{"a", "b", "c"}, []string{"a", "b"}, 2},
		{[]string{"a", "b", "c"}, []string{"a"}, 1},
		{[]string{"a", "b", "c"}, []string{"d"}, 0},
		{[]string{"a", "b"}, []string{"a", "b", "c"}, 2},
		{[]string{}, []string{"a"}, 0},
		{[]string{"a"}, []string{}, 0},
	}

	for i, tt := range tests {
		t.Run("", func(t *testing.T) {
			got := countPrefixMatch(tt.indexCols, tt.queryCols)
			if got != tt.expected {
				t.Errorf("Test %d: countPrefixMatch = %d, want %d", i, got, tt.expected)
			}
		})
	}
}

func TestIsEqualityCondition(t *testing.T) {
	tests := []struct {
		name     string
		where    sql.Expression
		column   string
		expected bool
	}{
		{
			name: "direct equality",
			where: &sql.BinaryExpr{
				Left:  &sql.ColumnRef{Name: "id"},
				Op:    sql.OpEq,
				Right: &sql.Literal{Value: "1", Type: sql.LiteralString},
			},
			column:   "id",
			expected: true,
		},
		{
			name: "equality on right side",
			where: &sql.BinaryExpr{
				Left:  &sql.Literal{Value: "1", Type: sql.LiteralString},
				Op:    sql.OpEq,
				Right: &sql.ColumnRef{Name: "id"},
			},
			column:   "id",
			expected: true,
		},
		{
			name: "non-equality operator",
			where: &sql.BinaryExpr{
				Left:  &sql.ColumnRef{Name: "id"},
				Op:    sql.OpLt,
				Right: &sql.Literal{Value: "100", Type: sql.LiteralString},
			},
			column:   "id",
			expected: false,
		},
		{
			name: "equality with different column",
			where: &sql.BinaryExpr{
				Left:  &sql.ColumnRef{Name: "status"},
				Op:    sql.OpEq,
				Right: &sql.Literal{Value: "active", Type: sql.LiteralString},
			},
			column:   "id",
			expected: false,
		},
		{
			name: "nested equality",
			where: &sql.BinaryExpr{
				Left: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "id"},
					Op:    sql.OpEq,
					Right: &sql.Literal{Value: "1", Type: sql.LiteralString},
				},
				Op:    sql.OpAnd,
				Right: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "status"},
					Op:    sql.OpEq,
					Right: &sql.Literal{Value: "active", Type: sql.LiteralString},
				},
			},
			column:   "id",
			expected: true,
		},
		{
			name: "paren equality",
			where: &sql.ParenExpr{
				Expr: &sql.BinaryExpr{
					Left:  &sql.ColumnRef{Name: "id"},
					Op:    sql.OpEq,
					Right: &sql.Literal{Value: "1", Type: sql.LiteralString},
				},
			},
			column:   "id",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isEqualityCondition(tt.where, tt.column)
			if got != tt.expected {
				t.Errorf("isEqualityCondition = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestIsLowSelectivity(t *testing.T) {
	if !IsLowSelectivity(0.1) {
		t.Error("0.1 should be low selectivity")
	}
	if !IsLowSelectivity(0.3) {
		t.Error("0.3 should be low selectivity (at threshold)")
	}
	if IsLowSelectivity(0.31) {
		t.Error("0.31 should not be low selectivity")
	}
}

func TestCalculateSelectivityFromConditions(t *testing.T) {
	tests := []struct {
		name          string
		selectivities []float64
		expected      float64
	}{
		{"empty", nil, 1.0},
		{"single", []float64{0.5}, 0.5},
		{"multiple AND", []float64{0.5, 0.5}, 0.25},
		{"three conditions", []float64{0.1, 0.1, 0.1}, 0.001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateSelectivityFromConditions(tt.selectivities)
			// Use approximate comparison for floating point
			if got < tt.expected-0.0001 || got > tt.expected+0.0001 {
				t.Errorf("CalculateSelectivityFromConditions = %v, want %v", got, tt.expected)
			}
		})
	}
}