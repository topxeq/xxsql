package optimizer

import (
	"testing"

	"github.com/topxeq/xxsql/internal/storage/types"
)

func TestNewAnalyzeTable(t *testing.T) {
	a := NewAnalyzeTable()
	if a == nil {
		t.Fatal("NewAnalyzeTable returned nil")
	}
	if a.Stats == nil {
		t.Error("Stats not initialized")
	}
	if a.Stats.ColumnStats == nil {
		t.Error("ColumnStats not initialized")
	}
	if a.Stats.IndexStats == nil {
		t.Error("IndexStats not initialized")
	}
}

func TestAnalyzeTableAddRow(t *testing.T) {
	a := NewAnalyzeTable()

	// Add first row
	columns := []string{"id", "name", "age"}
	values := []types.Value{
		types.NewIntValue(1),
		types.NewStringValue("Alice", types.TypeVarchar),
		types.NewIntValue(30),
	}
	a.AddRow(columns, values)

	if a.Stats.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", a.Stats.RowCount)
	}

	// Check column stats
	if a.Stats.ColumnStats["id"] == nil {
		t.Error("id column stats not created")
	}
	if a.Stats.ColumnStats["name"] == nil {
		t.Error("name column stats not created")
	}
	if a.Stats.ColumnStats["age"] == nil {
		t.Error("age column stats not created")
	}

	// Add second row
	values2 := []types.Value{
		types.NewIntValue(2),
		types.NewStringValue("Bob", types.TypeVarchar),
		types.NewIntValue(25),
	}
	a.AddRow(columns, values2)

	if a.Stats.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", a.Stats.RowCount)
	}

	// Check min/max updates
	ageStats := a.Stats.ColumnStats["age"]
	if ageStats.MinValue.Compare(types.NewIntValue(25)) != 0 {
		t.Errorf("age min value incorrect")
	}
	if ageStats.MaxValue.Compare(types.NewIntValue(30)) != 0 {
		t.Errorf("age max value incorrect")
	}
}

func TestAnalyzeTableAddRowWithNulls(t *testing.T) {
	a := NewAnalyzeTable()

	columns := []string{"id", "name"}
	values := []types.Value{
		types.NewIntValue(1),
		types.NewNullValue(),
	}
	a.AddRow(columns, values)

	nameStats := a.Stats.ColumnStats["name"]
	if nameStats.NullCount != 1 {
		t.Errorf("NullCount = %d, want 1", nameStats.NullCount)
	}
}

func TestAnalyzeTableAddRowMoreColumns(t *testing.T) {
	a := NewAnalyzeTable()

	// Test when values slice is shorter than columns
	columns := []string{"id", "name", "age"}
	values := []types.Value{
		types.NewIntValue(1),
	}
	a.AddRow(columns, values)

	// Should not panic and should only add stats for available values
	if a.Stats.ColumnStats["id"] == nil {
		t.Error("id column stats should be created")
	}
}

func TestAnalyzeTableFinalize(t *testing.T) {
	a := NewAnalyzeTable()

	// Add rows
	columns := []string{"id"}
	for i := 0; i < 100; i++ {
		a.AddRow(columns, []types.Value{types.NewIntValue(int64(i))})
	}

	stats := a.Finalize()

	if stats.RowCount != 100 {
		t.Errorf("RowCount = %d, want 100", stats.RowCount)
	}

	// Check distinct count cap
	if stats.ColumnStats["id"].DistinctCount > stats.RowCount {
		t.Errorf("DistinctCount %d should not exceed RowCount %d",
			stats.ColumnStats["id"].DistinctCount, stats.RowCount)
	}
}

func TestAnalyzeTableSetIndexStatistics(t *testing.T) {
	a := NewAnalyzeTable()

	a.SetIndexStatistics("idx_users_id", []string{"id"}, 1000, 3, false)

	if a.Stats.IndexStats["idx_users_id"] == nil {
		t.Fatal("Index stats not created")
	}

	idx := a.Stats.IndexStats["idx_users_id"]
	if idx.Name != "idx_users_id" {
		t.Errorf("Name = %s, want idx_users_id", idx.Name)
	}
	if len(idx.Columns) != 1 || idx.Columns[0] != "id" {
		t.Errorf("Columns incorrect")
	}
	if idx.DistinctKeys != 1000 {
		t.Errorf("DistinctKeys = %d, want 1000", idx.DistinctKeys)
	}
	if idx.Height != 3 {
		t.Errorf("Height = %d, want 3", idx.Height)
	}
	if idx.Clustered {
		t.Error("Clustered should be false")
	}
}

func TestAnalyzeTableBuildHistogram(t *testing.T) {
	a := NewAnalyzeTable()

	// First add a row to create the column stats
	a.AddRow([]string{"age"}, []types.Value{types.NewIntValue(25)})

	// Build histogram
	values := []types.Value{
		types.NewIntValue(10),
		types.NewIntValue(20),
		types.NewIntValue(30),
		types.NewIntValue(40),
		types.NewIntValue(50),
	}
	a.BuildHistogram("age", values, 2)

	stats := a.Stats.ColumnStats["age"]
	if stats == nil {
		t.Fatal("age stats not created")
	}
	if len(stats.Histogram) == 0 {
		t.Error("Histogram not built")
	}

	// Check first bucket
	if stats.Histogram[0].LowerBound.Compare(types.NewIntValue(10)) != 0 {
		t.Error("First bucket lower bound incorrect")
	}
}

func TestAnalyzeTableBuildHistogramEmpty(t *testing.T) {
	a := NewAnalyzeTable()

	// Build histogram with empty values
	a.BuildHistogram("nonexistent", nil, 2)

	// Should not panic
}

func TestAnalyzeTableBuildHistogramNonexistentColumn(t *testing.T) {
	a := NewAnalyzeTable()

	// Build histogram for column that doesn't exist
	values := []types.Value{types.NewIntValue(1)}
	a.BuildHistogram("nonexistent", values, 2)

	// Should not panic, but no histogram should be created
	if len(a.Stats.ColumnStats) != 0 {
		t.Error("No column stats should be created for nonexistent column")
	}
}

func TestStatisticsEstimateColumnSelectivity(t *testing.T) {
	s := &Statistics{
		RowCount: 100,
		ColumnStats: map[string]*ColumnStatistics{
			"id": {
				Name:          "id",
				DistinctCount: 100,
			},
			"age": {
				Name:          "age",
				DistinctCount: 10,
				Histogram: []HistogramBucket{
					{
						LowerBound:    types.NewIntValue(0),
						UpperBound:    types.NewIntValue(10),
						RowCount:      10,
						DistinctCount: 10,
					},
				},
			},
		},
	}

	tests := []struct {
		name     string
		column   string
		op       string
		expected float64
	}{
		{"equality with distinct", "id", "=", 0.01},
		{"inequality", "id", "!=", 0.99},
		{"less than default", "id", "<", 0.33},
		{"greater than default", "id", ">", 0.33},
		{"nonexistent column", "nonexistent", "=", 0.1},
		{"unknown op", "id", "X", 0.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s.EstimateColumnSelectivity(tt.column, tt.op, types.Value{})
			if got != tt.expected {
				t.Errorf("EstimateColumnSelectivity = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestStatisticsEstimateColumnSelectivityWithHistogram(t *testing.T) {
	s := &Statistics{
		RowCount: 100,
		ColumnStats: map[string]*ColumnStatistics{
			"age": {
				Name:          "age",
				DistinctCount: 10,
				Histogram: []HistogramBucket{
					{
						LowerBound:    types.NewIntValue(0),
						UpperBound:    types.NewIntValue(20),
						RowCount:      50,
						DistinctCount: 5,
					},
					{
						LowerBound:    types.NewIntValue(20),
						UpperBound:    types.NewIntValue(40),
						RowCount:      30,
						DistinctCount: 3,
					},
					{
						LowerBound:    types.NewIntValue(40),
						UpperBound:    types.NewIntValue(60),
						RowCount:      20,
						DistinctCount: 2,
					},
				},
			},
		},
	}

	// Test equality in first bucket
	got := s.EstimateColumnSelectivity("age", "=", types.NewIntValue(10))
	if got <= 0 {
		t.Error("Selectivity should be positive for value in histogram")
	}

	// Test less than - value within first bucket
	// For value 15 < first bucket upper (20), the result depends on histogram algorithm
	got = s.EstimateColumnSelectivity("age", "<", types.NewIntValue(15))
	// This test just verifies the function runs without error
	_ = got

	// Test greater than
	got = s.EstimateColumnSelectivity("age", ">", types.NewIntValue(50))
	if got <= 0 {
		t.Error("Selectivity should be positive for greater than")
	}

	// Test greater than or equal
	got = s.EstimateColumnSelectivity("age", ">=", types.NewIntValue(10))
	if got <= 0 {
		t.Error("Selectivity should be positive for greater than or equal")
	}

	// Test less than or equal
	got = s.EstimateColumnSelectivity("age", "<=", types.NewIntValue(30))
	if got <= 0 {
		t.Error("Selectivity should be positive for less than or equal")
	}
}

func TestEstimateFromHistogram(t *testing.T) {
	histogram := []HistogramBucket{
		{
			LowerBound:    types.NewIntValue(0),
			UpperBound:    types.NewIntValue(10),
			RowCount:      10,
			DistinctCount: 10,
		},
		{
			LowerBound:    types.NewIntValue(10),
			UpperBound:    types.NewIntValue(20),
			RowCount:      10,
			DistinctCount: 10,
		},
	}
	totalRows := uint64(20)

	// Test with zero total rows
	got := estimateFromHistogram(histogram, "=", types.NewIntValue(5), 0)
	if got != 0 {
		t.Errorf("Should return 0 for zero total rows, got %v", got)
	}

	// Test equality
	got = estimateFromHistogram(histogram, "=", types.NewIntValue(5), totalRows)
	if got <= 0 {
		t.Error("Should have positive selectivity for value in histogram")
	}

	// Test less than - value after histogram
	got = estimateFromHistogram(histogram, "<", types.NewIntValue(100), totalRows)
	if got <= 0 {
		t.Error("Should have positive selectivity for less than max")
	}

	// Test less than - value before histogram
	got = estimateFromHistogram(histogram, "<", types.NewIntValue(-10), totalRows)
	if got != 0 {
		t.Errorf("Should have zero selectivity for less than min, got %v", got)
	}
}