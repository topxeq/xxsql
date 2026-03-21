// Package optimizer provides query optimization for XxSql.
package optimizer

import (
	"math"

	"github.com/topxeq/xxsql/internal/sql"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// Statistics represents table and index statistics.
type Statistics struct {
	RowCount    uint64
	ColumnStats map[string]*ColumnStatistics
	IndexStats  map[string]*IndexStatistics
}

// ColumnStatistics represents statistics for a column.
type ColumnStatistics struct {
	Name          string
	DistinctCount uint64
	NullCount     uint64
	MinValue      types.Value
	MaxValue      types.Value
	Histogram     []HistogramBucket
}

// IndexStatistics represents statistics for an index.
type IndexStatistics struct {
	Name          string
	Columns       []string
	DistinctKeys  uint64
	Clustered     bool // True for primary key
	Selectivity   float64
	Height        int
	LeafPages     uint64
}

// HistogramBucket represents a bucket in an equi-height histogram.
type HistogramBucket struct {
	LowerBound   types.Value
	UpperBound   types.Value
	DistinctCount uint64
	RowCount     uint64
}

// Cost represents the estimated cost of a query operation.
type Cost struct {
	IOCost     float64 // Disk I/O cost
	CPUCost    float64 // CPU cost
	TotalCost  float64 // Total cost
	Cardinality uint64 // Estimated number of rows
}

// PlanType represents the type of execution plan.
type PlanType int

const (
	PlanTypeTableScan PlanType = iota
	PlanTypeIndexScan
	PlanTypeIndexRangeScan
	PlanTypeIndexPointLookup
)

// String returns the string representation of the plan type.
func (p PlanType) String() string {
	switch p {
	case PlanTypeTableScan:
		return "TABLE SCAN"
	case PlanTypeIndexScan:
		return "INDEX SCAN"
	case PlanTypeIndexRangeScan:
		return "INDEX RANGE SCAN"
	case PlanTypeIndexPointLookup:
		return "INDEX POINT LOOKUP"
	default:
		return "UNKNOWN"
	}
}

// Optimizer provides query optimization.
type Optimizer struct {
	stats map[string]*Statistics // table name -> statistics
}

// NewOptimizer creates a new optimizer.
func NewOptimizer() *Optimizer {
	return &Optimizer{
		stats: make(map[string]*Statistics),
	}
}

// UpdateStatistics updates statistics for a table.
func (o *Optimizer) UpdateStatistics(tableName string, stats *Statistics) {
	o.stats[tableName] = stats
}

// GetStatistics returns statistics for a table.
func (o *Optimizer) GetStatistics(tableName string) *Statistics {
	if stats, ok := o.stats[tableName]; ok {
		return stats
	}
	return nil
}

// EstimateSelectivity estimates the selectivity of a predicate.
// Returns a value between 0 and 1 representing the fraction of rows that match.
func (o *Optimizer) EstimateSelectivity(tableName string, column string, op sql.BinaryOp, value types.Value, stats *Statistics) float64 {
	if stats == nil {
		// Default selectivity estimates without statistics
		return defaultSelectivity(op)
	}

	colStats, ok := stats.ColumnStats[column]
	if !ok {
		return defaultSelectivity(op)
	}

	switch op {
	case sql.OpEq:
		// Equality: 1/distinct_count
		if colStats.DistinctCount > 0 {
			return 1.0 / float64(colStats.DistinctCount)
		}
		return 0.1

	case sql.OpLt, sql.OpLe, sql.OpGt, sql.OpGe:
		// Range: estimate based on histogram or default
		return estimateRangeSelectivity(colStats, op, value)

	case sql.OpNe:
		// Inequality: 1 - (1/distinct_count)
		if colStats.DistinctCount > 0 {
			return 1.0 - 1.0/float64(colStats.DistinctCount)
		}
		return 0.9

	default:
		return defaultSelectivity(op)
	}
}

// defaultSelectivity returns default selectivity for an operator.
func defaultSelectivity(op sql.BinaryOp) float64 {
	switch op {
	case sql.OpEq:
		return 0.1 // 10% for equality
	case sql.OpLt, sql.OpLe, sql.OpGt, sql.OpGe:
		return 0.33 // 33% for range
	case sql.OpNe:
		return 0.9 // 90% for inequality
	default:
		return 0.5 // 50% for unknown
	}
}

// estimateRangeSelectivity estimates selectivity for range predicates.
func estimateRangeSelectivity(colStats *ColumnStatistics, op sql.BinaryOp, value types.Value) float64 {
	if colStats.MinValue.Data == nil || colStats.MaxValue.Data == nil {
		return 0.33
	}

	// Simple estimation: position of value in range
	cmpMin := value.Compare(colStats.MinValue)
	cmpMax := value.Compare(colStats.MaxValue)

	switch op {
	case sql.OpLt:
		if cmpMin <= 0 {
			return 0.0 // value <= min, no rows
		}
		if cmpMax >= 0 {
			return 1.0 // value >= max, all rows
		}
		// Estimate position in range
		return 0.33 // Simplified: could use actual value position

	case sql.OpLe:
		if cmpMin < 0 {
			return 0.0
		}
		if cmpMax >= 0 {
			return 1.0
		}
		return 0.33

	case sql.OpGt:
		if cmpMax >= 0 {
			return 0.0 // value >= max, no rows
		}
		if cmpMin <= 0 {
			return 1.0 // value <= min, all rows
		}
		return 0.33

	case sql.OpGe:
		if cmpMax > 0 {
			return 0.0
		}
		if cmpMin <= 0 {
			return 1.0
		}
		return 0.33

	default:
		return 0.33
	}
}

// EstimateCost estimates the cost of different execution plans.
func (o *Optimizer) EstimateCost(tableName string, rowCount uint64, indexStats *IndexStatistics, selectivity float64) Cost {
	// Table scan cost
	tableScanCost := Cost{
		IOCost:     float64(rowCount) * 1.0, // One I/O per row (simplified)
		CPUCost:    float64(rowCount) * 0.1,
		Cardinality: rowCount,
	}
	tableScanCost.TotalCost = tableScanCost.IOCost + tableScanCost.CPUCost

	// Index scan cost
	var indexScanCost Cost
	if indexStats != nil {
		estimatedRows := uint64(float64(rowCount) * selectivity)

		// Index lookup cost: traverse B+ tree + fetch rows
		indexScanCost = Cost{
			IOCost:     float64(indexStats.Height) + float64(estimatedRows)*1.2, // Tree traversal + row fetches
			CPUCost:    float64(estimatedRows) * 0.2,
			Cardinality: estimatedRows,
		}
		indexScanCost.TotalCost = indexScanCost.IOCost + indexScanCost.CPUCost

		// Clustered index (primary key) is cheaper because rows are stored with keys
		if indexStats.Clustered {
			indexScanCost.IOCost = float64(indexStats.Height) + float64(estimatedRows)*0.5
			indexScanCost.TotalCost = indexScanCost.IOCost + indexScanCost.CPUCost
		}
	} else {
		// Default index cost estimation
		estimatedRows := uint64(float64(rowCount) * selectivity)
		indexScanCost = Cost{
			IOCost:     3.0 + float64(estimatedRows)*1.5, // Assume height 3
			CPUCost:    float64(estimatedRows) * 0.2,
			Cardinality: estimatedRows,
		}
		indexScanCost.TotalCost = indexScanCost.IOCost + indexScanCost.CPUCost
	}

	return indexScanCost
}

// ShouldUseIndex decides whether to use an index based on cost estimation.
func (o *Optimizer) ShouldUseIndex(tableName string, rowCount uint64, indexStats *IndexStatistics, selectivity float64) bool {
	// Calculate table scan cost
	tableScanCost := float64(rowCount) * 1.0 // Simplified

	// Calculate index scan cost
	var indexScanCost float64
	if indexStats != nil {
		estimatedRows := float64(rowCount) * selectivity
		indexScanCost = float64(indexStats.Height) + estimatedRows*1.2
		if indexStats.Clustered {
			indexScanCost = float64(indexStats.Height) + estimatedRows*0.5
		}
	} else {
		estimatedRows := float64(rowCount) * selectivity
		indexScanCost = 3.0 + estimatedRows*1.5
	}

	// Use index if it's cheaper
	// Also consider threshold: if selectivity is very low, always use index
	if selectivity <= 0.1 {
		return true
	}

	// If selectivity is very high, don't use index
	if selectivity >= 0.9 {
		return false
	}

	return indexScanCost < tableScanCost
}

// EstimateRows estimates the number of rows returned by a predicate.
func (o *Optimizer) EstimateRows(tableName string, rowCount uint64, selectivity float64) uint64 {
	estimated := uint64(float64(rowCount) * selectivity)
	if estimated < 1 && selectivity > 0 {
		return 1
	}
	if estimated > rowCount {
		return rowCount
	}
	return estimated
}

// ChooseIndex selects the best index for a query.
func (o *Optimizer) ChooseIndex(tableName string, where sql.Expression, availableIndexes map[string]*IndexStatistics) (string, PlanType) {
	if where == nil || len(availableIndexes) == 0 {
		return "", PlanTypeTableScan
	}

	// Extract columns from WHERE clause
	whereCols := extractColumnsFromExpr(where)
	if len(whereCols) == 0 {
		return "", PlanTypeTableScan
	}

	// Find matching indexes
	type indexMatch struct {
		name       string
		stats      *IndexStatistics
		matchCount int
		planType   PlanType
	}

	var matches []indexMatch

	for name, stats := range availableIndexes {
		matchCount := countPrefixMatch(stats.Columns, whereCols)
		if matchCount > 0 {
			planType := PlanTypeIndexScan
			if matchCount == 1 && isEqualityCondition(where, stats.Columns[0]) {
				planType = PlanTypeIndexPointLookup
			}
			matches = append(matches, indexMatch{
				name:       name,
				stats:      stats,
				matchCount: matchCount,
				planType:   planType,
			})
		}
	}

	if len(matches) == 0 {
		return "", PlanTypeTableScan
	}

	// Choose the best index (most matching columns, prefer clustered)
	best := matches[0]
	for _, m := range matches[1:] {
		if m.matchCount > best.matchCount {
			best = m
		} else if m.matchCount == best.matchCount {
			// Prefer clustered index
			if m.stats.Clustered && !best.stats.Clustered {
				best = m
			} else if m.stats.Clustered == best.stats.Clustered {
				// Prefer index with higher selectivity (lower distinct count)
				if m.stats.DistinctKeys > best.stats.DistinctKeys {
					best = m
				}
			}
		}
	}

	return best.name, best.planType
}

// extractColumnsFromExpr extracts column names from an expression.
func extractColumnsFromExpr(expr sql.Expression) []string {
	var cols []string

	switch e := expr.(type) {
	case *sql.ColumnRef:
		cols = append(cols, e.Name)
	case *sql.BinaryExpr:
		cols = append(cols, extractColumnsFromExpr(e.Left)...)
		cols = append(cols, extractColumnsFromExpr(e.Right)...)
	case *sql.UnaryExpr:
		cols = append(cols, extractColumnsFromExpr(e.Right)...)
	case *sql.ParenExpr:
		cols = append(cols, extractColumnsFromExpr(e.Expr)...)
	case *sql.FunctionCall:
		for _, arg := range e.Args {
			cols = append(cols, extractColumnsFromExpr(arg)...)
		}
	}

	return cols
}

// countPrefixMatch counts how many columns in the index match the prefix of the query columns.
func countPrefixMatch(indexCols, queryCols []string) int {
	match := 0
	for i := 0; i < len(indexCols) && i < len(queryCols); i++ {
		if indexCols[i] == queryCols[i] {
			match++
		} else {
			break
		}
	}
	return match
}

// isEqualityCondition checks if the WHERE clause contains an equality condition on a column.
func isEqualityCondition(where sql.Expression, column string) bool {
	switch e := where.(type) {
	case *sql.BinaryExpr:
		if e.Op == sql.OpEq {
			if col, ok := e.Left.(*sql.ColumnRef); ok && col.Name == column {
				return true
			}
			if col, ok := e.Right.(*sql.ColumnRef); ok && col.Name == column {
				return true
			}
		}
		return isEqualityCondition(e.Left, column) || isEqualityCondition(e.Right, column)
	case *sql.ParenExpr:
		return isEqualityCondition(e.Expr, column)
	}
	return false
}

// SelectivityThreshold is the threshold for using an index.
const SelectivityThreshold = 0.3

// IsLowSelectivity returns true if the selectivity is below the threshold.
func IsLowSelectivity(selectivity float64) bool {
	return selectivity <= SelectivityThreshold
}

// CalculateSelectivityFromConditions calculates overall selectivity from multiple conditions.
func CalculateSelectivityFromConditions(selectivities []float64) float64 {
	if len(selectivities) == 0 {
		return 1.0
	}

	// Assume conditions are ANDed together
	result := 1.0
	for _, s := range selectivities {
		result *= s
	}

	// Ensure result is within bounds
	return math.Max(0.0, math.Min(1.0, result))
}