// Package optimizer provides query optimization for XxSql.
package optimizer

import (
	"sort"

	"github.com/topxeq/xxsql/internal/storage/types"
)

// AnalyzeTable analyzes a table and collects statistics.
type AnalyzeTable struct {
	Stats *Statistics
}

// NewAnalyzeTable creates a new analyzer.
func NewAnalyzeTable() *AnalyzeTable {
	return &AnalyzeTable{
		Stats: &Statistics{
			ColumnStats: make(map[string]*ColumnStatistics),
			IndexStats:  make(map[string]*IndexStatistics),
		},
	}
}

// AddRow adds a row to the statistics.
func (a *AnalyzeTable) AddRow(columns []string, values []types.Value) {
	a.Stats.RowCount++

	for i, col := range columns {
		if i >= len(values) {
			continue
		}

		val := values[i]
		stats, ok := a.Stats.ColumnStats[col]
		if !ok {
			stats = &ColumnStatistics{
				Name: col,
			}
			a.Stats.ColumnStats[col] = stats
		}

		// Track null count
		if val.Null {
			stats.NullCount++
			continue
		}

		// Update min/max
		if stats.MinValue.Data == nil {
			stats.MinValue = val
			stats.MaxValue = val
		} else {
			if val.Compare(stats.MinValue) < 0 {
				stats.MinValue = val
			}
			if val.Compare(stats.MaxValue) > 0 {
				stats.MaxValue = val
			}
		}

		// Track distinct values (simplified: just count)
		stats.DistinctCount++
	}
}

// Finalize completes the analysis and returns the statistics.
func (a *AnalyzeTable) Finalize() *Statistics {
	// Adjust distinct count estimates
	// In reality, we'd use a more sophisticated algorithm (hyperloglog, etc.)
	// For now, we'll estimate distinct count as min(sqrt(row_count), actual_tracked)
	for _, stats := range a.Stats.ColumnStats {
		// Cap distinct count at row count
		if stats.DistinctCount > a.Stats.RowCount {
			stats.DistinctCount = a.Stats.RowCount
		}
	}

	return a.Stats
}

// SetIndexStatistics sets statistics for an index.
func (a *AnalyzeTable) SetIndexStatistics(name string, columns []string, distinctKeys uint64, height int, clustered bool) {
	a.Stats.IndexStats[name] = &IndexStatistics{
		Name:         name,
		Columns:      columns,
		DistinctKeys: distinctKeys,
		Height:       height,
		Clustered:    clustered,
	}
}

// BuildHistogram builds a histogram for a column.
func (a *AnalyzeTable) BuildHistogram(column string, values []types.Value, numBuckets int) {
	if len(values) == 0 {
		return
	}

	stats, ok := a.Stats.ColumnStats[column]
	if !ok {
		return
	}

	// Sort values
	sorted := make([]types.Value, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Compare(sorted[j]) < 0
	})

	// Build equi-height histogram
	bucketSize := len(sorted) / numBuckets
	if bucketSize < 1 {
		bucketSize = 1
	}

	stats.Histogram = make([]HistogramBucket, 0, numBuckets)

	for i := 0; i < len(sorted); i += bucketSize {
		end := i + bucketSize
		if end > len(sorted) {
			end = len(sorted)
		}

		bucket := HistogramBucket{
			LowerBound: sorted[i],
			UpperBound: sorted[end-1],
			RowCount:   uint64(end - i),
		}

		// Count distinct values in bucket
		distinct := 1
		for j := i + 1; j < end; j++ {
			if sorted[j].Compare(sorted[j-1]) != 0 {
				distinct++
			}
		}
		bucket.DistinctCount = uint64(distinct)

		stats.Histogram = append(stats.Histogram, bucket)
	}
}

// EstimateColumnSelectivity estimates selectivity using histogram.
func (s *Statistics) EstimateColumnSelectivity(column string, op string, value types.Value) float64 {
	stats, ok := s.ColumnStats[column]
	if !ok {
		return 0.1
	}

	// Use histogram for better estimation
	if len(stats.Histogram) > 0 {
		return estimateFromHistogram(stats.Histogram, op, value, s.RowCount)
	}

	// Fallback to simple estimation
	switch op {
	case "=":
		if stats.DistinctCount > 0 {
			return 1.0 / float64(stats.DistinctCount)
		}
		return 0.1
	case "<", "<=":
		return 0.33
	case ">", ">=":
		return 0.33
	case "<>", "!=":
		if stats.DistinctCount > 0 {
			return 1.0 - 1.0/float64(stats.DistinctCount)
		}
		return 0.9
	default:
		return 0.5
	}
}

// estimateFromHistogram estimates selectivity using histogram buckets.
func estimateFromHistogram(histogram []HistogramBucket, op string, value types.Value, totalRows uint64) float64 {
	if totalRows == 0 {
		return 0
	}

	var matchingRows uint64

	for _, bucket := range histogram {
		cmpLower := value.Compare(bucket.LowerBound)
		cmpUpper := value.Compare(bucket.UpperBound)

		switch op {
		case "=":
			if cmpLower >= 0 && cmpUpper <= 0 {
				// Value is in this bucket
				matchingRows += bucket.RowCount / bucket.DistinctCount
			}
		case "<":
			if cmpUpper < 0 {
				// All values in bucket are greater than value
				continue
			}
			if cmpLower < 0 {
				// Value is after this bucket
				matchingRows += bucket.RowCount
			} else {
				// Value is in this bucket, estimate fraction
				matchingRows += bucket.RowCount / 2
			}
		case "<=":
			if cmpUpper <= 0 {
				matchingRows += bucket.RowCount
			} else if cmpLower <= 0 {
				matchingRows += bucket.RowCount / 2
			}
		case ">":
			if cmpLower > 0 {
				matchingRows += bucket.RowCount
			} else if cmpUpper > 0 {
				matchingRows += bucket.RowCount / 2
			}
		case ">=":
			if cmpLower >= 0 {
				matchingRows += bucket.RowCount
			} else if cmpUpper >= 0 {
				matchingRows += bucket.RowCount / 2
			}
		}
	}

	return float64(matchingRows) / float64(totalRows)
}