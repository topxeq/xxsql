package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

// OutputFormat represents the output format type.
type OutputFormat int

const (
	FormatTable    OutputFormat = iota // ASCII table format
	FormatVertical                     // Vertical format (column: value)
	FormatJSON                         // JSON format
	FormatTSV                          // Tab-separated values
)

// String returns the string representation of the output format.
func (f OutputFormat) String() string {
	switch f {
	case FormatTable:
		return "table"
	case FormatVertical:
		return "vertical"
	case FormatJSON:
		return "json"
	case FormatTSV:
		return "tsv"
	default:
		return "unknown"
	}
}

// parseOutputFormat parses a string to OutputFormat.
func parseOutputFormat(s string) OutputFormat {
	switch strings.ToLower(s) {
	case "vertical", "v":
		return FormatVertical
	case "json", "j":
		return FormatJSON
	case "tsv", "t":
		return FormatTSV
	default:
		return FormatTable
	}
}

// Formatter defines the interface for output formatters.
type Formatter interface {
	Format(columns []string, rows [][]interface{})
}

// getFormatter returns the appropriate formatter for the output format.
func getFormatter(format OutputFormat) Formatter {
	switch format {
	case FormatVertical:
		return &verticalFormatter{}
	case FormatJSON:
		return &jsonFormatter{}
	case FormatTSV:
		return &tsvFormatter{}
	default:
		return &tableFormatter{}
	}
}

// tableFormatter formats output as ASCII table.
type tableFormatter struct{}

func (f *tableFormatter) Format(columns []string, rows [][]interface{}) {
	if len(columns) == 0 {
		return
	}

	// Calculate column widths
	widths := make([]int, len(columns))
	for i, col := range columns {
		widths[i] = len(col)
	}

	for _, row := range rows {
		for i, val := range row {
			s := formatValue(val)
			if len(s) > widths[i] {
				widths[i] = len(s)
			}
		}
	}

	// Limit column width
	maxWidth := 50
	for i := range widths {
		if widths[i] > maxWidth {
			widths[i] = maxWidth
		}
	}

	// Print top border
	fmt.Print("+")
	for _, w := range widths {
		fmt.Print(strings.Repeat("-", w+2), "+")
	}
	fmt.Println()

	// Print header
	fmt.Print("|")
	for i, col := range columns {
		fmt.Print(" ", padRight(col, widths[i]), " |")
	}
	fmt.Println()

	// Print header separator
	fmt.Print("+")
	for _, w := range widths {
		fmt.Print(strings.Repeat("-", w+2), "+")
	}
	fmt.Println()

	// Print rows
	for _, row := range rows {
		fmt.Print("|")
		for i, val := range row {
			s := formatValue(val)
			if len(s) > widths[i] {
				s = s[:widths[i]-3] + "..."
			}
			fmt.Print(" ", padRight(s, widths[i]), " |")
		}
		fmt.Println()
	}

	// Print bottom border
	fmt.Print("+")
	for _, w := range widths {
		fmt.Print(strings.Repeat("-", w+2), "+")
	}
	fmt.Println()
}

// verticalFormatter formats output vertically.
type verticalFormatter struct{}

func (f *verticalFormatter) Format(columns []string, rows [][]interface{}) {
	if len(columns) == 0 {
		return
	}

	// Calculate max column name width
	maxWidth := 0
	for _, col := range columns {
		if len(col) > maxWidth {
			maxWidth = len(col)
		}
	}

	for rowIdx, row := range rows {
		fmt.Printf("*************************** %d. row ***************************\n", rowIdx+1)
		for i, col := range columns {
			fmt.Printf("  %s: %s\n", padRight(col, maxWidth), formatValue(row[i]))
		}
	}
}

// jsonFormatter formats output as JSON.
type jsonFormatter struct{}

func (f *jsonFormatter) Format(columns []string, rows [][]interface{}) {
	if len(columns) == 0 {
		fmt.Println("[]")
		return
	}

	// Build array of objects
	result := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		obj := make(map[string]interface{})
		for j, col := range columns {
			obj[col] = row[j]
		}
		result[i] = obj
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fmt.Println("[]")
		return
	}
	fmt.Println(string(data))
}

// tsvFormatter formats output as tab-separated values.
type tsvFormatter struct{}

func (f *tsvFormatter) Format(columns []string, rows [][]interface{}) {
	if len(columns) == 0 {
		return
	}

	// Print header
	fmt.Println(strings.Join(columns, "\t"))

	// Print rows
	for _, row := range rows {
		values := make([]string, len(row))
		for i, val := range row {
			values[i] = formatValue(val)
		}
		fmt.Println(strings.Join(values, "\t"))
	}
}

// formatValue formats a value for display.
func formatValue(val interface{}) string {
	if val == nil {
		return "NULL"
	}

	switch v := val.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case int, int32, int64, uint, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// padRight pads a string with spaces on the right.
func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}
