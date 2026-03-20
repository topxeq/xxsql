package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// TestJulianDayFunction tests JULIANDAY function
func TestJulianDayFunction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-julianday-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE dates (
			id SEQ PRIMARY KEY,
			event VARCHAR(100),
			event_date VARCHAR(50)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = exec.Execute(`INSERT INTO dates (event, event_date) VALUES ('Launch', '2024-01-15')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test JULIANDAY
	result, err := exec.Execute(`
		SELECT event, event_date, JULIANDAY(event_date) AS jd
		FROM dates
	`)
	if err != nil {
		t.Fatalf("Failed to execute JULIANDAY query: %v", err)
	}

	t.Logf("JULIANDAY results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Verify Julian day is returned (should be around 2460325 for 2024-01-15)
	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
	jd, ok := result.Rows[0][2].(float64)
	if !ok {
		t.Errorf("Expected float64 for Julian day, got %T", result.Rows[0][2])
	} else if jd < 2460000 || jd > 2470000 {
		t.Errorf("Julian day %v seems out of range for 2024", jd)
	}
}

// TestDateModifyFunction tests DATE_MODIFY function
func TestDateModifyFunction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-datemodify-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE deadlines (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			base_date VARCHAR(50)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = exec.Execute(`INSERT INTO deadlines (name, base_date) VALUES ('Project', '2024-01-15')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test DATE_MODIFY with +7 days
	result, err := exec.Execute(`
		SELECT name, base_date,
		       DATE_MODIFY(base_date, '+7 days') AS deadline_plus_7,
		       DATE_MODIFY(base_date, '-1 month') AS deadline_minus_month
		FROM deadlines
	`)
	if err != nil {
		t.Fatalf("Failed to execute DATE_MODIFY query: %v", err)
	}

	t.Logf("DATE_MODIFY results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// Verify results
	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
	// +7 days from 2024-01-15 should be 2024-01-22 (may include time component)
	plus7 := result.Rows[0][2].(string)
	if plus7 != "2024-01-22" && plus7 != "2024-01-22 00:00:00" {
		t.Errorf("Expected 2024-01-22 for +7 days, got %v", plus7)
	}
}

// TestUnixTimestampFunctions tests UNIX_TIMESTAMP and FROM_UNIXTIME
func TestUnixTimestampFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-unixtime-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Test UNIX_TIMESTAMP without argument (current time)
	result, err := exec.Execute(`SELECT UNIX_TIMESTAMP() AS now_ts`)
	if err != nil {
		t.Fatalf("Failed to execute UNIX_TIMESTAMP: %v", err)
	}

	t.Logf("UNIX_TIMESTAMP() result: %v", result.Rows[0])

	// Test UNIX_TIMESTAMP with date string
	result, err = exec.Execute(`SELECT UNIX_TIMESTAMP('2024-01-15 12:00:00') AS ts`)
	if err != nil {
		t.Fatalf("Failed to execute UNIX_TIMESTAMP with date: %v", err)
	}

	t.Logf("UNIX_TIMESTAMP('2024-01-15 12:00:00') result: %v", result.Rows[0])
	ts := result.Rows[0][0].(int64)

	// Test FROM_UNIXTIME
	result, err = exec.Execute(fmt.Sprintf(`SELECT FROM_UNIXTIME(%d) AS date_str`, ts))
	if err != nil {
		t.Fatalf("Failed to execute FROM_UNIXTIME: %v", err)
	}

	t.Logf("FROM_UNIXTIME result: %v", result.Rows[0])
}

// TestDateExtractionFunctions tests QUARTER, WEEK, WEEKDAY, etc.
func TestDateExtractionFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-dateextract-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE events (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			event_date VARCHAR(50)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = exec.Execute(`INSERT INTO events (name, event_date) VALUES ('Event1', '2024-06-15')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test date extraction functions
	result, err := exec.Execute(`
		SELECT name, event_date,
		       QUARTER(event_date) AS q,
		       WEEK(event_date) AS w,
		       WEEKDAY(event_date) AS wd,
		       DAYOFYEAR(event_date) AS doy,
		       DAYOFMONTH(event_date) AS dom,
		       DAYOFWEEK(event_date) AS dow,
		       LAST_DAY(event_date) AS last_day
		FROM events
	`)
	if err != nil {
		t.Fatalf("Failed to execute date extraction query: %v", err)
	}

	t.Logf("Date extraction results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	// 2024-06-15 is in Q2
	if result.Rows[0][2].(int) != 2 {
		t.Errorf("Expected quarter 2, got %v", result.Rows[0][2])
	}

	// June 15 is day 15 of month
	if result.Rows[0][6].(int) != 15 {
		t.Errorf("Expected day of month 15, got %v", result.Rows[0][6])
	}
}

// TestTimestampDiffFunction tests TIMESTAMPDIFF function
func TestTimestampDiffFunction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-timestampdiff-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Test TIMESTAMPDIFF in days
	result, err := exec.Execute(`
		SELECT TIMESTAMPDIFF('DAY', '2024-01-01', '2024-01-15') AS days_diff
	`)
	if err != nil {
		t.Fatalf("Failed to execute TIMESTAMPDIFF: %v", err)
	}

	t.Logf("TIMESTAMPDIFF DAY result: %v", result.Rows[0])
	if result.Rows[0][0].(int64) != 14 {
		t.Errorf("Expected 14 days difference, got %v", result.Rows[0][0])
	}

	// Test TIMESTAMPDIFF in hours
	result, err = exec.Execute(`
		SELECT TIMESTAMPDIFF('HOUR', '2024-01-01 00:00:00', '2024-01-01 05:00:00') AS hours_diff
	`)
	if err != nil {
		t.Fatalf("Failed to execute TIMESTAMPDIFF HOUR: %v", err)
	}

	t.Logf("TIMESTAMPDIFF HOUR result: %v", result.Rows[0])
	if result.Rows[0][0].(int64) != 5 {
		t.Errorf("Expected 5 hours difference, got %v", result.Rows[0][0])
	}
}

// TestCombinedPhase5Features tests combinations of Phase 5 features
func TestCombinedPhase5Features(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-phase5-combined-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE schedule (
			id SEQ PRIMARY KEY,
			task VARCHAR(100),
			start_date VARCHAR(50),
			duration_days INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	tasks := []struct {
		taskName     string
		startDate    string
		durationDays int
	}{
		{"Task1", "2024-01-01", 7},
		{"Task2", "2024-01-15", 14},
		{"Task3", "2024-02-01", 30},
	}
	for _, task := range tasks {
		_, err = exec.Execute(fmt.Sprintf(`INSERT INTO schedule (task, start_date, duration_days) VALUES ('%s', '%s', %d)`, task.taskName, task.startDate, task.durationDays))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Combine multiple date functions
	result, err := exec.Execute(`
		SELECT task, start_date, duration_days,
		       DATE_MODIFY(start_date, '+' || duration_days || ' days') AS end_date,
		       QUARTER(start_date) AS quarter,
		       JULIANDAY(DATE_MODIFY(start_date, '+' || duration_days || ' days')) - JULIANDAY(start_date) AS calc_duration
		FROM schedule
		ORDER BY start_date
	`)
	if err != nil {
		t.Fatalf("Failed to execute combined query: %v", err)
	}

	t.Logf("Combined Phase 5 results:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}

	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}
}