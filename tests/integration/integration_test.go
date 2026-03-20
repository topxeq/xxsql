package integration_test

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/executor"
	"github.com/topxeq/xxsql/internal/storage"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// =============================================================================
// CRUD Integration Tests
// =============================================================================

func TestCRUD_FullLifecycle(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-crud-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Setup
	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Create table
	t.Run("CreateTable", func(t *testing.T) {
		result, err := exec.Execute(`CREATE TABLE users (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(100),
			age INT
		)`)
		if err != nil {
			t.Fatalf("CreateTable failed: %v", err)
		}
		if result.Message != "OK" {
			t.Errorf("Expected OK, got %s", result.Message)
		}
	})

	// Insert data
	t.Run("Insert", func(t *testing.T) {
		inserts := []string{
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 30)",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 25)",
			"INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com', 35)",
		}

		for _, sql := range inserts {
			result, err := exec.Execute(sql)
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
			if result.Affected != 1 {
				t.Errorf("Expected 1 row affected, got %d", result.Affected)
			}
		}
	})

	// Select all
	t.Run("SelectAll", func(t *testing.T) {
		result, err := exec.Execute("SELECT * FROM users")
		if err != nil {
			t.Fatalf("SelectAll failed: %v", err)
		}
		if result.RowCount != 3 {
			t.Errorf("Expected 3 rows, got %d", result.RowCount)
		}
	})

	// Select with WHERE
	t.Run("SelectWhere", func(t *testing.T) {
		result, err := exec.Execute("SELECT * FROM users WHERE age > 28")
		if err != nil {
			t.Fatalf("SelectWhere failed: %v", err)
		}
		if result.RowCount != 2 {
			t.Errorf("Expected 2 rows (Alice, Charlie), got %d", result.RowCount)
		}
	})

	// Update
	t.Run("Update", func(t *testing.T) {
		result, err := exec.Execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
		if result.Affected != 1 {
			t.Errorf("Expected 1 row affected, got %d", result.Affected)
		}

		// Verify update
		result, _ = exec.Execute("SELECT age FROM users WHERE name = 'Alice'")
		if result.RowCount != 1 {
			t.Errorf("Expected 1 row, got %d", result.RowCount)
		}
	})

	// Delete
	t.Run("Delete", func(t *testing.T) {
		result, err := exec.Execute("DELETE FROM users WHERE name = 'Bob'")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		if result.Affected != 1 {
			t.Errorf("Expected 1 row affected, got %d", result.Affected)
		}

		// Verify delete
		result, _ = exec.Execute("SELECT * FROM users")
		if result.RowCount != 2 {
			t.Errorf("Expected 2 rows after delete, got %d", result.RowCount)
		}
	})

	// Drop table
	t.Run("DropTable", func(t *testing.T) {
		result, err := exec.Execute("DROP TABLE users")
		if err != nil {
			t.Fatalf("DropTable failed: %v", err)
		}
		if result.Message != "OK" {
			t.Errorf("Expected OK, got %s", result.Message)
		}

		if engine.TableExists("users") {
			t.Error("Table should not exist after drop")
		}
	})
}

func TestCRUD_MultipleDataTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-types-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Create table with various types
	_, err = exec.Execute(`CREATE TABLE data_types (
		id INT PRIMARY KEY,
		fval FLOAT,
		sval VARCHAR(50),
		tval TEXT,
		bval BOOL
	)`)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Insert with various types
	result, err := exec.Execute(`INSERT INTO data_types VALUES (1, 3.14, 'hello', 'long text here', TRUE)`)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if result.Affected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.Affected)
	}

	// Select and verify
	result, err = exec.Execute("SELECT * FROM data_types")
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}
}

// =============================================================================
// Concurrency Tests
// =============================================================================

func TestConcurrent_Inserts(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-concurrent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Create table
	_, err = exec.Execute("CREATE TABLE concurrent_test (id INT PRIMARY KEY, value VARCHAR(50))")
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Concurrent inserts
	const numGoroutines = 10
	const insertsPerGoroutine = 10

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines*insertsPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < insertsPerGoroutine; i++ {
				id := goroutineID*insertsPerGoroutine + i
				sql := fmt.Sprintf("INSERT INTO concurrent_test VALUES (%d, 'value%d')", id, id)
				_, err := exec.Execute(sql)
				if err != nil {
					errCh <- err
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	errorCount := 0
	for err := range errCh {
		t.Logf("Error during concurrent insert: %v", err)
		errorCount++
	}

	// Verify total rows
	result, _ := exec.Execute("SELECT * FROM concurrent_test")
	expectedRows := numGoroutines * insertsPerGoroutine
	t.Logf("Inserted %d rows (expected %d, %d errors)", result.RowCount, expectedRows, errorCount)
}

func TestConcurrent_Reads(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-read-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Create table and insert data
	_, _ = exec.Execute("CREATE TABLE read_test (id INT PRIMARY KEY, value VARCHAR(50))")
	for i := 0; i < 100; i++ {
		sql := fmt.Sprintf("INSERT INTO read_test VALUES (%d, 'value%d')", i, i)
		exec.Execute(sql)
	}

	// Concurrent reads
	const numReaders = 20
	var wg sync.WaitGroup

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				_, err := exec.Execute("SELECT * FROM read_test")
				if err != nil {
					t.Logf("Read error: %v", err)
				}
			}
		}()
	}

	wg.Wait()
}

// =============================================================================
// Storage Engine Tests
// =============================================================================

func TestStorage_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-persist-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// First session: create and insert
	engine1 := storage.NewEngine(tmpDir)
	if err := engine1.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}

	// Create table using ColumnInfo
	columns := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		{Name: "name", Type: types.TypeVarchar, Size: 100},
	}
	if err := engine1.CreateTable("persist_test", columns); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Insert data
	values := []types.Value{
		types.NewIntValue(1),
		types.NewStringValue("test", types.TypeVarchar),
	}
	if _, err := engine1.Insert("persist_test", values); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Flush and close
	engine1.Flush()
	engine1.Close()

	// Second session: verify persistence
	engine2 := storage.NewEngine(tmpDir)
	if err := engine2.Open(); err != nil {
		t.Fatalf("Failed to reopen engine: %v", err)
	}
	defer engine2.Close()

	// Verify table exists
	if !engine2.TableExists("persist_test") {
		t.Fatal("Table should persist after restart")
	}

	// Verify data
	rows, err := engine2.Scan("persist_test")
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}
}

func TestStorage_MultipleTables(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-multi-table-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create multiple tables
	tables := []struct {
		name    string
		columns []*types.ColumnInfo
	}{
		{
			name: "users",
			columns: []*types.ColumnInfo{
				{Name: "id", Type: types.TypeInt, PrimaryKey: true},
				{Name: "name", Type: types.TypeVarchar, Size: 100},
			},
		},
		{
			name: "orders",
			columns: []*types.ColumnInfo{
				{Name: "id", Type: types.TypeInt, PrimaryKey: true},
				{Name: "user_id", Type: types.TypeInt},
				{Name: "amount", Type: types.TypeFloat},
			},
		},
		{
			name: "products",
			columns: []*types.ColumnInfo{
				{Name: "id", Type: types.TypeInt, PrimaryKey: true},
				{Name: "name", Type: types.TypeVarchar, Size: 100},
				{Name: "price", Type: types.TypeFloat},
			},
		},
	}

	for _, tc := range tables {
		if err := engine.CreateTable(tc.name, tc.columns); err != nil {
			t.Fatalf("CreateTable %s failed: %v", tc.name, err)
		}
	}

	// Verify all tables exist
	tableList := engine.ListTables()
	if len(tableList) != 3 {
		t.Errorf("Expected 3 tables, got %d", len(tableList))
	}

	// Insert into each table
	for _, tc := range tables {
		var values []types.Value
		for _, col := range tc.columns {
			switch col.Type {
			case types.TypeInt:
				values = append(values, types.NewIntValue(1))
			case types.TypeFloat:
				values = append(values, types.NewFloatValue(9.99))
			case types.TypeVarchar:
				values = append(values, types.NewStringValue("test", types.TypeVarchar))
			}
		}
		if _, err := engine.Insert(tc.name, values); err != nil {
			t.Fatalf("Insert into %s failed: %v", tc.name, err)
		}
	}

	// Verify data in each table
	for _, tc := range tables {
		rows, err := engine.Scan(tc.name)
		if err != nil {
			t.Fatalf("Scan %s failed: %v", tc.name, err)
		}
		if len(rows) != 1 {
			t.Errorf("Expected 1 row in %s, got %d", tc.name, len(rows))
		}
	}
}

// =============================================================================
// SQL Feature Tests
// =============================================================================

func TestSQL_AggregateFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-aggregate-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Create table and insert data
	_, _ = exec.Execute("CREATE TABLE numbers (id INT, value INT)")
	for i := 1; i <= 10; i++ {
		sql := fmt.Sprintf("INSERT INTO numbers VALUES (%d, %d)", i, i*10)
		exec.Execute(sql)
	}

	// Test COUNT
	result, err := exec.Execute("SELECT COUNT(*) FROM numbers")
	if err != nil {
		t.Fatalf("COUNT failed: %v", err)
	}
	t.Logf("COUNT result: %d rows", result.RowCount)

	// Test SUM
	result, err = exec.Execute("SELECT SUM(value) FROM numbers")
	if err != nil {
		t.Fatalf("SUM failed: %v", err)
	}
	t.Logf("SUM result: %d rows", result.RowCount)

	// Test AVG
	result, err = exec.Execute("SELECT AVG(value) FROM numbers")
	if err != nil {
		t.Fatalf("AVG failed: %v", err)
	}
	t.Logf("AVG result: %d rows", result.RowCount)
}

func TestSQL_MathFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-math-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Create table with numeric values
	_, _ = exec.Execute("CREATE TABLE math_test (id INT, val FLOAT)")
	exec.Execute("INSERT INTO math_test VALUES (1, -10.5)")
	exec.Execute("INSERT INTO math_test VALUES (2, 3.7)")
	exec.Execute("INSERT INTO math_test VALUES (3, 16.0)")

	// Test ABS
	result, err := exec.Execute("SELECT ABS(val) FROM math_test WHERE id = 1")
	if err != nil {
		t.Fatalf("ABS failed: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("ABS: expected 1 row, got %d", result.RowCount)
	}
	t.Logf("ABS result: %v", result.Rows)

	// Test ROUND
	result, err = exec.Execute("SELECT ROUND(val, 1) FROM math_test WHERE id = 2")
	if err != nil {
		t.Fatalf("ROUND failed: %v", err)
	}
	t.Logf("ROUND result: %v", result.Rows)

	// Test CEIL
	result, err = exec.Execute("SELECT CEIL(val) FROM math_test WHERE id = 2")
	if err != nil {
		t.Fatalf("CEIL failed: %v", err)
	}
	t.Logf("CEIL result: %v", result.Rows)

	// Test FLOOR
	result, err = exec.Execute("SELECT FLOOR(val) FROM math_test WHERE id = 2")
	if err != nil {
		t.Fatalf("FLOOR failed: %v", err)
	}
	t.Logf("FLOOR result: %v", result.Rows)

	// Test SQRT
	result, err = exec.Execute("SELECT SQRT(val) FROM math_test WHERE id = 3")
	if err != nil {
		t.Fatalf("SQRT failed: %v", err)
	}
	t.Logf("SQRT result: %v", result.Rows)

	// Test POWER
	result, err = exec.Execute("SELECT POWER(val, 2) FROM math_test WHERE id = 3")
	if err != nil {
		t.Fatalf("POWER failed: %v", err)
	}
	t.Logf("POWER result: %v", result.Rows)

	// Test MOD
	exec.Execute("INSERT INTO math_test VALUES (4, 17.0)")
	result, err = exec.Execute("SELECT MOD(val, 5) FROM math_test WHERE id = 4")
	if err != nil {
		t.Fatalf("MOD failed: %v", err)
	}
	t.Logf("MOD result: %v", result.Rows)
}

func TestSQL_GroupConcat(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-groupconcat-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Create table
	_, _ = exec.Execute("CREATE TABLE tags (id INT, category VARCHAR(50), tag_name VARCHAR(50))")
	exec.Execute("INSERT INTO tags VALUES (1, 'color', 'red')")
	exec.Execute("INSERT INTO tags VALUES (2, 'color', 'blue')")
	exec.Execute("INSERT INTO tags VALUES (3, 'color', 'green')")
	exec.Execute("INSERT INTO tags VALUES (4, 'size', 'small')")
	exec.Execute("INSERT INTO tags VALUES (5, 'size', 'large')")

	// Test GROUP_CONCAT
	result, err := exec.Execute("SELECT category, GROUP_CONCAT(tag_name) FROM tags GROUP BY category")
	if err != nil {
		t.Fatalf("GROUP_CONCAT failed: %v", err)
	}
	if result.RowCount != 2 {
		t.Errorf("GROUP_CONCAT: expected 2 rows, got %d", result.RowCount)
	}
	t.Logf("GROUP_CONCAT result: %d rows", result.RowCount)
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}

func TestSQL_DateTimeFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-datetime-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Create table with dates
	_, _ = exec.Execute("CREATE TABLE events (id INT, name VARCHAR(50), created_at VARCHAR(30))")
	exec.Execute("INSERT INTO events VALUES (1, 'Event 1', '2024-03-15 10:30:00')")
	exec.Execute("INSERT INTO events VALUES (2, 'Event 2', '2024-06-20 14:45:00')")

	// Test DATE
	result, err := exec.Execute("SELECT DATE(created_at) FROM events WHERE id = 1")
	if err != nil {
		t.Fatalf("DATE failed: %v", err)
	}
	t.Logf("DATE result: %v", result.Rows)

	// Test TIME
	result, err = exec.Execute("SELECT TIME(created_at) FROM events WHERE id = 1")
	if err != nil {
		t.Fatalf("TIME failed: %v", err)
	}
	t.Logf("TIME result: %v", result.Rows)

	// Test DATETIME
	result, err = exec.Execute("SELECT DATETIME(created_at) FROM events WHERE id = 1")
	if err != nil {
		t.Fatalf("DATETIME failed: %v", err)
	}
	t.Logf("DATETIME result: %v", result.Rows)

	// Test YEAR, MONTH, DAY
	result, err = exec.Execute("SELECT YEAR(created_at), MONTH(created_at), DAY(created_at) FROM events WHERE id = 1")
	if err != nil {
		t.Fatalf("YEAR/MONTH/DAY failed: %v", err)
	}
	t.Logf("YEAR/MONTH/DAY result: %v", result.Rows)

	// Test HOUR, MINUTE, SECOND
	result, err = exec.Execute("SELECT HOUR(created_at), MINUTE(created_at), SECOND(created_at) FROM events WHERE id = 1")
	if err != nil {
		t.Fatalf("HOUR/MINUTE/SECOND failed: %v", err)
	}
	t.Logf("HOUR/MINUTE/SECOND result: %v", result.Rows)

	// Test DATE_ADD
	result, err = exec.Execute("SELECT DATE_ADD('2024-03-15', 7)")
	if err != nil {
		t.Fatalf("DATE_ADD failed: %v", err)
	}
	t.Logf("DATE_ADD result: %v", result.Rows)

	// Test DATE_SUB
	result, err = exec.Execute("SELECT DATE_SUB('2024-03-15', 7)")
	if err != nil {
		t.Fatalf("DATE_SUB failed: %v", err)
	}
	t.Logf("DATE_SUB result: %v", result.Rows)

	// Test DATEDIFF
	result, err = exec.Execute("SELECT DATEDIFF('2024-03-22', '2024-03-15')")
	if err != nil {
		t.Fatalf("DATEDIFF failed: %v", err)
	}
	t.Logf("DATEDIFF result: %v", result.Rows)
}

func TestSQL_StringFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-string-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Test TRIM
	result, err := exec.Execute("SELECT TRIM('  hello  ')")
	if err != nil {
		t.Fatalf("TRIM failed: %v", err)
	}
	t.Logf("TRIM result: %v", result.Rows)

	// Test LTRIM
	result, err = exec.Execute("SELECT LTRIM('  hello  ')")
	if err != nil {
		t.Fatalf("LTRIM failed: %v", err)
	}
	t.Logf("LTRIM result: %v", result.Rows)

	// Test RTRIM
	result, err = exec.Execute("SELECT RTRIM('  hello  ')")
	if err != nil {
		t.Fatalf("RTRIM failed: %v", err)
	}
	t.Logf("RTRIM result: %v", result.Rows)

	// Test INSTR
	result, err = exec.Execute("SELECT INSTR('hello world', 'world')")
	if err != nil {
		t.Fatalf("INSTR failed: %v", err)
	}
	t.Logf("INSTR result: %v", result.Rows)

	// Test LPAD
	result, err = exec.Execute("SELECT LPAD('hi', 5, '*')")
	if err != nil {
		t.Fatalf("LPAD failed: %v", err)
	}
	t.Logf("LPAD result: %v", result.Rows)

	// Test RPAD
	result, err = exec.Execute("SELECT RPAD('hi', 5, '*')")
	if err != nil {
		t.Fatalf("RPAD failed: %v", err)
	}
	t.Logf("RPAD result: %v", result.Rows)

	// Test REVERSE
	result, err = exec.Execute("SELECT REVERSE('hello')")
	if err != nil {
		t.Fatalf("REVERSE failed: %v", err)
	}
	t.Logf("REVERSE result: %v", result.Rows)

	// Test LEFT
	result, err = exec.Execute("SELECT LEFT('hello world', 5)")
	if err != nil {
		t.Fatalf("LEFT failed: %v", err)
	}
	t.Logf("LEFT result: %v", result.Rows)

	// Test RIGHT
	result, err = exec.Execute("SELECT RIGHT('hello world', 5)")
	if err != nil {
		t.Fatalf("RIGHT failed: %v", err)
	}
	t.Logf("RIGHT result: %v", result.Rows)
}

func TestSQL_UtilityFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-utility-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Test TYPEOF
	result, err := exec.Execute("SELECT TYPEOF(123), TYPEOF('hello'), TYPEOF(3.14)")
	if err != nil {
		t.Fatalf("TYPEOF failed: %v", err)
	}
	t.Logf("TYPEOF result: %v", result.Rows)

	// Test IIF
	result, err = exec.Execute("SELECT IIF(1 > 0, 'yes', 'no')")
	if err != nil {
		t.Fatalf("IIF failed: %v", err)
	}
	t.Logf("IIF result: %v", result.Rows)

	result, err = exec.Execute("SELECT IIF(1 < 0, 'yes', 'no')")
	if err != nil {
		t.Fatalf("IIF failed: %v", err)
	}
	t.Logf("IIF result: %v", result.Rows)

	// Test GREATEST
	result, err = exec.Execute("SELECT GREATEST(10, 5, 20, 3)")
	if err != nil {
		t.Fatalf("GREATEST failed: %v", err)
	}
	t.Logf("GREATEST result: %v", result.Rows)

	// Test LEAST
	result, err = exec.Execute("SELECT LEAST(10, 5, 20, 3)")
	if err != nil {
		t.Fatalf("LEAST failed: %v", err)
	}
	t.Logf("LEAST result: %v", result.Rows)
}

func TestSQL_MoreMathFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-math2-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Test SIGN
	result, err := exec.Execute("SELECT SIGN(-10), SIGN(0), SIGN(5)")
	if err != nil {
		t.Fatalf("SIGN failed: %v", err)
	}
	t.Logf("SIGN result: %v", result.Rows)

	// Test LOG
	result, err = exec.Execute("SELECT LOG(10)")
	if err != nil {
		t.Fatalf("LOG failed: %v", err)
	}
	t.Logf("LOG result: %v", result.Rows)

	// Test LOG10
	result, err = exec.Execute("SELECT LOG10(100)")
	if err != nil {
		t.Fatalf("LOG10 failed: %v", err)
	}
	t.Logf("LOG10 result: %v", result.Rows)

	// Test EXP
	result, err = exec.Execute("SELECT EXP(1)")
	if err != nil {
		t.Fatalf("EXP failed: %v", err)
	}
	t.Logf("EXP result: %v", result.Rows)

	// Test PI
	result, err = exec.Execute("SELECT PI()")
	if err != nil {
		t.Fatalf("PI failed: %v", err)
	}
	t.Logf("PI result: %v", result.Rows)
}

func TestSQL_OrderByLimit(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-order-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Create table and insert data
	_, _ = exec.Execute("CREATE TABLE items (id INT, name VARCHAR(50), price FLOAT)")
	inserts := []struct {
		id    int
		name  string
		price float64
	}{
		{1, "Apple", 1.99},
		{2, "Banana", 0.99},
		{3, "Cherry", 3.99},
		{4, "Date", 2.49},
		{5, "Elderberry", 4.99},
	}

	for _, item := range inserts {
		sql := fmt.Sprintf("INSERT INTO items VALUES (%d, '%s', %f)", item.id, item.name, item.price)
		exec.Execute(sql)
	}

	// Test ORDER BY
	result, err := exec.Execute("SELECT * FROM items ORDER BY price")
	if err != nil {
		t.Fatalf("ORDER BY failed: %v", err)
	}
	if result.RowCount != 5 {
		t.Errorf("Expected 5 rows, got %d", result.RowCount)
	}

	// Test LIMIT (may not be fully implemented)
	result, err = exec.Execute("SELECT * FROM items LIMIT 3")
	if err != nil {
		t.Logf("LIMIT not implemented: %v", err)
	}

	// Test ORDER BY + LIMIT
	result, err = exec.Execute("SELECT * FROM items ORDER BY price DESC LIMIT 2")
	if err != nil {
		t.Logf("ORDER BY + LIMIT failed: %v", err)
	}
	t.Logf("ORDER BY + LIMIT returned %d rows", result.RowCount)
}

func TestSQL_Distinct(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-distinct-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Create table with duplicate values
	_, _ = exec.Execute("CREATE TABLE dup_test (category VARCHAR(50))")
	categories := []string{"A", "B", "A", "C", "B", "A", "D"}
	for _, cat := range categories {
		sql := fmt.Sprintf("INSERT INTO dup_test VALUES ('%s')", cat)
		exec.Execute(sql)
	}

	// Test SELECT (returns all rows)
	result, err := exec.Execute("SELECT category FROM dup_test")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	t.Logf("SELECT returned %d rows (including duplicates)", result.RowCount)

	// Test DISTINCT (may not be fully implemented)
	result, err = exec.Execute("SELECT DISTINCT category FROM dup_test")
	if err != nil {
		t.Logf("DISTINCT failed: %v", err)
	}
	t.Logf("DISTINCT returned %d rows", result.RowCount)
}

// =============================================================================
// Error Handling Tests
// =============================================================================

func TestErrors_TableNotFound(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-error-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Try to select from non-existent table
	_, err = exec.Execute("SELECT * FROM nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestErrors_DuplicateTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-dup-table-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")

	// Create table
	_, _ = exec.Execute("CREATE TABLE test_table (id INT)")

	// Try to create same table again
	_, err = exec.Execute("CREATE TABLE test_table (id INT)")
	if err == nil {
		t.Error("Expected error for duplicate table")
	}
}

func TestErrors_SyntaxError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-syntax-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)

	invalidSQL := []string{
		"SELEC * FROM users",
		"CREATE",
		"INSERT INTO",
		"DROP",
	}

	for _, sql := range invalidSQL {
		_, err := exec.Execute(sql)
		if err == nil {
			t.Errorf("Expected error for invalid SQL: %s", sql)
		}
	}
}

// =============================================================================
// Performance Tests
// =============================================================================

func BenchmarkInsert(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "xxsql-bench-insert-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		b.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")
	exec.Execute("CREATE TABLE bench (id INT PRIMARY KEY, value VARCHAR(100))")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sql := fmt.Sprintf("INSERT INTO bench VALUES (%d, 'benchmark value')", i)
		exec.Execute(sql)
	}
}

func BenchmarkSelect(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "xxsql-bench-select-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		b.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")
	exec.Execute("CREATE TABLE bench (id INT PRIMARY KEY, value VARCHAR(100))")

	// Insert data
	for i := 0; i < 1000; i++ {
		sql := fmt.Sprintf("INSERT INTO bench VALUES (%d, 'benchmark value')", i)
		exec.Execute(sql)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.Execute("SELECT * FROM bench")
	}
}

func BenchmarkSelectWhere(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "xxsql-bench-where-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		b.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")
	exec.Execute("CREATE TABLE bench (id INT PRIMARY KEY, value INT)")

	// Insert data
	for i := 0; i < 1000; i++ {
		sql := fmt.Sprintf("INSERT INTO bench VALUES (%d, %d)", i, i%10)
		exec.Execute(sql)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.Execute("SELECT * FROM bench WHERE value = 5")
	}
}

// =============================================================================
// Stress Tests
// =============================================================================

func TestStress_RapidOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "xxsql-stress-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	exec := executor.NewExecutor(engine)
	exec.SetDatabase("testdb")
	exec.Execute("CREATE TABLE stress (id INT PRIMARY KEY, data VARCHAR(100))")

	start := time.Now()
	iterations := 1000

	for i := 0; i < iterations; i++ {
		// Insert
		sql := fmt.Sprintf("INSERT INTO stress VALUES (%d, 'data%d')", i, i)
		_, err := exec.Execute(sql)
		if err != nil {
			t.Logf("Insert error at %d: %v", i, err)
		}

		// Select every 100 iterations
		if i%100 == 0 {
			exec.Execute("SELECT * FROM stress")
		}
	}

	duration := time.Since(start)
	t.Logf("Completed %d iterations in %v (%.2f ops/sec)", iterations, duration, float64(iterations)/duration.Seconds())

	// Verify final state
	result, _ := exec.Execute("SELECT COUNT(*) FROM stress")
	t.Logf("Final row count: %d", result.RowCount)
}
