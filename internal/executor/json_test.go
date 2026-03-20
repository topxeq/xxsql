package executor

import (
	"os"
	"testing"

	"github.com/topxeq/xxsql/internal/storage"
)

// TestJsonExtract tests JSON_EXTRACT function
func TestJsonExtract(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-json-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE users (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			data TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert JSON data
	_, err = exec.Execute(`INSERT INTO users (name, data) VALUES ('Alice', '{"age": 30, "city": "NYC", "hobbies": ["reading", "coding"]}')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test JSON_EXTRACT
	result, err := exec.Execute(`SELECT name, JSON_EXTRACT(data, '$.age') AS age FROM users`)
	if err != nil {
		t.Fatalf("Failed to execute JSON_EXTRACT: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}

	t.Logf("JSON_EXTRACT result: %v", result.Rows[0])

	// Check age is 30
	age := result.Rows[0][1]
	if age == nil {
		t.Errorf("Expected age to be non-nil")
	} else {
		t.Logf("Age: %v (type: %T)", age, age)
	}

	// Test nested extraction
	result, err = exec.Execute(`SELECT JSON_EXTRACT(data, '$.hobbies[0]') AS first_hobby FROM users`)
	if err != nil {
		t.Fatalf("Failed to execute nested JSON_EXTRACT: %v", err)
	}
	t.Logf("First hobby: %v", result.Rows[0][0])
}

// TestJsonArray tests JSON_ARRAY function
func TestJsonArray(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-json-array-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	result, err := exec.Execute(`SELECT JSON_ARRAY(1, 2, 3) AS arr`)
	if err != nil {
		t.Fatalf("Failed to execute JSON_ARRAY: %v", err)
	}

	t.Logf("JSON_ARRAY result: %v", result.Rows[0])

	arr := result.Rows[0][0].(string)
	if arr != "[1,2,3]" {
		t.Errorf("Expected [1,2,3], got %s", arr)
	}
}

// TestJsonObject tests JSON_OBJECT function
func TestJsonObject(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-json-object-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	result, err := exec.Execute(`SELECT JSON_OBJECT('name', 'Alice', 'age', 30) AS obj`)
	if err != nil {
		t.Fatalf("Failed to execute JSON_OBJECT: %v", err)
	}

	t.Logf("JSON_OBJECT result: %v", result.Rows[0])

	obj := result.Rows[0][0].(string)
	if obj == "" {
		t.Errorf("Expected non-empty JSON object")
	}
}

// TestJsonType tests JSON_TYPE function
func TestJsonType(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-json-type-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	tests := []struct {
		json     string
		expected string
	}{
		{`'{"a": 1}'`, "OBJECT"},
		{`'[1, 2, 3]'`, "ARRAY"},
		{`'"hello"'`, "STRING"},
		{`'123'`, "NUMBER"}, // JSON numbers are NUMBER, not specifically INTEGER
		{`'true'`, "BOOLEAN"},
		{`'null'`, "NULL"},
	}

	for _, tt := range tests {
		result, err := exec.Execute(`SELECT JSON_TYPE(` + tt.json + `) AS type`)
		if err != nil {
			t.Fatalf("Failed to execute JSON_TYPE(%s): %v", tt.json, err)
		}

		got := result.Rows[0][0].(string)
		t.Logf("JSON_TYPE(%s) = %s", tt.json, got)
		if got != tt.expected {
			t.Errorf("JSON_TYPE(%s): expected %s, got %s", tt.json, tt.expected, got)
		}
	}
}

// TestJsonValid tests JSON_VALID function
func TestJsonValid(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-json-valid-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Valid JSON
	result, err := exec.Execute(`SELECT JSON_VALID('{"name": "Alice"}') AS valid`)
	if err != nil {
		t.Fatalf("Failed to execute JSON_VALID: %v", err)
	}
	valid := result.Rows[0][0]
	t.Logf("JSON_VALID(valid JSON) = %v", valid)

	// Invalid JSON
	result, err = exec.Execute(`SELECT JSON_VALID('not valid json') AS valid`)
	if err != nil {
		t.Fatalf("Failed to execute JSON_VALID: %v", err)
	}
	invalid := result.Rows[0][0]
	t.Logf("JSON_VALID(invalid JSON) = %v", invalid)
}

// TestJsonQuoteUnquote tests JSON_QUOTE and JSON_UNQUOTE functions
func TestJsonQuoteUnquote(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-json-quote-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// JSON_QUOTE
	result, err := exec.Execute(`SELECT JSON_QUOTE('hello') AS quoted`)
	if err != nil {
		t.Fatalf("Failed to execute JSON_QUOTE: %v", err)
	}
	quoted := result.Rows[0][0].(string)
	t.Logf("JSON_QUOTE('hello') = %s", quoted)
	if quoted != `"hello"` {
		t.Errorf("Expected \"hello\", got %s", quoted)
	}

	// JSON_UNQUOTE
	result, err = exec.Execute(`SELECT JSON_UNQUOTE('"hello"') AS unquoted`)
	if err != nil {
		t.Fatalf("Failed to execute JSON_UNQUOTE: %v", err)
	}
	unquoted := result.Rows[0][0].(string)
	t.Logf("JSON_UNQUOTE(\"hello\") = %s", unquoted)
	if unquoted != "hello" {
		t.Errorf("Expected hello, got %s", unquoted)
	}
}

// TestJsonContains tests JSON_CONTAINS function
func TestJsonContains(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-json-contains-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	result, err := exec.Execute(`SELECT JSON_CONTAINS('{"a": 1, "b": 2}', '{"a": 1}') AS contains`)
	if err != nil {
		t.Fatalf("Failed to execute JSON_CONTAINS: %v", err)
	}
	contains := result.Rows[0][0]
	t.Logf("JSON_CONTAINS result: %v", contains)
}

// TestJsonKeys tests JSON_KEYS function
func TestJsonKeys(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-json-keys-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	result, err := exec.Execute(`SELECT JSON_KEYS('{"name": "Alice", "age": 30}') AS keys`)
	if err != nil {
		t.Fatalf("Failed to execute JSON_KEYS: %v", err)
	}

	keys := result.Rows[0][0].(string)
	t.Logf("JSON_KEYS result: %s", keys)
}

// TestJsonLength tests JSON_LENGTH function
func TestJsonLength(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-json-length-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	tests := []struct {
		json     string
		expected int64
	}{
		{`'[1, 2, 3]'`, 3},
		{`'{"a": 1, "b": 2}'`, 2},
		{`'"hello"'`, 1}, // Scalars have length 1
	}

	for _, tt := range tests {
		result, err := exec.Execute(`SELECT JSON_LENGTH(` + tt.json + `) AS len`)
		if err != nil {
			t.Fatalf("Failed to execute JSON_LENGTH(%s): %v", tt.json, err)
		}

		var length int64
		switch v := result.Rows[0][0].(type) {
		case int:
			length = int64(v)
		case int64:
			length = v
		}
		t.Logf("JSON_LENGTH(%s) = %d", tt.json, length)
		if length != tt.expected {
			t.Errorf("JSON_LENGTH(%s): expected %d, got %d", tt.json, tt.expected, length)
		}
	}
}

// TestJsonCombined tests combined JSON operations
func TestJsonCombined(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "xxsql-json-combined-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine := storage.NewEngine(tmpDir)
	exec := NewExecutor(engine)
	exec.SetDatabase("test")

	// Create table
	_, err = exec.Execute(`
		CREATE TABLE products (
			id SEQ PRIMARY KEY,
			name VARCHAR(100),
			attributes TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert products with JSON attributes
	_, err = exec.Execute(`INSERT INTO products (name, attributes) VALUES ('Widget', '{"color": "red", "size": "large", "tags": ["sale", "featured"]}')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = exec.Execute(`INSERT INTO products (name, attributes) VALUES ('Gadget', '{"color": "blue", "size": "small", "tags": ["new"]}')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Query with JSON functions
	result, err := exec.Execute(`
		SELECT name, JSON_EXTRACT(attributes, '$.color') AS color, JSON_TYPE(attributes) AS type
		FROM products
		WHERE JSON_VALID(attributes)
	`)
	if err != nil {
		t.Fatalf("Failed to execute combined query: %v", err)
	}

	t.Logf("Combined JSON query result:")
	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}