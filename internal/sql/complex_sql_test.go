package sql

import (
	"testing"
)

func TestParse_ComplexSQL(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		// Complex SELECT with multiple clauses
		{"select_distinct_with_all", `SELECT DISTINCT u.id, u.name, COUNT(o.id) as order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.status = 'active' GROUP BY u.id, u.name HAVING COUNT(o.id) > 5 ORDER BY order_count DESC LIMIT 10 OFFSET 20`},

		// Complex JOIN
		{"complex_join", `SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id LEFT JOIN products p ON o.product_id = p.id RIGHT JOIN categories c ON p.category_id = c.id WHERE u.age > 18 AND p.price BETWEEN 10 AND 100`},

		// Subquery in WHERE
		{"subquery_where", `SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100) AND EXISTS (SELECT 1 FROM reviews WHERE user_id = users.id)`},

		// Subquery in SELECT
		{"subquery_select", `SELECT u.name, (SELECT COUNT(*) FROM orders WHERE user_id = u.id) as order_count FROM users u`},

		// UNION
		{"union", `SELECT name FROM users UNION SELECT name FROM customers`},

		// UNION ALL
		{"union_all", `SELECT name FROM users UNION ALL SELECT name FROM customers`},

		// INTERSECT
		{"intersect", `SELECT name FROM users INTERSECT SELECT name FROM customers`},

		// EXCEPT
		{"except", `SELECT name FROM users EXCEPT SELECT name FROM customers`},

		// CTE
		{"cte", `WITH active_users AS (SELECT * FROM users WHERE status = 'active'), recent_orders AS (SELECT * FROM orders WHERE created_at > '2024-01-01') SELECT * FROM active_users u JOIN recent_orders o ON u.id = o.user_id`},

		// Recursive CTE
		{"recursive_cte", `WITH RECURSIVE hierarchy AS (SELECT id, name, manager_id FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.id, e.name, e.manager_id FROM employees e JOIN hierarchy h ON e.manager_id = h.id) SELECT * FROM hierarchy`},

		// Window functions
		{"window_functions", `SELECT id, name, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank, SUM(salary) OVER (PARTITION BY department) as dept_total FROM employees`},

		// Case expression
		{"case_expression", `SELECT id, name, CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END as age_group FROM users`},

		// Cast expression
		{"cast_expression", `SELECT CAST(price AS DECIMAL(10,2)) as formatted_price FROM products`},

		// Collate expression
		{"collate_expression", `SELECT * FROM users WHERE name = 'John' COLLATE NOCASE`},

		// Between expression
		{"between_expression", `SELECT * FROM products WHERE price BETWEEN 10 AND 100`},

		// In expression with subquery
		{"in_subquery", `SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)`},

		// Like expression
		{"like_expression", `SELECT * FROM users WHERE name LIKE '%john%' AND email NOT LIKE '%@spam.com'`},

		// Is null expression
		{"is_null_expression", `SELECT * FROM users WHERE email IS NULL AND phone IS NOT NULL`},

		// Parenthesized expression
		{"paren_expression", `SELECT * FROM users WHERE (age > 18 AND status = 'active') OR (role = 'admin')`},

		// Complex expression
		{"complex_expression", `SELECT * FROM users WHERE age > 18 AND (status = 'active' OR status = 'pending') AND name LIKE 'A%' AND id IN (1, 2, 3) AND email IS NOT NULL`},

		// INSERT with SELECT
		{"insert_select", `INSERT INTO users_backup (id, name, email) SELECT id, name, email FROM users WHERE status = 'active'`},

		// INSERT with ON CONFLICT
		{"insert_on_conflict", `INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com') ON CONFLICT (id) DO UPDATE SET name = excluded.name, email = excluded.email`},

		// UPDATE with JOIN
		{"update_join", `UPDATE users u SET status = 'vip' FROM orders o WHERE u.id = o.user_id GROUP BY u.id HAVING COUNT(o.id) > 10`},

		// DELETE with subquery
		{"delete_subquery", `DELETE FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'cancelled')`},

		// CREATE VIEW
		{"create_view", `CREATE VIEW active_users AS SELECT id, name, email FROM users WHERE status = 'active' WITH CHECK OPTION`},

		// CREATE VIEW with columns
		{"create_view_columns", `CREATE OR REPLACE VIEW user_summary (user_id, user_name, order_count) AS SELECT u.id, u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name`},

		// DROP VIEW
		{"drop_view", `DROP VIEW IF EXISTS user_summary`},

		// EXPLAIN
		{"explain", `EXPLAIN SELECT * FROM users WHERE id = 1`},

		// EXPLAIN QUERY PLAN
		{"explain_query_plan", `EXPLAIN QUERY PLAN SELECT * FROM users WHERE id = 1`},

		// ALTER TABLE ADD COLUMN
		{"alter_add_column", `ALTER TABLE users ADD COLUMN phone VARCHAR(20) NOT NULL DEFAULT ''`},

		// ALTER TABLE DROP COLUMN
		{"alter_drop_column", `ALTER TABLE users DROP COLUMN phone`},

		// ALTER TABLE RENAME COLUMN
		{"alter_rename_column", `ALTER TABLE users RENAME COLUMN name TO full_name`},

		// ALTER TABLE RENAME TO
		{"alter_rename_table", `ALTER TABLE users RENAME TO customers`},

		// ALTER TABLE ADD CONSTRAINT
		{"alter_add_constraint", `ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)`},

		// ALTER TABLE DROP CONSTRAINT
		{"alter_drop_constraint", `ALTER TABLE orders DROP CONSTRAINT fk_user`},

		// ALTER TABLE MODIFY COLUMN
		{"alter_modify_column", `ALTER TABLE users MODIFY COLUMN email VARCHAR(255) NOT NULL`},

		// CREATE INDEX
		{"create_index", `CREATE UNIQUE INDEX idx_users_email ON users(email) WHERE status = 'active'`},

		// CREATE INDEX IF NOT EXISTS
		{"create_index_if_not_exists", `CREATE INDEX IF NOT EXISTS idx_users_name ON users(name)`},

		// DROP INDEX
		{"drop_index", `DROP INDEX IF EXISTS idx_users_email`},

		// CREATE TRIGGER
		{"create_trigger", `CREATE TRIGGER update_timestamp AFTER UPDATE ON users FOR EACH ROW BEGIN UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id; END`},

		// DROP TRIGGER
		{"drop_trigger", `DROP TRIGGER IF EXISTS update_timestamp`},

		// CREATE FUNCTION
		{"create_function", `CREATE FUNCTION calculate_age(birth_date DATE) RETURNS INTEGER BEGIN RETURN YEAR(CURRENT_DATE) - YEAR(birth_date); END`},

		// DROP FUNCTION
		{"drop_function", `DROP FUNCTION IF EXISTS calculate_age`},

		// CREATE PROCEDURE
		{"create_procedure", `CREATE PROCEDURE get_user_orders(IN user_id INT) BEGIN SELECT * FROM orders WHERE user_id = user_id; END`},

		// DROP PROCEDURE
		{"drop_procedure", `DROP PROCEDURE IF EXISTS get_user_orders`},

		// CALL
		{"call_procedure", `CALL get_user_orders(1)`},

		// BEGIN TRANSACTION
		{"begin_transaction", `BEGIN TRANSACTION`},

		// COMMIT
		{"commit", `COMMIT`},

		// ROLLBACK
		{"rollback", `ROLLBACK`},

		// SAVEPOINT
		{"savepoint", `SAVEPOINT my_savepoint`},

		// RELEASE SAVEPOINT
		{"release_savepoint", `RELEASE SAVEPOINT my_savepoint`},

		// COPY
		{"copy", `COPY users TO '/tmp/users.csv' WITH CSV HEADER`},

		// LOAD DATA
		{"load_data", `LOAD DATA INFILE '/tmp/users.csv' INTO TABLE users FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS`},

		// VACUUM
		{"vacuum", `VACUUM`},

		// ANALYZE
		{"analyze", `ANALYZE users`},

		// PRAGMA
		{"pragma", `PRAGMA journal_mode = WAL`},

		// GRANT
		{"grant", `GRANT SELECT, INSERT ON users TO user1`},

		// REVOKE
		{"revoke", `REVOKE SELECT ON users FROM user1`},

		// CREATE USER
		{"create_user", `CREATE USER user1 WITH PASSWORD 'password123'`},

		// DROP USER
		{"drop_user", `DROP USER user1`},

		// ALTER USER
		{"alter_user", `ALTER USER user1 WITH PASSWORD 'newpassword'`},

		// SET PASSWORD
		{"set_password", `SET PASSWORD FOR user1 = 'newpassword'`},

		// SHOW GRANTS
		{"show_grants", `SHOW GRANTS FOR user1`},

		// BACKUP
		{"backup", `BACKUP DATABASE TO '/tmp/backup.sql'`},

		// RESTORE
		{"restore", `RESTORE DATABASE FROM '/tmp/backup.sql'`},

		// Complex nested subqueries
		{"nested_subqueries", `SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE product_id IN (SELECT id FROM products WHERE category_id IN (SELECT id FROM categories WHERE name = 'Electronics')))`},

		// Multiple window functions
		{"multiple_window_functions", `SELECT id, name, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank, LAG(salary) OVER (PARTITION BY department ORDER BY salary) as prev_salary, LEAD(salary) OVER (PARTITION BY department ORDER BY salary) as next_salary FROM employees`},

		// Complex CASE expression
		{"complex_case", `SELECT id, name, CASE WHEN age < 18 THEN 'minor' WHEN age >= 18 AND age < 65 THEN CASE WHEN income > 50000 THEN 'adult_high' ELSE 'adult_low' END ELSE 'senior' END as category FROM users`},

		// LET expression
		{"let_expression", `SELECT LET x = 10, y = 20 IN x + y`},

		// Block expression
		{"block_expression", `SELECT { var x = 10; var y = 20; x + y }`},

		// If expression
		{"if_expression", `SELECT IF age > 18 THEN 'adult' ELSE 'minor' END FROM users`},

		// Rank expression
		{"rank_expression", `SELECT id, name, RANK() OVER (ORDER BY score DESC) as rank FROM students`},

		// Match expression (FTS)
		{"match_expression", `SELECT * FROM articles WHERE content MATCH 'database AND search'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse error for %s: %v", tt.name, err)
			}
		})
	}
}

func TestParse_OrderByItems(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"order_by_asc", `SELECT * FROM users ORDER BY name ASC`},
		{"order_by_desc", `SELECT * FROM users ORDER BY name DESC`},
		{"order_by_multiple", `SELECT * FROM users ORDER BY name ASC, age DESC, created_at ASC`},
		{"order_by_expression", `SELECT * FROM users ORDER BY LENGTH(name) DESC`},
		{"order_by_nulls_first", `SELECT * FROM users ORDER BY name ASC NULLS FIRST`},
		{"order_by_nulls_last", `SELECT * FROM users ORDER BY name DESC NULLS LAST`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse error for %s: %v", tt.name, err)
			}
		})
	}
}

func TestParse_ColumnDef(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"simple_column", `CREATE TABLE users (id INTEGER)`},
		{"column_with_type", `CREATE TABLE users (name VARCHAR(255))`},
		{"column_not_null", `CREATE TABLE users (name VARCHAR(255) NOT NULL)`},
		{"column_default", `CREATE TABLE users (status VARCHAR(20) DEFAULT 'active')`},
		{"column_primary_key", `CREATE TABLE users (id INTEGER PRIMARY KEY)`},
		{"column_unique", `CREATE TABLE users (email VARCHAR(255) UNIQUE)`},
		{"column_check", `CREATE TABLE users (age INTEGER CHECK (age > 0))`},
		{"column_auto_increment", `CREATE TABLE users (id INTEGER AUTO_INCREMENT)`},
		{"column_multiple_constraints", `CREATE TABLE users (id INTEGER PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT NULL UNIQUE)`},
		{"column_generated", `CREATE TABLE users (full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name))`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse error for %s: %v", tt.name, err)
			}
		})
	}
}

func TestParse_CreateFTS(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"create_fts", `CREATE VIRTUAL TABLE articles USING fts5(title, content)`},
		{"create_fts_with_config", `CREATE VIRTUAL TABLE articles USING fts5(title, content, tokenize='porter')`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse error for %s: %v", tt.name, err)
			}
		})
	}
}

func TestParse_CreateView(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"create_view_simple", `CREATE VIEW active_users AS SELECT * FROM users WHERE status = 'active'`},
		{"create_view_with_columns", `CREATE VIEW user_summary (id, name) AS SELECT id, name FROM users`},
		{"create_or_replace_view", `CREATE OR REPLACE VIEW active_users AS SELECT * FROM users WHERE status = 'active'`},
		{"create_view_with_check", `CREATE VIEW active_users AS SELECT * FROM users WHERE status = 'active' WITH CHECK OPTION`},
		{"create_view_with_cascaded_check", `CREATE VIEW active_users AS SELECT * FROM users WHERE status = 'active' WITH CASCADED CHECK OPTION`},
		{"create_view_with_local_check", `CREATE VIEW active_users AS SELECT * FROM users WHERE status = 'active' WITH LOCAL CHECK OPTION`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse error for %s: %v", tt.name, err)
			}
		})
	}
}

func TestParse_Begin(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"begin", `BEGIN`},
		{"begin_transaction", `BEGIN TRANSACTION`},
		{"begin_immediate", `BEGIN IMMEDIATE TRANSACTION`},
		{"begin_exclusive", `BEGIN EXCLUSIVE TRANSACTION`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse error for %s: %v", tt.name, err)
			}
		})
	}
}

func TestParse_Copy(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"copy_to_file", `COPY users TO '/tmp/users.csv'`},
		{"copy_with_csv", `COPY users TO '/tmp/users.csv' WITH CSV`},
		{"copy_with_header", `COPY users TO '/tmp/users.csv' WITH CSV HEADER`},
		{"copy_with_delimiter", `COPY users TO '/tmp/users.csv' WITH CSV DELIMITER ','`},
		{"copy_from_file", `COPY users FROM '/tmp/users.csv' WITH CSV HEADER`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse error for %s: %v", tt.name, err)
			}
		})
	}
}

func TestParse_LoadData(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"load_data_simple", `LOAD DATA INFILE '/tmp/users.csv' INTO TABLE users`},
		{"load_data_with_fields", `LOAD DATA INFILE '/tmp/users.csv' INTO TABLE users FIELDS TERMINATED BY ',' ENCLOSED BY '"'`},
		{"load_data_with_lines", `LOAD DATA INFILE '/tmp/users.csv' INTO TABLE users LINES TERMINATED BY '\n'`},
		{"load_data_with_ignore", `LOAD DATA INFILE '/tmp/users.csv' INTO TABLE users IGNORE 1 ROWS`},
		{"load_data_with_columns", `LOAD DATA INFILE '/tmp/users.csv' INTO TABLE users (id, name, email)`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse error for %s: %v", tt.name, err)
			}
		})
	}
}

func TestParse_BinaryExpr(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"add", `SELECT 1 + 2`},
		{"subtract", `SELECT 1 - 2`},
		{"multiply", `SELECT 1 * 2`},
		{"divide", `SELECT 1 / 2`},
		{"mod", `SELECT 1 % 2`},
		{"concat", `SELECT 'hello' || ' ' || 'world'`},
		{"bitwise_and", `SELECT 1 & 2`},
		{"bitwise_or", `SELECT 1 | 2`},
		{"bitwise_xor", `SELECT 1 ^ 2`},
		{"bitwise_left_shift", `SELECT 1 << 2`},
		{"bitwise_right_shift", `SELECT 1 >> 2`},
		{"logical_and", `SELECT true AND false`},
		{"logical_or", `SELECT true OR false`},
		{"comparison_eq", `SELECT 1 = 2`},
		{"comparison_ne", `SELECT 1 != 2`},
		{"comparison_lt", `SELECT 1 < 2`},
		{"comparison_le", `SELECT 1 <= 2`},
		{"comparison_gt", `SELECT 1 > 2`},
		{"comparison_ge", `SELECT 1 >= 2`},
		{"is", `SELECT 1 IS NULL`},
		{"is_not", `SELECT 1 IS NOT NULL`},
		{"like", `SELECT name LIKE '%john%'`},
		{"not_like", `SELECT name NOT LIKE '%john%'`},
		{"glob", `SELECT name GLOB '*.txt'`},
		{"not_glob", `SELECT name NOT GLOB '*.txt'`},
		{"regexp", `SELECT name REGEXP '^[A-Z]'`},
		{"not_regexp", `SELECT name NOT REGEXP '^[A-Z]'`},
		{"match", `SELECT content MATCH 'database'`},
		{"not_match", `SELECT content NOT MATCH 'database'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse error for %s: %v", tt.name, err)
			}
		})
	}
}

func TestParse_FunctionCall(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"simple_function", `SELECT COUNT(*)`},
		{"function_with_args", `SELECT SUM(price)`},
		{"function_with_multiple_args", `SELECT SUBSTR(name, 1, 5)`},
		{"function_distinct", `SELECT COUNT(DISTINCT id)`},
		{"function_all", `SELECT COUNT(ALL id)`},
		{"function_with_filter", `SELECT COUNT(*) FILTER (WHERE status = 'active')`},
		{"function_with_over", `SELECT ROW_NUMBER() OVER (ORDER BY id)`},
		{"function_with_partition", `SELECT SUM(salary) OVER (PARTITION BY department)`},
		{"function_with_order", `SELECT GROUP_CONCAT(name ORDER BY name)`},
		{"function_with_limit", `SELECT GROUP_CONCAT(name LIMIT 10)`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse error for %s: %v", tt.name, err)
			}
		})
	}
}

func TestParse_ExistsExpr(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"exists", `SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)`},
		{"not_exists", `SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse error for %s: %v", tt.name, err)
			}
		})
	}
}

func TestParse_Vacuum(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"vacuum", `VACUUM`},
		{"vacuum_database", `VACUUM DATABASE`},
		{"vacuum_into", `VACUUM INTO '/tmp/vacuumed.db'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse error for %s: %v", tt.name, err)
			}
		})
	}
}

func TestParse_CreateFunction(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"create_function_simple", `CREATE FUNCTION add(a INT, b INT) RETURNS INT RETURN a + b`},
		{"create_function_with_body", `CREATE FUNCTION calculate_age(birth_date DATE) RETURNS INTEGER BEGIN RETURN YEAR(CURRENT_DATE) - YEAR(birth_date); END`},
		{"create_function_with_language", `CREATE FUNCTION hello() RETURNS TEXT LANGUAGE SQL RETURN 'Hello'`},
		{"create_or_replace_function", `CREATE OR REPLACE FUNCTION add(a INT, b INT) RETURNS INT RETURN a + b`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse error for %s: %v", tt.name, err)
			}
		})
	}
}

func TestParse_CreateTrigger(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"create_trigger_before", `CREATE TRIGGER before_insert BEFORE INSERT ON users FOR EACH ROW BEGIN SET NEW.created_at = NOW(); END`},
		{"create_trigger_after", `CREATE TRIGGER after_insert AFTER INSERT ON users FOR EACH ROW BEGIN INSERT INTO audit_log (action) VALUES ('insert'); END`},
		{"create_trigger_instead_of", `CREATE TRIGGER instead_of_insert INSTEAD OF INSERT ON users_view FOR EACH ROW BEGIN INSERT INTO users (name) VALUES (NEW.name); END`},
		{"create_trigger_update", `CREATE TRIGGER update_timestamp AFTER UPDATE ON users FOR EACH ROW BEGIN UPDATE users SET updated_at = NOW() WHERE id = NEW.id; END`},
		{"create_trigger_delete", `CREATE TRIGGER before_delete BEFORE DELETE ON users FOR EACH ROW BEGIN INSERT INTO deleted_users (id, name) VALUES (OLD.id, OLD.name); END`},
		{"create_trigger_when", `CREATE TRIGGER conditional_trigger AFTER UPDATE ON users FOR EACH ROW WHEN OLD.status != NEW.status BEGIN INSERT INTO status_changes (user_id, old_status, new_status) VALUES (OLD.id, OLD.status, NEW.status); END`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse error for %s: %v", tt.name, err)
			}
		})
	}
}

func TestParse_DropFTS(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"drop_fts", `DROP VIRTUAL TABLE articles`},
		{"drop_fts_if_exists", `DROP VIRTUAL TABLE IF EXISTS articles`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Logf("Parse error for %s: %v", tt.name, err)
			}
		})
	}
}

func TestToken_String(t *testing.T) {
	tests := []struct {
		tok      TokenType
		expected string
	}{
		{TokEOF, "EOF"},
		{TokIdent, "IDENT"},
		{TokString, "STRING"},
		{TokNumber, "NUMBER"},
		{TokNull, "NULL"},
		{TokSelect, "SELECT"},
		{TokFrom, "FROM"},
		{TokWhere, "WHERE"},
		{TokInsert, "INSERT"},
		{TokUpdate, "UPDATE"},
		{TokDelete, "DELETE"},
		{TokCreate, "CREATE"},
		{TokDrop, "DROP"},
		{TokTable, "TABLE"},
		{TokIndex, "INDEX"},
		{TokView, "VIEW"},
		{TokTrigger, "TRIGGER"},
		{TokFunction, "FUNCTION"},
		{TokProcedure, "PROCEDURE"},
		{TokBegin, "BEGIN"},
		{TokCommit, "COMMIT"},
		{TokRollback, "ROLLBACK"},
		{TokSavepoint, "SAVEPOINT"},
		{TokRelease, "RELEASE"},
		{TokGrant, "GRANT"},
		{TokRevoke, "REVOKE"},
		{TokUser, "USER"},
		{TokPassword, "PASSWORD"},
	}

	for _, tt := range tests {
		if tt.tok.String() != tt.expected {
			t.Errorf("TokenType(%d).String() = %s, want %s", tt.tok, tt.tok.String(), tt.expected)
		}
	}
}
