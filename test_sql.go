package main

import (
	"fmt"
	"github.com/topxeq/xxsql/internal/sql"
)

func main() {
	testCases := []string{
		"SELECT id, name FROM users WHERE id = 1",
		"SELECT * FROM users WHERE age > 18 AND status = 'active'",
		"SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
		"INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
		"UPDATE users SET name = 'Bob' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
		"CREATE TABLE users (id SEQ PRIMARY KEY, name VARCHAR(100), email VARCHAR(255))",
		"DROP TABLE IF EXISTS users",
		"SELECT COUNT(*) FROM users",
		"SELECT id, name FROM users ORDER BY name DESC LIMIT 10",
		"SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
		"SELECT category, SUM(price) FROM products GROUP BY category",
	}

	for _, tc := range testCases {
		fmt.Printf("\nInput: %s\n", tc)
		p := sql.NewParser(tc)
		stmt, err := p.Parse()
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		fmt.Printf("AST: %s\n", stmt.String())
	}
}