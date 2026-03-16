package main

import (
	"fmt"
	"strings"
)

// executeSQL executes a SQL query and displays the results.
func executeSQL(query string) error {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}

	// Determine query type
	upperQuery := strings.ToUpper(query)
	isQuery := strings.HasPrefix(upperQuery, "SELECT") ||
		strings.HasPrefix(upperQuery, "SHOW") ||
		strings.HasPrefix(upperQuery, "DESCRIBE") ||
		strings.HasPrefix(upperQuery, "DESC") ||
		strings.HasPrefix(upperQuery, "EXPLAIN")

	if isQuery {
		return executeQuery(query)
	}

	return executeExec(query)
}

// executeQuery executes a query that returns rows.
func executeQuery(query string) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Collect all rows
	var allRows [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		// Convert []byte to string
		row := make([]interface{}, len(columns))
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			} else {
				row[i] = v
			}
		}
		allRows = append(allRows, row)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Format and display results
	formatter := getFormatter(outFmt)
	formatter.Format(columns, allRows)

	fmt.Printf("%d row(s)\n", len(allRows))

	return nil
}

// executeExec executes a query that doesn't return rows.
func executeExec(query string) error {
	result, err := db.Exec(query)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		// Some queries don't support RowsAffected
		fmt.Println("Query OK")
		return nil
	}

	lastID, _ := result.LastInsertId()

	if lastID > 0 {
		fmt.Printf("Query OK, %d row(s) affected, last insert ID: %d\n", affected, lastID)
	} else if affected > 0 {
		fmt.Printf("Query OK, %d row(s) affected\n", affected)
	} else {
		fmt.Println("Query OK, 0 row(s) affected")
	}

	return nil
}

// listTables lists all tables in the current database.
func listTables() error {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return err
	}
	defer rows.Close()

	fmt.Println("Tables:")
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return err
		}
		fmt.Printf("  %s\n", table)
	}

	return rows.Err()
}

// describeTable shows the structure of a table.
func describeTable(name string) error {
	rows, err := db.Query("DESCRIBE " + name)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	var allRows [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		row := make([]interface{}, len(columns))
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			} else {
				row[i] = v
			}
		}
		allRows = append(allRows, row)
	}

	formatter := getFormatter(outFmt)
	formatter.Format(columns, allRows)

	return rows.Err()
}

// useDatabase switches to a different database.
func useDatabase(name string) error {
	// Execute USE statement
	_, err := db.Exec("USE " + name)
	if err != nil {
		return err
	}

	dbName = name
	fmt.Printf("Database changed to: %s\n", name)

	return nil
}
