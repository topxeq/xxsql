# XxSql Reference Manual

This document provides comprehensive reference documentation for XxSql SQL syntax, data types, functions, and features.

## Table of Contents

- [Data Types](#data-types)
- [DDL Statements](#ddl-statements)
- [DML Statements](#dml-statements)
- [Functions](#functions)
- [Operators](#operators)
- [Expressions](#expressions)
- [Subqueries](#subqueries)
- [Common Table Expressions (CTE)](#common-table-expressions-cte)
- [Window Functions](#window-functions)
- [JOINs](#joins)
- [Set Operations](#set-operations)
- [Constraints](#constraints)
- [Indexes](#indexes)
- [Views](#views)
- [Triggers](#triggers)
- [User-Defined Functions](#user-defined-functions)
- [User Management](#user-management)
- [Backup and Restore](#backup-and-restore)
- [System Commands](#system-commands)

---

## Data Types

### Numeric Types

| Type | Description | Range |
|------|-------------|-------|
| `SEQ` | Auto-increment integer (primary key) | -9223372036854775808 to 9223372036854775807 |
| `TINYINT` | Tiny integer | -128 to 127 |
| `SMALLINT` | Small integer | -32768 to 32767 |
| `INT`, `INTEGER`, `BIGINT` | Integer | -9223372036854775808 to 9223372036854775807 |
| `FLOAT`, `DOUBLE` | Floating point | IEEE 754 double precision |
| `DECIMAL(p,s)`, `NUMERIC(p,s)` | Exact numeric | Precision p, scale s |

### String Types

| Type | Description | Max Size |
|------|-------------|----------|
| `CHAR(n)` | Fixed-length string | n characters |
| `VARCHAR(n)` | Variable-length string | n characters |
| `TEXT` | Large text | 2GB |

### Binary Types

| Type | Description |
|------|-------------|
| `BLOB` | Binary large object |

### Boolean Type

| Type | Description |
|------|-------------|
| `BOOL`, `BOOLEAN` | TRUE or FALSE |

### Date/Time Types

| Type | Format | Description |
|------|--------|-------------|
| `DATE` | YYYY-MM-DD | Date only |
| `TIME` | HH:MM:SS | Time only |
| `DATETIME`, `TIMESTAMP` | YYYY-MM-DD HH:MM:SS | Date and time |

### Type Literals

```sql
-- String literals
'string value'
"string value"

-- Numeric literals
123
123.45
-123

-- Boolean literals
TRUE
FALSE

-- NULL
NULL

-- BLOB literals (hex notation)
X'48656c6c6f'
0x48656c6c6f

-- Date/Time literals
DATE '2024-01-15'
TIME '14:30:00'
TIMESTAMP '2024-01-15 14:30:00'
```

---

## DDL Statements

### CREATE TABLE

```sql
CREATE TABLE [IF NOT EXISTS] table_name (
    column_definition [,
    column_definition ...]
    [, table_constraint ...]
);

column_definition:
    column_name data_type
    [DEFAULT default_value]
    [NOT NULL | NULL]
    [PRIMARY KEY]
    [UNIQUE]
    [AUTO_INCREMENT]
    [GENERATED ALWAYS AS (expression) [VIRTUAL | STORED]]
    [CHECK (expression)]
    [COMMENT 'string']

table_constraint:
    PRIMARY KEY (column [, column ...])
    | UNIQUE (column [, column ...])
    | CHECK (expression)
    | FOREIGN KEY (column [, column ...])
        REFERENCES ref_table (ref_column [, ref_column ...])
        [ON DELETE action]
        [ON UPDATE action]
    | CONSTRAINT constraint_name table_constraint
```

**Examples:**

```sql
-- Simple table
CREATE TABLE users (
    id SEQ PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE
);

-- Table with constraints
CREATE TABLE orders (
    id SEQ PRIMARY KEY,
    user_id INT NOT NULL,
    amount DECIMAL(10,2) DEFAULT 0.00,
    status VARCHAR(20) DEFAULT 'pending',
    created_at DATETIME,
    CHECK (amount >= 0),
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Table with generated columns
CREATE TABLE products (
    id INT PRIMARY KEY,
    price DECIMAL(10,2),
    quantity INT,
    total_value DECIMAL(10,2) GENERATED ALWAYS AS (price * quantity) VIRTUAL
);

-- Table with CHECK constraint
CREATE TABLE employees (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    salary DECIMAL(10,2),
    CHECK (age >= 18 AND age <= 65),
    CHECK (salary > 0)
);
```

### ALTER TABLE

```sql
-- Add column
ALTER TABLE table_name ADD COLUMN column_name data_type [constraints];

-- Drop column
ALTER TABLE table_name DROP COLUMN column_name;

-- Modify column
ALTER TABLE table_name MODIFY COLUMN column_name new_definition;

-- Rename column
ALTER TABLE table_name RENAME COLUMN old_name TO new_name;

-- Rename table
ALTER TABLE table_name RENAME TO new_table_name;
```

**Examples:**

```sql
ALTER TABLE users ADD COLUMN age INT DEFAULT 0;
ALTER TABLE users DROP COLUMN age;
ALTER TABLE users MODIFY COLUMN name VARCHAR(200) NOT NULL;
ALTER TABLE users RENAME COLUMN name TO username;
ALTER TABLE users RENAME TO customers;
```

### DROP TABLE

```sql
DROP TABLE [IF EXISTS] table_name;
```

### CREATE INDEX

```sql
CREATE [UNIQUE] INDEX index_name ON table_name (column [, column ...]);
```

**Examples:**

```sql
CREATE INDEX idx_name ON users (name);
CREATE UNIQUE INDEX idx_email ON users (email);
CREATE INDEX idx_name_email ON users (name, email);
```

### DROP INDEX

```sql
DROP INDEX index_name ON table_name;
```

---

## DML Statements

### SELECT

```sql
SELECT [ALL | DISTINCT]
    select_expr [, select_expr ...]
    [FROM table_references
        [WHERE where_condition]
        [GROUP BY {col_name | expr} [, ...]]
        [HAVING having_condition]
        [ORDER BY {col_name | expr} [ASC | DESC] [, ...]]
        [LIMIT {[offset,] row_count | row_count OFFSET offset}]
    ]
```

**Examples:**

```sql
-- Basic select
SELECT * FROM users;
SELECT id, name FROM users;

-- With WHERE
SELECT * FROM users WHERE age > 18;
SELECT * FROM users WHERE name LIKE 'A%';

-- With ORDER BY
SELECT * FROM users ORDER BY name ASC;
SELECT * FROM users ORDER BY created_at DESC LIMIT 10;

-- With GROUP BY
SELECT category, COUNT(*) as count, AVG(price) as avg_price
FROM products
GROUP BY category
HAVING count > 5;

-- With LIMIT and OFFSET
SELECT * FROM users LIMIT 10 OFFSET 20;
SELECT * FROM users LIMIT 20, 10;  -- MySQL style
```

### INSERT

```sql
INSERT INTO table_name [(column_list)]
    VALUES (value_list) [, (value_list) ...]
    [ON CONFLICT [conflict_target] conflict_action]
    [RETURNING select_expr [, select_expr ...]];

conflict_action:
    DO NOTHING
    | DO UPDATE SET assignment [, assignment ...] [WHERE condition]
```

**Examples:**

```sql
-- Basic insert
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');

-- Multiple rows
INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com');

-- UPSERT (ON CONFLICT)
INSERT INTO users (id, name) VALUES (1, 'Alice')
    ON CONFLICT (id) DO UPDATE SET name = excluded.name;

INSERT INTO users (id, name) VALUES (1, 'Alice')
    ON CONFLICT DO NOTHING;

-- With RETURNING
INSERT INTO users (name) VALUES ('Alice') RETURNING *;
INSERT INTO users (name) VALUES ('Alice') RETURNING id, name;
```

### UPDATE

```sql
UPDATE table_name
    SET assignment [, assignment ...]
    [WHERE where_condition]
    [RETURNING select_expr [, select_expr ...]]

assignment:
    column_name = expression
```

**Examples:**

```sql
UPDATE users SET email = 'new@example.com' WHERE id = 1;
UPDATE users SET name = 'Alice', age = 25 WHERE id = 1;
UPDATE users SET status = 'inactive' WHERE last_login < '2024-01-01';
UPDATE users SET counter = counter + 1 WHERE id = 1;
UPDATE users SET status = 'verified' RETURNING *;
```

### DELETE

```sql
DELETE FROM table_name
    [WHERE where_condition]
    [RETURNING select_expr [, select_expr ...]]
```

**Examples:**

```sql
DELETE FROM users WHERE id = 1;
DELETE FROM users WHERE status = 'inactive';
DELETE FROM users WHERE created_at < '2023-01-01' RETURNING id, name;
```

### TRUNCATE

```sql
TRUNCATE TABLE table_name;
```

---

## Functions

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Count all rows |
| `COUNT(expr)` | Count non-null values |
| `COUNT(DISTINCT expr)` | Count distinct non-null values |
| `SUM(expr)` | Sum of values |
| `AVG(expr)` | Average of values |
| `MIN(expr)` | Minimum value |
| `MAX(expr)` | Maximum value |
| `GROUP_CONCAT(expr [, separator])` | Concatenate values with separator |
| `STDDEV(expr)` | Standard deviation |
| `STDDEV_SAMP(expr)` | Sample standard deviation |
| `VARIANCE(expr)` | Variance |
| `VAR_SAMP(expr)` | Sample variance |

**Examples:**

```sql
SELECT COUNT(*) FROM users;
SELECT COUNT(DISTINCT category) FROM products;
SELECT SUM(amount), AVG(amount) FROM orders;
SELECT GROUP_CONCAT(name, ', ') FROM users;
SELECT STDDEV(salary) FROM employees;
```

### String Functions

| Function | Description |
|----------|-------------|
| `UPPER(s)` | Convert to uppercase |
| `LOWER(s)` | Convert to lowercase |
| `LENGTH(s)` | String length in characters |
| `OCTET_LENGTH(s)` | String length in bytes |
| `SUBSTRING(s, start[, len])` | Extract substring |
| `CONCAT(s1, s2, ...)` | Concatenate strings |
| `CONCAT_WS(sep, s1, s2, ...)` | Concatenate with separator |
| `TRIM(s)` | Remove leading/trailing whitespace |
| `LTRIM(s)` | Remove leading whitespace |
| `RTRIM(s)` | Remove trailing whitespace |
| `LEFT(s, n)` | Leftmost n characters |
| `RIGHT(s, n)` | Rightmost n characters |
| `REPLACE(s, from, to)` | Replace all occurrences |
| `REVERSE(s)` | Reverse string |
| `REPEAT(s, n)` | Repeat string n times |
| `SPACE(n)` | String of n spaces |
| `LPAD(s, len, pad)` | Left-pad string |
| `RPAD(s, len, pad)` | Right-pad string |
| `INSTR(s, substr)` | Position of substring |
| `CHAR(n)` | Character from ASCII code |
| `ASCII(s)` | ASCII code of first character |
| `UNICODE(s)` | Unicode code of first character |
| `SOUNDEX(s)` | Soundex code |
| `FORMAT(n, format)` | Format number |

**Examples:**

```sql
SELECT UPPER('hello');           -- 'HELLO'
SELECT LOWER('HELLO');           -- 'hello'
SELECT LENGTH('hello');          -- 5
SELECT SUBSTRING('hello', 2, 3); -- 'ell'
SELECT CONCAT('Hello', ' ', 'World'); -- 'Hello World'
SELECT REPLACE('hello', 'l', 'L');    -- 'heLLo'
SELECT LEFT('hello', 3);         -- 'hel'
SELECT RIGHT('hello', 3);        -- 'llo'
```

### Numeric Functions

| Function | Description |
|----------|-------------|
| `ABS(n)` | Absolute value |
| `ROUND(n[, d])` | Round to d decimal places |
| `CEIL(n)`, `CEILING(n)` | Ceiling |
| `FLOOR(n)` | Floor |
| `MOD(n, m)` | Modulo |
| `POWER(n, m)` | n raised to power m |
| `SQRT(n)` | Square root |
| `SIGN(n)` | Sign (-1, 0, 1) |
| `LOG(n)` | Natural logarithm |
| `LOG10(n)` | Base-10 logarithm |
| `EXP(n)` | e raised to power n |
| `PI()` | Value of π |
| `RANDOM()` | Random float 0-1 |
| `RAND()` | Random float 0-1 |
| `TRUNCATE(n, d)` | Truncate to d decimal places |
| `COS(n)`, `SIN(n)`, `TAN(n)` | Trigonometric functions |
| `ACOS(n)`, `ASIN(n)`, `ATAN(n)` | Inverse trig functions |
| `ATAN2(y, x)` | Two-argument arctangent |
| `COT(n)` | Cotangent |
| `DEGREES(n)` | Radians to degrees |
| `RADIANS(n)` | Degrees to radians |

**Examples:**

```sql
SELECT ABS(-5);          -- 5
SELECT ROUND(3.456, 2);  -- 3.46
SELECT CEIL(3.1);        -- 4
SELECT FLOOR(3.9);       -- 3
SELECT MOD(10, 3);       -- 1
SELECT POWER(2, 3);      -- 8
SELECT SQRT(16);         -- 4
```

### Date/Time Functions

| Function | Description |
|----------|-------------|
| `NOW()` | Current datetime |
| `CURRENT_TIMESTAMP` | Current datetime |
| `CURRENT_DATE` | Current date |
| `CURRENT_TIME` | Current time |
| `DATE(expr)` | Extract date from datetime |
| `TIME(expr)` | Extract time from datetime |
| `YEAR(date)` | Year value |
| `MONTH(date)` | Month value (1-12) |
| `DAY(date)`, `DAYOFMONTH(date)` | Day of month (1-31) |
| `HOUR(datetime)` | Hour value (0-23) |
| `MINUTE(datetime)` | Minute value (0-59) |
| `SECOND(datetime)` | Second value (0-59) |
| `WEEKDAY(date)` | Day of week (0=Monday) |
| `QUARTER(date)` | Quarter (1-4) |
| `LAST_DAY(date)` | Last day of month |
| `DATE_ADD(date, interval)` | Add interval to date |
| `DATE_SUB(date, interval)` | Subtract interval from date |
| `DATEDIFF(d1, d2)` | Days between dates |
| `STRFTIME(format, datetime)` | Format datetime |
| `UNIX_TIMESTAMP(datetime)` | Unix timestamp |
| `FROM_UNIXTIME(ts)` | Unix timestamp to datetime |
| `TIMESTAMPDIFF(unit, d1, d2)` | Difference in specified unit |
| `MAKEDATE(year, dayofyear)` | Create date from year and day |
| `MAKETIME(hour, minute, second)` | Create time |
| `SEC_TO_TIME(seconds)` | Seconds to time |
| `TIME_TO_SEC(time)` | Time to seconds |

**Examples:**

```sql
SELECT NOW();                          -- '2024-01-15 14:30:00'
SELECT CURRENT_DATE;                   -- '2024-01-15'
SELECT YEAR('2024-01-15');             -- 2024
SELECT MONTH('2024-01-15');            -- 1
SELECT DATE_ADD(NOW(), INTERVAL 7 DAY);
SELECT DATEDIFF('2024-01-20', '2024-01-15');  -- 5
SELECT STRFTIME('%Y-%m-%d', NOW());
```

### JSON Functions

| Function | Description |
|----------|-------------|
| `JSON_EXTRACT(json, path)` | Extract value at path |
| `JSON_UNQUOTE(json)` | Remove quotes from JSON string |
| `JSON_TYPE(json)` | Type of JSON value |
| `JSON_VALID(json)` | Check if valid JSON |
| `JSON_KEYS(json)` | Get keys of JSON object |
| `JSON_LENGTH(json)` | Length of JSON array/object |
| `JSON_ARRAY(val, ...)` | Create JSON array |
| `JSON_OBJECT(key, val, ...)` | Create JSON object |
| `JSON_SET(json, path, val)` | Set value at path |
| `JSON_REPLACE(json, path, val)` | Replace value at path |
| `JSON_REMOVE(json, path)` | Remove value at path |
| `JSON_MERGE_PATCH(json1, json2)` | Merge JSON objects |

**Examples:**

```sql
SELECT JSON_EXTRACT('{"a": 1}', '$.a');      -- 1
SELECT JSON_ARRAY(1, 2, 3);                  -- '[1, 2, 3]'
SELECT JSON_OBJECT('name', 'Alice', 'age', 25);
SELECT JSON_SET('{"a": 1}', '$.b', 2);       -- '{"a": 1, "b": 2}'
SELECT JSON_REMOVE('{"a": 1, "b": 2}', '$.b'); -- '{"a": 1}'
```

### NULL Handling Functions

| Function | Description |
|----------|-------------|
| `COALESCE(val1, val2, ...)` | First non-null value |
| `NULLIF(val1, val2)` | NULL if values equal |
| `IFNULL(val, default)` | Return default if val is NULL |

**Examples:**

```sql
SELECT COALESCE(NULL, NULL, 'hello');  -- 'hello'
SELECT NULLIF(5, 5);                    -- NULL
SELECT IFNULL(NULL, 'default');         -- 'default'
```

### Conditional Functions

| Function | Description |
|----------|-------------|
| `IF(cond, true_val, false_val)` | If condition |
| `IIF(cond, true_val, false_val)` | Inline if |
| `CASE WHEN ... THEN ... ELSE ... END` | Case expression |
| `GREATEST(val1, val2, ...)` | Maximum of values |
| `LEAST(val1, val2, ...)` | Minimum of values |

**Examples:**

```sql
SELECT IF(age >= 18, 'adult', 'minor');
SELECT IIF(score >= 60, 'pass', 'fail');
SELECT
    CASE
        WHEN score >= 90 THEN 'A'
        WHEN score >= 80 THEN 'B'
        WHEN score >= 70 THEN 'C'
        ELSE 'F'
    END AS grade
FROM students;
SELECT GREATEST(10, 20, 30);  -- 30
SELECT LEAST(10, 20, 30);     -- 10
```

### Utility Functions

| Function | Description |
|----------|-------------|
| `TYPEOF(value)` | Type of value |
| `CAST(expr AS type)` | Type conversion |
| `HEX(value)` | Convert to hexadecimal |
| `UNHEX(string)` | Convert hex to binary |
| `HEX_ENCODE(value)` | Encode to hex string |
| `HEX_DECODE(string)` | Decode hex to binary |
| `BASE64_ENCODE(value)` | Encode to Base64 string |
| `BASE64_DECODE(string)` | Decode Base64 to binary |
| `MD5(string)` | MD5 hash (hex string) |
| `SHA1(string)` | SHA1 hash (hex string) |
| `SHA256(string)` | SHA256 hash (hex string) |
| `SHA512(string)` | SHA512 hash (hex string) |
| `UUID()` | Generate UUID |
| `LAST_INSERT_ID()` | Last auto-increment ID |
| `ROW_COUNT()` | Rows affected by last statement |
| `USER()`, `CURRENT_USER()` | Current user |
| `VERSION()` | Server version |
| `CONNECTION_ID()` | Connection ID |
| `REGEXP(pattern, string)` | Regex match |

**Examples:**

```sql
SELECT TYPEOF('hello');           -- 'VARCHAR'
SELECT CAST('123' AS INT);        -- 123
SELECT HEX('Hello');              -- '48656c6c6f'
SELECT UNHEX('48656c6c6f');       -- BLOB 'Hello'
SELECT HEX_ENCODE('hello');       -- '68656c6c6f'
SELECT HEX_DECODE('68656c6c6f');  -- BLOB 'hello'
SELECT BASE64_ENCODE('hello');    -- 'aGVsbG8='
SELECT BASE64_DECODE('aGVsbG8='); -- BLOB 'hello'
SELECT MD5('hello');              -- '5d41402abc4b2a76b9719d911017c592'
SELECT SHA256('hello');           -- '2cf24dba5fb0a30e...'
SELECT UUID();
SELECT LAST_INSERT_ID();
```

---

## Operators

### Comparison Operators

| Operator | Description |
|----------|-------------|
| `=` | Equal |
| `!=`, `<>` | Not equal |
| `<` | Less than |
| `<=` | Less than or equal |
| `>` | Greater than |
| `>=` | Greater than or equal |
| `<=>` | Null-safe equal |

### Logical Operators

| Operator | Description |
|----------|-------------|
| `AND` | Logical AND |
| `OR` | Logical OR |
| `NOT` | Logical NOT |

### Arithmetic Operators

| Operator | Description |
|----------|-------------|
| `+` | Addition |
| `-` | Subtraction |
| `*` | Multiplication |
| `/` | Division |
| `%` | Modulo |
| `^` | Power |
| `||` | String concatenation |

### Pattern Matching Operators

| Operator | Description |
|----------|-------------|
| `LIKE` | SQL pattern match (% and _) |
| `NOT LIKE` | Negated LIKE |
| `GLOB` | Unix-style pattern match (*, ?, []) |
| `NOT GLOB` | Negated GLOB |
| `REGEXP` | Regular expression match |
| `NOT REGEXP` | Negated REGEXP |

**LIKE Patterns:**
- `%` - Matches any sequence of characters
- `_` - Matches any single character

**GLOB Patterns:**
- `*` - Matches any sequence of characters
- `?` - Matches any single character
- `[abc]` - Matches any character in the set
- `[a-z]` - Matches any character in the range
- Case-sensitive

**Examples:**

```sql
-- LIKE (case-insensitive by default)
SELECT * FROM users WHERE name LIKE 'A%';    -- Starts with A
SELECT * FROM users WHERE name LIKE '%son';  -- Ends with son
SELECT * FROM users WHERE name LIKE '_ohn';  -- John, john, etc.

-- GLOB (case-sensitive, Unix-style)
SELECT * FROM files WHERE name GLOB '*.txt';
SELECT * FROM files WHERE name GLOB 'image?.png';
SELECT * FROM files WHERE name GLOB '[A-Z]*';  -- Starts with uppercase

-- REGEXP
SELECT * FROM users WHERE email REGEXP '^[a-z]+@[a-z]+\\.[a-z]+$';
```

### IN and BETWEEN

```sql
-- IN
WHERE column IN (value1, value2, ...)
WHERE column IN (SELECT ...)

-- NOT IN
WHERE column NOT IN (value1, value2, ...)
WHERE column NOT IN (SELECT ...)

-- BETWEEN
WHERE column BETWEEN value1 AND value2
WHERE column NOT BETWEEN value1 AND value2
```

### IS NULL / IS NOT NULL

```sql
WHERE column IS NULL
WHERE column IS NOT NULL
```

---

## Expressions

### CASE Expressions

```sql
-- Simple CASE
CASE expr
    WHEN value1 THEN result1
    WHEN value2 THEN result2
    ELSE default_result
END

-- Searched CASE
CASE
    WHEN condition1 THEN result1
    WHEN condition2 THEN result2
    ELSE default_result
END
```

### CAST Expressions

```sql
CAST(expr AS type)
```

Supported types: `INT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`, `CHAR`, `VARCHAR`, `TEXT`, `BLOB`, `BOOL`, `BOOLEAN`, `DATE`, `TIME`, `DATETIME`, `TIMESTAMP`

---

## Subqueries

XxSql supports comprehensive subquery capabilities.

### Scalar Subquery

Returns a single value:

```sql
SELECT (SELECT MAX(price) FROM products) AS max_price;
SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = u.id) AS order_count
FROM users u;
```

### IN / NOT IN Subquery

```sql
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);
SELECT * FROM products WHERE category_id NOT IN (SELECT id FROM inactive_categories);
```

### EXISTS / NOT EXISTS Subquery

```sql
SELECT * FROM users u WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.user_id = u.id
);
```

### ANY / ALL Subquery

```sql
-- ANY: true if comparison is true for any row
SELECT * FROM products WHERE price > ANY (SELECT price FROM discount_items);

-- ALL: true if comparison is true for all rows
SELECT * FROM products WHERE price > ALL (SELECT price FROM budget_items);
```

### Derived Table (Subquery in FROM)

```sql
SELECT * FROM (
    SELECT user_id, COUNT(*) as order_count
    FROM orders
    GROUP BY user_id
) AS user_stats WHERE order_count > 5;
```

### Correlated Subquery

```sql
-- Subquery referencing outer query
SELECT * FROM users u
WHERE salary > (
    SELECT AVG(salary) FROM users WHERE department = u.department
);
```

---

## Common Table Expressions (CTE)

### Non-recursive CTE

```sql
WITH cte_name AS (
    SELECT ...
)
SELECT * FROM cte_name;

-- Multiple CTEs
WITH
    cte1 AS (SELECT ...),
    cte2 AS (SELECT ...)
SELECT * FROM cte1 JOIN cte2 ON ...;

-- CTE with column aliases
WITH stats(dept, count, avg_salary) AS (
    SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department
)
SELECT * FROM stats WHERE count > 10;
```

### Recursive CTE

```sql
WITH RECURSIVE cte_name AS (
    -- Base case
    SELECT ...
    UNION ALL
    -- Recursive case (references cte_name)
    SELECT ... FROM cte_name WHERE ...
)
SELECT * FROM cte_name;

-- Example: Generate sequence 1-10
WITH RECURSIVE nums AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM nums WHERE n < 10
)
SELECT * FROM nums;

-- Example: Hierarchical query
WITH RECURSIVE org_chart AS (
    SELECT id, name, manager_id, 1 AS level
    FROM employees WHERE manager_id IS NULL
    UNION ALL
    SELECT e.id, e.name, e.manager_id, oc.level + 1
    FROM employees e
    JOIN org_chart oc ON e.manager_id = oc.id
)
SELECT * FROM org_chart ORDER BY level;
```

---

## Window Functions

### Syntax

```sql
function_name() OVER (
    [PARTITION BY column, ...]
    [ORDER BY column [ASC|DESC], ...]
)
```

### Supported Window Functions

| Function | Description |
|----------|-------------|
| `ROW_NUMBER()` | Sequential number (1, 2, 3, ...) |
| `RANK()` | Rank with gaps for ties (1, 2, 2, 4, ...) |
| `DENSE_RANK()` | Rank without gaps (1, 2, 2, 3, ...) |
| `COUNT()` | Count over window |
| `SUM()` | Sum over window |
| `AVG()` | Average over window |
| `MIN()` | Minimum over window |
| `MAX()` | Maximum over window |

### Examples

```sql
-- Row number
SELECT id, name,
       ROW_NUMBER() OVER (ORDER BY score DESC) AS rank
FROM students;

-- Rank within partition
SELECT id, region, amount,
       RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS region_rank
FROM sales;

-- Running total
SELECT id, amount,
       SUM(amount) OVER (ORDER BY id) AS running_total
FROM sales;

-- Partitioned aggregates
SELECT id, region, amount,
       SUM(amount) OVER (PARTITION BY region) AS region_total,
       AVG(amount) OVER () AS global_avg
FROM sales;
```

---

## JOINs

### JOIN Types

| Type | Description |
|------|-------------|
| `INNER JOIN` | Rows matching in both tables |
| `LEFT [OUTER] JOIN` | All left rows, matched right rows |
| `RIGHT [OUTER] JOIN` | All right rows, matched left rows |
| `FULL [OUTER] JOIN` | All rows from both tables |
| `CROSS JOIN` | Cartesian product |

### Syntax

```sql
SELECT columns
FROM table1
[INNER | LEFT | RIGHT | FULL | CROSS] JOIN table2
ON condition;

-- Multiple JOINs
SELECT *
FROM table1 t1
INNER JOIN table2 t2 ON t1.id = t2.t1_id
LEFT JOIN table3 t3 ON t2.id = t3.t2_id;
```

### Examples

```sql
-- Inner join
SELECT u.name, o.order_id
FROM users u
INNER JOIN orders o ON u.id = o.user_id;

-- Left join
SELECT u.name, o.order_id
FROM users u
LEFT JOIN orders o ON u.id = o.user_id;

-- Full outer join
SELECT u.name, o.order_id
FROM users u
FULL OUTER JOIN orders o ON u.id = o.user_id;

-- Cross join
SELECT u.name, p.product_name
FROM users u
CROSS JOIN products p;
```

---

## Set Operations

| Operation | Description |
|-----------|-------------|
| `UNION` | Combine, remove duplicates |
| `UNION ALL` | Combine, keep duplicates |
| `INTERSECT` | Rows in both queries |
| `EXCEPT` | Rows in first, not in second |

```sql
SELECT name FROM users
UNION
SELECT name FROM customers;

SELECT id FROM table_a
INTERSECT
SELECT id FROM table_b;

SELECT id FROM table_a
EXCEPT
SELECT id FROM table_b;
```

---

## Constraints

### PRIMARY KEY

```sql
CREATE TABLE users (
    id INT PRIMARY KEY
);

-- Composite primary key
CREATE TABLE order_items (
    order_id INT,
    product_id INT,
    PRIMARY KEY (order_id, product_id)
);
```

### UNIQUE

```sql
CREATE TABLE users (
    email VARCHAR(100) UNIQUE
);
```

### NOT NULL

```sql
CREATE TABLE users (
    name VARCHAR(100) NOT NULL
);
```

### DEFAULT

```sql
CREATE TABLE users (
    status VARCHAR(20) DEFAULT 'active',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### CHECK

```sql
CREATE TABLE products (
    price DECIMAL(10,2),
    CHECK (price > 0)
);

-- Named check constraint
CREATE TABLE employees (
    age INT,
    salary DECIMAL(10,2),
    CONSTRAINT chk_age CHECK (age >= 18),
    CONSTRAINT chk_salary CHECK (salary > 0)
);
```

### FOREIGN KEY

```sql
CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    FOREIGN KEY (user_id) REFERENCES users(id)
        ON DELETE CASCADE
        ON UPDATE RESTRICT
);
```

Referential actions: `CASCADE`, `RESTRICT`, `SET NULL`, `NO ACTION`

---

## Indexes

```sql
-- Create index
CREATE INDEX idx_name ON table_name (column);
CREATE UNIQUE INDEX idx_email ON users (email);
CREATE INDEX idx_compound ON table_name (col1, col2);

-- Drop index
DROP INDEX idx_name ON table_name;
```

---

## Views

```sql
-- Create view
CREATE VIEW view_name AS SELECT ...;
CREATE VIEW view_name (col1, col2) AS SELECT ...;

-- Drop view
DROP VIEW [IF EXISTS] view_name;

-- Example
CREATE VIEW active_users AS
    SELECT id, name FROM users WHERE status = 'active';

SELECT * FROM active_users;
```

---

## Triggers

```sql
CREATE TRIGGER trigger_name
    {BEFORE | AFTER} {INSERT | UPDATE | DELETE}
    ON table_name
    FOR EACH ROW
    BEGIN
        -- SQL statements
    END;

-- Drop trigger
DROP TRIGGER trigger_name;
```

---

## User-Defined Functions

XxSql supports two types of user-defined functions:

### 1. SQL-based UDFs (Legacy)

```sql
-- Simple function
CREATE FUNCTION func_name(params) RETURNS type
    RETURN expression;

-- Function with IF
CREATE FUNCTION func_name(param type) RETURNS type
    RETURN IF condition THEN result ELSE other END;

-- Function with block
CREATE FUNCTION func_name(params) RETURNS type
BEGIN
    LET var = value;
    RETURN expression;
END;

-- Drop function
DROP FUNCTION func_name;
```

**Examples:**

```sql
CREATE FUNCTION double(x INT) RETURNS INT RETURN x * 2;
CREATE FUNCTION greet(name VARCHAR DEFAULT 'World') RETURNS VARCHAR
    RETURN CONCAT('Hello, ', name);
```

### 2. XxScript-based UDFs (Recommended)

XxScript-based UDFs provide full scripting capabilities including variables, loops, conditionals, and SQL queries.

#### Syntax

**Dollar-quoted string (PostgreSQL style):**
```sql
CREATE FUNCTION func_name(params) RETURNS type AS $$
    -- XxScript code
    return value
$$;
```

**SCRIPT keyword:**
```sql
CREATE FUNCTION func_name(params) RETURNS type SCRIPT 'return expression';
```

#### Examples

**Simple arithmetic:**
```sql
CREATE FUNCTION add_nums(x, y) RETURNS INT AS $$
    return x + y
$$;

SELECT add_nums(3, 4);  -- Returns 7
```

**With conditionals:**
```sql
CREATE FUNCTION abs_val(x) RETURNS INT AS $$
    if x < 0 {
        return -x
    }
    return x
$$;

SELECT abs_val(-5);  -- Returns 5
```

**With loops:**
```sql
CREATE FUNCTION sum_to_n(n) RETURNS INT AS $$
    var total = 0
    for (var i = 1; i <= n; i = i + 1) {
        total = total + i
    }
    return total
$$;

SELECT sum_to_n(10);  -- Returns 55
```

**With SQL queries:**
```sql
CREATE FUNCTION get_user_count() RETURNS INT AS $$
    var result = db_query("SELECT COUNT(*) as cnt FROM users")
    if (len(result) > 0) {
        return result[0].cnt
    }
    return 0
$$;

CREATE FUNCTION get_user_name(user_id) RETURNS VARCHAR AS $$
    var result = db_query("SELECT name FROM users WHERE id = " + string(user_id))
    if (len(result) > 0) {
        return result[0].name
    }
    return null
$$;
```

**String manipulation:**
```sql
CREATE FUNCTION greet(name) RETURNS VARCHAR AS $$
    return "Hello, " + name + "!"
$$;

SELECT greet('World');  -- Returns 'Hello, World!'
```

**With OR REPLACE:**
```sql
CREATE OR REPLACE FUNCTION add_nums(x, y) RETURNS INT AS $$
    return x + y
$$;
```

#### XxScript Features in UDFs

| Feature | Description | Example |
|---------|-------------|---------|
| Variables | `var name = value` | `var x = 10` |
| Conditionals | `if/else` | `if (x > 0) { ... } else { ... }` |
| Loops | `for`, `while` | `for (var i = 0; i < n; i = i + 1) { ... }` |
| Functions | `func name(params) { ... }` | `func helper(x) { return x * 2 }` |
| SQL Queries | `db_query(sql)`, `db_exec(sql)` | `db_query("SELECT * FROM users")` |
| Error Handling | `try/catch`, `throw` | `try { ... } catch (e) { ... }` |

#### Built-in Functions Available in UDFs

- **String**: `len()`, `upper()`, `lower()`, `trim()`, `split()`, `join()`, `substr()`, `replace()`, `contains()`, `indexOf()`
- **Math**: `abs()`, `min()`, `max()`, `floor()`, `ceil()`, `round()`, `sqrt()`, `pow()`
- **Type**: `int()`, `float()`, `string()`, `typeof()`
- **Array**: `len()`, `push()`, `pop()`, `slice()`, `range()`
- **JSON**: `json()`, `jsonParse()`
- **Database**: `db_query(sql)`, `db_exec(sql)`, `db_query_row(sql)`

#### Drop Function

```sql
DROP FUNCTION func_name;
DROP FUNCTION IF EXISTS func_name;
```

---

## User Management

```sql
-- Create user
CREATE USER 'username' IDENTIFIED BY 'password';

-- Grant permissions
GRANT ALL ON *.* TO 'username';
GRANT SELECT, INSERT ON database.table TO 'username';

-- Revoke permissions
REVOKE INSERT ON database.table FROM 'username';

-- Show grants
SHOW GRANTS FOR 'username';

-- Drop user
DROP USER 'username';

-- Set password
SET PASSWORD FOR 'username' = 'new_password';
```

---

## Backup and Restore

```sql
-- Backup
BACKUP DATABASE TO '/path/to/backup.xbak';
BACKUP DATABASE TO '/path/to/backup.xbak' WITH COMPRESS;

-- Restore
RESTORE DATABASE FROM '/path/to/backup.xbak';
```

---

## System Commands

```sql
-- Show tables
SHOW TABLES;

-- Describe table
DESCRIBE table_name;
DESC table_name;

-- Show databases
SHOW DATABASES;

-- Show status
SHOW STATUS;

-- Show variables
SHOW VARIABLES;

-- Use database
USE database_name;

-- Explain query plan
EXPLAIN SELECT ...;
EXPLAIN QUERY PLAN SELECT ...;
```

---

## Keywords Reference

```
ADD, AFTER, ALL, ALTER, AND, ANY, AS, ASC, AUTO_INCREMENT,
BEGIN, BEFORE, BETWEEN, BIGINT, BINARY, BLOB, BOOL, BOOLEAN, BY,
CASCADE, CASE, CAST, CHAR, CHARACTER, CHECK, COLUMN, COMMENT,
CONSTRAINT, CREATE, CROSS, CURRENT_DATE, CURRENT_TIME,
CURRENT_TIMESTAMP, CURRENT_USER,
DATABASE, DATABASES, DATE, DATETIME, DECIMAL, DEFAULT, DELETE,
DESC, DESCRIBE, DISTINCT, DO, DOUBLE, DROP,
EACH, ELSE, END, EXISTS, EXCEPT, EXPLAIN,
FALSE, FLOAT, FOR, FOREIGN, FROM, FULL, FUNCTION,
GENERATED, GLOB, GRANT, GROUP, GROUP_CONCAT,
HAVING, HOUR,
IF, IN, INDEX, INNER, INSERT, INT, INTEGER, INTERSECT,
INTERVAL, INTO, IS,
JOIN,
KEY,
LAST_INSERT_ID, LEFT, LET, LIKE, LIMIT, LOG, LOWER,
MAX, MIN, MINUTE, MOD, MODIFY, MONTH,
NATURAL, NOT, NULL, NULLIF, NUMERIC,
OFFSET, ON, OR, ORDER, OUTER, OVER,
PARTITION, PASSWORD, PRIMARY, PROCEDURE,
QUARTER,
RANGE, RANK, REFERENCES, REGEXP, RENAME, REPLACE, RESTRICT,
RETURN, RETURNING, RETURNS, REVOKE, RIGHT, ROLE, ROW, ROW_NUMBER,
SECOND, SELECT, SEQ, SEQUENCE, SET, SHOW, SMALLINT, STORED,
SUM, SUBSTRING,
TABLE, TABLES, TEMP, TEMPORARY, TEXT, THEN, TIME, TIMESTAMP,
TO, TRIGGER, TRUNCATE, TRUE, TYPE,
UNION, UNIQUE, UPDATE, UPPER, USE, USER, USING,
VALUES, VARCHAR, VARIABLES, VIRTUAL,
WEEKDAY, WHEN, WHERE, WITH, WINDOW,
YEAR
```