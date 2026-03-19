-- ============================================================
-- XxSql Comprehensive SQL Test Suite
-- This file contains all common SQL operations for testing
-- ============================================================

-- ============================================================
-- Part 1: Database and Table Management (DDL)
-- ============================================================

-- Drop tables if they exist (for clean start)
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS categories;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS salaries;
DROP TABLE IF EXISTS audit_log;

-- ============================================================
-- Part 2: CREATE TABLE with various data types and constraints
-- ============================================================

-- Create users table with various constraints
CREATE TABLE users (
    id SEQ PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) UNIQUE,
    age INT DEFAULT 0,
    salary FLOAT DEFAULT 0.0,
    bio TEXT,
    is_active BOOL DEFAULT TRUE,
    created_at DATETIME,
    CHECK (age >= 0 AND age <= 150)
);

-- Create categories table
CREATE TABLE categories (
    id SEQ PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    parent_id INT DEFAULT 0
);

-- Create products table with foreign key-like reference
CREATE TABLE products (
    id SEQ PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category_id INT DEFAULT 0,
    price DECIMAL(10,2) DEFAULT 0.00,
    stock INT DEFAULT 0,
    created_at DATE,
    is_available BOOL DEFAULT TRUE
);

-- Create orders table
CREATE TABLE orders (
    id SEQ PRIMARY KEY,
    user_id INT NOT NULL,
    order_date DATETIME,
    total_amount DECIMAL(12,2) DEFAULT 0.00,
    status VARCHAR(20) DEFAULT 'pending'
);

-- Create order_items table
CREATE TABLE order_items (
    id SEQ PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT DEFAULT 1,
    unit_price DECIMAL(10,2) DEFAULT 0.00
);

-- Create departments table
CREATE TABLE departments (
    id SEQ PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(200)
);

-- Create employees table
CREATE TABLE employees (
    id SEQ PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    department_id INT DEFAULT 0,
    manager_id INT DEFAULT 0,
    hire_date DATE,
    salary FLOAT DEFAULT 0.0
);

-- Create salaries table for aggregate tests
CREATE TABLE salaries (
    id SEQ PRIMARY KEY,
    employee_id INT NOT NULL,
    amount DECIMAL(10,2) DEFAULT 0.00,
    effective_date DATE
);

-- Create audit_log table
CREATE TABLE audit_log (
    id SEQ PRIMARY KEY,
    table_name VARCHAR(50),
    action VARCHAR(20),
    record_id INT,
    created_at DATETIME
);

-- Create files table with BLOB column for binary data storage
CREATE TABLE files (
    id SEQ PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    mime_type VARCHAR(100),
    size INT DEFAULT 0,
    data BLOB,
    created_at DATETIME
);

-- Create images table for storing image thumbnails
CREATE TABLE images (
    id SEQ PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    width INT DEFAULT 0,
    height INT DEFAULT 0,
    thumbnail BLOB,
    full_image BLOB
);

-- ============================================================
-- Part 3: CREATE INDEX
-- ============================================================

CREATE INDEX idx_users_username ON users (username);
CREATE INDEX idx_users_email ON users (email);
CREATE INDEX idx_products_name ON products (name);
CREATE INDEX idx_products_category ON products (category_id);
CREATE INDEX idx_orders_user ON orders (user_id);
CREATE INDEX idx_orders_date ON orders (order_date);
CREATE UNIQUE INDEX idx_products_sku ON products (id, name);

-- ============================================================
-- Part 4: INSERT - Basic Data Population
-- ============================================================

-- Insert users
INSERT INTO users (username, email, age, salary, bio, is_active, created_at) VALUES ('alice', 'alice@example.com', 28, 75000.50, 'Software engineer interested in databases', TRUE, '2024-01-15 10:30:00');
INSERT INTO users (username, email, age, salary, bio, is_active, created_at) VALUES ('bob', 'bob@example.com', 35, 85000.00, 'Database administrator', TRUE, '2024-01-16 09:00:00');
INSERT INTO users (username, email, age, salary, bio, is_active, created_at) VALUES ('charlie', 'charlie@example.com', 42, 95000.75, 'Project manager', TRUE, '2024-01-17 14:20:00');
INSERT INTO users (username, email, age, salary, bio, is_active, created_at) VALUES ('diana', 'diana@example.com', 31, 72000.00, 'Data analyst', TRUE, '2024-01-18 11:45:00');
INSERT INTO users (username, email, age, salary, bio, is_active, created_at) VALUES ('eve', 'eve@example.com', 26, 68000.25, 'Junior developer', TRUE, '2024-02-01 08:00:00');
INSERT INTO users (username, email, age, salary, bio, is_active, created_at) VALUES ('frank', 'frank@example.com', 45, 110000.00, 'Senior architect', TRUE, '2024-02-05 16:30:00');
INSERT INTO users (username, email, age, salary, bio, is_active, created_at) VALUES ('grace', 'grace@example.com', 29, 78000.00, 'DevOps engineer', FALSE, '2024-02-10 12:00:00');
INSERT INTO users (username, email, age, salary, bio, is_active, created_at) VALUES ('henry', NULL, 38, 88000.50, 'Quality assurance lead', TRUE, '2024-02-15 09:30:00');
INSERT INTO users (username, email, age, salary, bio, is_active, created_at) VALUES ('ivy', 'ivy@example.com', 23, 55000.00, 'Intern', TRUE, '2024-03-01 10:00:00');
INSERT INTO users (username, email, age, salary, bio, is_active, created_at) VALUES ('jack', 'jack@example.com', 50, 125000.00, 'CTO', TRUE, '2024-03-05 11:15:00');

-- Insert categories
INSERT INTO categories (name, description, parent_id) VALUES ('Electronics', 'Electronic devices and accessories', 0);
INSERT INTO categories (name, description, parent_id) VALUES ('Clothing', 'Apparel and fashion items', 0);
INSERT INTO categories (name, description, parent_id) VALUES ('Books', 'Physical and digital books', 0);
INSERT INTO categories (name, description, parent_id) VALUES ('Home & Garden', 'Home improvement and garden supplies', 0);
INSERT INTO categories (name, description, parent_id) VALUES ('Sports', 'Sports equipment and accessories', 0);
INSERT INTO categories (name, description, parent_id) VALUES ('Computers', 'Laptops, desktops and components', 1);
INSERT INTO categories (name, description, parent_id) VALUES ('Phones', 'Smartphones and accessories', 1);
INSERT INTO categories (name, description, parent_id) VALUES ('Men Clothing', 'Men apparel', 2);
INSERT INTO categories (name, description, parent_id) VALUES ('Women Clothing', 'Women apparel', 2);

-- Insert products
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('Laptop Pro 15', 6, 1299.99, 50, '2024-01-01', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('Desktop Workstation', 6, 1899.00, 30, '2024-01-02', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('Smartphone X', 7, 999.99, 100, '2024-01-03', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('Wireless Earbuds', 7, 149.99, 200, '2024-01-04', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('Mechanical Keyboard', 6, 129.99, 150, '2024-01-05', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('Gaming Mouse', 6, 79.99, 180, '2024-01-06', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('4K Monitor', 6, 449.99, 75, '2024-01-07', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('USB-C Hub', 6, 49.99, 300, '2024-01-08', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('T-Shirt Basic', 8, 19.99, 500, '2024-01-09', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('Jeans Classic', 8, 59.99, 200, '2024-01-10', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('Summer Dress', 9, 79.99, 150, '2024-01-11', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('Running Shoes', 5, 119.99, 120, '2024-01-12', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('Yoga Mat', 5, 29.99, 200, '2024-01-13', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('Programming in Go', 3, 49.99, 80, '2024-01-14', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('Database Design', 3, 59.99, 60, '2024-01-15', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('Garden Tools Set', 4, 89.99, 45, '2024-01-16', TRUE);
INSERT INTO products (name, category_id, price, stock, created_at, is_available) VALUES ('Discontinued Item', 1, 19.99, 0, '2024-01-17', FALSE);

-- Insert departments
INSERT INTO departments (name, location) VALUES ('Engineering', 'Building A, Floor 3');
INSERT INTO departments (name, location) VALUES ('Marketing', 'Building B, Floor 2');
INSERT INTO departments (name, location) VALUES ('Sales', 'Building A, Floor 1');
INSERT INTO departments (name, location) VALUES ('Human Resources', 'Building C, Floor 1');
INSERT INTO departments (name, location) VALUES ('Finance', 'Building B, Floor 4');

-- Insert employees
INSERT INTO employees (name, department_id, manager_id, hire_date, salary) VALUES ('John Smith', 1, 0, '2020-01-15', 95000.00);
INSERT INTO employees (name, department_id, manager_id, hire_date, salary) VALUES ('Jane Doe', 1, 1, '2021-03-20', 85000.00);
INSERT INTO employees (name, department_id, manager_id, hire_date, salary) VALUES ('Mike Johnson', 1, 1, '2022-06-10', 75000.00);
INSERT INTO employees (name, department_id, manager_id, hire_date, salary) VALUES ('Sarah Williams', 2, 0, '2019-08-05', 80000.00);
INSERT INTO employees (name, department_id, manager_id, hire_date, salary) VALUES ('Tom Brown', 2, 4, '2023-01-10', 65000.00);
INSERT INTO employees (name, department_id, manager_id, hire_date, salary) VALUES ('Emily Davis', 3, 0, '2018-05-22', 90000.00);
INSERT INTO employees (name, department_id, manager_id, hire_date, salary) VALUES ('Chris Wilson', 3, 6, '2022-09-15', 70000.00);
INSERT INTO employees (name, department_id, manager_id, hire_date, salary) VALUES ('Lisa Anderson', 4, 0, '2017-11-01', 75000.00);
INSERT INTO employees (name, department_id, manager_id, hire_date, salary) VALUES ('David Taylor', 5, 0, '2019-02-28', 95000.00);
INSERT INTO employees (name, department_id, manager_id, hire_date, salary) VALUES ('Amy Martinez', 5, 9, '2023-04-05', 72000.00);

-- Insert salaries
INSERT INTO salaries (employee_id, amount, effective_date) VALUES (1, 90000.00, '2023-01-01');
INSERT INTO salaries (employee_id, amount, effective_date) VALUES (1, 95000.00, '2024-01-01');
INSERT INTO salaries (employee_id, amount, effective_date) VALUES (2, 80000.00, '2023-01-01');
INSERT INTO salaries (employee_id, amount, effective_date) VALUES (2, 85000.00, '2024-01-01');
INSERT INTO salaries (employee_id, amount, effective_date) VALUES (3, 70000.00, '2023-01-01');
INSERT INTO salaries (employee_id, amount, effective_date) VALUES (3, 75000.00, '2024-01-01');
INSERT INTO salaries (employee_id, amount, effective_date) VALUES (4, 75000.00, '2023-01-01');
INSERT INTO salaries (employee_id, amount, effective_date) VALUES (4, 80000.00, '2024-01-01');
INSERT INTO salaries (employee_id, amount, effective_date) VALUES (5, 60000.00, '2023-01-01');
INSERT INTO salaries (employee_id, amount, effective_date) VALUES (5, 65000.00, '2024-01-01');

-- Insert orders
INSERT INTO orders (user_id, order_date, total_amount, status) VALUES (1, '2024-03-01 10:30:00', 1449.98, 'completed');
INSERT INTO orders (user_id, order_date, total_amount, status) VALUES (1, '2024-03-05 14:20:00', 129.99, 'completed');
INSERT INTO orders (user_id, order_date, total_amount, status) VALUES (2, '2024-03-02 09:15:00', 1149.98, 'completed');
INSERT INTO orders (user_id, order_date, total_amount, status) VALUES (2, '2024-03-10 16:45:00', 179.98, 'shipped');
INSERT INTO orders (user_id, order_date, total_amount, status) VALUES (3, '2024-03-03 11:00:00', 1899.00, 'completed');
INSERT INTO orders (user_id, order_date, total_amount, status) VALUES (4, '2024-03-04 13:30:00', 79.99, 'completed');
INSERT INTO orders (user_id, order_date, total_amount, status) VALUES (5, '2024-03-06 08:45:00', 999.99, 'pending');
INSERT INTO orders (user_id, order_date, total_amount, status) VALUES (6, '2024-03-07 15:10:00', 249.98, 'shipped');
INSERT INTO orders (user_id, order_date, total_amount, status) VALUES (7, '2024-03-08 12:00:00', 599.98, 'completed');
INSERT INTO orders (user_id, order_date, total_amount, status) VALUES (8, '2024-03-09 17:30:00', 149.99, 'cancelled');

-- Insert order_items
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (1, 1, 1, 1299.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (1, 8, 1, 49.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (1, 5, 1, 129.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (2, 5, 1, 129.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (3, 3, 1, 999.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (3, 4, 1, 149.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (4, 9, 5, 19.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (4, 10, 1, 59.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (5, 2, 1, 1899.00);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (6, 11, 1, 79.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (7, 3, 1, 999.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (8, 5, 1, 129.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (8, 6, 1, 79.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (8, 7, 1, 449.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (9, 1, 1, 1299.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (9, 8, 2, 49.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (9, 4, 2, 149.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (10, 4, 1, 149.99);

-- Insert audit_log
INSERT INTO audit_log (table_name, action, record_id, created_at) VALUES ('users', 'INSERT', 1, '2024-03-01 10:00:00');
INSERT INTO audit_log (table_name, action, record_id, created_at) VALUES ('products', 'INSERT', 1, '2024-03-01 10:05:00');
INSERT INTO audit_log (table_name, action, record_id, created_at) VALUES ('orders', 'INSERT', 1, '2024-03-01 10:30:00');
INSERT INTO audit_log (table_name, action, record_id, created_at) VALUES ('users', 'UPDATE', 2, '2024-03-02 11:00:00');
INSERT INTO audit_log (table_name, action, record_id, created_at) VALUES ('products', 'UPDATE', 5, '2024-03-03 09:00:00');

-- Insert files with BLOB data (using hex notation)
INSERT INTO files (name, mime_type, size, data, created_at) VALUES ('test.txt', 'text/plain', 12, X'48656c6c6f20576f726c6421', '2024-03-01 10:00:00');
INSERT INTO files (name, mime_type, size, data, created_at) VALUES ('config.bin', 'application/octet-stream', 8, X'deadbeef01020304', '2024-03-02 11:30:00');
INSERT INTO files (name, mime_type, size, data, created_at) VALUES ('small.dat', 'application/octet-stream', 4, X'cafebabe', '2024-03-03 14:00:00');

-- Insert images with BLOB thumbnails
INSERT INTO images (filename, width, height, thumbnail, full_image) VALUES ('photo1.jpg', 800, 600, X'89504e470d0a1a0a', X'ffd8ffe000104a464946');
INSERT INTO images (filename, width, height, thumbnail, full_image) VALUES ('logo.png', 100, 100, X'89504e47', X'89504e470d0a1a0a0000000d');

-- ============================================================
-- Part 5: SELECT - Basic Queries
-- ============================================================

-- Select all from users
SELECT * FROM users;

-- Select specific columns
SELECT username, email, age FROM users;

-- Select with WHERE clause
SELECT * FROM users WHERE age > 30;
SELECT * FROM users WHERE is_active = TRUE;
SELECT * FROM users WHERE salary >= 80000 AND salary <= 100000;
SELECT * FROM users WHERE email IS NULL;
SELECT * FROM users WHERE email IS NOT NULL;

-- Select with ORDER BY
SELECT * FROM users ORDER BY age ASC;
SELECT * FROM users ORDER BY salary DESC;
SELECT * FROM users ORDER BY department_id ASC, salary DESC;

-- Select with LIMIT and OFFSET
SELECT * FROM users LIMIT 5;
SELECT * FROM users LIMIT 5 OFFSET 5;
SELECT * FROM products ORDER BY price DESC LIMIT 3;

-- Select with DISTINCT
SELECT DISTINCT status FROM orders;
SELECT DISTINCT category_id FROM products;
SELECT DISTINCT department_id FROM employees;

-- Select with LIKE pattern matching
SELECT * FROM users WHERE username LIKE 'a%';
SELECT * FROM users WHERE email LIKE '%@example.com';
SELECT * FROM products WHERE name LIKE '%Pro%';

-- Select with IN clause
SELECT * FROM users WHERE age IN (25, 30, 35, 40);
SELECT * FROM products WHERE category_id IN (1, 2, 3);
SELECT * FROM orders WHERE status IN ('completed', 'shipped');

-- Select with BETWEEN
SELECT * FROM users WHERE age BETWEEN 25 AND 35;
SELECT * FROM products WHERE price BETWEEN 50 AND 200;
SELECT * FROM orders WHERE order_date BETWEEN '2024-03-01' AND '2024-03-05';

-- Select BLOB data
SELECT id, name, mime_type, size FROM files;
SELECT id, filename, width, height FROM images;
SELECT id, name, size, data FROM files WHERE id = 1;
SELECT * FROM files WHERE name LIKE '%.txt';

-- ============================================================
-- Part 6: Aggregate Functions
-- ============================================================

-- COUNT
SELECT COUNT(*) AS total_users FROM users;
SELECT COUNT(email) AS users_with_email FROM users;
SELECT COUNT(DISTINCT status) AS order_statuses FROM orders;

-- SUM
SELECT SUM(salary) AS total_salary FROM users;
SELECT SUM(quantity) AS total_items FROM order_items;
SELECT SUM(total_amount) AS total_revenue FROM orders WHERE status = 'completed';

-- AVG
SELECT AVG(salary) AS average_salary FROM users;
SELECT AVG(price) AS average_price FROM products;
SELECT AVG(age) AS average_age FROM users WHERE is_active = TRUE;

-- MIN and MAX
SELECT MIN(salary) AS min_salary, MAX(salary) AS max_salary FROM users;
SELECT MIN(price) AS cheapest, MAX(price) AS most_expensive FROM products;
SELECT MIN(created_at) AS earliest, MAX(created_at) AS latest FROM users;

-- GROUP BY
SELECT status, COUNT(*) AS count FROM orders GROUP BY status;
SELECT category_id, COUNT(*) AS product_count FROM products GROUP BY category_id;
SELECT department_id, AVG(salary) AS avg_salary FROM employees GROUP BY department_id;

-- GROUP BY with HAVING
SELECT status, COUNT(*) AS count FROM orders GROUP BY status HAVING COUNT(*) > 1;
SELECT category_id, AVG(price) AS avg_price FROM products GROUP BY category_id HAVING AVG(price) > 100;
SELECT department_id, COUNT(*) AS emp_count FROM employees GROUP BY department_id HAVING COUNT(*) >= 2;

-- Multiple aggregates
SELECT
    category_id,
    COUNT(*) AS product_count,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    AVG(price) AS avg_price,
    SUM(stock) AS total_stock
FROM products
GROUP BY category_id;

-- ============================================================
-- Part 7: JOIN Operations
-- ============================================================

-- INNER JOIN
SELECT u.username, o.id AS order_id, o.total_amount, o.status
FROM users u
INNER JOIN orders o ON u.id = o.user_id
ORDER BY o.id;

-- INNER JOIN with multiple tables
SELECT u.username, o.id AS order_id, p.name AS product_name, oi.quantity, oi.unit_price
FROM users u
INNER JOIN orders o ON u.id = o.user_id
INNER JOIN order_items oi ON o.id = oi.order_id
INNER JOIN products p ON oi.product_id = p.id
ORDER BY o.id, oi.id;

-- LEFT JOIN
SELECT u.username, o.id AS order_id
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
ORDER BY u.id;

-- RIGHT JOIN
SELECT o.id AS order_id, u.username
FROM orders o
RIGHT JOIN users u ON o.user_id = u.id
ORDER BY u.id;

-- CROSS JOIN
SELECT d.name AS department, e.name AS employee
FROM departments d
CROSS JOIN employees e
LIMIT 10;

-- Self JOIN (employees with managers)
SELECT e.name AS employee, m.name AS manager
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
ORDER BY e.id;

-- Multiple JOINs for complex query
SELECT
    o.id AS order_id,
    u.username,
    o.order_date,
    o.status,
    COUNT(oi.id) AS item_count,
    SUM(oi.quantity * oi.unit_price) AS calculated_total
FROM orders o
INNER JOIN users u ON o.user_id = u.id
LEFT JOIN order_items oi ON o.id = oi.order_id
GROUP BY o.id, u.username, o.order_date, o.status
ORDER BY o.id;

-- JOIN with aggregation
SELECT
    c.name AS category,
    COUNT(p.id) AS product_count,
    SUM(p.stock) AS total_stock,
    AVG(p.price) AS avg_price
FROM categories c
LEFT JOIN products p ON c.id = p.category_id
GROUP BY c.id, c.name
ORDER BY product_count DESC;

-- ============================================================
-- Part 8: UNION Operations
-- ============================================================

-- UNION (removes duplicates)
SELECT username AS name FROM users
UNION
SELECT name FROM employees
ORDER BY name;

-- UNION ALL (keeps duplicates)
SELECT 'user' AS type, username AS name FROM users
UNION ALL
SELECT 'employee' AS type, name FROM employees
ORDER BY name
LIMIT 10;

-- UNION with WHERE
SELECT name FROM products WHERE price > 500
UNION
SELECT name FROM products WHERE stock > 100;

-- Multiple UNION
SELECT status AS status_type FROM orders
UNION
SELECT action AS status_type FROM audit_log
ORDER BY status_type;

-- ============================================================
-- Part 9: Subqueries
-- ============================================================

-- Subquery in WHERE clause
SELECT * FROM users WHERE salary > (SELECT AVG(salary) FROM users);
SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products);
SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE is_active = TRUE);

-- Subquery with EXISTS
SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);
SELECT * FROM products p WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id);

-- Subquery in SELECT
SELECT
    username,
    salary,
    (SELECT AVG(salary) FROM users) AS avg_salary,
    salary - (SELECT AVG(salary) FROM users) AS diff_from_avg
FROM users;

-- Subquery for comparison
SELECT * FROM employees WHERE salary = (SELECT MAX(salary) FROM employees);

-- ============================================================
-- Part 10: UPDATE Operations
-- ============================================================

-- Simple UPDATE
UPDATE users SET age = 29 WHERE username = 'alice';
UPDATE products SET stock = stock - 1 WHERE id = 1;

-- UPDATE multiple columns
UPDATE users SET salary = 80000.00, age = 32 WHERE username = 'diana';

-- UPDATE with calculation
UPDATE products SET price = price * 1.10 WHERE category_id = 6;
UPDATE users SET salary = salary * 1.05 WHERE age > 40;

-- UPDATE with WHERE IN
UPDATE products SET is_available = FALSE WHERE stock = 0;

-- UPDATE all rows (careful!)
UPDATE users SET is_active = TRUE WHERE is_active = FALSE;

-- ============================================================
-- Part 11: DELETE Operations
-- ============================================================

-- Create temporary table for delete tests
CREATE TABLE temp_delete_test (
    id SEQ PRIMARY KEY,
    name VARCHAR(50),
    value INT
);

INSERT INTO temp_delete_test (name, value) VALUES ('test1', 10);
INSERT INTO temp_delete_test (name, value) VALUES ('test2', 20);
INSERT INTO temp_delete_test (name, value) VALUES ('test3', 30);
INSERT INTO temp_delete_test (name, value) VALUES ('test4', 40);
INSERT INTO temp_delete_test (name, value) VALUES ('test5', 50);

-- DELETE with WHERE
DELETE FROM temp_delete_test WHERE value < 25;

-- DELETE with IN
DELETE FROM temp_delete_test WHERE id IN (4, 5);

-- DELETE all
DELETE FROM temp_delete_test;

-- Drop the test table
DROP TABLE IF EXISTS temp_delete_test;

-- ============================================================
-- Part 12: ALTER TABLE Operations
-- ============================================================

-- Add column
ALTER TABLE users ADD COLUMN phone VARCHAR(20);

-- Modify column
ALTER TABLE users MODIFY COLUMN phone VARCHAR(30);

-- Rename column
ALTER TABLE users RENAME COLUMN phone TO phone_number;

-- Drop column
ALTER TABLE users DROP COLUMN phone_number;

-- Rename table
ALTER TABLE audit_log RENAME TO activity_log;

-- Rename back
ALTER TABLE activity_log RENAME TO audit_log;

-- ============================================================
-- Part 13: Built-in Functions
-- ============================================================

-- String functions
SELECT username, UPPER(username) AS upper_name, LOWER(username) AS lower_name FROM users LIMIT 5;
SELECT username, LENGTH(username) AS name_length FROM users;
SELECT CONCAT(username, '@company.com') AS company_email FROM users LIMIT 5;
SELECT SUBSTRING(username, 1, 3) AS short_name FROM users LIMIT 5;

-- Numeric functions
SELECT ABS(-15) AS absolute;
SELECT ROUND(123.456, 2) AS rounded;
SELECT FLOOR(123.9) AS floored;
SELECT CEILING(123.1) AS ceilinged;

-- Date/Time functions (if supported)
SELECT CURRENT_DATE AS today;
SELECT CURRENT_TIME AS now_time;

-- ============================================================
-- Part 14: User Management
-- ============================================================

-- Create user
CREATE USER 'testuser' IDENTIFIED BY 'testpass123';

-- Grant permissions
GRANT ALL ON *.* TO 'testuser';
GRANT SELECT, INSERT ON testdb.* TO 'testuser';
GRANT SELECT, INSERT, UPDATE ON testdb.users TO 'testuser';

-- Show grants
SHOW GRANTS FOR 'testuser';

-- Revoke permissions
REVOKE INSERT ON testdb.users FROM 'testuser';
REVOKE ALL ON *.* FROM 'testuser';

-- Drop user
DROP USER 'testuser';

-- ============================================================
-- Part 15: TRUNCATE
-- ============================================================

-- Create table for truncate test
CREATE TABLE truncate_test (
    id SEQ PRIMARY KEY,
    data VARCHAR(100)
);

INSERT INTO truncate_test (data) VALUES ('data1');
INSERT INTO truncate_test (data) VALUES ('data2');
INSERT INTO truncate_test (data) VALUES ('data3');

-- Truncate
TRUNCATE TABLE truncate_test;

-- Verify truncate
SELECT COUNT(*) AS count_after_truncate FROM truncate_test;

-- Drop test table
DROP TABLE IF EXISTS truncate_test;

-- ============================================================
-- Part 16: Complex Queries
-- ============================================================

-- Top N customers by total spending
SELECT
    u.username,
    COUNT(o.id) AS order_count,
    SUM(o.total_amount) AS total_spent
FROM users u
INNER JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.username
ORDER BY total_spent DESC
LIMIT 5;

-- Product sales summary
SELECT
    p.id,
    p.name,
    p.price,
    COALESCE(SUM(oi.quantity), 0) AS total_sold,
    COALESCE(SUM(oi.quantity * oi.unit_price), 0) AS total_revenue
FROM products p
LEFT JOIN order_items oi ON p.id = oi.product_id
GROUP BY p.id, p.name, p.price
ORDER BY total_sold DESC;

-- Order status summary
SELECT
    status,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_value,
    AVG(total_amount) AS avg_order_value
FROM orders
GROUP BY status
ORDER BY order_count DESC;

-- Employee hierarchy with department
SELECT
    e.name AS employee,
    d.name AS department,
    m.name AS manager,
    e.salary
FROM employees e
INNER JOIN departments d ON e.department_id = d.id
LEFT JOIN employees m ON e.manager_id = m.id
ORDER BY d.name, e.name;

-- Salary history for employees
SELECT
    e.name,
    s.amount,
    s.effective_date
FROM employees e
INNER JOIN salaries s ON e.id = s.employee_id
ORDER BY e.name, s.effective_date DESC;

-- Products with category names
SELECT
    p.id,
    p.name AS product_name,
    c.name AS category_name,
    p.price,
    p.stock
FROM products p
LEFT JOIN categories c ON p.category_id = c.id
WHERE p.is_available = TRUE
ORDER BY c.name, p.price;

-- ============================================================
-- Part 17: DROP Operations
-- ============================================================

-- Drop index
DROP INDEX idx_users_username ON users;
DROP INDEX idx_products_sku ON products;

-- Drop tables (at the end for cleanup if needed)
-- DROP TABLE IF EXISTS images;
-- DROP TABLE IF EXISTS files;
-- DROP TABLE IF EXISTS order_items;
-- DROP TABLE IF EXISTS orders;
-- DROP TABLE IF EXISTS products;
-- DROP TABLE IF EXISTS categories;
-- DROP TABLE IF EXISTS users;
-- DROP TABLE IF EXISTS employees;
-- DROP TABLE IF EXISTS departments;
-- DROP TABLE IF EXISTS salaries;
-- DROP TABLE IF EXISTS audit_log;

-- ============================================================
-- Part 18: Backup and Recovery (if supported)
-- ============================================================

-- Backup database
BACKUP DATABASE TO '/tmp/xxsql_backup.xbak' WITH COMPRESS;

-- Restore database (commented out to prevent accidental overwrite)
-- RESTORE DATABASE FROM '/tmp/xxsql_backup.xbak';

-- ============================================================
-- End of Test Suite
-- ============================================================