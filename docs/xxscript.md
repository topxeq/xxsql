# XxScript - Scripting Language for XxSql

XxScript is a procedural scripting language optimized for database operations in XxSql. It provides tight integration with the database and HTTP request/response handling for building microservices.

## Usage

XxScript can be used in two ways:

1. **Microservices** - Create HTTP endpoints that execute XxScript code
2. **User-Defined Functions (UDF)** - Create SQL functions with full scripting capabilities

### User-Defined Functions

XxScript can be used to create powerful SQL functions:

```sql
-- Simple UDF
CREATE FUNCTION add_nums(x, y) RETURNS INT AS $$
    return x + y
$$;

-- UDF with SQL query
CREATE FUNCTION get_user_count() RETURNS INT AS $$
    var result = db_query("SELECT COUNT(*) as cnt FROM users")
    return result[0].cnt
$$;

-- Use in SQL queries
SELECT add_nums(3, 4);           -- Returns 7
SELECT get_user_count();         -- Returns user count
SELECT name, add_nums(id, 10) FROM users;
```

See the [User-Defined Functions](#user-defined-functions-1) section below for more details.

## Table of Contents

1. [Basic Syntax](#basic-syntax)
2. [Data Types](#data-types)
3. [Operators](#operators)
4. [Control Flow](#control-flow)
5. [Functions](#functions)
6. [Error Handling](#error-handling)
7. [Built-in Functions](#built-in-functions)
8. [HTTP Object](#http-object)
9. [Database Object](#database-object)
10. [Examples](#examples)

## Basic Syntax

### Variables

```javascript
// Declare and assign
var name = "Alice"
var age = 30
var active = true

// Reassign
name = "Bob"
```

### Comments

```javascript
// Single line comment
var x = 10  // inline comment
```

## Data Types

| Type | Example |
|------|---------|
| Number | `42`, `3.14` |
| String | `"hello"`, `'world'` |
| Boolean | `true`, `false` |
| Null | `null` |
| Array | `[1, 2, 3]` |
| Object | `{"name": "Alice", "age": 30}` |

## Operators

### Arithmetic
- `+` Addition / String concatenation
- `-` Subtraction
- `*` Multiplication
- `/` Division
- `%` Modulo

### Comparison
- `==` Equal
- `!=` Not equal
- `<` Less than
- `<=` Less than or equal
- `>` Greater than
- `>=` Greater than or equal

### Logical
- `&&` And
- `||` Or
- `!` Not

## Control Flow

### If/Else

```javascript
if (score >= 90) {
    grade = "A"
} else if (score >= 80) {
    grade = "B"
} else {
    grade = "C"
}
```

### For Loop

```javascript
for (var i = 0; i < 10; i = i + 1) {
    print(i)
}
```

### While Loop

```javascript
var count = 0
while (count < 5) {
    print(count)
    count = count + 1
}
```

### Break and Continue

```javascript
for (var i = 0; i < 10; i = i + 1) {
    if (i == 3) {
        continue  // Skip 3
    }
    if (i == 7) {
        break  // Stop at 7
    }
    print(i)
}
```

## Functions

### Defining Functions

```javascript
func greet(name) {
    return "Hello, " + name + "!"
}

var message = greet("Alice")
print(message)  // Hello, Alice!
```

### Functions with Multiple Parameters

```javascript
func add(a, b) {
    return a + b
}

func calculate(a, b, c) {
    return a * b + c
}
```

### Recursive Functions

```javascript
func factorial(n) {
    if (n <= 1) {
        return 1
    }
    return n * factorial(n - 1)
}

print(factorial(5))  // 120
```

## Error Handling

### Try/Catch

```javascript
try {
    // Code that might fail
    var data = jsonParse(userInput)
    process(data)
} catch (err) {
    http.status(400)
    http.json({"error": err})
}
```

### Throw

```javascript
func divide(a, b) {
    if (b == 0) {
        throw "Division by zero"
    }
    return a / b
}

try {
    var result = divide(10, 0)
} catch (err) {
    print("Error: " + err)
}
```

## Built-in Functions

### Type Conversion

| Function | Description | Example |
|----------|-------------|---------|
| `int(x)` | Convert to integer | `int("42")` → `42` |
| `float(x)` | Convert to float | `float("3.14")` → `3.14` |
| `string(x)` | Convert to string | `string(42)` → `"42"` |
| `typeof(x)` | Get type name | `typeof(42)` → `"int"` |

### String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `len(s)` | String length | `len("hello")` → `5` |
| `upper(s)` | Uppercase | `upper("hello")` → `"HELLO"` |
| `lower(s)` | Lowercase | `lower("HELLO")` → `"hello"` |
| `trim(s)` | Remove whitespace | `trim("  hi  ")` → `"hi"` |
| `trimPrefix(s, p)` | Remove prefix | `trimPrefix("hello", "he")` → `"llo"` |
| `trimSuffix(s, s)` | Remove suffix | `trimSuffix("hello", "lo")` → `"hel"` |
| `split(s, sep)` | Split string | `split("a,b,c", ",")` → `["a","b","c"]` |
| `join(arr, sep)` | Join array | `join([1,2,3], "-")` → `"1-2-3"` |
| `replace(s, old, new)` | Replace all | `replace("aa", "a", "b")` → `"bb"` |
| `hasPrefix(s, p)` | Check prefix | `hasPrefix("hello", "he")` → `true` |
| `hasSuffix(s, s)` | Check suffix | `hasSuffix("hello", "lo")` → `true` |
| `contains(s, sub)` | Contains substring | `contains("hello", "ell")` → `true` |
| `indexOf(s, sub)` | Find index | `indexOf("hello", "l")` → `2` |
| `substr(s, start, len)` | Substring | `substr("hello", 1, 3)` → `"ell"` |

### Array Functions

| Function | Description | Example |
|----------|-------------|---------|
| `len(arr)` | Array length | `len([1,2,3])` → `3` |
| `push(arr, v)` | Add element | `push([1,2], 3)` → `[1,2,3]` |
| `pop(arr)` | Last element | `pop([1,2,3])` → `3` |
| `slice(arr, start, end)` | Array slice | `slice([1,2,3,4], 1, 3)` → `[2,3]` |
| `range(n)` | Range of numbers | `range(3)` → `[0,1,2]` |

### Object Functions

| Function | Description | Example |
|----------|-------------|---------|
| `keys(obj)` | Get keys | `keys({"a":1})` → `["a"]` |
| `values(obj)` | Get values | `values({"a":1})` → `[1]` |

### Math Functions

| Function | Description | Example |
|----------|-------------|---------|
| `abs(x)` | Absolute value | `abs(-5)` → `5` |
| `min(a, b, ...)` | Minimum | `min(3, 1, 2)` → `1` |
| `max(a, b, ...)` | Maximum | `max(3, 1, 2)` → `3` |
| `floor(x)` | Round down | `floor(3.7)` → `3` |
| `ceil(x)` | Round up | `ceil(3.2)` → `4` |
| `round(x)` | Round nearest | `round(3.5)` → `4` |
| `sqrt(x)` | Square root | `sqrt(16)` → `4` |
| `pow(base, exp)` | Power | `pow(2, 3)` → `8` |

### JSON Functions

| Function | Description | Example |
|----------|-------------|---------|
| `json(x)` | To JSON string | `json({"a":1})` → `{"a":1}` |
| `jsonParse(s)` | Parse JSON | `jsonParse('{"a":1}')` → `{"a":1}` |

### Date/Time Functions

| Function | Description | Example |
|----------|-------------|---------|
| `now()` | Unix timestamp | `now()` → `1647849600` |
| `formatTime(ts, fmt)` | Format timestamp | `formatTime(now(), "2006-01-02")` |
| `parseTime(s, fmt)` | Parse to timestamp | `parseTime("2022-01-01", "2006-01-02")` |

### Crypto/Hash Functions

| Function | Description | Example |
|----------|-------------|---------|
| `md5(s)` | MD5 hash (hex) | `md5("hello")` → `"5d41402abc4b2a76b9719d911017c592"` |
| `sha1(s)` | SHA1 hash (hex) | `sha1("hello")` → `"aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"` |
| `sha256(s)` | SHA256 hash (hex) | `sha256("hello")` → `"2cf24dba5fb0a30e..."` |
| `sha512(s)` | SHA512 hash (hex) | `sha512("hello")` → `"9b71d224bd2f..."` |
| `base64Encode(s)` | Encode to Base64 | `base64Encode("hello")` → `"aGVsbG8="` |
| `base64Decode(s)` | Decode from Base64 | `base64Decode("aGVsbG8=")` → `"hello"` |
| `hexEncode(s)` | Encode to hex | `hexEncode("hello")` → `"68656c6c6f"` |
| `hexDecode(s)` | Decode from hex | `hexDecode("68656c6c6f")` → `"hello"` |
| `hmacSHA256(data, key)` | HMAC-SHA256 | `hmacSHA256("msg", "key")` → `"...hash..."` |

### File Functions

| Function | Description | Example |
|----------|-------------|---------|
| `fileSave(path, content)` | Save string to file | `fileSave("test.txt", "hello")` |
| `fileSave(path, content, "binary")` | Save binary (base64) | `fileSave("img.png", data, "binary")` |
| `fileRead(path)` | Read file as string | `fileRead("test.txt")` → `{"success":true, "data":"..."}` |
| `fileRead(path, "binary")` | Read file as base64 | `fileRead("img.png", "binary")` |
| `fileDelete(path)` | Delete file | `fileDelete("test.txt")` |
| `fileExists(path)` | Check if file exists | `fileExists("test.txt")` → `true/false` |
| `dirCreate(path)` | Create directory | `dirCreate("mydir/subdir")` |
| `dirList(path)` | List directory contents | `dirList("mydir")` → `[{"name":"...", "isDir":false, "size":100}, ...]` |
| `dirDelete(path)` | Delete empty directory | `dirDelete("mydir")` |
| `dirDelete(path, true)` | Delete directory recursively | `dirDelete("mydir", true)` |

**File paths** are relative to the server's configured data directory.

**Examples:**
```javascript
// Save and read text file
fileSave("data/hello.txt", "Hello World")
var result = fileRead("data/hello.txt")
print(result.data)

// Create nested directories
dirCreate("projects/myapp/static")

// Save binary data
var base64Image = "iVBORw0KGgo..."
fileSave("images/logo.png", base64Image, "binary")

// List files
var files = dirList("projects/myapp")
for (var i = 0; i < len(files); i = i + 1) {
    print(files[i].name, files[i].isDir)
}
```

### Output Functions

| Function | Description | Example |
|----------|-------------|---------|
| `print(...)` | Print values | `print("Hello", "World")` |
| `println(...)` | Print with newline | `println("Hello")` |
| `sprintf(fmt, ...)` | Format string | `sprintf("%d items", 5)` |

## HTTP Object

The `http` object provides access to HTTP request data and response methods.

### Request Properties

| Property | Description | Example |
|----------|-------------|---------|
| `http.method` | Request method | `"GET"`, `"POST"` |
| `http.path` | Request path | `"/api/users"` |
| `http.query` | Raw query string | `"id=1&name=test"` |
| `http.remoteAddr` | Client address | `"192.168.1.1:12345"` |
| `http.contentType` | Content-Type header | `"application/json"` |
| `http.userAgent` | User-Agent header | `"Mozilla/5.0..."` |

### Request Methods

```javascript
// Get query parameter
var id = http.param("id")

// Get header
var auth = http.header("Authorization")

// Get cookie
var session = http.cookie("session")

// Get request body as string
var body = http.body()

// Parse JSON body
var data = http.bodyJSON()
var name = data.name
```

### Response Methods

```javascript
// Set HTTP status code
http.status(201)

// Set response header
http.setHeader("Content-Type", "application/json")
http.setHeader("X-Custom", "value")

// Write JSON response
http.json({
    "status": "success",
    "data": result
})

// Write plain text
http.write("Hello, World!")

// Set cookie
http.setCookie("session", "abc123", 3600)  // name, value, maxAge

// Redirect
http.redirect("/login")
http.redirect("/login", 301)  // with custom status code
```

## Database Object

The `db` object provides database operations.

### Query

```javascript
// Execute SELECT and get all rows
var users = db.query("SELECT * FROM users WHERE age > 20")
for (var i = 0; i < len(users); i = i + 1) {
    print(users[i].name)
}
```

### Query Single Row

```javascript
// Get single row
var user = db.queryRow("SELECT * FROM users WHERE id = 1")
if (user != null) {
    print(user.name)
    print(user.email)
}
```

### Execute

```javascript
// Execute INSERT, UPDATE, DELETE
var result = db.exec("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")
print("Insert ID: " + result.insert_id)
print("Affected: " + result.affected)

// Update
var result = db.exec("UPDATE users SET name = 'Bob' WHERE id = 1")
print("Rows updated: " + result.affected)

// Delete
var result = db.exec("DELETE FROM users WHERE id = 1")
```

### Dynamic Queries

```javascript
// Build dynamic query from parameters
var minAge = int(http.param("min_age"))
var orderBy = http.param("sort")

if (orderBy == "") {
    orderBy = "name"
}

var users = db.query("SELECT * FROM users WHERE age >= " + string(minAge) + " ORDER BY " + orderBy)
http.json(users)
```

## Examples

### REST API Endpoint

```javascript
// GET /ms/api/users
var users = db.query("SELECT * FROM users ORDER BY name")
http.json({"users": users, "count": len(users)})
```

### CRUD Operations

```javascript
// Create user - POST /ms/api/user/create
var data = http.bodyJSON()

var result = db.exec(
    "INSERT INTO users (name, email, age) VALUES ('" +
    data.name + "', '" + data.email + "', " + string(data.age) + ")"
)

http.status(201)
http.json({
    "id": result.insert_id,
    "name": data.name,
    "message": "User created"
})
```

### Search with Pagination

```javascript
// Search users - GET /ms/api/users/search?q=alice&page=1
var query = http.param("q")
var page = int(http.param("page"))
if (page < 1) { page = 1 }

var pageSize = 20
var offset = (page - 1) * pageSize

var users = db.query(
    "SELECT * FROM users WHERE name LIKE '%" + query + "%' " +
    "LIMIT " + string(pageSize) + " OFFSET " + string(offset)
)

var total = db.queryRow(
    "SELECT COUNT(*) as count FROM users WHERE name LIKE '%" + query + "%'"
)

http.json({
    "users": users,
    "page": page,
    "total": total.count,
    "pages": ceil(total.count / pageSize)
})
```

### Error Handling

```javascript
// Create order with validation
var data = http.bodyJSON()

// Validate input
if (data.items == null || len(data.items) == 0) {
    http.status(400)
    http.json({"error": "Items required"})
}

try {
    // Create order
    var orderResult = db.exec(
        "INSERT INTO orders (user_id, total) VALUES (" +
        string(data.user_id) + ", " + string(data.total) + ")"
    )

    // Create order items
    for (var i = 0; i < len(data.items); i = i + 1) {
        var item = data.items[i]
        db.exec(
            "INSERT INTO order_items (order_id, product_id, quantity) VALUES (" +
            string(orderResult.insert_id) + ", " + string(item.product_id) +
            ", " + string(item.quantity) + ")"
        )
    }

    http.status(201)
    http.json({"order_id": orderResult.insert_id, "status": "created"})

} catch (err) {
    http.status(500)
    http.json({"error": "Failed to create order: " + string(err)})
}
```

### Authentication Check

```javascript
// Check API key from header
var apiKey = http.header("X-API-Key")

if (apiKey == "") {
    http.status(401)
    http.json({"error": "API key required"})
}

var user = db.queryRow("SELECT * FROM api_keys WHERE key = '" + apiKey + "'")

if (user == null) {
    http.status(403)
    http.json({"error": "Invalid API key"})
}

// Continue with authenticated request
var data = db.query("SELECT * FROM protected_data WHERE user_id = " + string(user.user_id))
http.json(data)
```

### Cookie-based Session

```javascript
// Login endpoint - POST /ms/auth/login
var data = http.bodyJSON()

var user = db.queryRow(
    "SELECT * FROM users WHERE username = '" + data.username + "' " +
    "AND password = '" + data.password + "'"
)

if (user == null) {
    http.status(401)
    http.json({"error": "Invalid credentials"})
}

// Set session cookie (in production, use proper session tokens)
http.setCookie("user_id", string(user.id), 86400)  // 24 hours
http.json({"message": "Logged in", "user": user.username})
```

### File Upload Handler

```javascript
// Handle file metadata (actual file upload would need server-side handling)
var data = http.bodyJSON()

// Store file metadata
var result = db.exec(
    "INSERT INTO files (name, size, type, path) VALUES ('" +
    data.name + "', " + string(data.size) + ", '" + data.type + "', '" +
    data.path + "')"
)

http.json({
    "id": result.insert_id,
    "name": data.name,
    "url": "/files/" + string(result.insert_id)
})
```

### Bulk Operations

```javascript
// Bulk insert
var items = http.bodyJSON()
var inserted = 0

for (var i = 0; i < len(items); i = i + 1) {
    var item = items[i]
    var result = db.exec(
        "INSERT INTO products (name, price, stock) VALUES ('" +
        item.name + "', " + string(item.price) + ", " + string(item.stock) + ")"
    )
    if (result.affected > 0) {
        inserted = inserted + 1
    }
}

http.json({
    "processed": len(items),
    "inserted": inserted,
    "message": sprintf("Inserted %d of %d items", inserted, len(items))
})
```

## Microservice Tables

To create a microservice endpoint, create a table with `SKEY` (primary key) and `SCRIPT` columns:

```sql
CREATE TABLE api (
    SKEY VARCHAR(50) PRIMARY KEY,
    SCRIPT TEXT
);

INSERT INTO api (SKEY, SCRIPT) VALUES ('hello', 'http.json({"message": "Hello!"})');
INSERT INTO api (SKEY, SCRIPT) VALUES ('users', 'var users = db.query("SELECT * FROM users"); http.json(users)');
```

Then access via HTTP:

```
GET /ms/api/hello       → {"message": "Hello!"}
GET /ms/api/users       → [...users...]
GET /ms/api/users?id=1  → filtered results
```

## Best Practices

1. **Validate Input**: Always validate user input before using in SQL queries
2. **Use Error Handling**: Wrap database operations in try/catch
3. **Return Proper Status Codes**: Use appropriate HTTP status codes
4. **Keep Scripts Simple**: Complex logic should be in your application layer
5. **Use Parameterized Queries**: Be careful of SQL injection with dynamic queries

## User-Defined Functions

XxScript can be used to create SQL User-Defined Functions (UDFs) with full scripting capabilities.

### Creating UDFs

**Dollar-quoted syntax (PostgreSQL style):**
```sql
CREATE FUNCTION func_name(param1, param2) RETURNS type AS $$
    -- XxScript code
    return value
$$;
```

**SCRIPT keyword syntax:**
```sql
CREATE FUNCTION func_name(param) RETURNS type SCRIPT 'return expression';
```

### UDF Examples

**Arithmetic function:**
```sql
CREATE FUNCTION add_nums(x, y) RETURNS INT AS $$
    return x + y
$$;

SELECT add_nums(3, 4);  -- Returns 7
```

**Conditional logic:**
```sql
CREATE FUNCTION abs_val(x) RETURNS INT AS $$
    if x < 0 {
        return -x
    }
    return x
$$;

SELECT abs_val(-5);  -- Returns 5
SELECT abs_val(10);  -- Returns 10
```

**Loop example:**
```sql
CREATE FUNCTION factorial(n) RETURNS INT AS $$
    if n <= 1 {
        return 1
    }
    var result = 1
    for (var i = 2; i <= n; i = i + 1) {
        result = result * i
    }
    return result
$$;

SELECT factorial(5);  -- Returns 120
```

**With SQL queries:**
```sql
CREATE FUNCTION get_user_name(user_id) RETURNS VARCHAR AS $$
    var result = db_query("SELECT name FROM users WHERE id = " + string(user_id))
    if (len(result) > 0) {
        return result[0].name
    }
    return null
$$;

SELECT get_user_name(1);  -- Returns user name
```

**Helper functions within UDF:**
```sql
CREATE FUNCTION calculate(a, b, c) RETURNS INT AS $$
    func helper(x) {
        return x * 2
    }
    return helper(a) + helper(b) + c
$$;

SELECT calculate(1, 2, 3);  -- Returns 9 (2 + 4 + 3)
```

### Differences from Microservices

| Feature | Microservice | UDF |
|---------|--------------|-----|
| HTTP access | `http` object available | No `http` object |
| SQL access | `db.query()`, `db.exec()` | `db_query()`, `db_exec()` |
| Return value | `http.json()` or `http.write()` | `return` statement |
| Usage | HTTP endpoint | SQL expression |

### Dropping UDFs

```sql
DROP FUNCTION func_name;
DROP FUNCTION IF EXISTS func_name;
```