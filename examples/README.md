# libsql-client-go Examples

This directory contains examples demonstrating how to use the libsql-client-go library.

## Prerequisites

- Go 1.20 or higher
- A running libsql server (sqld) for HTTP/WebSocket examples, or use local SQLite files

## Running Examples

### SQL Counter Example

The counter example demonstrates comprehensive usage of the library including:

- **Parameter Binding Styles**: All supported parameter formats (`?`, `?N`, `:name`, `@name`, `$name`)
- **Prepared Statements**: Efficient query execution with statement caching
- **Transactions**: ACID-compliant transactions with commit/rollback
- **Concurrent Operations**: Safe multi-threaded database access
- **Connection Management**: Single connection vs connection pooling

#### Run with Local SQLite File

```bash
cd examples/sql/counter
go run main.go
```

This will create a local `test.db` file in the current directory.

#### Run with libsql Server

First, start a local libsql server:

```bash
# Using Docker
docker run -p 8080:8080 ghcr.io/tursodatabase/libsql-server:latest

# Or using sqld directly
sqld --http-listen-addr 127.0.0.1:8080
```

Then run the example:

```bash
cd examples/sql/counter
go run main.go
```

The example will run against both the HTTP server (`http://127.0.0.1:8080`) and a local file (`file:test.db`).

## Example Features Demonstrated

### 1. Parameter Binding

The library supports multiple parameter binding styles:

```go
// Positional parameters (?)
db.Exec("INSERT INTO users VALUES (?, ?)", "John", 25)

// Indexed positional parameters (?N) - allows parameter reuse
db.Exec("UPDATE users SET name = ?1 WHERE id = ?2 AND status = ?1", "active", 123)

// Named parameters with colon (:name)
db.Exec("INSERT INTO users VALUES (:name, :age)",
    sql.Named("name", "John"),
    sql.Named("age", 25))

// Named parameters with @ (@name)
db.Exec("INSERT INTO users VALUES (@name, @age)",
    sql.Named("name", "John"),
    sql.Named("age", 25))

// Named parameters with $ ($name)
db.Exec("INSERT INTO users VALUES ($name, $age)",
    sql.Named("name", "John"),
    sql.Named("age", 25))
```

### 2. Prepared Statements

```go
stmt, err := db.Prepare("SELECT * FROM users WHERE age > ?")
defer stmt.Close()

rows, err := stmt.Query(18)
```

### 3. Transactions

```go
tx, err := db.BeginTx(ctx, nil)
defer tx.Rollback() // Rollback if not committed

// Execute multiple statements
tx.Exec("INSERT INTO accounts VALUES (?, ?)", 1, 100)
tx.Exec("UPDATE balances SET amount = amount - 100 WHERE id = ?", 2)

// Commit the transaction
err = tx.Commit()
```

### 4. Concurrent Access

The library is safe for concurrent use:

```go
// Multiple goroutines can safely use the same *sql.DB
go worker1(db)
go worker2(db)
go worker3(db)

// Or share a single connection
conn, _ := db.Conn(ctx)
go worker1WithConn(conn)
go worker2WithConn(conn)
```

## Connection Strings

The library supports multiple connection string formats:

### Remote libsql Server (HTTP)
```go
db, err := sql.Open("libsql", "http://localhost:8080")
db, err := sql.Open("libsql", "https://my-db.turso.io")
```

### Remote libsql Server (WebSocket)
```go
db, err := sql.Open("libsql", "ws://localhost:8080")
db, err := sql.Open("libsql", "wss://my-db.turso.io")
```

### Local SQLite File
```go
db, err := sql.Open("libsql", "file:local.db")
db, err := sql.Open("libsql", "file:/absolute/path/to/db.sqlite")
```

### Using libsql:// Protocol
```go
// Automatically uses HTTPS
db, err := sql.Open("libsql", "libsql://my-db.turso.io")

// Use HTTP with explicit port (TLS disabled)
db, err := sql.Open("libsql", "libsql://localhost:8080?tls=0")
```

## Using with Turso

To use with [Turso](https://turso.tech/):

```go
import (
    "database/sql"
    _ "github.com/tursodatabase/libsql-client-go/libsql"
)

func main() {
    url := "libsql://your-database.turso.io"
    authToken := "your-auth-token"

    connector, err := libsql.NewConnector(url, libsql.WithAuthToken(authToken))
    if err != nil {
        panic(err)
    }

    db := sql.OpenDB(connector)
    defer db.Close()

    // Use db as normal
}
```

## Additional Resources

- [libsql-client-go Documentation](https://github.com/tursodatabase/libsql-client-go)
- [Turso Documentation](https://docs.turso.tech/)
- [SQLite Documentation](https://www.sqlite.org/docs.html)
- [database/sql Package](https://pkg.go.dev/database/sql)

## Troubleshooting

### Connection Refused

If you get a "connection refused" error with HTTP examples:
1. Ensure sqld is running: `docker run -p 8080:8080 ghcr.io/tursodatabase/libsql-server:latest`
2. Check the port is correct in the example (default: 8080)
3. Verify no firewall is blocking the connection

### File Permission Errors

If you get permission errors with file examples:
1. Ensure you have write permissions in the directory
2. The `file:` scheme requires an absolute path or relative path from current directory
3. Use `file:///absolute/path/to/db` for absolute paths

## Contributing Examples

To contribute a new example:

1. Create a new directory under `examples/`
2. Add a clear README explaining what the example demonstrates
3. Include comments explaining key concepts
4. Ensure the example runs successfully
5. Submit a pull request
