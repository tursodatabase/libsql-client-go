<p align="center">
  <a href="https://docs.turso.tech/sdk/go/quickstart">
    <img alt="Turso + Go cover" src="https://github.com/tursodatabase/libsql-client-go/assets/950181/4edaaa78-aa41-4aa2-9d45-d21bbe4e807b" width="1000">
    <h3 align="center">Turso + Go</h3>
  </a>
</p>

<p align="center">
  Turso is a SQLite-compatible database built on libSQL.
</p>

<p align="center">
  <a href="https://turso.tech"><strong>Turso</strong></a> ·
  <a href="https://docs.turso.tech/quickstart"><strong>Quickstart</strong></a> ·
  <a href="/examples"><strong>Examples</strong></a> ·
  <a href="https://docs.turso.tech"><strong>Docs</strong></a> ·
  <a href="https://discord.com/invite/4B5D7hYwub"><strong>Discord</strong></a> ·
  <a href="https://blog.turso.tech/"><strong>Tutorials</strong></a>
</p>

---

## Install

```bash
go get github.com/tursodatabase/libsql-client-go
```

## Connect

This module implements a libSQL driver for the standard Go [`database/sql`](https://pkg.go.dev/database/sql) package that works with [Turso](#turso), [local SQLite](#local-turso), and [libSQL server](#libsql-server).

### Turso

Follow the [Turso Quickstart](https://docs.turso.tech/quickstart) to create an account, database, auth token, and connect to the shell to create a schema.

```go
package main

import (
  "database/sql"
  "fmt"
  "os"

  _ "github.com/tursodatabase/libsql-client-go/libsql"
)

func main() {
  url := "libsql://[DATABASE].turso.io?authToken=[TOKEN]"

  db, err := sql.Open("libsql", url)
  if err != nil {
    fmt.Fprintf(os.Stderr, "failed to open db %s: %s", url, err)
    os.Exit(1)
  }
  defer db.Close()

  ctx := context.Background()
}
```

### Local SQLite

To use a sqlite3 database file, you must also install and import one of the
following SQLite drivers:

- [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite) (non-CGO, recommended)
- [github.com/mattn/go-sqlite3](https://pkg.go.dev/github.com/mattn/go-sqlite3)

```go
import (
  "database/sql"
  "fmt"
  "os"

  _ "github.com/tursodatabase/libsql-client-go/libsql"
  _ "modernc.org/sqlite"
)

func main() {
  var url = "file:path/to/file.db"

  db, err := sql.Open("libsql", url)
  if err != nil {
    fmt.Fprintf(os.Stderr, "failed to open db %s: %s", url, err)
      os.Exit(1)
  }
}
```

### libSQL Server

You can use this module with [libSQL server](https://github.com/tursodatabase/libsql/tree/main/libsql-server) directly using one of these methods [here](https://github.com/tursodatabase/libsql/blob/main/docs/BUILD-RUN.md).

```go
package main

import (
  "database/sql"
  "fmt"
  "os"

  _ "github.com/tursodatabase/libsql-client-go/libsql"
)

func main() {
  var url = "http://127.0.0.1:8080"

  db, err := sql.Open("libsql", url)
  if err != nil {
    fmt.Fprintf(os.Stderr, "failed to open db %s: %s", url, err)
    os.Exit(1)
  }
}
```

## Execute

You can `database/sql` as you normally would:

```go
type User struct {
  ID   int
  Name string
}

func queryUsers(db *sql.DB)  {
  rows, err := db.Query("SELECT * FROM users")
  if err != nil {
    fmt.Fprintf(os.Stderr, "failed to execute query: %v\n", err)
    os.Exit(1)
  }
  defer rows.Close()

  var users []User

  for rows.Next() {
    var user User

    if err := rows.Scan(&user.ID, &user.Name); err != nil {
      fmt.Println("Error scanning row:", err)
      return
    }

    users = append(users, user)
    fmt.Println(user.ID, user.Name)
  }

  if err := rows.Err(); err != nil {
    fmt.Println("Error during rows iteration:", err)
  }
}
```

Then simply update `func main()` and pass `db`:

```go
queryUsers(db)
```

## Limitations

- **This library will be replaced with [`go-libsql`](https://github.com/libsql/go-libsql) once stable.**
- This driver currently does not support prepared statements using `db.Prepare` when querying sqld over HTTP.
- This driver does not support embedded replicas &mdash; see [`go-libsql`](https://github.com/libsql/go-libsql).

## License

This project is licensed under the MIT license.
