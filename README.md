# Go SDK for libSQL

[![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/libsql/libsql-client-go/blob/main/LICENSE)

This module implements a libSQL driver for the standard Go [database/sql
package]. You can use it to interact with the following types of databases:

- Local SQLite database files
- [libSQL sqld] instances (including [Turso])

## Installation

Install the driver into your module:

```bash
go get github.com/libsql/libsql-client-go
```

Import the driver into your code using a blank import:

```go
import (
	_ "github.com/libsql/libsql-client-go/libsql"
)
```

Ensure all the module requirements are up to date:

```bash
go mod tidy
```

### Add support for sqlite3 database files

To use a sqlite3 database file, you must also install and import one of the
following SQLite drivers:

- [modernc.org/sqlite] (non-CGO, recommended)
- [github.com/mattn/go-sqlite3]

This enables the use of `file:` URLs with this driver.

## Open a connection to sqld

Specify the "libsql" driver and a database URL in your call to `sql.Open`:

```go
import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/libsql/libsql-client-go/libsql"
)

var dbUrl = "http://127.0.0.1:8080"
db, err := sql.Open("libsql", dbUrl)
if err != nil {
    fmt.Fprintf(os.Stderr, "failed to open db %s: %s", dbUrl, err)
    os.Exit(1)
}
```

If your sqld instance is managed by Turso, the database URL must contain a
valid database auth token in the query string:

```go
var dbUrl = "libsql://[your-database].turso.io?authToken=[your-auth-token]"
```

## Open a connection to a local sqlite3 database file

You can use a `file:` URL to locate a sqlite3 database file for use with this
driver. The example below assumes that the package `modernc.org/sqlite` is
installed:

```go
import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/libsql/libsql-client-go/libsql"
	_ "modernc.org/sqlite"
)

var dbUrl = "file://path/to/file.db"
db, err := sql.Open("libsql", dbUrl)
if err != nil {
    fmt.Fprintf(os.Stderr, "failed to open db %s: %s", dbUrl, err)
    os.Exit(1)
}
```

## Compatibility with database/sql

This driver currently does not support prepared statements using [db.Prepare()]
when querying sqld over HTTP.

## License

This project is licensed under the MIT license.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `sqld` by you, shall be licensed as MIT, without any additional
terms or conditions.


[database/sql package]: https://pkg.go.dev/database/sql
[libSQL sqld]: https://github.com/libsql/sqld/
[Turso]: https://turso.tech
[modernc.org/sqlite]: https://pkg.go.dev/modernc.org/sqlite
[github.com/mattn/go-sqlite3]: https://pkg.go.dev/github.com/mattn/go-sqlite3
[db.Prepare()]: https://pkg.go.dev/database/sql#DB.Prepare
