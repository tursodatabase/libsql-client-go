package libsqldriver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"strings"

	"github.com/libsql/libsql-client-go/internal/sqld/sqldhttp"
	"github.com/libsql/libsql-client-go/internal/sqld/sqldwebsockets"
	"github.com/mattn/go-sqlite3"
)

type LibsqlDriver struct {
}

func (d *LibsqlDriver) Open(dbPath string) (driver.Conn, error) {
	if strings.HasPrefix(dbPath, "file:") {
		return (&sqlite3.SQLiteDriver{}).Open(dbPath)
	}
	if strings.HasPrefix(dbPath, "wss://") {
		jwt := os.Getenv("SQLD_AUTH_TOKEN")
		if len(jwt) == 0 {
			return nil, fmt.Errorf("missing authorization token. Please provide JWT by setting SQLD_AUTH_TOKEN env variable")
		}
		return sqldwebsockets.SqldConnect(dbPath, jwt)
	}
	if strings.HasPrefix(dbPath, "https://") {
		return sqldhttp.SqldConnect(dbPath), nil
	}
	return nil, fmt.Errorf("unsupported db path: %s\nThis driver supports only db paths that start with file://, https:// or wss://", dbPath)
}

func init() {
	sql.Register("libsql", &LibsqlDriver{})
}
