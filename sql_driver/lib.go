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

func (d *LibsqlDriver) Open(dbUrl string) (driver.Conn, error) {
	if strings.HasPrefix(dbUrl, "file:") {
		return (&sqlite3.SQLiteDriver{}).Open(dbUrl)
	}
	if strings.HasPrefix(dbUrl, "wss://") {
		jwt := os.Getenv("SQLD_AUTH_TOKEN")
		if len(jwt) == 0 {
			return nil, fmt.Errorf("missing authorization token. Please provide JWT by setting SQLD_AUTH_TOKEN env variable")
		}
		return sqldwebsockets.SqldConnect(dbUrl, jwt)
	}
	if strings.HasPrefix(dbUrl, "https://") {
		return sqldhttp.SqldConnect(dbUrl), nil
	}
	return nil, fmt.Errorf("unsupported db path: %s\nThis driver supports only db paths that start with file://, https:// or wss://", dbUrl)
}

func init() {
	sql.Register("libsql", &LibsqlDriver{})
}
