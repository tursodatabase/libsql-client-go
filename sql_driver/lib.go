package libsqldriver

import (
	"database/sql"
	"database/sql/driver"
	"strings"

	"github.com/libsql/libsql-client-go/internal/sqld/sqldhttp"
	"github.com/mattn/go-sqlite3"
)

type LibsqlDriver struct {
}

func (d *LibsqlDriver) Open(dbPath string) (driver.Conn, error) {
	if strings.HasPrefix(dbPath, "file:") {
		return (&sqlite3.SQLiteDriver{}).Open(dbPath)
	}
	return sqldhttp.SqldConnect(dbPath), nil
}

func init() {
	sql.Register("libsql", &LibsqlDriver{})
}
