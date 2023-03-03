package libsqldriver

import (
	"database/sql"
	"database/sql/driver"
	"strings"

	sqlddriver "github.com/libsql/libsql-client-go/internal/sqld/sqldriver"
	"github.com/mattn/go-sqlite3"
)

type LibsqlDriver struct {
}

func (d *LibsqlDriver) Open(dbPath string) (driver.Conn, error) {
	if strings.HasPrefix(dbPath, "file:") {
		return (&sqlite3.SQLiteDriver{}).Open(dbPath)
	}
	return sqlddriver.SqldConnect(dbPath), nil
}

func init() {
	sql.Register("libsql", &LibsqlDriver{})
}
