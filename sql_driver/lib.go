package libsqldriver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"

	"github.com/libsql/libsql-client-go/internal/sqld/sqldhttp"
	"github.com/libsql/libsql-client-go/internal/sqld/sqldwebsockets"
	"github.com/mattn/go-sqlite3"
)

type LibsqlDriver struct {
}

func (d *LibsqlDriver) Open(dbUrl string) (driver.Conn, error) {
	u, err := url.Parse(dbUrl)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "file" {
		return (&sqlite3.SQLiteDriver{}).Open(dbUrl)
	}
	if u.Scheme == "wss" || u.Scheme == "ws" {
		return sqldwebsockets.Connect(dbUrl, u.Query().Get("jwt"))
	}
	if u.Scheme == "https" || u.Scheme == "http" {
		return sqldhttp.Connect(dbUrl), nil
	}
	return nil, fmt.Errorf("unsupported db path: %s\nThis driver supports only db paths that start with file://, https://, http://, wss:// and ws://", dbUrl)
}

func init() {
	sql.Register("libsql", &LibsqlDriver{})
}
