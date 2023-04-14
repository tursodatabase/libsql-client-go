package libsql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/libsql/libsql-client-go/libsql/internal/http"
	"github.com/libsql/libsql-client-go/libsql/internal/ws"
	"modernc.org/sqlite"
	"net/url"
	"strings"
)

type LibsqlDriver struct {
}

func (d *LibsqlDriver) Open(dbUrl string) (driver.Conn, error) {
	u, err := url.Parse(dbUrl)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "file" {
		return (&sqlite.Driver{}).Open(dbUrl)
	}
	if u.Scheme == "wss" || u.Scheme == "ws" {
		return ws.Connect(dbUrl, u.Query().Get("jwt"))
	}
	if u.Scheme == "https" || u.Scheme == "http" {
		return http.Connect(dbUrl), nil
	}
	if u.Scheme == "libsql" {
		urlWithoutSchema, _ := strings.CutPrefix(dbUrl, "libsql://")
		url := fmt.Sprintf("wss://%s", urlWithoutSchema)
		return ws.Connect(url, u.Query().Get("jwt"))
	}
	return nil, fmt.Errorf("unsupported db path: %s\nThis driver supports only db paths that start with libsql://, file://, https://, http://, wss:// and ws://", dbUrl)
}

func init() {
	sql.Register("libsql", &LibsqlDriver{})
}
