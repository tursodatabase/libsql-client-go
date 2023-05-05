package libsql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"

	"github.com/libsql/libsql-client-go/libsql/internal/http"
	"github.com/libsql/libsql-client-go/libsql/internal/ws"
)

func contains(s []string, item string) bool {
	for idx := range s {
		if s[idx] == item {
			return true
		}
	}
	return false
}

type LibsqlDriver struct {
}

func (d *LibsqlDriver) Open(dbUrl string) (driver.Conn, error) {
	u, err := url.Parse(dbUrl)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "file" {
		expectedDrivers := []string{"sqlite", "sqlite3"}
		presentDrivers := sql.Drivers()
		for _, expectedDriver := range expectedDrivers {
			if contains(presentDrivers, expectedDriver) {
				db, err := sql.Open(expectedDriver, dbUrl)
				if err != nil {
					return nil, err
				}
				return db.Driver().Open(dbUrl)
			}
		}
		return nil, fmt.Errorf("no sqlite driver present. Please import sqlite or sqlite3 driver.")
	}
	if u.Scheme == "wss" || u.Scheme == "ws" {
		return ws.Connect(dbUrl, u.Query().Get("jwt"))
	}
	if u.Scheme == "https" || u.Scheme == "http" {
		return http.Connect(dbUrl, u.Query().Get("jwt")), nil
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
