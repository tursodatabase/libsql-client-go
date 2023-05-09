package libsql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/libsql/libsql-client-go/libsql/internal/http"
	"github.com/libsql/libsql-client-go/libsql/internal/ws"
	"net/url"
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

// ExtractJwt extracts the JWT from the URL and removes it from the url.
func extractJwt(u *url.URL) (string, error) {
	authToken := u.Query().Get("authToken")
	jwt := u.Query().Get("jwt")
	u.Query().Del("authToken")
	u.Query().Del("jwt")
	if authToken != "" && jwt != "" {
		return "", fmt.Errorf("both authToken and jwt are present in the url. Please use only one of them")
	}
	if authToken != "" {
		return authToken, nil
	} else {
		return jwt, nil
	}
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
	if u.Scheme == "libsql" {
		u.Scheme = "wss"
	}
	if u.Scheme == "wss" || u.Scheme == "ws" {
		jwt, err := extractJwt(u)
		if err != nil {
			return nil, err
		}
		return ws.Connect(u.String(), jwt)
	}
	if u.Scheme == "https" || u.Scheme == "http" {
		jwt, err := extractJwt(u)
		if err != nil {
			return nil, err
		}
		return http.Connect(u.String(), jwt), nil
	}
	return nil, fmt.Errorf("unsupported db path: %s\nThis driver supports only db paths that start with libsql://, file://, https://, http://, wss:// and ws://", dbUrl)
}

func init() {
	sql.Register("libsql", &LibsqlDriver{})
}
