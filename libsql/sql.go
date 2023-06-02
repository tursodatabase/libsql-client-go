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
func extractJwt(query *url.Values) (string, error) {
	authTokenSnake := query.Get("auth_token")
	authTokenCamel := query.Get("authToken")
	jwt := query.Get("jwt")
	query.Del("auth_token")
	query.Del("authToken")
	query.Del("jwt")

	countNonEmpty := func(slice ...string) int {
		count := 0
		for _, s := range slice {
			if s != "" {
				count++
			}
		}
		return count
	}

	if countNonEmpty(authTokenSnake, authTokenCamel, jwt) > 1 {
		return "", fmt.Errorf("please use at most one of the following query parameters: 'auth_token', 'authToken', 'jwt'")
	}

	if authTokenSnake != "" {
		return authTokenSnake, nil
	} else if authTokenCamel != "" {
		return authTokenCamel, nil
	} else {
		return jwt, nil
	}
}

func extractTls(query *url.Values, scheme string) (bool, error) {
	tls := query.Get("tls")
	query.Del("tls")
	if tls == "" {
		if scheme == "http" || scheme == "ws" {
			return false, nil
		} else {
			return true, nil
		}
	} else if tls == "0" {
		return false, nil
	} else if tls == "1" {
		return true, nil
	} else {
		return true, fmt.Errorf("unknown value of tls query parameter. Valid values are 0 and 1")
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

	query := u.Query()
	jwt, err := extractJwt(&query)
	if err != nil {
		return nil, err
	}

	tls, err := extractTls(&query, u.Scheme)
	if err != nil {
		return nil, err
	}

	for name, _ := range query {
		return nil, fmt.Errorf("unknown query parameter %#v", name)
	}
	u.RawQuery = ""

	if u.Scheme == "libsql" {
		if tls {
			u.Scheme = "wss"
		} else {
			if u.Port() == "" {
				return nil, fmt.Errorf("libsql:// URL with ?tls=0 must specify an explicit port")
			}
			u.Scheme = "ws"
		}
	}

	if (u.Scheme == "wss" || u.Scheme == "https") && !tls {
		return nil, fmt.Errorf("%s:// URL cannot opt out of TLS using ?tls=0", u.Scheme)
	}
	if (u.Scheme == "ws" || u.Scheme == "http") && tls {
		return nil, fmt.Errorf("%s:// URL cannot opt in to TLS using ?tls=1", u.Scheme)
	}

	if u.Scheme == "wss" || u.Scheme == "ws" {
		return ws.Connect(u.String(), jwt)
	}
	if u.Scheme == "https" || u.Scheme == "http" {
		return http.Connect(u.String(), jwt), nil
	}

	return nil, fmt.Errorf("unsupported db path: %s\nThis driver supports only db paths that start with libsql://, file://, https://, http://, wss:// and ws://", dbUrl)
}

func init() {
	sql.Register("libsql", &LibsqlDriver{})
}
