package libsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/libsql/libsql-client-go/libsql/internal/http"
	"github.com/libsql/libsql-client-go/libsql/internal/ws"
)

type config struct {
	authToken *string
	tls       *bool
	proxy     *string
}

type Option interface {
	apply(*config) error
}

type option func(*config) error

func (o option) apply(c *config) error {
	return o(c)
}

func WithAuthToken(authToken string) Option {
	return option(func(o *config) error {
		if o.authToken != nil {
			return fmt.Errorf("authToken already set")
		}
		if authToken == "" {
			return fmt.Errorf("authToken must not be empty")
		}
		o.authToken = &authToken
		return nil
	})
}

func WithTls(tls bool) Option {
	return option(func(o *config) error {
		if o.tls != nil {
			return fmt.Errorf("tls already set")
		}
		o.tls = &tls
		return nil
	})
}

func WithProxy(proxy string) Option {
	return option(func(o *config) error {
		if o.proxy != nil {
			return fmt.Errorf("proxy already set")
		}
		if proxy == "" {
			return fmt.Errorf("proxy must not be empty")
		}
		o.proxy = &proxy
		return nil
	})
}

func (c config) connector(dbPath string) (driver.Connector, error) {
	u, err := url.Parse(dbPath)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "file" {
		if strings.HasPrefix(dbPath, "file://") && !strings.HasPrefix(dbPath, "file:///") {
			return nil, fmt.Errorf("invalid database URL: %s. File URLs should not have double leading slashes. ", dbPath)
		}
		expectedDrivers := []string{"sqlite", "sqlite3"}
		presentDrivers := sql.Drivers()
		for _, expectedDriver := range expectedDrivers {
			if slices.Contains(presentDrivers, expectedDriver) {
				db, err := sql.Open(expectedDriver, dbPath)
				if err != nil {
					return nil, err
				}
				return &fileConnector{url: dbPath, driver: db.Driver()}, nil
			}
		}
		return nil, fmt.Errorf("no sqlite driver present. Please import sqlite or sqlite3 driver")
	}

	query := u.Query()
	if query.Has("auth_token") {
		return nil, fmt.Errorf("'auth_token' usage forbidden. Please use 'WithAuthToken' option instead")
	}
	if query.Has("authToken") {
		return nil, fmt.Errorf("'authToken' usage forbidden. Please use 'WithAuthToken' option instead")
	}
	if query.Has("jwt") {
		return nil, fmt.Errorf("'jwt' usage forbidden. Please use 'WithAuthToken' option instead")
	}
	if query.Has("tls") {
		return nil, fmt.Errorf("'tls' usage forbidden. Please use 'WithTls' option instead")
	}

	for name := range query {
		return nil, fmt.Errorf("unknown query parameter %#v", name)
	}

	if u.Scheme == "libsql" {
		if c.tls == nil || *c.tls {
			u.Scheme = "https"
		} else {
			if c.tls != nil && u.Port() == "" {
				return nil, fmt.Errorf("libsql:// URL without tls must specify an explicit port")
			}
			u.Scheme = "http"
		}
	}

	if (u.Scheme == "wss" || u.Scheme == "https") && c.tls != nil && !*c.tls {
		return nil, fmt.Errorf("%s:// URL cannot opt out of TLS. Only libsql:// can opt in/out of TLS", u.Scheme)
	}
	if (u.Scheme == "ws" || u.Scheme == "http") && c.tls != nil && *c.tls {
		return nil, fmt.Errorf("%s:// URL cannot opt in to TLS. Only libsql:// can opt in/out of TLS", u.Scheme)
	}

	authToken := ""
	if c.authToken != nil {
		authToken = *c.authToken
	}

	host := u.Host
	if c.proxy != nil {
		if u.Scheme == "ws" || u.Scheme == "wss" {
			return nil, fmt.Errorf("proxying of ws:// and wss:// URLs is not supported")
		}
		proxy, err := url.Parse(*c.proxy)
		if err != nil {
			return nil, err
		}
		u.Host = proxy.Host
		if proxy.Scheme != "" {
			u.Scheme = proxy.Scheme
		}
	}

	if u.Scheme == "wss" || u.Scheme == "ws" {
		return wsConnector{url: u.String(), authToken: authToken}, nil
	}
	if u.Scheme == "https" || u.Scheme == "http" {
		return httpConnector{url: u.String(), authToken: authToken, host: host}, nil
	}

	return nil, fmt.Errorf("unsupported URL scheme: %s\nThis driver supports only URLs that start with libsql://, file://, https://, http://, wss:// and ws://", u.Scheme)
}

func NewConnector(dbPath string, opts ...Option) (driver.Connector, error) {
	var config config
	errs := make([]error, 0, len(opts))
	for _, opt := range opts {
		if err := opt.apply(&config); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return config.connector(dbPath)
}

type httpConnector struct {
	url       string
	authToken string
	host      string
}

func (c httpConnector) Connect(_ctx context.Context) (driver.Conn, error) {
	return http.Connect(c.url, c.authToken, c.host), nil
}

func (c httpConnector) Driver() driver.Driver {
	return Driver{}
}

type wsConnector struct {
	url       string
	authToken string
}

func (c wsConnector) Connect(_ctx context.Context) (driver.Conn, error) {
	return ws.Connect(c.url, c.authToken)
}

func (c wsConnector) Driver() driver.Driver {
	return Driver{}
}

type fileConnector struct {
	url    string
	driver driver.Driver
}

func (c fileConnector) Connect(_ctx context.Context) (driver.Conn, error) {
	return c.driver.Open(c.url)
}

func (c fileConnector) Driver() driver.Driver {
	return Driver{}
}

type Driver struct {
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

func (d Driver) Open(dbUrl string) (driver.Conn, error) {
	u, err := url.Parse(dbUrl)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "file" {
		if strings.HasPrefix(dbUrl, "file://") && !strings.HasPrefix(dbUrl, "file:///") {
			return nil, fmt.Errorf("invalid database URL: %s. File URLs should not have double leading slashes. ", dbUrl)
		}
		expectedDrivers := []string{"sqlite", "sqlite3"}
		presentDrivers := sql.Drivers()
		for _, expectedDriver := range expectedDrivers {
			if slices.Contains(presentDrivers, expectedDriver) {
				db, err := sql.Open(expectedDriver, dbUrl)
				if err != nil {
					return nil, err
				}
				return db.Driver().Open(dbUrl)
			}
		}
		return nil, fmt.Errorf("no sqlite driver present. Please import sqlite or sqlite3 driver")
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

	for name := range query {
		return nil, fmt.Errorf("unknown query parameter %#v", name)
	}
	u.RawQuery = ""

	if u.Scheme == "libsql" {
		if tls {
			u.Scheme = "https"
		} else {
			if u.Port() == "" {
				return nil, fmt.Errorf("libsql:// URL with ?tls=0 must specify an explicit port")
			}
			u.Scheme = "http"
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
		return http.Connect(u.String(), jwt, u.Host), nil
	}

	return nil, fmt.Errorf("unsupported URL scheme: %s\nThis driver supports only URLs that start with libsql://, file://, https://, http://, wss:// and ws://", u.Scheme)
}

func init() {
	sql.Register("libsql", Driver{})
}
