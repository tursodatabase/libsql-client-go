package http

import (
	"database/sql/driver"

	"github.com/tursodatabase/libsql-client-go/libsql/internal/http/hranaV2"
)

func Connect(url, jwt, host string) driver.Conn {
	return hranaV2.Connect(url, jwt, host)
}
