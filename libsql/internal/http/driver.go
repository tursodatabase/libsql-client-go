package http

import (
	"database/sql/driver"
	"github.com/libsql/libsql-client-go/libsql/internal/http/basic"
	"github.com/libsql/libsql-client-go/libsql/internal/http/hranaV2"
)

func Connect(url, jwt string) driver.Conn {
	if hranaV2.IsSupported(url, jwt) {
		return hranaV2.Connect(url, jwt)
	}
	return basic.Connect(url, jwt)
}
