package http

import (
	"database/sql/driver"
	"github.com/libsql/libsql-client-go/libsql/internal/http/basic"
)

func Connect(url, jwt string) driver.Conn {
	return basic.Connect(url, jwt)
}
