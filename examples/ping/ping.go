// A simple example showing how to connect to a libsql server.
//
// Specify the libsql server URL via an env variable. If you are running
// libsql server locally, you can use the following:
//
// export LIBSQL_URL=http://127.0.0.1:8000
//
// If you are using Turso, you can use the following:
//
// export LIBSQL_URL=libsql://[your-database].turso.io?authToken=[your-auth-token]
package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
)

func main() {
	var dbUrl = os.Getenv("LIBSQL_URL")
	// dbUrl = "libsql://settled-ultron-avinassh.turso.io?authToken=eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJpYXQiOiIyMDIzLTEyLTA5VDE0OjAwOjMyLjE0Nzg3MzQ3NFoiLCJpZCI6IjU2NTM4MTMxLTk0MzUtMTFlZS1hMzQxLTZhY2IyYWVmMjgzZCJ9.TiGq3ctAwuSNUhqEBijWhhh_7VZQhRkxZzqQmwihmutfu8htPSfZmVU3WWTquqKq4CqgiEmUgqseBInsqGKvCg"
	dbUrl = "http://127.0.0.1:8000/"
	db, err := sql.Open("libsql", dbUrl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open db %s: %s", dbUrl, err)
		os.Exit(1)
	}
	defer db.Close()
	if err = db.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to ping db %s: %s", dbUrl, err)
		os.Exit(1)
	}
}
