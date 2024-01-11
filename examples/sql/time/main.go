package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
)

func fatalOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	dbURL := os.Getenv("LIBSQL_TEST_HTTP_DB_URL")
	if dbURL == "" {
		dbURL = "http://127.0.0.1:8000/dev/db1"
	}
	db, err := sql.Open("libsql", dbURL)
	fatalOnErr(err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test (name TEXT, time INTEGER)")
	fatalOnErr(err)

	now := time.Now().Unix()
	_, err = db.Exec("INSERT INTO test (name, time) VALUES (?, ?)", "hello world", now)
	fatalOnErr(err)

	var got int64
	err = db.QueryRow("SELECT time FROM test WHERE name = ?", "hello world").Scan(&got)
	fatalOnErr(err)
	fmt.Printf("inserted = %d, got = %d\n", now, got)

	// query all rows
	rows, err := db.Query("SELECT * FROM test")
	fatalOnErr(err)
	defer rows.Close()
	for rows.Next() {
		var name string
		var time int64
		err := rows.Scan(&name, &time)
		fatalOnErr(err)
		fmt.Printf("name = %s, time = %d\n", name, time)
	}
}
