package main

import (
	"database/sql"
	"fmt"
	"os"
	"sync"

	_ "github.com/libsql/libsql-client-go/sql_driver"
)

func exec(db *sql.DB, stmt string, args ...any) sql.Result {
	res, err := db.Exec(stmt, args...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to execute statement %s: %s", stmt, err)
		os.Exit(1)
	}
	return res
}

func query(db *sql.DB, stmt string, args ...any) *sql.Rows {
	res, err := db.Query(stmt, args...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to execute query %s: %s", stmt, err)
		os.Exit(1)
	}
	return res
}

func runCounterExample(dbPath string) {
	db, err := sql.Open("libsql", dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open db %s: %s", dbPath, err)
		os.Exit(1)
	}
	exec(db, "CREATE TABLE IF NOT EXISTS counter(country TEXT, city TEXT, value INT, PRIMARY KEY(country, city)) WITHOUT ROWID")

	incCounterStatementPositionalArgs := "INSERT INTO counter(country, city, value) VALUES(?, ?, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = ? AND city = ?"
	exec(db, incCounterStatementPositionalArgs, "PL", "WAW", "PL", "WAW")
	exec(db, incCounterStatementPositionalArgs, "FI", "HEL", "FI", "HEL")
	/* Uncomment once https://github.com/libsql/sqld/issues/237 is fixed */
	// incCounterStatementNamedArgs := "INSERT INTO counter(country, city, value) VALUES(:country, :city, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = :country AND city = :city"
	// exec(db, incCounterStatementNamedArgs, sql.Named("country", "PL"), sql.Named("city", "WAW"))
	// exec(db, incCounterStatementNamedArgs, sql.Named("country", "FI"), sql.Named("city", "HEL"))
	// incCounterStatementNamedArgs2 := "INSERT INTO counter(country, city, value) VALUES(@country, @city, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = @country AND city = @city"
	// exec(db, incCounterStatementNamedArgs2, sql.Named("country", "PL"), sql.Named("city", "WAW"))
	// exec(db, incCounterStatementNamedArgs2, sql.Named("country", "FI"), sql.Named("city", "HEL"))
	// incCounterStatementNamedArgs3 := "INSERT INTO counter(country, city, value) VALUES($country, $city, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = $country AND city = $city"
	// exec(db, incCounterStatementNamedArgs3, sql.Named("country", "PL"), sql.Named("city", "WAW"))
	// exec(db, incCounterStatementNamedArgs3, sql.Named("country", "FI"), sql.Named("city", "HEL"))
	rows := query(db, "SELECT * FROM counter")
	for rows.Next() {
		var row struct {
			country string
			city    string
			value   int
		}
		if err := rows.Scan(&row.country, &row.city, &row.value); err != nil {
			fmt.Fprintf(os.Stderr, "failed to scan row: %s", err)
			os.Exit(1)
		}
		fmt.Println(row)
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "errors from query: %s", err)
		os.Exit(1)
	}
}

func runConcurrentExample(dbPath string) {
	db, err := sql.Open("libsql", dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open db %s: %s", dbPath, err)
		os.Exit(1)
	}
	exec(db, "DROP TABLE IF EXISTS table1")
	exec(db, "DROP TABLE IF EXISTS table2")
	exec(db, "DROP TABLE IF EXISTS table3")
	exec(db, "CREATE TABLE table1(key int, value int)")
	exec(db, "CREATE TABLE table2(key int, value int)")
	exec(db, "CREATE TABLE table3(key int, value int)")
	for i := 1; i < 10; i++ {
		exec(db, "INSERT INTO table1 VALUES(?, ?)", i, i)
		exec(db, "INSERT INTO table2 VALUES(?, ?)", i, -1*i)
		exec(db, "INSERT INTO table3 VALUES(?, ?)", i, 0)
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 1; i < 100; i++ {
			rows := query(db, "SELECT value FROM table1")
			for rows.Next() {
				var v int
				if err := rows.Scan(&v); err != nil {
					fmt.Fprintf(os.Stderr, "failed to scan row: %s", err)
					os.Exit(1)
				}
				if v <= 0 {
					fmt.Fprintf(os.Stderr, "got non-positive value from table1: %d", v)
					os.Exit(1)
				}
			}
			if err := rows.Err(); err != nil {
				fmt.Fprintf(os.Stderr, "errors from query: %s", err)
				os.Exit(1)
			}
		}
	}(&wg)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 1; i < 100; i++ {
			rows := query(db, "SELECT value FROM table2")
			for rows.Next() {
				var v int
				if err := rows.Scan(&v); err != nil {
					fmt.Fprintf(os.Stderr, "failed to scan row: %s", err)
					os.Exit(1)
				}
				if v >= 0 {
					fmt.Fprintf(os.Stderr, "got non-negative value from table2: %d", v)
					os.Exit(1)
				}
			}
			if err := rows.Err(); err != nil {
				fmt.Fprintf(os.Stderr, "errors from query: %s", err)
				os.Exit(1)
			}
		}
	}(&wg)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 1; i < 100; i++ {
			rows := query(db, "SELECT value FROM table3")
			for rows.Next() {
				var v int
				if err := rows.Scan(&v); err != nil {
					fmt.Fprintf(os.Stderr, "failed to scan row: %s", err)
					os.Exit(1)
				}
				if v != 0 {
					fmt.Fprintf(os.Stderr, "got non-zero value from table3: %d", v)
					os.Exit(1)
				}
			}
			if err := rows.Err(); err != nil {
				fmt.Fprintf(os.Stderr, "errors from query: %s", err)
				os.Exit(1)
			}
		}
	}(&wg)
	wg.Wait()
}

var dbUrl = ""
var dbFile = "file:test.db"

func main() {
	runCounterExample(dbUrl)
	runCounterExample(dbFile)
	runConcurrentExample(dbUrl)
	runConcurrentExample(dbFile)
}
