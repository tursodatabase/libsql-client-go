// Package main demonstrates the usage of libsql-client-go with various database operations.
// This example showcases:
// - Different parameter binding styles (positional, indexed, named)
// - Prepared statements
// - Transactions
// - Concurrent operations
// - Connection management
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"
)

// Helper functions for database operations

// pingContext verifies the database connection is alive.
func pingContext(ctx context.Context, db *sql.DB) {
	err := db.PingContext(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to ping db: %s", err)
		os.Exit(1)
	}
}

// exec executes a statement on the database and handles errors.
func exec(ctx context.Context, db *sql.DB, stmt string, args ...any) sql.Result {
	res, err := db.ExecContext(ctx, stmt, args...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to execute statement %s: %s", stmt, err)
		os.Exit(1)
	}
	return res
}

// query executes a query on the database and handles errors.
func query(ctx context.Context, db *sql.DB, stmt string, args ...any) *sql.Rows {
	res, err := db.QueryContext(ctx, stmt, args...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to execute query %s: %s", stmt, err)
		os.Exit(1)
	}
	return res
}

// queryConn executes a query on a specific connection and handles errors.
func queryConn(ctx context.Context, conn *sql.Conn, stmt string, args ...any) *sql.Rows {
	res, err := conn.QueryContext(ctx, stmt, args...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to execute query %s: %s", stmt, err)
		os.Exit(1)
	}
	return res
}

// execTx executes a statement within a transaction and handles errors.
func execTx(ctx context.Context, tx *sql.Tx, stmt string, args ...any) sql.Result {
	res, err := tx.ExecContext(ctx, stmt, args...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to execute statement %s: %s", stmt, err)
		os.Exit(1)
	}
	return res
}

// queryTx executes a query within a transaction and handles errors.
func queryTx(ctx context.Context, tx *sql.Tx, stmt string, args ...any) *sql.Rows {
	res, err := tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to execute query %s: %s", stmt, err)
		os.Exit(1)
	}
	return res
}

// runCounterExample demonstrates various features of libsql-client-go:
// - Creating tables
// - Different parameter binding styles (?, ?N, :name, @name, $name)
// - Prepared statements
// - Transactions with rollback handling
// - Query iteration and result scanning
func runCounterExample(dbPath string) {
	db, err := sql.Open("libsql", dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open db %s: %s", dbPath, err)
		os.Exit(1)
	}
	ctx := context.Background()

	// Verify database connection
	pingContext(ctx, db)

	// Create counter table with composite primary key
	exec(ctx, db, "CREATE TABLE IF NOT EXISTS counter(country TEXT, city TEXT, value INT, PRIMARY KEY(country, city)) WITHOUT ROWID")

	// Example 1: Positional arguments with ? placeholders
	incCounterStatementPositionalArgs := "INSERT INTO counter(country, city, value) VALUES(?, ?, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = ? AND city = ?"
	exec(ctx, db, incCounterStatementPositionalArgs, "PL", "WAW", "PL", "WAW")
	exec(ctx, db, incCounterStatementPositionalArgs, "PL", "WAW", "PL", "WAW")

	// Example 2: Indexed positional arguments with ?N placeholders (allows parameter reuse)
	incCounterStatementPositionalArgsWithIndexes := "INSERT INTO counter(country, city, value) VALUES(?1, ?2, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = ?1 AND city = ?2"
	exec(ctx, db, incCounterStatementPositionalArgsWithIndexes, "FI", "HEL")
	exec(ctx, db, incCounterStatementPositionalArgsWithIndexes, "FI", "HEL")

	// Example 3: Named arguments with :name syntax
	incCounterStatementNamedArgs := "INSERT INTO counter(country, city, value) VALUES(:country, :city, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = :country AND city = :city"
	exec(ctx, db, incCounterStatementNamedArgs, sql.Named("country", "PL"), sql.Named("city", "WAW"))
	exec(ctx, db, incCounterStatementNamedArgs, sql.Named("country", "FI"), sql.Named("city", "HEL"))

	// Example 4: Named arguments with @name syntax
	incCounterStatementNamedArgs2 := "INSERT INTO counter(country, city, value) VALUES(@country, @city, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = @country AND city = @city"
	exec(ctx, db, incCounterStatementNamedArgs2, sql.Named("country", "PL"), sql.Named("city", "WAW"))
	exec(ctx, db, incCounterStatementNamedArgs2, sql.Named("country", "FI"), sql.Named("city", "HEL"))

	// Example 5: Named arguments with $name syntax
	incCounterStatementNamedArgs3 := "INSERT INTO counter(country, city, value) VALUES($country, $city, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = $country AND city = $city"
	exec(ctx, db, incCounterStatementNamedArgs3, sql.Named("country", "PL"), sql.Named("city", "WAW"))
	exec(ctx, db, incCounterStatementNamedArgs3, sql.Named("country", "FI"), sql.Named("city", "HEL"))

	// Example 6: Prepared statements for better performance with repeated queries
	{
		stmt, err := db.Prepare("UPDATE counter SET value = value + 1 WHERE country = ? AND city = ?")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to prepare statement %s: %s", incCounterStatementPositionalArgs, err)
			os.Exit(1)
		}
		defer stmt.Close()
		_, err = stmt.Exec("FI", "HEL")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to execute prepared statement %s for FI: %s", incCounterStatementPositionalArgs, err)
			os.Exit(1)
		}
		_, err = stmt.Exec("PL", "WAW")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to execute prepared statement %s for PL: %s", incCounterStatementPositionalArgs, err)
			os.Exit(1)
		}
	}

	// Example 7: Query all counters using a prepared statement
	{
		stmt, err := db.Prepare("SELECT * FROM counter")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to prepare statement %s: %s", "SELECT * FROM counter", err)
			os.Exit(1)
		}
		defer stmt.Close()
		rows, err := stmt.Query()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to execute prepared statement %s: %s", "SELECT * FROM counter", err)
			os.Exit(1)
		}
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

	// Example 8: Direct query without prepared statement
	rows := query(ctx, db, "SELECT * FROM counter")
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

	// Example 9: Transaction with conditional logic
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start a transaction: %s", err)
		os.Exit(1)
	}
	// Defer a rollback in case anything fails
	defer func() {
		if err != nil {
			err = tx.Rollback()
			if err != nil {
				log.Fatal(err)
			}
		}
	}()

	// Query specific counters within the transaction
	// Note: Use single quotes for string literals in SQL (SQLite quirk #8: https://www.sqlite.org/quirks.html)
	rows = queryTx(ctx, tx, `SELECT * FROM counter WHERE (country = 'PL' AND city = 'WAW') OR (country = 'FI' AND city = 'HEL')`)
	wawValue := -1
	helValue := -1
	for rows.Next() {
		var row struct {
			country string
			city    string
			value   int
		}
		if err = rows.Scan(&row.country, &row.city, &row.value); err != nil {
			fmt.Fprintf(os.Stderr, "failed to scan row: %s", err)
			os.Exit(1)
		}
		if row.country == "PL" && row.city == "WAW" {
			wawValue = row.value
		}
		if row.country == "FI" && row.city == "HEL" {
			helValue = row.value
		}
	}
	if err = rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "errors from query: %s", err)
		os.Exit(1)
	}

	// Conditional update: if Helsinki has higher value, update Warsaw to match
	if helValue > wawValue {
		execTx(ctx, tx, `INSERT INTO counter(country, city, value) VALUES('PL', 'WAW', ?) ON CONFLICT DO UPDATE SET value = ? WHERE country = 'PL' AND city = 'WAW'`, helValue, helValue)
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		fmt.Fprintf(os.Stderr, "error commiting the transaction: %s", err)
		os.Exit(1)
	}
}

// runConcurrentExample demonstrates safe concurrent access to the database.
// Multiple goroutines query different tables simultaneously, verifying data integrity.
func runConcurrentExample(dbPath string) {
	db, err := sql.Open("libsql", dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open db %s: %s", dbPath, err)
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup test tables with different data patterns
	exec(ctx, db, "DROP TABLE IF EXISTS table1")
	exec(ctx, db, "DROP TABLE IF EXISTS table2")
	exec(ctx, db, "DROP TABLE IF EXISTS table3")
	exec(ctx, db, "CREATE TABLE table1(key int, value int)")
	exec(ctx, db, "CREATE TABLE table2(key int, value int)")
	exec(ctx, db, "CREATE TABLE table3(key int, value int)")

	// Populate tables with test data
	// table1: positive values, table2: negative values, table3: zero values
	for i := 1; i < 10; i++ {
		exec(ctx, db, "INSERT INTO table1 VALUES(?, ?)", i, i)
		exec(ctx, db, "INSERT INTO table2 VALUES(?, ?)", i, -1*i)
		exec(ctx, db, "INSERT INTO table3 VALUES(?, ?)", i, 0)
	}

	// Launch concurrent workers
	var wg sync.WaitGroup
	wg.Add(3)

	// worker reads from a table repeatedly and validates data integrity
	worker := func(tableName string, check func(int)) {
		defer wg.Done()
		for i := 1; i < 100; i++ {
			rows := query(ctx, db, "SELECT value FROM "+tableName)
			for rows.Next() {
				var v int
				if err := rows.Scan(&v); err != nil {
					fmt.Fprintf(os.Stderr, "failed to scan row: %s", err)
					os.Exit(1)
				}
				check(v)
			}
			if err := rows.Err(); err != nil {
				fmt.Fprintf(os.Stderr, "errors from query: %s", err)
				os.Exit(1)
			}
		}
	}
	go worker("table1", func(v int) {
		if v <= 0 {
			fmt.Fprintf(os.Stderr, "got non-positive value from table1: %d", v)
			os.Exit(1)
		}
	})
	go worker("table2", func(v int) {
		if v >= 0 {
			fmt.Fprintf(os.Stderr, "got non-negative value from table2: %d", v)
			os.Exit(1)
		}
	})
	go worker("table3", func(v int) {
		if v != 0 {
			fmt.Fprintf(os.Stderr, "got non-zero value from table3: %d", v)
			os.Exit(1)
		}
	})
	wg.Wait()
	fmt.Println("Concurrent example completed successfully")
}

// runConcurrentOnOneConnectionExample demonstrates concurrent access using a single connection.
// This shows how multiple goroutines can safely share a single database connection.
func runConcurrentOnOneConnectionExample(dbPath string) {
	db, err := sql.Open("libsql", dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open db %s: %s", dbPath, err)
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup test tables
	exec(ctx, db, "DROP TABLE IF EXISTS table1")
	exec(ctx, db, "DROP TABLE IF EXISTS table2")
	exec(ctx, db, "DROP TABLE IF EXISTS table3")
	exec(ctx, db, "CREATE TABLE table1(key int, value int)")
	exec(ctx, db, "CREATE TABLE table2(key int, value int)")
	exec(ctx, db, "CREATE TABLE table3(key int, value int)")

	// Populate tables
	for i := 1; i < 10; i++ {
		exec(ctx, db, "INSERT INTO table1 VALUES(?, ?)", i, i)
		exec(ctx, db, "INSERT INTO table2 VALUES(?, ?)", i, -1*i)
		exec(ctx, db, "INSERT INTO table3 VALUES(?, ?)", i, 0)
	}

	var wg sync.WaitGroup
	wg.Add(3)

	// Get a single connection to be shared across goroutines
	conn, err := db.Conn(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get db connection %s: %s", dbPath, err)
		os.Exit(1)
	}
	defer conn.Close()

	// worker uses the shared connection for queries
	worker := func(tableName string, check func(int)) {
		defer wg.Done()
		for i := 1; i < 100; i++ {
			rows := queryConn(ctx, conn, "SELECT value FROM "+tableName)
			for rows.Next() {
				var v int
				if err := rows.Scan(&v); err != nil {
					fmt.Fprintf(os.Stderr, "failed to scan row: %s", err)
					os.Exit(1)
				}
				check(v)
			}
			if err := rows.Err(); err != nil {
				fmt.Fprintf(os.Stderr, "errors from query: %s", err)
				os.Exit(1)
			}
		}
	}
	go worker("table1", func(v int) {
		if v <= 0 {
			fmt.Fprintf(os.Stderr, "got non-positive value from table1: %d", v)
			os.Exit(1)
		}
	})
	go worker("table2", func(v int) {
		if v >= 0 {
			fmt.Fprintf(os.Stderr, "got non-negative value from table2: %d", v)
			os.Exit(1)
		}
	})
	go worker("table3", func(v int) {
		if v != 0 {
			fmt.Fprintf(os.Stderr, "got non-zero value from table3: %d", v)
			os.Exit(1)
		}
	})
	wg.Wait()
	fmt.Println("Concurrent on one connection example completed successfully")
}

// Database URLs for examples
var (
	// dbUrl points to a local libsql server (e.g., sqld)
	dbUrl = "http://127.0.0.1:8080"
	// dbFile points to a local SQLite file
	dbFile = "file:test.db"
)

func main() {
	fmt.Println("Running counter example with HTTP...")
	runCounterExample(dbUrl)

	fmt.Println("\nRunning counter example with local file...")
	runCounterExample(dbFile)

	fmt.Println("\nRunning concurrent example with HTTP...")
	runConcurrentExample(dbUrl)

	fmt.Println("\nRunning concurrent example with local file...")
	runConcurrentExample(dbFile)

	fmt.Println("\nRunning concurrent on one connection example with HTTP...")
	runConcurrentOnOneConnectionExample(dbUrl)

	fmt.Println("\nRunning concurrent on one connection example with local file...")
	runConcurrentOnOneConnectionExample(dbFile)

	fmt.Println("\nAll examples completed successfully!")
}
