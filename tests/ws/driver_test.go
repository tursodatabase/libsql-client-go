package ws

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"testing"

	"github.com/tursodatabase/libsql-client-go/libsql"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
)

// setupDB sets up a test database by connecting to libsql server and creates a `test` table
func setupDB(ctx context.Context, t *testing.T) *sql.DB {
	dbURL := os.Getenv("LIBSQL_TEST_WS_DB_URL")
	authToken := os.Getenv("LIBSQL_TEST_WS_AUTH_TOKEN")
	var connector driver.Connector
	var err error
	if authToken == "" {
		connector, err = libsql.NewConnector(dbURL)
	} else {
		connector, err = libsql.NewConnector(dbURL, libsql.WithAuthToken(authToken))
	}
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func cleanupDB(ctx context.Context, t *testing.T, db *sql.DB) {
	_, err := db.ExecContext(ctx, "DROP TABLE test")
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
}

func assertRows(ctx context.Context, t *testing.T, db *sql.DB) {
	rows, err := db.QueryContext(ctx, "SELECT * FROM test")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var expectedId int64 = 1
	for rows.Next() {
		var id int64
		var name string
		err := rows.Scan(&id, &name)
		if err != nil {
			t.Fatal(err)
		}
		if id != expectedId {
			t.Fatalf("id should be %d", expectedId)
		}
		if name != "hello world" {
			t.Fatal("name should be hello world")
		}
		expectedId++
	}
	err = rows.Err()
	if err != nil {
		t.Fatal(err)
	}
}

func TestPing(t *testing.T) {
	ctx := context.Background()
	db := setupDB(ctx, t)

	// This ping should succeed because the database is up and running
	err := db.PingContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupDB(ctx, t, db)
		// This ping should return an error because the database is already closed
		err = db.PingContext(ctx)
		if err == nil {
			t.Fatal("db.Ping succeeded when it should have failed")
		}
	})

}

func TestQueryExec(t *testing.T) {
	ctx := context.Background()
	db := setupDB(ctx, t)
	_, err := db.ExecContext(ctx, "INSERT INTO test (name) VALUES (?)", "hello world")
	if err != nil {
		t.Fatal(err)
	}
	assertRows(ctx, t, db)

	t.Cleanup(func() {
		cleanupDB(ctx, t, db)
	})
}

func TestTransactions(t *testing.T) {
	ctx := context.Background()
	db := setupDB(ctx, t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO test (name) VALUES (?)", "hello world")
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	assertRows(ctx, t, db)

	t.Cleanup(func() {
		cleanupDB(ctx, t, db)
	})
}

func TestPreparedStatements(t *testing.T) {
	ctx := context.Background()
	db := setupDB(ctx, t)
	stmt, err := db.PrepareContext(ctx, "INSERT INTO test (name) VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := stmt.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = stmt.ExecContext(ctx, "hello world")
	if err != nil {
		t.Fatal(err)
	}
	assertRows(ctx, t, db)

	t.Cleanup(func() {
		cleanupDB(ctx, t, db)
	})
}

func TestPreparedStatementsWithTransactions(t *testing.T) {
	ctx := context.Background()
	db := setupDB(ctx, t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO test (name) VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = stmt.ExecContext(ctx, "hello world")
	if err != nil {
		t.Fatal(err)
	}
	func() { // needed to surpress linter
		err = stmt.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	assertRows(ctx, t, db)

	t.Cleanup(func() {
		cleanupDB(ctx, t, db)
	})
}

func TestTransactionsRollback(t *testing.T) {
	ctx := context.Background()
	db := setupDB(ctx, t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO test (name) VALUES (?)", "hello world")
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("count should be 0")
	}

	t.Cleanup(func() {
		cleanupDB(ctx, t, db)
	})
}

func TestPreparedStatementsWithTransactionsRollback(t *testing.T) {
	ctx := context.Background()
	db := setupDB(ctx, t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO test (name) VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = stmt.ExecContext(ctx, "hello world")
	if err != nil {
		t.Fatal(err)
	}
	func() { // needed to surpress linter
		err = stmt.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("count should be 0")
	}

	t.Cleanup(func() {
		cleanupDB(ctx, t, db)
	})
}

func TestCancelContext(t *testing.T) {
	db := setupDB(context.Background(), t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
	if err == nil {
		t.Fatal("should have failed")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatal("should have failed with context.Canceled")
	}
	db.Close()
}

func TestCancelTransactionWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	db := setupDB(ctx, t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO test (name) VALUES (?)", "hello world")
	if err != nil {
		t.Fatal(err)
	}
	// let's cancel the context before the commit
	cancel()
	err = tx.Commit()
	if err == nil {
		t.Fatal("should have failed")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatal("should have failed with context.Canceled")
	}
	// rolling back the transaction should not result in any error
	err = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupDB(context.Background(), t, db)
	})
}

func TestDataTypes(t *testing.T) {
	db := setupDB(context.Background(), t)
	defer db.Close()
	ctx := context.Background()
	var (
		text        string
		nullText    sql.NullString
		integer     sql.NullInt64
		nullInteger sql.NullInt64
		boolean     bool
		float8      float64
		nullFloat   sql.NullFloat64
		bytea       []byte
	)
	err := db.QueryRowContext(ctx, "SELECT 'foobar' as text, NULL as text,  NULL as integer, 42 as integer, 1 as boolean, X'000102' as bytea, 3.14 as float8, NULL as float8;").Scan(&text, &nullText, &nullInteger, &integer, &boolean, &bytea, &float8, &nullFloat)
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case text != "foobar":
		t.Error("value mismatch - text")
	case nullText.Valid:
		t.Error("null text is valid")
	case nullInteger.Valid:
		t.Error("null integer is valid")
	case !integer.Valid:
		t.Error("integer is not valid")
	case integer.Int64 != 42:
		t.Error("value mismatch - integer")
	case !boolean:
		t.Error("value mismatch - boolean")
	case float8 != 3.14:
		t.Error("value mismatch - float8")
	case !bytes.Equal(bytea, []byte{0, 1, 2}):
		t.Error("value mismatch - bytea")
	case nullFloat.Valid:
		t.Error("null float is valid")
	}
}

func TestExecResult(t *testing.T) {
	ctx := context.Background()
	db := setupDB(ctx, t)
	result, err := db.ExecContext(ctx, "INSERT INTO test (name) VALUES (?)", "hello world")
	if err != nil {
		t.Fatal(err)
	}
	assertRows(ctx, t, db)

	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if affected != 1 {
		t.Fatal("affected should be 1")
	}
	lastInsertID, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if lastInsertID != 1 {
		t.Fatal("lastInsertID should be 1")
	}
	t.Cleanup(func() {
		cleanupDB(ctx, t, db)
	})
}

func TestParameterSyntaxSupport(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := setupDB(ctx, t)

	_, err := db.ExecContext(ctx, "INSERT INTO test (name) VALUES (?1)", "hello world")
	if err != nil {
		t.Fatal(err)
	}
	assertRows(ctx, t, db)

	_, err = db.ExecContext(ctx, "INSERT INTO test (name) VALUES (?)", "hello world")
	if err != nil {
		t.Fatal(err)
	}
	assertRows(ctx, t, db)

	_, err = db.ExecContext(ctx, "INSERT INTO test (name) VALUES (:a)", sql.Named("a", "hello world"))
	if err != nil {
		t.Fatal(err)
	}
	assertRows(ctx, t, db)

	_, err = db.ExecContext(ctx, "INSERT INTO test (name) VALUES (@a)", sql.Named("a", "hello world"))
	if err != nil {
		t.Fatal(err)
	}
	assertRows(ctx, t, db)

	_, err = db.ExecContext(ctx, "INSERT INTO test (name) VALUES ($a)", sql.Named("a", "hello world"))
	if err != nil {
		t.Fatal(err)
	}
	assertRows(ctx, t, db)

	t.Cleanup(func() {
		cleanupDB(ctx, t, db)
	})
}
