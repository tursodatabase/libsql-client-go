package http

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/libsql/libsql-client-go/libsql/internal/http/shared"
)

type httpResultsRowsProvider struct {
	results []httpResults
}

func (r *httpResultsRowsProvider) SetsCount() int {
	return len(r.results)
}

func (r *httpResultsRowsProvider) RowsCount(setIdx int) int {
	return len(r.results[setIdx].Results.Rows)
}

func (r *httpResultsRowsProvider) Columns(setIdx int) []string {
	return r.results[setIdx].Results.Columns
}

func (r *httpResultsRowsProvider) FieldValue(setIdx, rowIdx, columnIdx int) driver.Value {
	return r.results[setIdx].Results.Rows[rowIdx][columnIdx]
}

func (r *httpResultsRowsProvider) Error(setIdx int) string {
	if r.results[setIdx].Error != nil {
		return r.results[setIdx].Error.Message
	}
	return ""
}

func (r *httpResultsRowsProvider) HasResult(setIdx int) bool {
	return r.results[setIdx].Results != nil
}

type conn struct {
	url string
	jwt string
}

func Connect(url, jwt string) *conn {
	return &conn{url, jwt}
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare method not implemented")
}

func (c *conn) Close() error {
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("begin method not implemented")
}

func execute(ctx context.Context, url, jwt, query string, args []driver.NamedValue) ([]httpResults, error) {
	stmts, params, err := shared.ParseStatement(query, args)
	if err != nil {
		return nil, fmt.Errorf("failed to execute SQL: %s\n%w", query, err)
	}

	rs, err := callSqld(ctx, url, jwt, stmts, params)
	if err != nil {
		return nil, fmt.Errorf("failed to execute SQL: %s\n%w", query, err)
	}
	return rs, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	rs, err := execute(ctx, c.url, c.jwt, query, args)
	if err != nil {
		return nil, err
	}

	if err := assertNoResultWithError(rs, query); err != nil {
		return nil, err
	}

	return shared.NewResult(0, 0), nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rs, err := execute(ctx, c.url, c.jwt, query, args)
	if err != nil {
		return nil, err
	}

	return shared.NewRows(&httpResultsRowsProvider{rs}), nil
}

func assertNoResultWithError(resultSets []httpResults, query string) error {
	for _, result := range resultSets {
		if result.Error != nil {
			return fmt.Errorf("failed to execute SQL: %s\n%s", query, result.Error.Message)
		}
		if result.Results == nil {
			return fmt.Errorf("no results for statement")
		}
	}
	return nil
}
