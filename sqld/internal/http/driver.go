package http

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"sort"
)

type result struct {
	id      int64
	changes int64
}

func (r *result) LastInsertId() (int64, error) {
	return r.id, nil
}

func (r *result) RowsAffected() (int64, error) {
	return r.changes, nil
}

type rows struct {
	result        *resultSet
	currentRowIdx int
}

func (r *rows) Columns() []string {
	return r.result.Columns
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.currentRowIdx == len(r.result.Rows) {
		return io.EOF
	}
	count := len(r.result.Rows[r.currentRowIdx])
	for idx := 0; idx < count; idx++ {
		dest[idx] = r.result.Rows[r.currentRowIdx][idx]
	}
	r.currentRowIdx++
	return nil
}

type conn struct {
	url string
}

func Connect(url string) *conn {
	return &conn{url}
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("Prepare method not implemented")
}

func (c *conn) Close() error {
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("Begin method not implemented")
}

func convertArgs(args []driver.NamedValue) params {
	if len(args) == 0 {
		return params{}
	}
	sortedArgs := [](*driver.NamedValue){}
	for idx := range args {
		sortedArgs = append(sortedArgs, &args[idx])
	}
	sort.Slice(sortedArgs, func(i, j int) bool {
		return sortedArgs[i].Ordinal < sortedArgs[j].Ordinal
	})
	names := [](string){}
	values := [](any){}
	for idx := range sortedArgs {
		if len(sortedArgs[idx].Name) > 0 {
			names = append(names, sortedArgs[idx].Name)
		}
		values = append(values, sortedArgs[idx].Value)
	}
	return params{Names: names, Values: values}
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	_, err := callSqld(c.url, query, convertArgs(args))
	if err != nil {
		return nil, err
	}
	return &result{0, 0}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rs, err := callSqld(c.url, query, convertArgs(args))
	if err != nil {
		return nil, err
	}
	return &rows{rs, 0}, nil
}
