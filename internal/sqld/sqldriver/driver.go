package sqlddriver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"sort"

	sqldhttp "github.com/libsql/libsql-client-go/internal/sqld/http"
)

type sqldResult struct {
	id      int64
	changes int64
}

func (r *sqldResult) LastInsertId() (int64, error) {
	return r.id, nil
}

func (r *sqldResult) RowsAffected() (int64, error) {
	return r.changes, nil
}

type sqldRows struct {
	result        *sqldhttp.ResultSet
	currentRowIdx int
}

func (r *sqldRows) Columns() []string {
	return r.result.Columns
}

func (r *sqldRows) Close() error {
	return nil
}

func (r *sqldRows) Next(dest []driver.Value) error {
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

type sqldConn struct {
	url string
}

func SqldConnect(url string) *sqldConn {
	return &sqldConn{url}
}

func (c *sqldConn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("Prepare method not implemented")
}

func (c *sqldConn) Close() error {
	return nil
}

func (c *sqldConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("Begin method not implemented")
}

func convertArgs(args []driver.NamedValue) sqldhttp.Params {
	if len(args) == 0 {
		return sqldhttp.Params{}
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
	return sqldhttp.Params{Names: names, Values: values}
}

func (c *sqldConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	_, err := sqldhttp.CallSqld(c.url, query, convertArgs(args))
	if err != nil {
		return nil, err
	}
	return &sqldResult{0, 0}, nil
}

func (c *sqldConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rs, err := sqldhttp.CallSqld(c.url, query, convertArgs(args))
	if err != nil {
		return nil, err
	}
	return &sqldRows{rs, 0}, nil
}
