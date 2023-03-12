package sqldwebsockets

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
	res           *execResponse
	currentRowIdx int
}

func (r *rows) Columns() []string {
	return r.res.columns()
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.currentRowIdx == r.res.rowsCount() {
		return io.EOF
	}
	count := r.res.rowLen(r.currentRowIdx)
	for idx := 0; idx < count; idx++ {
		v, err := r.res.value(r.currentRowIdx, idx)
		if err != nil {
			return err
		}
		dest[idx] = v
	}
	r.currentRowIdx++
	return nil
}

type conn struct {
	ws *websocketConn
}

func Connect(url string, jwt string) (*conn, error) {
	c, err := connect(url, jwt)
	if err != nil {
		return nil, err
	}
	return &conn{c}, nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("Prepare method not implemented")
}

func (c *conn) Close() error {
	return c.ws.Close()
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("Begin method not implemented")
}

func convertArgs(args []driver.NamedValue) params {
	if len(args) == 0 {
		return params{}
	}
	positionalArgs := [](*driver.NamedValue){}
	namedArgs := []namedParam{}
	for idx := range args {
		if len(args[idx].Name) > 0 {
			namedArgs = append(namedArgs, namedParam{args[idx].Name, args[idx].Value})
		} else {
			positionalArgs = append(positionalArgs, &args[idx])
		}
	}
	sort.Slice(positionalArgs, func(i, j int) bool {
		return positionalArgs[i].Ordinal < positionalArgs[j].Ordinal
	})
	posArgs := [](any){}
	for idx := range positionalArgs {
		posArgs = append(posArgs, positionalArgs[idx].Value)
	}
	return params{PositinalArgs: posArgs, NamedArgs: namedArgs}
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	res, err := c.ws.exec(query, convertArgs(args), false)
	if err != nil {
		return nil, err
	}
	return &result{0, res.affectedRowCount()}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	res, err := c.ws.exec(query, convertArgs(args), true)
	if err != nil {
		return nil, err
	}
	return &rows{res, 0}, nil
}
