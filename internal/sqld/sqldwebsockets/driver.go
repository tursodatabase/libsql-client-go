package sqldwebsockets

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"sort"
)

type sqldWsResult struct {
	id      int64
	changes int64
}

func (r *sqldWsResult) LastInsertId() (int64, error) {
	return r.id, nil
}

func (r *sqldWsResult) RowsAffected() (int64, error) {
	return r.changes, nil
}

type sqldWsRows struct {
	res           *ExecResponse
	currentRowIdx int
}

func (r *sqldWsRows) Columns() []string {
	return r.res.Columns()
}

func (r *sqldWsRows) Close() error {
	return nil
}

func (r *sqldWsRows) Next(dest []driver.Value) error {
	if r.currentRowIdx == r.res.RowsCount() {
		return io.EOF
	}
	count := r.res.RowLen(r.currentRowIdx)
	for idx := 0; idx < count; idx++ {
		v, err := r.res.Value(r.currentRowIdx, idx)
		if err != nil {
			return err
		}
		dest[idx] = v
	}
	r.currentRowIdx++
	return nil
}

type sqldWsConn struct {
	ws *SqldWebsocket
}

func SqldConnect(url string, jwt string) (*sqldWsConn, error) {
	c, err := Connect(url, jwt)
	if err != nil {
		return nil, err
	}
	return &sqldWsConn{c}, nil
}

func (c *sqldWsConn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("Prepare method not implemented")
}

func (c *sqldWsConn) Close() error {
	return c.ws.Close()
}

func (c *sqldWsConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("Begin method not implemented")
}

func convertArgs(args []driver.NamedValue) Params {
	if len(args) == 0 {
		return Params{}
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
	return Params{Names: names, Values: values}
}

func (c *sqldWsConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	res, err := c.ws.Exec(query, convertArgs(args), false)
	if err != nil {
		return nil, err
	}
	return &sqldWsResult{0, res.AffectedRowCount()}, nil
}

func (c *sqldWsConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	res, err := c.ws.Exec(query, convertArgs(args), true)
	if err != nil {
		return nil, err
	}
	return &sqldWsRows{res, 0}, nil
}
