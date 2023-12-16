package ws

import (
	"context"
	"database/sql/driver"
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

type stmt struct {
	c     *conn
	query string
}

func (s stmt) Close() error {
	return nil
}

func (s stmt) NumInput() int {
	return -1
}

func convertToNamed(args []driver.Value) []driver.NamedValue {
	if len(args) == 0 {
		return nil
	}
	result := []driver.NamedValue{}
	for idx := range args {
		result = append(result, driver.NamedValue{Ordinal: idx, Value: args[idx]})
	}
	return result
}

func (s stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), convertToNamed(args))
}

func (s stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), convertToNamed(args))
}

func (s stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.c.ExecContext(ctx, s.query, args)
}

func (s stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.c.QueryContext(ctx, s.query, args)
}

func (c *conn) Ping() error {
	return c.PingContext(context.Background())
}

func (c *conn) PingContext(ctx context.Context) error {
	_, err := c.ws.exec(ctx, "SELECT 1", params{}, false)
	return err
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	return stmt{c, query}, nil
}

func (c *conn) Close() error {
	return c.ws.Close()
}

type tx struct {
	c *conn
}

func (t tx) Commit() error {
	_, err := t.c.ExecContext(context.Background(), "COMMIT", nil)
	if err != nil {
		return err
	}
	return nil
}

func (t tx) Rollback() error {
	_, err := t.c.ExecContext(context.Background(), "ROLLBACK", nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	_, err := c.ExecContext(ctx, "BEGIN", nil)
	if err != nil {
		return tx{nil}, err
	}
	return tx{c}, nil
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
	res, err := c.ws.exec(ctx, query, convertArgs(args), false)
	if err != nil {
		return nil, err
	}
	return &result{res.lastInsertId(), res.affectedRowCount()}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	res, err := c.ws.exec(ctx, query, convertArgs(args), true)
	if err != nil {
		return nil, err
	}
	return &rows{res, 0}, nil
}
