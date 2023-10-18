package hranaV2

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/libsql/libsql-client-go/libsql/internal/hrana"
	"github.com/libsql/libsql-client-go/libsql/internal/http/shared"
	"io"
	"net/http"
	"time"
)

func Connect(url, jwt, host string) driver.Conn {
	return &hranaV2Conn{url, jwt, host, "", false}
}

type hranaV2Stmt struct {
	conn     *hranaV2Conn
	numInput int
	sql      string
}

func (s *hranaV2Stmt) Close() error {
	return nil
}

func (s *hranaV2Stmt) NumInput() int {
	return s.numInput
}

func convertToNamed(args []driver.Value) []driver.NamedValue {
	if len(args) == 0 {
		return nil
	}
	var result []driver.NamedValue
	for idx := range args {
		result = append(result, driver.NamedValue{Ordinal: idx, Value: args[idx]})
	}
	return result
}

func (s *hranaV2Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), convertToNamed(args))
}

func (s *hranaV2Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), convertToNamed(args))
}

func (s *hranaV2Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.sql, args)
}

func (s *hranaV2Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.sql, args)
}

type hranaV2Conn struct {
	url          string
	jwt          string
	host         string
	baton        string
	streamClosed bool
}

func (h *hranaV2Conn) Prepare(query string) (driver.Stmt, error) {
	return h.PrepareContext(context.Background(), query)
}

func (h *hranaV2Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmts, paramInfos, err := shared.ParseStatement(query)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("only one statement is supported got %d", len(stmts))
	}
	numInput := -1
	if len(paramInfos[0].NamedParameters) == 0 {
		numInput = paramInfos[0].PositionalParametersCount
	}
	return &hranaV2Stmt{h, numInput, query}, nil
}

func (h *hranaV2Conn) Close() error {
	return nil
}

func (h *hranaV2Conn) Begin() (driver.Tx, error) {
	return h.BeginTx(context.Background(), driver.TxOptions{})
}

type hranaV2Tx struct {
	conn *hranaV2Conn
}

func (h hranaV2Tx) Commit() error {
	_, err := h.conn.ExecContext(context.Background(), "COMMIT", nil)
	return err
}

func (h hranaV2Tx) Rollback() error {
	_, err := h.conn.ExecContext(context.Background(), "ROLLBACK", nil)
	return err
}

func (h *hranaV2Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.ReadOnly {
		return nil, fmt.Errorf("read only transactions are not supported")
	}
	if opts.Isolation != driver.IsolationLevel(sql.LevelDefault) {
		return nil, fmt.Errorf("isolation level %d is not supported", opts.Isolation)
	}
	_, err := h.ExecContext(ctx, "BEGIN", nil)
	if err != nil {
		return nil, err
	}
	return &hranaV2Tx{h}, nil
}

func (h *hranaV2Conn) sendPipelineRequest(ctx context.Context, msg *hrana.PipelineRequest, streamClose bool) (*hrana.PipelineResponse, error) {
	if h.streamClosed {
		// If the stream is closed, we can't send any more requests using this connection.
		return nil, fmt.Errorf("stream is closed: %w", driver.ErrBadConn)
	}
	if h.baton != "" {
		msg.Baton = h.baton
	}
	reqBody, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", h.url+"/v2/pipeline", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	if len(h.jwt) > 0 {
		req.Header.Set("Authorization", "Bearer "+h.jwt)
	}
	req.Host = h.host
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		// We need to remember that the stream is closed so we don't try to send any more requests using this connection.
		h.streamClosed = true
		var serverError struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(body, &serverError); err == nil {
			return nil, fmt.Errorf("error code %d: %s", resp.StatusCode, serverError.Error)
		}
		var errResponse hrana.Error
		if err := json.Unmarshal(body, &errResponse); err == nil {
			if errResponse.Code != nil {
				if *errResponse.Code == "STREAM_EXPIRED" {
					return nil, fmt.Errorf("error code %s: %s\n%w", *errResponse.Code, errResponse.Message, driver.ErrBadConn)
				} else {
					return nil, fmt.Errorf("error code %s: %s", *errResponse.Code, errResponse.Message)
				}
			}
			return nil, errors.New(errResponse.Message)
		}
		return nil, fmt.Errorf("error code %d: %s", resp.StatusCode, string(body))
	}
	var result hrana.PipelineResponse
	if err = json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	h.baton = result.Baton
	if result.Baton == "" && !streamClose {
		// We need to remember that the stream is closed so we don't try to send any more requests using this connection.
		h.streamClosed = true
	}
	if result.BaseUrl != "" {
		h.url = result.BaseUrl
	}
	return &result, nil
}

func (h *hranaV2Conn) executeStmt(ctx context.Context, query string, args []driver.NamedValue, wantRows bool) (*hrana.PipelineResponse, error) {
	stmts, params, err := shared.ParseStatementAndArgs(query, args)
	if err != nil {
		return nil, fmt.Errorf("failed to execute SQL: %s\n%w", query, err)
	}
	msg := &hrana.PipelineRequest{}
	if len(stmts) == 1 {
		executeStream, err := hrana.ExecuteStream(stmts[0], params[0], wantRows)
		if err != nil {
			return nil, fmt.Errorf("failed to execute SQL: %s\n%w", query, err)
		}
		msg.Add(*executeStream)
	} else {
		batchStream, err := hrana.BatchStream(stmts, params, wantRows)
		if err != nil {
			return nil, fmt.Errorf("failed to execute SQL: %s\n%w", query, err)
		}
		msg.Add(*batchStream)
	}

	result, err := h.sendPipelineRequest(ctx, msg, false)
	if err != nil {
		return nil, fmt.Errorf("failed to execute SQL: %s\n%w", query, err)
	}

	if result.Results[0].Error != nil {
		return nil, fmt.Errorf("failed to execute SQL: %s\n%s", query, result.Results[0].Error.Message)
	}
	if result.Results[0].Response == nil {
		return nil, fmt.Errorf("failed to execute SQL: %s\n%s", query, "no response received")
	}
	return result, nil
}

func (h *hranaV2Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := h.executeStmt(ctx, query, args, false)
	if err != nil {
		return nil, err
	}
	switch result.Results[0].Response.Type {
	case "execute":
		res, err := result.Results[0].Response.ExecuteResult()
		if err != nil {
			return nil, err
		}
		return shared.NewResult(res.GetLastInsertRowId(), int64(res.AffectedRowCount)), nil
	case "batch":
		res, err := result.Results[0].Response.BatchResult()
		if err != nil {
			return nil, err
		}
		lastInsertRowId := int64(0)
		affectedRowCount := int64(0)
		for _, r := range res.StepResults {
			rowId := r.GetLastInsertRowId()
			if rowId > 0 {
				lastInsertRowId = rowId
			}
			affectedRowCount += int64(r.AffectedRowCount)
		}
		return shared.NewResult(lastInsertRowId, affectedRowCount), nil
	default:
		return nil, fmt.Errorf("failed to execute SQL: %s\n%s", query, "unknown response type")
	}
}

type StmtResultRowsProvider struct {
	r *hrana.StmtResult
}

func (p *StmtResultRowsProvider) SetsCount() int {
	return 1
}

func (p *StmtResultRowsProvider) RowsCount(setIdx int) int {
	if setIdx != 0 {
		return 0
	}
	return len(p.r.Rows)
}

func (p *StmtResultRowsProvider) Columns(setIdx int) []string {
	if setIdx != 0 {
		return nil
	}
	res := make([]string, len(p.r.Cols))
	for i, c := range p.r.Cols {
		if c.Name != nil {
			res[i] = *c.Name
		}
	}
	return res
}

func (p *StmtResultRowsProvider) FieldValue(setIdx, rowIdx, colIdx int) driver.Value {
	if setIdx != 0 {
		return nil
	}
	return p.r.Rows[rowIdx][colIdx].ToValue(p.r.Cols[colIdx].Type)
}

func (p *StmtResultRowsProvider) Error(setIdx int) string {
	return ""
}

func (p *StmtResultRowsProvider) HasResult(setIdx int) bool {
	return setIdx == 0
}

type BatchResultRowsProvider struct {
	r *hrana.BatchResult
}

func (p *BatchResultRowsProvider) SetsCount() int {
	return len(p.r.StepResults)
}

func (p *BatchResultRowsProvider) RowsCount(setIdx int) int {
	if setIdx >= len(p.r.StepResults) || p.r.StepResults[setIdx] == nil {
		return 0
	}
	return len(p.r.StepResults[setIdx].Rows)
}

func (p *BatchResultRowsProvider) Columns(setIdx int) []string {
	if setIdx >= len(p.r.StepResults) || p.r.StepResults[setIdx] == nil {
		return nil
	}
	res := make([]string, len(p.r.StepResults[setIdx].Cols))
	for i, c := range p.r.StepResults[setIdx].Cols {
		if c.Name != nil {
			res[i] = *c.Name
		}
	}
	return res
}

func (p *BatchResultRowsProvider) FieldValue(setIdx, rowIdx, colIdx int) driver.Value {
	if setIdx >= len(p.r.StepResults) || p.r.StepResults[setIdx] == nil {
		return nil
	}
	return p.r.StepResults[setIdx].Rows[rowIdx][colIdx].ToValue(p.r.StepResults[setIdx].Cols[colIdx].Type)
}

func (p *BatchResultRowsProvider) Error(setIdx int) string {
	if setIdx >= len(p.r.StepErrors) || p.r.StepErrors[setIdx] == nil {
		return ""
	}
	return p.r.StepErrors[setIdx].Message
}

func (p *BatchResultRowsProvider) HasResult(setIdx int) bool {
	return setIdx < len(p.r.StepResults) && p.r.StepResults[setIdx] != nil
}

func (h *hranaV2Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	result, err := h.executeStmt(ctx, query, args, true)
	if err != nil {
		return nil, err
	}
	switch result.Results[0].Response.Type {
	case "execute":
		res, err := result.Results[0].Response.ExecuteResult()
		if err != nil {
			return nil, err
		}
		return shared.NewRows(&StmtResultRowsProvider{res}), nil
	case "batch":
		res, err := result.Results[0].Response.BatchResult()
		if err != nil {
			return nil, err
		}
		return shared.NewRows(&BatchResultRowsProvider{res}), nil
	default:
		return nil, fmt.Errorf("failed to execute SQL: %s\n%s", query, "unknown response type")
	}
}

func (h *hranaV2Conn) closeStream(ctx context.Context) error {
	msg := &hrana.PipelineRequest{}
	msg.Add(hrana.CloseStream())
	result, err := h.sendPipelineRequest(ctx, msg, true)
	if err != nil {
		return fmt.Errorf("failed to close stream: %w", err)
	}

	if result.Results[0].Error != nil {
		return fmt.Errorf("failed to close stream: %s", result.Results[0].Error.Message)
	}
	if result.Results[0].Response == nil {
		return errors.New("failed to close stream: no response received")
	}
	return nil
}

func (h *hranaV2Conn) ResetSession(ctx context.Context) error {
	if h.baton != "" {
		err := h.closeStream(ctx)
		h.baton = ""
		return err
	}
	return nil
}
