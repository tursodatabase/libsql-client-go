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

func IsSupported(url, jwt string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", url+"/v2", nil)
	if err != nil {
		return false
	}
	if len(jwt) > 0 {
		req.Header.Set("Authorization", "Bearer "+jwt)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func Connect(url, jwt string) driver.Conn {
	return &hranaV2Conn{url, jwt, "", 0, false}
}

type hranaV2Stmt struct {
	conn     *hranaV2Conn
	numInput int
	sqlId    int32
}

func (s *hranaV2Stmt) Close() error {
	var req hrana.PipelineRequest
	req.Add(hrana.CloseStoredSqlStream(s.sqlId))
	_, err := s.conn.sendPipelineRequest(context.Background(), &req)
	return err
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
	msg := hrana.PipelineRequest{}
	params, err := shared.ConvertArgs(args)
	if err != nil {
		return nil, err
	}
	executeStream, err := hrana.ExecuteStoredStream(s.sqlId, params, false)
	if err != nil {
		return nil, err
	}
	msg.Add(*executeStream)
	result, err := s.conn.sendPipelineRequest(ctx, &msg)
	if err != nil {
		return nil, err
	}
	if result.Results[0].Error != nil {
		return nil, errors.New(result.Results[0].Error.Message)
	}
	if result.Results[0].Response == nil {
		return nil, errors.New("no response received")
	}
	res, err := result.Results[0].Response.ExecuteResult()
	if err != nil {
		return nil, err
	}
	return shared.NewResult(res.GetLastInsertRowId(), int64(res.AffectedRowCount)), nil
}

func (s *hranaV2Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	msg := hrana.PipelineRequest{}
	params, err := shared.ConvertArgs(args)
	if err != nil {
		return nil, err
	}
	executeStream, err := hrana.ExecuteStoredStream(s.sqlId, params, true)
	if err != nil {
		return nil, err
	}
	msg.Add(*executeStream)
	result, err := s.conn.sendPipelineRequest(ctx, &msg)
	if err != nil {
		return nil, err
	}
	if result.Results[0].Error != nil {
		return nil, errors.New(result.Results[0].Error.Message)
	}
	if result.Results[0].Response == nil {
		return nil, errors.New("no response received")
	}
	res, err := result.Results[0].Response.ExecuteResult()
	if err != nil {
		return nil, err
	}
	return shared.NewRows(&StmtResultRowsProvider{res}), nil
}

type hranaV2Conn struct {
	url          string
	jwt          string
	baton        string
	nextSqlId    int32
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
	var req hrana.PipelineRequest
	sqlId := h.nextSqlId
	req.Add(hrana.StoreSqlStream(query, sqlId))
	h.nextSqlId++

	_, err = h.sendPipelineRequest(ctx, &req)
	if err != nil {
		return nil, err
	}

	return &hranaV2Stmt{h, numInput, sqlId}, nil
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

func (h *hranaV2Conn) sendPipelineRequest(ctx context.Context, msg *hrana.PipelineRequest) (*hrana.PipelineResponse, error) {
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
	if result.Baton == "" {
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

	result, err := h.sendPipelineRequest(ctx, msg)
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
	return p.r.Rows[rowIdx][colIdx].ToValue()
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
	return p.r.StepResults[setIdx].Rows[rowIdx][colIdx].ToValue()
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
