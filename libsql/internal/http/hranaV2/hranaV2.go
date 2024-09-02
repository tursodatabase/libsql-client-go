package hranaV2

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tursodatabase/libsql-client-go/sqliteparserutils"
	"io"
	"net/http"
	net_url "net/url"
	"runtime/debug"
	"strings"

	"github.com/tursodatabase/libsql-client-go/libsql/internal/hrana"
	"github.com/tursodatabase/libsql-client-go/libsql/internal/http/shared"
)

var commitHash string

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, module := range info.Deps {
			if module.Path == "github.com/tursodatabase/libsql-client-go" {
				parts := strings.Split(module.Version, "-")
				if len(parts) == 3 {
					commitHash = parts[2][:6]
					return
				}
			}
		}
	}
	commitHash = "unknown"
}

func Connect(url, jwt, host string, schemaDb bool) driver.Conn {
	return &hranaV2Conn{url, jwt, host, schemaDb, "", false, 0}
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
	url              string
	jwt              string
	host             string
	schemaDb         bool
	baton            string
	streamClosed     bool
	replicationIndex uint64
}

func (h *hranaV2Conn) Ping() error {
	return h.PingContext(context.Background())
}

func (h *hranaV2Conn) PingContext(ctx context.Context) error {
	_, err := h.executeStmt(ctx, "SELECT 1", nil, false)
	return err
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
	if h.baton != "" {
		go func(baton, url, jwt, host string) {
			msg := hrana.PipelineRequest{Baton: baton}
			msg.Add(hrana.CloseStream())
			_, _, _ = sendPipelineRequest(context.Background(), &msg, url, jwt, host)
		}(h.baton, h.url, h.jwt, h.host)
	}
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
	if h.replicationIndex > 0 {
		addReplicationIndex(msg, h.replicationIndex)
	}
	result, streamClosed, err := sendPipelineRequest(ctx, msg, h.url, h.jwt, h.host)
	if streamClosed {
		h.streamClosed = true
	}
	if err != nil {
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
	if idx := getReplicationIndex(&result); idx > h.replicationIndex {
		h.replicationIndex = idx
	}
	return &result, nil
}

func addReplicationIndex(msg *hrana.PipelineRequest, replicationIndex uint64) {
	for i := range msg.Requests {
		if msg.Requests[i].Stmt != nil && msg.Requests[i].Stmt.ReplicationIndex == nil {
			msg.Requests[i].Stmt.ReplicationIndex = &replicationIndex
		} else if msg.Requests[i].Batch != nil && msg.Requests[i].Batch.ReplicationIndex == nil {
			msg.Requests[i].Batch.ReplicationIndex = &replicationIndex
		}
	}
}

func getReplicationIndex(response *hrana.PipelineResponse) uint64 {
	if response == nil || len(response.Results) == 0 {
		return 0
	}
	var replicationIndex uint64
	for _, result := range response.Results {
		if result.Response == nil {
			continue
		}
		if result.Response.Type == "execute" {
			if res, err := result.Response.ExecuteResult(); err == nil && res.ReplicationIndex != nil {
				if *res.ReplicationIndex > replicationIndex {
					replicationIndex = *res.ReplicationIndex
				}
			}
		} else if result.Response.Type == "batch" {
			if res, err := result.Response.BatchResult(); err == nil && res.ReplicationIndex != nil {
				if *res.ReplicationIndex > replicationIndex {
					replicationIndex = *res.ReplicationIndex
				}
			}
		}
	}
	return replicationIndex
}

func sendPipelineRequest(ctx context.Context, msg *hrana.PipelineRequest, url string, jwt string, host string) (result hrana.PipelineResponse, streamClosed bool, err error) {
	reqBody, err := json.Marshal(msg)
	if err != nil {
		return hrana.PipelineResponse{}, false, err
	}
	pipelineURL, err := net_url.JoinPath(url, "/v2/pipeline")
	if err != nil {
		return hrana.PipelineResponse{}, false, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", pipelineURL, bytes.NewReader(reqBody))
	if err != nil {
		return hrana.PipelineResponse{}, false, err
	}
	if len(jwt) > 0 {
		req.Header.Set("Authorization", "Bearer "+jwt)
	}
	req.Header.Set("x-libsql-client-version", "libsql-remote-go-"+commitHash)
	req.Host = host
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return hrana.PipelineResponse{}, false, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return hrana.PipelineResponse{}, false, err
	}
	if resp.StatusCode != http.StatusOK {
		// We need to remember that the stream is closed so we don't try to send any more requests using this connection.
		var serverError struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(body, &serverError); err == nil {
			return hrana.PipelineResponse{}, true, fmt.Errorf("error code %d: %s", resp.StatusCode, serverError.Error)
		}
		var errResponse hrana.Error
		if err := json.Unmarshal(body, &errResponse); err == nil {
			if errResponse.Code != nil {
				if *errResponse.Code == "STREAM_EXPIRED" {
					return hrana.PipelineResponse{}, true, fmt.Errorf("error code %s: %s\n%w", *errResponse.Code, errResponse.Message, driver.ErrBadConn)
				} else {
					return hrana.PipelineResponse{}, true, fmt.Errorf("error code %s: %s", *errResponse.Code, errResponse.Message)
				}
			}
			return hrana.PipelineResponse{}, true, errors.New(errResponse.Message)
		}
		return hrana.PipelineResponse{}, true, fmt.Errorf("error code %d: %s", resp.StatusCode, string(body))
	}
	if err = json.Unmarshal(body, &result); err != nil {
		return hrana.PipelineResponse{}, false, err
	}
	return result, false, nil
}

func (h *hranaV2Conn) executeMsg(ctx context.Context, msg *hrana.PipelineRequest) (*hrana.PipelineResponse, error) {
	result, err := h.sendPipelineRequest(ctx, msg, false)
	if err != nil {
		return nil, err
	}

	for _, r := range result.Results {
		if r.Error != nil {
			return nil, errors.New(r.Error.Message)
		}
		if r.Response == nil {
			return nil, errors.New("no response received")
		}
	}
	return result, nil
}

type chunker struct {
	chunk    []string
	iterator *sqliteparserutils.StatementIterator
	limit    int
}

func newChunker(iterator *sqliteparserutils.StatementIterator, limit int) *chunker {
	return &chunker{iterator: iterator, chunk: make([]string, 0, limit), limit: limit}
}

func isTransactionStatement(stmt string) bool {
	patterns := [][]byte{[]byte("begin"), []byte("commit"), []byte("end"), []byte("rollback")}
	for _, p := range patterns {
		if len(stmt) >= len(p) && bytes.Equal(bytes.ToLower([]byte(stmt[0:len(p)])), p) {
			return true
		}
	}
	return false
}

func (c *chunker) Next() (chunk []string, isEOF bool) {
	c.chunk = c.chunk[:0]
	var stmt string
	for !isEOF && len(c.chunk) < c.limit {
		stmt, _, isEOF = c.iterator.Next()
		// We need to skip transaction statements. Chunks run in a transaction by default.
		if stmt != "" && !isTransactionStatement(stmt) {
			c.chunk = append(c.chunk, stmt)
		}
	}
	return c.chunk, isEOF
}

func (h *hranaV2Conn) executeSingleStmt(ctx context.Context, stmt string, wantRows bool) (*hrana.PipelineResponse, error) {
	msg := &hrana.PipelineRequest{}
	executeStream, err := hrana.ExecuteStream(stmt, nil, wantRows)
	if err != nil {
		return nil, fmt.Errorf("failed to execute SQL: %s\n%w", stmt, err)
	}
	msg.Add(*executeStream)
	res, err := h.executeMsg(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to execute SQL: %s\n%w", stmt, err)
	}
	return res, nil
}

func (h *hranaV2Conn) executeInChunks(ctx context.Context, query string, wantRows bool) (*hrana.PipelineResponse, error) {
	const chunkSize = 4096
	iterator := sqliteparserutils.CreateStatementIterator(query)
	chunker := newChunker(iterator, chunkSize)

	chunk, isEOF := chunker.Next()
	if isEOF && len(chunk) == 1 {
		return h.executeSingleStmt(ctx, chunk[0], wantRows)
	}

	_, err := h.executeSingleStmt(ctx, "BEGIN", false)
	if err != nil {
		return nil, err
	}

	batch := &hrana.Batch{Steps: make([]hrana.BatchStep, chunkSize)}
	msg := &hrana.PipelineRequest{}
	msg.Add(hrana.StreamRequest{Type: "batch", Batch: batch})
	for idx := range batch.Steps {
		batch.Steps[idx].Stmt.WantRows = wantRows
	}

	result := &hrana.PipelineResponse{}
	for {
		for idx := range chunk {
			batch.Steps[idx].Stmt.Sql = &chunk[idx]
		}
		if len(chunk) < chunkSize {
			// We can trim batch.Steps because this is the last chunk anyway.
			// isEOF has to be true at this point.
			batch.Steps = batch.Steps[:len(chunk)]
		}
		res, err := h.executeMsg(ctx, msg)
		if err != nil {
			h.closeStream()
			return nil, fmt.Errorf("failed to execute SQL:\n%w", err)
		}
		result.Baton = res.Baton
		result.BaseUrl = res.BaseUrl
		result.Results = append(result.Results, res.Results...)
		if isEOF {
			break
		}
		chunk, isEOF = chunker.Next()
	}
	_, err = h.executeSingleStmt(ctx, "COMMIT", false)
	if err != nil {
		h.closeStream()
		return nil, err
	}
	return result, nil
}

func (h *hranaV2Conn) executeStmt(ctx context.Context, query string, args []driver.NamedValue, wantRows bool) (*hrana.PipelineResponse, error) {
	const querySizeLimitForChunking = 20 * 1024 * 1024
	if len(args) == 0 && len(query) > querySizeLimitForChunking && !h.schemaDb {
		return h.executeInChunks(ctx, query, wantRows)
	}
	stmts, params, err := shared.ParseStatementAndArgs(query, args)
	if err != nil {
		return nil, fmt.Errorf("failed to execute SQL:\n%w", err)
	}
	msg := &hrana.PipelineRequest{}
	if len(stmts) == 1 {
		var p *shared.Params
		if len(params) > 0 {
			p = &params[0]
		}
		executeStream, err := hrana.ExecuteStream(stmts[0], p, wantRows)
		if err != nil {
			return nil, fmt.Errorf("failed to execute SQL:\n%w", err)
		}
		msg.Add(*executeStream)
	} else {
		batchStream, err := hrana.BatchStream(stmts, params, wantRows, !h.schemaDb)
		if err != nil {
			return nil, fmt.Errorf("failed to execute SQL:\n%w", err)
		}
		msg.Add(*batchStream)
	}

	resp, err := h.executeMsg(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to execute SQL:\n%w", err)
	}
	return resp, nil
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
		upperBound := len(res.StepResults)
		if !h.schemaDb {
			upperBound -= 1
		}
		for idx := 0; idx < upperBound; idx++ {
			r := res.StepResults[idx]
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
		if !h.schemaDb {
			res.StepResults = res.StepResults[:len(res.StepResults)-1]
			res.StepErrors = res.StepErrors[:len(res.StepErrors)-1]
		}
		return shared.NewRows(&BatchResultRowsProvider{res}), nil
	default:
		return nil, fmt.Errorf("failed to execute SQL: %s\n%s", query, "unknown response type")
	}
}

func (h *hranaV2Conn) closeStream() {
	if h.baton != "" {
		go func(baton, url, jwt, host string) {
			msg := hrana.PipelineRequest{Baton: baton}
			msg.Add(hrana.CloseStream())
			_, _, _ = sendPipelineRequest(context.Background(), &msg, url, jwt, host)
		}(h.baton, h.url, h.jwt, h.host)
		h.baton = ""
	}
}

func (h *hranaV2Conn) ResetSession(ctx context.Context) error {
	h.closeStream()
	return nil
}
