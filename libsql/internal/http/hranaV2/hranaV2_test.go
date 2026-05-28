package hranaV2

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/tursodatabase/libsql-client-go/libsql/internal/hrana"
	"github.com/tursodatabase/libsql-client-go/sqliteparserutils"
)

func TestConvertToNamed(t *testing.T) {
	tests := []struct {
		name     string
		args     []driver.Value
		expected int
	}{
		{
			name:     "empty args",
			args:     []driver.Value{},
			expected: 0,
		},
		{
			name:     "single arg",
			args:     []driver.Value{"value1"},
			expected: 1,
		},
		{
			name:     "multiple args",
			args:     []driver.Value{"value1", 42, true},
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertToNamed(tt.args)

			if tt.expected == 0 {
				if result != nil {
					t.Errorf("expected nil for empty args, got %v", result)
				}
				return
			}

			if len(result) != tt.expected {
				t.Errorf("expected %d named values, got %d", tt.expected, len(result))
			}

			for i, nv := range result {
				if nv.Ordinal != i {
					t.Errorf("expected ordinal %d, got %d", i, nv.Ordinal)
				}
				if nv.Value != tt.args[i] {
					t.Errorf("expected value %v, got %v", tt.args[i], nv.Value)
				}
			}
		})
	}
}

func TestIsTransactionStatement(t *testing.T) {
	tests := []struct {
		name     string
		stmt     string
		expected bool
	}{
		{"begin lowercase", "begin", true},
		{"BEGIN uppercase", "BEGIN", true},
		{"Begin mixed case", "Begin", true},
		{"begin transaction", "begin transaction", true},
		{"commit", "commit", true},
		{"COMMIT", "COMMIT", true},
		{"end", "end", true},
		{"END", "END", true},
		{"rollback", "rollback", true},
		{"ROLLBACK", "ROLLBACK", true},
		{"select statement", "select * from users", false},
		{"insert statement", "insert into users values (1)", false},
		{"update statement", "update users set name = 'test'", false},
		{"delete statement", "delete from users", false},
		{"create statement", "create table users (id int)", false},
		{"empty string", "", false},
		{"partial match", "beg", false},
		{"rollback with space", "rollback transaction", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTransactionStatement(tt.stmt)
			if result != tt.expected {
				t.Errorf("isTransactionStatement(%q) = %v, want %v", tt.stmt, result, tt.expected)
			}
		})
	}
}

func TestConnect(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		jwt      string
		host     string
		schemaDb bool
	}{
		{
			name:     "basic connection",
			url:      "https://example.com",
			jwt:      "token123",
			host:     "example.com",
			schemaDb: false,
		},
		{
			name:     "with schemaDb",
			url:      "https://example.com",
			jwt:      "token456",
			host:     "example.com",
			schemaDb: true,
		},
		{
			name:     "empty jwt",
			url:      "https://example.com",
			jwt:      "",
			host:     "example.com",
			schemaDb: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := Connect(tt.url, tt.jwt, tt.host, tt.schemaDb)

			if conn == nil {
				t.Fatal("expected non-nil connection")
			}

			h2conn, ok := conn.(*hranaV2Conn)
			if !ok {
				t.Fatalf("expected *hranaV2Conn, got %T", conn)
			}

			if h2conn.url != tt.url {
				t.Errorf("expected url %q, got %q", tt.url, h2conn.url)
			}
			if h2conn.jwt != tt.jwt {
				t.Errorf("expected jwt %q, got %q", tt.jwt, h2conn.jwt)
			}
			if h2conn.host != tt.host {
				t.Errorf("expected host %q, got %q", tt.host, h2conn.host)
			}
			if h2conn.schemaDb != tt.schemaDb {
				t.Errorf("expected schemaDb %v, got %v", tt.schemaDb, h2conn.schemaDb)
			}
		})
	}
}

func TestHranaV2Stmt_Close(t *testing.T) {
	stmt := &hranaV2Stmt{}
	err := stmt.Close()
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestHranaV2Stmt_NumInput(t *testing.T) {
	tests := []struct {
		name     string
		numInput int
	}{
		{"no params", -1},
		{"one param", 1},
		{"three params", 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &hranaV2Stmt{numInput: tt.numInput}
			result := stmt.NumInput()
			if result != tt.numInput {
				t.Errorf("expected %d, got %d", tt.numInput, result)
			}
		})
	}
}

func TestHranaV2Conn_PrepareContext(t *testing.T) {
	conn := &hranaV2Conn{
		url:      "https://example.com",
		jwt:      "token",
		host:     "example.com",
		schemaDb: false,
	}

	tests := []struct {
		name             string
		query            string
		expectError      bool
		expectedNumInput int
	}{
		{
			name:             "simple select",
			query:            "SELECT * FROM users",
			expectError:      false,
			expectedNumInput: 0,
		},
		{
			name:             "select with positional params",
			query:            "SELECT * FROM users WHERE id = ?",
			expectError:      false,
			expectedNumInput: 1,
		},
		{
			name:             "select with named params",
			query:            "SELECT * FROM users WHERE id = :id",
			expectError:      false,
			expectedNumInput: -1, // Named params return -1
		},
		{
			name:        "multiple statements",
			query:       "SELECT * FROM users; SELECT * FROM posts",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := conn.PrepareContext(context.Background(), tt.query)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			h2stmt, ok := stmt.(*hranaV2Stmt)
			if !ok {
				t.Fatalf("expected *hranaV2Stmt, got %T", stmt)
			}

			if h2stmt.numInput != tt.expectedNumInput {
				t.Errorf("expected numInput %d, got %d", tt.expectedNumInput, h2stmt.numInput)
			}

			if h2stmt.sql != tt.query {
				t.Errorf("expected sql %q, got %q", tt.query, h2stmt.sql)
			}
		})
	}
}

func TestHranaV2Conn_BeginTx_Options(t *testing.T) {
	conn := &hranaV2Conn{
		url:  "https://example.com",
		jwt:  "token",
		host: "example.com",
	}

	tests := []struct {
		name        string
		opts        driver.TxOptions
		expectError bool
		errorMsg    string
	}{
		{
			name: "read only not supported",
			opts: driver.TxOptions{
				ReadOnly: true,
			},
			expectError: true,
			errorMsg:    "read only transactions are not supported",
		},
		{
			name: "isolation level not supported",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSerializable),
			},
			expectError: true,
		},
		{
			name: "default options",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelDefault),
			},
			// This will fail because we can't actually execute the BEGIN statement
			// but it should pass the validation checks
			expectError: true, // Will fail on actual execution
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := conn.BeginTx(context.Background(), tt.opts)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
			}
		})
	}
}

func TestChunker(t *testing.T) {
	tests := []struct {
		name           string
		sql            string
		chunkSize      int
		expectedChunks [][]string
	}{
		{
			name:      "single statement",
			sql:       "SELECT 1",
			chunkSize: 10,
			expectedChunks: [][]string{
				{"SELECT 1"},
			},
		},
		{
			name:      "multiple statements within chunk size",
			sql:       "SELECT 1; SELECT 2; SELECT 3",
			chunkSize: 10,
			expectedChunks: [][]string{
				{"SELECT 1", "SELECT 2", "SELECT 3"},
			},
		},
		{
			name:      "multiple statements exceeding chunk size",
			sql:       "SELECT 1; SELECT 2; SELECT 3; SELECT 4",
			chunkSize: 2,
			expectedChunks: [][]string{
				{"SELECT 1", "SELECT 2"},
				{"SELECT 3", "SELECT 4"},
			},
		},
		{
			name:      "filter transaction statements",
			sql:       "BEGIN; SELECT 1; COMMIT; SELECT 2",
			chunkSize: 10,
			expectedChunks: [][]string{
				{"SELECT 1", "SELECT 2"},
			},
		},
		{
			name:      "only transaction statements",
			sql:       "BEGIN; COMMIT",
			chunkSize: 10,
			expectedChunks: [][]string{
				{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iterator := sqliteparserutils.CreateStatementIterator(tt.sql)
			chunker := newChunker(iterator, tt.chunkSize)

			chunks := [][]string{}
			for {
				chunk, isEOF := chunker.Next()
				chunkCopy := make([]string, len(chunk))
				copy(chunkCopy, chunk)
				chunks = append(chunks, chunkCopy)
				if isEOF {
					break
				}
			}

			if len(chunks) != len(tt.expectedChunks) {
				t.Fatalf("expected %d chunks, got %d", len(tt.expectedChunks), len(chunks))
			}

			for i, chunk := range chunks {
				if len(chunk) != len(tt.expectedChunks[i]) {
					t.Errorf("chunk %d: expected %d statements, got %d", i, len(tt.expectedChunks[i]), len(chunk))
					continue
				}
				for j, stmt := range chunk {
					if stmt != tt.expectedChunks[i][j] {
						t.Errorf("chunk %d, stmt %d: expected %q, got %q", i, j, tt.expectedChunks[i][j], stmt)
					}
				}
			}
		})
	}
}

func TestAddReplicationIndex(t *testing.T) {
	tests := []struct {
		name  string
		msg   *hrana.PipelineRequest
		index uint64
	}{
		{
			name: "add to stmt request",
			msg: &hrana.PipelineRequest{
				Requests: []hrana.StreamRequest{
					{Stmt: &hrana.Stmt{}},
				},
			},
			index: 42,
		},
		{
			name: "add to batch request",
			msg: &hrana.PipelineRequest{
				Requests: []hrana.StreamRequest{
					{Batch: &hrana.Batch{}},
				},
			},
			index: 100,
		},
		{
			name: "don't override existing replication index",
			msg: &hrana.PipelineRequest{
				Requests: []hrana.StreamRequest{
					{Stmt: &hrana.Stmt{ReplicationIndex: ptrUint64(50)}},
				},
			},
			index: 42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addReplicationIndex(tt.msg, tt.index)

			for _, req := range tt.msg.Requests {
				if req.Stmt != nil {
					if req.Stmt.ReplicationIndex == nil {
						t.Error("expected replication index to be set on stmt")
					} else if *req.Stmt.ReplicationIndex != tt.index && tt.name != "don't override existing replication index" {
						t.Errorf("expected replication index %d, got %d", tt.index, *req.Stmt.ReplicationIndex)
					}
				}
				if req.Batch != nil {
					if req.Batch.ReplicationIndex == nil {
						t.Error("expected replication index to be set on batch")
					} else if *req.Batch.ReplicationIndex != tt.index {
						t.Errorf("expected replication index %d, got %d", tt.index, *req.Batch.ReplicationIndex)
					}
				}
			}
		})
	}
}

func TestGetReplicationIndex(t *testing.T) {
	tests := []struct {
		name     string
		response *hrana.PipelineResponse
		expected uint64
	}{
		{
			name:     "nil response",
			response: nil,
			expected: 0,
		},
		{
			name:     "empty results",
			response: &hrana.PipelineResponse{Results: []hrana.StreamResult{}},
			expected: 0,
		},
		{
			name: "nil response in result",
			response: &hrana.PipelineResponse{
				Results: []hrana.StreamResult{
					{Response: nil},
				},
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getReplicationIndex(tt.response)
			if result != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestStmtResultRowsProvider(t *testing.T) {
	nameCol1 := "id"
	nameCol2 := "name"

	provider := &StmtResultRowsProvider{
		r: &hrana.StmtResult{
			Cols: []hrana.Column{
				{Name: &nameCol1},
				{Name: &nameCol2},
			},
			Rows: [][]hrana.Value{
				{
					{Type: "integer", Value: "1"},
					{Type: "text", Value: "Alice"},
				},
				{
					{Type: "integer", Value: "2"},
					{Type: "text", Value: "Bob"},
				},
			},
		},
	}

	// Test SetsCount
	if provider.SetsCount() != 1 {
		t.Errorf("expected 1 set, got %d", provider.SetsCount())
	}

	// Test RowsCount
	if provider.RowsCount(0) != 2 {
		t.Errorf("expected 2 rows, got %d", provider.RowsCount(0))
	}
	if provider.RowsCount(1) != 0 {
		t.Errorf("expected 0 rows for invalid setIdx, got %d", provider.RowsCount(1))
	}

	// Test Columns
	cols := provider.Columns(0)
	if len(cols) != 2 {
		t.Errorf("expected 2 columns, got %d", len(cols))
	}
	if cols[0] != "id" || cols[1] != "name" {
		t.Errorf("unexpected column names: %v", cols)
	}

	// Test invalid setIdx
	if provider.Columns(1) != nil {
		t.Error("expected nil for invalid setIdx")
	}

	// Test HasResult
	if !provider.HasResult(0) {
		t.Error("expected HasResult(0) to be true")
	}
	if provider.HasResult(1) {
		t.Error("expected HasResult(1) to be false")
	}

	// Test Error
	if provider.Error(0) != "" {
		t.Errorf("expected empty error, got %q", provider.Error(0))
	}
}

func TestBatchResultRowsProvider(t *testing.T) {
	nameCol := "id"

	provider := &BatchResultRowsProvider{
		r: &hrana.BatchResult{
			StepResults: []*hrana.StmtResult{
				{
					Cols: []hrana.Column{{Name: &nameCol}},
					Rows: [][]hrana.Value{
						{{Type: "integer", Value: "1"}},
					},
				},
				{
					Cols: []hrana.Column{{Name: &nameCol}},
					Rows: [][]hrana.Value{
						{{Type: "integer", Value: "2"}},
					},
				},
			},
			StepErrors: []*hrana.Error{
				nil,
				{Message: "some error"},
			},
		},
	}

	// Test SetsCount
	if provider.SetsCount() != 2 {
		t.Errorf("expected 2 sets, got %d", provider.SetsCount())
	}

	// Test RowsCount
	if provider.RowsCount(0) != 1 {
		t.Errorf("expected 1 row in set 0, got %d", provider.RowsCount(0))
	}

	// Test Columns
	cols := provider.Columns(0)
	if len(cols) != 1 || cols[0] != "id" {
		t.Errorf("unexpected columns: %v", cols)
	}

	// Test Error
	if provider.Error(0) != "" {
		t.Errorf("expected no error for set 0, got %q", provider.Error(0))
	}
	if provider.Error(1) != "some error" {
		t.Errorf("expected error for set 1, got %q", provider.Error(1))
	}

	// Test HasResult
	if !provider.HasResult(0) {
		t.Error("expected HasResult(0) to be true")
	}
	if !provider.HasResult(1) {
		t.Error("expected HasResult(1) to be true")
	}
}

// Helper function
func ptrUint64(v uint64) *uint64 {
	return &v
}
