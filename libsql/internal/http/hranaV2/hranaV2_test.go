package hranaV2_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/tursodatabase/libsql-client-go/libsql"
)

type pipelineCall struct {
	baton string
	types string
}

// dropSQL makes the fake server drop the connection without answering, like a network failure.
const dropSQL = "SELECT 'drop'"

// fakeHranaServer answers /v2/pipeline like a server that hands out one baton per stream.
// Batons in lost are answered with lostStatus and lostBody, the way a server answers a stream it no longer has.
type fakeHranaServer struct {
	mu         sync.Mutex
	calls      []pipelineCall
	streams    int
	lost       map[string]bool
	lostStatus int
	lostBody   string
}

func (s *fakeHranaServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var req struct {
		Baton    string `json:"baton"`
		Requests []struct {
			Type string `json:"type"`
			Stmt *struct {
				Sql string `json:"sql"`
			} `json:"stmt"`
		} `json:"requests"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	call := pipelineCall{baton: req.Baton}
	closing := false
	results := []any{}
	for _, q := range req.Requests {
		if q.Stmt != nil && q.Stmt.Sql == dropSQL {
			s.calls = append(s.calls, pipelineCall{baton: req.Baton, types: "dropped "})
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close()
			return
		}
		call.types += q.Type + " "
		if q.Type == "close" {
			closing = true
			results = append(results, map[string]any{"type": "ok", "response": map[string]any{"type": "close"}})
			continue
		}
		results = append(results, map[string]any{"type": "ok", "response": map[string]any{
			"type":   "execute",
			"result": map[string]any{"cols": []any{}, "rows": []any{}, "affected_row_count": 0},
		}})
	}
	s.calls = append(s.calls, call)
	if s.lost[req.Baton] {
		w.WriteHeader(s.lostStatus)
		_, _ = io.WriteString(w, s.lostBody)
		return
	}
	baton := req.Baton
	if baton == "" {
		s.streams++
		baton = fmt.Sprintf("stream-%d", s.streams)
	}
	if closing {
		baton = ""
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"baton": baton, "results": results})
}

func (s *fakeHranaServer) snapshot() []pipelineCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]pipelineCall(nil), s.calls...)
}

func openFake(t *testing.T) (*fakeHranaServer, *sql.DB) {
	t.Helper()
	fake := &fakeHranaServer{lost: map[string]bool{}}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	connector, err := libsql.NewConnector(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return fake, db
}

func splitCloses(calls []pipelineCall) (executes, closes []pipelineCall) {
	for _, c := range calls {
		if c.types == "close " {
			closes = append(closes, c)
		} else {
			executes = append(executes, c)
		}
	}
	return executes, closes
}

func query(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

// Closes run in a goroutine, so give them time to land before reading the calls.
func settle() { time.Sleep(100 * time.Millisecond) }

func TestPooledConnectionReusesStreamAcrossQueries(t *testing.T) {
	fake, db := openFake(t)
	for i := 0; i < 10; i++ {
		query(t, db, "SELECT 1")
	}
	settle()

	calls := fake.snapshot()
	if len(calls) != 10 {
		t.Fatalf("10 queries sent %d requests, want 10: %+v", len(calls), calls)
	}
	for i, c := range calls {
		wantBaton := "stream-1"
		if i == 0 {
			wantBaton = ""
		}
		if c.baton != wantBaton || c.types != "execute " {
			t.Fatalf("request %d = %+v, want execute on baton %q", i, c, wantBaton)
		}
	}
}

func TestStreamThatRanATransactionIsClosedBeforeReuse(t *testing.T) {
	fake, db := openFake(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	query(t, db, "SELECT 1")
	query(t, db, "SELECT 1")
	settle()

	got := fmt.Sprint(splitCloses(fake.snapshot()))
	want := "[{ execute } {stream-1 execute } {stream-1 execute } { execute } {stream-2 execute }] [{stream-1 close }]"
	if got != want {
		t.Fatalf("requests = %s, want %s", got, want)
	}
}

func TestStreamThatChangedSessionSettingsIsClosedBeforeReuse(t *testing.T) {
	fake, db := openFake(t)
	if _, err := db.Exec("PRAGMA foreign_keys = OFF"); err != nil {
		t.Fatal(err)
	}
	query(t, db, "SELECT 1")
	settle()

	got := fmt.Sprint(splitCloses(fake.snapshot()))
	want := "[{ execute } { execute }] [{stream-1 close }]"
	if got != want {
		t.Fatalf("requests = %s, want %s", got, want)
	}
}

func TestStreamOfAFailedRequestIsClosedBeforeReuse(t *testing.T) {
	fake, db := openFake(t)
	query(t, db, "SELECT 1")
	rows, err := db.Query(dropSQL)
	if err == nil {
		defer rows.Close()
		t.Fatal("query on a dropped connection succeeded")
	}
	query(t, db, "SELECT 1")
	settle()

	got := fmt.Sprint(splitCloses(fake.snapshot()))
	want := "[{ execute } {stream-1 dropped } { execute }] [{stream-1 close }]"
	if got != want {
		t.Fatalf("requests = %s, want %s", got, want)
	}
}

func TestLostStreamRetriesOnANewStream(t *testing.T) {
	for _, tc := range []struct {
		server string
		status int
		body   string
	}{
		{"404 stream not found", http.StatusNotFound, `{"error":"stream not found: stream-1"}`},
		{"400 STREAM_EXPIRED", http.StatusBadRequest, `{"message":"The stream has expired due to inactivity","code":"STREAM_EXPIRED"}`},
	} {
		t.Run(tc.server, func(t *testing.T) {
			fake, db := openFake(t)
			query(t, db, "SELECT 1")
			fake.mu.Lock()
			fake.lost["stream-1"] = true
			fake.lostStatus, fake.lostBody = tc.status, tc.body
			fake.mu.Unlock()

			query(t, db, "SELECT 1")
			settle()

			got := fmt.Sprint(fake.snapshot())
			want := "[{ execute } {stream-1 execute } { execute }]"
			if got != want {
				t.Fatalf("requests = %s, want %s", got, want)
			}
		})
	}
}

func TestNotFoundForAnotherReasonIsNotRetried(t *testing.T) {
	fake, db := openFake(t)
	query(t, db, "SELECT 1")
	fake.mu.Lock()
	fake.lost["stream-1"] = true
	fake.lostStatus, fake.lostBody = http.StatusNotFound, `{"error":"database not found"}`
	fake.mu.Unlock()

	rows, err := db.Query("SELECT 1")
	if err == nil {
		defer rows.Close()
		t.Fatal("query on a missing database succeeded")
	}
	settle()

	got := fmt.Sprint(fake.snapshot())
	want := "[{ execute } {stream-1 execute }]"
	if got != want {
		t.Fatalf("requests = %s, want %s", got, want)
	}
}
