package ws

import (
	"context"
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// defaultWSTimeout specifies the timeout used for initial http connection
var defaultWSTimeout = 120 * time.Second

func errorMsg(errorResp interface{}) string {
	return errorResp.(map[string]interface{})["error"].(map[string]interface{})["message"].(string)
}

func isErrorResp(resp interface{}) bool {
	return resp.(map[string]interface{})["type"] == "response_error"
}

type websocketConn struct {
	conn   *websocket.Conn
	idPool *idPool
}

type namedParam struct {
	Name  string
	Value any
}

type params struct {
	PositinalArgs []any
	NamedArgs     []namedParam
}

func convertValue(v any) (map[string]interface{}, error) {
	res := map[string]interface{}{}
	if v == nil {
		res["type"] = "null"
	} else if integer, ok := v.(int64); ok {
		res["type"] = "integer"
		res["value"] = strconv.FormatInt(integer, 10)
	} else if text, ok := v.(string); ok {
		res["type"] = "text"
		res["value"] = text
	} else if blob, ok := v.([]byte); ok {
		res["type"] = "blob"
		res["base64"] = base64.StdEncoding.WithPadding(base64.NoPadding).EncodeToString(blob)
	} else if float, ok := v.(float64); ok {
		res["type"] = "float"
		res["value"] = float
	} else if boolean, ok := v.(bool); ok {
		res["type"] = "integer"
		if boolean {
			res["value"] = "1"
		} else {
			res["value"] = "0"
		}
	} else {
		return nil, fmt.Errorf("unsupported value type: %s", v)
	}
	return res, nil
}

type execResponse struct {
	resp map[string]interface{}
}

func (r *execResponse) affectedRowCount() int64 {
	return int64(r.resp["affected_row_count"].(float64))
}

func (r *execResponse) lastInsertId() int64 {
	id, ok := r.resp["last_insert_rowid"].(string)
	if !ok {
		return 0
	}
	value, _ := strconv.ParseInt(id, 10, 64)
	return value
}

func (r *execResponse) columns() []string {
	res := []string{}
	cols := r.resp["cols"].([]interface{})
	for idx := range cols {
		var v string = ""
		if cols[idx].(map[string]interface{})["name"] != nil {
			v = cols[idx].(map[string]interface{})["name"].(string)
		}
		res = append(res, v)
	}
	return res
}

func (r *execResponse) rowsCount() int {
	return len(r.resp["rows"].([]interface{}))
}

func (r *execResponse) rowLen(rowIdx int) int {
	return len(r.resp["rows"].([]interface{})[rowIdx].([]interface{}))
}

func (r *execResponse) value(rowIdx int, colIdx int) (any, error) {
	val := r.resp["rows"].([]interface{})[rowIdx].([]interface{})[colIdx].(map[string]interface{})
	switch val["type"] {
	case "null":
		return nil, nil
	case "integer":
		v, err := strconv.ParseInt(val["value"].(string), 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "text":
		return val["value"].(string), nil
	case "blob":
		base64Encoded := val["base64"].(string)
		v, err := base64.StdEncoding.WithPadding(base64.NoPadding).DecodeString(base64Encoded)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "float":
		return val["value"].(float64), nil
	}
	return nil, fmt.Errorf("unrecognized value type: %s", val["type"])
}

func (ws *websocketConn) exec(ctx context.Context, sql string, sqlParams params, wantRows bool) (*execResponse, error) {
	requestId := ws.idPool.Get()
	defer ws.idPool.Put(requestId)
	stmt := map[string]interface{}{
		"sql":       sql,
		"want_rows": wantRows,
	}
	if len(sqlParams.PositinalArgs) > 0 {
		args := []map[string]interface{}{}
		for idx := range sqlParams.PositinalArgs {
			v, err := convertValue(sqlParams.PositinalArgs[idx])
			if err != nil {
				return nil, err
			}
			args = append(args, v)
		}
		stmt["args"] = args
	}
	if len(sqlParams.NamedArgs) > 0 {
		args := []map[string]interface{}{}
		for idx := range sqlParams.NamedArgs {
			v, err := convertValue(sqlParams.NamedArgs[idx].Value)
			if err != nil {
				return nil, err
			}
			arg := map[string]interface{}{
				"name":  sqlParams.NamedArgs[idx].Name,
				"value": v,
			}
			args = append(args, arg)
		}
		stmt["named_args"] = args
	}
	err := wsjson.Write(ctx, ws.conn, map[string]interface{}{
		"type":       "request",
		"request_id": requestId,
		"request": map[string]interface{}{
			"type":      "execute",
			"stream_id": 0,
			"stmt":      stmt,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %s", driver.ErrBadConn, err.Error())
	}

	var resp interface{}
	if err = wsjson.Read(ctx, ws.conn, &resp); err != nil {
		return nil, fmt.Errorf("%w: %s", driver.ErrBadConn, err.Error())
	}

	if isErrorResp(resp) {
		err = fmt.Errorf("unable to execute %s: %s", sql, errorMsg(resp))
		return nil, err
	}

	return &execResponse{resp.(map[string]interface{})["response"].(map[string]interface{})["result"].(map[string]interface{})}, nil
}

func (ws *websocketConn) Close() error {
	return ws.conn.Close(websocket.StatusNormalClosure, "All's good")
}

func connect(url string, jwt string) (*websocketConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultWSTimeout)
	defer cancel()
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols: []string{"hrana1"},
	})
	if err != nil {
		return nil, err
	}

	c.SetReadLimit(1024 * 1024 * 16) // 16MB

	err = wsjson.Write(ctx, c, map[string]interface{}{
		"type": "hello",
		"jwt":  jwt,
	})
	if err != nil {
		c.Close(websocket.StatusInternalError, err.Error())
		return nil, err
	}

	err = wsjson.Write(ctx, c, map[string]interface{}{
		"type":       "request",
		"request_id": 0,
		"request": map[string]interface{}{
			"type":      "open_stream",
			"stream_id": 0,
		},
	})
	if err != nil {
		c.Close(websocket.StatusInternalError, err.Error())
		return nil, err
	}

	var helloResp interface{}
	err = wsjson.Read(ctx, c, &helloResp)
	if err != nil {
		c.Close(websocket.StatusInternalError, err.Error())
		return nil, err
	}
	if helloResp.(map[string]interface{})["type"] == "hello_error" {
		err = fmt.Errorf("handshake error: %s", errorMsg(helloResp))
		c.Close(websocket.StatusProtocolError, err.Error())
		return nil, err
	}

	var openStreamResp interface{}
	err = wsjson.Read(ctx, c, &openStreamResp)
	if err != nil {
		c.Close(websocket.StatusInternalError, err.Error())
		return nil, err
	}

	if isErrorResp(openStreamResp) {
		err = fmt.Errorf("unable to open stream: %s", errorMsg(helloResp))
		c.Close(websocket.StatusProtocolError, err.Error())
		return nil, err
	}
	return &websocketConn{c, newIDPool()}, nil
}

// Below is modified IDPool from "vitess.io/vitess/go/pools"

// idPool is used to ensure that the set of IDs in use concurrently never
// contains any duplicates. The IDs start at 1 and increase without bound, but
// will never be larger than the peak number of concurrent uses.
//
// idPool's Get() and Put() methods can be used concurrently.
type idPool struct {
	sync.Mutex

	// used holds the set of values that have been returned to us with Put().
	used map[uint32]bool
	// maxUsed remembers the largest value we've given out.
	maxUsed uint32
}

// NewIDPool creates and initializes an idPool.
func newIDPool() *idPool {
	return &idPool{
		used:    make(map[uint32]bool),
		maxUsed: 0,
	}
}

// Get returns an ID that is unique among currently active users of this pool.
func (pool *idPool) Get() (id uint32) {
	pool.Lock()
	defer pool.Unlock()

	// Pick a value that's been returned, if any.
	for key := range pool.used {
		delete(pool.used, key)
		return key
	}

	// No recycled IDs are available, so increase the pool size.
	pool.maxUsed++
	return pool.maxUsed
}

// Put recycles an ID back into the pool for others to use. Putting back a value
// or 0, or a value that is not currently "checked out", will result in a panic
// because that should never happen except in the case of a programming error.
func (pool *idPool) Put(id uint32) {
	pool.Lock()
	defer pool.Unlock()

	if id < 1 || id > pool.maxUsed {
		panic(fmt.Errorf("idPool.Put(%v): invalid value, must be in the range [1,%v]", id, pool.maxUsed))
	}

	if pool.used[id] {
		panic(fmt.Errorf("idPool.Put(%v): can't put value that was already recycled", id))
	}

	// If we're recycling maxUsed, just shrink the pool.
	if id == pool.maxUsed {
		pool.maxUsed = id - 1
		return
	}

	// Add it to the set of recycled IDs.
	pool.used[id] = true
}
