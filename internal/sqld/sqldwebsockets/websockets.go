package sqldwebsockets

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
	"vitess.io/vitess/go/pools"
)

func errorMsg(errorResp interface{}) string {
	return errorResp.(map[string]interface{})["error"].(map[string]interface{})["message"].(string)
}

func isErrorResp(resp interface{}) bool {
	return resp.(map[string]interface{})["type"] == "response_error"
}

type websocketConn struct {
	conn   *websocket.Conn
	idPool *pools.IDPool
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
		res["value"] = base64.StdEncoding.EncodeToString(blob)
	} else if float, ok := v.(float64); ok {
		res["type"] = "float"
		res["value"] = float
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
		base64Encoded := val["value"].(string)
		v, err := base64.StdEncoding.DecodeString(base64Encoded)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "float":
		return val["value"].(float64), nil
	}
	return nil, fmt.Errorf("unrecognized value type: %s", val["type"])
}

func (ws *websocketConn) exec(sql string, sqlParams params, wantRows bool) (*execResponse, error) {
	ctx := context.TODO()
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
		return nil, err
	}

	var resp interface{}
	if err = wsjson.Read(ctx, ws.conn, &resp); err != nil {
		return nil, err
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
	ctx := context.TODO()
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols: []string{"hrana1"},
	})
	if err != nil {
		return nil, err
	}

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
	return &websocketConn{c, pools.NewIDPool(0)}, nil
}
