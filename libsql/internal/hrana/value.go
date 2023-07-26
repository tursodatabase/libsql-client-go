package hrana

import (
	"encoding/base64"
	"fmt"
	"strconv"
)

type Value struct {
	Type   string `json:"type"`
	Value  any    `json:"value,omitempty"`
	Base64 string `json:"base64,omitempty"`
}

func (v Value) ToValue() any {
	if v.Type == "blob" {
		bytes, err := base64.StdEncoding.WithPadding(base64.NoPadding).DecodeString(v.Base64)
		if err != nil {
			return nil
		}
		return bytes
	} else if v.Type == "integer" {
		integer, err := strconv.ParseInt(v.Value.(string), 10, 64)
		if err != nil {
			return nil
		}
		return integer
	}
	return v.Value
}

func ToValue(v any) (Value, error) {
	var res Value
	if v == nil {
		res.Type = "null"
	} else if integer, ok := v.(int64); ok {
		res.Type = "integer"
		res.Value = strconv.FormatInt(integer, 10)
	} else if integer, ok := v.(int); ok {
		res.Type = "integer"
		res.Value = strconv.FormatInt(int64(integer), 10)
	} else if text, ok := v.(string); ok {
		res.Type = "text"
		res.Value = text
	} else if blob, ok := v.([]byte); ok {
		res.Type = "blob"
		res.Base64 = base64.StdEncoding.WithPadding(base64.NoPadding).EncodeToString(blob)
	} else if float, ok := v.(float64); ok {
		res.Type = "float"
		res.Value = float
	} else {
		return res, fmt.Errorf("unsupported value type: %s", v)
	}
	return res, nil
}
