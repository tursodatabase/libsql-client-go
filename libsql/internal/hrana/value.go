package hrana

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Value struct {
	Type   string  `json:"type"`
	Value  any     `json:"value,omitempty"`
	Base64 *string `json:"base64,omitempty"`
}

func (v Value) ToValue(columnType *string) any {
	if v.Type == "blob" {
		if v.Base64 == nil {
			return nil
		}
		bytes, err := base64.StdEncoding.WithPadding(base64.NoPadding).DecodeString(*v.Base64)
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
	} else if columnType != nil {
		if (strings.ToLower(*columnType) == "timestamp" || strings.ToLower(*columnType) == "datetime") && v.Type == "text" {
			for _, format := range []string{
				"2006-01-02 15:04:05.999999999-07:00",
				"2006-01-02T15:04:05.999999999-07:00",
				"2006-01-02 15:04:05.999999999",
				"2006-01-02T15:04:05.999999999",
				"2006-01-02 15:04:05",
				"2006-01-02T15:04:05",
				"2006-01-02 15:04",
				"2006-01-02T15:04",
				"2006-01-02",
			} {
				if t, err := time.ParseInLocation(format, v.Value.(string), time.UTC); err == nil {
					return t
				}
			}
		}
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
		b64 := base64.StdEncoding.WithPadding(base64.NoPadding).EncodeToString(blob)
		res.Base64 = &b64
	} else if float, ok := v.(float64); ok {
		res.Type = "float"
		res.Value = float
	} else if t, ok := v.(time.Time); ok {
		res.Type = "text"
		res.Value = t.Format("2006-01-02 15:04:05.999999999-07:00")
	} else if t, ok := v.(bool); ok {
		res.Type = "integer"
		res.Value = "0"
		if t {
			res.Value = "1"
		}
	} else {
		return res, fmt.Errorf("unsupported value type: %s", v)
	}
	return res, nil
}
