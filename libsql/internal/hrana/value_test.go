package hrana

import (
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func toPtr[T any](v T) *T {
	return &v
}

func TestValueToValue(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		value      Value
		want       any
	}{
		{
			name: "null",
			value: Value{
				Type:  "null",
				Value: nil,
			},
			want: nil,
		},
		{
			name: "int",
			value: Value{
				Type:  "integer",
				Value: strconv.FormatInt(int64(42), 10),
			},
			want: int64(42),
		},
		{
			name: "string",
			value: Value{
				Type:  "text",
				Value: "foo",
			},
			want: "foo",
		},
		{
			name: "bytes",
			value: Value{
				Type:   "blob",
				Base64: toPtr("YmFy"),
			},
			want: []byte("bar"),
		},
		{
			name: "bytes",
			value: Value{
				Type:   "blob",
				Base64: toPtr(""),
			},
			want: []byte{},
		},
		{
			name: "float",
			value: Value{
				Type:  "float",
				Value: 3.14,
			},
			want: 3.14,
		},
		{
			name:       "timestamp",
			columnType: "TIMESTAMP",
			value: Value{
				Type:  "text",
				Value: "0001-01-01 01:00:00+00:00",
			},
			want: time.Time{}.Add(time.Hour),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var columnType *string = nil
			if tt.columnType != "" {
				columnType = &tt.columnType
			}
			got := tt.value.ToValue(columnType)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ToValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestToValue(t *testing.T) {
	tests := []struct {
		name    string
		value   any
		want    Value
		wantErr bool
	}{
		{
			name:  "null",
			value: nil,
			want: Value{
				Type: "null",
			},
		},
		{
			name:  "int",
			value: 42,
			want: Value{
				Type:  "integer",
				Value: strconv.FormatInt(int64(42), 10),
			},
		},
		{
			name:  "string",
			value: "foo",
			want: Value{
				Type:  "text",
				Value: "foo",
			},
		},
		{
			name:  "bytes",
			value: []byte{},
			want: Value{
				Type:   "blob",
				Base64: toPtr(""),
			},
		},
		{
			name:  "float",
			value: 3.14,
			want: Value{
				Type:  "float",
				Value: 3.14,
			},
		},
		{
			name:  "boolean",
			value: true,
			want: Value{
				Type:  "integer",
				Value: "1",
			},
		},
		{
			name:  "timestamp",
			value: time.Time{}.Add(time.Hour),
			want: Value{
				Type:  "text",
				Value: "0001-01-01 01:00:00+00:00",
			},
		},
		{
			name:    "unsupported",
			value:   make(chan int),
			want:    Value{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToValue(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ToValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMarshal(t *testing.T) {
	tests := []struct {
		name      string
		value     Value
		marshaled string
	}{
		{
			name: "null",
			value: Value{
				Type: "null",
			},
			marshaled: `{"type":"null"}`,
		},
		{
			name: "int",
			value: Value{
				Type:  "integer",
				Value: strconv.FormatInt(int64(42), 10),
			},
			marshaled: `{"type":"integer","value":"42"}`,
		},
		{
			name: "string",
			value: Value{
				Type:  "text",
				Value: "foo",
			},
			marshaled: `{"type":"text","value":"foo"}`,
		},
		{
			name: "bytes",
			value: Value{
				Type:   "blob",
				Base64: toPtr("YmFy"),
			},
			marshaled: `{"type":"blob","base64":"YmFy"}`,
		},
		{
			name: "float",
			value: Value{
				Type:  "float",
				Value: 3.14,
			},
			marshaled: `{"type":"float","value":3.14}`,
		},
		{
			name: "timestamp",
			value: Value{
				Type:  "text",
				Value: time.Time{}.Add(time.Hour),
			},
			marshaled: `{"type":"text","value":"0001-01-01T01:00:00Z"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.value)
			if err != nil {
				t.Errorf("json.Marshal() error = %v", err)
				return
			}
			if !reflect.DeepEqual(string(got), tt.marshaled) {
				t.Errorf("json.Marshal() = %v, want %v", string(got), tt.marshaled)
			}
		})
	}
}

func TestUnmarshal(t *testing.T) {
	tests := []struct {
		name      string
		value     Value
		marshaled string
	}{
		{
			name: "null",
			value: Value{
				Type: "null",
			},
			marshaled: `{"type":"null"}`,
		},
		{
			name: "int",
			value: Value{
				Type:  "integer",
				Value: strconv.FormatInt(int64(42), 10),
			},
			marshaled: `{"type":"integer","value":"42"}`,
		},
		{
			name: "string",
			value: Value{
				Type:  "text",
				Value: "foo",
			},
			marshaled: `{"type":"text","value":"foo"}`,
		},
		{
			name: "bytes",
			value: Value{
				Type:   "blob",
				Base64: toPtr("YmFy"),
			},
			marshaled: `{"type":"blob","base64":"YmFy"}`,
		},
		{
			name: "float",
			value: Value{
				Type:  "float",
				Value: 3.14,
			},
			marshaled: `{"type":"float","value":3.14}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got Value
			err := json.Unmarshal([]byte(tt.marshaled), &got)
			if err != nil {
				t.Errorf("json.Marshal() error = %v", err)
				return
			}
			if !reflect.DeepEqual(got, tt.value) {
				t.Errorf("json.Unmarshal() = %v, want %v", got, tt.value)
			}
		})
	}
}
