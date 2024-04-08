package ws

import (
	"fmt"
	"reflect"
	"testing"
)

func TestConvertValue(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  map[string]any
		err   error
	}{
		{
			name:  "nil",
			value: nil,
			want: map[string]any{
				"type": "null",
			},
			err: nil,
		},
		{
			name:  "integer",
			value: int64(42),
			want: map[string]any{
				"type":  "integer",
				"value": "42",
			},
			err: nil,
		},
		{
			name:  "text",
			value: "turso for win",
			want: map[string]any{
				"type":  "text",
				"value": "turso for win",
			},
			err: nil,
		},
		{
			name:  "blob",
			value: []byte("hello world"),
			want: map[string]any{
				"type": "blob",
				// `hello world` encoded is `aGVsbG8gd29ybGQ=` but we want without padding
				"base64": "aGVsbG8gd29ybGQ",
			},
			err: nil,
		},
		{
			name:  "float",
			value: 3.14,
			want: map[string]any{
				"type":  "float",
				"value": 3.14,
			},
			err: nil,
		},
		{
			name:  "boolean_true",
			value: true,
			want: map[string]any{
				"type":  "integer",
				"value": "1",
			},
			err: nil,
		},
		{
			name:  "boolean_false",
			value: false,
			want: map[string]any{
				"type":  "integer",
				"value": "0",
			},
			err: nil,
		},
		{
			name:  "unsupported",
			value: struct{}{},
			want:  nil,
			err:   fmt.Errorf("unsupported value type: %s", struct{}{}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(tt.value)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(err, tt.err) {
				t.Errorf("got error %v, want %v", err, tt.err)
			}
		})
	}
}

func Test_execResponse_lastInsertId(t *testing.T) {
	tests := []struct {
		name  string
		value map[string]interface{}
		want  int64
	}{
		{
			name:  "valid",
			value: map[string]interface{}{"last_insert_rowid": "42"},
			want:  42,
		},
		{
			name:  "empty",
			value: map[string]interface{}{},
			want:  0,
		},
		{
			name:  "invalid",
			value: map[string]interface{}{"last_insert_rowid": "invalid"},
			want:  0,
		},
		{
			name:  "invalid_type",
			value: map[string]interface{}{"last_insert_rowid": 42.0},
			want:  0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &execResponse{
				resp: tt.value,
			}
			if got := r.lastInsertId(); got != tt.want {
				t.Errorf("lastInsertId() = %v, want %v", got, tt.want)
			}
		})
	}
}
