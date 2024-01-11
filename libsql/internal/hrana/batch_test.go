package hrana

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestBatch_MarshalJSON(t *testing.T) {
	testCases := []struct {
		ReplicationIndex *uint64
		expected         []byte
	}{
		{
			ReplicationIndex: uint64Ptr(42),
			expected:         []byte(`{"replication_index":"42","steps":null}`),
		},
		{
			ReplicationIndex: uint64Ptr(0),
			expected:         []byte(`{"replication_index":"0","steps":null}`),
		},
		{
			ReplicationIndex: nil,
			expected:         []byte(`{"steps":null}`),
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			batchResult := &Batch{
				ReplicationIndex: tc.ReplicationIndex,
			}
			result, err := json.Marshal(batchResult)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("JSON output is not correct. got = %s, want = %s", result, tc.expected)
			}
		})
	}
}
