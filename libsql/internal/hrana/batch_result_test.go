package hrana

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestBatchResult_UnmarshalJSON(t *testing.T) {
	testCases := []struct {
		name     string
		jsonData []byte
		expected *uint64
	}{
		{
			jsonData: []byte(`{"replication_index":1}`),
			expected: uint64Ptr(1),
		},
		{
			jsonData: []byte(`{"replication_index":"1"}`),
			expected: uint64Ptr(1),
		},
		{
			jsonData: []byte(`{"replication_index":""}`),
			expected: nil,
		},
		{
			jsonData: []byte(`{}`),
			expected: nil,
		},
		{
			jsonData: []byte(`{"replication_index":"0"}`),
			expected: uint64Ptr(0),
		},
		{
			jsonData: []byte(`{"replication_index":0}`),
			expected: uint64Ptr(0),
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			batchResult := &BatchResult{}
			err := json.Unmarshal(tc.jsonData, batchResult)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !reflect.DeepEqual(batchResult.ReplicationIndex, tc.expected) {
				t.Errorf("ReplicationIndex field is not correctly unmarshaled got = %v, want = %v", batchResult.ReplicationIndex, tc.expected)
			}
		})
	}
}

func uint64Ptr(n uint64) *uint64 {
	return &n
}
