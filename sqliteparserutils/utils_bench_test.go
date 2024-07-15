package sqliteparserutils

import (
	"runtime"
	"strings"
	"testing"
)

func BenchmarkSplitStatement(b *testing.B) {
	var (
		mem   runtime.MemStats
		count = 8192
	)

	hugeStatement := strings.Repeat("INSERT INTO t VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);", count)
	for i := 0; i < b.N; i++ {
		statements, _ := SplitStatement(hugeStatement)
		if len(statements) != count {
			b.Fail()
		}
		// these intermediate logs can help to estimate memory working set (instead of total allocations)
		runtime.ReadMemStats(&mem)
		b.Logf("heap in use: %v", mem.HeapInuse)
	}
}
