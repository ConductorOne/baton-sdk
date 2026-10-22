package pebble

import (
	"runtime"
	"testing"
)

const (
	benchmarkSpillArenaBytes = 128 << 20
	benchmarkSpillEntryBytes = 64 << 10
)

func BenchmarkSpillArenaFirstFill(b *testing.B) {
	for _, size := range []struct {
		name  string
		bytes int
	}{
		{name: "Small", bytes: 1 << 20},
		{name: "ThresholdCrossing", bytes: 8 << 20},
		{name: "Full", bytes: benchmarkSpillArenaBytes - benchmarkSpillEntryBytes},
	} {
		b.Run(size.name, func(b *testing.B) {
			for _, tc := range []struct {
				name string
				free func() *spillArenaFreeList
			}{
				{
					name: "FixedBaseline",
					free: func() *spillArenaFreeList {
						free := newSpillArenaFreeList(benchmarkSpillArenaBytes, 1)
						free.ch <- make([]byte, 0, benchmarkSpillArenaBytes)
						return free
					},
				},
				{
					name: "Promote",
					free: func() *spillArenaFreeList {
						return newSpillArenaFreeList(benchmarkSpillArenaBytes, 1)
					},
				},
			} {
				b.Run(tc.name, func(b *testing.B) {
					key := make([]byte, benchmarkSpillEntryBytes)
					entries := size.bytes / len(key)
					bytesPerFill := int64(entries * len(key))
					b.SetBytes(bytesPerFill)
					b.ReportAllocs()
					b.ResetTimer()

					for b.Loop() {
						free := tc.free()
						s := &spillSorter{
							chunkBytes: benchmarkSpillArenaBytes,
							free:       free,
							arena:      free.get(),
							views:      make([]kvView, 0, entries),
						}
						for range entries {
							if err := s.add(key, nil); err != nil {
								b.Fatal(err)
							}
						}
						runtime.KeepAlive(s)
					}
				})
			}
		})
	}
}
