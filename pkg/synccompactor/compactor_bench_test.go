package synccompactor

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// benchGrantIDs returns n distinct grant ids ("g0".."g{n-1}").
func benchGrantIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = "g" + strconv.Itoa(i)
	}
	return ids
}

// benchmarkCompaction folds two same-shape inputs (one full, one partial,
// fully overlapping grant keys so the keep-newer dedup does real work) and
// times Compact end to end. Inputs are built once outside the timed loop;
// each iteration runs a fresh compactor into its own output dir. usePebble
// selects the engine under test so the pebble merge path and the sqlite
// ATTACH path are measured on identical input data.
func benchmarkCompaction(b *testing.B, n int, usePebble bool) {
	ctx := context.Background()
	ids := benchGrantIDs(n)

	src := b.TempDir()
	p1 := filepath.Join(src, "in1.c1z")
	p2 := filepath.Join(src, "in2.c1z")

	var s1, s2 string
	if usePebble {
		s1 = buildPebbleInput(b, ctx, p1, connectorstore.SyncTypeFull, ids...)
		s2 = buildPebbleInput(b, ctx, p2, connectorstore.SyncTypePartial, ids...)
	} else {
		s1 = buildSQLiteInput(b, ctx, p1, connectorstore.SyncTypeFull, ids...)
		s2 = buildSQLiteInput(b, ctx, p2, connectorstore.SyncTypePartial, ids...)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outDir := filepath.Join(b.TempDir(), strconv.Itoa(i))
		require.NoError(b, os.MkdirAll(outDir, 0o755))

		entries := []*CompactableSync{{FilePath: p1, SyncID: s1}, {FilePath: p2, SyncID: s2}}
		opts := []Option{WithTmpDir(b.TempDir())}
		if usePebble {
			opts = append(opts, WithEngine(dotc1z.EnginePebble))
		}
		c, cleanup, err := NewCompactor(ctx, outDir, entries, opts...)
		require.NoError(b, err)
		_, err = c.Compact(ctx)
		require.NoError(b, err)
		require.NoError(b, cleanup())
	}
}

func BenchmarkCompactPebble1k(b *testing.B)  { benchmarkCompaction(b, 1000, true) }
func BenchmarkCompactSQLite1k(b *testing.B)  { benchmarkCompaction(b, 1000, false) }
func BenchmarkCompactPebble10k(b *testing.B) { benchmarkCompaction(b, 10000, true) }
func BenchmarkCompactSQLite10k(b *testing.B) { benchmarkCompaction(b, 10000, false) }
