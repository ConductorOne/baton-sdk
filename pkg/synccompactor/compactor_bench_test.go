package synccompactor

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// benchGrantIDs returns n distinct grant ids ("g0".."g{n-1}").
func benchGrantIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = "g" + strconv.Itoa(i)
	}
	return ids
}

// benchmarkCompaction builds two Pebble inputs with n grants each (with
// fully overlapping grant keys so the keep-newer dedup does real work) and
// times Compact end to end. Inputs are built once outside the timed loop;
// each iteration runs a fresh compactor into its own output dir.
func benchmarkCompaction(b *testing.B, n int) {
	ctx := context.Background()
	ids := benchGrantIDs(n)

	src := b.TempDir()
	p1 := filepath.Join(src, "in1.c1z")
	p2 := filepath.Join(src, "in2.c1z")

	s1 := buildPebbleInput(b, ctx, p1, connectorstore.SyncTypeFull, ids...)
	s2 := buildPebbleInput(b, ctx, p2, connectorstore.SyncTypePartial, ids...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outDir := filepath.Join(b.TempDir(), strconv.Itoa(i))
		require.NoError(b, os.MkdirAll(outDir, 0o755))

		entries := []*CompactableSync{{FilePath: p1, SyncID: s1}, {FilePath: p2, SyncID: s2}}
		c, cleanup, err := NewCompactor(ctx, outDir, entries, WithTmpDir(b.TempDir()))
		require.NoError(b, err)
		_, err = c.Compact(ctx)
		require.NoError(b, err)
		require.NoError(b, cleanup())
	}
}

func BenchmarkCompactPebble1k(b *testing.B)  { benchmarkCompaction(b, 1000) }
func BenchmarkCompactPebble10k(b *testing.B) { benchmarkCompaction(b, 10000) }
