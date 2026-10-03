package pebble

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cpebble "github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

// A background compaction that hits a corrupt block returns its error to no
// caller; CheckpointTo must still refuse to copy the corrupt table into a new
// artifact.
func TestCheckpointToRefusesAfterCompactionFindsCorruption(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(ctx, dir)
	require.NoError(t, err)
	syncID := ksuid.New().String()
	require.NoError(t, e.bindCurrentSync(syncID))
	for i := range 1000 {
		p := fmt.Sprintf("p%06d", i)
		require.NoError(t, e.PutGrantRecord(ctx, makeGrant(syncID, canonicalTestGrantID("e1", "user", p), "e1", p)))
	}
	require.NoError(t, e.Close())
	flipMiddleByte(t, largestSST(t, dir))

	e, err = Open(ctx, dir)
	require.NoError(t, err)
	defer func() { _ = e.Close() }()
	// An L0 table overlapping the corrupt one, so the compaction merges
	// them and reads every block instead of moving the corrupt table.
	require.NoError(t, e.bindCurrentSync(syncID))
	p := "p000000a"
	require.NoError(t, e.PutGrantRecord(ctx, makeGrant(syncID, canonicalTestGrantID("e1", "user", p), "e1", p)))
	require.NoError(t, e.Flush(ctx))

	err = e.CompactAllRanges(ctx)
	require.True(t, cpebble.IsCorruptionError(err), "premise: the compaction must read the corrupt block, got: %v", err)

	err = e.CheckpointTo(ctx, filepath.Join(t.TempDir(), "checkpoint"))
	require.Error(t, err)
	require.True(t, cpebble.IsCorruptionError(err), "got: %v", err)
}

// flipMiddleByte corrupts a data block of the SST at path without changing
// its size, so pebble's open-time size check still passes.
func flipMiddleByte(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(raw), 4096, "sst too small for an interior flip")
	raw[len(raw)/2] ^= 0xFF
	require.NoError(t, os.WriteFile(path, raw, 0o600)) // #nosec G703 -- fixture sst in the test's temp dir.
}

func largestSST(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	best := ""
	var bestSize int64
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".sst") {
			continue
		}
		info, err := ent.Info()
		require.NoError(t, err)
		if info.Size() > bestSize {
			bestSize = info.Size()
			best = filepath.Join(dir, ent.Name())
		}
	}
	require.NotEmpty(t, best, "no sst files under %s", dir)
	return best
}
