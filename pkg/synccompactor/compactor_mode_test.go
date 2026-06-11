package synccompactor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

// TestResolvePebbleModeCutover exercises the auto-selection gate: the
// large-base + small-partial shape auto-selects fold (now that fold
// mints a fresh sync id), every other shape auto-selects overlay, and
// an explicit BATON_EXPERIMENTAL_PEBBLE_COMPACTOR value forces the mode
// regardless of sizes. The gate reads only file sizes, so plain files
// stand in for c1z inputs.
func TestResolvePebbleModeCutover(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mk := func(name string, size int) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, make([]byte, size), 0o600))
		return path
	}
	newC := func(paths ...string) *Compactor {
		entries := make([]*CompactableSync, 0, len(paths))
		for i, p := range paths {
			entries = append(entries, &CompactableSync{FilePath: p, SyncID: fmt.Sprintf("s%d", i)})
		}
		return &Compactor{entries: entries}
	}

	// Lower the base-size floor so the test doesn't need a 256MiB file.
	t.Setenv("BATON_PEBBLE_FOLD_MIN_BASE_BYTES", "1000")

	bigBase := mk("base.c1z", 4000)
	smallPartial := mk("p-small.c1z", 100)
	bigPartial := mk("p-big.c1z", 2000)
	tinyBase := mk("tiny.c1z", 100)

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "")
	// Large base + partials within the ratio is fold's winning shape →
	// auto-selects fold.
	require.Equal(t, PebbleCompactorModeFold, newC(bigBase, smallPartial).resolvePebbleMode(ctx))
	require.Equal(t, PebbleCompactorModeFold,
		newC(bigBase, smallPartial, smallPartial, smallPartial, smallPartial).resolvePebbleMode(ctx))
	// Partials past the ratio fall outside the gate → overlay.
	require.Equal(t, PebbleCompactorModeOverlay,
		newC(bigBase, smallPartial, smallPartial, smallPartial, smallPartial, smallPartial).resolvePebbleMode(ctx))
	require.Equal(t, PebbleCompactorModeOverlay, newC(bigBase, bigPartial).resolvePebbleMode(ctx))
	require.Equal(t, PebbleCompactorModeOverlay, newC(tinyBase, smallPartial).resolvePebbleMode(ctx))
	// Unstat-able base counts as zero → overlay; the open fails later
	// with the real error.
	require.Equal(t, PebbleCompactorModeOverlay,
		newC(filepath.Join(dir, "missing.c1z"), smallPartial).resolvePebbleMode(ctx))

	// Explicit env values force the mode regardless of sizes — fold
	// stays reachable when requested manually.
	for _, mode := range []PebbleCompactorMode{PebbleCompactorModeKWay, PebbleCompactorModeOverlay, PebbleCompactorModeFold} {
		t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", string(mode))
		require.Equal(t, mode, newC(tinyBase, smallPartial).resolvePebbleMode(ctx))
	}

	// An explicit compactor option also forces fold.
	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "")
	explicit := newC(tinyBase, smallPartial)
	WithPebbleCompactorMode(PebbleCompactorModeFold)(explicit)
	require.Equal(t, PebbleCompactorModeFold, explicit.resolvePebbleMode(ctx))

	// Unrecognized values fall back to auto selection: a non-fold shape
	// (partials too large for the gate) auto-selects overlay.
	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "bogus")
	require.Equal(t, PebbleCompactorModeOverlay, newC(bigBase, bigPartial).resolvePebbleMode(ctx))
}

func TestInferEngineFromInputFormats(t *testing.T) {
	dir := t.TempDir()
	writeHeader := func(name string, header []byte) string {
		t.Helper()
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, header, 0o600))
		return path
	}

	v1 := writeHeader("sqlite.c1z", dotc1z.C1ZFileHeader)
	v3 := writeHeader("pebble.c1z", dotc1z.C1Z3FileHeader)

	engine, err := (&Compactor{entries: []*CompactableSync{
		{FilePath: v1, SyncID: "s1"},
		{FilePath: v1, SyncID: "s2"},
	}}).inferEngineFromInputs()
	require.NoError(t, err)
	require.Equal(t, dotc1z.EngineSQLite, engine)

	engine, err = (&Compactor{entries: []*CompactableSync{
		{FilePath: v3, SyncID: "s1"},
		{FilePath: v3, SyncID: "s2"},
	}}).inferEngineFromInputs()
	require.NoError(t, err)
	require.Equal(t, dotc1z.EnginePebble, engine)

	_, err = (&Compactor{entries: []*CompactableSync{
		{FilePath: v1, SyncID: "s1"},
		{FilePath: v3, SyncID: "s2"},
	}}).inferEngineFromInputs()
	require.Error(t, err)

	engine, err = (&Compactor{
		engine: dotc1z.EngineSQLite,
		entries: []*CompactableSync{
			{FilePath: v3, SyncID: "s1"},
			{FilePath: v3, SyncID: "s2"},
		},
	}).inferEngineFromInputs()
	require.NoError(t, err)
	require.Equal(t, dotc1z.EngineSQLite, engine)
}
