package synccompactor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
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

	bigBase := mk("base.c1z", 4000)
	smallPartial := mk("p-small.c1z", 100)
	bigPartial := mk("p-big.c1z", 2000)
	tinyBase := mk("tiny.c1z", 100)
	tinyPartial := mk("p-tiny.c1z", 10)

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "")
	// Base + partials within the 10% ratio is fold's winning shape →
	// auto-selects fold. No base-size floor by default.
	require.Equal(t, PebbleCompactorModeFold, newC(bigBase, smallPartial).resolvePebbleMode(ctx))
	// Exactly at the 10% boundary → still fold.
	require.Equal(t, PebbleCompactorModeFold,
		newC(bigBase, smallPartial, smallPartial, smallPartial, smallPartial).resolvePebbleMode(ctx))
	require.Equal(t, PebbleCompactorModeFold, newC(tinyBase, tinyPartial).resolvePebbleMode(ctx))
	// Partials past the ratio fall outside the gate → overlay.
	require.Equal(t, PebbleCompactorModeOverlay,
		newC(bigBase, smallPartial, smallPartial, smallPartial, smallPartial, smallPartial).resolvePebbleMode(ctx))
	require.Equal(t, PebbleCompactorModeOverlay, newC(bigBase, bigPartial).resolvePebbleMode(ctx))
	require.Equal(t, PebbleCompactorModeOverlay, newC(tinyBase, smallPartial).resolvePebbleMode(ctx))
	// Unstat-able base counts as zero → overlay; the open fails later
	// with the real error.
	require.Equal(t, PebbleCompactorModeOverlay,
		newC(filepath.Join(dir, "missing.c1z"), smallPartial).resolvePebbleMode(ctx))
	// An explicit base-size floor (ops escape hatch) still declines
	// fold for bases below it.
	t.Setenv("BATON_PEBBLE_FOLD_MIN_BASE_BYTES", "1000")
	require.Equal(t, PebbleCompactorModeOverlay, newC(tinyBase, tinyPartial).resolvePebbleMode(ctx))
	require.Equal(t, PebbleCompactorModeFold, newC(bigBase, smallPartial).resolvePebbleMode(ctx))
	t.Setenv("BATON_PEBBLE_FOLD_MIN_BASE_BYTES", "")

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
	require.Equal(t, PebbleCompactorModeOverlay, newC(bigBase, bigPartial, smallPartial).resolvePebbleMode(ctx))
}

// TestResolvePebbleModeFoldWasteCutover exercises the waste leg of the
// auto gate: a base whose manifest carries accumulated fold dead bytes
// past the cutover (default 15% of live payload bytes) forces an
// overlay rebuild even when the size gate would pick fold. The base
// envelopes are written with exactly controlled raw payload size and
// fold_dead_bytes, so the ratio arithmetic is deterministic.
func TestResolvePebbleModeFoldWasteCutover(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	mkEnv := func(name string, payloadBytes int, dead int64, enc c1zv3.PayloadEncoding) string {
		t.Helper()
		payloadDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(payloadDir, "payload.bin"), make([]byte, payloadBytes), 0o600))
		m := c1zv3.C1ZManifestV3_builder{
			Engine:          "pebble",
			PayloadEncoding: enc,
			FoldDeadBytes:   dead,
		}.Build()
		path := filepath.Join(dir, name)
		f, err := os.Create(path)
		require.NoError(t, err)
		defer f.Close()
		_, err = formatv3.WriteEnvelopeWithReuse(f, m, payloadDir, nil)
		require.NoError(t, err)
		return path
	}
	newC := func(base string, opts ...Option) *Compactor {
		// A 1-byte partial keeps the size gate firmly inside fold's
		// shape, so any overlay result comes from the waste leg.
		partial := filepath.Join(dir, "partial.c1z")
		require.NoError(t, os.WriteFile(partial, []byte{0}, 0o600))
		c := &Compactor{entries: []*CompactableSync{
			{FilePath: base, SyncID: "base"},
			{FilePath: partial, SyncID: "p"},
		}}
		for _, opt := range opts {
			opt(c)
		}
		return c
	}
	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "")

	const indexed = c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD
	noWaste := mkEnv("nowaste.c1z", 10000, 0, indexed)
	lowWaste := mkEnv("low.c1z", 10000, 1000, indexed)   // 1000/9000 ≈ 11% ≤ 15% → fold
	highWaste := mkEnv("high.c1z", 10000, 2000, indexed) // 2000/8000 = 25% > 15% → overlay

	require.Equal(t, PebbleCompactorModeFold, newC(noWaste).resolvePebbleMode(ctx))
	require.Equal(t, PebbleCompactorModeFold, newC(lowWaste).resolvePebbleMode(ctx))
	require.Equal(t, PebbleCompactorModeOverlay, newC(highWaste).resolvePebbleMode(ctx))

	// The option moves the cutover in both directions, and a negative
	// value disables the waste leg entirely.
	require.Equal(t, PebbleCompactorModeFold, newC(highWaste, WithFoldMaxWastePercent(30)).resolvePebbleMode(ctx))
	require.Equal(t, PebbleCompactorModeOverlay, newC(lowWaste, WithFoldMaxWastePercent(10)).resolvePebbleMode(ctx))
	require.Equal(t, PebbleCompactorModeFold, newC(highWaste, WithFoldMaxWastePercent(-1)).resolvePebbleMode(ctx))

	// Recorded waste but no indexed trailer (tar payload): live bytes
	// can't be computed cheaply, so the gate errs toward the rebuild.
	tarWaste := mkEnv("tar.c1z", 10000, 100, c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR)
	require.Equal(t, PebbleCompactorModeOverlay, newC(tarWaste).resolvePebbleMode(ctx))

	// An explicit fold mode bypasses the waste leg like every other
	// auto-gate check.
	require.Equal(t, PebbleCompactorModeFold,
		newC(highWaste, WithPebbleCompactorMode(PebbleCompactorModeFold)).resolvePebbleMode(ctx))
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

	// All-SQLite → SQLite.
	engine, err := (&Compactor{entries: []*CompactableSync{
		{FilePath: v1, SyncID: "s1"},
		{FilePath: v1, SyncID: "s2"},
	}}).inferEngineFromInputs()
	require.NoError(t, err)
	require.Equal(t, dotc1z.EngineSQLite, engine)

	// All-Pebble → Pebble.
	engine, err = (&Compactor{entries: []*CompactableSync{
		{FilePath: v3, SyncID: "s1"},
		{FilePath: v3, SyncID: "s2"},
	}}).inferEngineFromInputs()
	require.NoError(t, err)
	require.Equal(t, dotc1z.EnginePebble, engine)

	// Mixed SQLite/Pebble → Pebble (any Pebble wins).
	engine, err = (&Compactor{entries: []*CompactableSync{
		{FilePath: v1, SyncID: "s1"},
		{FilePath: v3, SyncID: "s2"},
	}}).inferEngineFromInputs()
	require.NoError(t, err, "mixed inputs must no longer return an error; any Pebble input wins")
	require.Equal(t, dotc1z.EnginePebble, engine, "mixed input must produce Pebble output")

	// Explicit Pebble + all-Pebble → Pebble.
	engine, err = (&Compactor{
		engine: dotc1z.EnginePebble,
		entries: []*CompactableSync{
			{FilePath: v3, SyncID: "s1"},
			{FilePath: v3, SyncID: "s2"},
		},
	}).inferEngineFromInputs()
	require.NoError(t, err)
	require.Equal(t, dotc1z.EnginePebble, engine)

	// Explicit SQLite + Pebble input → ErrEnginePolicyConflict.
	_, err = (&Compactor{
		engine: dotc1z.EngineSQLite,
		entries: []*CompactableSync{
			{FilePath: v3, SyncID: "s1"},
			{FilePath: v3, SyncID: "s2"},
		},
	}).inferEngineFromInputs()
	require.ErrorIs(t, err, ErrEnginePolicyConflict,
		"explicit SQLite with Pebble input must return ErrEnginePolicyConflict")

	// Explicit Pebble + SQLite inputs → Pebble (SQLite inputs are converted).
	engine, err = (&Compactor{
		engine: dotc1z.EnginePebble,
		entries: []*CompactableSync{
			{FilePath: v1, SyncID: "s1"},
			{FilePath: v1, SyncID: "s2"},
		},
	}).inferEngineFromInputs()
	require.NoError(t, err)
	require.Equal(t, dotc1z.EnginePebble, engine, "explicit Pebble with SQLite inputs is valid")
}
