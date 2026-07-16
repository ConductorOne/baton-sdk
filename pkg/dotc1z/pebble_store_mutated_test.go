package dotc1z

// Engine-derived dirty backstop: the pebbleStore marks its envelope-save
// dirty bit per wrapper method BY CONVENTION, and every engine mutation
// path that misses a wrapper silently skips the save on Close — the
// clear-only replay pre-clear was one such path, and the class was only
// fixed instance-by-instance. Engine.Mutated latches when any write
// closure runs, and Close saves on EITHER signal, so the class is closed
// structurally: bypassing a wrapper can cost a spurious save, never a
// lost one.

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	pebbleengine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

func TestPebbleCloseSavesOnEngineMutationBypassingWrappers(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "mutated.c1z")

	// A finished sync, saved and closed: the reopened handle starts with
	// the wrapper dirty bit clear.
	store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble), WithTmpDir(tmpDir))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	// Reopen writable and mutate through the ENGINE directly — the
	// wrapper-bypass shape (any future engine method without a
	// dirty-marking wrapper takes exactly this path).
	store, err = NewStore(ctx, path, WithEngine(c1zstore.EnginePebble), WithTmpDir(tmpDir))
	require.NoError(t, err)
	require.NoError(t, store.SetCurrentSync(ctx, syncID))
	holder, ok := any(store).(interface{ PebbleEngine() *pebbleengine.Engine })
	require.True(t, ok)
	require.NoError(t, holder.PebbleEngine().PutSourceCacheEntry(ctx, "grants", "scope-x", "etag-x"))
	require.NoError(t, store.Close(ctx))

	// The mutation must have survived Close: without the engine-derived
	// backstop the wrapper dirty bit was never set, the envelope save was
	// skipped, and the write existed only in the discarded temp dir.
	reopened, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble), WithReadOnly(true), WithTmpDir(tmpDir))
	require.NoError(t, err)
	defer func() { _ = reopened.Close(ctx) }()
	require.NoError(t, reopened.SetCurrentSync(ctx, syncID))
	sc, ok := reopened.(SourceCacheStore)
	require.True(t, ok)
	entry, found, err := sc.LookupSourceCacheEntry(ctx, sourcecache.RowKindGrants, "scope-x")
	require.NoError(t, err)
	require.True(t, found,
		"an engine mutation that bypassed the store wrappers must still force the envelope save on Close")
	require.Equal(t, "etag-x", entry.CacheValidator)
}
