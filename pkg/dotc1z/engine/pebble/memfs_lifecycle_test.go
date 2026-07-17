package pebble

// Behavioral guard for WithVFS coverage: the full engine lifecycle
// runs over a PURE in-memory filesystem (vfs.NewMem — not a wrapped
// vfs.Default). Any engine path that slips back to direct host IO for
// something the DB reads fails here loudly: a file written through
// os lands where the MemFS can't see it ("does not exist" at
// ingest/open), and a file written through the MemFS can't be read
// back through os. This is the dynamic complement of the static
// registry in os_io_allowlist_test.go — the registry catches new call
// sites at review time, this catches the read-back split at run time
// (it is exactly the test that fails on the two bugs found in PR-1
// review: truncateCheckpointWALs os.ReadDir'ing a MemFS checkpoint,
// and the id-index migration os.Rename'ing a MemFS-created SST).
//
// Deliberately NOT covered, matching the engine-FS contract:
// spill-chunk scratch (engine-private OS IO), clone-sync's envelope
// save (host-path IO by contract), and the store layer above the
// engine.

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func TestEngineLifecycleOverPureMemFS(t *testing.T) {
	skipOnWindowsMemFS(t)
	ctx := context.Background()
	w := defaultSweepWorkload()
	memFS := vfs.NewMem()

	// Sync: pages (including the expanded-grant page, which stages
	// deferred-index SSTs at EndSync) + seal.
	e, err := Open(ctx, "memfs-db", WithVFS(memFS))
	require.NoError(t, err)
	a := NewAdapter(e)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, w.write(ctx, a))
	require.True(t, e.deferredIdxPending.Load(),
		"the workload must arm the deferred rebuild so EndSync exercises SST staging over the MemFS")
	require.NoError(t, a.EndSync(ctx))
	w.verifyComplete(ctx, t, e, syncID, "sealed engine over MemFS")

	// Writable checkpoint: cut it through the engine FS and reopen it
	// as its own engine over the same MemFS. Covers db.Checkpoint +
	// truncateCheckpointWALs + a fresh Open, all FS-side.
	require.NoError(t, e.CheckpointTo(ctx, "memfs-checkpoint"))
	ce, err := Open(ctx, "memfs-checkpoint", WithVFS(memFS))
	require.NoError(t, err, "the checkpoint must be complete and openable on the engine FS")
	w.verifyComplete(ctx, t, ce, syncID, "checkpoint reopened over MemFS")
	require.NoError(t, ce.Close())
	require.NoError(t, e.Close())

	// Reopen the primary after a clean close — open-time migrations and
	// marker probes run FS-side too.
	e2, err := Open(ctx, "memfs-db", WithVFS(memFS))
	require.NoError(t, err)
	defer func() { _ = e2.Close() }()
	w.verifyComplete(ctx, t, e2, syncID, "primary reopened over MemFS")
}
