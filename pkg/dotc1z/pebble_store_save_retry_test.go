package dotc1z

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestPebbleCloseRetryAfterFailedSave pins the recovery path Close
// advertises: a save failure leaves the store open with the unpacked data
// preserved, and a second Close after the operator fixes the condition must
// SUCCEED. The failure mode this guards: the first save's CheckpointTo
// leaves tmpDir/checkpoint behind, and pebble's Checkpoint refuses an
// existing destination — without clearing the stale dir, every retry would
// fail with ErrExist forever, deadlocking the advertised recovery.
func TestPebbleCloseRetryAfterFailedSave(t *testing.T) {
	ctx := context.Background()
	outDir := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.MkdirAll(outDir, 0o755))
	path := filepath.Join(outDir, "retry.c1z")

	store, err := NewStore(ctx, path, WithEngine(EnginePebble))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")))
	require.NoError(t, store.EndSync(ctx))

	// Make the envelope write fail AFTER the checkpoint succeeds: the
	// output dir refuses new files, so save() checkpoints into tmpDir and
	// then dies opening retry.c1z.tmp.
	require.NoError(t, os.Chmod(outDir, 0o555))
	restored := false
	restore := func() {
		if !restored {
			restored = true
			require.NoError(t, os.Chmod(outDir, 0o755))
		}
	}
	defer restore()

	err = store.Close(ctx)
	require.Error(t, err, "Close with an unwritable output dir must fail")

	// Operator fixes the condition; the retry must succeed despite the
	// stale checkpoint dir the failed save left behind.
	restore()
	require.NoError(t, store.Close(ctx), "Close retry after fixing the save condition")

	// The saved artifact is complete and readable.
	ro, err := NewStore(ctx, path, WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, ro.Close(ctx)) }()
	require.NoError(t, ro.SetCurrentSync(ctx, syncID))
	resp, err := ro.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1, "saved artifact must contain the synced grant")
}
