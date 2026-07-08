package dotc1z

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
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

	store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")))
	require.NoError(t, store.EndSync(ctx))

	// Make the envelope write fail AFTER the checkpoint succeeds: a
	// non-empty DIRECTORY squatting on the staging path makes save()'s
	// OpenFile(retry.c1z.tmp, O_CREATE) fail on every platform, and its
	// own failure-path cleanup (os.Remove) can't delete it either.
	// (A read-only output dir — chmod 0555 — is NOT a portable injection:
	// Windows' directory read-only bit doesn't block file creation.)
	tmpPath := path + ".tmp"
	require.NoError(t, os.MkdirAll(filepath.Join(tmpPath, "blocker"), 0o755))
	restored := false
	restore := func() {
		if !restored {
			restored = true
			require.NoError(t, os.RemoveAll(tmpPath))
		}
	}
	defer restore()

	err = store.Close(ctx)
	require.Error(t, err, "Close with a blocked staging path must fail")

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
