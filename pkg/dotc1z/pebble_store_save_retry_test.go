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

	// Make the publish step of the save fail AFTER the checkpoint succeeds:
	// a non-empty DIRECTORY squatting on the OUTPUT path makes the final
	// rename fail on every platform (rename refuses a non-empty dir), and
	// the staging file itself lives at an unpredictable exclusive name, so
	// squatting the staging path is no longer possible.
	// (A read-only output dir — chmod 0555 — is NOT a portable injection:
	// Windows' directory read-only bit doesn't block file creation.)
	require.NoError(t, os.MkdirAll(filepath.Join(path, "blocker"), 0o755))
	restored := false
	restore := func() {
		if !restored {
			restored = true
			require.NoError(t, os.RemoveAll(path))
		}
	}
	defer restore()

	err = store.Close(ctx)
	require.Error(t, err, "Close with a blocked output path must fail")

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
