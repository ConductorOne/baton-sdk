package pebble_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// TestSavedC1ZReopensAfterWALTruncation covers the end-to-end close
// contract through the envelope: sync → Close (save) → reopen the
// .c1z writable (the connector-resume path) → data intact → run a
// second sync → close → reopen again. Exercises both the read and
// write reopen paths against a WAL-free checkpoint payload.
//
// Lives in the external test package because it drives the store
// through dotc1z, which imports this engine package.
func TestSavedC1ZReopensAfterWALTruncation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "wal-free.c1z")

	runSync := func(rtID string) string {
		t.Helper()
		w, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
		require.NoError(t, err)
		syncID, err := w.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, w.PutResourceTypes(ctx, v2.ResourceType_builder{Id: rtID}.Build()))
		require.NoError(t, w.EndSync(ctx))
		require.NoError(t, w.Close(ctx))
		return syncID
	}

	first := runSync("user")
	second := runSync("group")

	// Reopen read-only. Single-sync contract: the second runSync did a
	// fresh StartNewSync on the reopened file, which REPLACES the first
	// sync. The second must survive the save/reopen round trip; the
	// first must be gone.
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { _ = w.Close(ctx) }()
	eng, ok := pebble.AsEngine(w)
	require.True(t, ok, "not a pebble store: %T", w)
	_, err = eng.GetSyncRunRecord(ctx, second)
	require.NoError(t, err, "second sync %s lost across save/reopen", second)
	_, err = eng.GetSyncRunRecord(ctx, first)
	require.Error(t, err, "first sync %s should have been replaced by the second (single-sync contract)", first)
}
