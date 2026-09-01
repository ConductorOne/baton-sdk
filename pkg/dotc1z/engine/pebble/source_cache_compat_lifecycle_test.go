package pebble

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// TestSourceCacheCompatRecordLifecycle replaces the Phase 6a
// `C30-compatibility-record-lifecycle` exclusion: the compat record was
// schema-only then, and Phase 6b added the Pebble writer/reader
// (PutSourceCacheCompatRecord / GetSourceCacheCompatRecord). Per 6a C30 the
// record must be included in reset, cleanup, invalidation, and leak
// accounting — WITHOUT asserting eligibility/matching semantics, which are
// orchestration behavior verified by the 6b chaos gate suite.
//
// Leak accounting rides along in every cell: the newTestEngine fixture's
// Close reports leaked iterators, Get closers, and unreleased batches.
func TestSourceCacheCompatRecordLifecycle(t *testing.T) {
	newRecord := func(id string) *v3.SourceCacheCompatRecord {
		return v3.SourceCacheCompatRecord_builder{
			Id:                           id,
			ConnectorCacheGeneration:     "gen-1",
			ConnectorConfigFingerprint:   "cfg-1",
			SdkMaterializationGeneration: "sdkmat-1",
			SyncSelectionFingerprint:     "sel-1",
		}.Build()
	}

	t.Run("round-trip-forces-singleton-id", func(t *testing.T) {
		ctx := context.Background()
		e, _ := newTestEngine(t)
		a := NewAdapter(e)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		// The caller's id is deliberately wrong: the writer must force the
		// singleton constant so no caller can mint a second record.
		require.NoError(t, e.PutSourceCacheCompatRecord(ctx, newRecord("not-compat")))
		require.NoError(t, a.EndSync(ctx))

		got, err := e.GetSourceCacheCompatRecord(ctx)
		require.NoError(t, err)
		require.Equal(t, "compat", got.GetId())
		require.Equal(t, "gen-1", got.GetConnectorCacheGeneration())
		require.Equal(t, "cfg-1", got.GetConnectorConfigFingerprint())
		require.Equal(t, "sdkmat-1", got.GetSdkMaterializationGeneration())
		require.Equal(t, "sel-1", got.GetSyncSelectionFingerprint())
	})

	t.Run("replay-state-invalidation-wipes-it", func(t *testing.T) {
		// The compat key lives inside the source-cache family on purpose:
		// fold/rebuild compactions invalidate replay state with one range
		// tombstone, and a compat record must never outlive the manifest
		// entries whose recording conditions it declares.
		ctx := context.Background()
		e, _ := newTestEngine(t)
		a := NewAdapter(e)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, e.PutSourceCacheCompatRecord(ctx, newRecord("compat")))
		require.NoError(t, a.EndSync(ctx))

		require.NoError(t, e.InvalidateSourceCacheReplayState(ctx, false))
		_, err = e.GetSourceCacheCompatRecord(ctx)
		require.ErrorIs(t, err, pebble.ErrNotFound,
			"replay-state invalidation must remove the compat record with the manifest entries")
	})

	t.Run("reset-for-replacement-sync-wipes-it", func(t *testing.T) {
		ctx := context.Background()
		e, _ := newTestEngine(t)
		a := NewAdapter(e)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, e.PutSourceCacheCompatRecord(ctx, newRecord("compat")))
		require.NoError(t, a.EndSync(ctx))

		// StartNewSync excises every sync-scoped keyspace before binding
		// the replacement sync; the compat record must not survive into a
		// sync whose recording conditions it does not describe.
		_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		_, err = e.GetSourceCacheCompatRecord(ctx)
		require.ErrorIs(t, err, pebble.ErrNotFound,
			"a replacement sync must not inherit the prior sync's compat record")
		require.NoError(t, a.EndSync(ctx))
	})

	t.Run("cleanup-ranges-cover-the-compat-key", func(t *testing.T) {
		// Same shape as TestSyncScopedRangesCoverEveryWrittenIndex: the
		// cleanup/reset range list must contain the compat key, so a
		// future keyspace move fails here instead of leaking the record
		// past a sync delete.
		key := rawdb.SourceCacheCompatKey()
		covered := false
		for _, r := range scopedRanges() {
			if bytes.Compare(key, r[0]) >= 0 && bytes.Compare(key, r[1]) < 0 {
				covered = true
				break
			}
		}
		require.True(t, covered, "SourceCacheCompatKey not covered by any syncScopedRanges entry: %x", key)
	})
}
