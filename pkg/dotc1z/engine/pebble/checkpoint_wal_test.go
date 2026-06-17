package pebble

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// TestCheckpointWALsAreEmpty locks in the close/save contract: a
// checkpoint cut by CheckpointTo carries no WAL bytes. Pebble copies
// physical WAL files into checkpoints, and WAL recycling fills those
// files with stale garbage whose replay (CRC mismatch → per-bit
// bit-flip diagnostic) measured ~23% of total CPU in a 500-source
// compaction. CheckpointTo flushes first, so truncating the copied
// WALs to zero loses nothing — and the checkpoint must reopen cleanly
// with all data intact.
func TestCheckpointWALsAreEmpty(t *testing.T) {
	ctx := context.Background()
	eng, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, eng.MarkFreshSync(syncID))
	require.NoError(t, eng.PutSyncRunRecord(ctx, v3.SyncRunRecord_builder{
		SyncId:    syncID,
		Type:      v3.SyncType_SYNC_TYPE_FULL,
		StartedAt: timestamppb.Now(),
		EndedAt:   timestamppb.Now(),
	}.Build()))
	grants := make([]*v3.GrantRecord, 0, 100)
	for i := 0; i < 100; i++ {
		grants = append(grants, v3.GrantRecord_builder{
			ExternalId: "grant-" + strconv.Itoa(i),
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: "group", ResourceId: "g1", EntitlementId: "member",
			}.Build(),
			Principal: v3.PrincipalRef_builder{
				ResourceTypeId: "user", ResourceId: "u" + strconv.Itoa(i),
			}.Build(),
			DiscoveredAt: timestamppb.Now(),
		}.Build())
	}
	require.NoError(t, eng.PutGrantRecords(ctx, grants...))
	require.NoError(t, eng.EndFreshSync(ctx))

	ckDir := filepath.Join(t.TempDir(), "checkpoint")
	require.NoError(t, eng.CheckpointTo(ctx, ckDir), "CheckpointTo")

	// Every WAL segment in the checkpoint must be zero bytes.
	entries, err := os.ReadDir(ckDir)
	require.NoError(t, err)
	for _, ent := range entries {
		if ent.IsDir() || filepath.Ext(ent.Name()) != ".log" {
			continue
		}
		info, err := ent.Info()
		require.NoError(t, err)
		require.Equal(t, int64(0), info.Size(), "checkpoint WAL %s has %d bytes, want 0 (stale recycled content leaks into every open)", ent.Name(), info.Size())
	}

	// The checkpoint must reopen cleanly (read-only — the compaction
	// source path) with every record intact.
	reopened, err := Open(ctx, ckDir, WithReadOnly(true))
	require.NoError(t, err, "Open checkpoint")
	defer func() { _ = reopened.Close() }()
	rec, err := reopened.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err, "GetSyncRunRecord")
	require.NotNil(t, rec.GetEndedAt(), "reopened sync run lost ended_at")
	count := 0
	err = reopened.IterateGrants(ctx, func(*v3.GrantRecord) bool {
		count++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 100, count, "reopened grant count")
}
