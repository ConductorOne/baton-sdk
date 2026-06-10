package pebble

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/segmentio/ksuid"
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
	if err := eng.MarkFreshSync(syncID); err != nil {
		t.Fatal(err)
	}
	if err := eng.PutSyncRunRecord(ctx, v3.SyncRunRecord_builder{
		SyncId:    syncID,
		Type:      v3.SyncType_SYNC_TYPE_FULL,
		StartedAt: timestamppb.Now(),
		EndedAt:   timestamppb.Now(),
	}.Build()); err != nil {
		t.Fatal(err)
	}
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
	if err := eng.PutGrantRecords(ctx, grants...); err != nil {
		t.Fatal(err)
	}
	if err := eng.EndFreshSync(ctx); err != nil {
		t.Fatal(err)
	}

	ckDir := filepath.Join(t.TempDir(), "checkpoint")
	if err := eng.CheckpointTo(ctx, ckDir); err != nil {
		t.Fatalf("CheckpointTo: %v", err)
	}

	// Every WAL segment in the checkpoint must be zero bytes.
	entries, err := os.ReadDir(ckDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, ent := range entries {
		if ent.IsDir() || filepath.Ext(ent.Name()) != ".log" {
			continue
		}
		info, err := ent.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != 0 {
			t.Fatalf("checkpoint WAL %s has %d bytes, want 0 (stale recycled content leaks into every open)", ent.Name(), info.Size())
		}
	}

	// The checkpoint must reopen cleanly (read-only — the compaction
	// source path) with every record intact.
	reopened, err := Open(ctx, ckDir, WithReadOnly(true))
	if err != nil {
		t.Fatalf("Open checkpoint: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	rec, err := reopened.GetSyncRunRecord(ctx, syncID)
	if err != nil {
		t.Fatalf("GetSyncRunRecord: %v", err)
	}
	if rec.GetEndedAt() == nil {
		t.Fatal("reopened sync run lost ended_at")
	}
	count := 0
	if err := reopened.IterateGrantsBySync(ctx, syncID, func(*v3.GrantRecord) bool {
		count++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatalf("reopened grant count = %d, want 100", count)
	}
}
