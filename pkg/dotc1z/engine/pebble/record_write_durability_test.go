package pebble

import (
	"context"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// syncCountingFS counts Sync/SyncData/SyncTo calls against WAL files
// (".log"). Write options are an argument, not an observable: counting
// what pebble asked the filesystem to do is what distinguishes a NoSync
// commit from a Sync one.
type syncCountingFS struct {
	vfs.FS
	walSyncs atomic.Int64
}

func (fs *syncCountingFS) wrap(name string, f vfs.File, err error) (vfs.File, error) {
	if err != nil || filepath.Ext(name) != ".log" {
		return f, err
	}
	return &syncCountingFile{File: f, n: &fs.walSyncs}, nil
}

func (fs *syncCountingFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.Create(name, category)
	return fs.wrap(name, f, err)
}

func (fs *syncCountingFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname, category)
	return fs.wrap(newname, f, err)
}

type syncCountingFile struct {
	vfs.File
	n *atomic.Int64
}

func (f *syncCountingFile) Sync() error {
	f.n.Add(1)
	return f.File.Sync()
}

func (f *syncCountingFile) SyncData() error {
	f.n.Add(1)
	return f.File.SyncData()
}

func (f *syncCountingFile) SyncTo(length int64) (bool, error) {
	f.n.Add(1)
	return f.File.SyncTo(length)
}

func durabilityTestGrants(lo, hi int) []*v3.GrantRecord {
	grants := make([]*v3.GrantRecord, 0, hi-lo)
	for i := lo; i < hi; i++ {
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
	return grants
}

// TestBoundSyncRecordWritesDoNotSyncTheWAL pins recordWriteOpts: record
// writes commit NoSync whether the sync is fresh or bound. Binding a
// sync used to send every batch out with pebble.Sync, which is an fsync
// per Put* call for the whole ingest.
//
// The writes still have to reach the artifact, and they do without the
// WAL: CheckpointTo flushes memtables before cutting the checkpoint and
// then truncates the copied WALs to zero (truncateCheckpointWALs), so
// the artifact is SST bytes only. A WAL nobody synced and nobody reads
// cannot cost the artifact a record.
func TestBoundSyncRecordWritesDoNotSyncTheWAL(t *testing.T) {
	ctx := context.Background()
	fs := &syncCountingFS{FS: vfs.NewMem()}
	eng, err := Open(ctx, "durability-db", WithVFS(fs), withPanicOnFatalLogger())
	require.NoError(t, err)
	defer func() { _ = eng.Close() }()

	syncID := ksuid.New().String()
	require.NoError(t, eng.MarkFreshSync(syncID))
	require.NoError(t, eng.PutSyncRunRecord(ctx, v3.SyncRunRecord_builder{
		SyncId:    syncID,
		Type:      v3.SyncType_SYNC_TYPE_FULL,
		StartedAt: timestamppb.Now(),
	}.Build()))
	require.NoError(t, eng.PutGrantRecords(ctx, durabilityTestGrants(0, 50)...))
	require.NoError(t, eng.EndFreshSync(ctx))

	// Bind the sync. bindCurrentSync clears freshSync, so every write
	// below takes the path this test is about.
	require.NoError(t, eng.SetCurrentSync(ctx, syncID))
	require.False(t, eng.IsFreshSync(), "SetCurrentSync left the sync fresh, so the writes below cover nothing")

	// Count only the bound writes: the keyspace-version stamp and the
	// fresh seal legitimately sync, and they already happened.
	before := fs.walSyncs.Load()
	require.NoError(t, eng.PutGrantRecords(ctx, durabilityTestGrants(50, 100)...))
	require.Equal(t, before, fs.walSyncs.Load(), "PutGrantRecords on a bound sync fsynced the WAL")

	require.NoError(t, eng.EndSync(ctx))

	ckDir := "durability-checkpoint"
	require.NoError(t, eng.CheckpointTo(ctx, ckDir), "CheckpointTo")

	// The checkpoint's WALs are zero bytes, so the grants found below
	// came out of SSTs the flush produced, not out of a WAL replay.
	entries, err := fs.List(ckDir)
	require.NoError(t, err)
	sawWAL := false
	for _, name := range entries {
		if filepath.Ext(name) != ".log" {
			continue
		}
		info, err := fs.Stat(fs.PathJoin(ckDir, name))
		require.NoError(t, err)
		if info.IsDir() {
			continue
		}
		sawWAL = true
		require.Equal(t, int64(0), info.Size(), "checkpoint WAL %s is not empty", name)
	}
	require.True(t, sawWAL, "checkpoint held no WAL at all, so the zero-byte check proved nothing")

	reopened, err := Open(ctx, ckDir, WithVFS(fs), WithReadOnly(true))
	require.NoError(t, err, "Open checkpoint")
	defer func() { _ = reopened.Close() }()
	count := 0
	require.NoError(t, reopened.IterateGrants(ctx, func(*v3.GrantRecord) bool {
		count++
		return true
	}))
	require.Equal(t, 100, count, "grants written on a bound sync are missing from the checkpoint")
}
