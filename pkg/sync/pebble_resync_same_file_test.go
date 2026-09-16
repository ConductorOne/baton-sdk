package sync

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	"github.com/conductorone/baton-sdk/pkg/logging"
)

// A self-hosted connector re-run writes into the same sync.c1z. The second
// run's StartNewSync finds the first sync and resets the keyspace; the
// saved artifact must carry only the second sync, down to the bytes: a
// record of the first run that survived in an SST would ship in the
// envelope even though no read can see it.
func TestPebbleResyncIntoSameFileShipsOnlyTheNewSync(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tempDir := t.TempDir()
	c1zPath := filepath.Join(tempDir, "sync.c1z")

	const run1Needle = "RUN1-NEEDLE-7f3a9c"
	const run2Needle = "RUN2-NEEDLE-2b8e41"
	runSync := func(needle string) {
		mc := newMockConnector()
		mc.rtDB = append(mc.rtDB, userResourceType, groupResourceType)
		for i := range 40 {
			_, _, err := mc.AddGroup(ctx, fmt.Sprintf("%s-group-%03d", needle, i))
			require.NoError(t, err)
		}
		syncer, err := NewSyncer(ctx, mc,
			WithC1ZPath(c1zPath),
			WithTmpDir(tempDir),
			WithStorageEngine(c1zstore.EnginePebble),
			WithDontExpandGrants(),
		)
		require.NoError(t, err)
		require.NoError(t, syncer.Sync(ctx))
		require.NoError(t, syncer.Close(ctx))
	}

	runSync(run1Needle)
	require.Positive(t, c1zVersionHits(t, c1zPath, run1Needle), "premise: the first artifact holds its own records")

	runSync(run2Needle)

	runs := listPebbleSyncRuns(t, ctx, c1zPath, tempDir)
	require.Len(t, runs, 1, "a v3 file holds one sync")
	require.NotNil(t, runs[0].EndedAt, "the second sync finished")

	require.Zero(t, c1zVersionHits(t, c1zPath, run1Needle),
		"no byte of the first sync may survive the reset into the second artifact")
	require.Positive(t, c1zVersionHits(t, c1zPath, run2Needle), "oracle: the second sync's records are there")
}

func listPebbleSyncRuns(t *testing.T, ctx context.Context, path, tmpDir string) []*c1zstore.SyncRun {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	lister, ok := store.(interface {
		ListSyncRuns(context.Context, string, uint32) ([]*c1zstore.SyncRun, string, error)
	})
	require.True(t, ok)
	runs, _, err := lister.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	return runs
}

// c1zVersionHits decodes the envelope and counts every internal KV in
// every SST — shadowed and deleted versions included — plus raw WAL
// bytes, whose key or value contains needle. Iterators collapse to the
// newest version, so this is the only way to see what ships.
func c1zVersionHits(t *testing.T, c1zPath, needle string) int {
	t.Helper()
	f, err := os.Open(c1zPath)
	require.NoError(t, err)
	defer f.Close()
	dir := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	_, _, err = formatv3.ExtractEnvelopePayload(f, dir)
	require.NoError(t, err)

	n := []byte(needle)
	hits := 0
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, ent := range entries {
		path := filepath.Join(dir, ent.Name())
		switch filepath.Ext(ent.Name()) {
		case ".sst":
			sf, err := vfs.Default.Open(path)
			require.NoError(t, err)
			readable, err := sstable.NewSimpleReadable(sf)
			require.NoError(t, err)
			r, err := sstable.NewReader(context.Background(), readable, sstable.ReaderOptions{Comparer: pebble.DefaultComparer})
			require.NoError(t, err)
			it, err := r.NewIter(sstable.NoTransforms, nil, nil, sstable.TableBlobContext{})
			require.NoError(t, err)
			for kv := it.First(); kv != nil; kv = it.Next() {
				val, _, err := kv.Value(nil)
				require.NoError(t, err)
				if bytes.Contains(kv.K.UserKey, n) || bytes.Contains(val, n) {
					hits++
				}
			}
			require.NoError(t, it.Close())
			require.NoError(t, r.Close())
		case ".log":
			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			hits += bytes.Count(raw, n)
		}
	}
	return hits
}
