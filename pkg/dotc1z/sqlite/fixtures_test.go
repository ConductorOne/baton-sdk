package sqlite_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	sqlitereader "github.com/conductorone/baton-sdk/pkg/dotc1z/sqlite"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

var fixtureNames = []string{
	"finished_full.c1z",
	"unfinished_partial.c1z",
	"multi_sync.c1z",
	"empty.c1z",
	"duplicate_identity_grants.c1z",
	"old_schema.c1z",
	"edge_keys.c1z",
	"bare.db",
	"slim_grants.c1z",
}

func TestFixtureGoldenDigests(t *testing.T) {
	ctx := context.Background()
	for _, name := range fixtureNames {
		t.Run(name, func(t *testing.T) {
			sourcePath := filepath.Join("testdata", name)
			before, err := os.ReadFile(sourcePath)
			require.NoError(t, err)
			source, err := sqlitereader.Open(ctx, sourcePath, sqlitereader.WithTmpDir(t.TempDir()))
			require.NoError(t, err)
			outPath := filepath.Join(t.TempDir(), "out.c1z")
			stats, err := source.ToPebble(ctx, outPath, "")
			require.NoError(t, err)
			require.NoError(t, source.Close(ctx))
			after, err := os.ReadFile(sourcePath)
			require.NoError(t, err)
			require.Equal(t, before, after)

			got := digestConvertedFile(t, outPath, stats.DestSyncID)
			want, err := os.ReadFile(filepath.Join("testdata", name+".golden"))
			require.NoError(t, err)
			require.Equal(t, strings.TrimSpace(string(want)), got)
		})
	}
}

func TestFixtureReferenceParity(t *testing.T) {
	ctx := context.Background()
	for _, name := range fixtureNames {
		if name == "slim_grants.c1z" || name == "bare.db" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			sourcePath := filepath.Join("testdata", name)
			tmpDir := t.TempDir()
			reference, err := dotc1z.NewC1ZFile(ctx, sourcePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(tmpDir))
			require.NoError(t, err)
			referencePath := filepath.Join(tmpDir, "reference.c1z")
			referenceStats, err := reference.ToPebble(ctx, referencePath, "")
			require.NoError(t, err)
			require.NoError(t, reference.Close(ctx))

			source, err := sqlitereader.Open(ctx, sourcePath, sqlitereader.WithTmpDir(tmpDir))
			require.NoError(t, err)
			actualPath := filepath.Join(tmpDir, "actual.c1z")
			actualStats, err := source.ToPebble(ctx, actualPath, "")
			require.NoError(t, err)
			require.NoError(t, source.Close(ctx))

			require.Equal(t, referenceStats.SourceSyncID, actualStats.SourceSyncID)
			require.Equal(t, referenceStats.DestSyncID, actualStats.DestSyncID)
			require.Equal(t, referenceStats.ResourceTypes.Rows, actualStats.ResourceTypes.Rows)
			require.Equal(t, referenceStats.Resources.Rows, actualStats.Resources.Rows)
			require.Equal(t, referenceStats.Entitlements.Rows, actualStats.Entitlements.Rows)
			require.Equal(t, referenceStats.Grants.Rows, actualStats.Grants.Rows)
			require.Equal(t, referenceStats.Assets.Rows, actualStats.Assets.Rows)
			require.Equal(t, referenceStats.Sessions.Rows, actualStats.Sessions.Rows)
			require.Equal(t, referenceStats.DiscardedSyncs, toReferenceDiscarded(actualStats.DiscardedSyncs))
			require.Equal(t,
				convertedKeyspace(t, referencePath),
				convertedKeyspace(t, actualPath),
			)
			if actualStats.DestSyncID != "" {
				require.True(t, proto.Equal(
					syncRecord(t, referencePath, referenceStats.DestSyncID),
					syncRecord(t, actualPath, actualStats.DestSyncID),
				))
				require.True(t, proto.Equal(
					stableStatsRecord(t, referencePath, referenceStats.DestSyncID),
					stableStatsRecord(t, actualPath, actualStats.DestSyncID),
				))
			}
		})
	}
}

func toReferenceDiscarded(discarded []sqlitereader.DiscardedSync) []dotc1z.DiscardedSync {
	if discarded == nil {
		return nil
	}
	out := make([]dotc1z.DiscardedSync, len(discarded))
	for i, item := range discarded {
		out[i] = dotc1z.DiscardedSync{
			ID:        item.ID,
			Type:      item.Type,
			StartedAt: item.StartedAt,
			EndedAt:   item.EndedAt,
		}
	}
	return out
}

func convertedKeyspace(t testing.TB, path string) map[string]string {
	t.Helper()
	ctx := context.Background()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	engine, ok := pebble.AsEngine(store)
	require.True(t, ok)
	iter, err := engine.NewIter(nil)
	require.NoError(t, err)
	defer iter.Close()
	out := make(map[string]string)
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 2 || key[0] != 0x03 {
			continue
		}
		switch key[1] {
		case 0x01, 0x02, 0x03, 0x04, 0x05, 0x07:
			out[string(append([]byte(nil), key...))] = string(stableRecordValue(t, key[1], iter.Value()))
		}
	}
	require.NoError(t, iter.Error())
	return out
}

func digestConvertedFile(t testing.TB, path, syncID string) string {
	t.Helper()
	ctx := context.Background()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	engine, ok := pebble.AsEngine(store)
	require.True(t, ok)

	hash := sha256.New()
	iter, err := engine.NewIter(nil)
	require.NoError(t, err)
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 2 || key[0] != 0x03 {
			continue
		}
		switch key[1] {
		case 0x01, 0x02, 0x03, 0x04, 0x05, 0x07:
		default:
			continue
		}
		writeDigestPart(hash, key)
		writeDigestPart(hash, stableRecordValue(t, key[1], iter.Value()))
	}
	require.NoError(t, iter.Error())
	if syncID != "" {
		record, err := engine.GetSyncRunRecord(ctx, syncID)
		require.NoError(t, err)
		record.SetStartedAt(nil)
		record.SetEndedAt(nil)
		data, err := proto.MarshalOptions{Deterministic: true}.Marshal(record)
		require.NoError(t, err)
		writeDigestPart(hash, data)
		stats := stableStatsRecord(t, path, syncID)
		statsData, err := proto.MarshalOptions{Deterministic: true}.Marshal(stats)
		require.NoError(t, err)
		writeDigestPart(hash, statsData)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func stableRecordValue(t testing.TB, bucket byte, data []byte) []byte {
	t.Helper()
	var message proto.Message
	switch bucket {
	case 0x01:
		record := &v3.ResourceTypeRecord{}
		require.NoError(t, proto.Unmarshal(data, record))
		record.SetDiscoveredAt(nil)
		message = record
	case 0x02:
		record := &v3.ResourceRecord{}
		require.NoError(t, proto.Unmarshal(data, record))
		record.SetDiscoveredAt(nil)
		message = record
	case 0x03:
		record := &v3.EntitlementRecord{}
		require.NoError(t, proto.Unmarshal(data, record))
		record.SetDiscoveredAt(nil)
		message = record
	case 0x04:
		record := &v3.GrantRecord{}
		require.NoError(t, proto.Unmarshal(data, record))
		record.SetDiscoveredAt(nil)
		message = record
	case 0x05:
		record := &v3.AssetRecord{}
		require.NoError(t, proto.Unmarshal(data, record))
		record.SetDiscoveredAt(nil)
		message = record
	default:
		return append([]byte(nil), data...)
	}
	stable, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	require.NoError(t, err)
	return stable
}

type digestWriter interface {
	Write([]byte) (int, error)
}

func writeDigestPart(writer digestWriter, data []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(data)))
	_, _ = writer.Write(size[:])
	_, _ = writer.Write(data)
}

func syncRecord(t testing.TB, path, syncID string) *v3.SyncRunRecord {
	t.Helper()
	ctx := context.Background()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	engine, ok := pebble.AsEngine(store)
	require.True(t, ok)
	record, err := engine.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	return record
}

func statsRecord(t testing.TB, path, syncID string) *v3.SyncStatsRecord {
	t.Helper()
	ctx := context.Background()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	engine, ok := pebble.AsEngine(store)
	require.True(t, ok)
	record, err := pebble.ReadSyncStatsRecord(ctx, engine, syncID)
	require.NoError(t, err)
	require.NotNil(t, record)
	return record
}

func stableStatsRecord(t testing.TB, path, syncID string) *v3.SyncStatsRecord {
	t.Helper()
	record := statsRecord(t, path, syncID)
	record.SetWrittenAt(nil)
	return record
}
