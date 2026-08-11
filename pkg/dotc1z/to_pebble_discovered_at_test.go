package dotc1z

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// TestToPebblePreservesDiscoveredAt pins that conversion carries each
// record's sqlite discovered_at column onto the v3 record instead of
// re-stamping conversion wall-clock time. Every Pebble merge strategy
// picks winners by newest discovered_at, so a re-stamp would make a
// converted input override genuinely newer records during multi-engine
// compaction.
func TestToPebblePreservesDiscoveredAt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	src, err := NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, src.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
	))
	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	user := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	require.NoError(t, src.PutResources(ctx, group, user))
	member := v2.Entitlement_builder{Id: "member", Resource: group, Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT}.Build()
	require.NoError(t, src.PutEntitlements(ctx, member))
	require.NoError(t, src.PutGrants(ctx, v2.Grant_builder{Id: "g1", Principal: user, Entitlement: member}.Build()))

	// Backdate every record's discovered_at to a fixed instant well in
	// the past, stamped exactly as the writers do: sqliteTimeFormat in local
	// time. The instant is therefore a local one — the column carries no zone,
	// and conversion interprets it as local (localizeSQLiteTimestamp).
	want := time.Date(2020, time.January, 2, 3, 4, 5, 123456789, time.Local)
	stamp := want.Format(sqliteTimeFormat)
	for _, table := range []string{resourceTypes.Name(), resources.Name(), entitlements.Name(), grants.Name()} {
		_, err := src.rawDb.ExecContext(ctx, "UPDATE "+table+" SET discovered_at = ?", stamp) //nolint:gosec // fixed internal table names.
		require.NoError(t, err)
	}
	require.NoError(t, src.EndSync(ctx))

	outPath := filepath.Join(dir, "out.c1z")
	_, err = src.ToPebble(ctx, outPath, syncID)
	require.NoError(t, err)

	dest, err := NewStore(ctx, outPath, WithReadOnly(true), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dest.Close(ctx)) }()
	eng, ok := pebble.AsEngine(dest)
	require.True(t, ok)

	iter, err := eng.NewIter(nil)
	require.NoError(t, err)
	defer iter.Close()

	checked := 0
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 2 || key[0] != 0x03 {
			continue
		}
		var m proto.Message
		switch key[1] {
		case 0x01:
			m = &v3.ResourceTypeRecord{}
		case 0x02:
			m = &v3.ResourceRecord{}
		case 0x03:
			m = &v3.EntitlementRecord{}
		case 0x04:
			m = &v3.GrantRecord{}
		default:
			continue
		}
		require.NoError(t, proto.Unmarshal(iter.Value(), m))
		var got time.Time
		switch r := m.(type) {
		case *v3.ResourceTypeRecord:
			got = r.GetDiscoveredAt().AsTime()
		case *v3.ResourceRecord:
			got = r.GetDiscoveredAt().AsTime()
		case *v3.EntitlementRecord:
			got = r.GetDiscoveredAt().AsTime()
		case *v3.GrantRecord:
			got = r.GetDiscoveredAt().AsTime()
		}
		require.True(t, got.Equal(want),
			"record in bucket %#02x has discovered_at %s; conversion must preserve the source's %s, not re-stamp conversion time", key[1], got, want)
		checked++
	}
	require.NoError(t, iter.Error())
	require.Equal(t, 6, checked, "expected 2 resource types + 2 resources + 1 entitlement + 1 grant")
}

// TestToPebblePreservesStartedAtAgeCutoff pins that conversion copies the
// source sync's started_at. Without that, StartNewSync stamps now and an
// abandoned unfinished sync (past unfinishedSyncMaxAge) becomes a live
// resume target after conversion.
func TestToPebblePreservesStartedAtAgeCutoff(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	src, err := NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.CheckpointSync(ctx, "stale-token"))
	// Do not EndSync — leave unfinished, then backdate past the one-week cutoff.
	// Stamped in local time, as the writers do: the column carries a zone-less
	// local wall clock, so seeding a UTC one would misrepresent the instant the
	// conversion is supposed to preserve.
	staleStart := time.Now().AddDate(0, 0, -30)
	_, err = src.db.ExecContext(ctx,
		`UPDATE `+syncRuns.Name()+` SET started_at = ? WHERE sync_id = ?`,
		staleStart.Format(sqliteTimeFormat), syncID,
	)
	require.NoError(t, err)

	outPath := filepath.Join(dir, "out.c1z")
	_, err = src.ToPebble(ctx, outPath, syncID)
	require.NoError(t, err)

	dst, err := NewStore(ctx, outPath, WithEngine(c1zstore.EnginePebble), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close(ctx)) }()

	eng, ok := pebble.AsEngine(dst)
	require.True(t, ok)
	rec, err := eng.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.Nil(t, rec.GetEndedAt())
	require.Equal(t, "stale-token", rec.GetSyncToken())
	require.WithinDuration(t, staleStart, rec.GetStartedAt().AsTime(), time.Second)

	unfinished, err := eng.LatestUnfinishedSyncRecord(ctx, nil)
	require.NoError(t, err)
	require.Nil(t, unfinished, "stale unfinished sync must stay past the age cutoff after conversion")
}

// TestToPebbleEmptySyncIDSkipsAbandonedSync is the end-to-end form of
// resolveConvertSyncID's stale-unfinished ranking, on the path that matters:
// convert-open passing "" on a file whose newest sync is abandoned work.
//
// Picking the abandoned sync by started_at alone yields a file that can neither
// be resumed (past the age cutoff) nor read as a snapshot (no ended_at), so the
// older finished full sync has to win even though it started first.
func TestToPebbleEmptySyncIDSkipsAbandonedSync(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	src, err := NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	backdate := func(syncID, column string, age time.Duration) {
		t.Helper()
		_, execErr := src.db.ExecContext(ctx,
			`UPDATE `+syncRuns.Name()+` SET `+column+` = ? WHERE sync_id = ?`,
			time.Now().Add(-age).Format(sqliteTimeFormat), syncID,
		)
		require.NoError(t, execErr)
	}

	finishedID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
	}.Build()))
	require.NoError(t, src.EndSync(ctx))
	backdate(finishedID, "started_at", 60*24*time.Hour)
	backdate(finishedID, "ended_at", 60*24*time.Hour)

	// Started later than the finished sync, so started_at alone would pick
	// it, but abandoned well past the resume cutoff.
	abandonedID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.CheckpointSync(ctx, "stale-token"))
	backdate(abandonedID, "started_at", 30*24*time.Hour)

	outPath := filepath.Join(dir, "out.c1z")
	stats, err := src.ToPebble(ctx, outPath, "")
	require.NoError(t, err)
	require.Equal(t, finishedID, stats.SourceSyncID, "must convert the finished sync, not the abandoned one")

	dst, err := NewStore(ctx, outPath, WithEngine(c1zstore.EnginePebble), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close(ctx)) }()

	latest, err := dst.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, latest, "converted file must be readable as a finished snapshot")
	require.Equal(t, finishedID, latest.ID)
}
