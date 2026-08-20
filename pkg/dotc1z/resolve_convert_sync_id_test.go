package dotc1z

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

type convertSyncSeed struct {
	id       string
	syncType connectorstore.SyncType
	started  time.Duration  // offset from base
	ended    *time.Duration // nil = unfinished; offset from base when set
}

// TestResolveConvertSyncID_Mixed covers ConvertResolveBehaviorNewest's ranking.
// Offsets are relative to now, because the ranking depends on whether an
// unfinished sync is still within unfinishedSyncMaxAge: fresh unfinished syncs
// compete on started_at alone, while stale ones (unresumable and unreadable)
// lose to any finished sync.
func TestResolveConvertSyncID_Mixed(t *testing.T) {
	base := time.Now()
	const (
		stale = -30 * 24 * time.Hour
		// Just past the cutoff, to catch an off-by-a-day comparison
		// (the historical bug: an RFC3339 bound made same-date rows
		// compare as older).
		barelyStale = -unfinishedSyncMaxAge - time.Hour
		barelyFresh = -unfinishedSyncMaxAge + time.Hour
	)

	tests := []struct {
		name  string
		syncs []convertSyncSeed
		want  string
	}{
		{
			name: "newest started_at wins across types",
			syncs: []convertSyncSeed{
				{id: "full-done", syncType: connectorstore.SyncTypeFull, started: -2 * time.Hour, ended: dur(-90 * time.Minute)},
				{id: "partial-done", syncType: connectorstore.SyncTypePartial, started: -time.Hour, ended: dur(-30 * time.Minute)},
			},
			want: "partial-done",
		},
		{
			name: "fresh unfinished beats older finished",
			syncs: []convertSyncSeed{
				{id: "full-done", syncType: connectorstore.SyncTypeFull, started: -2 * time.Hour, ended: dur(-90 * time.Minute)},
				{id: "full-open", syncType: connectorstore.SyncTypeFull, started: -time.Hour},
			},
			want: "full-open",
		},
		{
			name: "fresh unfinished beats older finished of another type",
			syncs: []convertSyncSeed{
				{id: "partial-done", syncType: connectorstore.SyncTypePartial, started: -2 * time.Hour, ended: dur(-90 * time.Minute)},
				{id: "full-open", syncType: connectorstore.SyncTypeFull, started: -time.Hour},
			},
			want: "full-open",
		},
		{
			name: "finished beats newer stale unfinished",
			syncs: []convertSyncSeed{
				{id: "full-done", syncType: connectorstore.SyncTypeFull, started: stale, ended: dur(stale + time.Minute)},
				{id: "full-abandoned", syncType: connectorstore.SyncTypeFull, started: barelyStale},
			},
			want: "full-done",
		},
		{
			name: "finished beats stale unfinished even when the finished one is much older",
			syncs: []convertSyncSeed{
				{id: "full-done", syncType: connectorstore.SyncTypeFull, started: -365 * 24 * time.Hour, ended: dur(-365*24*time.Hour + time.Minute)},
				{id: "full-abandoned", syncType: connectorstore.SyncTypeFull, started: barelyStale},
			},
			want: "full-done",
		},
		{
			name: "unfinished just inside the cutoff still beats an older finished sync",
			syncs: []convertSyncSeed{
				{id: "full-done", syncType: connectorstore.SyncTypeFull, started: stale, ended: dur(stale + time.Minute)},
				{id: "full-open", syncType: connectorstore.SyncTypeFull, started: barelyFresh},
			},
			want: "full-open",
		},
		{
			name: "newest stale unfinished wins when there is no finished sync",
			syncs: []convertSyncSeed{
				{id: "full-older", syncType: connectorstore.SyncTypeFull, started: stale},
				{id: "full-newer", syncType: connectorstore.SyncTypeFull, started: barelyStale},
			},
			want: "full-newer",
		},
		{
			name: "newer unfinished beats older unfinished",
			syncs: []convertSyncSeed{
				{id: "full-old", syncType: connectorstore.SyncTypeFull, started: -2 * time.Hour},
				{id: "full-new", syncType: connectorstore.SyncTypeFull, started: -time.Hour},
			},
			want: "full-new",
		},
		{
			name: "newer started_at wins among finished syncs",
			syncs: []convertSyncSeed{
				{id: "full-long", syncType: connectorstore.SyncTypeFull, started: -2 * time.Hour, ended: dur(-10 * time.Minute)},
				{id: "full-short", syncType: connectorstore.SyncTypeFull, started: -time.Hour, ended: dur(-50 * time.Minute)},
			},
			want: "full-short",
		},
		{
			name: "unknown sync types are ignored",
			syncs: []convertSyncSeed{
				{id: "full-done", syncType: connectorstore.SyncTypeFull, started: -2 * time.Hour, ended: dur(-90 * time.Minute)},
				{id: "unknown-new", syncType: connectorstore.SyncType("frobnitz"), started: -time.Hour, ended: dur(-30 * time.Minute)},
			},
			want: "full-done",
		},
		{
			name: "a stale unfinished sync is still resolvable on its own",
			syncs: []convertSyncSeed{
				{id: "full-abandoned", syncType: connectorstore.SyncTypeFull, started: stale},
			},
			want: "full-abandoned",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			src, err := NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), WithTmpDir(dir))
			require.NoError(t, err)
			defer func() { require.NoError(t, src.Close(ctx)) }()

			for _, s := range tc.syncs {
				seedConvertSyncRun(t, ctx, src, s, base)
			}

			got, err := src.resolveConvertSyncID(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestGetLatestUnfinishedSyncCutoff pins the resume cutoff that
// resolveConvertSyncID's ranking mirrors. The two read the same bound, so a
// sync the resolver treats as live must actually be resumable — otherwise
// conversion picks a sync that can be neither resumed nor read.
func TestGetLatestUnfinishedSyncCutoff(t *testing.T) {
	ctx := context.Background()
	base := time.Now()

	for _, tc := range []struct {
		name    string
		started time.Duration
		want    string
	}{
		{name: "well within the cutoff", started: -time.Hour, want: "full-open"},
		{name: "just within the cutoff", started: -unfinishedSyncMaxAge + time.Hour, want: "full-open"},
		{name: "just past the cutoff", started: -unfinishedSyncMaxAge - time.Hour, want: ""},
		{name: "long past the cutoff", started: -30 * 24 * time.Hour, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			src, err := NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), WithTmpDir(dir))
			require.NoError(t, err)
			defer func() { require.NoError(t, src.Close(ctx)) }()

			seedConvertSyncRun(t, ctx, src, convertSyncSeed{
				id:       "full-open",
				syncType: connectorstore.SyncTypeFull,
				started:  tc.started,
			}, base)

			run, err := src.getLatestUnfinishedSync(ctx, connectorstore.SyncTypeFull)
			require.NoError(t, err)
			if tc.want == "" {
				require.Nil(t, run, "sync past the cutoff must not be resumable")
				return
			}
			require.NotNil(t, run, "sync within the cutoff must be resumable")
			require.Equal(t, tc.want, run.ID)
		})
	}
}

func dur(d time.Duration) *time.Duration { return &d }

func seedConvertSyncRun(t *testing.T, ctx context.Context, c *C1File, s convertSyncSeed, base time.Time) {
	t.Helper()
	require.NoError(t, c.insertSyncRun(ctx, s.id, s.syncType, ""))

	started := base.Add(s.started).Format(sqliteTimeFormat)
	_, err := c.db.ExecContext(ctx,
		`UPDATE `+syncRuns.Name()+` SET started_at = ? WHERE sync_id = ?`,
		started, s.id,
	)
	require.NoError(t, err)

	if s.ended == nil {
		return
	}
	ended := base.Add(*s.ended).Format(sqliteTimeFormat)
	_, err = c.db.ExecContext(ctx,
		`UPDATE `+syncRuns.Name()+` SET ended_at = ? WHERE sync_id = ?`,
		ended, s.id,
	)
	require.NoError(t, err)
}
