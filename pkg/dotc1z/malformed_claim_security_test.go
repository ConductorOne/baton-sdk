package dotc1z

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestSecurity_InvalidVerificationClaimReadsUnverified pins how sync-run
// reads treat an ingest-invariant verification marker that is present but
// not valid for this SDK: a mode this SDK does not know (a newer SDK may
// add one), empty coverage, or coverage bytes that do not decode. The run
// reads as unverified, so no consumer trusts the claim, and the read
// succeeds, so an artifact from a newer SDK never fails ListSyncRuns or
// LatestFinishedSyncOfAnyType here.
func TestSecurity_InvalidVerificationClaimReadsUnverified(t *testing.T) {
	ctx := context.Background()

	const syncID = "2YpKj9CDvFqPHLhMZWB4h0X1YyL"
	ddl := func(gen, coverage, mode string) []string {
		return []string{
			`CREATE TABLE v1_resource_types (id integer primary key, external_id text not null, data blob not null, sync_id text not null, discovered_at datetime not null)`,
			`CREATE TABLE v1_resources (id integer primary key, resource_type_id text not null, external_id text not null,` +
				` parent_resource_type_id text, parent_resource_id text, data blob not null,` +
				` sync_id text not null, discovered_at datetime not null)`,
			`CREATE TABLE v1_entitlements (id integer primary key, resource_type_id text not null,` +
				` resource_id text not null, external_id text not null, data blob not null,` +
				` sync_id text not null, discovered_at datetime not null)`,
			`CREATE TABLE v1_grants (id integer primary key, resource_type_id text not null,` +
				` resource_id text not null, entitlement_id text not null,` +
				` principal_resource_type_id text not null, principal_resource_id text not null,` +
				` external_id text not null, data blob not null, sync_id text not null,` +
				` discovered_at datetime not null)`,
			`CREATE TABLE v1_sync_runs (id integer primary key, sync_id text not null,` +
				` started_at datetime not null, ended_at datetime, sync_token text not null,` +
				` sync_type text not null default 'full', parent_sync_id text not null default '',` +
				` supports_diff integer not null default 0, grants_backfilled integer not null default 0,` +
				` stats text, ingest_invariant_generation text not null default '',` +
				` ingest_invariant_coverage text not null default '',` +
				` ingest_invariant_mode text not null default '')`,
			`CREATE TABLE v1_assets (id integer primary key, external_id text not null, content_type text not null, data blob not null, sync_id text not null, discovered_at datetime not null)`,
			`CREATE TABLE v1_connector_sessions (id integer primary key, sync_id text NOT NULL, key TEXT NOT NULL, value BLOB NOT NULL)`,
			`INSERT INTO v1_sync_runs (sync_id, started_at, ended_at, sync_token, sync_type, grants_backfilled,` +
				` ingest_invariant_generation, ingest_invariant_coverage, ingest_invariant_mode)` +
				` VALUES ('` + syncID + `', '2024-01-01T00:00:00Z', '2024-01-01T00:01:00Z', 'tok', 'full', 1,` +
				` '` + gen + `', '` + coverage + `', '` + mode + `')`,
		}
	}

	cases := []struct {
		name         string
		gen          string
		coverage     string
		mode         string
		wantVerified bool
	}{
		{name: "mode unknown to this SDK", gen: "gen-1", coverage: `["I1","I5"]`, mode: "a_future_mode"},
		{name: "empty coverage", gen: "gen-1", coverage: `[]`, mode: string(c1zstore.IngestInvariantVerificationModeConnector)},
		{name: "coverage bytes do not decode", gen: "gen-1", coverage: `{not json`, mode: string(c1zstore.IngestInvariantVerificationModeConnector)},
		{name: "absent marker", gen: "", coverage: ``, mode: ""},
		{name: "valid marker (control)", gen: "gen-1", coverage: `["I1","I5"]`, mode: string(c1zstore.IngestInvariantVerificationModeConnector), wantVerified: true},
	}

	t.Run("sqlite", func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				path := hostileV1Fixture(t, "claim.c1z", ddl(tc.gen, tc.coverage, tc.mode)...)
				store, err := NewStore(ctx, path, WithReadOnly(true))
				require.NoError(t, err)
				defer func() { require.NoError(t, store.Close(ctx)) }()

				run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
				require.NoError(t, err)
				require.NotNil(t, run)
				require.Equal(t, syncID, run.ID)
				require.Equal(t, tc.wantVerified, run.IsVerified())
				if !tc.wantVerified {
					require.Empty(t, run.Generation, "an unverified claim must not surface its generation")
				}
			})
		}
	})

	t.Run("pebble", func(t *testing.T) {
		for _, tc := range cases {
			if tc.coverage == `{not json` {
				continue // pebble stores coverage as a repeated proto field, not JSON
			}
			t.Run(tc.name, func(t *testing.T) {
				store, err := NewStore(ctx, filepath.Join(t.TempDir(), "claim.c1z"), WithEngine(c1zstore.EnginePebble))
				require.NoError(t, err)
				defer func() { require.NoError(t, store.Close(ctx)) }()
				id, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				require.NoError(t, store.EndSync(ctx))

				ps, ok := store.(*pebbleStore)
				require.True(t, ok)
				rec, err := ps.GetSyncRunRecord(ctx, id)
				require.NoError(t, err)
				var coverage []string
				if tc.coverage == `["I1","I5"]` {
					coverage = []string{"I1", "I5"}
				}
				rec.SetIngestInvariantGeneration(tc.gen)
				rec.SetIngestInvariantCoverage(coverage)
				rec.SetIngestInvariantMode(tc.mode)
				require.NoError(t, ps.PutSyncRunRecord(ctx, rec))

				run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
				require.NoError(t, err)
				require.NotNil(t, run)
				require.Equal(t, tc.wantVerified, run.IsVerified())
				if !tc.wantVerified {
					require.Empty(t, run.Generation, "an unverified claim must not surface its generation")
				}
			})
		}
	})
}
