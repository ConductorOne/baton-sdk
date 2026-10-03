package dotc1z

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestSecurity_MalformedVerificationClaimRejected guards the
// malformed-metadata-claim contract: an ingest-invariant verification
// marker that is structurally PRESENT but INVALID (unknown mode, empty
// coverage on a present generation, malformed coverage bytes) is a
// deterministic data defect and must reject with
// c1zstore.ErrDataRejected. ABSENT markers stay unverified (legacy
// acceptance — the crash window between seal and marker write is
// legitimate), and a current-generation marker never bypasses content
// validation at import.
func TestSecurity_MalformedVerificationClaimRejected(t *testing.T) {
	ctx := context.Background()

	// The verification columns: generation, coverage (JSON), mode.
	baseDDL := []string{
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
	}

	syncRow := func(gen, coverage, mode string) string {
		return `INSERT INTO v1_sync_runs (sync_id, started_at, ended_at, sync_token, sync_type, grants_backfilled,` +
			` ingest_invariant_generation, ingest_invariant_coverage, ingest_invariant_mode)` +
			` VALUES ('2YpKj9CDvFqPHLhMZWB4h0X1YyL', '2024-01-01T00:00:00Z', '2024-01-01T00:01:00Z', 'tok', 'full', 1,` +
			` '` + gen + `', '` + coverage + `', '` + mode + `')`
	}

	t.Run("unknown mode rejected", func(t *testing.T) {
		ddl := append(append([]string(nil), baseDDL...),
			syncRow("gen-1", `["I1","I5"]`, "attacker-chosen-mode"))
		path := hostileV1Fixture(t, "badclaim.c1z", ddl...)
		store, err := NewStore(ctx, path, WithReadOnly(true))
		require.NoError(t, err, "the malformed claim lives in sync-run metadata; it surfaces on the metadata read")
		_, readErr := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
		require.Error(t, readErr)
		require.ErrorIs(t, readErr, c1zstore.ErrDataRejected)
		require.NoError(t, store.Close(ctx))
	})

	t.Run("empty coverage on present generation rejected", func(t *testing.T) {
		ddl := append(append([]string(nil), baseDDL...),
			syncRow("gen-1", `[]`, "connector"))
		path := hostileV1Fixture(t, "badclaim.c1z", ddl...)
		store, err := NewStore(ctx, path, WithReadOnly(true))
		require.NoError(t, err)
		_, readErr := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
		require.Error(t, readErr)
		require.ErrorIs(t, readErr, c1zstore.ErrDataRejected)
		require.NoError(t, store.Close(ctx))
	})

	t.Run("malformed coverage bytes rejected", func(t *testing.T) {
		ddl := append(append([]string(nil), baseDDL...),
			syncRow("gen-1", `{not json`, "connector"))
		path := hostileV1Fixture(t, "badclaim.c1z", ddl...)
		store, err := NewStore(ctx, path, WithReadOnly(true))
		require.NoError(t, err)
		_, readErr := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
		require.Error(t, readErr)
		require.ErrorIs(t, readErr, c1zstore.ErrDataRejected)
		require.NoError(t, store.Close(ctx))
	})

	t.Run("absent marker stays unverified (control)", func(t *testing.T) {
		ddl := append(append([]string(nil), baseDDL...),
			syncRow("", ``, ""))
		path := hostileV1Fixture(t, "noclaim.c1z", ddl...)
		store, err := NewStore(ctx, path, WithReadOnly(true))
		require.NoError(t, err, "an ABSENT marker is legacy acceptance, never a rejection")
		defer func() { require.NoError(t, store.Close(ctx)) }()
		run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
		require.NoError(t, err)
		require.NotNil(t, run)
		require.False(t, run.IsVerified(),
			"absent marker must read as unverified")
	})

	t.Run("valid marker reads verified (control)", func(t *testing.T) {
		ddl := append(append([]string(nil), baseDDL...),
			syncRow("gen-1", `["I1","I5"]`, "connector"))
		path := hostileV1Fixture(t, "goodclaim.c1z", ddl...)
		store, err := NewStore(ctx, path, WithReadOnly(true))
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Close(ctx)) }()
		run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
		require.NoError(t, err)
		require.NotNil(t, run)
		require.True(t, run.IsVerified())
	})
}
