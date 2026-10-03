package dotc1z

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// TestSecurity_LegacyOldSchemaAcceptedAndReadsCorrectly is the acceptance
// control for the schema guard: an old schema (no expansion or
// needs_expansion columns, no modern sync_runs metadata columns, an inline
// GrantExpandable annotation inside a grant blob) must open read-only, read
// correct sync metadata and exact grant identities with the inline
// annotation stripped, and leave the envelope byte-unchanged. The guard
// rejects executable schema, never an older layout.
func TestSecurity_LegacyOldSchemaAcceptedAndReadsCorrectly(t *testing.T) {
	ctx := context.Background()

	// One ordinary grant and one expandable grant, each carrying its
	// GrantExpandable INLINE (the legacy shape the backfill migration
	// extracts into the side column).
	ordinary := v2.Grant_builder{
		Id: "grant-ordinary",
		Entitlement: v2.Entitlement_builder{
			Id: "ent-viewer",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
			}.Build(),
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		}.Build(),
	}.Build()

	expandable := v2.Grant_builder{
		Id: "grant-expandable",
		Entitlement: v2.Entitlement_builder{
			Id: "ent-member",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(),
			}.Build(),
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(),
		}.Build(),
	}.Build()
	expandable.SetAnnotations(annotations.New(&v2.GrantExpandable{
		EntitlementIds: []string{"ent-member-of-x"},
	}))

	mustMarshal := func(m proto.Message) []byte {
		t.Helper()
		b, err := proto.Marshal(m)
		require.NoError(t, err)
		return b
	}

	// The legacy DDL: old-shape grants table WITHOUT expansion/
	// needs_expansion columns, old-shape sync_runs WITHOUT the modern
	// metadata columns (linked_sync_id, ingest_invariant_*), one
	// finished full sync with grants_backfilled absent (pending), and
	// ALTER-table-free historical column sets.
	ddl := []string{
		`CREATE TABLE v1_resource_types (id integer primary key, external_id text not null, data blob not null, sync_id text not null, discovered_at datetime not null)`,
		`CREATE TABLE v1_resources (id integer primary key, resource_type_id text not null, external_id text not null, parent_resource_type_id text, parent_resource_id text,` +
			`data blob not null, sync_id text not null, discovered_at datetime not null)`,
		`CREATE TABLE v1_entitlements (id integer primary key, resource_type_id text not null, resource_id text not null, external_id text not null, data blob not null,` +
			`sync_id text not null, discovered_at datetime not null)`,
		`CREATE TABLE v1_grants (id integer primary key, resource_type_id text not null, resource_id text not null, entitlement_id text not null, principal_resource_type_id text not null,` +
			`principal_resource_id text not null, external_id text not null, data blob not null, sync_id text not null, discovered_at datetime not null)`,
		`CREATE TABLE v1_sync_runs (id integer primary key, sync_id text not null, started_at datetime not null, ended_at datetime, sync_token text not null,` +
			`sync_type text not null default 'full', parent_sync_id text not null default '', supports_diff integer not null default 0, grants_backfilled integer not null default 0, stats text)`,
		`CREATE TABLE v1_assets (id integer primary key, external_id text not null, content_type text not null, data blob not null, sync_id text not null, discovered_at datetime not null)`,
		`CREATE TABLE v1_connector_sessions (id integer primary key, sync_id text NOT NULL, key TEXT NOT NULL, value BLOB NOT NULL)`,
		// Historical partial/secondary indexes must survive the guard.
		`CREATE INDEX old_idx_grants_sync ON v1_grants (sync_id)`,
		// Rows.
		`INSERT INTO v1_sync_runs (sync_id, started_at, ended_at, sync_token, sync_type, grants_backfilled) VALUES ('legacy-sync', '2024-01-01T00:00:00Z', '2024-01-01T00:01:00Z',` +
			`'legacy-token', 'full', 0)`,
		`INSERT INTO v1_grants (id, resource_type_id, resource_id, entitlement_id,` +
			` principal_resource_type_id, principal_resource_id, external_id, data, sync_id, discovered_at)` +
			` VALUES (1, 'group', 'g1', 'ent-viewer', 'user', 'alice', 'grant-ordinary', X'` +
			hexEncode(mustMarshal(ordinary)) +
			`', 'legacy-sync', '2024-01-01T00:00:00Z')`,
		`INSERT INTO v1_grants (id, resource_type_id, resource_id, entitlement_id,` +
			` principal_resource_type_id, principal_resource_id, external_id, data, sync_id, discovered_at)` +
			` VALUES (2, 'group', 'g2', 'ent-member', 'user', 'bob', 'grant-expandable', X'` +
			hexEncode(mustMarshal(expandable)) +
			`', 'legacy-sync', '2024-01-01T00:00:00Z')`,
	}

	path := hostileV1Fixture(t, "legacy.c1z", ddl...)
	before := readSourceBytes(t, path)

	// Read-only open must accept the old layout.
	store, err := NewStore(ctx, path, WithReadOnly(true))
	require.NoError(t, err, "a fully-specified old-schema artifact must open read-only (legacy acceptance)")
	defer func() { require.NoError(t, store.Close(ctx)) }()

	// Correct sync metadata through the reader surface.
	syncsResp, err := store.ListSyncs(ctx, reader_v2.SyncsReaderServiceListSyncsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, syncsResp.GetSyncs(), 1)
	require.Equal(t, "legacy-sync", syncsResp.GetSyncs()[0].GetId())
	require.Equal(t, "legacy-token", syncsResp.GetSyncs()[0].GetSyncToken())
	require.Equal(t, "full", syncsResp.GetSyncs()[0].GetSyncType())

	// Exact grant identities.
	grantsResp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, grantsResp.GetList(), 2)
	ids := []string{grantsResp.GetList()[0].GetId(), grantsResp.GetList()[1].GetId()}
	require.ElementsMatch(t, []string{"grant-ordinary", "grant-expandable"}, ids)

	// The migration must have extracted the inline GrantExpandable
	// annotation into the side column: the plain read path re-attaches
	// nothing, so the listed grant must NOT carry the inline annotation.
	var expandableListed *v2.Grant
	for _, g := range grantsResp.GetList() {
		if g.GetId() == "grant-expandable" {
			expandableListed = g
		}
	}
	require.NotNil(t, expandableListed)
	require.False(t, hasGrantExpandable(expandableListed),
		"the inline GrantExpandable annotation must be stripped from the stored grant blob (plain ListGrants drops expansion topology)")

	// The envelope must be byte-for-byte unchanged by the read-only open
	// (normalization happens only in the private decoded working DB).
	require.Equal(t, before, readSourceBytes(t, path),
		"read-only open must not rewrite the original envelope")
}

// hexEncode renders b as uppercase hex for embedding in a SQL blob literal
// (X'...').
func hexEncode(b []byte) string {
	const hexdigits = "0123456789ABCDEF"
	out := make([]byte, 0, len(b)*2)
	for _, v := range b {
		out = append(out, hexdigits[v>>4], hexdigits[v&0x0f])
	}
	return string(out)
}

// TestSecurity_LegacyOldSchemaConvertsAndIsolates runs the same kind of
// old-schema artifact through the derived-output routes that reopen its
// catalog: ToPebble conversion and CopyIsolateSync must both pass the
// schema guard and preserve the grant identity.
func TestSecurity_LegacyOldSchemaConvertsAndIsolates(t *testing.T) {
	ctx := context.Background()

	mustMarshal := func(m proto.Message) []byte {
		t.Helper()
		b, err := proto.Marshal(m)
		require.NoError(t, err)
		return b
	}
	grant := v2.Grant_builder{
		Id: "grant-legacy",
		Entitlement: v2.Entitlement_builder{
			Id: "ent-legacy",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
			}.Build(),
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		}.Build(),
	}.Build()

	ddl := []string{
		`CREATE TABLE v1_resource_types (id integer primary key, external_id text not null, data blob not null, sync_id text not null, discovered_at datetime not null)`,
		`CREATE TABLE v1_resources (id integer primary key, resource_type_id text not null, external_id text not null, parent_resource_type_id text, parent_resource_id text,` +
			`data blob not null, sync_id text not null, discovered_at datetime not null)`,
		`CREATE TABLE v1_entitlements (id integer primary key, resource_type_id text not null, resource_id text not null, external_id text not null, data blob not null,` +
			`sync_id text not null, discovered_at datetime not null)`,
		`CREATE TABLE v1_grants (id integer primary key, resource_type_id text not null, resource_id text not null, entitlement_id text not null, principal_resource_type_id text not null,` +
			`principal_resource_id text not null, external_id text not null, data blob not null, sync_id text not null, discovered_at datetime not null)`,
		`CREATE TABLE v1_sync_runs (id integer primary key, sync_id text not null, started_at datetime not null, ended_at datetime, sync_token text not null,` +
			`sync_type text not null default 'full', parent_sync_id text not null default '', supports_diff integer not null default 0, grants_backfilled integer not null default 0, stats text)`,
		`CREATE TABLE v1_assets (id integer primary key, external_id text not null, content_type text not null, data blob not null, sync_id text not null, discovered_at datetime not null)`,
		`CREATE TABLE v1_connector_sessions (id integer primary key, sync_id text NOT NULL, key TEXT NOT NULL, value BLOB NOT NULL)`,
		`INSERT INTO v1_sync_runs (sync_id, started_at, ended_at, sync_token, sync_type, grants_backfilled) VALUES ('2YpKj9CDvFqPHLhMZWB4h0X1YyL', '2024-01-01T00:00:00Z',` +
			`'2024-01-01T00:01:00Z', 'legacy-token', 'full', 0)`,
		`INSERT INTO v1_grants (id, resource_type_id, resource_id, entitlement_id,` +
			` principal_resource_type_id, principal_resource_id, external_id, data, sync_id, discovered_at)` +
			` VALUES (1, 'group', 'g1', 'ent-legacy', 'user', 'alice', 'grant-legacy', X'` +
			hexEncode(mustMarshal(grant)) +
			`', '2YpKj9CDvFqPHLhMZWB4h0X1YyL', '2024-01-01T00:00:00Z')`,
	}
	path := hostileV1Fixture(t, "legacy.c1z", ddl...)

	f, err := NewC1ZFile(ctx, path)
	require.NoError(t, err, "writable open of the old-schema artifact must succeed")
	pebblePath := filepath.Join(t.TempDir(), "legacy.pebble.c1z")
	_, err = f.ToPebble(ctx, pebblePath, "2YpKj9CDvFqPHLhMZWB4h0X1YyL")
	require.NoError(t, err, "conversion of a legacy artifact must succeed")
	isoPath := filepath.Join(t.TempDir(), "legacy.iso.c1z")
	require.NoError(t, f.CopyIsolateSync(ctx, isoPath, ""), "isolating a legacy artifact must pass the schema guard")
	require.NoError(t, f.Close(ctx))

	for _, out := range []string{pebblePath, isoPath} {
		s, err := NewStore(ctx, out, WithReadOnly(true))
		require.NoError(t, err, out)
		grantsResp, err := s.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
		require.NoError(t, err)
		require.Len(t, grantsResp.GetList(), 1, out)
		require.Equal(t, "grant-legacy", grantsResp.GetList()[0].GetId(), out)
		require.NoError(t, s.Close(ctx))
	}
}
