package dotc1z_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestGrantDiscoveredAtReaderAcrossEngines proves the additive
// GrantDiscoveredAtReader capability behaves correctly on BOTH engines
// through the public store surface:
//
//   - Pebble implements the capability and returns the grant's stored
//     discovered_at. The value is stamped once at write and is stable
//     across reads (a fresh now() on every call would not be), and it is
//     surfaced even though the v2.Grant translation drops the field.
//   - SQLite (deprecated) does not implement the capability: the type
//     assertion fails, which callers must treat as "discovered_at
//     unavailable from this store" and fall back — never an error.
func TestGrantDiscoveredAtReaderAcrossEngines(t *testing.T) {
	ctx := context.Background()

	const grantID = "grant-alice-member"

	seed := func(t *testing.T, store connectorstore.Writer) {
		t.Helper()
		_, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, store.PutResourceTypes(ctx,
			v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
			v2.ResourceType_builder{Id: "app", DisplayName: "App", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_APP}}.Build(),
		))
		appRes := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "app", Resource: "gh"}.Build(),
		}.Build()
		alice := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		}.Build()
		require.NoError(t, store.PutResources(ctx, appRes, alice))
		ent := v2.Entitlement_builder{Id: "ent-A", Resource: appRes, Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION, Slug: "A"}.Build()
		require.NoError(t, store.PutEntitlements(ctx, ent))
		grant := v2.Grant_builder{
			Id:          grantID,
			Entitlement: ent,
			Principal:   alice,
		}.Build()
		require.NoError(t, store.PutGrants(ctx, grant))
		require.NoError(t, store.EndSync(ctx))
	}

	openStore := func(t *testing.T, engine c1zstore.Engine) connectorstore.Writer {
		t.Helper()
		path := filepath.Join(t.TempDir(), string(engine)+".c1z")
		store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
		require.NoError(t, err)
		seed(t, store)
		return store
	}

	t.Run("pebble returns stored discovered_at", func(t *testing.T) {
		store := openStore(t, c1zstore.EnginePebble)
		defer func() { _ = store.Close(ctx) }()

		reader, ok := store.(connectorstore.GrantDiscoveredAtReader)
		require.True(t, ok, "Pebble store must implement GrantDiscoveredAtReader")

		got, found, err := reader.GetGrantDiscoveredAt(ctx, grantID)
		require.NoError(t, err)
		require.True(t, found, "the seeded grant must be found")
		require.NotNil(t, got, "Pebble must surface a stored discovered_at")

		// Stable across reads: a stored value round-trips identically,
		// whereas a fabricated now() would differ between calls.
		again, found2, err := reader.GetGrantDiscoveredAt(ctx, grantID)
		require.NoError(t, err)
		require.True(t, found2)
		require.Equal(t, got.AsTime(), again.AsTime(),
			"discovered_at must be the STORED value, stable across reads, not a fresh now()")

		// Unknown id is a clean not-found, never an error.
		_, found3, err := reader.GetGrantDiscoveredAt(ctx, "no-such-grant")
		require.NoError(t, err)
		require.False(t, found3)
	})

	t.Run("sqlite cleanly reports unavailable", func(t *testing.T) {
		store := openStore(t, c1zstore.EngineSQLite)
		defer func() { _ = store.Close(ctx) }()

		_, ok := store.(connectorstore.GrantDiscoveredAtReader)
		require.False(t, ok,
			"deprecated SQLite store must NOT implement GrantDiscoveredAtReader; "+
				"callers fall back to 'discovered_at unavailable'")
	})
}
