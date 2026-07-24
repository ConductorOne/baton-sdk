package pebble

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestGetGrantDiscoveredAtRoundTrip proves the stored per-grant
// discovered_at survives to the new GrantDiscoveredAtReader surface
// verbatim — the reader returns the value on disk, never a fresh
// now(). It seeds a record with a deterministic, clearly-past timestamp
// directly via the engine (bypassing the adapter's now() stamp), the
// same technique the StoreExpandedGrants discovered_at tests use.
func TestGetGrantDiscoveredAtRoundTrip(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, filepath.Join(t.TempDir(), "engine"))
	require.NoError(t, err)
	defer func() { _ = e.Close() }()

	// Compile-time contract is asserted in adapter.go; assert here too so
	// this test fails loudly if the capability is ever dropped.
	var _ connectorstore.GrantDiscoveredAtReader = e

	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	seeded := timestamppb.New(time.Date(2021, 6, 7, 8, 9, 10, 0, time.UTC))
	seed := v3.GrantRecord_builder{
		ExternalId: "g-1",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: canonicalTestEntID("ent-A"),
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user", ResourceId: "alice",
		}.Build(),
		DiscoveredAt: seeded,
	}.Build()
	require.NoError(t, e.PutGrantRecord(ctx, seed))

	got, found, err := e.GetGrantDiscoveredAt(ctx, "g-1")
	require.NoError(t, err)
	require.True(t, found, "seeded grant must be found")
	require.NotNil(t, got, "discovered_at must survive to the reader surface")
	require.Equal(t, seeded.AsTime(), got.AsTime(),
		"GetGrantDiscoveredAt must return the STORED discovered_at, not a fresh now()")
}

// TestGetGrantDiscoveredAtUnknownID confirms an unresolvable id is
// reported as found=false with a nil error and nil timestamp — "no such
// grant", not an error.
func TestGetGrantDiscoveredAtUnknownID(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, filepath.Join(t.TempDir(), "engine"))
	require.NoError(t, err)
	defer func() { _ = e.Close() }()

	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	got, found, err := e.GetGrantDiscoveredAt(ctx, "does-not-exist")
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, got)
}

// TestGetGrantDiscoveredAtNilStamp confirms a grant stored without a
// discovered_at reports found=true with a nil timestamp — the caller's
// signal to treat it as "unknown", distinct from "no such grant".
func TestGetGrantDiscoveredAtNilStamp(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, filepath.Join(t.TempDir(), "engine"))
	require.NoError(t, err)
	defer func() { _ = e.Close() }()

	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	seed := v3.GrantRecord_builder{
		ExternalId: "g-no-stamp",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: canonicalTestEntID("ent-A"),
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user", ResourceId: "bob",
		}.Build(),
	}.Build()
	require.Nil(t, seed.GetDiscoveredAt())
	require.NoError(t, e.PutGrantRecord(ctx, seed))

	got, found, err := e.GetGrantDiscoveredAt(ctx, "g-no-stamp")
	require.NoError(t, err)
	require.True(t, found, "a stored grant is found even with no discovered_at")
	require.Nil(t, got, "an unstamped grant returns a nil timestamp, not epoch zero")
}
