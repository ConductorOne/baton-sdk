package pebble

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// grantWith builds a GrantRecord with explicit entitlement/principal/resource
// components so the test can exercise colon-bearing ids and varied families.
func grantWith(externalID, entRT, entRID, entID, prinRT, prinID string) *v3.GrantRecord {
	return v3.GrantRecord_builder{
		ExternalId: externalID,
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: entRT,
			ResourceId:     entRID,
			EntitlementId:  entID,
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: prinRT,
			ResourceId:     prinID,
		}.Build(),
	}.Build()
}

func dumpIndexKeys(t *testing.T, e *Engine, lo, hi []byte) []string {
	t.Helper()
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	require.NoError(t, err)
	defer iter.Close()
	var keys []string
	for iter.First(); iter.Valid(); iter.Next() {
		keys = append(keys, string(append([]byte(nil), iter.Key()...)))
	}
	require.NoError(t, iter.Error())
	return keys
}

// TestDeferredGrantIndexesMatchInline proves that the deferred index build
// (BATON_PEBBLE_DEFER_EXPANSION_INDEXES) produces byte-identical index keyspaces
// to the normal inline write path, across all grant index families — including
// colon-bearing entitlement/principal ids that stress the tuple codec.
func TestDeferredGrantIndexesMatchInline(t *testing.T) {
	ctx := context.Background()
	syncID := ksuid.New().String()

	// A varied set: distinct entitlements/resources/principals, several with
	// colons inside resource ids and principal ids.
	mk := func() []*v3.GrantRecord {
		return []*v3.GrantRecord{
			grantWith("github:repo:123:write:user:alice", "repo", "123", "github:repo:123:write", "user", "alice"),
			grantWith("github:repo:123:write:user:bob", "repo", "123", "github:repo:123:write", "user", "bob"),
			grantWith("g2", "group", "eng", "group:eng:member", "user", "carol:contractor"),
			grantWith("g3", "group", "eng:platform", "group:eng:platform:admin", "group", "oncall"),
			grantWith("g4", "app", "github", "app:github:read", "user", "dave"),
		}
	}

	// Entitlement records backing the grants' entitlements, so the deferred
	// build's entitlement->resource map can resolve by_entitlement_resource.
	// Their resource (type, id) must match each grant's entitlement ref.
	ents := []*v3.EntitlementRecord{
		v3.EntitlementRecord_builder{ExternalId: "github:repo:123:write", Resource: v3.ResourceRef_builder{ResourceTypeId: "repo", ResourceId: "123"}.Build()}.Build(),
		v3.EntitlementRecord_builder{ExternalId: "group:eng:member", Resource: v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "eng"}.Build()}.Build(),
		v3.EntitlementRecord_builder{ExternalId: "group:eng:platform:admin", Resource: v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "eng:platform"}.Build()}.Build(),
		v3.EntitlementRecord_builder{ExternalId: "app:github:read", Resource: v3.ResourceRef_builder{ResourceTypeId: "app", ResourceId: "github"}.Build()}.Build(),
	}

	control, _ := newTestEngine(t)
	require.NoError(t, control.SetCurrentSync(syncID))
	require.NoError(t, control.PutEntitlementRecords(ctx, ents...))
	require.NoError(t, control.PutExpandedGrantRecords(ctx, mk()))

	deferred, _ := newTestEngine(t)
	deferred.deferGrantIndexes = true
	require.NoError(t, deferred.SetCurrentSync(syncID))
	require.NoError(t, deferred.PutEntitlementRecords(ctx, ents...))
	require.NoError(t, deferred.PutExpandedGrantRecords(ctx, mk()))
	require.True(t, deferred.deferredIdxPending.Load(), "expansion write should mark a deferred rebuild pending")
	require.NoError(t, deferred.BuildDeferredGrantIndexes(ctx))

	families := []struct {
		name   string
		lo, hi []byte
	}{
		{"by_entitlement", GrantByEntitlementLowerBound(), GrantByEntitlementUpperBound()},
		{"by_entitlement_resource", GrantByEntitlementResourceLowerBound(), GrantByEntitlementResourceUpperBound()},
		{"by_principal", GrantByPrincipalLowerBound(), GrantByPrincipalUpperBound()},
		{"by_principal_resource_type", GrantByPrincipalResourceTypeLowerBound(), GrantByPrincipalResourceTypeUpperBound()},
	}
	for _, f := range families {
		want := dumpIndexKeys(t, control, f.lo, f.hi)
		got := dumpIndexKeys(t, deferred, f.lo, f.hi)
		require.Equal(t, want, got, "family %s: deferred build must match inline writes", f.name)
		require.NotEmpty(t, got, "family %s should have entries", f.name)
	}
}
