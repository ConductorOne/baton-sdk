package pebble

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

func lookupTestEnt(rt, rid, externalID string) *v3.EntitlementRecord {
	return v3.EntitlementRecord_builder{
		ExternalId: externalID,
		Resource:   v3.ResourceRef_builder{ResourceTypeId: rt, ResourceId: rid}.Build(),
	}.Build()
}

func lookupTestGrant(externalID, entRT, entRID, entID, principalRT, principalID string) *v3.GrantRecord {
	return v3.GrantRecord_builder{
		ExternalId: externalID,
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: entRT,
			ResourceId:     entRID,
			EntitlementId:  entID,
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: principalRT,
			ResourceId:     principalID,
		}.Build(),
	}.Build()
}

// TestBareIDEntitlementLookupExactlyOne pins the exactly-one rule for
// entitlement bare-id resolution: an opaque id resolves through the lazy
// record map by byte-equality; an id shared by two entitlements on
// DIFFERENT resources — representable only in the new layout; legacy keyed
// by the string and silently overwrote — is a loud ambiguity error for
// both reads and deletes, never a guess.
func TestBareIDEntitlementLookupExactlyOne(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	require.NoError(t, e.SetCurrentSync(ksuid.New().String()))

	require.NoError(t, e.PutEntitlementRecords(ctx,
		lookupTestEnt("app", "github", "opaque-ent"),
		lookupTestEnt("app", "github", "app:github:read"), // SDK-shaped, stripped form
		lookupTestEnt("group", "eng", "shared-id"),
		lookupTestEnt("group", "sales", "shared-id"), // same string, different resource
	))

	// Opaque id: lazy-map exact match.
	got, err := e.GetEntitlementRecord(ctx, "opaque-ent")
	require.NoError(t, err)
	require.Equal(t, "opaque-ent", got.GetExternalId())

	// SDK-shaped id round-trips byte-identical through the stripped encoding.
	got, err = e.GetEntitlementRecord(ctx, "app:github:read")
	require.NoError(t, err)
	require.Equal(t, "app:github:read", got.GetExternalId())

	// Zero matches → NotFound.
	_, err = e.GetEntitlementRecord(ctx, "no-such-id")
	require.ErrorIs(t, err, pebble.ErrNotFound)

	// Two matches → explicit ambiguity, for reads AND deletes.
	_, err = e.GetEntitlementRecord(ctx, "shared-id")
	require.ErrorIs(t, err, ErrAmbiguousExternalID)
	err = e.DeleteEntitlementRecord(ctx, "shared-id")
	require.ErrorIs(t, err, ErrAmbiguousExternalID, "an ambiguous string must never guess a delete")

	// Both ambiguous rows are intact.
	var n int
	require.NoError(t, e.IterateEntitlements(ctx, func(r *v3.EntitlementRecord) bool {
		if r.GetExternalId() == "shared-id" {
			n++
		}
		return true
	}))
	require.Equal(t, 2, n, "ambiguous delete must not remove either row")
}

// TestBareIDEntitlementLookupInvalidation pins the generation-counter
// invalidation: a lookup builds the map, a subsequent entitlement write
// invalidates it, and the next lookup sees the new row.
func TestBareIDEntitlementLookupInvalidation(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	require.NoError(t, e.SetCurrentSync(ksuid.New().String()))

	require.NoError(t, e.PutEntitlementRecords(ctx, lookupTestEnt("app", "github", "first")))
	_, err := e.GetEntitlementRecord(ctx, "first")
	require.NoError(t, err, "build the map")
	_, err = e.GetEntitlementRecord(ctx, "second")
	require.ErrorIs(t, err, pebble.ErrNotFound)

	require.NoError(t, e.PutEntitlementRecords(ctx, lookupTestEnt("app", "github", "second")))
	got, err := e.GetEntitlementRecord(ctx, "second")
	require.NoError(t, err, "write must invalidate the cached map")
	require.Equal(t, "second", got.GetExternalId())

	require.NoError(t, e.DeleteEntitlementRecord(ctx, "first"))
	_, err = e.GetEntitlementRecord(ctx, "first")
	require.ErrorIs(t, err, pebble.ErrNotFound, "delete must invalidate the cached map")
}

// TestBareIDGrantLookupExactlyOne pins grant bare-id resolution: the SDK
// concat shape resolves by candidate probing; a probe hit only counts when
// the row's stored public id equals the query; a connector-custom id with
// no concat shape is NotFound (no scan-sized fallback exists for grants);
// two rows whose public ids equal one string are a loud ambiguity error.
func TestBareIDGrantLookupExactlyOne(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	require.NoError(t, e.SetCurrentSync(ksuid.New().String()))

	require.NoError(t, e.PutEntitlementRecords(ctx,
		lookupTestEnt("app", "github", "opaque-ent"),
	))

	// SDK-shaped: stored external id IS the concat.
	sdkID := "app:github:read:user:alice"
	require.NoError(t, e.PutGrantRecord(ctx,
		lookupTestGrant(sdkID, "app", "github", "app:github:read", "user", "alice")))
	got, err := e.GetGrantRecord(ctx, sdkID)
	require.NoError(t, err)
	require.Equal(t, sdkID, got.GetExternalId())

	// Opaque entitlement id: the concat resolves through the entitlement
	// record map, not the direct byte-split.
	opaqueConcat := "opaque-ent:user:bob"
	require.NoError(t, e.PutGrantRecord(ctx,
		lookupTestGrant(opaqueConcat, "app", "github", "opaque-ent", "user", "bob")))
	got, err = e.GetGrantRecord(ctx, opaqueConcat)
	require.NoError(t, err)
	require.Equal(t, "bob", got.GetPrincipal().GetResourceId())

	// Connector-custom id (no concat shape): resolves via the stored-id
	// scan of last resort — SQLite keyed rows by this id, so string reads
	// must find it for reader parity.
	require.NoError(t, e.PutGrantRecord(ctx,
		lookupTestGrant("grant-42", "app", "github", "app:github:write", "user", "carol")))
	got, err = e.GetGrantRecord(ctx, "grant-42")
	require.NoError(t, err)
	require.Equal(t, "carol", got.GetPrincipal().GetResourceId(),
		"custom stored id must resolve via the stored-id scan fallback")
	// Its concat reconstruction does not address it: the probe hit is not
	// counted (stored id differs from the query), and the scan fallback
	// matches stored ids only — no row STORES the concat string.
	_, err = e.GetGrantRecord(ctx, "app:github:write:user:carol")
	require.ErrorIs(t, err, pebble.ErrNotFound,
		"a row with a custom stored id is addressed by that id, not its concat")

	// Probe-first precedence: a row whose custom STORED id happens to equal
	// another row's concat never shadows it — the combinatorial probe
	// resolves the SDK-shaped row outright (exactly one probe hit), and the
	// stored-id scan runs only when every probe misses. The custom-id row
	// stays addressable by refs.
	require.NoError(t, e.PutGrantRecord(ctx,
		lookupTestGrant(sdkID, "group", "eng", "group:eng:member", "user", "dave")))
	got, err = e.GetGrantRecord(ctx, sdkID)
	require.NoError(t, err)
	require.Equal(t, "alice", got.GetPrincipal().GetResourceId(),
		"concat query addresses the SDK-shaped row; custom-id rows are refs-only")

	// TRUE ambiguity: two SDK-shaped rows whose concats are byte-equal —
	// entitlements on different resources sharing an external-id string.
	dup := "shared-ent:user:erin"
	require.NoError(t, e.PutEntitlementRecords(ctx,
		lookupTestEnt("team", "red", "shared-ent"),
		lookupTestEnt("team", "blue", "shared-ent"),
	))
	require.NoError(t, e.PutGrantRecord(ctx,
		lookupTestGrant(dup, "team", "red", "shared-ent", "user", "erin")))
	require.NoError(t, e.PutGrantRecord(ctx,
		lookupTestGrant(dup, "team", "blue", "shared-ent", "user", "erin")))
	_, err = e.GetGrantRecord(ctx, dup)
	require.ErrorIs(t, err, ErrAmbiguousExternalID)
	err = e.DeleteGrantRecord(ctx, dup)
	require.ErrorIs(t, err, ErrAmbiguousExternalID, "an ambiguous string must never guess a delete")
}

// TestDeleteGrantByIdentityRefsIsExact pins the sync-internal delete path:
// refs address exactly one row regardless of what public id string it
// carries, deleting a missing row is a no-op, and rows that merely share
// the ambiguous public id are untouched.
func TestDeleteGrantByIdentityRefsIsExact(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	require.NoError(t, e.SetCurrentSync(ksuid.New().String()))

	shared := "app:github:read:user:alice"
	a := lookupTestGrant(shared, "app", "github", "app:github:read", "user", "alice")
	b := lookupTestGrant(shared, "group", "eng", "group:eng:member", "user", "dave")
	require.NoError(t, e.PutGrantRecord(ctx, a))
	require.NoError(t, e.PutGrantRecord(ctx, b))

	// The string is ambiguous, but refs are not.
	require.NoError(t, e.DeleteGrantByIdentityRefs(ctx, a))
	var survivors []string
	require.NoError(t, e.IterateGrants(ctx, func(r *v3.GrantRecord) bool {
		survivors = append(survivors, r.GetEntitlement().GetEntitlementId())
		return true
	}))
	require.Equal(t, []string{"group:eng:member"}, survivors, "refs delete removes exactly its row")

	// Delete of a non-existent identity is a no-op.
	require.NoError(t, e.DeleteGrantByIdentityRefs(ctx, a))

	// Incomplete refs are an error, never a bare-id fallback.
	err := e.DeleteGrantByIdentityRefs(ctx, v3.GrantRecord_builder{ExternalId: shared}.Build())
	require.Error(t, err)
}
