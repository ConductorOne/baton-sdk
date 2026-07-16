package pebble

// Pins the pre-clear failure window of the replay functions: the
// resume-idempotency pre-clear commits intermediate batches, so a
// pre-clear that fails partway (error or cancellation) has already
// mutated the keyspace — the empty-keyspace fast-path flags must be
// disarmed and (for entitlements) the bare-id lookup map invalidated
// even though the replay itself returned an error. Before the fix, the
// entitlement invalidation defer was registered AFTER the pre-clear's
// error return, and the grants/resources defers ignored pre-clear
// deletes entirely.

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// failAfterNChecks is a context whose Err() reports Canceled after the
// first n calls succeed — a deterministic mid-loop cancellation for
// paths that poll ctx.Err() once per iteration.
type failAfterNChecks struct {
	context.Context
	remaining int
}

func (c *failAfterNChecks) Err() error {
	if c.remaining <= 0 {
		return context.Canceled
	}
	c.remaining--
	return nil
}

// plantScopedEntitlement writes a scope-stamped entitlement row and its
// scope index key DIRECTLY into the db, bypassing PutEntitlementRecords —
// simulating rows a crashed earlier replay attempt committed, without
// consuming the fresh-empty flag the way a live writer would.
func plantScopedEntitlement(t *testing.T, e *Engine, scope, rid string) {
	t.Helper()
	rec := v3.EntitlementRecord_builder{
		ExternalId:     "group:" + rid + ":member",
		Resource:       v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: rid}.Build(),
		SourceScopeKey: scope,
	}.Build()
	id, err := entitlementIdentityFromRecord(rec)
	require.NoError(t, err)
	val, err := marshalRecord(rec)
	require.NoError(t, err)
	require.NoError(t, e.db.Set(encodeEntitlementIdentityKey(id), val, nil))
	require.NoError(t, e.db.Set(encodeEntitlementBySourceScopeIndexKey(scope, id), nil, nil))
}

func plantScopedGrant(t *testing.T, e *Engine, scope, principalID string) {
	t.Helper()
	rec := v3.GrantRecord_builder{
		ExternalId: "group:g1:member:user:" + principalID,
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "group", ResourceId: "g1", EntitlementId: "group:g1:member",
		}.Build(),
		Principal:      v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: principalID}.Build(),
		SourceScopeKey: scope,
	}.Build()
	id, err := grantIdentityFromRecord(rec)
	require.NoError(t, err)
	val, err := marshalRecord(rec)
	require.NoError(t, err)
	require.NoError(t, e.db.Set(encodeGrantIdentityKey(id), val, nil))
	require.NoError(t, e.db.Set(encodeGrantBySourceScopeIndexKey(scope, id), nil, nil))
}

func plantScopedResource(t *testing.T, e *Engine, scope, rid string) {
	t.Helper()
	rec := v3.ResourceRecord_builder{
		ResourceTypeId: "user",
		ResourceId:     rid,
		SourceScopeKey: scope,
	}.Build()
	val, err := marshalRecord(rec)
	require.NoError(t, err)
	require.NoError(t, e.db.Set(encodeResourceKey("user", rid), val, nil))
	require.NoError(t, e.db.Set(encodeResourceBySourceScopeIndexKey(scope, "user", rid), nil, nil))
}

func TestReplayPreClearPartialFailureDisarmsFastPaths(t *testing.T) {
	ctx := context.Background()
	old := replayBatchRows
	replayBatchRows = 1
	t.Cleanup(func() { replayBatchRows = old })

	scope := "scope-preclear-test"

	// One shared, empty previous file: the copy loop has nothing to do;
	// everything under test happens in the pre-clear.
	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	t.Run("entitlements", func(t *testing.T) {
		cur := newAdapter(t)
		_, err := cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e := cur.PebbleEngine()
		for i := 0; i < 3; i++ {
			plantScopedEntitlement(t, e, scope, fmt.Sprintf("g%d", i))
		}
		// One iteration succeeds (its single-row batch commits), the
		// second sees the cancellation: a partial pre-clear.
		cctx := &failAfterNChecks{Context: ctx, remaining: 1}
		_, err = e.ReplaySourceCacheEntitlements(cctx, prev.PebbleEngine(), scope)
		require.Error(t, err, "the partial pre-clear must surface its failure")
		require.False(t, e.takeFreshEntitlementsEmpty(),
			"a pre-clear that committed deletes mutated the keyspace; the empty-keyspace fast path must be disarmed even on failure")
	})

	t.Run("grants", func(t *testing.T) {
		cur := newAdapter(t)
		_, err := cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e := cur.PebbleEngine()
		for i := 0; i < 3; i++ {
			plantScopedGrant(t, e, scope, fmt.Sprintf("u%d", i))
		}
		cctx := &failAfterNChecks{Context: ctx, remaining: 1}
		_, err = e.ReplaySourceCacheGrants(cctx, prev.PebbleEngine(), scope)
		require.Error(t, err)
		require.False(t, e.takeFreshGrantsEmpty(),
			"grants pre-clear deletes must disarm the fast path even on failure")
	})

	t.Run("resources", func(t *testing.T) {
		cur := newAdapter(t)
		_, err := cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e := cur.PebbleEngine()
		for i := 0; i < 3; i++ {
			plantScopedResource(t, e, scope, fmt.Sprintf("u%d", i))
		}
		cctx := &failAfterNChecks{Context: ctx, remaining: 1}
		_, err = e.ReplaySourceCacheResources(cctx, prev.PebbleEngine(), scope)
		require.Error(t, err)
		require.False(t, e.takeFreshResourcesEmpty(),
			"resources pre-clear deletes must disarm the fast path even on failure")
	})

	// Control: a replay that fails BEFORE anything commits leaves the
	// fast path armed — the disarm is keyed on landed mutations, not on
	// the mere attempt.
	t.Run("no mutation, no disarm", func(t *testing.T) {
		cur := newAdapter(t)
		_, err := cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e := cur.PebbleEngine()
		// Nothing planted: pre-clear iterates zero rows, copy loop
		// copies zero rows (prev is empty), no commits with content.
		_, err = e.ReplaySourceCacheEntitlements(ctx, prev.PebbleEngine(), scope)
		require.NoError(t, err)
		require.True(t, e.takeFreshEntitlementsEmpty(),
			"an empty replay must not burn the first-put fast path")
	})
}
