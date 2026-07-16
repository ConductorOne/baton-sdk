package pebble

// Table-driven harness for the recurring bug class of this branch:
// POST-MUTATION OBLIGATIONS SKIPPED ON THE ERROR PATH. Every mutation
// path that commits intermediate batches owes side effects the moment
// any commit lands — empty-keyspace fast-path disarm, bare-id lookup
// invalidation, manifest invalidation, stats-stash invalidation — and
// each has independently shipped a bug where the obligation only fired
// on full success (the replay pre-clear, the partial replay copy, the
// deferred-grant-stats stash).
//
// One table row per mutation path: force a failure AFTER the first
// landed chunk (via the replay batch var or the drop chunk hook) and
// assert every declared obligation fired anyway. A new mutation path
// gets a table row, not a bespoke test.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// obligationsCase drives one mutation path to a mid-flight failure.
// run must return the injected error (asserted non-nil); obligations
// assert the engine state the committed prefix demands.
type obligationsCase struct {
	name        string
	run         func(t *testing.T, ctx context.Context, cur, prev *Adapter, syncID string) error
	obligations func(t *testing.T, e *Engine, syncID string)
}

// plantDanglingGrants puts enough grants under one missing-referent
// shape to span at least one full drop chunk, all stamped with scopeA
// under a live manifest entry, so the chunk hook can fire mid-drop.
// varyEnt=true varies the entitlement per grant (one shared principal —
// the by_principal drop shape); varyEnt=false keeps one entitlement
// (the by_entitlement drop shape).
func plantDanglingGrants(t *testing.T, ctx context.Context, a *Adapter, varyEnt bool) {
	t.Helper()
	const total = dropBatchRows + 500
	scoped := sourcecache.WithScope(ctx, scopeA)
	const putChunk = 1000
	for start := 0; start < total; start += putChunk {
		end := min(start+putChunk, total)
		page := make([]*v2.Grant, 0, end-start)
		for i := start; i < end; i++ {
			if varyEnt {
				page = append(page, scGrant(fmt.Sprintf("member%05d", i), "dangler", false))
			} else {
				page = append(page, scGrant("member", fmt.Sprintf("u%05d", i), false))
			}
		}
		require.NoError(t, a.PutGrants(scoped, page...))
	}
	require.NoError(t, a.PebbleEngine().PutSourceCacheEntry(ctx, "grants", scopeA, "etag-v1"))
}

func requireStashInvalidated(t *testing.T, e *Engine, syncID string) {
	t.Helper()
	require.Nil(t, e.takeDeferredGrantStats(syncID),
		"committed drop chunks make the stashed grant counts stale; the stash must be invalidated even when the drop fails")
}

func requireScopeAInvalidated(t *testing.T, e *Engine) {
	t.Helper()
	rec, err := e.GetSourceCacheEntry(context.Background(), "grants", scopeA)
	require.NoError(t, err)
	require.True(t, rec.GetInvalidated(),
		"the scope losing rows must be invalidated with the first committed chunk")
}

func TestFailedMutationPathsFireObligations(t *testing.T) {
	injected := errors.New("injected mid-mutation failure")

	cases := []obligationsCase{
		{
			// Replay pre-clear: crashed-attempt residue deleted, then the
			// copy is cancelled. The empty-keyspace fast path must be
			// disarmed by the landed deletes alone.
			name: "replay-grants-preclear",
			run: func(t *testing.T, ctx context.Context, cur, prev *Adapter, _ string) error {
				e := cur.PebbleEngine()
				for i := 0; i < 3; i++ {
					plantScopedGrant(t, e, scopeA, fmt.Sprintf("u%d", i))
				}
				cctx := &failAfterNChecks{Context: ctx, remaining: 1}
				_, err := e.ReplaySourceCacheGrants(cctx, prev.PebbleEngine(), scopeA)
				return err
			},
			obligations: func(t *testing.T, e *Engine, _ string) {
				require.False(t, e.takeFreshGrantsEmpty(),
					"landed pre-clear deletes must disarm the grants fast path")
			},
		},
		{
			name: "replay-entitlements-preclear",
			run: func(t *testing.T, ctx context.Context, cur, prev *Adapter, _ string) error {
				e := cur.PebbleEngine()
				for i := 0; i < 3; i++ {
					plantScopedEntitlement(t, e, scopeA, fmt.Sprintf("g%d", i))
				}
				cctx := &failAfterNChecks{Context: ctx, remaining: 1}
				_, err := e.ReplaySourceCacheEntitlements(cctx, prev.PebbleEngine(), scopeA)
				return err
			},
			obligations: func(t *testing.T, e *Engine, _ string) {
				require.False(t, e.takeFreshEntitlementsEmpty(),
					"landed pre-clear deletes must disarm the entitlements fast path (and bump the bare-id generation)")
			},
		},
		{
			name: "replay-resources-preclear",
			run: func(t *testing.T, ctx context.Context, cur, prev *Adapter, _ string) error {
				e := cur.PebbleEngine()
				for i := 0; i < 3; i++ {
					plantScopedResource(t, e, scopeA, fmt.Sprintf("u%d", i))
				}
				cctx := &failAfterNChecks{Context: ctx, remaining: 1}
				_, err := e.ReplaySourceCacheResources(cctx, prev.PebbleEngine(), scopeA)
				return err
			},
			obligations: func(t *testing.T, e *Engine, _ string) {
				require.False(t, e.takeFreshResourcesEmpty(),
					"landed pre-clear deletes must disarm the resources fast path")
			},
		},
		{
			// I8's drop arm: one entitlement, > one chunk of grants. Fail
			// after the first committed chunk.
			name: "drop-grants-for-entitlement",
			run: func(t *testing.T, ctx context.Context, cur, prev *Adapter, syncID string) error {
				e := cur.PebbleEngine()
				plantDanglingGrants(t, ctx, cur, false)
				e.stashDeferredGrantStats(&deferredGrantStats{syncID: syncID, grants: 999_999})
				e.testDropChunkHook = func() error { return injected }
				t.Cleanup(func() { e.testDropChunkHook = nil })
				_, _, err := e.DeleteGrantsForEntitlement(ctx, "group:g1:member", "group", "g1")
				return err
			},
			obligations: func(t *testing.T, e *Engine, syncID string) {
				requireScopeAInvalidated(t, e)
				requireStashInvalidated(t, e, syncID)
			},
		},
		{
			// I9's drop arm: one principal, > one chunk of grants.
			name: "drop-grants-for-principal",
			run: func(t *testing.T, ctx context.Context, cur, prev *Adapter, syncID string) error {
				e := cur.PebbleEngine()
				plantDanglingGrants(t, ctx, cur, true)
				e.stashDeferredGrantStats(&deferredGrantStats{syncID: syncID, grants: 999_999})
				e.testDropChunkHook = func() error { return injected }
				t.Cleanup(func() { e.testDropChunkHook = nil })
				_, _, err := e.DeleteGrantsForPrincipal(ctx, "user", "dangler")
				return err
			},
			obligations: func(t *testing.T, e *Engine, syncID string) {
				requireScopeAInvalidated(t, e)
				requireStashInvalidated(t, e, syncID)
			},
		},
		{
			// I7's drop arm: entitlements under one resource. Obligation:
			// the bare-id keyspace note (observable via the fresh flag —
			// the delete mutated the keyspace, so the flag must be gone).
			name: "drop-entitlements-for-resource",
			run: func(t *testing.T, ctx context.Context, cur, prev *Adapter, _ string) error {
				e := cur.PebbleEngine()
				const total = dropBatchRows + 500
				const putChunk = 1000
				for start := 0; start < total; start += putChunk {
					end := min(start+putChunk, total)
					page := make([]*v2.Entitlement, 0, end-start)
					for i := start; i < end; i++ {
						page = append(page, v2.Entitlement_builder{
							Id: fmt.Sprintf("group:g1:member%05d", i),
							Resource: v2.Resource_builder{
								Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
							}.Build(),
						}.Build())
					}
					require.NoError(t, cur.PutEntitlements(sourcecache.WithScope(ctx, scopeA), page...))
				}
				require.NoError(t, e.PutSourceCacheEntry(ctx, "entitlements", scopeA, "etag-v1"))
				e.testDropChunkHook = func() error { return injected }
				t.Cleanup(func() { e.testDropChunkHook = nil })
				_, _, err := e.DeleteEntitlementsForResource(ctx, "group", "g1", 5)
				return err
			},
			obligations: func(t *testing.T, e *Engine, _ string) {
				rec, err := e.GetSourceCacheEntry(context.Background(), "entitlements", scopeA)
				require.NoError(t, err)
				require.True(t, rec.GetInvalidated(),
					"the scope losing entitlement rows must be invalidated with the first committed chunk")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			oldBatch := replayBatchRows
			replayBatchRows = 1
			t.Cleanup(func() { replayBatchRows = oldBatch })

			prev := newAdapter(t)
			_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			cur := newAdapter(t)
			syncID, err := cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)

			err = tc.run(t, ctx, cur, prev, syncID)
			require.Error(t, err, "the case must fail mid-mutation; a clean run proves nothing about error-path obligations")
			tc.obligations(t, cur.PebbleEngine(), syncID)
		})
	}
}
