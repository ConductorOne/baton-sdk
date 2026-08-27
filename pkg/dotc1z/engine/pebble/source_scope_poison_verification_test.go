package pebble

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// CO-015 row-partition poison verification.
//
// These tests were written BEFORE the poison mechanism was implemented and
// pin the target contract: any mutation acting for scope S that removes a
// row from scope X's stamped set (X != S, X != "") durably poisons
// (row_kind, X). A poisoned scope is refused as a replay source at
// preflight — the error contains "poisoned" — before any destination
// mutation. Scopes acting on themselves (their own tombstone paths) never
// self-poison, and disjoint scopes are never poisoned (no false positives).
//
// Without poison, every "refused" case below is a SILENT replay of a
// shrunken row set: restamps and deletes clean the loser's index entries,
// and CO-014's seal counts the post-damage state, so index, stamps, and
// sealed count are all self-consistent with the damaged partition. Only a
// marker staged at mutation time can catch it.

// TestVerificationPoisonCrossScopeRestampOrders is the interleaving-order
// matrix: whichever scope loses a row to a later cross-scope write must be
// poisoned under every emission order, and only damaged scopes are
// poisoned.
func TestVerificationPoisonCrossScopeRestampOrders(t *testing.T) {
	// Distinct principals give distinct grant identities; the same
	// (entitlement, principal) re-put under another scope is a restamp of
	// the same row.
	for _, tc := range []struct {
		name string
		// each step emits the named row under the named scope ("" =
		// unscoped put, which clears the stamp).
		steps []struct{ row, scope string }
		// scopes expected poisoned (replay refused) / clean (replay ok,
		// with expected surviving row count).
		poisoned []string
		clean    map[string]int64
	}{
		{
			name: "a-then-b",
			steps: []struct{ row, scope string }{
				{"r1", "scope-a"}, {"r1", "scope-b"},
			},
			poisoned: []string{"scope-a"},
			clean:    map[string]int64{"scope-b": 1},
		},
		{
			name: "b-then-a",
			steps: []struct{ row, scope string }{
				{"r1", "scope-b"}, {"r1", "scope-a"},
			},
			poisoned: []string{"scope-b"},
			clean:    map[string]int64{"scope-a": 1},
		},
		{
			name: "mixed-orders-poison-both",
			steps: []struct{ row, scope string }{
				// r1: A then B (A loses); r2: B then A (B loses). A third
				// disjoint scope C stays clean throughout.
				{"r1", "scope-a"}, {"r2", "scope-b"},
				{"r1", "scope-b"}, {"r2", "scope-a"},
				{"r3", "scope-c"},
			},
			poisoned: []string{"scope-a", "scope-b"},
			clean:    map[string]int64{"scope-c": 1},
		},
		{
			name: "bounce-a-b-a-poisons-both",
			steps: []struct{ row, scope string }{
				// A's loss is observed at the A->B step, B's at B->A. A
				// ends up owning the row; poisoning A anyway is the
				// documented conservative outcome.
				{"r1", "scope-a"}, {"r1", "scope-b"}, {"r1", "scope-a"},
			},
			poisoned: []string{"scope-a", "scope-b"},
			clean:    map[string]int64{},
		},
		{
			name: "restamp-to-unscoped",
			steps: []struct{ row, scope string }{
				// An unscoped overwrite clears the stamp: scope-a lost the
				// row to an actor outside every scope (reconciliation's
				// same-identity rewrite shape).
				{"r1", "scope-a"}, {"r1", ""},
			},
			poisoned: []string{"scope-a"},
			clean:    map[string]int64{},
		},
		{
			name: "disjoint-scopes-no-false-positive",
			steps: []struct{ row, scope string }{
				{"r1", "scope-a"}, {"r2", "scope-b"},
			},
			poisoned: nil,
			clean:    map[string]int64{"scope-a": 1, "scope-b": 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			prev := newAdapter(t)
			_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)

			for _, step := range tc.steps {
				putCtx := ctx
				if step.scope != "" {
					putCtx = sourcecache.WithScope(ctx, step.scope)
				}
				require.NoError(t, prev.PutGrants(putCtx, scGrant("member", step.row, false)))
			}

			sealScopes := make([]string, 0, len(tc.poisoned)+len(tc.clean))
			sealScopes = append(sealScopes, tc.poisoned...)
			for scope := range tc.clean {
				sealScopes = append(sealScopes, scope)
			}
			sealReplaySource(ctx, t, prev.PebbleEngine(), sourcecache.RowKindGrants, sealScopes...)

			for _, scope := range tc.poisoned {
				dst := newAdapter(t)
				_, err := dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				before := dumpKeyRangeTest(t, dst.PebbleEngine(), nil, nil)
				_, err = dst.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), scope)
				require.ErrorContains(t, err, "poisoned",
					"scope %q lost a row to a cross-scope write and must be refused as a replay source", scope)
				require.Equal(t, before, dumpKeyRangeTest(t, dst.PebbleEngine(), nil, nil),
					"poison refusal must precede any destination mutation")
			}
			for scope, wantRows := range tc.clean {
				dst := newAdapter(t)
				_, err := dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				res, err := dst.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), scope)
				require.NoError(t, err, "undamaged scope %q must remain replayable", scope)
				require.Equal(t, wantRows, res.Rows)
			}
		})
	}
}

// TestVerificationPoisonAllKindRestampAndUnscopedDelete pins the two loss
// events for every row kind: a cross-scope restamp poisons the old scope,
// and an unscoped maintenance delete (DeleteResourceRecord /
// DeleteEntitlementRecord / DeleteGrantRecord — the path external-principal
// reconciliation reaches) poisons the deleted row's scope.
func TestVerificationPoisonAllKindRestampAndUnscopedDelete(t *testing.T) {
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind)+"/restamp", func(t *testing.T) {
			ctx := t.Context()
			prev := newAdapter(t)
			_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			driver := newSourceScopeMutationDriver(t, prev, kind)
			require.NoError(t, driver.put(ctx, "scope-a"))
			require.NoError(t, driver.put(ctx, "scope-b"))
			sealReplaySource(ctx, t, prev.PebbleEngine(), kind, "scope-a", "scope-b")

			dst := newAdapter(t)
			_, err = dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			_, err = driver.replay(ctx, dst.PebbleEngine(), prev.PebbleEngine(), "scope-a")
			require.ErrorContains(t, err, "poisoned")
			res, err := driver.replay(ctx, dst.PebbleEngine(), prev.PebbleEngine(), "scope-b")
			require.NoError(t, err)
			require.Equal(t, int64(1), res.Rows)
		})
		t.Run(string(kind)+"/unscoped-delete", func(t *testing.T) {
			ctx := t.Context()
			prev := newAdapter(t)
			_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			driver := newSourceScopeMutationDriver(t, prev, kind)
			require.NoError(t, driver.put(ctx, "scope-a"))
			require.NoError(t, driver.delete(ctx))
			sealReplaySource(ctx, t, prev.PebbleEngine(), kind, "scope-a")

			dst := newAdapter(t)
			_, err = dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			_, err = driver.replay(ctx, dst.PebbleEngine(), prev.PebbleEngine(), "scope-a")
			require.ErrorContains(t, err, "poisoned",
				"an unscoped delete removed scope-a's row; a silent empty replay would be data loss")
		})
	}
}

// TestVerificationPoisonCrossScopeCanonicalTombstone pins the acting-scope
// contract on the canonical-ID tombstone path: the same bounded delete
// poisons the row's scope when acting for a DIFFERENT scope and never
// self-poisons when acting for the row's own scope. This is the store-level
// DeleteSourceCacheRows shape with the acting scope threaded through.
func TestVerificationPoisonCrossScopeCanonicalTombstone(t *testing.T) {
	for _, tc := range []struct {
		name        string
		actingScope string
		poisoned    bool
	}{
		{name: "cross-scope-poisons", actingScope: "scope-b", poisoned: true},
		{name: "own-scope-does-not", actingScope: "scope-a", poisoned: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			prev := newAdapter(t)
			_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			doomed := scGrant("member", "alice", false)
			kept := scGrant("member", "bob", false)
			require.NoError(t, prev.PutGrants(sourcecache.WithScope(ctx, "scope-a"), doomed, kept))
			require.NoError(t, prev.PebbleEngine().DeleteGrantRecordsBounded(ctx, []string{doomed.GetId()}, tc.actingScope))
			sealReplaySource(ctx, t, prev.PebbleEngine(), sourcecache.RowKindGrants, "scope-a")

			dst := newAdapter(t)
			_, err = dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			res, err := dst.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), "scope-a")
			if tc.poisoned {
				require.ErrorContains(t, err, "poisoned",
					"a canonical-ID tombstone acting for another scope removed scope-a's row")
				return
			}
			require.NoError(t, err, "a scope's own canonical-ID tombstones must not poison it")
			require.Equal(t, int64(1), res.Rows)
		})
	}
}

// TestVerificationPoisonScopedTombstonesDoNotSelfPoison pins the
// exemption: a scope shrinking ITSELF through the scoped tombstone paths
// is the legitimate delta-shrink flow and must stay replayable.
func TestVerificationPoisonScopedTombstonesDoNotSelfPoison(t *testing.T) {
	ctx := t.Context()

	t.Run("grants-by-principal", func(t *testing.T) {
		prev := newAdapter(t)
		_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, prev.PutGrants(sourcecache.WithScope(ctx, "scope-a"),
			scGrant("member", "alice", false), scGrant("member", "bob", false)))
		deleted, err := prev.PebbleEngine().DeleteGrantsByPrincipalsInScope(ctx, "scope-a", map[string]struct{}{"alice": {}})
		require.NoError(t, err)
		require.Equal(t, int64(1), deleted)
		sealReplaySource(ctx, t, prev.PebbleEngine(), sourcecache.RowKindGrants, "scope-a")

		dst := newAdapter(t)
		_, err = dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		res, err := dst.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), "scope-a")
		require.NoError(t, err, "a scope's own tombstones must not poison it")
		require.Equal(t, int64(1), res.Rows)
	})

	t.Run("resources-by-id", func(t *testing.T) {
		prev := newAdapter(t)
		_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
		u2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build()}.Build()
		require.NoError(t, prev.PutResources(sourcecache.WithScope(ctx, "scope-a"), u1, u2))
		deleted, err := prev.PebbleEngine().DeleteResourcesByIDsInScope(ctx, "scope-a", map[string]struct{}{"u1": {}})
		require.NoError(t, err)
		require.Equal(t, int64(1), deleted)
		sealReplaySource(ctx, t, prev.PebbleEngine(), sourcecache.RowKindResources, "scope-a")

		dst := newAdapter(t)
		_, err = dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		res, err := dst.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
		require.NoError(t, err)
		require.Equal(t, int64(1), res.Rows)
	})
}

// TestVerificationPoisonReconciliationShapeRefusesReplay is the
// feature-intersection pin (CO-016): external-principal reconciliation
// deletes a scope's match-annotated placeholder grants (unscoped, by
// identity) and re-issues derived grants unscoped. The grant scope must be
// poisoned — without poison this replays a silently shrunken set, because
// the seal counted the post-reconciliation state and everything is
// self-consistent. Resource and entitlement scopes emitted by the same
// sync stay replayable: the blast radius is the placeholder-emitting
// grant scope only.
func TestVerificationPoisonReconciliationShapeRefusesReplay(t *testing.T) {
	ctx := t.Context()
	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// The scope's fetch: a resource, an entitlement, and two placeholder
	// grants, all stamped scope-a.
	scoped := sourcecache.WithScope(ctx, "scope-a")
	group := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	ent := v2.Entitlement_builder{Id: "group:g1:member", Resource: group}.Build()
	require.NoError(t, prev.PutResources(scoped, group))
	require.NoError(t, prev.PutEntitlements(scoped, ent))
	placeholderAlice := scGrant("member", "placeholder-alice", false)
	placeholderBob := scGrant("member", "placeholder-bob", false)
	require.NoError(t, prev.PutGrants(scoped, placeholderAlice, placeholderBob))

	// Reconciliation's shape: delete the placeholders by identity with no
	// acting scope, then write derived re-issued grants unscoped.
	require.NoError(t, prev.PebbleEngine().DeleteGrantRecord(ctx, placeholderAlice.GetId()))
	require.NoError(t, prev.PebbleEngine().DeleteGrantRecord(ctx, placeholderBob.GetId()))
	require.NoError(t, prev.PutGrants(ctx,
		scGrant("member", "matched-alice", false), scGrant("member", "matched-bob", false)))

	sealReplaySourceMulti(ctx, t, prev.PebbleEngine(), map[sourcecache.RowKind][]string{
		sourcecache.RowKindResources:    {"scope-a"},
		sourcecache.RowKindEntitlements: {"scope-a"},
		sourcecache.RowKindGrants:       {"scope-a"},
	})

	dst := newAdapter(t)
	_, err = dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	_, err = dst.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), "scope-a")
	require.ErrorContains(t, err, "poisoned",
		"reconciliation deleted scope-a's placeholder grants; replay would silently drop them")

	res, err := dst.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err, "reconciliation never touches resource scopes")
	require.Equal(t, int64(1), res.Rows)
	res, err = dst.PebbleEngine().ReplaySourceCacheEntitlements(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err, "reconciliation never touches entitlement scopes")
	require.Equal(t, int64(1), res.Rows)
}

// TestVerificationPoisonSurvivesReopenAndInvalidationDropsIt pins the
// marker's lifecycle boundaries: poison is a durable key (it must survive
// the artifact being closed and reopened — the exact path a next sync
// takes to its previous c1z), and the compaction replay-state invalidation
// wipes it together with the manifest entries it qualifies (a store with
// no validators has nothing left for poison to protect).
func TestVerificationPoisonSurvivesReopenAndInvalidationDropsIt(t *testing.T) {
	ctx := t.Context()
	dbDir := filepath.Join(t.TempDir(), "db")
	e, err := Open(context.Background(), dbDir)
	require.NoError(t, err)
	closed := false
	defer func() {
		if !closed {
			_ = e.Close()
		}
	}()
	a := NewAdapter(e)
	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-a"), scGrant("member", "r1", false)))
	// Cross-scope restamp: poisons scope-a.
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-b"), scGrant("member", "r1", false)))
	sealReplaySource(ctx, t, e, sourcecache.RowKindGrants, "scope-a", "scope-b")
	require.NoError(t, e.Close())
	closed = true

	re, err := Open(context.Background(), dbDir)
	require.NoError(t, err)
	defer func() { require.NoError(t, re.Close()) }()

	poisoned, err := re.SourceCachePoisoned(ctx, string(sourcecache.RowKindGrants), "scope-a")
	require.NoError(t, err)
	require.True(t, poisoned, "poison must survive close/reopen")
	dst := newAdapter(t)
	_, err = dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	_, err = dst.PebbleEngine().ReplaySourceCacheGrants(ctx, re, "scope-a")
	require.ErrorContains(t, err, "poisoned")
	res, err := dst.PebbleEngine().ReplaySourceCacheGrants(ctx, re, "scope-b")
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Rows)

	require.NoError(t, re.InvalidateSourceCacheReplayState(ctx, false))
	poisoned, err = re.SourceCachePoisoned(ctx, string(sourcecache.RowKindGrants), "scope-a")
	require.NoError(t, err)
	require.False(t, poisoned, "replay-state invalidation wipes the whole source-cache family, poison included")
}

// TestVerificationPoisonEventsAreLogged pins the CO-015 telemetry
// contract directly: staging a poison marker logs scope, kind, and cause
// after the batch commits (so a perpetually-cold scope is diagnosable
// from logs), one batch poisoning one scope many times logs once, a
// LATER batch re-poisoning the same scope logs nothing new (batch-level
// dedup cannot span re-minted batches, so the engine observer dedups per
// (kind, scope) per open — a persistently mis-partitioned connector must
// not warn once per 10k-row chunk), and a FAILED commit delivers no
// event and leaves no durable marker — delivery is strictly post-commit.
func TestVerificationPoisonEventsAreLogged(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	openCtx := ctxzap.ToContext(context.Background(), zap.New(core))
	dbDir := filepath.Join(t.TempDir(), "db")
	e, err := Open(openCtx, dbDir)
	require.NoError(t, err)
	defer func() { require.NoError(t, e.Close()) }()

	ctx := t.Context()
	a := NewAdapter(e)
	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	// One batch restamps TWO of scope-a's rows: one poison event, one line.
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-a"),
		scGrant("member", "r1", false), scGrant("member", "r2", false)))
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-b"),
		scGrant("member", "r1", false), scGrant("member", "r2", false)))

	entries := logs.FilterFieldKey("scope_key").All()
	require.Len(t, entries, 1, "one batch poisoning one scope must log exactly once")
	fields := entries[0].ContextMap()
	require.Equal(t, "grants", fields["row_kind"])
	require.Equal(t, "scope-a", fields["scope_key"])
	require.Equal(t, "cross-scope restamp", fields["cause"])

	// Failed-commit half: this restamp (b -> c) stages poison for scope-b,
	// but the injected commit failure must suppress both the log line and
	// the durable marker.
	injected := errors.New("injected commit failure")
	e.db.SetRecordCommitTestHook(func() error { return injected })
	err = a.PutGrants(sourcecache.WithScope(ctx, "scope-c"),
		scGrant("member", "r1", false))
	require.ErrorIs(t, err, injected)
	e.db.SetRecordCommitTestHook(nil)
	require.Len(t, logs.FilterFieldKey("scope_key").All(), 1,
		"a failed commit must deliver no poison event")
	poisoned, err := e.SourceCachePoisoned(ctx, string(sourcecache.RowKindGrants), "scope-b")
	require.NoError(t, err)
	require.False(t, poisoned, "a failed commit must leave no durable poison marker")

	// Cross-batch dedup half: a LATER batch (fresh RecordBatch, so the
	// batch-level dedup set is gone) poisons scope-a again via a new
	// row's a -> b restamp. The marker write is idempotent; the observer
	// must not log a second line for the same (kind, scope).
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-a"),
		scGrant("member", "r3", false)))
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-b"),
		scGrant("member", "r3", false)))
	poisoned, err = e.SourceCachePoisoned(ctx, string(sourcecache.RowKindGrants), "scope-a")
	require.NoError(t, err)
	require.True(t, poisoned, "the re-poisoning batch must still land the durable marker")
	require.Len(t, logs.FilterFieldKey("scope_key").All(), 1,
		"re-poisoning the same (kind, scope) in a later batch must not log again")
}

// TestVerificationPoisonLogSuppressionCap pins the dedup-set bound on the
// poison observer (engine.go): past the cap, UNSEEN scopes stop logging
// behind exactly one suppression notice, already-seen scopes still
// deduplicate silently, and the durable markers — the source of truth —
// land for every poisoned scope regardless. The seam shrinks the
// production 4096 bound to 1 so three scopes exercise all three branches.
func TestVerificationPoisonLogSuppressionCap(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	openCtx := ctxzap.ToContext(context.Background(), zap.New(core))
	dbDir := filepath.Join(t.TempDir(), "db")
	e, err := Open(openCtx, dbDir)
	require.NoError(t, err)
	defer func() { require.NoError(t, e.Close()) }()
	e.test.poisonLogSetCap = 1

	ctx := t.Context()
	a := NewAdapter(e)
	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Under the cap: scope-a's poison logs its scope line and fills the set.
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-a"),
		scGrant("member", "r1", false)))
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-b"),
		scGrant("member", "r1", false)))
	require.Len(t, logs.FilterFieldKey("scope_key").All(), 1)

	// At the cap, first UNSEEN scope: one suppression notice, no scope line.
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-c"),
		scGrant("member", "r2", false)))
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-d"),
		scGrant("member", "r2", false)))
	require.Len(t, logs.FilterFieldKey("scope_key").All(), 1,
		"a scope poisoned past the cap must not log a scope line")
	require.Len(t, logs.FilterFieldKey("bound").All(), 1,
		"crossing the cap must log the suppression notice")

	// Second unseen scope: the notice must not repeat.
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-e"),
		scGrant("member", "r3", false)))
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-f"),
		scGrant("member", "r3", false)))
	require.Len(t, logs.FilterFieldKey("bound").All(), 1,
		"the suppression notice must fire exactly once per open")

	// Already-seen scope past the cap: still deduplicated silently.
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-a"),
		scGrant("member", "r4", false)))
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-b"),
		scGrant("member", "r4", false)))
	require.Len(t, logs.FilterFieldKey("scope_key").All(), 1)
	require.Len(t, logs.FilterFieldKey("bound").All(), 1)

	// Suppression is log-only: every poisoned scope's durable marker landed.
	for _, scope := range []string{"scope-a", "scope-c", "scope-e"} {
		poisoned, err := e.SourceCachePoisoned(ctx, string(sourcecache.RowKindGrants), scope)
		require.NoError(t, err)
		require.True(t, poisoned, "suppressed logging must not suppress the durable marker for %s", scope)
	}
}
