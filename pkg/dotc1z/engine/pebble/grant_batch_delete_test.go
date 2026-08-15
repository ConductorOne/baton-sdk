package pebble

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// batchDeleteSyncID pins the sync id: grant keys embed it, so two
// independently-built fixtures are only byte-comparable when they share one.
const batchDeleteSyncID = "0ujsswThIGTUYm2K8FjOOfXtY1K"

// makeExpandableGrant is makeGrant with needs_expansion set, so a fixture
// populates by_needs_expansion as well as by_principal — a batched delete
// that dropped either index obligation must be visible.
func makeExpandableGrant(externalID, entID, principalID string) *v3.GrantRecord {
	r := makeGrant("", externalID, entID, principalID)
	r.SetNeedsExpansion(true)
	return r
}

// batchDeleteFixtureGrants spans two entitlement partitions (so a delete
// invalidates one digest partition and must leave the other intact) and mixes
// expandable and plain grants.
func batchDeleteFixtureGrants() []*v3.GrantRecord {
	return []*v3.GrantRecord{
		makeGrant("", "g-a-alice", "ent-A", "alice"),
		makeExpandableGrant("g-a-bob", "ent-A", "bob"),
		makeGrant("", "g-a-carol", "ent-A", "carol"),
		makeGrant("", "g-b-dave", "ent-B", "dave"),
		makeExpandableGrant("g-b-erin", "ent-B", "erin"),
		makeGrant("", "g-b-frank", "ent-B", "frank"),
	}
}

// batchDeleteFixture builds the store shape the syncer's external-principal
// cleanup deletes into: grants written, EndSync run (which builds the digest
// state, so deletes owe digest invalidation), then the sync rebound so the
// engine is writable and NOT fresh.
func batchDeleteFixture(t *testing.T) *Engine {
	t.Helper()
	ctx := context.Background()
	e, _ := newTestEngine(t)
	a := NewAdapter(e)

	_, err := a.StartNewSyncWithID(ctx, connectorstore.SyncTypeFull, batchDeleteSyncID, "")
	require.NoError(t, err)
	putEnt(t, e, ctx, "ent-A")
	putEnt(t, e, ctx, "ent-B")
	require.NoError(t, e.PutGrantRecords(ctx, batchDeleteFixtureGrants()...))
	require.NoError(t, a.EndSync(ctx))
	require.NoError(t, a.SetCurrentSync(ctx, batchDeleteSyncID))

	// Non-vacuity: every family the comparison inspects must be populated
	// before anything is deleted, or "indistinguishable" would be trivial.
	for name, keys := range grantKeyspaces(t, e) {
		require.NotEmpty(t, keys, "fixture must populate the %s keyspace", name)
	}
	return e
}

func dumpKeyRange(t testing.TB, e *Engine, lo, hi []byte) map[string][]byte {
	t.Helper()
	it, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	require.NoError(t, err)
	defer func() { _ = it.Close() }()
	out := make(map[string][]byte)
	for it.First(); it.Valid(); it.Next() {
		out[string(it.Key())] = append([]byte(nil), it.Value()...)
	}
	require.NoError(t, it.Error())
	return out
}

// grantKeyspaces dumps every key family a grant delete is obligated to
// maintain: the primary row, both live secondary indexes, and the digest
// state (nodes + hash index). Two stores agreeing on all five are
// indistinguishable as far as grants are concerned — which is the whole risk
// of batching, since a batched delete that skipped an index or digest
// obligation would show up here and nowhere else.
func grantKeyspaces(t testing.TB, e *Engine) map[string]map[string][]byte {
	t.Helper()
	grantPrefix := encodeGrantPrefix()
	return map[string]map[string][]byte{
		"primary":            dumpKeyRange(t, e, grantPrefix, upperBoundOf(grantPrefix)),
		"by_principal":       dumpKeyRange(t, e, GrantByPrincipalLowerBound(), GrantByPrincipalUpperBound()),
		"by_needs_expansion": dumpKeyRange(t, e, GrantByNeedsExpansionLowerBound(), GrantByNeedsExpansionUpperBound()),
		"digest_nodes":       dumpKeyRange(t, e, DigestLowerBound(), DigestUpperBound()),
		"digest_hash_index":  dumpKeyRange(t, e, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()),
	}
}

func requireSameGrantKeyspaces(t *testing.T, batched, singular *Engine) {
	t.Helper()
	got := grantKeyspaces(t, batched)
	want := grantKeyspaces(t, singular)
	for name := range want {
		require.Equal(t, want[name], got[name],
			"batched delete diverged from the singular delete in the %s keyspace", name)
	}
}

// deleteSingularly applies the same records through the one-commit-per-grant
// path, which is the oracle the batch path must match.
func deleteSingularly(t *testing.T, e *Engine, records ...*v3.GrantRecord) {
	t.Helper()
	ctx := context.Background()
	for _, r := range records {
		require.NoError(t, e.DeleteGrantByIdentityRefs(ctx, r))
	}
}

// TestDeleteGrantsByRefsMatchesSingularPath is the equivalence oracle: the
// batched delete must leave a store indistinguishable from one where the same
// grants were deleted one durable commit at a time, across the primary rows,
// both secondary indexes, and the digest state.
func TestDeleteGrantsByRefsMatchesSingularPath(t *testing.T) {
	ctx := context.Background()
	victims := []*v3.GrantRecord{
		makeGrant("", "g-a-alice", "ent-A", "alice"),
		makeExpandableGrant("g-a-bob", "ent-A", "bob"),
		makeExpandableGrant("g-b-erin", "ent-B", "erin"),
	}

	// chunk=2 forces a chunk boundary mid-set with a six-row fixture, so the
	// production chunking loop is exercised without a 10k-grant fixture.
	for _, chunk := range []int{1, 2, grantDeleteBatchChunk} {
		batched := batchDeleteFixture(t)
		require.NoError(t, batched.deleteGrantsByIdentityRefs(ctx, chunk, victims...))

		singular := batchDeleteFixture(t)
		deleteSingularly(t, singular, victims...)

		// The deletes must actually have happened, or the comparison below
		// is between two untouched stores.
		require.Equal(t, 3, countKeys(t, batched, encodeGrantPrefix()),
			"three of six grants must remain")
		require.Equal(t, 0, countKeys(t, batched, encodeGrantByNeedsExpansionPrefix()),
			"both expandable grants were deleted, so the index must be empty")

		requireSameGrantKeyspaces(t, batched, singular)
	}
}

// TestDeleteGrantsByRefsAbsentGrantIsNoOp pins the existence-probe
// short-circuit: an identity that was never stored must stage nothing, so the
// batch does not tombstone the digest partition of an entitlement it never
// touched. Staging unconditionally would pass a "grant is gone" assertion
// while silently invalidating live digests.
func TestDeleteGrantsByRefsAbsentGrantIsNoOp(t *testing.T) {
	ctx := context.Background()
	absent := []*v3.GrantRecord{
		makeGrant("", "g-a-nobody", "ent-A", "nobody"),
		makeGrant("", "g-z-ghost", "ent-Z", "ghost"),
	}

	e := batchDeleteFixture(t)
	before := grantKeyspaces(t, e)
	_, rootPresent, err := e.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err)
	require.True(t, rootPresent, "fixture premise: the sealed store has a global digest root")

	require.NoError(t, e.DeleteGrantsByIdentityRefs(ctx, absent...))

	for name, keys := range grantKeyspaces(t, e) {
		require.Equal(t, before[name], keys,
			"deleting a non-existent grant must not touch the %s keyspace", name)
	}
	_, rootPresent, err = e.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err)
	require.True(t, rootPresent, "an absent-grant delete must not invalidate the global digest root")

	// And the singular path agrees.
	singular := batchDeleteFixture(t)
	deleteSingularly(t, singular, absent...)
	requireSameGrantKeyspaces(t, e, singular)
}

// TestDeleteGrantsByRefsMixedBatch covers the batch-specific cases the
// singular path cannot express: present and absent identities interleaved,
// and the same key staged for deletion twice inside one RecordBatch.
func TestDeleteGrantsByRefsMixedBatch(t *testing.T) {
	ctx := context.Background()
	mixed := []*v3.GrantRecord{
		makeGrant("", "g-a-alice", "ent-A", "alice"),   // present
		makeGrant("", "g-a-nobody", "ent-A", "nobody"), // absent
		makeGrant("", "g-a-alice", "ent-A", "alice"),   // duplicate of a present row
		makeExpandableGrant("g-b-erin", "ent-B", "erin"),
		makeGrant("", "g-z-ghost", "ent-Z", "ghost"),     // absent, unknown partition
		makeExpandableGrant("g-b-erin", "ent-B", "erin"), // duplicate of an expandable row
	}

	// chunk=3 splits the duplicate pairs across chunks for one of the runs,
	// so both "duplicate inside one batch" and "duplicate across batches"
	// are covered.
	for _, chunk := range []int{3, grantDeleteBatchChunk} {
		batched := batchDeleteFixture(t)
		require.NoError(t, batched.deleteGrantsByIdentityRefs(ctx, chunk, mixed...))

		singular := batchDeleteFixture(t)
		deleteSingularly(t, singular, mixed...)

		require.Equal(t, 4, countKeys(t, batched, encodeGrantPrefix()),
			"exactly the two present identities must be gone")
		requireSameGrantKeyspaces(t, batched, singular)
	}
}
