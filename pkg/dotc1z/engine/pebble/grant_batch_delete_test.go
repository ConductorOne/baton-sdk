package pebble

import (
	"context"
	"strconv"
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

// deleteBatchedWithChunk is DeleteGrantsByIdentityRefs with the chunk size
// overridden, for the cases that need to straddle a boundary cheaply.
func deleteBatchedWithChunk(ctx context.Context, e *Engine, chunk int, records ...*v3.GrantRecord) error {
	ids := make([]grantIdentity, 0, len(records))
	for _, r := range records {
		id, err := grantIdentityFromRecord(r)
		if err != nil {
			return err
		}
		ids = append(ids, id)
	}
	return e.deleteGrantsByIdentities(ctx, chunk, ids)
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

	// chunk=1 and chunk=2 straddle boundaries within the six-row fixture;
	// grantDeleteBatchChunk is the single-chunk case. That the chunking loop
	// runs at all is pinned separately, by
	// TestDeleteGrantsByRefsCommitsInChunks.
	for _, chunk := range []int{1, 2, grantDeleteBatchChunk} {
		batched := batchDeleteFixture(t)
		require.NoError(t, deleteBatchedWithChunk(ctx, batched, chunk, victims...))

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
		require.NoError(t, deleteBatchedWithChunk(ctx, batched, chunk, mixed...))

		singular := batchDeleteFixture(t)
		deleteSingularly(t, singular, mixed...)

		require.Equal(t, 4, countKeys(t, batched, encodeGrantPrefix()),
			"exactly the two present identities must be gone")
		requireSameGrantKeyspaces(t, batched, singular)
	}
}

// chunkProbeCtx counts ctx.Err() calls and optionally starts failing them
// after a given number. deleteGrantsByIdentities consults ctx exactly once
// per chunk and nothing else in the call path touches it, so the count IS the
// number of chunks — which makes the chunking observable without adding any
// production test seam.
type chunkProbeCtx struct {
	context.Context
	checks     int
	cancelUpon int
	// onCheck runs at each boundary, before the sealed re-check that
	// immediately follows ctx.Err() in the loop — which is how a test can
	// seal the engine strictly between two chunks.
	onCheck func(check int)
}

func (c *chunkProbeCtx) Err() error {
	c.checks++
	if c.onCheck != nil {
		c.onCheck(c.checks)
	}
	if c.cancelUpon > 0 && c.checks >= c.cancelUpon {
		return context.Canceled
	}
	return c.Context.Err()
}

// batchDeleteScaleFixture writes n grants under one entitlement, seals to
// build digest state, and rebinds the sync — the same shape as
// batchDeleteFixture but sized to cross the real chunk boundary.
func batchDeleteScaleFixture(t *testing.T, n int) (*Engine, []*v3.GrantRecord) {
	t.Helper()
	ctx := context.Background()
	e, _ := newTestEngine(t)
	a := NewAdapter(e)

	_, err := a.StartNewSyncWithID(ctx, connectorstore.SyncTypeFull, batchDeleteSyncID, "")
	require.NoError(t, err)
	putEnt(t, e, ctx, "ent-A")
	recs := make([]*v3.GrantRecord, 0, n)
	for i := range n {
		recs = append(recs, makeGrant("", "g-"+strconv.Itoa(i), "ent-A", "user-"+strconv.Itoa(i)))
	}
	require.NoError(t, e.PutGrantRecords(ctx, recs...))
	require.NoError(t, a.EndSync(ctx))
	require.NoError(t, a.SetCurrentSync(ctx, batchDeleteSyncID))
	require.Equal(t, n, countKeys(t, e, encodeGrantPrefix()))
	return e, recs
}

// TestDeleteGrantsByRefsCommitsInChunks pins that the production entry point
// actually chunks at grantDeleteBatchChunk, using a record count that crosses
// the real boundary twice. Without this, a change that ignored the chunk size
// and staged one unbounded batch — the exact memory blow-up the constant
// exists to prevent — would pass every other test in this file.
func TestDeleteGrantsByRefsCommitsInChunks(t *testing.T) {
	n := grantDeleteBatchChunk*2 + grantDeleteBatchChunk/2 // 2.5 chunks
	e, recs := batchDeleteScaleFixture(t, n)

	probe := &chunkProbeCtx{Context: context.Background()}
	require.NoError(t, e.DeleteGrantsByIdentityRefs(probe, recs...))

	require.Equal(t, 3, probe.checks,
		"%d grants at a chunk of %d must be committed as 3 chunks", n, grantDeleteBatchChunk)
	require.Equal(t, 0, countKeys(t, e, encodeGrantPrefix()), "every grant must be deleted")
	require.Equal(t, 0, countKeys(t, e, GrantByPrincipalLowerBound()),
		"by_principal must be emptied alongside the primary rows")
}

// TestDeleteGrantsByRefsHonorsCancellation covers the other half of the chunk
// contract: a cancelled context aborts at the next chunk boundary, and the
// chunks already committed stay committed. The apply phase this exists for
// was previously uninterruptible for its whole ~956s.
func TestDeleteGrantsByRefsHonorsCancellation(t *testing.T) {
	n := grantDeleteBatchChunk * 3
	e, recs := batchDeleteScaleFixture(t, n)

	// Pass the first boundary check, fail the second: exactly one chunk
	// commits.
	probe := &chunkProbeCtx{Context: context.Background(), cancelUpon: 2}
	err := e.DeleteGrantsByIdentityRefs(probe, recs...)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, n-grantDeleteBatchChunk, countKeys(t, e, encodeGrantPrefix()),
		"the one chunk committed before the cancel must be durable, and no more")
}

// TestDeleteGrantsByRefsRefusesSealedEngine is the ordinary case: an engine
// already sealed at entry refuses, via withWrite's own check.
func TestDeleteGrantsByRefsRefusesSealedEngine(t *testing.T) {
	ctx := context.Background()
	e := batchDeleteFixture(t)
	require.NoError(t, NewAdapter(e).EndSync(ctx))

	err := e.DeleteGrantsByIdentityRefs(ctx, batchDeleteFixtureGrants()...)
	require.ErrorIs(t, err, ErrEngineSealed)
	require.Equal(t, 6, countKeys(t, e, encodeGrantPrefix()),
		"a sealed engine must not have deleted anything")
}

// TestDeleteGrantsByRefsStopsWhenSealedMidCall pins the fence that holding
// the write lock across every chunk would otherwise coarsen from per-write to
// per-call. seal() takes only sealMu and never waits on writeWG, so an
// in-flight bulk delete CAN be sealed underneath — and the remaining chunks,
// with the digest invalidations they stage, must not land after finalize has
// begun rebuilding the deferred by_principal index.
//
// Sealing from the boundary hook makes that deterministic: the hook runs
// immediately before the loop's sealed re-check.
func TestDeleteGrantsByRefsStopsWhenSealedMidCall(t *testing.T) {
	n := grantDeleteBatchChunk * 3
	e, recs := batchDeleteScaleFixture(t, n)

	probe := &chunkProbeCtx{Context: context.Background()}
	probe.onCheck = func(check int) {
		if check == 2 {
			e.seal()
		}
	}
	err := e.DeleteGrantsByIdentityRefs(probe, recs...)

	require.ErrorIs(t, err, ErrEngineSealed,
		"chunks after the seal must be refused, not committed")
	require.Equal(t, n-grantDeleteBatchChunk, countKeys(t, e, encodeGrantPrefix()),
		"only the chunk committed before the seal may have applied")
}
