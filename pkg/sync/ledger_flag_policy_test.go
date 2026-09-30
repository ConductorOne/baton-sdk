package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
)

type ledgerPageCommitFailsForOp struct {
	c1zstore.PageLedgerStore
	op ActionOp
}

func (s ledgerPageCommitFailsForOp) BeginPage() c1zstore.PageWriter {
	return ledgerPageWriterFailsForOp{PageWriter: s.PageLedgerStore.BeginPage(), op: s.op}
}

type ledgerPageWriterFailsForOp struct {
	c1zstore.PageWriter
	op ActionOp
}

func (w ledgerPageWriterFailsForOp) Commit(ctx context.Context, id c1zstore.LedgerActionIdentity, row *c1zstore.LedgerRow) error {
	if id.Op == w.op.String() {
		return errLedgerInjectedPage
	}
	return w.PageWriter.Commit(ctx, id, row)
}

func listGrantIDs(t *testing.T, f *ledgerFixture) []string {
	t.Helper()
	grants, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	var ids []string
	for _, g := range grants.GetList() {
		ids = append(ids, g.GetId())
	}
	return ids
}

// CO-039 §7: a resumer whose expansion flags conflict with the pass's state
// is refused before any write. C1 expands through an empty connector, so
// the refusing connector here stands in for it: any list call is a failure.
func TestLedgerExpansionOnlyRefusesIncompleteCollection(t *testing.T) {
	ctx := t.Context()
	source, want, _ := ledgerUnexpandedSource(t)
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "incomplete.c1z"), false)
	first, err := NewSyncer(ctx, source, WithConnectorStore(f.store), WithDontExpandGrants())
	require.NoError(t, err)
	first.(*syncer).caps.pageLedger = ledgerPageCommitFailsForOp{PageLedgerStore: f.ledger, op: SyncGrantsOp}
	require.ErrorIs(t, first.Sync(ctx), errLedgerInjectedPage)
	id := first.(*syncer).syncID
	pending, phase, err := f.ledger.PendingWork(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, c1zstore.LedgerQueueCollecting, phase)
	require.NotEmpty(t, pending, "collection has begun and is not done")
	before := ledgerRawSnapshot(t, f.engine)
	require.NoError(t, f.store.Close(ctx))

	f = openLedgerFixtureAt(t, f.path, false)
	expander, err := NewSyncer(ctx, ledgerExpansionConnector{mockConnector: newMockConnector()}, WithConnectorStore(f.store), WithSyncID(id), WithOnlyExpandGrants())
	require.NoError(t, err)
	err = expander.Sync(ctx)
	require.ErrorIs(t, err, ErrLedgerStateConflict)
	require.ErrorContains(t, err, "collecting")
	require.ErrorContains(t, err, id)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)), "a refused resume writes nothing")
	require.NoError(t, f.store.Close(ctx))

	f = openLedgerFixtureAt(t, f.path, false)
	plain, err := NewSyncer(ctx, source, WithConnectorStore(f.store), WithSyncID(id), WithDontExpandGrants())
	require.NoError(t, err)
	require.NoError(t, plain.Sync(ctx))
	require.ElementsMatch(t, want, listGrantIDs(t, f))
	require.NoError(t, f.store.SetCurrentSync(ctx, id))
	require.Equal(t, c1zstore.LedgerQueueAbsent, ledgerPhase(t, f.ledger))
}

func TestLedgerDontExpandRefusesPassInExpansion(t *testing.T) {
	t.Setenv("BATON_PEBBLE_SYNTH_LAYER_SEGMENT_ROWS", "1")
	ctx := t.Context()
	_, reference := ledgerExpansionFixture(t)
	require.NoError(t, newLedgerExpansionPublicSyncer(t, reference).Sync(ctx))
	want := listGrantIDs(t, reference)

	_, f := ledgerExpansionFixture(t)
	first := newLedgerExpansionPublicSyncer(t, f)
	observer := &ledgerExpansionLayerObserver{expandedGrantLayerStorer: first.caps.expandedGrantLayer, failFinish: true}
	first.caps.expandedGrantLayer = observer
	require.ErrorIs(t, first.Sync(ctx), errLedgerInjectedPage)
	require.Positive(t, observer.begins, "expansion began")
	id := first.syncID
	before := ledgerRawSnapshot(t, f.engine)
	require.NoError(t, f.store.Close(ctx))

	f = openLedgerFixtureAt(t, f.path, false)
	skipper, err := NewSyncer(ctx, ledgerExpansionConnector{mockConnector: newMockConnector()}, WithConnectorStore(f.store), WithSyncID(id), WithDontExpandGrants(), WithLedgerDebug(true))
	require.NoError(t, err)
	err = skipper.Sync(ctx)
	require.ErrorIs(t, err, ErrLedgerStateConflict, "the pass committed to expansion; a resumer cannot skip it")
	require.ErrorContains(t, err, "expanding")
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)), "a refused resume writes nothing")
	require.NoError(t, f.store.Close(ctx))

	f = openLedgerFixtureAt(t, f.path, false)
	require.NoError(t, f.store.SetCurrentSync(ctx, id))
	require.NoError(t, newLedgerExpansionPublicSyncer(t, f).Sync(ctx))
	require.ElementsMatch(t, want, listGrantIDs(t, f))
}

// A finished baseline-SDK upload: records, a legacy token with an empty
// stack, ended_at, no ledger family. C1 resumes it with only-expand through
// an empty connector. Green before CO-039 and must stay green.
func TestLedgerExpansionOnlyExpandsFinishedBaselineUpload(t *testing.T) {
	ctx := t.Context()
	f := newLedgerFixture(t)
	id := f.engine.CurrentSyncID()
	require.NoError(t, f.store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(), v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()))
	alice := v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build()
	require.NoError(t, f.store.PutResources(ctx, v2.Resource_builder{Id: alice}.Build()))
	groups := make([]*v2.Resource, 3)
	for i, name := range []string{"a", "b", "c"} {
		groups[i] = v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: name}.Build()}.Build()
	}
	require.NoError(t, f.store.PutResources(ctx, groups...))
	for _, g := range groups {
		require.NoError(t, f.store.PutEntitlements(ctx, et.NewAssignmentEntitlement(g, "member")))
	}
	grants := []*v2.Grant{gt.NewGrant(groups[0], "member", alice)}
	for i := 1; i < len(groups); i++ {
		annotation := v2.GrantExpandable_builder{EntitlementIds: []string{et.NewEntitlementID(groups[i-1], "member")}}.Build()
		grants = append(grants, gt.NewGrant(groups[i], "member", groups[i-1].GetId(), gt.WithAnnotation(annotation)))
	}
	require.NoError(t, f.store.PutGrants(ctx, grants...))
	token, err := marshalToken(newRunState(), newRunStats())
	require.NoError(t, err)
	require.NoError(t, f.store.CheckpointSync(ctx, token))
	require.NoError(t, f.store.EndSync(ctx))
	want := append(listGrantIDs(t, f),
		gt.NewGrant(groups[1], "member", alice).GetId(),
		gt.NewGrant(groups[2], "member", alice).GetId(),
		gt.NewGrant(groups[2], "member", groups[0].GetId()).GetId())
	require.NoError(t, f.store.Close(ctx))

	f = openLedgerFixtureAt(t, f.path, false)
	expander, err := NewSyncer(ctx, ledgerExpansionConnector{mockConnector: newMockConnector()}, WithConnectorStore(f.store), WithSyncID(id), WithOnlyExpandGrants())
	require.NoError(t, err)
	require.NoError(t, expander.Sync(ctx), "the C1 path: takeover, Init plans expansion only, seal")
	require.ElementsMatch(t, want, listGrantIDs(t, f))
	require.NoError(t, f.store.SetCurrentSync(ctx, id))
	require.Equal(t, c1zstore.LedgerQueueAbsent, ledgerPhase(t, f.ledger))
	finished, err := f.engine.BoundSyncFinished(ctx)
	require.NoError(t, err)
	require.True(t, finished)
}

// The refusal must precede the takeover: a legacy checkpoint mid-collection
// that a baseline SDK could still resume is left exactly as it was found.
func TestLedgerExpansionOnlyRefusesLegacyCheckpointBeforeTakeover(t *testing.T) {
	ctx := t.Context()
	f := newLedgerFixture(t)
	id := f.engine.CurrentSyncID()
	prior := newRunState()
	prior.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceTypeID: "group", PageToken: "remaining"})
	token, err := marshalToken(prior, newRunStats())
	require.NoError(t, err)
	require.NoError(t, f.store.CheckpointSync(ctx, token))
	before := ledgerRawSnapshot(t, f.engine)
	require.NoError(t, f.store.Close(ctx))

	f = openLedgerFixtureAt(t, f.path, false)
	expander, err := NewSyncer(ctx, ledgerExpansionConnector{mockConnector: newMockConnector()}, WithConnectorStore(f.store), WithSyncID(id), WithOnlyExpandGrants())
	require.NoError(t, err)
	err = expander.Sync(ctx)
	require.ErrorIs(t, err, ErrLedgerStateConflict)
	require.ErrorContains(t, err, "legacy checkpoint mid-collection")
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)), "no takeover: the token is still the checkpoint")
	require.NoError(t, f.store.SetCurrentSync(ctx, id))
	state, err := f.ledger.State(ctx)
	require.NoError(t, err)
	require.True(t, state.Token)
	require.Equal(t, c1zstore.LedgerQueueAbsent, state.Phase)
}

// A baseline-SDK checkpoint whose stack is the expansion step has finished
// collecting and is at or inside expansion. It is taken over as Expanding, so
// a dont-expand resumer is refused before the takeover, and a plain resumer
// finishes the expansion.
func TestLedgerLegacyCheckpointInExpansionIsTakenOverAsExpanding(t *testing.T) {
	build := func(t *testing.T) (*ledgerFixture, string, []string) {
		ctx := t.Context()
		f := newLedgerFixture(t)
		id := f.engine.CurrentSyncID()
		require.NoError(t, f.store.PutResourceTypes(ctx,
			v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(), v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()))
		alice := v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build()
		require.NoError(t, f.store.PutResources(ctx, v2.Resource_builder{Id: alice}.Build()))
		groups := make([]*v2.Resource, 3)
		for i, name := range []string{"a", "b", "c"} {
			groups[i] = v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: name}.Build()}.Build()
		}
		require.NoError(t, f.store.PutResources(ctx, groups...))
		for _, g := range groups {
			require.NoError(t, f.store.PutEntitlements(ctx, et.NewAssignmentEntitlement(g, "member")))
		}
		grants := []*v2.Grant{gt.NewGrant(groups[0], "member", alice)}
		for i := 1; i < len(groups); i++ {
			annotation := v2.GrantExpandable_builder{EntitlementIds: []string{et.NewEntitlementID(groups[i-1], "member")}}.Build()
			grants = append(grants, gt.NewGrant(groups[i], "member", groups[i-1].GetId(), gt.WithAnnotation(annotation)))
		}
		require.NoError(t, f.store.PutGrants(ctx, grants...))
		prior := newRunState()
		prior.setFact(factNeedsExpansion)
		prior.pushAction(ctx, Action{Op: SyncGrantExpansionOp})
		token, err := marshalToken(prior, newRunStats())
		require.NoError(t, err)
		require.NoError(t, f.store.CheckpointSync(ctx, token))
		expanded := append(listGrantIDs(t, f),
			gt.NewGrant(groups[1], "member", alice).GetId(),
			gt.NewGrant(groups[2], "member", alice).GetId(),
			gt.NewGrant(groups[2], "member", groups[0].GetId()).GetId())
		require.NoError(t, f.store.Close(ctx))
		return openLedgerFixtureAt(t, f.path, false), id, expanded
	}

	t.Run("dont-expand is refused before takeover", func(t *testing.T) {
		ctx := t.Context()
		f, id, _ := build(t)
		before := ledgerRawSnapshot(t, f.engine)
		skipper, err := NewSyncer(ctx, ledgerExpansionConnector{mockConnector: newMockConnector()}, WithConnectorStore(f.store), WithSyncID(id), WithDontExpandGrants())
		require.NoError(t, err)
		err = skipper.Sync(ctx)
		require.ErrorIs(t, err, ErrLedgerStateConflict)
		require.ErrorContains(t, err, "legacy checkpoint in expansion")
		require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)), "no takeover: the token is still the checkpoint")
	})

	t.Run("plain resume takes over as expanding and finishes", func(t *testing.T) {
		ctx := t.Context()
		f, id, expanded := build(t)
		s := ledgerContinuationSyncer(f)
		require.NoError(t, f.store.SetCurrentSync(ctx, id))
		phase, err := s.prepareLedgerState(ctx, "takeover", false)
		require.NoError(t, err)
		require.Equal(t, c1zstore.LedgerQueueExpanding, phase, "taken over at the phase the stack implies")
		require.Equal(t, c1zstore.LedgerQueueExpanding, ledgerPhase(t, f.ledger))
		require.NoError(t, f.store.Close(ctx))

		f = openLedgerFixtureAt(t, f.path, false)
		resumer, err := NewSyncer(ctx, ledgerExpansionConnector{mockConnector: newMockConnector()}, WithConnectorStore(f.store), WithSyncID(id))
		require.NoError(t, err)
		require.NoError(t, resumer.Sync(ctx))
		require.ElementsMatch(t, expanded, listGrantIDs(t, f))
	})
}

// Collection happens once per sync ID. A finished sync accepts an
// expansion-only rebind and refuses every collection rebind, including one
// that repeats the flags the collection ran with.
func TestLedgerFinishedSyncRefusesCollection(t *testing.T) {
	ctx := t.Context()
	source, _, _ := ledgerUnexpandedSource(t)
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "finished.c1z"), false)
	first, err := NewSyncer(ctx, source, WithConnectorStore(f.store), WithSkipEntitlementsAndGrants(true))
	require.NoError(t, err)
	require.NoError(t, first.Sync(ctx))
	id := first.(*syncer).syncID
	before := ledgerRawSnapshot(t, f.engine)
	require.NoError(t, f.store.Close(ctx))

	for name, opts := range map[string][]SyncOpt{
		"full":       nil,
		"same-flags": {WithSkipEntitlementsAndGrants(true)},
	} {
		t.Run(name, func(t *testing.T) {
			f = openLedgerFixtureAt(t, f.path, false)
			rebind, err := NewSyncer(ctx, source, append([]SyncOpt{WithConnectorStore(f.store), WithSyncID(id)}, opts...)...)
			require.NoError(t, err)
			err = rebind.Sync(ctx)
			require.ErrorIs(t, err, ErrLedgerStateConflict)
			require.ErrorContains(t, err, "finished")
			require.ErrorContains(t, err, id)
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)), "a refused rebind writes nothing")
			require.NoError(t, f.store.Close(ctx))
		})
	}

	f = openLedgerFixtureAt(t, f.path, false)
	expander, err := NewSyncer(ctx, ledgerExpansionConnector{mockConnector: newMockConnector()}, WithConnectorStore(f.store), WithSyncID(id), WithOnlyExpandGrants())
	require.NoError(t, err)
	require.NoError(t, expander.Sync(ctx), "expansion is the one pass a finished sync accepts")
	require.Empty(t, listGrantIDs(t, f), "a skip pass has nothing to expand")
	require.True(t, expander.(*syncer).run.hasFact(factShouldSkipEntitlementsAndGrants), "the expansion pass reads what the collection was")
}

// Collection flags are the pass's from its first page on. A resumer that
// changes them is refused before any write; one that repeats them finishes.
func TestLedgerCollectionFlagsLockedMidCollection(t *testing.T) {
	ctx := t.Context()
	source, want, _ := ledgerUnexpandedSource(t)
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "locked.c1z"), false)
	first, err := NewSyncer(ctx, source, WithConnectorStore(f.store), WithDontExpandGrants())
	require.NoError(t, err)
	first.(*syncer).caps.pageLedger = ledgerPageCommitFailsForOp{PageLedgerStore: f.ledger, op: SyncGrantsOp}
	require.ErrorIs(t, first.Sync(ctx), errLedgerInjectedPage)
	id := first.(*syncer).syncID
	pending, phase, err := f.ledger.PendingWork(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, c1zstore.LedgerQueueCollecting, phase)
	require.NotEmpty(t, pending, "collection has begun and is not done")
	before := ledgerRawSnapshot(t, f.engine)
	require.NoError(t, f.store.Close(ctx))

	for name, opts := range map[string][]SyncOpt{
		"skip-entitlements-and-grants": {WithSkipEntitlementsAndGrants(true)},
		"skip-grants":                  {WithSkipGrants(true)},
		"resource-types":               {WithSyncResourceTypes([]string{"user"})},
		"targets":                      {WithTargetedSyncResources([]*v2.Resource{v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "a"}.Build()}.Build()})},
	} {
		t.Run(name, func(t *testing.T) {
			f = openLedgerFixtureAt(t, f.path, false)
			resumer, err := NewSyncer(ctx, source, append([]SyncOpt{WithConnectorStore(f.store), WithSyncID(id), WithDontExpandGrants()}, opts...)...)
			require.NoError(t, err)
			err = resumer.Sync(ctx)
			require.ErrorIs(t, err, ErrLedgerStateConflict)
			require.ErrorContains(t, err, "collecting")
			require.ErrorContains(t, err, id)
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)), "a refused resume writes nothing")
			require.NoError(t, f.store.Close(ctx))
		})
	}

	f = openLedgerFixtureAt(t, f.path, false)
	same, err := NewSyncer(ctx, source, WithConnectorStore(f.store), WithSyncID(id), WithDontExpandGrants())
	require.NoError(t, err)
	require.NoError(t, same.Sync(ctx))
	require.ElementsMatch(t, want, listGrantIDs(t, f))
}

// An expansion-only invocation knows nothing about how the file was
// collected; C1 runs it through an empty connector. Its collection flags are
// not read: the file's facts say what the data is, and nothing it passes is
// written back as a fact.
func TestLedgerExpansionOnlyIgnoresCollectionFlags(t *testing.T) {
	ctx := t.Context()
	source, _, expanded := ledgerUnexpandedSource(t)
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "expand.c1z"), false)
	first, err := NewSyncer(ctx, source, WithConnectorStore(f.store), WithDontExpandGrants())
	require.NoError(t, err)
	require.NoError(t, first.Sync(ctx))
	id := first.(*syncer).syncID
	require.NoError(t, f.store.Close(ctx))

	f = openLedgerFixtureAt(t, f.path, false)
	target := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "a"}.Build()}.Build()
	expander, err := NewSyncer(ctx, ledgerExpansionConnector{mockConnector: newMockConnector()}, WithConnectorStore(f.store), WithSyncID(id), WithOnlyExpandGrants(),
		WithSkipEntitlementsAndGrants(true), WithSkipGrants(true), WithSyncResourceTypes([]string{"group"}), WithTargetedSyncResources([]*v2.Resource{target}))
	require.NoError(t, err)
	require.NoError(t, expander.Sync(ctx))
	require.ElementsMatch(t, expanded, listGrantIDs(t, f))
	run := expander.(*syncer).run
	require.False(t, run.hasFact(factShouldSkipEntitlementsAndGrants))
	require.False(t, run.hasFact(factShouldSkipGrants))
	require.False(t, run.hasFact(factShouldFetchRelatedResources))
}
