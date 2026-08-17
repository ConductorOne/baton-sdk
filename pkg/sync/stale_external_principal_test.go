package sync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/stretchr/testify/require"
)

// failAfterNPutGrants lets the first n PutGrants calls through, then fails
// every call after that -- simulating an interruption partway through
// processGrantsWithExternalPrincipals's scan/expand loop, after some batches
// have already durably flushed but before the scan (and the delete loop)
// complete. successfulBatches records what actually got through, so callers
// can assert the cut landed where they intended it to (see
// requireExternalMatchBatchFlushedBeforeCut) instead of assuming it from the
// call count alone.
type failAfterNPutGrants struct {
	c1zstore.Store
	n                 int
	calls             int
	successfulBatches [][]*v2.Grant
}

var errMidScanCut = errors.New("test: cut after N PutGrants calls")

func (f *failAfterNPutGrants) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	f.calls++
	if f.calls > f.n {
		return errMidScanCut
	}
	batch := make([]*v2.Grant, len(grants))
	copy(batch, grants)
	f.successfulBatches = append(f.successfulBatches, batch)
	return f.Store.PutGrants(ctx, grants...)
}

// DeleteGrantByRefs forwards like countingGrantPutStore's (see its comment):
// embedding the c1zstore.Store interface doesn't promote this optional
// method, so without an explicit passthrough the syncer's delete loop
// always falls back to the id-based path even on engines (Pebble) that
// support the refs-based one.
func (f *failAfterNPutGrants) DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error {
	deleter, ok := f.Store.(grantByRefsDeleter)
	if !ok {
		return f.DeleteGrant(ctx, grant.GetId())
	}
	return deleter.DeleteGrantByRefs(ctx, grant)
}

// requireExternalMatchBatchFlushedBeforeCut fails the test if none of the
// successful PutGrants calls before the cut actually contained a resolved
// external-match replacement (principal in externalUserIDs). Without this,
// a future change to page sizing or call ordering could shift the cut to
// land before any external-match batch flushes, silently making the cut
// point no longer exercise resume-after-partial-progress -- the test would
// still pass, having stopped testing anything.
func requireExternalMatchBatchFlushedBeforeCut(t *testing.T, cutStore *failAfterNPutGrants, externalUserIDs map[string]bool) {
	t.Helper()
	for _, batch := range cutStore.successfulBatches {
		for _, g := range batch {
			if externalUserIDs[g.GetPrincipal().GetId().GetResource()] {
				return
			}
		}
	}
	t.Fatalf("no external-match replacement grant was flushed before the cut (%d successful batches); "+
		"the cut point no longer exercises resume-after-partial-progress", len(cutStore.successfulBatches))
}

// TestDeleteStaleExternalPrincipalsRevokesGrantAfterCutAndShrink pins the
// end-to-end invariant: a departed external principal's grant must not
// survive an interrupted-then-resumed sync, on either engine.
//
// It asserts the outcome, not the mechanism -- two paths can revoke the
// grant, and which one gets there first has already changed once: the
// match scan re-evaluates and drops it because a resolved replacement
// retains its ExternalResourceMatch* annotation (newGrantForExternalPrincipal),
// while deleteStaleExternalPrincipals's own no-refs-deleter fallback is
// masked here and pinned directly by
// TestExternalPrincipalCleanupFallsBackToIDDeleteWithoutRefsDeleters instead.
//
// The interrupted-then-resumed shape matters: a resume can skip re-running
// native grant-sync for an already-checkpointed entitlement, so the usual
// "fresh listing replaces old grants" pruning may not fire again -- which
// two independent full syncs would not exercise.
func testDeleteStaleExternalPrincipalsRevokesGrantAfterCutAndShrink(t *testing.T, engine c1zstore.Engine) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "stale-external-principal-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	internalMc := newMockConnector()
	internalMc.rtDB = append(internalMc.rtDB, userResourceType, groupResourceType)
	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)

	// Enough individually-matched users to force a mid-scan flush (so some
	// replacements are durably written -- and checkpointed -- before the
	// cut, not all deferred to the final unconditional flush).
	const userCount = 2*externalGrantFlushBatchSize + 50
	extUsers := make([]*v2.Resource, 0, userCount)
	for i := range userCount {
		u, err := externalMc.AddUserProfile(ctx, fmt.Sprintf("ext_user_%d", i), map[string]any{})
		require.NoError(t, err)
		extUsers = append(extUsers, u)
	}

	internalGroup, _, err := internalMc.AddGroup(ctx, "internal_group")
	require.NoError(t, err)
	grants := make([]*v2.Grant, 0, len(extUsers))
	for i, u := range extUsers {
		grants = append(grants, gt.NewGrant(
			internalGroup, "member",
			v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: fmt.Sprintf("placeholder_%d", i)}.Build(),
			gt.WithAnnotation(v2.ExternalResourceMatchID_builder{Id: u.GetId().GetResource()}.Build()),
		))
	}
	internalMc.grantDB[internalGroup.GetId().GetResource()] = grants

	externalC1zpath := filepath.Join(tempDir, "external.c1z")
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zpath), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, externalSyncer.Sync(ctx))
	require.NoError(t, externalSyncer.Close(ctx))

	internalC1zpath := filepath.Join(tempDir, "internal.c1z")

	// Cut after enough PutGrants calls to flush at least one resolved
	// replacement (native grant sync consumes the first call, so n:2 lets
	// exactly one external-match batch through).
	rawStore1, err := dotc1z.NewStore(ctx, internalC1zpath, dotc1z.WithEngine(engine), dotc1z.WithTmpDir(tempDir))
	require.NoError(t, err)
	cutStore := &failAfterNPutGrants{Store: rawStore1, n: 2}
	syncer1, err := NewSyncer(ctx, internalMc, WithConnectorStore(cutStore), WithTmpDir(tempDir), WithExternalResourceC1ZPath(externalC1zpath))
	require.NoError(t, err)
	require.ErrorIs(t, syncer1.Sync(ctx), errMidScanCut)
	requireExternalMatchBatchFlushedBeforeCut(t, cutStore, func() map[string]bool {
		ids := make(map[string]bool, len(extUsers))
		for _, u := range extUsers {
			ids[u.GetId().GetResource()] = true
		}
		return ids
	}())

	ctxCancel, cancel := context.WithCancel(ctx)
	cancel()
	require.NoError(t, syncer1.Close(ctxCancel))

	preStore, err := dotc1z.NewStore(ctx, internalC1zpath, dotc1z.WithEngine(engine), dotc1z.WithTmpDir(tempDir))
	require.NoError(t, err)
	preGrants, err := preStore.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	require.NoError(t, preStore.Close(ctx))
	var survivorID string
	for _, g := range preGrants.GetList() {
		for _, u := range extUsers {
			if g.GetPrincipal().GetId().GetResource() == u.GetId().GetResource() {
				survivorID = u.GetId().GetResource()
			}
		}
	}
	require.NotEmpty(t, survivorID, "test setup requires at least one resolved replacement before the cut")

	// The resolved user leaves before resume.
	externalMc2 := newMockConnector()
	externalMc2.rtDB = append(externalMc2.rtDB, userResourceType, groupResourceType)
	for _, u := range extUsers {
		if u.GetId().GetResource() == survivorID {
			continue
		}
		_, err := externalMc2.AddUserProfile(ctx, u.GetId().GetResource(), map[string]any{})
		require.NoError(t, err)
	}
	externalC1zpath2 := filepath.Join(tempDir, "external2.c1z")
	externalSyncer2, err := NewSyncer(ctx, externalMc2, WithC1ZPath(externalC1zpath2), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, externalSyncer2.Sync(ctx))
	require.NoError(t, externalSyncer2.Close(ctx))

	// Resume against the shrunk external source.
	rawStore2, err := dotc1z.NewStore(ctx, internalC1zpath, dotc1z.WithEngine(engine), dotc1z.WithTmpDir(tempDir))
	require.NoError(t, err)
	syncer2, err := NewSyncer(ctx, internalMc, WithConnectorStore(rawStore2), WithTmpDir(tempDir), WithExternalResourceC1ZPath(externalC1zpath2))
	require.NoError(t, err)
	require.NoError(t, syncer2.Sync(ctx))
	require.NoError(t, syncer2.Close(ctx))

	finalStore, err := dotc1z.NewStore(ctx, internalC1zpath, dotc1z.WithEngine(engine), dotc1z.WithTmpDir(tempDir))
	require.NoError(t, err)
	finalGrants, err := finalStore.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	require.NoError(t, finalStore.Close(ctx))
	for _, g := range finalGrants.GetList() {
		require.NotEqual(t, survivorID, g.GetPrincipal().GetId().GetResource(),
			"grant for the departed external user must not survive as a phantom")
	}
}

func TestDeleteStaleExternalPrincipalsRevokesGrantAfterCutAndShrinkSQLite(t *testing.T) {
	testDeleteStaleExternalPrincipalsRevokesGrantAfterCutAndShrink(t, c1zstore.EngineSQLite)
}

func TestDeleteStaleExternalPrincipalsRevokesGrantAfterCutAndShrinkPebble(t *testing.T) {
	testDeleteStaleExternalPrincipalsRevokesGrantAfterCutAndShrink(t, c1zstore.EnginePebble)
}

// testResolvedGrantRevokedWhenMatchCriteriaChanges is a regression test for
// why newGrantForExternalPrincipal retains the match annotation instead of
// stripping it. Stripping was tried to stop a resume from re-matching an
// already-resolved leftover, but it broke self-healing for a case
// deleteStaleExternalPrincipals can't cover: a principal that still EXISTS
// but whose profile changed no longer satisfies the placeholder's match --
// with the annotation retained, the resumed scan re-encounters, re-evaluates,
// and deletes it like any other unmatched grant. Verified on both engines,
// since (unlike the departed-principal case) this one isn't Pebble-safe
// either way.
func testResolvedGrantRevokedWhenMatchCriteriaChanges(t *testing.T, engine c1zstore.Engine) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "match-criteria-change-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	internalMc := newMockConnector()
	internalMc.rtDB = append(internalMc.rtDB, userResourceType, groupResourceType)
	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)

	// Enough matching users that a single placeholder's fan-out spans several
	// flush batches, so the cut below lands mid-expansion with some
	// replacements already durably written.
	const userCount = 2*externalGrantFlushBatchSize + 50
	extUsers := make([]*v2.Resource, 0, userCount)
	for i := range userCount {
		u, err := externalMc.AddUserProfile(ctx, fmt.Sprintf("ext_user_%d", i), map[string]any{"department": "Sales"})
		require.NoError(t, err)
		extUsers = append(extUsers, u)
	}

	internalGroup, _, err := internalMc.AddGroup(ctx, "internal_group")
	require.NoError(t, err)
	// Two placeholders, not one per user like the MatchID test above: a
	// single department=Sales placeholder already fans out to every user
	// (newGrantForExternalPrincipal keys on principal+entitlement, not the
	// placeholder), so userCount placeholders would just rewrite the same
	// userCount ids userCount times. Two, not one, keeps the only thing a
	// single placeholder loses: a flush batch straddling two placeholders'
	// expansions.
	const placeholderCount = 2
	grants := make([]*v2.Grant, 0, placeholderCount)
	for i := range placeholderCount {
		grants = append(grants, gt.NewGrant(
			internalGroup, "member",
			v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: fmt.Sprintf("placeholder_%d", i)}.Build(),
			gt.WithAnnotation(v2.ExternalResourceMatch_builder{
				ResourceType: v2.ResourceType_TRAIT_USER, Key: "department", Value: "Sales",
			}.Build()),
		))
	}
	internalMc.grantDB[internalGroup.GetId().GetResource()] = grants

	externalC1zpath := filepath.Join(tempDir, "external.c1z")
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zpath), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, externalSyncer.Sync(ctx))
	require.NoError(t, externalSyncer.Close(ctx))

	internalC1zpath := filepath.Join(tempDir, "internal.c1z")
	rawStore1, err := dotc1z.NewStore(ctx, internalC1zpath, dotc1z.WithEngine(engine), dotc1z.WithTmpDir(tempDir))
	require.NoError(t, err)
	cutStore := &failAfterNPutGrants{Store: rawStore1, n: 2}
	syncer1, err := NewSyncer(ctx, internalMc, WithConnectorStore(cutStore), WithTmpDir(tempDir), WithExternalResourceC1ZPath(externalC1zpath))
	require.NoError(t, err)
	require.ErrorIs(t, syncer1.Sync(ctx), errMidScanCut)

	ctxCancel, cancel := context.WithCancel(ctx)
	cancel()
	require.NoError(t, syncer1.Close(ctxCancel))

	preStore, err := dotc1z.NewStore(ctx, internalC1zpath, dotc1z.WithEngine(engine), dotc1z.WithTmpDir(tempDir))
	require.NoError(t, err)
	preGrants, err := preStore.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	require.NoError(t, preStore.Close(ctx))
	var survivorID string
	for _, g := range preGrants.GetList() {
		for _, u := range extUsers {
			if g.GetPrincipal().GetId().GetResource() == u.GetId().GetResource() {
				survivorID = u.GetId().GetResource()
			}
		}
	}
	require.NotEmpty(t, survivorID, "test setup requires at least one resolved replacement before the cut")

	// The resolved user's department changes -- still present, no longer matches.
	externalMc2 := newMockConnector()
	externalMc2.rtDB = append(externalMc2.rtDB, userResourceType, groupResourceType)
	for _, u := range extUsers {
		dept := "Sales"
		if u.GetId().GetResource() == survivorID {
			dept = "Marketing"
		}
		_, err := externalMc2.AddUserProfile(ctx, u.GetId().GetResource(), map[string]any{"department": dept})
		require.NoError(t, err)
	}
	externalC1zpath2 := filepath.Join(tempDir, "external2.c1z")
	externalSyncer2, err := NewSyncer(ctx, externalMc2, WithC1ZPath(externalC1zpath2), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, externalSyncer2.Sync(ctx))
	require.NoError(t, externalSyncer2.Close(ctx))

	rawStore2, err := dotc1z.NewStore(ctx, internalC1zpath, dotc1z.WithEngine(engine), dotc1z.WithTmpDir(tempDir))
	require.NoError(t, err)
	syncer2, err := NewSyncer(ctx, internalMc, WithConnectorStore(rawStore2), WithTmpDir(tempDir), WithExternalResourceC1ZPath(externalC1zpath2))
	require.NoError(t, err)
	require.NoError(t, syncer2.Sync(ctx))
	require.NoError(t, syncer2.Close(ctx))

	finalStore, err := dotc1z.NewStore(ctx, internalC1zpath, dotc1z.WithEngine(engine), dotc1z.WithTmpDir(tempDir))
	require.NoError(t, err)
	finalGrants, err := finalStore.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	require.NoError(t, finalStore.Close(ctx))
	for _, g := range finalGrants.GetList() {
		require.NotEqual(t, survivorID, g.GetPrincipal().GetId().GetResource(),
			"grant for the no-longer-matching user must not survive as a phantom")
	}
}

func TestResolvedGrantRevokedWhenMatchCriteriaChangesSQLite(t *testing.T) {
	testResolvedGrantRevokedWhenMatchCriteriaChanges(t, c1zstore.EngineSQLite)
}

func TestResolvedGrantRevokedWhenMatchCriteriaChangesPebble(t *testing.T) {
	testResolvedGrantRevokedWhenMatchCriteriaChanges(t, c1zstore.EnginePebble)
}
