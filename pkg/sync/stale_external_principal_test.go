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
// end-to-end invariant: an external principal that disappears between an
// interrupted attempt and its resume must not leave a live grant behind, on
// either storage engine.
//
// It asserts the outcome, not the mechanism, on purpose -- two independent
// paths can revoke such a grant and which one gets there first has already
// changed once underneath this test:
//   - processGrantsWithExternalPrincipals's own scan, because a resolved
//     replacement retains its ExternalResourceMatch* annotation (see
//     newGrantForExternalPrincipal) and so is re-evaluated and dropped once
//     its principal is gone;
//   - deleteStaleExternalPrincipals, which reconciles principal rows an
//     earlier attempt copied in. Its no-refs-deleter fallback -- the SQLite
//     path -- is pinned separately and directly by
//     TestExternalPrincipalCleanupFallsBackToIDDeleteWithoutRefsDeleters,
//     because from out here the first path masks it.
//
// The interrupted-then-resumed shape is load-bearing, and two independent
// full syncs would not substitute for it: a resume can skip re-running the
// native grant-sync step for an already-checkpointed entitlement, so the
// ordinary "a fresh listing replaces what is stored for this entitlement"
// pruning -- which does clean this up across two full syncs on every engine
// -- does not necessarily run again. Disabling both revocation paths above
// leaves the phantom grant in place here, which is what makes that concrete.
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

// TestResolvedGrantRevokedWhenMatchCriteriaChangesAfterCut is a regression
// test for why newGrantForExternalPrincipal retains the placeholder's
// ExternalResourceMatch* annotation on its replacement grant instead of
// stripping it. Stripping was tried as a fix for retry amplification (a
// resumed sync's fresh scan re-matching an already-resolved leftover), but
// it broke self-healing for a case deleteStaleExternalPrincipals cannot
// cover: an external principal that still EXISTS (so it's never "stale")
// but whose profile value changes between an interrupted attempt and its
// resume, so it no longer satisfies the placeholder's match criteria. With
// the annotation retained, the resumed scan re-encounters the leftover
// replacement, re-evaluates it, finds no match, and deletes it via the same
// path as any other unmatched grant -- verified here across both engines,
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
	// Two placeholders, not one per user. Unlike the ExternalResourceMatchID
	// test above -- where each placeholder matches exactly one user, so
	// spanning multiple flush batches takes userCount of them -- one
	// department=Sales placeholder here already fans out to every user, so
	// userCount placeholders would each expand to the *same* userCount
	// replacement ids (newGrantForExternalPrincipal keys a replacement on
	// principal + entitlement, not on the placeholder), turning a 1k-row
	// assertion into a 1M-row rewrite for no extra coverage. Two rather than
	// one keeps the only thing a single placeholder loses: expandedGrantsBuf
	// is not reset per placeholder, so a flush batch straddling two
	// placeholders' expansions stays exercised.
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
