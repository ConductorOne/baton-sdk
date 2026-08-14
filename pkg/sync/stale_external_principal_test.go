package sync

import (
	"context"
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

// TestDeleteStaleExternalPrincipalsRevokesGrantAfterCutAndShrink is a
// regression test for a gap in deleteStaleExternalPrincipals: it previously
// required resourceRecordDeleter + entitlementRecordDeleter +
// grantByRefsDeleter support to do ANYTHING, but only the Pebble engine
// implements any of the three -- so on SQLite (the default storage engine
// absent an explicit opt-in; see c1's FEATURE_FLAG_ID_PEBBLE_DEFAULT_STORAGE_ENGINE)
// this whole reconciliation silently no-opped. Before newGrantForExternalPrincipal
// stripped the ExternalResourceMatch* annotation from resolved replacement
// grants, that gap was accidentally masked: a resolved grant retained its
// match annotation forever, so a later scan of processGrantsWithExternalPrincipals
// would re-evaluate and delete it once its external principal disappeared.
// Once resolved grants stopped carrying that annotation, SQLite lost its
// only path to revoking access for a departed external principal -- a real,
// silent phantom-grant regression on the majority of tenants (Pebble is a
// manually-gated, SKU-restricted opt-in; SQLite is the default).
//
// This specifically needs the interrupted-then-resumed shape, not just two
// independent full syncs: a resume can skip re-running the native
// grant-sync step for an already-checkpointed entitlement, so the ordinary
// "a fresh sync's listing replaces what's stored for this entitlement"
// pruning -- which does clean this up across two independent full syncs on
// every engine -- does not necessarily run again on resume. That's exactly
// when deleteStaleExternalPrincipals is the only remaining path.
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
