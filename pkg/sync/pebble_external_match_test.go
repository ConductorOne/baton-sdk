package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/logging"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// TestPebble_ExternalResourceMatchID_RemappingSkipped is the Pebble
// counterpart to TestExternalResourceMatchIDWithExpandableRemapping
// (pkg/sync/syncer_test.go).
//
// On SQLite, the external-resource-match remapping path runs:
//
//  1. The internal connector emits a grant carrying both an
//     `ExternalResourceMatchID` annotation (referencing an external
//     resource id) and a `GrantExpandable` annotation pointing at
//     an entitlement on a PLACEHOLDER resource.
//  2. After sync, `processGrantsWithExternalPrincipals`
//     (pkg/sync/syncer.go) iterates grants via
//     `s.store.Grants().ListWithAnnotations(ctx)`. For grants
//     carrying both annotations it computes
//     `newExpandableEntId := entitlement.NewEntitlementID(matched_external_principal, slug)`
//     and stamps a *new* `GrantExpandable` annotation on the
//     expanded grant, with `EntitlementIds = [newExpandableEntId]`.
//     The original placeholder entitlement id never appears in
//     storage.
//
// On Pebble that path silently no-ops. The remapping is gated on
// `if expandableAnno != nil`, where `expandableAnno = ga.Annotation`
// — the *typed* pointer Pebble's grant store yields for each row.
// Pebble's `pebbleGrantStore.ListWithAnnotationsPage`
// (pkg/dotc1z/engine/pebble/adapter_grants_store.go) sets
// `Annotation: nil` unconditionally:
//
//	rows = append(rows, dotc1z.GrantAnnotation{
//	    Grant:                   V3GrantToV2(rec),
//	    Annotation:              nil, // Stack 6 fills this in
//	    ...
//	})
//
// The `// Stack 6 fills this in` comment is the bug. So on Pebble,
// the remapping branch never executes, `newGrantForExternalPrincipal`
// copies the original grant's *placeholder* `GrantExpandable`
// annotation onto the expanded grant verbatim, and the placeholder
// entitlement id is what ends up persisted.
//
// Downstream impact: chained expansion through external-matched
// principals (the entire reason ExternalResourceMatchID +
// GrantExpandable was designed to compose) breaks silently. The
// expander walks the placeholder entitlement id, finds nothing
// (the placeholder resource was never synced), and the
// transitive grants vanish.
//
// This test exercises the same scenario as
// `TestExternalResourceMatchIDWithExpandableRemapping` but with the
// internal store on Pebble. It asserts the post-sync expansion
// store contains the REMAPPED entitlement id, not the placeholder.
func TestPebble_ExternalResourceMatchID_RemappingSkipped(t *testing.T) {
	ctx := t.Context()
	ctx, err := logging.Init(ctx)
	require.NoError(t, err)

	tempDir := t.TempDir()

	internalMc := newMockConnector()
	internalMc.rtDB = append(internalMc.rtDB, userResourceType, groupResourceType)

	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)

	// External group (the matched principal). Synced first so the
	// remapped entitlement id resolves to a real entitlement.
	_, extGroupEnt, err := externalMc.AddGroup(ctx, "ext_role")
	require.NoError(t, err)

	internalGroup, _, err := internalMc.AddGroup(ctx, "internal_group")
	require.NoError(t, err)

	// PLACEHOLDER resource whose entitlement id appears in the
	// GrantExpandable annotation. The remapping path should
	// rewrite this to ext_role's member entitlement.
	placeholderResource := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "placeholder_role"}.Build(),
	}.Build()
	placeholderEntID, err := bid.MakeBid(v2.Entitlement_builder{
		Resource: placeholderResource,
		Slug:     "member",
	}.Build())
	require.NoError(t, err)

	internalMc.grantDB[internalGroup.GetId().GetResource()] = []*v2.Grant{
		gt.NewGrant(
			internalGroup,
			"member",
			placeholderResource.GetId(),
			gt.WithAnnotation(v2.ExternalResourceMatchID_builder{
				Id: "ext_role",
			}.Build()),
			gt.WithAnnotation(v2.GrantExpandable_builder{
				EntitlementIds:  []string{placeholderEntID},
				Shallow:         true,
				ResourceTypeIds: []string{"user"},
			}.Build()),
		),
	}

	// External sync — SQLite is fine for the external store; the
	// bug we're isolating is in the internal store's grant-
	// annotation surface.
	externalC1zpath := filepath.Join(tempDir, "external.c1z")
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zpath), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, externalSyncer.Sync(ctx))
	require.NoError(t, externalSyncer.Close(ctx))

	// Internal sync — Pebble.
	internalC1zpath := filepath.Join(tempDir, "internal-pebble.c1z")
	internalStore, err := dotc1z.NewStore(ctx, internalC1zpath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	internalSyncer, err := NewSyncer(ctx, internalMc,
		WithConnectorStore(internalStore),
		WithTmpDir(tempDir),
		WithExternalResourceC1ZPath(externalC1zpath),
		WithDontExpandGrants(),
	)
	require.NoError(t, err)
	require.NoError(t, internalSyncer.Sync(ctx))
	require.NoError(t, internalSyncer.Close(ctx))

	// Re-open the produced Pebble c1z and walk PendingExpansion.
	// `PendingExpansion` populates the typed `Annotation` pointer
	// correctly even on Pebble (it's an engine-internal path
	// reading the v3.GrantRecord.Expansion field directly), so any
	// remapped entitlement id we expected to be stored would show
	// up here. The bug is upstream: the *write* never happened.
	reopen, err := dotc1z.NewStore(ctx, internalC1zpath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { _ = reopen.Close(ctx) }()

	prevSync, err := reopen.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, prevSync, "expected a finished full sync on the internal Pebble c1z")
	require.NoError(t, reopen.SetCurrentSync(ctx, prevSync.ID))

	var defs []dotc1z.PendingExpansion
	for pe, err := range reopen.Grants().PendingExpansion(ctx) {
		require.NoError(t, err)
		defs = append(defs, pe)
	}

	// The remapped entitlement id we expect to find — the matched
	// external group's member entitlement.
	expectedRemappedEntID := extGroupEnt.GetId()

	foundRemapped := false
	foundPlaceholder := false
	for _, def := range defs {
		for _, srcEntID := range def.Annotation.GetEntitlementIds() {
			if srcEntID == placeholderEntID {
				foundPlaceholder = true
			}
			if srcEntID == expectedRemappedEntID {
				foundRemapped = true
			}
		}
	}

	require.False(t, foundPlaceholder,
		"the placeholder entitlement id %q must not survive into storage; "+
			"processGrantsWithExternalPrincipals is supposed to remap it to the matched "+
			"external principal's entitlement id. On Pebble the remapping branch is gated on "+
			"`ga.Annotation != nil`, but pebbleGrantStore.ListWithAnnotationsPage hardcodes "+
			"`Annotation: nil` (// Stack 6 fills this in), so the branch never runs and the "+
			"placeholder is persisted verbatim",
		placeholderEntID)

	require.True(t, foundRemapped,
		"expansion store must contain the remapped entitlement id %q (built from the matched "+
			"external principal); on Pebble the remapping branch in "+
			"processGrantsWithExternalPrincipals never ran, so this entitlement id was never written",
		expectedRemappedEntID)
}

// TestPebbleExternalC1Z verifies that a Pebble-format external resource c1z
// can be opened and read by the syncer. Before the fix, NewExternalC1FileReader
// always called decompressC1z which expects a v1 (SQLite/zstd) magic byte and
// returned "magic number mismatch" on v3 (Pebble) files.
func TestPebbleExternalC1Z(t *testing.T) {
	ctx := t.Context()
	ctx, err := logging.Init(ctx)
	require.NoError(t, err)

	tempDir := t.TempDir()

	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)

	externalUser, err := externalMc.AddUser(ctx, "ext_user")
	require.NoError(t, err)

	externalGroup, _, err := externalMc.AddGroup(ctx, "ext_group")
	require.NoError(t, err)
	externalMc.grantDB[externalGroup.GetId().GetResource()] = []*v2.Grant{
		gt.NewGrant(externalGroup, "member", externalUser.GetId()),
	}

	// Produce the external c1z using the Pebble engine.
	externalC1ZPath := filepath.Join(tempDir, "external-pebble.c1z")
	externalStore, err := dotc1z.NewStore(ctx, externalC1ZPath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	externalSyncer, err := NewSyncer(ctx, externalMc,
		WithConnectorStore(externalStore),
		WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	require.NoError(t, externalSyncer.Sync(ctx))
	require.NoError(t, externalSyncer.Close(ctx))

	// Internal connector grants member to an internal user via ExternalResourceMatch.
	internalMc := newMockConnector()
	internalMc.rtDB = append(internalMc.rtDB, userResourceType, groupResourceType)

	internalUser, err := internalMc.AddUser(ctx, "int_user")
	require.NoError(t, err)

	internalGroup, _, err := internalMc.AddGroup(ctx, "int_group")
	require.NoError(t, err)
	internalMc.grantDB[internalGroup.GetId().GetResource()] = []*v2.Grant{
		gt.NewGrant(
			internalGroup,
			"member",
			internalUser.GetId(),
			gt.WithAnnotation(v2.ExternalResourceMatch_builder{
				Key:          "profile_key",
				Value:        "ext_user",
				ResourceType: v2.ResourceType_TRAIT_USER,
			}.Build()),
		),
	}

	// Internal sync uses the Pebble external c1z — this is the path that
	// previously failed with "magic number mismatch".
	internalC1ZPath := filepath.Join(tempDir, "internal.c1z")
	internalSyncer, err := NewSyncer(ctx, internalMc,
		WithC1ZPath(internalC1ZPath),
		WithTmpDir(tempDir),
		WithExternalResourceC1ZPath(externalC1ZPath),
	)
	require.NoError(t, err)
	require.NoError(t, internalSyncer.Sync(ctx))
	require.NoError(t, internalSyncer.Close(ctx))

	// Verify grants were written to the internal store.
	internalStore, err := dotc1z.NewC1ZFile(ctx, internalC1ZPath)
	require.NoError(t, err)
	defer func() { _ = internalStore.Close(ctx) }()

	allGrants, err := internalStore.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, allGrants.GetList(), "expected grants written to the internal store after syncing against a Pebble external c1z")
}
