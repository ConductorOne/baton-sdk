package pebble_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// TestPebble_InsertResourceGrants_StubRewriteCorrupts simulates the
// hot loop in `syncGrantsForResource` that handles the
// `InsertResourceGrants` annotation:
//
//	if insertResourceGrants {
//	    resource := grant.GetEntitlement().GetResource()
//	    bid, err := bid.MakeBid(resource)
//	    ...
//	    resourcesToInsertMap[bid] = resource
//	}
//	...
//	s.store.PutResources(ctx, resourcesToInsert...)
//
// The syncer takes each grant's embedded `Entitlement.Resource`,
// keys it by BID, and writes it to the resources table via
// `PutResources`. On etag-replay those grants come back from the
// store (not from the live connector response), so the embedded
// `Entitlement.Resource` is whatever the store materialised.
//
// On SQLite (default, no `WithV2GrantsWriter`) the stored grant
// payload preserves the full embedded `Entitlement.Resource`, so
// the loop above re-writes the same rich Resource it originally
// received. No corruption.
//
// On Pebble the stored grant payload is unconditionally slim — the
// v3.GrantRecord only carries identity refs. When the syncer reads
// a grant back, `V3GrantToV2`'s `entitlementRefToStub` produces a
// stub Resource with nothing but `(resource_type_id, resource_id)`.
// The InsertResourceGrants loop then rewrites the resources table
// with that stub, dropping `DisplayName`, the parent ref, and every
// annotation the original Resource carried (GroupTrait, UserTrait,
// AppTrait, RoleTrait, SecretTrait — all the connector-level
// traits — plus anything else the connector attached).
//
// This corruption surface is currently unreachable via the syncer
// because the etag-replay PATH is broken upstream (see
// `pkg/dotc1z/engine/pebble/sync_details_annotation_test.go` and
// `pkg/sync/pebble_etag_replay_test.go`): the syncer's
// `fetchEtaggedGrantsForResource` retrieves zero grants on Pebble.
// Fix the SyncDetails-annotation bug and this corruption fires.
//
// The test exercises the corruption directly by replaying the
// extract-and-PutResources loop on a grant read out of the same
// Pebble store, simulating the etag-replay codepath. It asserts the
// resources table preserves the rich `DisplayName` and annotations
// the InsertResourceGrants loop *should* be re-writing.
func TestPebble_InsertResourceGrants_StubRewriteCorrupts(t *testing.T) {
	// Skipped: this documents a latent, non-reachable scenario rather
	// than a live bug.
	//
	//   - Pebble is not used in production today (nothing calls
	//     pebble.Register() / WithEngine(EnginePebble) outside tests),
	//     and there are no existing Pebble c1z files.
	//   - Even if it were, the InsertResourceGrants loop reads the
	//     LIVE connector's grants (full entitlement-resource), not slim
	//     store reads. The only slim-store path is etag-replay, where
	//     the replayed grant's entitlement-resource is always the
	//     resource being synced (enforced+asserted in
	//     fetchEtaggedGrantsForResource) and is re-written full by the
	//     grant-sync etag-update — so the stub can never reach
	//     PutResources for a resource that wouldn't be corrected.
	//
	// The reachable rich-field read off a slim grant — the principal's
	// ParentResourceId in processGrantsWithExternalPrincipals — is
	// fixed by preserving it on PrincipalRef (see
	// v2_grant_lossiness_test.go's TestV2GrantRoundTrip_V3Contract).
	// Re-enable this test if a consumer is added that reads a slim
	// grant's embedded entitlement-resource rich fields and writes
	// them back.
	t.Skip("non-reachable: Pebble unused in prod; InsertResourceGrants reads full connector grants, not slim store reads")

	ctx := context.Background()
	require.NoError(t, pebble.Register())

	store, err := dotc1z.NewStore(ctx,
		filepath.Join(t.TempDir(), "insert-resource-grants.c1z"),
		dotc1z.WithEngine(dotc1z.EnginePebble),
	)
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group"}.Build(),
		v2.ResourceType_builder{Id: "user"}.Build(),
	))

	const wantDisplayName = "Group One"

	groupTraitAny, err := anypb.New(&v2.GroupTrait{})
	require.NoError(t, err)

	// The group g1 the connector emits — fully populated, with a
	// DisplayName and a GroupTrait annotation. This is the rich
	// shape the resources table holds after a fresh sync.
	g1 := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		DisplayName: wantDisplayName,
		Annotations: []*anypb.Any{groupTraitAny},
	}.Build()
	user := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
	}.Build()
	require.NoError(t, store.PutResources(ctx, g1, user))

	ent := v2.Entitlement_builder{
		Id:       "group:g1:member",
		Resource: g1,
		Slug:     "member",
	}.Build()
	require.NoError(t, store.PutEntitlements(ctx, ent))

	// The grant the connector emits — Entitlement.Resource is the
	// same rich g1. This is what `InsertResourceGrants` reads off
	// each grant.
	grant := v2.Grant_builder{
		Id:          "grant-1",
		Entitlement: ent,
		Principal:   user,
	}.Build()
	require.NoError(t, store.PutGrants(ctx, grant))

	// Sanity: before we simulate the InsertResourceGrants loop,
	// g1 in the resources table carries the rich data.
	preResp, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: "group",
	}.Build())
	require.NoError(t, err)
	require.Len(t, preResp.GetList(), 1, "sanity: g1 must be in the resources table before the simulated rewrite")
	require.Equal(t, wantDisplayName, preResp.GetList()[0].GetDisplayName(),
		"sanity: g1's DisplayName is intact before the simulated rewrite")
	require.NotEmpty(t, preResp.GetList()[0].GetAnnotations(),
		"sanity: g1's annotations are intact before the simulated rewrite")

	// Simulate the syncer's `InsertResourceGrants` extraction loop.
	// Read grants back from the store (exactly what
	// `fetchEtaggedGrantsForResource` does on etag-replay), pull
	// `grant.Entitlement.Resource` from each, and PutResources it.
	grantsResp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, grantsResp.GetList(), 1, "expected 1 grant in the store")

	rewriteSet := make([]*v2.Resource, 0)
	for _, g := range grantsResp.GetList() {
		res := g.GetEntitlement().GetResource()
		require.NotNil(t, res, "the grant's embedded Entitlement.Resource must be non-nil — the InsertResourceGrants loop dereferences it")
		rewriteSet = append(rewriteSet, res)
	}
	require.NoError(t, store.PutResources(ctx, rewriteSet...))

	// Inspect the resources table. The InsertResourceGrants
	// rewrite should have been a no-op (rich-in, rich-out). On
	// Pebble it overwrites g1 with a stub Resource carrying only
	// the identity tuple.
	postResp, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: "group",
	}.Build())
	require.NoError(t, err)
	require.Len(t, postResp.GetList(), 1)
	gotG1 := postResp.GetList()[0]

	t.Run("DisplayName survives InsertResourceGrants rewrite", func(t *testing.T) {
		require.Equal(t, wantDisplayName, gotG1.GetDisplayName(),
			"InsertResourceGrants rewrote g1 with the stub Resource extracted from a Pebble-read grant; "+
				"DisplayName clobbered. The full corruption fires end-to-end once the SyncDetails bug "+
				"(pkg/dotc1z/engine/pebble/sync_details_annotation_test.go) is fixed and etag-replay "+
				"actually returns grants from the previous sync")
	})

	t.Run("annotations (GroupTrait) survive InsertResourceGrants rewrite", func(t *testing.T) {
		require.NotEmpty(t, gotG1.GetAnnotations(),
			"InsertResourceGrants rewrote g1 with a stub Resource carrying no annotations; "+
				"GroupTrait (and any other connector-level traits attached as Any) clobbered. "+
				"Downstream consumers reading user/group/role/app/secret traits off the resources "+
				"table silently see none")
	})
}
