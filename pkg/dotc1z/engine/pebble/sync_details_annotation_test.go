package pebble_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	c1zpb "github.com/conductorone/baton-sdk/pb/c1/c1z/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// TestPebble_ListGrants_HonorsSyncDetailsAnnotation is the regression
// guard for `Adapter.resolveActiveSync` honoring the `c1zpb.SyncDetails`
// annotation on incoming requests: a ListGrants request carrying
// SyncDetails{Id: ...} and NO ActiveSyncId must resolve (without error)
// and return the file's sync grants.
//
// A v3 Pebble c1z holds exactly one sync by contract, so the annotation
// can only point at that sync — the cross-sync etag-replay use case the
// SQLite engine supports (reading a PREVIOUS sync distinct from the
// in-progress one) does not apply, because a new sync replaces the prior
// one in place. The annotation must still be parsed and honored rather
// than rejected (a malformed SyncDetails surfaces an error instead).
func TestPebble_ListGrants_HonorsSyncDetailsAnnotation(t *testing.T) {
	ctx := context.Background()

	store, err := dotc1z.NewStore(ctx,
		filepath.Join(t.TempDir(), "syncdetails.c1z"),
		dotc1z.WithEngine(dotc1z.EnginePebble),
	)
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group"}.Build(),
		v2.ResourceType_builder{Id: "user"}.Build(),
	))
	group := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	user := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
	}.Build()
	require.NoError(t, store.PutResources(ctx, group, user))

	ent := v2.Entitlement_builder{
		Id:       "group:g1:member",
		Resource: group,
		Slug:     "member",
	}.Build()
	require.NoError(t, store.PutEntitlements(ctx, ent))

	grant := v2.Grant_builder{
		Id:          "grant-sync1",
		Entitlement: ent,
		Principal:   user,
	}.Build()
	require.NoError(t, store.PutGrants(ctx, grant))
	require.NoError(t, store.EndSync(ctx))

	// SyncDetails annotation carrying the sync ID, NO ActiveSyncId set —
	// the shape the syncer builds. The Resource filter is intentionally
	// omitted to isolate the SyncDetails annotation behavior.
	annos := annotations.Annotations{}
	annos.Update(c1zpb.SyncDetails_builder{Id: syncID}.Build())
	req := v2.GrantsServiceListGrantsRequest_builder{
		Annotations: annos,
	}.Build()

	resp, err := store.ListGrants(ctx, req)
	require.NoError(t, err, "ListGrants should honor a SyncDetails annotation pointing at the file's sync")
	require.Len(t, resp.GetList(), 1)
	require.Equal(t, "group:g1:member:user:u1", resp.GetList()[0].GetId())
}
