package pebble_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestV2FieldPreservation locks in V2↔V3 round-trip for the fields
// the 2026-05-27 audit found silently dropped on Pebble:
//
//   - Entitlement.slug         (used by syncer + reports)
//   - Entitlement.grantable_to (used by syncer's principal-type narrowing)
//   - ResourceType.description (informational, on v2 wire contract)
//   - ResourceType.sourced_externally (source attribution flag)
//
// Each subtest writes one record carrying the field and asserts the
// read path returns it intact. SQLite preserved these implicitly via
// the data blob; v3 needs dedicated fields since refs are identity-
// only.
func TestV2FieldPreservation(t *testing.T) {
	ctx := context.Background()
	store, err := dotc1z.NewStore(ctx, filepath.Join(t.TempDir(), "preserve.c1z"), dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	_ = syncID

	rt := v2.ResourceType_builder{
		Id:                "employee",
		DisplayName:       "Employee",
		Description:       "Full-time staff with desk access",
		SourcedExternally: true,
		Traits:            []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
	}.Build()
	rtRole := v2.ResourceType_builder{
		Id:          "role",
		DisplayName: "Role",
	}.Build()
	require.NoError(t, store.PutResourceTypes(ctx, rt, rtRole))

	app := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "app", Resource: "gh"}.Build(),
	}.Build()
	ent := v2.Entitlement_builder{
		Id:          "ent-A",
		Resource:    app,
		Slug:        "admin",
		Description: "Admins of the GitHub app",
		Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		GrantableTo: []*v2.ResourceType{
			v2.ResourceType_builder{Id: "employee"}.Build(),
			v2.ResourceType_builder{Id: "role"}.Build(),
		},
	}.Build()
	require.NoError(t, store.PutEntitlements(ctx, ent))
	require.NoError(t, store.EndSync(ctx))

	t.Run("ResourceType description + sourced_externally preserved", func(t *testing.T) {
		resp, err := store.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
		require.NoError(t, err)
		var got *v2.ResourceType
		for _, r := range resp.GetList() {
			if r.GetId() == "employee" {
				got = r
				break
			}
		}
		require.NotNil(t, got, "employee resource type missing from list")
		require.Equal(t, "Full-time staff with desk access", got.GetDescription())
		require.True(t, got.GetSourcedExternally())
		require.Equal(t, "Employee", got.GetDisplayName())
	})

	t.Run("Entitlement slug + grantable_to preserved", func(t *testing.T) {
		resp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 1)
		got := resp.GetList()[0]
		require.Equal(t, "admin", got.GetSlug())
		require.Equal(t, "Admins of the GitHub app", got.GetDescription())
		require.Len(t, got.GetGrantableTo(), 2)
		ids := []string{got.GetGrantableTo()[0].GetId(), got.GetGrantableTo()[1].GetId()}
		require.Contains(t, ids, "employee")
		require.Contains(t, ids, "role")
	})
}
