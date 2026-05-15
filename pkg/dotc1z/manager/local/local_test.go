package local

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func TestWithV2GrantsWriter_SetsField(t *testing.T) {
	lm := &localManager{}
	require.False(t, lm.v2GrantsWriter)
	WithV2GrantsWriter(true)(lm)
	require.True(t, lm.v2GrantsWriter)
	WithV2GrantsWriter(false)(lm)
	require.False(t, lm.v2GrantsWriter)
}

func TestLoadC1Z_SlimWriterOptionPropagates(t *testing.T) {
	// Proves the option reaches the underlying C1File's writer, not
	// just the localManager struct.
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "slim.c1z")

	m, err := New(ctx, path, WithV2GrantsWriter(true))
	require.NoError(t, err)

	c1f, err := m.LoadC1Z(ctx)
	require.NoError(t, err)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	groupRT := v2.ResourceType_builder{Id: "group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user"}.Build()
	require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "Group 1"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "User 1"}.Build()
	require.NoError(t, c1f.PutResources(ctx, g1, u1))

	ent := v2.Entitlement_builder{
		Id:          "ent-1",
		Resource:    g1,
		DisplayName: "Read",
		Description: "Read access",
		Slug:        "read",
	}.Build()
	require.NoError(t, c1f.PutEntitlements(ctx, ent))

	grant := v2.Grant_builder{
		Id:          "ent-1:user:u1",
		Entitlement: ent,
		Principal:   u1,
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, grant))
	require.NoError(t, c1f.EndSync(ctx))

	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	read := resp.GetList()[0]

	require.Equal(t, "ent-1", read.GetEntitlement().GetId())
	require.Empty(t, read.GetEntitlement().GetDisplayName(), "slim writer should drop DisplayName from embedded entitlement; reader stub leaves it empty")
	require.Empty(t, read.GetEntitlement().GetDescription())
	require.Empty(t, read.GetEntitlement().GetSlug())
	require.Equal(t, "user", read.GetPrincipal().GetId().GetResourceType())
	require.Equal(t, "u1", read.GetPrincipal().GetId().GetResource())
	require.Empty(t, read.GetPrincipal().GetDisplayName())

	_ = syncID
}

func TestLoadC1Z_WithoutSlimOptionWritesFullBlob(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "full.c1z")

	m, err := New(ctx, path)
	require.NoError(t, err)

	c1f, err := m.LoadC1Z(ctx)
	require.NoError(t, err)

	_, err = c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	groupRT := v2.ResourceType_builder{Id: "group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user"}.Build()
	require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "Group 1"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "User 1"}.Build()
	require.NoError(t, c1f.PutResources(ctx, g1, u1))

	ent := v2.Entitlement_builder{
		Id:          "ent-1",
		Resource:    g1,
		DisplayName: "Read",
		Description: "Read access",
		Slug:        "read",
	}.Build()
	require.NoError(t, c1f.PutEntitlements(ctx, ent))

	grant := v2.Grant_builder{
		Id:          "ent-1:user:u1",
		Entitlement: ent,
		Principal:   u1,
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, grant))
	require.NoError(t, c1f.EndSync(ctx))

	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	read := resp.GetList()[0]

	require.Equal(t, "Read", read.GetEntitlement().GetDisplayName(), "full writer should preserve DisplayName")
	require.Equal(t, "Read access", read.GetEntitlement().GetDescription())
	require.Equal(t, "read", read.GetEntitlement().GetSlug())
	require.Equal(t, "User 1", read.GetPrincipal().GetDisplayName())
}
