package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

func TestBucketGrants_DifferentEntitlementShowsAsCreatedAndDeleted(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "diff.c1z")

	resTypes := []*v2.ResourceType{
		v2.ResourceType_builder{Id: "group"}.Build(),
		v2.ResourceType_builder{Id: "user"}.Build(),
	}
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent-1", Resource: g1}.Build()
	ent2 := v2.Entitlement_builder{Id: "ent-2", Resource: g1}.Build()
	grant1 := v2.Grant_builder{Id: "ent-1:user:u1", Entitlement: ent1, Principal: u1}.Build()
	grant2 := v2.Grant_builder{Id: "ent-2:user:u1", Entitlement: ent2, Principal: u1}.Build()

	oldSyncID := writeOneSync(ctx, t, path, resTypes, []*v2.Resource{g1, u1}, []*v2.Entitlement{ent1, ent2}, []*v2.Grant{grant1})
	newSyncID := writeOneSync(ctx, t, path, resTypes, []*v2.Resource{g1, u1}, []*v2.Entitlement{ent1, ent2}, []*v2.Grant{grant2})

	c1f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	defer func() { _ = c1f.Close(ctx) }()

	diff, err := bucketGrants(ctx, c1f, oldSyncID, newSyncID)
	require.NoError(t, err)
	require.Len(t, diff.GetCreated(), 1)
	require.Len(t, diff.GetDeleted(), 1)
	require.Empty(t, diff.GetModified())
}

// Calling this twice on the same path lands both syncs in the same
// c1z, which is what bucketGrants needs to diff across them.
func writeOneSync(
	ctx context.Context,
	t *testing.T,
	path string,
	resTypes []*v2.ResourceType,
	resources []*v2.Resource,
	entitlements []*v2.Entitlement,
	grants []*v2.Grant,
) string {
	t.Helper()

	c1f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, c1f.PutResourceTypes(ctx, resTypes...))
	require.NoError(t, c1f.PutResources(ctx, resources...))
	require.NoError(t, c1f.PutEntitlements(ctx, entitlements...))
	require.NoError(t, c1f.PutGrants(ctx, grants...))
	require.NoError(t, c1f.EndSync(ctx))
	require.NoError(t, c1f.Close(ctx))
	return syncID
}
