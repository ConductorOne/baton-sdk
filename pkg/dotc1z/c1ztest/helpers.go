package c1ztest

import (
	"context"
	"fmt"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

type C1ZCounts struct {
	ResourceTypeCount int
	ResourceCount     int
	UserCount         int
	EntitlementCount  int
	GrantCount        int
}

func CreateTestSync(ctx context.Context, t *testing.T, f *dotc1z.C1File, counts C1ZCounts) (string, error) {
	// Add a sync
	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	for i := range counts.ResourceTypeCount {
		err = f.PutResourceTypes(ctx, v2.ResourceType_builder{
			Id:          fmt.Sprintf("rt-%d", i),
			DisplayName: fmt.Sprintf("Resource Type %d", i),
		}.Build())
		require.NoError(t, err)
	}

	for i := range counts.ResourceCount {
		err = f.PutResources(ctx, v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: fmt.Sprintf("rt-%d", i%counts.ResourceTypeCount),
				Resource:     fmt.Sprintf("resource-%d", i),
			}.Build(),
		}.Build())
		require.NoError(t, err)
	}

	for i := range counts.UserCount {
		err = f.PutResources(ctx, v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     fmt.Sprintf("user-%d", i),
			}.Build(),
		}.Build())
		require.NoError(t, err)
	}

	for i := range counts.EntitlementCount {
		err = f.PutEntitlements(ctx, v2.Entitlement_builder{
			Id: fmt.Sprintf("ent-%d", i),
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: fmt.Sprintf("rt-%d", i%counts.ResourceTypeCount),
					Resource:     fmt.Sprintf("resource-%d", i%counts.ResourceCount),
				}.Build(),
			}.Build(),
		}.Build())
		require.NoError(t, err)
	}

	for i := range counts.GrantCount {
		err = f.PutGrants(ctx, v2.Grant_builder{
			Id: fmt.Sprintf("grant-%d", i),
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "user",
					Resource:     fmt.Sprintf("user-%d", i%counts.UserCount),
				}.Build(),
			}.Build(),
			Entitlement: v2.Entitlement_builder{
				Id: fmt.Sprintf("ent-%d", i%counts.EntitlementCount),
				Resource: v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: fmt.Sprintf("rt-%d", i%counts.ResourceTypeCount),
						Resource:     fmt.Sprintf("resource-%d", i%counts.ResourceCount),
					}.Build(),
				}.Build(),
			}.Build(),
		}.Build())
		require.NoError(t, err)
	}

	// Delete 25% of grants.
	for i := 0; i < counts.GrantCount; i += 4 {
		err = f.DeleteGrant(ctx, fmt.Sprintf("grant-%d", i))
		require.NoError(t, err)
	}

	err = f.EndSync(ctx)
	require.NoError(t, err)

	return syncID, nil
}
