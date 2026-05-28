package dotc1z

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func TestListResourceTypes(t *testing.T) {
	ctx := t.Context()
	testFilePath := filepath.Join(t.TempDir(), "resource_types_test.c1z")

	f, err := NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)

	sync1ID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, f.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
	))
	require.NoError(t, f.EndSync(ctx))

	sync2ID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, f.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "app", DisplayName: "Application"}.Build(),
	))
	require.NoError(t, f.EndSync(ctx))

	require.NoError(t, f.Close(ctx))

	f, err = NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, f.Close(ctx))
	})

	t.Run("without ActiveSyncId", func(t *testing.T) {
		resp, err := f.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
		require.NoError(t, err)
		require.Equal(t, []string{"app"}, resourceTypeIDs(resp.GetList()))
	})

	t.Run("with ActiveSyncId for first sync", func(t *testing.T) {
		resp, err := f.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
			ActiveSyncId: sync1ID,
		}.Build())
		require.NoError(t, err)
		require.Equal(t, []string{"group", "user"}, resourceTypeIDs(resp.GetList()))
	})

	t.Run("with ActiveSyncId for latest sync", func(t *testing.T) {
		resp, err := f.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
			ActiveSyncId: sync2ID,
		}.Build())
		require.NoError(t, err)
		require.Equal(t, []string{"app"}, resourceTypeIDs(resp.GetList()))
	})
}

func resourceTypeIDs(resourceTypes []*v2.ResourceType) []string {
	ids := make([]string, 0, len(resourceTypes))
	for _, rt := range resourceTypes {
		ids = append(ids, rt.GetId())
	}
	slices.Sort(ids)
	return ids
}
