package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

// managedDeviceResourceType mirrors a real opt-in resource type (e.g.
// baton-jamf's managedDevice): it carries the OptInRequired annotation and so
// must stay off a default local/CLI sync until an operator names it.
var managedDeviceResourceType = v2.ResourceType_builder{
	Id:          "managed_device",
	DisplayName: "Managed Device",
	Annotations: annotations.New(&v2.OptInRequired{}),
}.Build()

// TestShouldSyncResourceType exercises the inclusion decision directly.
func TestShouldSyncResourceType(t *testing.T) {
	tests := []struct {
		name             string
		syncResourceType []string
		rt               *v2.ResourceType
		want             bool
	}{
		{
			name:             "no filter, no opt-in annotation -> included",
			syncResourceType: nil,
			rt:               groupResourceType,
			want:             true,
		},
		{
			name:             "no filter, opt-in-required -> excluded",
			syncResourceType: nil,
			rt:               managedDeviceResourceType,
			want:             false,
		},
		{
			name:             "explicit filter selects opt-in-required -> included (this is the opt-in)",
			syncResourceType: []string{"managed_device"},
			rt:               managedDeviceResourceType,
			want:             true,
		},
		{
			name:             "explicit filter omits opt-in-required -> excluded",
			syncResourceType: []string{"group"},
			rt:               managedDeviceResourceType,
			want:             false,
		},
		{
			name:             "explicit filter omits a normal type -> excluded",
			syncResourceType: []string{"group"},
			rt:               userResourceType,
			want:             false,
		},
		{
			name:             "explicit filter selects a normal type -> included",
			syncResourceType: []string{"user"},
			rt:               userResourceType,
			want:             true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &syncer{syncResourceTypes: tt.syncResourceType}
			require.Equal(t, tt.want, s.shouldSyncResourceType(tt.rt))
		})
	}
}

// collectResourceTypeIDs reads back every resource type persisted to a c1z.
func collectResourceTypeIDs(t *testing.T, ctx context.Context, c1zPath string) map[string]struct{} {
	t.Helper()
	store, err := dotc1z.NewC1ZFile(ctx, c1zPath)
	require.NoError(t, err)
	defer store.Close(ctx)

	ids := make(map[string]struct{})
	pageToken := ""
	for {
		resp, err := store.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		for _, rt := range resp.GetList() {
			ids[rt.GetId()] = struct{}{}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return ids
}

// TestOptInRequiredResourceTypeDefaultSyncExcluded is the regression guard: on a
// default (empty-filter) local/CLI sync, a resource type carrying OptInRequired
// must NOT be persisted. This FAILS on pre-fix code, which included every
// advertised resource type on the default path.
func TestOptInRequiredResourceTypeDefaultSyncExcluded(t *testing.T) {
	runWithSyncModes(t, func(t *testing.T, extraOpts []SyncOpt) {
		ctx := t.Context()

		tempDir, err := os.MkdirTemp("", "baton-optin-default-sync-test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)
		c1zPath := filepath.Join(tempDir, "optin-default-sync.c1z")

		mc := newMockConnector()
		mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType, managedDeviceResourceType)

		opts := append([]SyncOpt{WithC1ZPath(c1zPath), WithTmpDir(tempDir)}, extraOpts...)
		syncer, err := NewSyncer(ctx, mc, opts...)
		require.NoError(t, err)
		require.NoError(t, syncer.Sync(ctx))
		require.NoError(t, syncer.Close(ctx))

		ids := collectResourceTypeIDs(t, ctx, c1zPath)
		require.NotContains(t, ids, "managed_device", "opt-in-required type must be excluded from a default sync")
		require.Contains(t, ids, "group", "non-opt-in type must still sync (backward compat)")
		require.Contains(t, ids, "user", "non-opt-in type must still sync (backward compat)")
	})
}

// TestOptInRequiredResourceTypeExplicitlySelected verifies the opt-in path:
// naming the opt-in-required type via --sync-resource-types includes it.
func TestOptInRequiredResourceTypeExplicitlySelected(t *testing.T) {
	runWithSyncModes(t, func(t *testing.T, extraOpts []SyncOpt) {
		ctx := t.Context()

		tempDir, err := os.MkdirTemp("", "baton-optin-explicit-sync-test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)
		c1zPath := filepath.Join(tempDir, "optin-explicit-sync.c1z")

		mc := newMockConnector()
		mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType, managedDeviceResourceType)

		opts := append([]SyncOpt{
			WithC1ZPath(c1zPath),
			WithTmpDir(tempDir),
			WithSyncResourceTypes([]string{"managed_device"}),
		}, extraOpts...)
		syncer, err := NewSyncer(ctx, mc, opts...)
		require.NoError(t, err)
		require.NoError(t, syncer.Sync(ctx))
		require.NoError(t, syncer.Close(ctx))

		ids := collectResourceTypeIDs(t, ctx, c1zPath)
		require.Contains(t, ids, "managed_device", "explicitly selected opt-in type must sync")
		require.NotContains(t, ids, "group", "types outside an explicit filter must not sync")
		require.NotContains(t, ids, "user", "types outside an explicit filter must not sync")
	})
}
