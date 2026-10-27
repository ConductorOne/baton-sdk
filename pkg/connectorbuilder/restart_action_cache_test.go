package connectorbuilder

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

// failingListSyncer answers every list call with fail.
type failingListSyncer struct {
	testResourceSyncerV2Simple
	fail error
}

func (s *failingListSyncer) List(context.Context, *v2.ResourceId, resource.SyncOpAttrs) ([]*v2.Resource, *resource.SyncOpResults, error) {
	return nil, nil, s.fail
}

func (s *failingListSyncer) Entitlements(context.Context, *v2.Resource, resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	return nil, nil, s.fail
}

func (s *failingListSyncer) Grants(context.Context, *v2.Resource, resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	return nil, nil, s.fail
}

func (s *failingListSyncer) StaticEntitlements(context.Context, resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	return nil, nil, s.fail
}

// A list call that invalidates its page token clears the uhttp caches, so
// the restart page is fetched from the provider instead of replayed from a
// cached 200. Any other list error leaves the caches alone.
func TestListCallInvalidationClearsHTTPCaches(t *testing.T) {
	ctx := t.Context()
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"value":[],"@odata.nextLink":"page2"}`))
	}))
	defer server.Close()
	client, err := uhttp.NewBaseHttpClientWithContext(ctx, server.Client())
	require.NoError(t, err)
	target, err := url.Parse(server.URL + "/list")
	require.NoError(t, err)
	get := func() {
		req, err := client.NewRequest(ctx, http.MethodGet, target)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}
	get()
	get()
	if hits.Load() != 1 {
		t.Skip("uhttp cache disabled in this environment")
	}

	resourceType := "group"
	resourceRef := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: resourceType, Resource: "g1"}.Build()}.Build()
	calls := map[string]func(ctx context.Context, c types.ConnectorServer) error{
		"list-resources": func(ctx context.Context, c types.ConnectorServer) error {
			_, err := c.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{ResourceTypeId: resourceType}.Build())
			return err
		},
		"list-entitlements": func(ctx context.Context, c types.ConnectorServer) error {
			_, err := c.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{Resource: resourceRef}.Build())
			return err
		},
		"list-grants": func(ctx context.Context, c types.ConnectorServer) error {
			_, err := c.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{Resource: resourceRef}.Build())
			return err
		},
		"list-static-entitlements": func(ctx context.Context, c types.ConnectorServer) error {
			_, err := c.ListStaticEntitlements(ctx, v2.EntitlementsServiceListStaticEntitlementsRequest_builder{ResourceTypeId: resourceType}.Build())
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			t.Run("other error keeps the cache", func(t *testing.T) {
				syncer := &failingListSyncer{testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: resourceType}, fail: errors.New("boom")}
				connector, err := NewConnector(ctx, newTestConnectorV2([]ResourceSyncerV2{syncer}))
				require.NoError(t, err)
				before := hits.Load()
				require.Error(t, call(ctx, connector))
				get()
				require.Equal(t, before, hits.Load(), "cached")
			})
			t.Run("invalidation clears the cache", func(t *testing.T) {
				syncer := &failingListSyncer{testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: resourceType}, fail: pagination.NewRestartActionError("", "expired")}
				connector, err := NewConnector(ctx, newTestConnectorV2([]ResourceSyncerV2{syncer}))
				require.NoError(t, err)
				err = call(ctx, connector)
				_, invalidates := pagination.RestartActionFromError(err)
				require.True(t, invalidates, "the detail survives the builder's wrap: %v", err)
				before := hits.Load()
				get()
				require.Equal(t, before+1, hits.Load(), "fetched again")
			})
		})
	}
}
