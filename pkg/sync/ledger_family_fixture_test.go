package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"fmt"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type ledgerFamilyConnector struct{ *mockConnector }

func newLedgerFamilyConnector(t *testing.T) *ledgerFamilyConnector {
	t.Helper()
	c := &ledgerFamilyConnector{newMockConnector()}
	c.rtDB = []*v2.ResourceType{userResourceType, groupResourceType}
	for i := range 2 {
		_, err := c.AddUser(t.Context(), fmt.Sprintf("user-%d", i))
		require.NoError(t, err)
	}
	for i := range 2 {
		group, member, err := c.AddGroup(t.Context(), fmt.Sprintf("group-%d", i))
		require.NoError(t, err)
		second := proto.Clone(member).(*v2.Entitlement)
		second.SetId(member.GetId() + "-viewer")
		second.SetSlug("viewer")
		c.entDB[group.GetId().GetResource()] = append(c.entDB[group.GetId().GetResource()], second)
		for _, user := range c.resourceDB[userResourceType.GetId()] {
			c.AddGroupMember(t.Context(), group, user)
		}
	}
	return c
}

func ledgerFamilyPage[T any](list []T, cursor string) ([]T, string) {
	if len(list) == 0 {
		return nil, ""
	}
	if cursor == "" {
		if len(list) > 1 {
			return list[:1], "1"
		}
		return list, ""
	}
	return list[1:], ""
}
func (c *ledgerFamilyConnector) ListResourceTypes(
	_ context.Context, r *v2.ResourceTypesServiceListResourceTypesRequest, _ ...grpc.CallOption,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	list, next := ledgerFamilyPage(c.rtDB, r.GetPageToken())
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{List: list, NextPageToken: next}.Build(), nil
}
func (c *ledgerFamilyConnector) ListResources(_ context.Context, r *v2.ResourcesServiceListResourcesRequest, _ ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	list, next := ledgerFamilyPage(c.resourceDB[r.GetResourceTypeId()], r.GetPageToken())
	return v2.ResourcesServiceListResourcesResponse_builder{List: list, NextPageToken: next}.Build(), nil
}
func (c *ledgerFamilyConnector) ListEntitlements(_ context.Context, r *v2.EntitlementsServiceListEntitlementsRequest, _ ...grpc.CallOption) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	list, next := ledgerFamilyPage(c.entDB[r.GetResource().GetId().GetResource()], r.GetPageToken())
	return v2.EntitlementsServiceListEntitlementsResponse_builder{List: list, NextPageToken: next}.Build(), nil
}
func (c *ledgerFamilyConnector) ListStaticEntitlements(
	_ context.Context, r *v2.EntitlementsServiceListStaticEntitlementsRequest, _ ...grpc.CallOption,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	list, next := ledgerFamilyPage([]*v2.Entitlement{{Slug: "static-a"}, {Slug: "static-b"}}, r.GetPageToken())
	return v2.EntitlementsServiceListStaticEntitlementsResponse_builder{List: list, NextPageToken: next}.Build(), nil
}
func (c *ledgerFamilyConnector) ListGrants(_ context.Context, r *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	list, next := ledgerFamilyPage(c.grantDB[r.GetResource().GetId().GetResource()], r.GetPageToken())
	return v2.GrantsServiceListGrantsResponse_builder{List: list, NextPageToken: next}.Build(), nil
}
