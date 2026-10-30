package sync

import (
	"context"
	"io"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

func (s *chaosPebbleStore) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	return s.Engine.Records().PutGrants(ctx, grants...)
}

func (s *chaosPebbleStore) UnsafePutUniqueGrants(ctx context.Context, grants ...*v2.Grant) error {
	return s.Engine.Records().UnsafePutUniqueGrants(ctx, grants...)
}

func (s *chaosPebbleStore) PutResourceTypes(ctx context.Context, rts ...*v2.ResourceType) error {
	return s.Engine.Records().PutResourceTypes(ctx, rts...)
}

func (s *chaosPebbleStore) PutResources(ctx context.Context, resources ...*v2.Resource) error {
	return s.Engine.Records().PutResources(ctx, resources...)
}

func (s *chaosPebbleStore) PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error {
	return s.Engine.Records().PutEntitlements(ctx, entitlements...)
}

func (s *chaosPebbleStore) DeleteGrant(ctx context.Context, grantID string) error {
	return s.Engine.Records().DeleteGrant(ctx, grantID)
}

func (s *chaosPebbleStore) DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error {
	return s.Engine.Records().DeleteGrantByRefs(ctx, grant)
}

func (s *chaosPebbleStore) DeleteGrantsByRefs(ctx context.Context, grants ...*v2.Grant) error {
	return s.Engine.Records().DeleteGrantsByRefs(ctx, grants...)
}

func (s *chaosPebbleStore) PutAsset(ctx context.Context, assetRef *v2.AssetRef, contentType string, data []byte) error {
	return s.Engine.Records().PutAsset(ctx, assetRef, contentType, data)
}

func (s *chaosPebbleStore) GetAsset(ctx context.Context, req *v2.AssetServiceGetAssetRequest) (string, io.Reader, error) {
	return s.Engine.Records().GetAsset(ctx, req)
}

func (s *chaosPebbleStore) ListGrants(ctx context.Context, req *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	return s.Engine.Records().ListGrants(ctx, req)
}

func (s *chaosPebbleStore) ListResources(ctx context.Context, req *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	return s.Engine.Records().ListResources(ctx, req)
}

func (s *chaosPebbleStore) ListResourceTypes(ctx context.Context, req *v2.ResourceTypesServiceListResourceTypesRequest) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return s.Engine.Records().ListResourceTypes(ctx, req)
}

func (s *chaosPebbleStore) ListEntitlements(ctx context.Context, req *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	return s.Engine.Records().ListEntitlements(ctx, req)
}

func (s *chaosPebbleStore) ListStaticEntitlements(
	ctx context.Context,
	req *v2.EntitlementsServiceListStaticEntitlementsRequest,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	return s.Engine.Records().ListStaticEntitlements(ctx, req)
}

func (s *chaosPebbleStore) GetGrant(ctx context.Context, req *reader_v2.GrantsReaderServiceGetGrantRequest) (*reader_v2.GrantsReaderServiceGetGrantResponse, error) {
	return s.Engine.Records().GetGrant(ctx, req)
}

func (s *chaosPebbleStore) GetEntitlement(ctx context.Context, req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	return s.Engine.Records().GetEntitlement(ctx, req)
}

func (s *chaosPebbleStore) GetResource(ctx context.Context, req *reader_v2.ResourcesReaderServiceGetResourceRequest) (*reader_v2.ResourcesReaderServiceGetResourceResponse, error) {
	return s.Engine.Records().GetResource(ctx, req)
}

func (s *chaosPebbleStore) GetResourceType(ctx context.Context, req *reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest) (*reader_v2.ResourceTypesReaderServiceGetResourceTypeResponse, error) {
	return s.Engine.Records().GetResourceType(ctx, req)
}

//nolint:revive // method name mirrors the protobuf-generated gRPC server interface
func (s *chaosPebbleStore) ListResourcesByIds(
	ctx context.Context,
	req *reader_v2.ResourcesReaderServiceListResourcesByIdsRequest,
) (*reader_v2.ResourcesReaderServiceListResourcesByIdsResponse, error) {
	return s.Engine.Records().ListResourcesByIds(ctx, req)
}

//nolint:revive // method name mirrors the protobuf-generated gRPC server interface
func (s *chaosPebbleStore) ListEntitlementsByIds(
	ctx context.Context,
	req *reader_v2.EntitlementsReaderServiceListEntitlementsByIdsRequest,
) (*reader_v2.EntitlementsReaderServiceListEntitlementsByIdsResponse, error) {
	return s.Engine.Records().ListEntitlementsByIds(ctx, req)
}

func (s *chaosPebbleStore) GrantsForEntitlementPrincipalSorted() bool {
	return s.Engine.Records().GrantsForEntitlementPrincipalSorted()
}

func (s *chaosPebbleStore) ListGrantsForEntitlement(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	return s.Engine.Records().ListGrantsForEntitlement(ctx, req)
}

func (s *chaosPebbleStore) ListGrantPrincipalKeysForEntitlement(
	ctx context.Context,
	entitlement *v2.Entitlement,
	pageToken string,
	pageSize uint32,
) ([]string, string, error) {
	return s.Engine.Records().ListGrantPrincipalKeysForEntitlement(ctx, entitlement, pageToken, pageSize)
}

func (s *chaosPebbleStore) ListGrantsForPrincipal(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForPrincipalRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForPrincipalResponse, error) {
	return s.Engine.Records().ListGrantsForPrincipal(ctx, req)
}

func (s *chaosPebbleStore) ListGrantsForResourceType(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForResourceTypeResponse, error) {
	return s.Engine.Records().ListGrantsForResourceType(ctx, req)
}

func (s *chaosPebbleStore) ListGrantsForEntitlements(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementsResponse, error) {
	return s.Engine.Records().ListGrantsForEntitlements(ctx, req)
}

func (s *chaosPebbleStore) PutResourceRecord(ctx context.Context, rec *v3.ResourceRecord) error {
	return s.Engine.Records().PutResourceRecord(ctx, rec)
}

func (s *chaosPebbleStore) PutResourceRecords(ctx context.Context, records ...*v3.ResourceRecord) error {
	return s.Engine.Records().PutResourceRecords(ctx, records...)
}

func (s *chaosPebbleStore) GetResourceRecord(ctx context.Context, resourceTypeID, resourceID string) (*v3.ResourceRecord, error) {
	return s.Engine.Records().GetResourceRecord(ctx, resourceTypeID, resourceID)
}

func (s *chaosPebbleStore) DeleteResourceRecord(ctx context.Context, resourceTypeID, resourceID string) error {
	return s.Engine.Records().DeleteResourceRecord(ctx, resourceTypeID, resourceID)
}

func (s *chaosPebbleStore) IterateResources(ctx context.Context, yield func(*v3.ResourceRecord) bool) error {
	return s.Engine.Records().IterateResources(ctx, yield)
}

func (s *chaosPebbleStore) IterateResourcesByParent(ctx context.Context, parentRT, parentID string, yield func(*v3.ResourceRecord) bool) error {
	return s.Engine.Records().IterateResourcesByParent(ctx, parentRT, parentID, yield)
}

func (s *chaosPebbleStore) PutEntitlementRecord(ctx context.Context, rec *v3.EntitlementRecord) error {
	return s.Engine.Records().PutEntitlementRecord(ctx, rec)
}

func (s *chaosPebbleStore) PutEntitlementRecords(ctx context.Context, records ...*v3.EntitlementRecord) error {
	return s.Engine.Records().PutEntitlementRecords(ctx, records...)
}

func (s *chaosPebbleStore) GetEntitlementRecord(ctx context.Context, externalID string) (*v3.EntitlementRecord, error) {
	return s.Engine.Records().GetEntitlementRecord(ctx, externalID)
}

func (s *chaosPebbleStore) DeleteEntitlementRecord(ctx context.Context, externalID string) error {
	return s.Engine.Records().DeleteEntitlementRecord(ctx, externalID)
}

func (s *chaosPebbleStore) DeleteEntitlementRecordByIdentity(
	ctx context.Context,
	resourceTypeID string,
	resourceID string,
	externalID string,
) error {
	return s.Engine.Records().DeleteEntitlementRecordByIdentity(ctx, resourceTypeID, resourceID, externalID)
}

func (s *chaosPebbleStore) DeleteEntitlementRecords(ctx context.Context, externalIDs []string, actingScope string) error {
	return s.Engine.Records().DeleteEntitlementRecords(ctx, externalIDs, actingScope)
}

func (s *chaosPebbleStore) IterateEntitlements(ctx context.Context, yield func(*v3.EntitlementRecord) bool) error {
	return s.Engine.Records().IterateEntitlements(ctx, yield)
}

func (s *chaosPebbleStore) IterateEntitlementsByResource(ctx context.Context, resourceTypeID, resourceID string, yield func(*v3.EntitlementRecord) bool) error {
	return s.Engine.Records().IterateEntitlementsByResource(ctx, resourceTypeID, resourceID, yield)
}

func (s *chaosPebbleStore) PutGrantRecord(ctx context.Context, rec *v3.GrantRecord) error {
	return s.Engine.Records().PutGrantRecord(ctx, rec)
}

func (s *chaosPebbleStore) PutGrantRecords(ctx context.Context, records ...*v3.GrantRecord) error {
	return s.Engine.Records().PutGrantRecords(ctx, records...)
}

func (s *chaosPebbleStore) PutExpandedGrantRecords(ctx context.Context, records []*v3.GrantRecord) error {
	return s.Engine.Records().PutExpandedGrantRecords(ctx, records)
}

func (s *chaosPebbleStore) PutSynthesizedGrantRecords(ctx context.Context, records []*v3.GrantRecord) error {
	return s.Engine.Records().PutSynthesizedGrantRecords(ctx, records)
}

func (s *chaosPebbleStore) BeginSynthesizedGrantLayer(ctx context.Context) (bool, error) {
	return s.Engine.Records().BeginSynthesizedGrantLayer(ctx)
}

func (s *chaosPebbleStore) FinishSynthesizedGrantLayer(ctx context.Context) error {
	return s.Engine.Records().FinishSynthesizedGrantLayer(ctx)
}

func (s *chaosPebbleStore) AbortSynthesizedGrantLayer(ctx context.Context) error {
	return s.Engine.Records().AbortSynthesizedGrantLayer(ctx)
}

func (s *chaosPebbleStore) UnsafePutUniqueGrantRecords(ctx context.Context, records ...*v3.GrantRecord) error {
	return s.Engine.Records().UnsafePutUniqueGrantRecords(ctx, records...)
}

func (s *chaosPebbleStore) GetGrantRecord(ctx context.Context, externalID string) (*v3.GrantRecord, error) {
	return s.Engine.Records().GetGrantRecord(ctx, externalID)
}

func (s *chaosPebbleStore) DeleteGrantRecord(ctx context.Context, externalID string) error {
	return s.Engine.Records().DeleteGrantRecord(ctx, externalID)
}

func (s *chaosPebbleStore) DeleteGrantByIdentityRefs(ctx context.Context, rec *v3.GrantRecord) error {
	return s.Engine.Records().DeleteGrantByIdentityRefs(ctx, rec)
}

func (s *chaosPebbleStore) DeleteGrantsByIdentityRefs(ctx context.Context, records ...*v3.GrantRecord) error {
	return s.Engine.Records().DeleteGrantsByIdentityRefs(ctx, records...)
}

func (s *chaosPebbleStore) IterateGrants(ctx context.Context, yield func(*v3.GrantRecord) bool) error {
	return s.Engine.Records().IterateGrants(ctx, yield)
}

func (s *chaosPebbleStore) IterateGrantsByEntitlement(ctx context.Context, entitlementID string, yield func(*v3.GrantRecord) bool) error {
	return s.Engine.Records().IterateGrantsByEntitlement(ctx, entitlementID, yield)
}

func (s *chaosPebbleStore) IterateGrantsByPrincipal(ctx context.Context, principalRT, principalID string, yield func(*v3.GrantRecord) bool) error {
	return s.Engine.Records().IterateGrantsByPrincipal(ctx, principalRT, principalID, yield)
}

func (s *chaosPebbleStore) IterateGrantsByPrincipalResourceType(ctx context.Context, principalRT string, yield func(*v3.GrantRecord) bool) error {
	return s.Engine.Records().IterateGrantsByPrincipalResourceType(ctx, principalRT, yield)
}

func (s *chaosPebbleStore) IterateGrantsByNeedsExpansion(ctx context.Context, yield func(*v3.GrantRecord) bool) error {
	return s.Engine.Records().IterateGrantsByNeedsExpansion(ctx, yield)
}

func (s *chaosPebbleStore) PutResourceTypeRecord(ctx context.Context, rec *v3.ResourceTypeRecord) error {
	return s.Engine.Records().PutResourceTypeRecord(ctx, rec)
}

func (s *chaosPebbleStore) PutResourceTypeRecords(ctx context.Context, records ...*v3.ResourceTypeRecord) error {
	return s.Engine.Records().PutResourceTypeRecords(ctx, records...)
}

func (s *chaosPebbleStore) GetResourceTypeRecord(ctx context.Context, externalID string) (*v3.ResourceTypeRecord, error) {
	return s.Engine.Records().GetResourceTypeRecord(ctx, externalID)
}

func (s *chaosPebbleStore) DeleteResourceTypeRecord(ctx context.Context, externalID string) error {
	return s.Engine.Records().DeleteResourceTypeRecord(ctx, externalID)
}

func (s *chaosPebbleStore) IterateResourceTypes(ctx context.Context, yield func(*v3.ResourceTypeRecord) bool) error {
	return s.Engine.Records().IterateResourceTypes(ctx, yield)
}

func (s *chaosPebbleStore) PutAssetRecord(ctx context.Context, rec *v3.AssetRecord) error {
	return s.Engine.Records().PutAssetRecord(ctx, rec)
}

func (s *chaosPebbleStore) GetAssetRecord(ctx context.Context, externalID string) (*v3.AssetRecord, error) {
	return s.Engine.Records().GetAssetRecord(ctx, externalID)
}

func (s *chaosPebbleStore) DeleteAssetRecord(ctx context.Context, externalID string) error {
	return s.Engine.Records().DeleteAssetRecord(ctx, externalID)
}

func (s *chaosPebbleStore) IterateAssets(ctx context.Context, yield func(*v3.AssetRecord) bool) error {
	return s.Engine.Records().IterateAssets(ctx, yield)
}

func (s *chaosPebbleStore) PaginateGrants(
	ctx context.Context, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	return s.Engine.Records().PaginateGrants(ctx, cursor, limit)
}

func (s *chaosPebbleStore) PaginateGrantsByPrincipal(
	ctx context.Context, principalRT, principalID, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	return s.Engine.Records().PaginateGrantsByPrincipal(ctx, principalRT, principalID, cursor, limit)
}

func (s *chaosPebbleStore) PaginateGrantsByEntitlementResource(
	ctx context.Context, entRT, entRID, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	return s.Engine.Records().PaginateGrantsByEntitlementResource(ctx, entRT, entRID, cursor, limit)
}

func (s *chaosPebbleStore) PaginateGrantsByPrincipalResourceType(
	ctx context.Context, principalRT, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	return s.Engine.Records().PaginateGrantsByPrincipalResourceType(ctx, principalRT, cursor, limit)
}

func (s *chaosPebbleStore) PaginateGrantsByNeedsExpansion(
	ctx context.Context, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	return s.Engine.Records().PaginateGrantsByNeedsExpansion(ctx, cursor, limit)
}

func (s *chaosPebbleStore) PaginateResources(
	ctx context.Context, cursor string, limit int,
) ([]*v3.ResourceRecord, string, error) {
	return s.Engine.Records().PaginateResources(ctx, cursor, limit)
}

func (s *chaosPebbleStore) PaginateResourcesByParent(
	ctx context.Context, parentRT, parentID, cursor string, limit int,
) ([]*v3.ResourceRecord, string, error) {
	return s.Engine.Records().PaginateResourcesByParent(ctx, parentRT, parentID, cursor, limit)
}

func (s *chaosPebbleStore) PaginateResourceTypes(
	ctx context.Context, cursor string, limit int,
) ([]*v3.ResourceTypeRecord, string, error) {
	return s.Engine.Records().PaginateResourceTypes(ctx, cursor, limit)
}

func (s *chaosPebbleStore) PaginateEntitlements(
	ctx context.Context, cursor string, limit int,
) ([]*v3.EntitlementRecord, string, error) {
	return s.Engine.Records().PaginateEntitlements(ctx, cursor, limit)
}

func (s *chaosPebbleStore) PaginateEntitlementsByResource(
	ctx context.Context, resourceTypeID, resourceID, cursor string, limit int,
) ([]*v3.EntitlementRecord, string, error) {
	return s.Engine.Records().PaginateEntitlementsByResource(ctx, resourceTypeID, resourceID, cursor, limit)
}
