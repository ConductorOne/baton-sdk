package dotc1z

import (
	"context"
	"io"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

func (s *pebbleStore) GetAsset(ctx context.Context, req *v2.AssetServiceGetAssetRequest) (string, io.Reader, error) {
	return s.engine.Records().GetAsset(ctx, req)
}

func (s *pebbleStore) ListGrants(ctx context.Context, req *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	return s.engine.Records().ListGrants(ctx, req)
}

func (s *pebbleStore) ListResources(ctx context.Context, req *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	return s.engine.Records().ListResources(ctx, req)
}

func (s *pebbleStore) ListResourceTypes(ctx context.Context, req *v2.ResourceTypesServiceListResourceTypesRequest) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return s.engine.Records().ListResourceTypes(ctx, req)
}

func (s *pebbleStore) ListEntitlements(ctx context.Context, req *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	return s.engine.Records().ListEntitlements(ctx, req)
}

func (s *pebbleStore) ListStaticEntitlements(
	ctx context.Context,
	req *v2.EntitlementsServiceListStaticEntitlementsRequest,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	return s.engine.Records().ListStaticEntitlements(ctx, req)
}

func (s *pebbleStore) GetGrant(ctx context.Context, req *reader_v2.GrantsReaderServiceGetGrantRequest) (*reader_v2.GrantsReaderServiceGetGrantResponse, error) {
	return s.engine.Records().GetGrant(ctx, req)
}

func (s *pebbleStore) GetEntitlement(ctx context.Context, req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	return s.engine.Records().GetEntitlement(ctx, req)
}

func (s *pebbleStore) GetResource(ctx context.Context, req *reader_v2.ResourcesReaderServiceGetResourceRequest) (*reader_v2.ResourcesReaderServiceGetResourceResponse, error) {
	return s.engine.Records().GetResource(ctx, req)
}

func (s *pebbleStore) GetResourceType(ctx context.Context, req *reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest) (*reader_v2.ResourceTypesReaderServiceGetResourceTypeResponse, error) {
	return s.engine.Records().GetResourceType(ctx, req)
}

//nolint:revive // method name mirrors the protobuf-generated gRPC server interface
func (s *pebbleStore) ListResourcesByIds(
	ctx context.Context,
	req *reader_v2.ResourcesReaderServiceListResourcesByIdsRequest,
) (*reader_v2.ResourcesReaderServiceListResourcesByIdsResponse, error) {
	return s.engine.Records().ListResourcesByIds(ctx, req)
}

//nolint:revive // method name mirrors the protobuf-generated gRPC server interface
func (s *pebbleStore) ListEntitlementsByIds(
	ctx context.Context,
	req *reader_v2.EntitlementsReaderServiceListEntitlementsByIdsRequest,
) (*reader_v2.EntitlementsReaderServiceListEntitlementsByIdsResponse, error) {
	return s.engine.Records().ListEntitlementsByIds(ctx, req)
}

func (s *pebbleStore) GrantsForEntitlementPrincipalSorted() bool {
	return s.engine.Records().GrantsForEntitlementPrincipalSorted()
}

func (s *pebbleStore) ListGrantsForEntitlement(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	return s.engine.Records().ListGrantsForEntitlement(ctx, req)
}

func (s *pebbleStore) ListGrantPrincipalKeysForEntitlement(
	ctx context.Context,
	entitlement *v2.Entitlement,
	pageToken string,
	pageSize uint32,
) ([]string, string, error) {
	return s.engine.Records().ListGrantPrincipalKeysForEntitlement(ctx, entitlement, pageToken, pageSize)
}

func (s *pebbleStore) ListGrantsForPrincipal(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForPrincipalRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForPrincipalResponse, error) {
	return s.engine.Records().ListGrantsForPrincipal(ctx, req)
}

func (s *pebbleStore) ListGrantsForResourceType(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForResourceTypeResponse, error) {
	return s.engine.Records().ListGrantsForResourceType(ctx, req)
}

func (s *pebbleStore) ListGrantsForEntitlements(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementsResponse, error) {
	return s.engine.Records().ListGrantsForEntitlements(ctx, req)
}

func (s *pebbleStore) PutResourceRecord(ctx context.Context, rec *v3.ResourceRecord) error {
	return s.engine.Records().PutResourceRecord(ctx, rec)
}

func (s *pebbleStore) PutResourceRecords(ctx context.Context, records ...*v3.ResourceRecord) error {
	return s.engine.Records().PutResourceRecords(ctx, records...)
}

func (s *pebbleStore) GetResourceRecord(ctx context.Context, resourceTypeID, resourceID string) (*v3.ResourceRecord, error) {
	return s.engine.Records().GetResourceRecord(ctx, resourceTypeID, resourceID)
}

func (s *pebbleStore) IterateResources(ctx context.Context, yield func(*v3.ResourceRecord) bool) error {
	return s.engine.Records().IterateResources(ctx, yield)
}

func (s *pebbleStore) IterateResourcesByParent(ctx context.Context, parentRT, parentID string, yield func(*v3.ResourceRecord) bool) error {
	return s.engine.Records().IterateResourcesByParent(ctx, parentRT, parentID, yield)
}

func (s *pebbleStore) PutEntitlementRecord(ctx context.Context, rec *v3.EntitlementRecord) error {
	return s.engine.Records().PutEntitlementRecord(ctx, rec)
}

func (s *pebbleStore) PutEntitlementRecords(ctx context.Context, records ...*v3.EntitlementRecord) error {
	return s.engine.Records().PutEntitlementRecords(ctx, records...)
}

func (s *pebbleStore) GetEntitlementRecord(ctx context.Context, externalID string) (*v3.EntitlementRecord, error) {
	return s.engine.Records().GetEntitlementRecord(ctx, externalID)
}

func (s *pebbleStore) DeleteEntitlementRecord(ctx context.Context, externalID string) error {
	return s.engine.Records().DeleteEntitlementRecord(ctx, externalID)
}

func (s *pebbleStore) DeleteEntitlementRecordByIdentity(
	ctx context.Context,
	resourceTypeID string,
	resourceID string,
	externalID string,
) error {
	return s.engine.Records().DeleteEntitlementRecordByIdentity(ctx, resourceTypeID, resourceID, externalID)
}

func (s *pebbleStore) DeleteEntitlementRecords(ctx context.Context, externalIDs []string, actingScope string) error {
	return s.engine.Records().DeleteEntitlementRecords(ctx, externalIDs, actingScope)
}

func (s *pebbleStore) IterateEntitlements(ctx context.Context, yield func(*v3.EntitlementRecord) bool) error {
	return s.engine.Records().IterateEntitlements(ctx, yield)
}

func (s *pebbleStore) IterateEntitlementsByResource(ctx context.Context, resourceTypeID, resourceID string, yield func(*v3.EntitlementRecord) bool) error {
	return s.engine.Records().IterateEntitlementsByResource(ctx, resourceTypeID, resourceID, yield)
}

func (s *pebbleStore) PutGrantRecord(ctx context.Context, rec *v3.GrantRecord) error {
	return s.engine.Records().PutGrantRecord(ctx, rec)
}

func (s *pebbleStore) PutGrantRecords(ctx context.Context, records ...*v3.GrantRecord) error {
	return s.engine.Records().PutGrantRecords(ctx, records...)
}

func (s *pebbleStore) PutExpandedGrantRecords(ctx context.Context, records []*v3.GrantRecord) error {
	return s.engine.Records().PutExpandedGrantRecords(ctx, records)
}

func (s *pebbleStore) PutSynthesizedGrantRecords(ctx context.Context, records []*v3.GrantRecord) error {
	return s.engine.Records().PutSynthesizedGrantRecords(ctx, records)
}

func (s *pebbleStore) BeginSynthesizedGrantLayer(ctx context.Context) (bool, error) {
	return s.engine.Records().BeginSynthesizedGrantLayer(ctx)
}

func (s *pebbleStore) FinishSynthesizedGrantLayer(ctx context.Context) error {
	return s.engine.Records().FinishSynthesizedGrantLayer(ctx)
}

func (s *pebbleStore) AbortSynthesizedGrantLayer(ctx context.Context) error {
	return s.engine.Records().AbortSynthesizedGrantLayer(ctx)
}

func (s *pebbleStore) UnsafePutUniqueGrantRecords(ctx context.Context, records ...*v3.GrantRecord) error {
	return s.engine.Records().UnsafePutUniqueGrantRecords(ctx, records...)
}

func (s *pebbleStore) GetGrantRecord(ctx context.Context, externalID string) (*v3.GrantRecord, error) {
	return s.engine.Records().GetGrantRecord(ctx, externalID)
}

func (s *pebbleStore) DeleteGrantRecord(ctx context.Context, externalID string) error {
	return s.engine.Records().DeleteGrantRecord(ctx, externalID)
}

func (s *pebbleStore) DeleteGrantByIdentityRefs(ctx context.Context, rec *v3.GrantRecord) error {
	return s.engine.Records().DeleteGrantByIdentityRefs(ctx, rec)
}

func (s *pebbleStore) DeleteGrantsByIdentityRefs(ctx context.Context, records ...*v3.GrantRecord) error {
	return s.engine.Records().DeleteGrantsByIdentityRefs(ctx, records...)
}

func (s *pebbleStore) IterateGrants(ctx context.Context, yield func(*v3.GrantRecord) bool) error {
	return s.engine.Records().IterateGrants(ctx, yield)
}

func (s *pebbleStore) IterateGrantsByEntitlement(ctx context.Context, entitlementID string, yield func(*v3.GrantRecord) bool) error {
	return s.engine.Records().IterateGrantsByEntitlement(ctx, entitlementID, yield)
}

func (s *pebbleStore) IterateGrantsByPrincipal(ctx context.Context, principalRT, principalID string, yield func(*v3.GrantRecord) bool) error {
	return s.engine.Records().IterateGrantsByPrincipal(ctx, principalRT, principalID, yield)
}

func (s *pebbleStore) IterateGrantsByPrincipalResourceType(ctx context.Context, principalRT string, yield func(*v3.GrantRecord) bool) error {
	return s.engine.Records().IterateGrantsByPrincipalResourceType(ctx, principalRT, yield)
}

func (s *pebbleStore) IterateGrantsByNeedsExpansion(ctx context.Context, yield func(*v3.GrantRecord) bool) error {
	return s.engine.Records().IterateGrantsByNeedsExpansion(ctx, yield)
}

func (s *pebbleStore) PutResourceTypeRecord(ctx context.Context, rec *v3.ResourceTypeRecord) error {
	return s.engine.Records().PutResourceTypeRecord(ctx, rec)
}

func (s *pebbleStore) PutResourceTypeRecords(ctx context.Context, records ...*v3.ResourceTypeRecord) error {
	return s.engine.Records().PutResourceTypeRecords(ctx, records...)
}

func (s *pebbleStore) GetResourceTypeRecord(ctx context.Context, externalID string) (*v3.ResourceTypeRecord, error) {
	return s.engine.Records().GetResourceTypeRecord(ctx, externalID)
}

func (s *pebbleStore) DeleteResourceTypeRecord(ctx context.Context, externalID string) error {
	return s.engine.Records().DeleteResourceTypeRecord(ctx, externalID)
}

func (s *pebbleStore) IterateResourceTypes(ctx context.Context, yield func(*v3.ResourceTypeRecord) bool) error {
	return s.engine.Records().IterateResourceTypes(ctx, yield)
}

func (s *pebbleStore) PutAssetRecord(ctx context.Context, rec *v3.AssetRecord) error {
	return s.engine.Records().PutAssetRecord(ctx, rec)
}

func (s *pebbleStore) GetAssetRecord(ctx context.Context, externalID string) (*v3.AssetRecord, error) {
	return s.engine.Records().GetAssetRecord(ctx, externalID)
}

func (s *pebbleStore) DeleteAssetRecord(ctx context.Context, externalID string) error {
	return s.engine.Records().DeleteAssetRecord(ctx, externalID)
}

func (s *pebbleStore) IterateAssets(ctx context.Context, yield func(*v3.AssetRecord) bool) error {
	return s.engine.Records().IterateAssets(ctx, yield)
}

func (s *pebbleStore) PaginateGrants(
	ctx context.Context, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	return s.engine.Records().PaginateGrants(ctx, cursor, limit)
}

func (s *pebbleStore) PaginateGrantsByPrincipal(
	ctx context.Context, principalRT, principalID, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	return s.engine.Records().PaginateGrantsByPrincipal(ctx, principalRT, principalID, cursor, limit)
}

func (s *pebbleStore) PaginateGrantsByEntitlementResource(
	ctx context.Context, entRT, entRID, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	return s.engine.Records().PaginateGrantsByEntitlementResource(ctx, entRT, entRID, cursor, limit)
}

func (s *pebbleStore) PaginateGrantsByPrincipalResourceType(
	ctx context.Context, principalRT, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	return s.engine.Records().PaginateGrantsByPrincipalResourceType(ctx, principalRT, cursor, limit)
}

func (s *pebbleStore) PaginateGrantsByNeedsExpansion(
	ctx context.Context, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	return s.engine.Records().PaginateGrantsByNeedsExpansion(ctx, cursor, limit)
}

func (s *pebbleStore) PaginateResources(
	ctx context.Context, cursor string, limit int,
) ([]*v3.ResourceRecord, string, error) {
	return s.engine.Records().PaginateResources(ctx, cursor, limit)
}

func (s *pebbleStore) PaginateResourcesByParent(
	ctx context.Context, parentRT, parentID, cursor string, limit int,
) ([]*v3.ResourceRecord, string, error) {
	return s.engine.Records().PaginateResourcesByParent(ctx, parentRT, parentID, cursor, limit)
}

func (s *pebbleStore) PaginateResourceTypes(
	ctx context.Context, cursor string, limit int,
) ([]*v3.ResourceTypeRecord, string, error) {
	return s.engine.Records().PaginateResourceTypes(ctx, cursor, limit)
}

func (s *pebbleStore) PaginateEntitlements(
	ctx context.Context, cursor string, limit int,
) ([]*v3.EntitlementRecord, string, error) {
	return s.engine.Records().PaginateEntitlements(ctx, cursor, limit)
}

func (s *pebbleStore) PaginateEntitlementsByResource(
	ctx context.Context, resourceTypeID, resourceID, cursor string, limit int,
) ([]*v3.EntitlementRecord, string, error) {
	return s.engine.Records().PaginateEntitlementsByResource(ctx, resourceTypeID, resourceID, cursor, limit)
}
