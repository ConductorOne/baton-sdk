package dotc1z

import (
	"context"
	"io"
	"iter"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// The read half of the store's interface surface, forwarded to the engine
// one method at a time.
//
// pebbleStore used to embed *pebble.Engine, which satisfied these for free —
// and, in the same stroke, promoted every engine mutator onto the store.
// Those promoted mutators bypassed withMutation entirely: no admission, no
// dirty bit, invisible to the wrapper-inventory test, and indistinguishable
// at the call site from a guarded method. Spelling the reads out costs this
// file and buys the guarantee that the store's method set contains exactly
// what is written here plus the wrappers in pebble_store.go. A mutator added
// to the engine tomorrow cannot appear on the store by accident.
//
// Keep this file free of logic. Anything that needs to touch admission,
// dirty tracking, or the envelope lifecycle belongs in pebble_store.go.

func (s *pebbleStore) CurrentSyncStep(ctx context.Context) (string, error) {
	return s.Engine.CurrentSyncStep(ctx)
}

func (s *pebbleStore) GetAsset(ctx context.Context, req *v2.AssetServiceGetAssetRequest) (string, io.Reader, error) {
	return s.Engine.GetAsset(ctx, req)
}

func (s *pebbleStore) GetEntitlement(
	ctx context.Context,
	req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest,
) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	return s.Engine.GetEntitlement(ctx, req)
}

func (s *pebbleStore) GetGrant(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceGetGrantRequest,
) (*reader_v2.GrantsReaderServiceGetGrantResponse, error) {
	return s.Engine.GetGrant(ctx, req)
}

func (s *pebbleStore) GetLatestFinishedSync(
	ctx context.Context,
	req *reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest,
) (*reader_v2.SyncsReaderServiceGetLatestFinishedSyncResponse, error) {
	return s.Engine.GetLatestFinishedSync(ctx, req)
}

func (s *pebbleStore) GetResource(
	ctx context.Context,
	req *reader_v2.ResourcesReaderServiceGetResourceRequest,
) (*reader_v2.ResourcesReaderServiceGetResourceResponse, error) {
	return s.Engine.GetResource(ctx, req)
}

func (s *pebbleStore) GetResourceType(
	ctx context.Context,
	req *reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest,
) (*reader_v2.ResourceTypesReaderServiceGetResourceTypeResponse, error) {
	return s.Engine.GetResourceType(ctx, req)
}

func (s *pebbleStore) GetSync(
	ctx context.Context,
	req *reader_v2.SyncsReaderServiceGetSyncRequest,
) (*reader_v2.SyncsReaderServiceGetSyncResponse, error) {
	return s.Engine.GetSync(ctx, req)
}

func (s *pebbleStore) ListEntitlements(
	ctx context.Context,
	req *v2.EntitlementsServiceListEntitlementsRequest,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	return s.Engine.ListEntitlements(ctx, req)
}

//nolint:revive // method name mirrors the protobuf-generated gRPC server interface
func (s *pebbleStore) ListEntitlementsByIds(
	ctx context.Context,
	req *reader_v2.EntitlementsReaderServiceListEntitlementsByIdsRequest,
) (*reader_v2.EntitlementsReaderServiceListEntitlementsByIdsResponse, error) {
	return s.Engine.ListEntitlementsByIds(ctx, req)
}

func (s *pebbleStore) ListGrants(
	ctx context.Context,
	req *v2.GrantsServiceListGrantsRequest,
) (*v2.GrantsServiceListGrantsResponse, error) {
	return s.Engine.ListGrants(ctx, req)
}

func (s *pebbleStore) ListGrantsForEntitlement(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	return s.Engine.ListGrantsForEntitlement(ctx, req)
}

func (s *pebbleStore) ListGrantsForEntitlements(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementsResponse, error) {
	return s.Engine.ListGrantsForEntitlements(ctx, req)
}

func (s *pebbleStore) ListGrantsForPrincipal(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForPrincipalRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForPrincipalResponse, error) {
	return s.Engine.ListGrantsForPrincipal(ctx, req)
}

func (s *pebbleStore) ListGrantsForResourceType(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForResourceTypeResponse, error) {
	return s.Engine.ListGrantsForResourceType(ctx, req)
}

func (s *pebbleStore) ListResources(
	ctx context.Context,
	req *v2.ResourcesServiceListResourcesRequest,
) (*v2.ResourcesServiceListResourcesResponse, error) {
	return s.Engine.ListResources(ctx, req)
}

//nolint:revive // method name mirrors the protobuf-generated gRPC server interface
func (s *pebbleStore) ListResourcesByIds(
	ctx context.Context,
	req *reader_v2.ResourcesReaderServiceListResourcesByIdsRequest,
) (*reader_v2.ResourcesReaderServiceListResourcesByIdsResponse, error) {
	return s.Engine.ListResourcesByIds(ctx, req)
}

func (s *pebbleStore) ListResourceTypes(
	ctx context.Context,
	req *v2.ResourceTypesServiceListResourceTypesRequest,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return s.Engine.ListResourceTypes(ctx, req)
}

func (s *pebbleStore) ListStaticEntitlements(
	ctx context.Context,
	req *v2.EntitlementsServiceListStaticEntitlementsRequest,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	return s.Engine.ListStaticEntitlements(ctx, req)
}

func (s *pebbleStore) ListSyncs(
	ctx context.Context,
	req *reader_v2.SyncsReaderServiceListSyncsRequest,
) (*reader_v2.SyncsReaderServiceListSyncsResponse, error) {
	return s.Engine.ListSyncs(ctx, req)
}

func (s *pebbleStore) ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*c1zstore.SyncRun, string, error) {
	return s.Engine.ListSyncRuns(ctx, pageToken, pageSize)
}

// Optional capability reads. Callers reach these through a type assertion
// rather than a declared parameter type, so a missing forwarder here is not a
// build failure anywhere — it just silently stops the capability from being
// discovered. See pebble_store_capabilities_test.go, which asserts each one.

func (s *pebbleStore) CurrentDBSizeBytes() (int64, error) {
	return s.Engine.CurrentDBSizeBytes()
}

func (s *pebbleStore) LatestFinishedSyncID(ctx context.Context, syncType connectorstore.SyncType) (string, error) {
	return s.Engine.LatestFinishedSyncID(ctx, syncType)
}

func (s *pebbleStore) Stats(ctx context.Context, syncType connectorstore.SyncType, syncID string) (map[string]int64, error) {
	return s.Engine.Stats(ctx, syncType, syncID)
}

func (s *pebbleStore) V3GrantReader() connectorstore.V3GrantReader {
	return s.Engine.V3GrantReader()
}

func (s *pebbleStore) ListGrantPrincipalKeysForEntitlement(
	ctx context.Context,
	entitlement *v2.Entitlement,
	pageToken string,
	pageSize uint32,
) ([]string, string, error) {
	return s.Engine.ListGrantPrincipalKeysForEntitlement(ctx, entitlement, pageToken, pageSize)
}

func (s *pebbleStore) StreamGrants(
	ctx context.Context,
	syncID string,
	opts connectorstore.StreamGrantsOptions,
) iter.Seq2[*v2.Grant, error] {
	return s.Engine.StreamGrants(ctx, syncID, opts)
}

func (s *pebbleStore) StreamResources(
	ctx context.Context,
	syncID string,
	opts connectorstore.StreamResourcesOptions,
) iter.Seq2[*v2.Resource, error] {
	return s.Engine.StreamResources(ctx, syncID, opts)
}

func (s *pebbleStore) StreamEntitlements(ctx context.Context, syncID string) iter.Seq2[*v2.Entitlement, error] {
	return s.Engine.StreamEntitlements(ctx, syncID)
}

// GrantsForEntitlementPrincipalSorted reports that ListGrantsForEntitlement
// pages come back principal-sorted, which is what lets the expander use the
// topological-merge path. The syncer discovers it by inline type assertion
// (pkg/sync/syncer.go), so losing this forwarder does not fail anything —
// every Pebble sync just silently falls back to the source-batched expander.
// That exact regression shipped on this branch when the engine was
// un-embedded, and only an independent review caught it.
func (s *pebbleStore) GrantsForEntitlementPrincipalSorted() bool {
	return s.Engine.GrantsForEntitlementPrincipalSorted()
}

// LatestFinishedSyncRecord exists so the fold compactor can pick its base
// sync without extracting a raw engine from the destination store. Source
// files still go through pebble.AsEngine — the merge pipeline consumes
// concrete engines — but the shared destination should never have an
// unguarded handle pulled out of it for the sake of one read.
func (s *pebbleStore) LatestFinishedSyncRecord(ctx context.Context, typeOK func(v3.SyncType) bool) (*v3.SyncRunRecord, error) {
	return s.Engine.LatestFinishedSyncRecord(ctx, typeOK)
}
