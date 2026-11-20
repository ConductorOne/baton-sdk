package sync

import (
	"context"
	"iter"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

func (s *chaosPebbleStore) Metadata() connectorstore.StoreMetadata {
	return s.engine.Metadata()
}

func (s *chaosPebbleStore) ResumeSync(ctx context.Context, syncType connectorstore.SyncType, syncID string) (string, error) {
	return s.engine.ResumeSync(ctx, syncType, syncID)
}

func (s *chaosPebbleStore) StartOrResumeSync(ctx context.Context, syncType connectorstore.SyncType, syncID string) (string, bool, error) {
	return s.engine.StartOrResumeSync(ctx, syncType, syncID)
}

func (s *chaosPebbleStore) StartNewSync(ctx context.Context, syncType connectorstore.SyncType, parentSyncID string) (string, error) {
	return s.engine.StartNewSync(ctx, syncType, parentSyncID)
}

func (s *chaosPebbleStore) SetCurrentSync(ctx context.Context, syncID string) error {
	return s.engine.SetCurrentSync(ctx, syncID)
}

func (s *chaosPebbleStore) CurrentSyncStep(ctx context.Context) (string, error) {
	return s.engine.CurrentSyncStep(ctx)
}

func (s *chaosPebbleStore) EndSync(ctx context.Context) error {
	return s.engine.EndSync(ctx)
}

func (s *chaosPebbleStore) EndSyncWithStats(ctx context.Context, stats c1zstore.SyncStats) error {
	return s.engine.EndSyncWithStats(ctx, stats)
}

func (s *chaosPebbleStore) Cleanup(ctx context.Context) error {
	return s.engine.Cleanup(ctx)
}

func (s *chaosPebbleStore) Grants() c1zstore.GrantStore {
	return s.engine.Grants()
}

func (s *chaosPebbleStore) FileOps() c1zstore.FileOps {
	return s.engine.FileOps()
}

func (s *chaosPebbleStore) ArchiveLedgerReport(ctx context.Context) ([]byte, error) {
	return s.engine.ArchiveLedgerReport(ctx)
}

func (s *chaosPebbleStore) GetArchivedLedgerReport(ctx context.Context) ([]byte, error) {
	return s.engine.GetArchivedLedgerReport(ctx)
}

func (s *chaosPebbleStore) BoundSyncUnstarted(ctx context.Context) (bool, error) {
	return s.engine.BoundSyncUnstarted(ctx)
}

func (s *chaosPebbleStore) GetSync(ctx context.Context, req *reader_v2.SyncsReaderServiceGetSyncRequest) (*reader_v2.SyncsReaderServiceGetSyncResponse, error) {
	return s.engine.GetSync(ctx, req)
}

func (s *chaosPebbleStore) ListSyncs(ctx context.Context, req *reader_v2.SyncsReaderServiceListSyncsRequest) (*reader_v2.SyncsReaderServiceListSyncsResponse, error) {
	return s.engine.ListSyncs(ctx, req)
}

func (s *chaosPebbleStore) GetLatestFinishedSync(
	ctx context.Context,
	req *reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest,
) (*reader_v2.SyncsReaderServiceGetLatestFinishedSyncResponse, error) {
	return s.engine.GetLatestFinishedSync(ctx, req)
}

func (s *chaosPebbleStore) Ledger() *pebble.Ledger {
	return s.engine.Ledger()
}

func (s *chaosPebbleStore) LedgerState(ctx context.Context) (c1zstore.LedgerState, error) {
	return s.engine.LedgerState(ctx)
}

func (s *chaosPebbleStore) LatestFinishedSyncID(ctx context.Context, syncType connectorstore.SyncType) (string, error) {
	return s.engine.LatestFinishedSyncID(ctx, syncType)
}

func (s *chaosPebbleStore) CurrentDBSizeBytes() (int64, error) {
	return s.engine.CurrentDBSizeBytes()
}

func (s *chaosPebbleStore) V3GrantReader() connectorstore.V3GrantReader {
	return s.engine.V3GrantReader()
}

func (s *chaosPebbleStore) StreamGrants(ctx context.Context, syncID string, opts connectorstore.StreamGrantsOptions) iter.Seq2[*v2.Grant, error] {
	return s.engine.StreamGrants(ctx, syncID, opts)
}

func (s *chaosPebbleStore) StreamResources(ctx context.Context, syncID string, opts connectorstore.StreamResourcesOptions) iter.Seq2[*v2.Resource, error] {
	return s.engine.StreamResources(ctx, syncID, opts)
}

func (s *chaosPebbleStore) StreamEntitlements(ctx context.Context, syncID string) iter.Seq2[*v2.Entitlement, error] {
	return s.engine.StreamEntitlements(ctx, syncID)
}

func (s *chaosPebbleStore) ForEachDistinctGrantEntitlementResource(ctx context.Context, visit func(resourceTypeID, resourceID string) error) error {
	return s.engine.ForEachDistinctGrantEntitlementResource(ctx, visit)
}

func (s *chaosPebbleStore) GrantsForEntResourceCarryInsertFact(ctx context.Context, resourceTypeID, resourceID string) (bool, error) {
	return s.engine.GrantsForEntResourceCarryInsertFact(ctx, resourceTypeID, resourceID)
}

func (s *chaosPebbleStore) GrantsForEntitlementAllCarryInsertFact(ctx context.Context, entitlementID, entResourceTypeID, entResourceID string) (bool, error) {
	return s.engine.GrantsForEntitlementAllCarryInsertFact(ctx, entitlementID, entResourceTypeID, entResourceID)
}

func (s *chaosPebbleStore) HasResourceRecord(ctx context.Context, resourceTypeID, resourceID string) (bool, error) {
	return s.engine.HasResourceRecord(ctx, resourceTypeID, resourceID)
}

func (s *chaosPebbleStore) ForEachDistinctEntitlementResource(ctx context.Context, visit func(resourceTypeID, resourceID string) error) error {
	return s.engine.ForEachDistinctEntitlementResource(ctx, visit)
}

func (s *chaosPebbleStore) ForEachDanglingGrantEntitlement(ctx context.Context, visit func(entitlementID, resourceTypeID, resourceID string) error) error {
	return s.engine.ForEachDanglingGrantEntitlement(ctx, visit)
}

func (s *chaosPebbleStore) EnsureGrantIndexes(ctx context.Context) error {
	return s.engine.EnsureGrantIndexes(ctx)
}

func (s *chaosPebbleStore) ForEachDanglingGrantPrincipal(ctx context.Context, visit func(principalRT, principalID string, matchAnnotatedOnly bool, carrierGrants int64) error) error {
	return s.engine.ForEachDanglingGrantPrincipal(ctx, visit)
}

func (s *chaosPebbleStore) GrantGenerationDigest(ctx context.Context) (c1zstore.GrantGenerationDigest, bool, error) {
	root, ok, err := s.engine.Digests().GetGrantDigestGlobalRoot(ctx)
	if err != nil || !ok {
		return c1zstore.GrantGenerationDigest{}, ok, err
	}
	return c1zstore.GrantGenerationDigest{
		Hash:       append([]byte(nil), root.Hash...),
		Count:      root.Count,
		ABIVersion: pebble.GrantDigestABIVersion,
	}, true, nil
}

func (s *chaosPebbleStore) PutEntitlementGraphBlob(ctx context.Context, data []byte) error {
	return s.engine.Digests().PutEntitlementGraphSidecar(ctx, data)
}

func (s *chaosPebbleStore) GetEntitlementGraphBlob(ctx context.Context) ([]byte, error) {
	return s.engine.Digests().GetEntitlementGraphSidecar(ctx)
}

func (s *chaosPebbleStore) DeleteEntitlementGraphBlob(ctx context.Context) error {
	return s.engine.Digests().DeleteEntitlementGraphSidecar(ctx)
}

func (s *chaosPebbleStore) DeleteEntitlementByRefs(ctx context.Context, entitlement *v2.Entitlement) error {
	resourceID := entitlement.GetResource().GetId()
	return s.DeleteEntitlementRecordByIdentity(ctx, resourceID.GetResourceType(), resourceID.GetResource(), entitlement.GetId())
}

func (s *chaosPebbleStore) MarkIngestInvariantsVerified(ctx context.Context, syncID string, verification c1zstore.IngestInvariantVerification) error {
	return s.SyncMeta().(c1zstore.IngestInvariantVerificationWriter).MarkIngestInvariantsVerified(ctx, syncID, verification)
}

func (s *chaosPebbleStore) ClearIngestInvariantVerification(ctx context.Context, syncID string) error {
	return s.SyncMeta().(c1zstore.IngestInvariantVerificationWriter).ClearIngestInvariantVerification(ctx, syncID)
}

func (s *chaosPebbleStore) StoreNewExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	return s.Grants().(newExpandedGrantStorer).StoreNewExpandedGrants(ctx, grants...)
}

func (s *chaosPebbleStore) StoreNewExpandedGrantContributions(ctx context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error {
	return s.Grants().(newExpandedGrantContributionStorer).StoreNewExpandedGrantContributions(ctx, dest, principals, sources)
}

func (s *chaosPebbleStore) BeginExpandedGrantLayer(ctx context.Context) (bool, error) {
	return s.Grants().(expandedGrantLayerStorer).BeginExpandedGrantLayer(ctx)
}

func (s *chaosPebbleStore) AddExpandedGrantLayerContributions(ctx context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error {
	return s.Grants().(expandedGrantLayerStorer).AddExpandedGrantLayerContributions(ctx, dest, principals, sources)
}

func (s *chaosPebbleStore) FinishExpandedGrantLayer(ctx context.Context) error {
	return s.Grants().(expandedGrantLayerStorer).FinishExpandedGrantLayer(ctx)
}

func (s *chaosPebbleStore) AbortExpandedGrantLayer(ctx context.Context) error {
	return s.Grants().(expandedGrantLayerStorer).AbortExpandedGrantLayer(ctx)
}
