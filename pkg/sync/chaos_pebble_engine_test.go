package sync

import (
	"context"
	"iter"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
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

func (s *chaosPebbleStore) CheckpointSync(ctx context.Context, syncToken string) error {
	return s.engine.CheckpointSync(ctx, syncToken)
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
