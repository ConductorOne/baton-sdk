package dotc1z

import (
	"context"
	"iter"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

func (s *pebbleStore) ResumeSync(ctx context.Context, syncType connectorstore.SyncType, syncID string) (string, error) {
	return s.engine.ResumeSync(ctx, syncType, syncID)
}

func (s *pebbleStore) SetCurrentSync(ctx context.Context, syncID string) error {
	return s.engine.SetCurrentSync(ctx, syncID)
}

func (s *pebbleStore) StartNewSyncWithID(ctx context.Context, syncType connectorstore.SyncType, syncID, parentSyncID string) (string, error) {
	return s.engine.StartNewSyncWithID(ctx, syncType, syncID, parentSyncID)
}

func (s *pebbleStore) CurrentSyncStep(ctx context.Context) (string, error) {
	return s.engine.CurrentSyncStep(ctx)
}

func (s *pebbleStore) GetSync(ctx context.Context, req *reader_v2.SyncsReaderServiceGetSyncRequest) (*reader_v2.SyncsReaderServiceGetSyncResponse, error) {
	return s.engine.GetSync(ctx, req)
}

func (s *pebbleStore) ListSyncs(ctx context.Context, req *reader_v2.SyncsReaderServiceListSyncsRequest) (*reader_v2.SyncsReaderServiceListSyncsResponse, error) {
	return s.engine.ListSyncs(ctx, req)
}

func (s *pebbleStore) GetLatestFinishedSync(ctx context.Context, req *reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest) (*reader_v2.SyncsReaderServiceGetLatestFinishedSyncResponse, error) {
	return s.engine.GetLatestFinishedSync(ctx, req)
}

func (s *pebbleStore) GetArchivedLedgerReport(ctx context.Context) ([]byte, error) {
	return s.engine.GetArchivedLedgerReport(ctx)
}

func (s *pebbleStore) BoundSyncUnstarted(ctx context.Context) (bool, error) {
	return s.engine.BoundSyncUnstarted(ctx)
}

func (s *pebbleStore) StreamGrants(ctx context.Context, syncID string, opts connectorstore.StreamGrantsOptions) iter.Seq2[*v2.Grant, error] {
	return s.engine.StreamGrants(ctx, syncID, opts)
}

func (s *pebbleStore) StreamResources(ctx context.Context, syncID string, opts connectorstore.StreamResourcesOptions) iter.Seq2[*v2.Resource, error] {
	return s.engine.StreamResources(ctx, syncID, opts)
}

func (s *pebbleStore) StreamEntitlements(ctx context.Context, syncID string) iter.Seq2[*v2.Entitlement, error] {
	return s.engine.StreamEntitlements(ctx, syncID)
}

func (s *pebbleStore) GetEntitlementGrantDigest(ctx context.Context, ent *v2.Entitlement) (connectorstore.GrantDigest, bool, error) {
	return s.engine.GetEntitlementGrantDigest(ctx, ent)
}

func (s *pebbleStore) GetEntitlementGrantDigestNodes(ctx context.Context, ent *v2.Entitlement, level int) ([]connectorstore.GrantDigestNode, bool, error) {
	return s.engine.GetEntitlementGrantDigestNodes(ctx, ent, level)
}

func (s *pebbleStore) ScanEntitlementGrantBucket(ctx context.Context, ent *v2.Entitlement, bucket connectorstore.GrantDigestBucket, yield func(*v2.Grant) bool) error {
	return s.engine.ScanEntitlementGrantBucket(ctx, ent, bucket, yield)
}

func (s *pebbleStore) CurrentDBSizeBytes() (int64, error) {
	return s.engine.CurrentDBSizeBytes()
}

func (s *pebbleStore) V3GrantReader() connectorstore.V3GrantReader {
	return s.engine.V3GrantReader()
}

func (s *pebbleStore) LatestFinishedSyncID(ctx context.Context, syncType connectorstore.SyncType) (string, error) {
	return s.engine.LatestFinishedSyncID(ctx, syncType)
}

func (s *pebbleStore) FileOpsWithEncoding(encoding c1zstore.PayloadEncoding) c1zstore.FileOps {
	return s.engine.FileOpsWithEncoding(encoding)
}

func (s *pebbleStore) Flush(ctx context.Context) error {
	return s.engine.Flush(ctx)
}

func (s *pebbleStore) CompactAllRanges(ctx context.Context) error {
	return s.engine.CompactAllRanges(ctx)
}

func (s *pebbleStore) LedgerState(ctx context.Context) (c1zstore.LedgerState, error) {
	return s.engine.LedgerState(ctx)
}

func (s *pebbleStore) CheckpointTo(ctx context.Context, destDir string) error {
	return s.engine.CheckpointTo(ctx, destDir)
}

func (s *pebbleStore) SessionGet(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	return s.engine.SessionGet(ctx, key, opt...)
}

func (s *pebbleStore) SessionSet(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	return s.engine.SessionSet(ctx, key, value, opt...)
}

func (s *pebbleStore) SessionGetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	return s.engine.SessionGetMany(ctx, keys, opt...)
}

func (s *pebbleStore) SessionGetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	return s.engine.SessionGetAll(ctx, pageToken, opt...)
}

func (s *pebbleStore) SessionSetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	return s.engine.SessionSetMany(ctx, values, opt...)
}

func (s *pebbleStore) SessionDelete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	return s.engine.SessionDelete(ctx, key, opt...)
}

func (s *pebbleStore) SessionClear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	return s.engine.SessionClear(ctx, opt...)
}

func (s *pebbleStore) GetSourceCacheEntry(ctx context.Context, rowKind, scopeKey string) (*v3.SourceCacheEntryRecord, error) {
	return s.engine.GetSourceCacheEntry(ctx, rowKind, scopeKey)
}

func (s *pebbleStore) SourceCachePoisoned(ctx context.Context, rowKind, scopeKey string) (bool, error) {
	return s.engine.SourceCachePoisoned(ctx, rowKind, scopeKey)
}

func (s *pebbleStore) ReplaySourceCacheResources(ctx context.Context, prev *pebble.Engine, scopeKey string) (pebble.SourceCacheReplayResult, error) {
	return s.engine.ReplaySourceCacheResources(ctx, prev, scopeKey)
}

func (s *pebbleStore) ReplaySourceCacheEntitlements(ctx context.Context, prev *pebble.Engine, scopeKey string) (pebble.SourceCacheReplayResult, error) {
	return s.engine.ReplaySourceCacheEntitlements(ctx, prev, scopeKey)
}

func (s *pebbleStore) ReplaySourceCacheGrants(ctx context.Context, prev *pebble.Engine, scopeKey string) (pebble.SourceCacheReplayResult, error) {
	return s.engine.ReplaySourceCacheGrants(ctx, prev, scopeKey)
}

func (s *pebbleStore) DeleteResourceRecordsBounded(ctx context.Context, refs []pebble.ResourceRef, actingScope string) error {
	return s.engine.DeleteResourceRecordsBounded(ctx, refs, actingScope)
}

func (s *pebbleStore) DeleteGrantRecordsBounded(ctx context.Context, externalIDs []string, actingScope string) error {
	return s.engine.DeleteGrantRecordsBounded(ctx, externalIDs, actingScope)
}

func (s *pebbleStore) DeleteGrantsByExternalIDsInScope(ctx context.Context, scopeKey string, ids map[string]struct{}) (int64, error) {
	return s.engine.DeleteGrantsByExternalIDsInScope(ctx, scopeKey, ids)
}

func (s *pebbleStore) DeleteGrantsByPrincipalsInScope(ctx context.Context, scopeKey string, principalIDs map[string]struct{}) (int64, error) {
	return s.engine.DeleteGrantsByPrincipalsInScope(ctx, scopeKey, principalIDs)
}

func (s *pebbleStore) DeleteResourcesByIDsInScope(ctx context.Context, scopeKey string, resourceIDs map[string]struct{}) (int64, error) {
	return s.engine.DeleteResourcesByIDsInScope(ctx, scopeKey, resourceIDs)
}

func (s *pebbleStore) SyncRuns() *pebble.SyncRuns {
	return s.engine.SyncRuns()
}

func (s *pebbleStore) GenerateLedgerReport(ctx context.Context) ([]byte, error) {
	return s.engine.GenerateLedgerReport(ctx)
}

func (s *pebbleStore) GetArchivedLedgerOptions(ctx context.Context, attempt string) (*c1zstore.LedgerReportOptions, error) {
	return s.engine.GetArchivedLedgerOptions(ctx, attempt)
}
