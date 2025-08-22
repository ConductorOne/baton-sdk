package engine

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

type AttachedStorageEngine interface {
	CompactTable(ctx context.Context, destSyncID string, baseSyncID string, appliedSyncID string, tableName string) error
	CompactResourceTypes(ctx context.Context, destSyncID string, baseSyncID string, appliedSyncID string) error
	CompactResources(ctx context.Context, destSyncID string, baseSyncID string, appliedSyncID string) error
	CompactEntitlements(ctx context.Context, destSyncID string, baseSyncID string, appliedSyncID string) error
	CompactGrants(ctx context.Context, destSyncID string, baseSyncID string, appliedSyncID string) error
	DetachFile(dbName string) (AttachedStorageEngine, error)
}

type StorageEngine interface {
	connectorstore.Reader
	connectorstore.Writer

	// Dirty returns if the backend has written any changes (and thus should be serialized into a new c1z).
	Dirty() bool
	// Stats returns resource counts for the latest sync.
	Stats(ctx context.Context) (map[string]int64, error)
	// CloneSync clones the syncid to a new directory in the current working dir and returns the path. Callers must clean up.
	CloneSync(ctx context.Context, syncID string) (string, error)
	GenerateSyncDiff(ctx context.Context, baseSyncID string, appliedSyncID string) (string, error)
	ViewSync(ctx context.Context, syncID string) error

	// TODO(morgabra): cross-engine support?
	AttachFile(other StorageEngine, dbName string) (AttachedStorageEngine, error)

	// TODO(morgabra): What's up with these? Should be part of some other interface?
	PutResourceTypesIfNewer(ctx context.Context, resourceTypesObjs ...*v2.ResourceType) error
	PutResourcesIfNewer(ctx context.Context, resourceObjs ...*v2.Resource) error
	PutGrantsIfNewer(ctx context.Context, bulkGrants ...*v2.Grant) error
	PutEntitlementsIfNewer(ctx context.Context, entitlementObjs ...*v2.Entitlement) error

	// TODO(morgabra): What's up with these? Should be part of some other interface?
	ListGrantsForPrincipal(ctx context.Context, req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error)
	ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*SyncRun, string, error)

	// TODO(morgabra): Meh, compaction wants this but the engine doesn't actually care :x Bad interface I guess.
	OutputFilepath() (string, error)
}
