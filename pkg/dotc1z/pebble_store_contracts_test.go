package dotc1z

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

var _ c1zstore.Store = (*pebbleStore)(nil)
var _ c1zstore.PageLedgerStore = (*pebbleStore)(nil)
var _ c1zstore.GrantGenerationDigestReader = (*pebbleStore)(nil)
var _ c1zstore.WriteHookStore = (*pebbleStore)(nil)
var _ connectorstore.Reader = (*pebbleStore)(nil)
var _ connectorstore.StreamingReader = (*pebbleStore)(nil)
var _ connectorstore.LatestFinishedSyncIDFetcher = (*pebbleStore)(nil)
var _ connectorstore.DBSizeProvider = (*pebbleStore)(nil)
var _ connectorstore.V3GrantReaderProvider = (*pebbleStore)(nil)
var _ SourceCacheStore = (*pebbleStore)(nil)
var _ IngestInvariantStore = (*pebbleStore)(nil)

type syncIDPreservingStarter interface {
	StartNewSyncWithID(context.Context, connectorstore.SyncType, string, string) (string, error)
}

type syncRunLister interface {
	ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*c1zstore.SyncRun, string, error)
}

type rollbackExpansionStore interface {
	c1zstore.Store
	connectorstore.StreamingReader
	RollbackExpansion(ctx context.Context, syncID string, dryRun bool, opts ...RollbackOption) (*RollbackResult, error)
}

type grantByRefsDeleter interface {
	DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error
}

type grantsByRefsBatchDeleter interface {
	DeleteGrantsByRefs(ctx context.Context, grants ...*v2.Grant) error
}

type entitlementGraphStore interface {
	PutEntitlementGraphBlob(ctx context.Context, data []byte) error
	GetEntitlementGraphBlob(ctx context.Context) ([]byte, error)
	DeleteEntitlementGraphBlob(ctx context.Context) error
}

type deleteEntitlementGraphBlob interface {
	DeleteEntitlementGraphBlob(context.Context) error
}

type resourceRecordDeleter interface {
	DeleteResourceRecord(ctx context.Context, resourceTypeID, resourceID string) error
}

type entitlementRecordDeleter interface {
	DeleteEntitlementByRefs(ctx context.Context, entitlement *v2.Entitlement) error
}

type grantPrincipalKeyLister interface {
	ListGrantPrincipalKeysForEntitlement(context.Context, *v2.Entitlement, string, uint32) ([]string, string, error)
}

type principalSortedGrantLister interface {
	GrantsForEntitlementPrincipalSorted() bool
}

type pebbleEngineAccessor interface {
	PebbleEngine() *pebble.Engine
}

type engineOnlyCloser interface {
	CloseEngineOnly() error
}

type fixtureNormalizer interface {
	NormalizeForFixtureSave(ctx context.Context, syncID string) error
}

type storeDirtyMarker interface {
	MarkDirty()
}

type foldDeadBytesAdder interface {
	AddFoldDeadBytes(n int64)
}

type currentSyncSetter interface {
	SetCurrentSync(ctx context.Context, syncID string) error
}

type supportsDiffWriter interface {
	SetSupportsDiff(ctx context.Context, syncID string) error
}

type grantRecordDeleter interface {
	DeleteGrantRecord(ctx context.Context, externalID string) error
}

var _ syncIDPreservingStarter = (*pebbleStore)(nil)
var _ syncRunLister = (*pebbleStore)(nil)
var _ rollbackExpansionStore = (*pebbleStore)(nil)
var _ grantByRefsDeleter = (*pebbleStore)(nil)
var _ grantsByRefsBatchDeleter = (*pebbleStore)(nil)
var _ entitlementGraphStore = (*pebbleStore)(nil)
var _ deleteEntitlementGraphBlob = (*pebbleStore)(nil)
var _ resourceRecordDeleter = (*pebbleStore)(nil)
var _ entitlementRecordDeleter = (*pebbleStore)(nil)
var _ grantPrincipalKeyLister = (*pebbleStore)(nil)
var _ principalSortedGrantLister = (*pebbleStore)(nil)
var _ pebbleEngineAccessor = (*pebbleStore)(nil)
var _ engineOnlyCloser = (*pebbleStore)(nil)
var _ fixtureNormalizer = (*pebbleStore)(nil)
var _ storeDirtyMarker = (*pebbleStore)(nil)
var _ foldDeadBytesAdder = (*pebbleStore)(nil)
var _ currentSyncSetter = (*pebbleStore)(nil)
var _ supportsDiffWriter = (*pebbleStore)(nil)
var _ grantRecordDeleter = (*pebbleStore)(nil)
