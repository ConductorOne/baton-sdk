package connectorstore

import (
	"context"
	"io"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

type SyncType string

const (
	SyncTypeFull             SyncType = "full"
	SyncTypePartial          SyncType = "partial"
	SyncTypeResourcesOnly    SyncType = "resources_only"
	SyncTypePartialUpserts   SyncType = "partial_upserts"   // Diff sync: additions and modifications
	SyncTypePartialDeletions SyncType = "partial_deletions" // Diff sync: deletions
	SyncTypeAny              SyncType = ""
)

var AllSyncTypes = []SyncType{
	SyncTypeAny,
	SyncTypeFull,
	SyncTypePartial,
	SyncTypeResourcesOnly,
	SyncTypePartialUpserts,
	SyncTypePartialDeletions,
}

// ConnectorStoreReader implements the ConnectorV2 API, along with getters for individual objects.
type Reader interface {
	v2.ResourceTypesServiceServer
	reader_v2.ResourceTypesReaderServiceServer

	v2.ResourcesServiceServer
	reader_v2.ResourcesReaderServiceServer

	v2.EntitlementsServiceServer
	reader_v2.EntitlementsReaderServiceServer

	v2.GrantsServiceServer
	reader_v2.GrantsReaderServiceServer

	reader_v2.SyncsReaderServiceServer

	// GetAsset does not implement the AssetServer on the reader here. In other situations we were able to easily 'fake'
	// the GRPC api, but because this is defined as a streaming RPC, it isn't trivial to implement grpc streaming as part of the c1z format.
	GetAsset(ctx context.Context, req *v2.AssetServiceGetAssetRequest) (string, io.Reader, error)

	Close(ctx context.Context) error
}

// ConnectorStoreWriter defines an implementation for a connector v2 datasource writer. This is used to store sync data from an upstream provider.
type Writer interface {
	Reader
	ResumeSync(ctx context.Context, syncType SyncType, syncID string) (string, error)
	StartOrResumeSync(ctx context.Context, syncType SyncType, syncID string) (string, bool, error)
	StartNewSync(ctx context.Context, syncType SyncType, parentSyncID string) (string, error)
	SetCurrentSync(ctx context.Context, syncID string) error
	CurrentSyncStep(ctx context.Context) (string, error)
	CheckpointSync(ctx context.Context, syncToken string) error
	EndSync(ctx context.Context) error
	PutAsset(ctx context.Context, assetRef *v2.AssetRef, contentType string, data []byte) error
	Cleanup(ctx context.Context) error

	PutGrants(ctx context.Context, grants ...*v2.Grant) error
	PutResourceTypes(ctx context.Context, resourceTypes ...*v2.ResourceType) error
	PutResources(ctx context.Context, resources ...*v2.Resource) error
	PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error
	DeleteGrant(ctx context.Context, grantId string) error

	// ListExpandableGrants lists expandable grants directly from SQL columns,
	// returning lightweight structs without unmarshalling full grant protos.
	ListExpandableGrants(ctx context.Context, opts ...ListExpandableGrantsOption) ([]*ExpandableGrantDef, string, error)
}

// ExpansionStore provides methods for grant expansion operations.
// Not all store implementations support expansion; callers should type-assert.
type ExpansionStore interface {
	// SetSupportsDiff marks the sync as supporting diff operations.
	SetSupportsDiff(ctx context.Context, syncID string) error
}

// ExpandableGrantDef is a lightweight representation of an expandable grant row,
// using queryable columns instead of unmarshalling the full grant proto.
type ExpandableGrantDef struct {
	RowID                   int64
	GrantExternalID         string
	TargetEntitlementID     string
	PrincipalResourceTypeID string
	PrincipalResourceID     string
	SourceEntitlementIDs    []string
	Shallow                 bool
	ResourceTypeIDs         []string
	NeedsExpansion          bool
}

// ListExpandableGrantsOption configures a ListExpandableGrants query.
type ListExpandableGrantsOption func(*ListExpandableGrantsOptions)

// ListExpandableGrantsOptions holds the resolved options for ListExpandableGrants.
type ListExpandableGrantsOptions struct {
	PageToken          string
	PageSize           uint32
	NeedsExpansionOnly bool
	SyncID             string
}

// WithExpandableGrantsPageToken sets the page token for pagination.
func WithExpandableGrantsPageToken(t string) ListExpandableGrantsOption {
	return func(o *ListExpandableGrantsOptions) { o.PageToken = t }
}

// WithExpandableGrantsPageSize sets the page size.
func WithExpandableGrantsPageSize(n uint32) ListExpandableGrantsOption {
	return func(o *ListExpandableGrantsOptions) { o.PageSize = n }
}

// WithExpandableGrantsNeedsExpansionOnly filters to grants that need expansion processing.
func WithExpandableGrantsNeedsExpansionOnly(b bool) ListExpandableGrantsOption {
	return func(o *ListExpandableGrantsOptions) { o.NeedsExpansionOnly = b }
}

// WithExpandableGrantsSyncID forces listing expandable grants for a specific sync ID.
func WithExpandableGrantsSyncID(syncID string) ListExpandableGrantsOption {
	return func(o *ListExpandableGrantsOptions) { o.SyncID = syncID }
}
