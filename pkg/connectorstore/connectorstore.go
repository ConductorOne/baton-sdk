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

// StoreMetadata describes the storage backing a Reader. Returned by
// Reader.Metadata() — present on every Reader, no type assertion
// needed. Fields are best-effort: implementations that don't back an
// on-disk c1z artifact (mocks, in-memory wrappers, gRPC clients)
// return a zero StoreMetadata.
//
// The struct may grow over time. Consumers must treat unknown values
// gracefully — e.g. log them but don't switch behavior on them
// unless the value is recognized.
type StoreMetadata struct {
	// Engine identifies the storage backend. Values match
	// dotc1z.Engine string values; using string here keeps
	// connectorstore from depending on dotc1z (avoids an import
	// cycle).
	//   "sqlite" — original .c1z, v1 magic + zstd-compressed SQLite
	//   "pebble" — v3 .c1z3, Pebble LSM in a v3 envelope
	//   ""       — unknown or not backed by an on-disk c1z
	Engine string

	// Format identifies the on-disk magic-byte format. Values match
	// dotc1z.C1ZFormat.String().
	//   "v1" — "C1ZF\x00" magic (SQLite payload)
	//   "v3" — "C1Z3\x00" magic (Pebble payload + manifest)
	//   ""   — unknown / virtual store
	Format string

	// PayloadEncoding identifies the v3 envelope payload framing.
	// Empty for v1 / SQLite. Values match
	// dotc1z.PayloadEncoding.String():
	//   "tar_zstd" — Pebble checkpoint as zstd-compressed tar
	//   "tar"      — Pebble checkpoint as uncompressed tar
	//   ""         — N/A or unset
	PayloadEncoding string
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

	// Metadata describes the storage backing this Reader. Required
	// on every implementation; readers that don't back an on-disk
	// c1z return a zero StoreMetadata. Cheap call — implementations
	// must not perform I/O.
	Metadata() StoreMetadata

	Close(ctx context.Context) error
}

// LatestFinishedSyncIDFetcher returns the most-recently-finished sync ID of the
// given type, or empty string if no such sync exists. This is a small optional
// capability separate from Reader/Writer because not every store implementation
// can answer it (e.g. gRPC-backed readers have a different flavor via
// SyncsReaderServiceGetLatestFinishedSync).
//
// This interface lives in connectorstore so that producers (e.g. *dotc1z.C1File)
// and consumers (e.g. pkg/sync) reference a single authoritative declaration,
// preventing the name/signature drift that occurred between PR #473 and RFC 0002.
type LatestFinishedSyncIDFetcher interface {
	LatestFinishedSyncID(ctx context.Context, syncType SyncType) (string, error)
}

// DBSizeProvider is an optional capability for a store that can report its
// current uncompressed working-set size (e.g. dotc1z.C1File stat'ing its
// sqlite file). Consumed by the syncer's ProgressLog to include
// decompressed_bytes and growth delta in the periodic "Expanding grants"
// log during long-running grant expansions — the Expander itself is
// recreated each RunSingleStep by the syncer, so this state cannot live
// there.
type DBSizeProvider interface {
	CurrentDBSizeBytes() (int64, error)
}

// ExpansionGrantLister is an optional capability for stores whose ListGrants
// strips the GrantExpandable annotation into a side column on write (the SQLite
// engine does; see dotc1z). ListGrantsWithExpansion is the paginated read that
// re-attaches it, so a copy/sanitize that keeps ListGrants' resumable
// page-cursor semantics (rather than switching to StreamGrants) still preserves
// the grant-expansion topology end-to-end. Mirrors
// StreamGrantsOptions.IncludeExpansion for the unary paginated read path.
// Discovered by type assertion; readers that don't strip expansion (e.g. the
// Pebble adapter, whose ListGrants already carries it) need not implement it.
type ExpansionGrantLister interface {
	ListGrantsWithExpansion(ctx context.Context, request *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error)
}

// GrantUpsertMode controls how grant conflicts are resolved during upsert.
type GrantUpsertMode int

const (
	// GrantUpsertModeReplace updates conflicting grants unconditionally.
	GrantUpsertModeReplace GrantUpsertMode = iota
	// GrantUpsertModePreserveExpansion updates grant data while preserving existing
	// expansion and needs_expansion columns.
	GrantUpsertModePreserveExpansion
)

// GrantUpsertOptions configures internal grant upsert behavior.
type GrantUpsertOptions struct {
	Mode GrantUpsertMode
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
}
