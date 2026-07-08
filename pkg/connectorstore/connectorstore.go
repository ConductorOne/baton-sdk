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
	// c1zstore.Engine string values; using string here keeps
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
	// c1zstore.PayloadEncoding.String():
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

// GrantDigest is the root of an entitlement's grant digest: a
// content-defined hash over all of the entitlement's grants plus the
// grant count. Two entitlements (in the same or different c1z files)
// with the same Count and Hash hold the same set of grants.
type GrantDigest struct {
	// Hash is the digest of every grant under the entitlement. Opaque
	// bytes — compare for equality, don't interpret. Empty when Count
	// is 0.
	Hash []byte
	// Count is the number of grants under the entitlement.
	Count int64
	// Level is the digest's native (stored) rollup level: the leaf
	// granularity it was built at, expressed as a count of
	// principal-hash bits, so the leaf level holds up to 2^Level
	// buckets. Level 0 means only the root is stored (small or empty
	// entitlements). It is the finest level GetEntitlementGrantDigestNodes
	// can serve cheaply (by folding the stored leaves); a finer level
	// would require a full grant-index scan.
	Level int
}

// GrantDigestNode is one bucket of an entitlement's grant digest at a
// requested rollup level: a content hash + grant count over the grants
// whose principal-hash falls in that bucket.
type GrantDigestNode struct {
	// Index is the bucket index, in [0, 2^level). For level >= 1 nodes
	// are returned sparsely and in ascending Index — only non-empty
	// buckets appear, so a returned leaf node always has Count > 0. (At
	// level 0 the single root node is always returned, even with
	// Count 0.)
	Index uint32
	// Hash is the digest over the bucket's grants.
	Hash []byte
	// Count is the number of grants in the bucket.
	Count int64
}

// GrantDigestBucket addresses one rollup bucket of an entitlement's
// grant digest: the grants whose principal-hash falls under Index at the
// given Level (2^Level buckets; Level 0 = the whole entitlement, Index
// ignored). Index is exactly a GrantDigestNode.Index returned by
// GetEntitlementGrantDigestNodes at the same Level — list nodes to find
// the buckets, then scan the ones you want.
type GrantDigestBucket struct {
	// Level is the bucket width in principal-hash bits (2^Level buckets);
	// 0 = the whole entitlement.
	Level int
	// Index is the bucket index in [0, 2^Level); ignored when Level == 0.
	Index uint32
}

// EntitlementGrantDigestReader is an optional Reader capability that
// returns the precomputed grant digest for an entitlement. It is
// engine-specific: only the Pebble engine maintains the digest, so
// callers must type-assert and fall back when the assertion fails or
// found is false.
//
// Digests are keyed by the entitlement's STRUCTURAL identity —
// (resource_type_id, resource_id, entitlement id) — so every method
// takes a *v2.Entitlement stub, the same addressing the grants-for-
// entitlement readers use. Pass the resource ref when you have it (any
// caller iterating entitlement records does): identity then derives
// exactly from the structured parts, with no lookup and no ambiguity.
// A stub carrying only the Id falls back to bare-id resolution under
// the exactly-one rule; an id that matches entitlements on more than
// one resource is an error, never a guess. A nil stub or empty Id is
// an error.
//
// found is false when the reader has no digest built for the
// entitlement (it was never synced, the file predates / opted out of
// the digest index — see WithGrantDigestIndex — or a bare id resolved
// to nothing). A nil error with found=false means "no digest", not
// "error".
type EntitlementGrantDigestReader interface {
	// GetEntitlementGrantDigest returns the entitlement's digest root —
	// the whole-entitlement hash + grant count, plus the digest's native
	// rollup Level. One key read.
	GetEntitlementGrantDigest(ctx context.Context, entitlement *v2.Entitlement) (digest GrantDigest, found bool, err error)

	// GetEntitlementGrantDigestNodes lists the digest's rollup nodes for
	// the entitlement at `level`: 2^level principal-hash buckets, with
	// level 0 returning a single node (the root) covering the whole
	// entitlement.
	//
	// For 0 <= level <= the native Level (GrantDigest.Level) this folds
	// the stored leaves — one contiguous scan of the digest keyspace, no
	// grant-index scan. For a finer level it falls back to scanning the
	// grant index directly (O(grants)) — slower, but it never errors on a
	// "too deep" level. The principal-hash carries a bounded number of
	// bits, so a level beyond that resolution is served at the maximum
	// (you may get fewer than 2^level distinct buckets). found is false
	// when no digest exists.
	GetEntitlementGrantDigestNodes(ctx context.Context, entitlement *v2.Entitlement, level int) (nodes []GrantDigestNode, found bool, err error)

	// ScanEntitlementGrantBucket yields every grant in one digest bucket
	// of the entitlement (see GrantDigestBucket) as a v2.Grant, stopping
	// early if yield returns false. Bucket Level 0 scans the whole
	// entitlement; a Level finer than the bucket-hash resolution is
	// clamped (matching GetEntitlementGrantDigestNodes). It reads the
	// grant hash index, which exists only on files whose digest was
	// built (they are derived together at seal): callers must check
	// GetEntitlementGrantDigest first and treat found=false as "scan
	// unavailable — read the grants directly", not as "no grants". It
	// yields nothing when there is no active sync or no matching grants.
	ScanEntitlementGrantBucket(ctx context.Context, entitlement *v2.Entitlement, bucket GrantDigestBucket, yield func(grant *v2.Grant) bool) error
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
