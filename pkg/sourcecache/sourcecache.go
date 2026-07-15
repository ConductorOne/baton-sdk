// Package sourcecache defines the connector-facing surface of source-cache
// replay (see proto/c1/connector/v2/annotation_source_cache.proto).
//
// A connector that can cheaply revalidate upstream data — HTTP conditional
// requests (GitHub), delta queries (Microsoft Graph) — opts in by attaching
// SourceCacheCapability MODE_READ_WRITE to its Validate response. During a
// sync it looks up the previous validator for a scope via the Lookup the SDK
// provides on SyncOpAttrs, revalidates upstream, and either emits fresh rows
// tagged with SourceCacheRecord or asks the SDK to replay the previous rows
// with SourceCacheReplay.
//
// The connector owns scope computation; the SDK only keys storage by the
// connector-supplied scope key. The validator (HTTP ETag, delta token, ...) is opaque
// to the SDK.
//
// Invariant that keeps replay safe: a connector must only emit
// SourceCacheReplay for a scope whose validator it received from THIS sync's
// Lookup. The lookup need not happen in the same call that emits the
// replay: a planning call may batch-resolve many scopes and pass the
// verdicts to sibling cursors through EnqueuePageTokens page tokens — that
// satisfies the invariant, because the validator still originates from the
// consuming sync. What's forbidden is a validator that outlives a sync
// (connector-side caches, config, upstream echoes). When source cache is
// disabled or degraded (no capability, no usable previous sync, unsupported
// storage engine) the SDK installs NoopLookup, every lookup misses, and a
// well-behaved connector naturally falls back to full fetch.
//
// Replay equivalence: a cached sync must reproduce what a full resync
// would produce. Replayed rows are verbatim copies of the previous sync's
// rows with one deliberate exception — expander-written Sources on direct
// grants (classified by a self-source entry, mirroring RollbackExpansion)
// are stripped at copy time so the current sync's expansion recomputes
// them from true state; re-expansion only adds contributions, so carrying
// them verbatim would immortalize contributions removed upstream.
// Connector-set Sources (no self-source) are public connector data and
// survive replay byte-for-byte.
package sourcecache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// RowKind partitions source-cache scopes by the row type they produce.
// It doubles as the row_kind value stored in SourceCacheEntryRecord.
type RowKind string

const (
	RowKindResources    RowKind = "resources"
	RowKindEntitlements RowKind = "entitlements"
	RowKindGrants       RowKind = "grants"
)

// Valid reports whether k is one of the defined row kinds.
func (k RowKind) Valid() bool {
	switch k {
	case RowKindResources, RowKindEntitlements, RowKindGrants:
		return true
	}
	return false
}

// ValidateRowKind returns an error if rowKind is not one of the known
// RowKind* constants.
func ValidateRowKind(rowKind RowKind) error {
	if !rowKind.Valid() {
		return fmt.Errorf("invalid source cache row kind: %q", rowKind)
	}
	return nil
}

// maxScopeKeyLen bounds scope identifiers on the wire and in storage
// keys. Deliberately generous: the shape is a connector convention
// (HashScope produces 64 hex chars) and is not enforced beyond
// non-emptiness and this cap while the model is being proven out against
// real providers.
const maxScopeKeyLen = 256

// ValidateScopeKey returns an error when scopeKey is empty, unreasonably
// long, or contains a NUL byte. Connectors conventionally use HashScope,
// but any stable identifier is accepted. NUL is reserved: composite
// "kind\x00scope" keys appear in inspection surfaces (engine snapshots),
// and a scope key containing NUL could alias another entry there.
func ValidateScopeKey(scopeKey string) error {
	if scopeKey == "" {
		return fmt.Errorf("source cache scope key is required")
	}
	if len(scopeKey) > maxScopeKeyLen {
		return fmt.Errorf("source cache scope key too long: %d bytes (max %d)", len(scopeKey), maxScopeKeyLen)
	}
	if strings.ContainsRune(scopeKey, 0) {
		return fmt.Errorf("source cache scope key must not contain NUL bytes")
	}
	return nil
}

// Entry is a previous sync's persisted validator for one scope.
type Entry struct {
	// CacheValidator is the opaque upstream validator: a literal HTTP
	// ETag, a delta token, etc. Never interpreted by the SDK.
	CacheValidator string

	// DiscoveredAt is when the entry was written.
	DiscoveredAt time.Time
}

// Lookup resolves a scope's previous-sync validator. The SDK provides an
// implementation on SyncOpAttrs; connectors call it before revalidating
// upstream.
type Lookup interface {
	// Lookup returns the previous sync's entry for
	// (rowKind, scopeKey). found=false means no entry: fetch fresh.
	// Implementations must treat internal read errors that leave fresh
	// fetch available as misses rather than failing the connector call.
	Lookup(ctx context.Context, rowKind RowKind, scopeKey string) (entry Entry, found bool, err error)
}

// NoopLookup is the Lookup installed when source cache is disabled or
// degraded. Every lookup misses.
type NoopLookup struct{}

var _ Lookup = NoopLookup{}

func (NoopLookup) Lookup(context.Context, RowKind, string) (Entry, bool, error) {
	return Entry{}, false, nil
}

// SourceCacheSetter is implemented by connector clients/servers that can receive a
// source-cache lookup implementation from the sync runner. The SDK calls
// SetSourceCache(lookup) at the start of each sync and SetSourceCache(nil)
// when the sync ends so a late RPC can't read stale state. Calls must be
// balanced per sync — exactly one nil clear for each non-nil install, and
// no clear from a sync that never installed — because the shared
// subprocess slot counts installs to detect concurrent-sync contention
// (see GRPCServer.SetSourceCache).
type SourceCacheSetter interface {
	SetSourceCache(ctx context.Context, lookup Lookup)
}

// HashScope returns the lowercase-hex sha256 of a canonical scope string.
// Convenience for connectors; any stable identifier is acceptable as a
// scope key (only non-emptiness and a length cap are enforced).
func HashScope(canonicalScope string) string {
	sum := sha256.Sum256([]byte(canonicalScope))
	return hex.EncodeToString(sum[:])
}
