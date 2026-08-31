// Package sourcecache defines the connector-facing surface of source-cache
// replay (see proto/c1/connector/v2/annotation_source_cache.proto).
//
// ADVANCED FUNCTIONALITY. Source-cache replay is an advanced, opt-in
// capability with strict correctness obligations on the connector (scope
// partitioning, validator lifetime — see the invariants below). Most
// connectors should not use this package; adopt it only in coordination
// with the SDK maintainers.
//
// WIRING. The syncer parses SourceCacheCapability from the Validate
// response at the start of every sync attempt (including resumes). When the
// capability is MODE_READ_WRITE and the previous-sync artifact passes the
// eligibility gates (Pebble engine, finished non-compacted FULL sync, clean
// ingest quality, current materialization witness, byte-matching compat
// record), the syncer installs a warm Lookup via SetLookup.SetSourceCache
// at sync start and delivers nil on every exit; otherwise connectors
// observe NoopLookup and every lookup misses. The lookup reaches connector
// code through SyncOpAttrs.Lookup at the four list sites (resources, static
// entitlements, entitlements, grants).
//
// DELIVERY LIMITATION (CO-6b-001 in
// docs/verification/sync-replay-6b/plan.md). SetSourceCache is only a
// conduit: the transport that constructs the connector client must also
// wire a live in-process setter behind it. The standard subprocess wrapper
// (internal/connector.NewWrapper) has no RPC backchannel for an interface
// value and never wires one, so subprocess-run connectors observe
// NoopLookup in this phase; in-tree, only the chaos harness's in-process
// clients deliver. The syncer probes deliverability before install
// (LookupDeliverabilityProbe) and keeps the consume side cold when the
// transport cannot deliver — no warm lookup, and any SourceCacheReplay
// annotation fails loud — so a structurally-satisfied but unwired
// SetLookup is never reported warm. The produce side (row stamping,
// validator publish) still runs, so such syncs build artifacts that can
// seed a warm sync once a delivering transport exists — unless the
// artifact was quality-blocked as a replay source (unreplayable page
// shapes such as child resource types or InsertResourceGrants, compat
// drift or capability withdrawal across a resume, or row-partition
// violations), in which case it seeds nothing and the next sync runs
// cold.
//
// Composition with external-resource reconciliation (CO-016): connectors
// using ExternalResourceMatch* annotations get their placeholder-grant
// scopes poisoned every sync by design — destructive reconciliation
// deletes match-annotated placeholder grants and re-issues derived grants
// unscoped, which the row-partition contract records as poison. Poison
// reads as a lookup miss, so those scopes cold-fetch inside an
// otherwise-warm sync: graceful, pre-replay behavior. Replay for such
// scopes cannot engage until the non-destructive reconciliation rework
// lands.
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
// connector-supplied scope key. The validator (etag, delta token) is opaque
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
// SESSION STORE (CO-6b-009 in docs/verification/sync-replay-6b/plan.md).
// The connector session store carries the same discipline:
//
//   - Sessions are ATTEMPT-scoped under this protocol. A resumed sync
//     clears its session namespace before any connector call, because
//     session writes commit outside the checkpoint mechanism and a
//     crashed attempt's cached premises would otherwise silently feed
//     rounds whose rows the resume re-grounds. Treat sessions as a cache
//     that can vanish between any two calls; rebuild on miss.
//   - Replay/record verdicts must be derived from upstream evidence
//     (conditional requests, delta tokens), never from session-cached
//     verdicts — a session-cached MATCH is exactly the "validator that
//     outlives its evidence" shape above, and it launders staleness past
//     every SDK gate.
//   - A replayed scope's rows never pass through the connector, so
//     session state built while GENERATING rows ("principals I emitted
//     this sync") is silently partial for scopes the connector chose to
//     replay. Do not consume such state for cross-scope decisions unless
//     every contributing scope was fetched fresh this sync.
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

// ValidateScopeKey returns an error when scopeKey is empty or
// unreasonably long. Connectors conventionally use HashScope, but any
// stable identifier is accepted.
func ValidateScopeKey(scopeKey string) error {
	if scopeKey == "" {
		return fmt.Errorf("source cache scope key is required")
	}
	if len(scopeKey) > maxScopeKeyLen {
		return fmt.Errorf("source cache scope key too long: %d bytes (max %d)", len(scopeKey), maxScopeKeyLen)
	}
	return nil
}

// Entry is a previous sync's persisted validator for one scope.
type Entry struct {
	// CacheValidator is opaque to the SDK: an HTTP ETag, delta token, etc.
	CacheValidator string

	// DiscoveredAt is when the entry was written.
	DiscoveredAt time.Time
}

// Lookup resolves a scope's previous-sync validator. The SDK provides an
// implementation on SyncOpAttrs; connectors call it before revalidating
// upstream.
type Lookup interface {
	// LookupPreviousSourceCache returns the previous sync's entry for
	// (rowKind, scopeKey). found=false means no entry: fetch fresh.
	// Implementations must treat internal read errors that leave fresh
	// fetch available as misses rather than failing the connector call.
	LookupPreviousSourceCache(ctx context.Context, rowKind RowKind, scopeKey string) (entry Entry, found bool, err error)
}

// NoopLookup is the Lookup installed when source cache is disabled or
// degraded. Every lookup misses.
type NoopLookup struct{}

var _ Lookup = NoopLookup{}

func (NoopLookup) LookupPreviousSourceCache(context.Context, RowKind, string) (Entry, bool, error) {
	return Entry{}, false, nil
}

// SetLookup is implemented by connector clients/servers that can receive a
// source-cache lookup implementation from the sync runner. The SDK calls
// SetSourceCache(lookup) at the start of each sync and SetSourceCache(nil)
// when the sync ends so a late RPC can't read stale state.
type SetLookup interface {
	SetSourceCache(ctx context.Context, lookup Lookup)
}

// LookupDeliverabilityProbe is optionally implemented by connector clients
// whose SetSourceCache may be a structural no-op — a wrapper that satisfies
// SetLookup while forwarding to a transport that cannot carry an interface
// value (the runner's subprocess wrapper, CO-6b-001). False means the
// connector will observe NoopLookup no matter what the syncer delivers, so
// lookup install must not report warm. Clients that do not implement the
// probe are presumed deliverable: they own their SetSourceCache and are
// expected to satisfy SetLookup only when delivery is real.
//
// This interface is defined here — rather than privately in pkg/sync — so
// implementing clients can compile-pin it (var _ LookupDeliverabilityProbe
// = ...) and a method rename cannot silently sever the probe from the
// syncer's type assertion.
type LookupDeliverabilityProbe interface {
	SourceCacheLookupDeliverable() bool
}

// MaterializationWitnessReader is the optional store capability behind the
// G5 / CO-017 cross-version fold fence: the previous artifact's envelope
// must carry the save-time materialization witness byte-equal to this SDK's
// MaterializationPolicyGeneration or the sync degrades cold. Named here —
// rather than asserted inline in pkg/sync — so the implementing store can
// compile-pin it (var _ MaterializationWitnessReader = ...) and a method
// rename cannot silently sever the fence from the syncer's type assertion
// (same discipline as LookupDeliverabilityProbe).
type MaterializationWitnessReader interface {
	SourceCacheMaterializationWitness() string
}

// HashScope returns the lowercase-hex sha256 of a canonical scope string.
// Convenience for connectors; any stable identifier is acceptable as a
// scope key (only non-emptiness and a length cap are enforced).
func HashScope(canonicalScope string) string {
	sum := sha256.Sum256([]byte(canonicalScope))
	return hex.EncodeToString(sum[:])
}
