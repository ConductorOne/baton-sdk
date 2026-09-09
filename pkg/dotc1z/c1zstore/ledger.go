package c1zstore

import (
	"context"
	"time"

	"github.com/conductorone/baton-sdk/pkg/sourcecache"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// The page ledger contract (docs/tasks/sound-syncs-solutions-brief.md
// §3). A store that implements PageLedgerStore can commit one syncer
// page — every record the page wrote plus a row recording the page as
// done — as a single atomic unit, and can answer "did this page
// commit?" on resume. Engines that cannot (SQLite) simply do not
// implement it; the syncer checks with a type assertion and falls back
// to the token-only path.
//
// The types here are engine-neutral mirrors of the storage protos so
// the sync pipeline never imports c1/storage/v3.

// LedgerActionIdentity is the semantic identity of one syncer action
// instance (brief §3.2): the flat tuple that determines the connector
// request the page makes. Equal identity ⇒ equal work. Op is the
// action kind's stable string name (pkg/sync.ActionOp.String()).
type LedgerActionIdentity struct {
	Op                   string
	ResourceTypeID       string
	ResourceID           string
	ParentResourceTypeID string
	ParentResourceID     string
	PageToken            string
	TypeScoped           bool
	// Spawned is carried for faithful child re-push; it is compared but
	// does not participate in the ledger key.
	Spawned bool
}

// LedgerRow is one committed page's completion record.
type LedgerRow struct {
	Identity LedgerActionIdentity
	// NextPageToken is the action's cursor after this page; "" means the
	// action finished. Empty after a scrub (see Scrubbed).
	NextPageToken string
	// Children are the actions this page pushed, with full identity.
	Children []LedgerActionIdentity
	// Attempt is the syncer attempt that committed the page.
	Attempt     string
	CommittedAt time.Time

	ResourceTypesWritten uint64
	ResourcesWritten     uint64
	EntitlementsWritten  uint64
	GrantsWritten        uint64

	// Replayed: the page's rows came from source-cache replay.
	Replayed bool
	// TypeScopedPlanned: the action's type-scoped planning ran on this
	// page (post-transition action state that must ride in the row).
	TypeScopedPlanned bool
	// Scrubbed: tokens were replaced by hashes at seal (the connector
	// declared its page tokens sensitive). Identity.PageToken,
	// NextPageToken and children tokens are empty.
	Scrubbed bool

	// Per-page timings (brief §3.12). Zero when not measured.
	PageDuration      time.Duration
	ConnectorDuration time.Duration
	WaitDuration      time.Duration
}

// LedgerCounters is one (run, worker) bucket's contents, or the
// sync-level fold of every bucket (brief §3.6, §3.13): counters, call
// totals and durations sum; max latencies take the max; flags OR.
// Names are the syncer's; the store treats them as opaque.
type LedgerCounters struct {
	Counters map[string]uint64
	Flags    uint64
	// ConnectorCalls: calls made by the bucket's committed pages, by
	// method.
	ConnectorCalls map[string]CallStat
	// StepDurationsMs / SessionCalls: run-level stats, carried by the
	// run's reserved bucket (see SyncStatsStore.PutCounterBucket).
	StepDurationsMs map[string]int64
	SessionCalls    map[string]CallStat
}

// RunBucketWorker is the reserved worker index of a run's run-level
// stats bucket. Page buckets use real worker indexes (bounded by the
// worker count); nothing else writes this index.
const RunBucketWorker uint32 = 0xFFFFFFFF

// LedgerFrontier is the takeover record of a sync that began token-only
// (brief §3.8): the checkpoint state the ledger now owns.
type LedgerFrontier struct {
	State       string
	Attempt     string
	TakenOverAt time.Time
}

// CallStat is one method's cumulative call counters (mirror of
// storage CallStat).
type CallStat struct {
	Count    int64
	TotalMs  int64
	MaxMs    int64
	Errors   int64
	Timeouts int64
}

// Add folds another CallStat in: counts and totals sum, max takes the
// max.
func (c *CallStat) Add(o CallStat) {
	c.Count += o.Count
	c.TotalMs += o.TotalMs
	c.Errors += o.Errors
	c.Timeouts += o.Timeouts
	if o.MaxMs > c.MaxMs {
		c.MaxMs = o.MaxMs
	}
}

// RunStats is the sync's whole-run timing / call stats (brief §3.13):
// the fold of every counter bucket's stats. Names are the syncer's;
// the store treats them as opaque.
type RunStats struct {
	StepDurationsMs    map[string]int64
	ConnectorCallStats map[string]CallStat
	SessionStoreStats  map[string]CallStat
	CompletedActions   uint64
}

// IngestQuality is the sync's connector-ingestion quality (mirror of
// storage IngestQualityStats).
type IngestQuality struct {
	SourceCacheReplayBlocked      bool
	EntitlementsDropped           uint64
	GrantsDropped                 uint64
	GrantResourcesDropped         uint64
	ExpansionResourceTypesDropped uint64
	ExpansionsDropped             uint64
	InvalidResourceTypesObserved  uint64
	InvalidResourcesObserved      uint64
	InvalidEntitlementsObserved   uint64
	ReasonFlags                   uint64
}

// SyncStats is what the syncer hands the store at seal
// (EndSyncWithStats): the sync's whole-run timing / call stats (the fold
// over every attempt) and its ingest quality. The store lays them over
// the record counts it computes itself when it persists the sync's
// stats. This is the front door the checkpoint token used to be a back
// door for.
type SyncStats struct {
	Run           RunStats
	IngestQuality *IngestQuality
}

// PageWriter buffers one page's writes and commits them with the
// page's ledger row in one atomic unit. Not safe for concurrent use.
// The Put* methods mirror connectorstore.Writer's so a handler can be
// pointed at either. Discard drops everything; a discarded or failed
// page is, to the store, a page that never ran.
type PageWriter interface {
	PutResourceTypes(ctx context.Context, resourceTypes ...*v2.ResourceType) error
	PutResources(ctx context.Context, resources ...*v2.Resource) error
	PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error
	PutGrants(ctx context.Context, grants ...*v2.Grant) error

	// GetResource is the page-scoped read: the page's own staged
	// resource if it has one, else the store's. Returns
	// connectorstore.ErrResourceNotFound semantics via the store's
	// usual not-found error.
	GetResource(ctx context.Context, resourceTypeID, resourceID string) (*v2.Resource, error)
	// GetEntitlement is the page-scoped read for entitlements, by id.
	GetEntitlement(ctx context.Context, entitlementID string) (*v2.Entitlement, error)

	// DeleteGrants removes grants by their structural refs as part of the
	// page: staged now, applied in the commit after the page's puts, so
	// a crash never leaves the removal without the rows that replace it
	// (the external-resource phase deletes the originals it re-issues).
	DeleteGrants(ctx context.Context, grants ...*v2.Grant) error

	// DropStagedSourceCacheRows removes buffered rows that a source-cache
	// tombstone in the same page names (the page's own put-then-delete).
	// Rows already in the store are the store's tombstone's business
	// (SourceCacheStore.DeleteSourceCacheRows*); the two together give
	// a page "upserts, then deletes" regardless of when its buffer lands.
	// canonicalIDs are the kind's public ids; principalIDs match grants
	// by principal id and resources by resource id within scopeKey.
	DropStagedSourceCacheRows(kind sourcecache.RowKind, scopeKey string, canonicalIDs, principalIDs []string) (int, error)

	// SetFact records a sync-level fact the page established (a monotone
	// bit such as "needs_expansion"); it lands in the page's unit.
	SetFact(name string) error
	// SetFactValue is SetFact with a value (a source-cache hit's
	// validator); last writer wins.
	SetFactValue(name, value string) error
	// SetCounterBucket sets the (run, worker) counter bucket the page's
	// commit writes: the worker's cumulative total for the run, never a
	// delta. Last call before Commit wins.
	SetCounterBucket(runID string, worker uint32, counters LedgerCounters) error

	// Commit applies the buffered records and the ledger row for id in
	// one unit. row may be nil (a bare completion). On failure nothing
	// landed and the writer remains usable for a retry.
	Commit(ctx context.Context, id LedgerActionIdentity, row *LedgerRow) error
	Discard()
}

// PageLedgerStore is implemented by stores that support atomic pages.
type PageLedgerStore interface {
	// BeginPage starts buffering a page.
	BeginPage() PageWriter
	// GetLedgerRow reports whether the page identified by id committed.
	// found is false both when no row exists and when a row exists but
	// echoes a different identity (a key collision, counted by the
	// store): in either case the page must run.
	GetLedgerRow(ctx context.Context, id LedgerActionIdentity) (row *LedgerRow, found bool, err error)
	// SetLedgerTokensSensitive declares that the connector's page tokens
	// may carry credentials; the store scrubs ledger tokens to hashes
	// at seal.
	SetLedgerTokensSensitive(sensitive bool)

	// LedgerFacts returns every fact a committed page (or the takeover)
	// established. Absent means never durably established.
	LedgerFacts(ctx context.Context) (map[string]string, error)
	// LedgerCounters folds every counter bucket into the sync-level value.
	LedgerCounters(ctx context.Context) (LedgerCounters, error)
	// LedgerFrontier returns the takeover record, if any.
	LedgerFrontier(ctx context.Context) (frontier *LedgerFrontier, found bool, err error)
	// TakeoverToken migrates the open sync's checkpoint token into the
	// ledger in one unit: frontier record, the given facts, an initial
	// counter bucket under runID, and the token cleared. Returns the
	// state it moved; "" when there was no token.
	TakeoverToken(ctx context.Context, runID string, facts []string, counters LedgerCounters) (state string, err error)
	// BoundSyncFinished reports whether the currently bound sync has
	// already sealed (its run record carries an end time). Metadata
	// only — no stats are computed.
	BoundSyncFinished(ctx context.Context) (bool, error)
	// ResetLedger drops the whole ledger family — rows, facts, buckets,
	// frontier. The syncer calls it when it rebinds a FINISHED sync
	// (WithSyncID over a sealed run: the compactor's expansion pass, the
	// rollback tool's replay, a reused syncer): the retained ledger
	// describes the run that produced the sealed data, and a rebind is a
	// new run that rewrites it. Trusting the old rows would make every
	// action look complete and seal the rebind without doing anything.
	ResetLedger(ctx context.Context) error
}

// SyncStatsStore is the stats side of a store that writes no checkpoint
// token (brief §3.13). Page-shaped stats ride each page's bucket
// (PageWriter.SetCounterBucket); what is not page-shaped — phase
// durations, session-store calls — goes in the run's reserved bucket
// through PutCounterBucket, and the seal takes the fold. Separate from
// PageLedgerStore because it is about the sync's stats, not its pages;
// the syncer requires both when atomic pages are on.
type SyncStatsStore interface {
	// PutCounterBucket blind-writes one (run, worker) bucket outside a
	// page: the run's whole cumulative value for that index, so a later
	// write supersedes an earlier one and the fold never double counts.
	// The syncer uses it for RunBucketWorker only, best-effort, at
	// phase boundaries, stop and seal.
	PutCounterBucket(ctx context.Context, runID string, worker uint32, counters LedgerCounters) error
	// EndSyncWithStats seals the sync with its final stats: the store
	// persists them with the record counts it computes at seal. This is
	// the only way a ledgered sync seals — plain EndSync refuses a sync
	// whose ledger is in flight, so stats can never be silently absent.
	EndSyncWithStats(ctx context.Context, stats SyncStats) error
}
