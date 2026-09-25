package c1zstore

import (
	"context"
	"time"

	"github.com/conductorone/baton-sdk/pkg/sourcecache"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// Request arguments, not an execution ID; distinct work may share them.
type LedgerActionIdentity struct {
	Op                   string
	ResourceTypeID       string
	ResourceID           string
	ParentResourceTypeID string
	ParentResourceID     string
	PageToken            string
	TypeScoped           bool
}

// LedgerChild is an action a page pushed. Spawned is the syncer's
// accounting flag for a connector-enqueued sibling cursor, not part of the
// child's identity.
type LedgerChild struct {
	WorkID   uint64 `json:"work_id,omitempty"`
	Identity LedgerActionIdentity
	Spawned  bool
}

type LedgerCollectionStats struct {
	ListResponses                      uint64 `json:"list_responses"`
	EmptyListResponses                 uint64 `json:"empty_list_responses"`
	EmptyListResponsesWithContinuation uint64 `json:"empty_list_responses_with_continuation"`
	ResourceTypesReceived              uint64 `json:"resource_types_received"`
	ResourcesReceived                  uint64 `json:"resources_received"`
	EntitlementsReceived               uint64 `json:"entitlements_received"`
	GrantsReceived                     uint64 `json:"grants_received"`
	ResourceTypesExcludedBySelection   uint64 `json:"resource_types_excluded_by_selection"`
	EntitlementsExcludedByType         uint64 `json:"entitlements_excluded_by_type"`
	GrantsExcludedByType               uint64 `json:"grants_excluded_by_type"`
	DerivedResourcesExcludedByType     uint64 `json:"derived_resources_excluded_by_type"`
	ResourceTypesExcludedInvalid       uint64 `json:"resource_types_excluded_invalid"`
	ResourcesExcludedInvalid           uint64 `json:"resources_excluded_invalid"`
	EntitlementsExcludedInvalid        uint64 `json:"entitlements_excluded_invalid"`
}

type LedgerRow struct {
	WorkID       uint64
	WorkRevision uint64
	Collection   *LedgerCollectionStats
	Identity     LedgerActionIdentity
	// Empty when the action finished, and after a scrub (Scrubbed).
	NextPageToken string
	Children      []LedgerChild
	Attempt       string
	CommittedAt   time.Time

	ResourceTypesWritten uint64
	ResourcesWritten     uint64
	EntitlementsWritten  uint64
	GrantsWritten        uint64

	Replayed          bool
	TypeScopedPlanned bool
	// Scrubbed: tokens replaced by hashes at seal; Identity.PageToken,
	// NextPageToken and the children's tokens are empty.
	Scrubbed bool
	// Spawned: the page's own action was a spawned cursor (see LedgerChild).
	Spawned bool

	ObservationsRecorded     bool
	ConnectorAttempts        uint64
	ConnectorErrors          uint64
	SDKRetryWaitDuration     time.Duration
	SDKRateLimitWaitDuration time.Duration

	PageDuration      time.Duration
	ConnectorDuration time.Duration
	WaitDuration      time.Duration
}

// Folding buckets: counters, call totals and durations sum; max latencies
// take the max; flags OR.
type LedgerCounters struct {
	Counters       map[string]uint64
	Flags          uint64
	ConnectorCalls map[string]CallStat
	// Run-level, carried by the run's RunBucketWorker bucket only.
	StepDurationsMs map[string]int64
	SessionCalls    map[string]CallStat
}

func (c LedgerCounters) IsZero() bool {
	return len(c.Counters) == 0 && c.Flags == 0 &&
		len(c.ConnectorCalls) == 0 && len(c.StepDurationsMs) == 0 && len(c.SessionCalls) == 0
}

// Reserved index of the run-level stats bucket; page buckets use real worker
// indexes.
const RunBucketWorker uint32 = 0xFFFFFFFF

// Written by a page batch when SetRetainLedgerTokens was declared, so the
// seal honors it in whatever process finishes the sync.
const LedgerFactRetainTokens = "c1z.retain_tokens" //nolint:gosec // Ledger fact name, not a credential value.

const LedgerFactDiscardOnSeal = "c1z.discard_ledger_on_seal"

// Reserved index for the takeover's migrated counters. Buckets are blind-written
// whole totals, so sharing worker 0 or RunBucketWorker would overwrite them.
const TakeoverBucketWorker uint32 = 0xFFFFFFFE

type LedgerFrontier struct {
	State       string
	Attempt     string
	TakenOverAt time.Time
}

type CallStat struct {
	Count    int64
	TotalMs  int64
	MaxMs    int64
	Errors   int64
	Timeouts int64
}

func (c *CallStat) Add(o CallStat) {
	c.Count += o.Count
	c.TotalMs += o.TotalMs
	c.Errors += o.Errors
	c.Timeouts += o.Timeouts
	if o.MaxMs > c.MaxMs {
		c.MaxMs = o.MaxMs
	}
}

type RunStats struct {
	StepDurationsMs    map[string]int64
	ConnectorCallStats map[string]CallStat
	SessionStoreStats  map[string]CallStat
	CompletedActions   uint64
}

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

type SyncStats struct {
	Run           RunStats
	IngestQuality *IngestQuality
}

// Not safe for concurrent use.
type PageWriter interface {
	// Commit checks this revision and atomically applies the row's continuation
	// and children to pending work alongside records and accounting. Optional
	// childKeys align with row.Children; nonempty keys claim unique scheduling
	// relations in that batch, independently of pagination request identity.
	SetPendingWork(work LedgerWork, childKeys ...string) error

	PutResourceTypes(ctx context.Context, resourceTypes ...*v2.ResourceType) error
	PutResources(ctx context.Context, resources ...*v2.Resource) error
	PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error
	PutGrants(ctx context.Context, grants ...*v2.Grant) error
	// Snapshots data; repeated asset IDs use the last staged value.
	PutAsset(ctx context.Context, assetRef *v2.AssetRef, contentType string, data []byte) error

	// Reads include staged writes; repeated writes to the same identity use
	// the latest value. GetEntitlement rejects IDs shared by distinct identities.
	GetResource(ctx context.Context, resourceTypeID, resourceID string) (*v2.Resource, error)
	GetEntitlement(ctx context.Context, entitlementID string) (*v2.Entitlement, error)

	// Applied in the commit after the page's puts.
	DeleteGrants(ctx context.Context, grants ...*v2.Grant) error

	// Removes buffered rows a same-page source-cache tombstone names; rows
	// already in the store are the store tombstone's business.
	DropStagedSourceCacheRows(ctx context.Context, kind sourcecache.RowKind, scopeKey string, canonicalIDs, principalIDs []string) (int, error)

	SetFact(name string) error
	// Last writer wins.
	SetFactValue(name, value string) error
	// The worker's cumulative total for the run, never a delta; last call
	// before Commit wins.
	SetCounterBucket(runID string, worker uint32, counters LedgerCounters) error

	// On failure nothing of the page lands, but the store is durably stamped
	// ledgered before the first commit, so a failed first page does not permit
	// falling back to checkpoint tokens.
	Commit(ctx context.Context, id LedgerActionIdentity, row *LedgerRow) error
	Discard()
}

type PageLedgerStore interface {
	// PendingWork returns at most limit entries in descending ID order; beforeID
	// is exclusive when nonzero. limit is 1–100. initialized distinguishes absent from empty state.
	PendingWork(ctx context.Context, beforeID uint64, limit int) (work []LedgerWork, initialized bool, err error)
	// Seeds an absent queue in stack order; an initialized queue is unchanged.
	InitializePendingWork(ctx context.Context, work []LedgerWork, facts ...string) error
	PendingWorkAfter(ctx context.Context, afterID uint64, limit int) ([]LedgerWork, bool, error)
	HasScheduledWork(ctx context.Context, key string) (bool, error)
	// Removes a completed local phase and records cumulative run accounting;
	// no completed-page row or transaction around that phase's writes is added.
	CompletePendingWork(ctx context.Context, work LedgerWork, runID string, counters LedgerCounters) error
	// Consumes the matching checkpoint and seeds pending work in the same batch.
	TakeoverPendingWork(ctx context.Context, runID, expectedToken string, facts []string, counters LedgerCounters, work []LedgerWork) (string, error)

	GenerateLedgerReport(ctx context.Context) ([]byte, error)
	// Saves retained history or returns the report already archived during disposal.
	ArchiveLedgerReport(ctx context.Context) ([]byte, error)
	GetArchivedLedgerReport(ctx context.Context) ([]byte, error)
	// Empty attempt selects latest; only the first and latest snapshots are retained.
	GetArchivedLedgerOptions(ctx context.Context, attempt string) (*LedgerReportOptions, error)
	// Restores an empty finished ledger, or matching unfinished discard recovery state.
	RestoreLedgerArchive(ctx context.Context) error
	BeginPage() PageWriter
	// Diagnostic lookup by request arguments; multiple work instances may match.
	// PendingWork is the recovery authority.
	GetLedgerRow(ctx context.Context, id LedgerActionIdentity) (row *LedgerRow, found bool, err error)
	// Keeps verbatim page tokens in the sealed artifact. The default scrubs them:
	// a page token can carry a credential.
	SetRetainLedgerTokens(retain bool)

	LedgerFacts(ctx context.Context) (map[string]string, error)
	// Before an attempt starts writing, atomically fold older buckets into one total.
	// Prior-attempt writers must be stopped. Current buckets are preserved; repeated calls are idempotent.
	FoldLedgerCounters(ctx context.Context, currentRunID string) error
	LedgerCounters(ctx context.Context) (LedgerCounters, error)
	LedgerFrontier(ctx context.Context) (frontier *LedgerFrontier, found bool, err error)
	// Migrates the open sync's checkpoint token into the ledger in one unit;
	// "" when there was no token.
	TakeoverToken(ctx context.Context, runID string, facts []string, counters LedgerCounters) (state string, err error)
	BoundSyncFinished(ctx context.Context) (bool, error)
	// True only for an unfinished binding without checkpoint, archive, records or collection/replay state.
	// Read-only; session state does not count.
	BoundSyncUnstarted(ctx context.Context) (bool, error)
	// Preserves records and sync metadata; removes ledger rows, facts and counters.
	DropLedger(ctx context.Context) error
	// Clears page rows, the takeover frontier and named facts in one synced
	// batch. Requires an ended bound sync with no pending work; retains counters,
	// all other facts, records and sync metadata for processing under the same sync ID.
	ClearLedgerRows(ctx context.Context, clearFacts []string) error
	// Blind-writes the run's whole cumulative bucket; a later write supersedes.
	PutCounterBucket(ctx context.Context, runID string, worker uint32, counters LedgerCounters) error
	// Completes collection with no pending work. Plain EndSync preserves recovery state.
	// LedgerFactDiscardOnSeal archives then discards the ledger before finishing.
	// Report-generation failure still discards history; recovery-state write failure prevents seal.
	EndSyncWithStats(ctx context.Context, stats SyncStats) error
}
