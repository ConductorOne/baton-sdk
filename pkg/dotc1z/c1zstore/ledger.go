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

// The pending-work declaration's phase: which operations the store accepts.
// Absent: no declaration. Collecting: pages may commit. Expanding: no pages;
// the expansion entry completes locally. Sealing: the terminal page landed;
// only the seal follows.
type LedgerQueuePhase uint8

// The values are the durable encoding; Expanding was added after Sealing.
const (
	LedgerQueueAbsent     LedgerQueuePhase = 0
	LedgerQueueCollecting LedgerQueuePhase = 1
	LedgerQueueSealing    LedgerQueuePhase = 2
	LedgerQueueExpanding  LedgerQueuePhase = 3
)

func (p LedgerQueuePhase) String() string {
	switch p {
	case LedgerQueueAbsent:
		return "absent"
	case LedgerQueueCollecting:
		return "collecting"
	case LedgerQueueExpanding:
		return "expanding"
	case LedgerQueueSealing:
		return "sealing"
	}
	return "invalid"
}

// Written by BeginPass: this pass began on a sealed sync. The archive links
// the pass's report to the preceding one when the fact is present.
const LedgerFactFollowOnPass = "c1z.pass.follow_on"

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
	// Commit then moves the declaration to Sealing in the page's batch. Commit
	// refuses unless the phase is Collecting or Expanding, no pending work
	// remains, and the row has no continuation, children or work.
	SetTerminal() error

	// On failure nothing of the page lands, but the store is durably stamped
	// ledgered before the first commit, so a failed first page does not permit
	// falling back to checkpoint tokens. Refused once the declaration is
	// Expanding or Sealing.
	Commit(ctx context.Context, id LedgerActionIdentity, row *LedgerRow) error
	Discard()
}

// What state the bound sync's pass is in, in one read.
type LedgerState struct {
	Phase    LedgerQueuePhase
	Finished bool // ended_at is set
	Token    bool // a legacy checkpoint token awaits takeover
}

// LedgerLifecycle is the pass's state machine. Each transition is one synced
// batch validated under the write lock; each is refused outside its source
// state. The Collecting/Expanding → Sealing transition is a page:
// PageWriter.SetTerminal.
//
//	Unstarted  --BeginCollecting-->                              Collecting
//	Unstarted+token --BeginFromToken-->                          Collecting | Expanding
//	Collecting --BeginExpanding-->                               Expanding
//	Collecting | Expanding --terminal page-->                    Sealing
//	Sealing    --Seal-->                                         Sealed
//	Sealed     --BeginPass-->                                    Collecting
type LedgerLifecycle interface {
	State(ctx context.Context) (LedgerState, error)
	// Seeds an absent queue in stack order with the facts (a "" value is a
	// bare fact); an initialized queue is unchanged. Refused on a finished
	// sync: that is BeginPass.
	BeginCollecting(ctx context.Context, seeds []LedgerWork, facts map[string]string) error
	// Consumes the matching checkpoint token and seeds the queue in one batch
	// at the given phase, with the facts (a "" value is a bare fact). Returns
	// the token consumed.
	BeginFromToken(ctx context.Context, runID, expectedToken string, facts map[string]string, counters LedgerCounters, seeds []LedgerWork, phase LedgerQueuePhase) (string, error)
	// Requires the pending range to hold exactly the expansion entry.
	BeginExpanding(ctx context.Context) error
	// One batch: archive, the family's remaining keys, ended_at. Nothing is
	// written after it. LedgerFactDiscardOnSeal, read from the terminal page,
	// selects default disposal; its absence retains history.
	Seal(ctx context.Context, stats SyncStats) error
	// Opens a new collection pass on a finished sync with no declaration and
	// no legacy token, in one batch: prior rows, scheduling relations and
	// frontier go; archived facts the family lacks return, minus clearFacts;
	// archived counters return only when the family has no bucket; the seeds,
	// a Collecting declaration and LedgerFactFollowOnPass are staged.
	BeginPass(ctx context.Context, seeds []LedgerWork, clearFacts []string) error
}

// LedgerQueue is the pending work and the pages that consume it.
type LedgerQueue interface {
	// At most limit entries in descending ID order; beforeID is exclusive when
	// nonzero. limit is 1–100. The phase is read in the same call so a caller
	// sees one declaration state with the entries.
	PendingWork(ctx context.Context, beforeID uint64, limit int) (work []LedgerWork, phase LedgerQueuePhase, err error)
	PendingWorkAfter(ctx context.Context, afterID uint64, limit int) ([]LedgerWork, LedgerQueuePhase, error)
	HasScheduledWork(ctx context.Context, key string) (bool, error)
	BeginPage() PageWriter
	// Removes a completed local step and records cumulative run accounting;
	// no page row is added.
	CompletePendingWork(ctx context.Context, work LedgerWork, runID string, counters LedgerCounters) error
	// Diagnostic lookup by request arguments; multiple work instances may match.
	// PendingWork is the recovery authority.
	GetLedgerRow(ctx context.Context, id LedgerActionIdentity) (row *LedgerRow, found bool, err error)
}

// LedgerAccounting is the attempt-scoped facts and counters.
type LedgerAccounting interface {
	LedgerFacts(ctx context.Context) (map[string]string, error)
	LedgerCounters(ctx context.Context) (LedgerCounters, error)
	// Before an attempt starts writing, fold older buckets into one total.
	// Prior-attempt writers must be stopped; repeated calls are idempotent.
	FoldLedgerCounters(ctx context.Context, currentRunID string) error
	// Blind-writes the run's whole cumulative bucket; a later write supersedes.
	PutCounterBucket(ctx context.Context, runID string, worker uint32, counters LedgerCounters) error
	// Blind-writes named fact values outside any page, all in one synced
	// batch; a later write supersedes. An empty value records presence.
	PutLedgerFacts(ctx context.Context, facts map[string]string) error
}

// LedgerArchive is the sealed pass's report and retention.
type LedgerArchive interface {
	// Saves retained history or returns the report already archived at seal.
	ArchiveLedgerReport(ctx context.Context) ([]byte, error)
	GetArchivedLedgerReport(ctx context.Context) ([]byte, error)
	// Keeps verbatim page tokens in the sealed artifact. The default scrubs them:
	// a page token can carry a credential.
	SetRetainLedgerTokens(retain bool)
	// Preserves records and sync metadata; removes ledger rows, facts and counters.
	DropLedger(ctx context.Context) error
	LedgerFrontier(ctx context.Context) (frontier *LedgerFrontier, found bool, err error)
	// True only for an unfinished binding without checkpoint, archive, records or collection/replay state.
	BoundSyncUnstarted(ctx context.Context) (bool, error)
}

type PageLedgerStore interface {
	LedgerLifecycle
	LedgerQueue
	LedgerAccounting
	LedgerArchive
}
