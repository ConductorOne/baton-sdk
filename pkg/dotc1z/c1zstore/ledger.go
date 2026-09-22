package c1zstore

import (
	"context"
	"iter"
	"time"

	"github.com/conductorone/baton-sdk/pkg/sourcecache"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// A store implementing PageLedgerStore commits one syncer page and its ledger
// row as one unit. Stores that cannot (SQLite) do not implement it; the
// syncer type-asserts and falls back to the token-only path.

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
	Collection *LedgerCollectionStats
	Identity   LedgerActionIdentity
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
	PutResourceTypes(ctx context.Context, resourceTypes ...*v2.ResourceType) error
	PutResources(ctx context.Context, resources ...*v2.Resource) error
	PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error
	PutGrants(ctx context.Context, grants ...*v2.Grant) error
	// Ordered stored/staged merge, with pending deletes omitted. Staging is captured when iteration starts.
	ListGrantsWithAnnotations(ctx context.Context) iter.Seq2[GrantAnnotation, error]
	// Snapshots data; repeated asset IDs use the last staged value.
	PutAsset(ctx context.Context, assetRef *v2.AssetRef, contentType string, data []byte) error

	// Reads include staged writes; repeated writes to the same identity use
	// the latest value. GetEntitlement rejects IDs shared by distinct identities.
	GetResource(ctx context.Context, resourceTypeID, resourceID string) (*v2.Resource, error)
	GetEntitlement(ctx context.Context, entitlementID string) (*v2.Entitlement, error)

	// Applied after all page puts, without cascading. Reads ignore pending deletes.
	DeleteResources(ctx context.Context, resources ...*v2.Resource) error
	DeleteEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error

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
	GenerateLedgerReport(ctx context.Context) ([]byte, error)
	ArchiveLedgerReport(ctx context.Context) ([]byte, error)
	GetArchivedLedgerReport(ctx context.Context) ([]byte, error)
	GetArchivedLedgerOptions(ctx context.Context, attempt string) (*LedgerReportOptions, error)
	RestoreLedgerArchive(ctx context.Context) error
	BeginPage() PageWriter
	// found is false when no row exists and when a row echoes a different
	// identity; either way the page must run.
	GetLedgerRow(ctx context.Context, id LedgerActionIdentity) (row *LedgerRow, found bool, err error)
	// Keeps verbatim page tokens in the sealed artifact. The default scrubs them:
	// a page token can carry a credential.
	SetRetainLedgerTokens(retain bool)

	LedgerFacts(ctx context.Context) (map[string]string, error)
	LedgerCounters(ctx context.Context) (LedgerCounters, error)
	LedgerFrontier(ctx context.Context) (frontier *LedgerFrontier, found bool, err error)
	// Migrates the open sync's checkpoint token into the ledger in one unit;
	// "" when there was no token.
	TakeoverToken(ctx context.Context, runID string, facts []string, counters LedgerCounters) (state string, err error)
	BoundSyncFinished(ctx context.Context) (bool, error)
	// The syncer calls it when rebinding a FINISHED sync: trusting the old rows
	// would make every action look complete.
	DropLedger(ctx context.Context) error
	// Clears page rows, the takeover frontier and named facts in one synced
	// batch. Requires a finished bound sync; retains counters, all other facts,
	// records and sync metadata for further processing under the same sync ID.
	ClearLedgerRows(ctx context.Context, clearFacts []string) error
	// Blind-writes the run's whole cumulative bucket; a later write supersedes.
	PutCounterBucket(ctx context.Context, runID string, worker uint32, counters LedgerCounters) error
	// The only way a ledgered sync seals; plain EndSync refuses one.
	EndSyncWithStats(ctx context.Context, stats SyncStats) error
}
