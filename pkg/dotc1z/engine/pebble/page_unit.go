package pebble

// PageUnit is the engine side of the atomic page (docs/tasks/sound-
// syncs-solutions-brief.md §3.1): everything one syncer page writes,
// plus the ledger row that records the page as done, committed as ONE
// pebble batch. The store is therefore only ever in one of two states
// per page — not run (no rows, no ledger row) or done (rows and ledger
// row) — and a crash anywhere leaves exactly the set of committed
// pages, which the ledger enumerates.
//
// Buffer-then-stage, not stage-as-you-go. A page spans connector calls
// (seconds to minutes), so the unit cannot hold the write barrier for
// its lifetime, and staging encoded keys outside the barrier would race
// the read-before-write index cleanup between concurrent pages (two
// pages re-parenting the same resource would each read the same prior
// value and leave an orphan index entry). The unit therefore buffers
// TYPED records and, at Commit, runs the exact staging logic the
// Put*Records paths run — same dedup, same read-before-write, same
// typed rawdb ops — into one RecordBatch under the barrier, then adds
// the ledger row and commits once. Every invariant the single-call
// paths hold, the unit holds, because it is the same code.
//
// Reads inside a page see the page's own writes (brief §3.5): the
// unit answers GetResourceRecord from its buffer first, then the DB.
//
// Memory: the buffer is the page. For the write bursts the brief
// identifies (static entitlements over every resource of a type, a
// replayed "all users" scope) the SST vehicle (§3.5) will replace the
// batch above a size threshold; until then the unit is the batch path
// only and callers keep pages page-sized.

import (
	"fmt"

	"context"
	"errors"

	"github.com/conductorone/baton-sdk/pkg/bid"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// ErrPageUnitCommitted is returned by a unit used after Commit or
// Discard.
var ErrPageUnitCommitted = errors.New("pebble page unit: already committed or discarded")

// ErrPageUnitForeignSync is returned by Commit when the sync open now is
// not the one the page was begun under.
var ErrPageUnitForeignSync = errors.New("pebble page unit: sync changed since the page was begun")

type resourceBufKey struct{ rt, id string }

// PageUnit buffers one page's writes. Not safe for concurrent use; a
// page is executed by one worker.
type PageUnit struct {
	e *Engine
	// syncID is the sync open when the page was begun. Commit refuses to
	// land in any other one (see ErrPageUnitForeignSync).
	syncID string

	resourceTypes []*v3.ResourceTypeRecord
	resources     []*v3.ResourceRecord
	// resourceIdx maps identity → latest index in resources, for the
	// batch-then-DB read.
	resourceIdx  map[resourceBufKey]int
	entitlements []*v3.EntitlementRecord
	// entitlementIdx maps external id → latest index in entitlements.
	entitlementIdx map[string]int
	grants         []*v3.GrantRecord
	// grantDeletes are grants the page removes, by structural identity
	// (the external-resource phase's replaced originals). Applied at
	// Commit after the page's puts, in the same batch: a buffered put of
	// the same identity is dropped (the delete wins, as it does when the
	// two are separate store calls), and a store row is staged for
	// removal with its index cleanup.
	grantDeletes []grantIdentity

	// facts / bucket: sync-level state the page establishes (brief
	// §3.6), staged in the page's batch so it is exactly as durable as
	// the page. Facts are blind-set monotone bits; the bucket is the
	// worker's whole counter total for this run (not a delta).
	facts       []ledgerFact
	bucketKey   []byte
	bucketValue *v3.LedgerCounterBucket

	done bool
}

// ledgerFact is one staged fact: a name and an optional value.
type ledgerFact struct {
	name, value string
}

// StageFactValue records a sync-level fact with a value (a source-cache
// hit's validator). Last writer wins across pages.
func (u *PageUnit) StageFactValue(name, value string) error {
	if u.done {
		return ErrPageUnitCommitted
	}
	u.facts = append(u.facts, ledgerFact{name: name, value: value})
	return nil
}

// StageFact records that this page established the named sync-level
// fact (e.g. "needs_expansion"). Idempotent.
func (u *PageUnit) StageFact(name string) error {
	if u.done {
		return ErrPageUnitCommitted
	}
	u.facts = append(u.facts, ledgerFact{name: name})
	return nil
}

// StageCounterBucket sets the (run, worker) counter bucket this page's
// commit writes. The value is the worker's cumulative total for the
// run; the caller owns the cache. Last call wins.
func (u *PageUnit) StageCounterBucket(runID string, worker uint32, bucket *v3.LedgerCounterBucket) error {
	if u.done {
		return ErrPageUnitCommitted
	}
	u.bucketKey = encodeLedgerCounterKey(runID, worker)
	u.bucketValue = bucket
	return nil
}

// NewPageUnit starts buffering a page, bound to the sync open now.
func (e *Engine) NewPageUnit() *PageUnit {
	return &PageUnit{e: e, syncID: e.CurrentSyncID()}
}

// StageResourceTypes buffers resource types for the page's commit.
func (u *PageUnit) StageResourceTypes(records ...*v3.ResourceTypeRecord) error {
	if u.done {
		return ErrPageUnitCommitted
	}
	for _, r := range records {
		if r != nil {
			u.resourceTypes = append(u.resourceTypes, r)
		}
	}
	return nil
}

// StageResources buffers resources for the page's commit.
func (u *PageUnit) StageResources(records ...*v3.ResourceRecord) error {
	if u.done {
		return ErrPageUnitCommitted
	}
	for _, r := range records {
		if r == nil {
			continue
		}
		if u.resourceIdx == nil {
			u.resourceIdx = make(map[resourceBufKey]int)
		}
		u.resourceIdx[resourceBufKey{r.GetResourceTypeId(), r.GetResourceId()}] = len(u.resources)
		u.resources = append(u.resources, r)
	}
	return nil
}

// StageEntitlements buffers entitlements for the page's commit.
func (u *PageUnit) StageEntitlements(records ...*v3.EntitlementRecord) error {
	if u.done {
		return ErrPageUnitCommitted
	}
	for _, r := range records {
		if r == nil {
			continue
		}
		if u.entitlementIdx == nil {
			u.entitlementIdx = make(map[string]int)
		}
		u.entitlementIdx[r.GetExternalId()] = len(u.entitlements)
		u.entitlements = append(u.entitlements, r)
	}
	return nil
}

// StageGrants buffers grants for the page's commit.
func (u *PageUnit) StageGrants(records ...*v3.GrantRecord) error {
	if u.done {
		return ErrPageUnitCommitted
	}
	for _, r := range records {
		if r != nil {
			u.grants = append(u.grants, r)
		}
	}
	return nil
}

// GetResourceRecord is the page-scoped read: the page's own staged
// resource if it has one (latest staged wins, matching the commit's
// last-occurrence dedup), else the DB. Returns pebble.ErrNotFound as
// the engine's GetResourceRecord does.
func (u *PageUnit) GetResourceRecord(ctx context.Context, resourceTypeID, resourceID string) (*v3.ResourceRecord, error) {
	if u.done {
		return nil, ErrPageUnitCommitted
	}
	if i, ok := u.resourceIdx[resourceBufKey{resourceTypeID, resourceID}]; ok {
		return u.resources[i], nil
	}
	return u.e.GetResourceRecord(ctx, resourceTypeID, resourceID)
}

// StageGrantDeletes buffers grant removals by structural identity for
// the page's commit. A record whose refs derive no identity could not
// have been stored, so it is an error, never a fallback to string
// resolution (see DeleteGrantByRefs).
func (u *PageUnit) StageGrantDeletes(records ...*v3.GrantRecord) error {
	if u.done {
		return ErrPageUnitCommitted
	}
	for _, r := range records {
		if r == nil {
			continue
		}
		id, err := grantIdentityFromRecord(r)
		if err != nil {
			return fmt.Errorf("page grant delete: grant %q: %w", r.GetExternalId(), err)
		}
		u.grantDeletes = append(u.grantDeletes, id)
	}
	return nil
}

// DropStagedRows removes from the buffer the rows a source-cache
// tombstone in the SAME page names: the page's own put-then-delete.
// The store cannot see a buffered put, so the store-side tombstone
// (DeleteSourceCacheRows*, a registered pre-row bypass) only reaches
// rows earlier pages committed; this is the buffer half of B3's
// within-page order (upserts before deletes). canonicalIDs are the
// kind's public ids (grants/entitlements: external id; resources: the
// bid:r: resource BID); principalIDs match grants by principal id and
// resources by resource id, in scopeKey only — the same rules as the
// store's scoped delete. Returns the number of buffered rows dropped.
func (u *PageUnit) DropStagedRows(kind string, scopeKey string, canonicalIDs, principalIDs []string) (int, error) {
	if u.done {
		return 0, ErrPageUnitCommitted
	}
	canonical := make(map[string]struct{}, len(canonicalIDs))
	for _, id := range canonicalIDs {
		canonical[id] = struct{}{}
	}
	principals := make(map[string]struct{}, len(principalIDs))
	for _, id := range principalIDs {
		principals[id] = struct{}{}
	}
	dropped := 0
	switch kind {
	case "grants":
		kept := u.grants[:0]
		for _, g := range u.grants {
			_, byID := canonical[g.GetExternalId()]
			_, byPrincipal := principals[g.GetPrincipal().GetResourceId()]
			if byID || (byPrincipal && g.GetSourceScopeKey() == scopeKey) {
				dropped++
				continue
			}
			kept = append(kept, g)
		}
		u.grants = kept
	case "entitlements":
		kept := u.entitlements[:0]
		for _, r := range u.entitlements {
			if _, hit := canonical[r.GetExternalId()]; hit {
				dropped++
				continue
			}
			kept = append(kept, r)
		}
		u.entitlements = kept
		u.entitlementIdx = nil
		for i, r := range u.entitlements {
			if u.entitlementIdx == nil {
				u.entitlementIdx = make(map[string]int)
			}
			u.entitlementIdx[r.GetExternalId()] = i
		}
	case "resources":
		refs := make(map[resourceBufKey]struct{}, len(canonicalIDs))
		for _, id := range canonicalIDs {
			r, err := bid.ParseResourceBid(id)
			if err != nil {
				return 0, fmt.Errorf("page tombstone: invalid resource bid %q: %w", id, err)
			}
			refs[resourceBufKey{r.GetId().GetResourceType(), r.GetId().GetResource()}] = struct{}{}
		}
		kept := u.resources[:0]
		for _, r := range u.resources {
			_, byRef := refs[resourceBufKey{r.GetResourceTypeId(), r.GetResourceId()}]
			_, byID := principals[r.GetResourceId()]
			if byRef || (byID && r.GetSourceScopeKey() == scopeKey) {
				dropped++
				continue
			}
			kept = append(kept, r)
		}
		u.resources = kept
		u.resourceIdx = nil
		for i, r := range u.resources {
			if u.resourceIdx == nil {
				u.resourceIdx = make(map[resourceBufKey]int)
			}
			u.resourceIdx[resourceBufKey{r.GetResourceTypeId(), r.GetResourceId()}] = i
		}
	default:
		return 0, fmt.Errorf("page tombstone: unknown row kind %q", kind)
	}
	return dropped, nil
}

// GetEntitlementRecord is the page-scoped read for entitlements: the
// page's own staged record by external id if it has one, else the DB.
//
// Guarded on done for the same reason as GetResourceRecord: release
// clears the buffer, so a read of a staged id after Commit or Discard
// would index a nil slice.
func (u *PageUnit) GetEntitlementRecord(ctx context.Context, externalID string) (*v3.EntitlementRecord, error) {
	if u.done {
		return nil, ErrPageUnitCommitted
	}
	if i, ok := u.entitlementIdx[externalID]; ok {
		return u.entitlements[i], nil
	}
	return u.e.GetEntitlementRecord(ctx, externalID)
}

// Empty reports whether nothing has been staged. A page that wrote
// nothing still commits (its ledger row is the fact that it ran).
//
// Every staged thing counts, not just the record slices: a page that
// staged only grant deletes, only a fact, or only its counter bucket
// writes on commit and is not empty. A caller that skipped it on the
// strength of the four slices alone would silently drop the external
// resource phase's replaced originals.
func (u *PageUnit) Empty() bool {
	return len(u.resourceTypes) == 0 && len(u.resources) == 0 &&
		len(u.entitlements) == 0 && len(u.grants) == 0 &&
		len(u.grantDeletes) == 0 && len(u.facts) == 0 &&
		u.bucketValue == nil
}

// Commit applies the buffered records and the ledger row for id in
// one batch. row may be nil (a bare completion); its identity and
// record counts are set here from id and the buffer, and committed_at
// defaults to now. On success the unit is spent. On failure NOTHING
// landed (the batch is discarded) and the unit stays usable for a
// retry, which matches the brief's crash contract: a failed page is
// indistinguishable from a page that never ran.
func (u *PageUnit) Commit(ctx context.Context, id LedgerIdentity, row *v3.LedgerRow) error {
	if u.done {
		return ErrPageUnitCommitted
	}
	if row == nil {
		row = &v3.LedgerRow{}
	} else {
		row = cloneLedgerRow(row)
	}
	row.SetIdentity(ledgerIdentityToProto(id))
	if row.GetCommittedAt() == nil {
		row.SetCommittedAt(timestamppb.Now())
	}
	if !row.GetScrubbed() && len(row.GetNextPageTokenHash()) == 0 {
		row.SetNextPageTokenHash(ledgerTokenHash(row.GetNextPageToken()))
	}
	for _, c := range row.GetChildren() {
		if len(c.GetPageTokenHash()) == 0 {
			c.SetPageTokenHash(ledgerTokenHash(c.GetPageToken()))
		}
	}
	if len(u.grantDeletes) > 0 && len(u.grants) > 0 {
		// The delete wins over a buffered put of the same identity; the
		// put is dropped so its index entries are never staged.
		doomed := make(map[grantIdentity]struct{}, len(u.grantDeletes))
		for _, id := range u.grantDeletes {
			doomed[id] = struct{}{}
		}
		kept := u.grants[:0]
		for _, g := range u.grants {
			if id, err := grantIdentityFromRecord(g); err == nil {
				if _, hit := doomed[id]; hit {
					continue
				}
			}
			kept = append(kept, g)
		}
		u.grants = kept
	}
	key := encodeLedgerKey(id)

	e := u.e
	err := e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		// A page that outlived a completed EndSync/StartNewSync pair
		// would otherwise land the previous run's records in the
		// replacement sync, and its ledger row would enumerate a page the
		// new run never ran. Nothing later can tell them apart: the
		// keyspace holds one sync at a time and sync_id is not in the
		// keys. withWrite's sealed check rejects a commit arriving
		// between the seal and the next bind, but the rebound engine is
		// unsealed again and would accept it.
		//
		// The binding flips under currentSyncMu rather than writeMu, so
		// this is not atomic against a bind racing the next few
		// instructions; it closes the wide window (a page lives for
		// seconds to minutes), not that one.
		if now := e.CurrentSyncID(); u.syncID != "" && now != u.syncID {
			return fmt.Errorf("%w: begun under %s, now %s", ErrPageUnitForeignSync, u.syncID, now)
		}
		// The in-flight stamp precedes the first row (synced, its own
		// write): a token-only SDK must refuse this file from here until
		// seal. See keyspaceVersionLedgerInFlight.
		if err := e.markLedgerInFlight(); err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
		defer batch.Close()

		fresh := e.IsFreshSync()
		resourceTypes, err := stageResourceTypeRecords(batch, u.resourceTypes)
		if err != nil {
			return err
		}
		resources, err := e.stageResourceRecords(batch, u.resources)
		if err != nil {
			return err
		}
		entitlements, err := e.stageEntitlementRecords(batch, u.entitlements)
		if err != nil {
			return err
		}
		grants, err := e.stageGrantRecords(batch, u.grants)
		if err != nil {
			return err
		}
		for _, id := range u.grantDeletes {
			if _, err := e.stageGrantDeleteIfPresentLocked(batch, id); err != nil {
				return err
			}
		}
		// Every stager dedups by identity, so a page that staged one
		// identity twice has fewer keys in the keyspace than records in its
		// buffer. The counts are the only record of what the page put there.
		row.SetResourceTypesWritten(resourceTypes)
		row.SetResourcesWritten(resources)
		row.SetEntitlementsWritten(entitlements)
		row.SetGrantsWritten(grants)
		rowVal, err := marshalRecord(row)
		if err != nil {
			return err
		}
		if err := batch.StageLedgerRow(key, rowVal); err != nil {
			return err
		}
		for _, f := range u.facts {
			if err := batch.StageLedgerFactValue(encodeLedgerFactKey(f.name), f.value); err != nil {
				return err
			}
		}
		// Record the retain opt-out durably, in the batch that carries the
		// tokens it governs, so the process that seals honors it even if
		// it is not the process that declared it. Blind-set on every page:
		// a fact is a monotone last-writer-wins key, so re-staging costs
		// one key and self-heals a run whose declaring process died after
		// the first page.
		if e.retainLedgerTokens.Load() {
			if err := batch.StageLedgerFact(encodeLedgerFactKey(c1zstore.LedgerFactRetainTokens)); err != nil {
				return err
			}
		}
		if u.bucketKey != nil {
			bv, err := marshalRecord(u.bucketValue)
			if err != nil {
				return err
			}
			if err := batch.StageLedgerCounterBucket(u.bucketKey, bv); err != nil {
				return err
			}
		}
		opts := writeOpts(e.opts.durability)
		if fresh {
			// Pages commit NoSync as today; EndFreshSync's flush and
			// every pebble.Sync commit in the finalize sequence harden
			// them (see endSyncFinalize). A crash before that loses
			// whole pages, never parts of one.
			opts = pebble.NoSync
		}
		if err := batch.Commit(opts); err != nil {
			return err
		}
		if len(u.entitlements) > 0 {
			e.noteEntitlementKeyspaceWrite()
		}
		return nil
	})
	if err != nil {
		return err
	}
	u.release()
	return nil
}

// Discard drops the buffer without writing. The page is then, to the
// store, a page that never ran.
func (u *PageUnit) Discard() { u.release() }

func (u *PageUnit) release() {
	u.done = true
	u.resourceTypes, u.resources, u.entitlements, u.grants = nil, nil, nil, nil
	u.resourceIdx, u.entitlementIdx = nil, nil
	u.grantDeletes = nil
	u.facts = nil
	u.bucketKey, u.bucketValue = nil, nil
}
