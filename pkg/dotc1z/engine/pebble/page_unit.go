package pebble

import (
	"fmt"

	"context"
	"errors"

	"github.com/conductorone/baton-sdk/pkg/bid"

	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

var ErrPageUnitCommitted = errors.New("pebble page unit: already committed or discarded")

var ErrPageUnitForeignSync = errors.New("pebble page unit: sync changed since the page was begun")

type resourceBufKey struct{ rt, id string }

// Not safe for concurrent use.
type pageUnit struct {
	l      *Ledger
	syncID string

	resourceTypes  []*v3.ResourceTypeRecord
	resources      []*v3.ResourceRecord
	resourceIdx    map[resourceBufKey]int
	entitlements   []*v3.EntitlementRecord
	entitlementIdx map[string]int
	grants         []*v3.GrantRecord
	// Applied at Commit after the puts, in the same batch; a buffered put of the
	// same identity is dropped, as when the two are separate store calls.
	grantDeletes []grantIdentity

	// The bucket is the worker's whole counter total for this run, not a delta.
	facts       []ledgerFact
	bucketKey   []byte
	bucketValue *v3.LedgerCounterBucket

	done bool
}

type ledgerFact struct {
	name, value string
}

func (u *pageUnit) StageFactValue(name, value string) error {
	if u.done {
		return ErrPageUnitCommitted
	}
	u.facts = append(u.facts, ledgerFact{name: name, value: value})
	return nil
}

func (u *pageUnit) StageFact(name string) error {
	if u.done {
		return ErrPageUnitCommitted
	}
	u.facts = append(u.facts, ledgerFact{name: name})
	return nil
}

func (u *pageUnit) StageCounterBucket(runID string, worker uint32, bucket *v3.LedgerCounterBucket) error {
	if u.done {
		return ErrPageUnitCommitted
	}
	if bucket == nil {
		return errors.New("StageCounterBucket: nil bucket")
	}
	u.bucketKey = encodeLedgerCounterKey(runID, worker)
	u.bucketValue = bucket
	return nil
}

func (l *Ledger) newPageUnit() *pageUnit {
	return &pageUnit{l: l, syncID: l.e.CurrentSyncID()}
}

func (u *pageUnit) StageResourceTypes(records ...*v3.ResourceTypeRecord) error {
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

func (u *pageUnit) StageResources(records ...*v3.ResourceRecord) error {
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

func (u *pageUnit) StageEntitlements(records ...*v3.EntitlementRecord) error {
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

func (u *pageUnit) StageGrants(records ...*v3.GrantRecord) error {
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

// Latest staged wins, matching the commit's last-occurrence dedup.
func (u *pageUnit) resourceRecord(ctx context.Context, resourceTypeID, resourceID string) (*v3.ResourceRecord, error) {
	if u.done {
		return nil, ErrPageUnitCommitted
	}
	if i, ok := u.resourceIdx[resourceBufKey{resourceTypeID, resourceID}]; ok {
		return u.resources[i], nil
	}
	return u.l.e.GetResourceRecord(ctx, resourceTypeID, resourceID)
}

// A record whose refs derive no identity could not have been stored, so it
// is an error, never a fallback to string resolution.
func (u *pageUnit) StageGrantDeletes(records ...*v3.GrantRecord) error {
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

// The buffer half of a same-page tombstone: the store-side tombstone cannot
// see a buffered put.
func (u *pageUnit) DropStagedRows(kind string, scopeKey string, canonicalIDs, principalIDs []string) (int, error) {
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

// Guarded on done: release clears the buffer, so a read of a staged id
// after Commit or Discard would index a nil slice.
func (u *pageUnit) entitlementRecord(ctx context.Context, externalID string) (*v3.EntitlementRecord, error) {
	if u.done {
		return nil, ErrPageUnitCommitted
	}
	if i, ok := u.entitlementIdx[externalID]; ok {
		return u.entitlements[i], nil
	}
	return u.l.e.GetEntitlementRecord(ctx, externalID)
}

// On failure the unit stays usable for a retry.
func (u *pageUnit) Commit(ctx context.Context, id c1zstore.LedgerActionIdentity, row *v3.LedgerRow) error {
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

	l := u.l
	err := l.e.withWrite(func() error {
		if err := l.e.requireCurrentSync(); err != nil {
			return err
		}
		if err := u.requireSameSync(); err != nil {
			return err
		}
		if err := l.markInFlightLocked(); err != nil {
			return err
		}
		batch := l.e.db.NewRecordBatch()
		defer batch.Close()

		resourceTypes, err := stageResourceTypeRecords(batch, u.resourceTypes)
		if err != nil {
			return err
		}
		resources, err := l.e.stageResourceRecords(batch, u.resources)
		if err != nil {
			return err
		}
		entitlements, err := l.e.stageEntitlementRecords(batch, u.entitlements)
		if err != nil {
			return err
		}
		grants, err := l.e.stageGrantRecords(batch, u.grants)
		if err != nil {
			return err
		}
		for _, id := range u.grantDeletes {
			if _, err := l.e.stageGrantDeleteIfPresentLocked(batch, id); err != nil {
				return err
			}
		}
		// Stagers dedup by identity, so the buffer length overcounts; these counts
		// are the only record of what the page wrote.
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
		// Blind-set on every page: a fact is last-writer-wins, and re-staging
		// self-heals a run whose declaring process died after the first page.
		if l.retainTokens.Load() {
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
		if err := batch.Commit(recordWriteOpts); err != nil {
			return err
		}
		if len(u.entitlements) > 0 {
			l.e.noteEntitlementKeyspaceWrite()
		}
		return nil
	})
	if err != nil {
		return err
	}
	u.release()
	return nil
}

func (u *pageUnit) Discard() { u.release() }

func (u *pageUnit) release() {
	u.done = true
	u.resourceTypes, u.resources, u.entitlements, u.grants = nil, nil, nil, nil
	u.resourceIdx, u.entitlementIdx = nil, nil
	u.grantDeletes = nil
	u.facts = nil
	u.bucketKey, u.bucketValue = nil, nil
}

// sync_id is not in the keys, so a page begun under a previous sync would
// land its records in the replacement with nothing to tell them apart.
func (u *pageUnit) requireSameSync() error {
	if now := u.l.e.CurrentSyncID(); u.syncID != "" && now != u.syncID {
		return fmt.Errorf("%w: begun under %s, now %s", ErrPageUnitForeignSync, u.syncID, now)
	}
	return nil
}
