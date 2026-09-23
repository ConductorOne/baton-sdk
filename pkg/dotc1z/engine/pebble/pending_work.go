package pebble

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/cockroachdb/pebble/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

var ErrStaleLedgerWork = errors.New("pending ledger work changed or completed")

const maxPendingWorkRead = 100

func pendingWorkKey(id uint64) []byte {
	return binary.BigEndian.AppendUint64(rawdb.LedgerPendingPrefix(), id)
}

func encodeWorkHistoryKey(id c1zstore.LedgerActionIdentity, workID, revision uint64) []byte {
	key := encodeLedgerKey(id)
	key = binary.BigEndian.AppendUint64(key, workID)
	return binary.BigEndian.AppendUint64(key, revision)
}

func (l *Ledger) workState() (uint64, bool, error) {
	value, closer, err := l.e.db.Get(rawdb.LedgerWorkStateKey())
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer closer.Close()
	if len(value) != 9 || value[0] != 1 {
		return 0, false, errors.New("invalid pending-work state")
	}
	return binary.BigEndian.Uint64(value[1:]), true, nil
}

func stageWorkState(batch *rawdb.RecordBatch, id uint64) error {
	return batch.StageLedgerWorkState(binary.BigEndian.AppendUint64([]byte{1}, id))
}

func stagePendingWork(batch *rawdb.RecordBatch, work c1zstore.LedgerWork) error {
	id := work.Action.Identity
	for _, value := range []string{id.Op, id.ResourceTypeID, id.ResourceID, id.ParentResourceTypeID, id.ParentResourceID, id.PageToken, work.SchedulingKey} {
		if !utf8.ValidString(value) {
			return errors.New("pending work contains invalid UTF-8")
		}
	}

	data, err := json.Marshal(work)
	if err != nil {
		return err
	}
	return batch.StagePendingWork(pendingWorkKey(work.ID), data)
}

func (l *Ledger) InitializePendingWork(ctx context.Context, actions []c1zstore.LedgerWork, facts ...string) error {
	return l.e.withWrite(func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := l.e.requireCurrentSync(); err != nil {
			return err
		}
		record, err := l.e.GetSyncRunRecord(ctx, l.e.CurrentSyncID())
		if err != nil {
			return err
		}
		if record.GetSyncToken() != "" {
			return errors.New("checkpoint must be consumed through pending-work takeover")
		}
		_, initialized, err := l.workState()
		if err != nil {
			return err
		}
		if initialized {
			return nil
		}
		lo, hi := rawdb.LedgerRowBounds()
		it, err := l.e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
		if err != nil {
			return err
		}
		history := it.First()
		err = errors.Join(it.Error(), it.Close())
		if err != nil {
			return err
		}
		if history {
			return errors.New("cannot initialize pending work over completed history")
		}
		for _, action := range actions {
			if action.ID != 0 || action.Revision != 0 {
				return errors.New("initial work must not have assigned IDs or revisions")
			}
		}
		if err := l.markInFlightLocked(); err != nil {
			return err
		}
		batch := l.e.db.NewRecordBatch()
		defer batch.Close()
		if err := stageInitialWork(batch, l.e.CurrentSyncID(), actions); err != nil {
			return err
		}
		for _, fact := range facts {
			if err := batch.StageLedgerFact(encodeLedgerFactKey(fact)); err != nil {
				return err
			}
		}
		return batch.Commit(pebble.Sync)
	})
}

func (l *Ledger) PendingWork(ctx context.Context, beforeID uint64, limit int) ([]c1zstore.LedgerWork, bool, error) {
	return l.readPendingWork(ctx, beforeID, 0, limit, false)
}

func (l *Ledger) PendingWorkAfter(ctx context.Context, afterID uint64, limit int) ([]c1zstore.LedgerWork, bool, error) {
	return l.readPendingWork(ctx, 0, afterID, limit, true)
}

func (l *Ledger) readPendingWork(ctx context.Context, beforeID, afterID uint64, limit int, ascending bool) ([]c1zstore.LedgerWork, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if limit < 1 || limit > maxPendingWorkRead {
		return nil, false, errors.New("pending work read limit must be between 1 and 100")
	}
	_, initialized, err := l.workState()
	if err != nil || !initialized {
		return nil, initialized, err
	}
	lo, hi := rawdb.LedgerPendingBounds()
	if beforeID != 0 {
		hi = pendingWorkKey(beforeID)
	}
	if ascending && afterID == math.MaxUint64 {
		return nil, true, nil
	}
	if ascending {
		lo = pendingWorkKey(afterID + 1)
	}
	it, err := l.e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, true, err
	}
	defer it.Close()
	result := make([]c1zstore.LedgerWork, 0, limit)
	first, step := it.Last, it.Prev
	if ascending {
		first, step = it.First, it.Next
	}
	for valid := first(); valid && len(result) < limit; valid = step() {
		if err := ctx.Err(); err != nil {
			return nil, true, err
		}
		var work c1zstore.LedgerWork
		if !utf8.Valid(it.Value()) {
			return nil, true, errors.New("pending work contains invalid UTF-8")
		}
		if err := json.Unmarshal(it.Value(), &work); err != nil {
			return nil, true, err
		}
		if len(it.Key()) != len(rawdb.LedgerPendingPrefix())+8 || work.ID == 0 || binary.BigEndian.Uint64(it.Key()[len(rawdb.LedgerPendingPrefix()):]) != work.ID {
			return nil, true, errors.New("invalid pending-work identity")
		}
		result = append(result, work)
	}
	return result, true, it.Error()
}

func (l *Ledger) stageWorkTransition(ctx context.Context, batch *rawdb.RecordBatch, expected c1zstore.LedgerWork, id c1zstore.LedgerActionIdentity, row *v3.LedgerRow, childKeys []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	value, closer, err := l.e.db.Get(pendingWorkKey(expected.ID))
	if errors.Is(err, pebble.ErrNotFound) {
		return ErrStaleLedgerWork
	}
	if err != nil {
		return err
	}
	var current c1zstore.LedgerWork
	err = json.Unmarshal(value, &current)
	closeErr := closer.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if current.SyncID != l.e.CurrentSyncID() || current.SyncID != expected.SyncID || current.ID != expected.ID || current.Revision != expected.Revision || current.Action.Identity != id {
		return ErrStaleLedgerWork
	}
	if len(childKeys) != 0 && len(childKeys) != len(row.GetChildren()) {
		return errors.New("child scheduling keys do not match transition")
	}
	if len(childKeys) > 0 {
		seen := make(map[string]bool)
		children := make([]*v3.LedgerChild, 0, len(row.GetChildren()))
		keptKeys := make([]string, 0, len(childKeys))
		for i, child := range row.GetChildren() {
			key := childKeys[i]
			if key != "" {
				if seen[key] {
					continue
				}
				scheduled, err := l.HasScheduledWork(ctx, key)
				if err != nil {
					return err
				}
				if scheduled {
					continue
				}
				seen[key] = true
				if err := batch.StageLedgerScheduling(schedulingWorkKey(key)); err != nil {
					return err
				}
			}
			children = append(children, child)
			keptKeys = append(keptKeys, key)
		}
		row.SetChildren(children)
		childKeys = keptKeys
	}
	last, initialized, err := l.workState()
	if err != nil {
		return err
	}
	if !initialized {
		return errors.New("pending work has no allocator state")
	}
	if uint64(len(row.GetChildren())) > math.MaxUint64-last {
		return errors.New("pending work ID overflow")
	}
	if row.GetNextPageToken() == "" {
		if err := batch.StagePendingWorkDelete(pendingWorkKey(current.ID)); err != nil {
			return err
		}
	} else {
		if current.Revision == math.MaxUint64 {
			return errors.New("pending work revision overflow")
		}
		current.Revision++
		current.Action.Identity.PageToken = row.GetNextPageToken()
		current.TypeScopedPlanned = row.GetTypeScopedPlanned()
		if err := stagePendingWork(batch, current); err != nil {
			return err
		}
	}
	for index, child := range row.GetChildren() {
		if child == nil || child.GetIdentity() == nil {
			return fmt.Errorf("invalid child of work %d", current.ID)
		}
		last++
		work := c1zstore.LedgerWork{ID: last, SyncID: current.SyncID, Action: c1zstore.LedgerChild{Identity: ledgerIdentityFromProto(child.GetIdentity()), Spawned: child.GetSpawned()}}
		if len(childKeys) > 0 {
			work.SchedulingKey = childKeys[index]
		}
		if err := stagePendingWork(batch, work); err != nil {
			return err
		}
		child.SetWorkId(last)
		child.GetIdentity().SetPageToken("")
	}
	if len(row.GetChildren()) > 0 {
		return stageWorkState(batch, last)
	}
	return nil
}

func stageInitialWork(batch *rawdb.RecordBatch, syncID string, actions []c1zstore.LedgerWork) error {
	for i, action := range actions {
		if action.ID != 0 || action.Revision != 0 {
			return errors.New("initial work must not have assigned IDs or revisions")
		}
		action.ID = uint64(i) + 1
		action.SyncID = syncID
		if action.SchedulingKey != "" {
			if err := batch.StageLedgerScheduling(schedulingWorkKey(action.SchedulingKey)); err != nil {
				return err
			}
		}
		if err := stagePendingWork(batch, action); err != nil {
			return err
		}
	}
	return stageWorkState(batch, uint64(len(actions)))
}

type pendingWorkSeed struct {
	token string
	work  []c1zstore.LedgerWork
}

func (l *Ledger) TakeoverPendingWork(ctx context.Context, runID, expectedToken string, facts []string, counters c1zstore.LedgerCounters, work []c1zstore.LedgerWork) (string, error) {
	if expectedToken == "" {
		return "", errors.New("pending-work takeover requires a decoded checkpoint")
	}
	return l.takeover(ctx, runID, facts, counters, &pendingWorkSeed{token: expectedToken, work: work})
}

func (l *Ledger) CompletePendingWork(ctx context.Context, work c1zstore.LedgerWork, runID string, counters c1zstore.LedgerCounters) error {
	if runID == "" {
		return errors.New("local work completion requires an attempt ID")
	}
	return l.e.withWrite(func() error {
		if err := l.e.requireCurrentSync(); err != nil {
			return err
		}
		batch := l.e.db.NewRecordBatch()
		defer batch.Close()
		if err := l.stageWorkTransition(ctx, batch, work, work.Action.Identity, v3.LedgerRow_builder{}.Build(), nil); err != nil {
			return err
		}
		value, err := marshalRecord(ledgerCountersToProto(counters))
		if err != nil {
			return err
		}
		if err := batch.StageLedgerCounterBucket(encodeLedgerCounterKey(runID, c1zstore.RunBucketWorker), value); err != nil {
			return err
		}
		return batch.Commit(recordWriteOpts)
	})
}

func schedulingWorkKey(key string) []byte {
	return append(rawdb.LedgerSchedulingPrefix(), []byte(key)...)
}

func (l *Ledger) HasScheduledWork(ctx context.Context, key string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	_, closer, err := l.e.db.Get(schedulingWorkKey(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, closer.Close()
}
