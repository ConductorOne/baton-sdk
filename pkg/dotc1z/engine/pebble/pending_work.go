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

// A page commit or work transition in a phase that accepts none.
var ErrLedgerQueuePhase = errors.New("pending-work declaration accepts no pages in this phase")

const workStateVersion = 2

// Value: version, last allocated ID, phase. Version 1 had no phase byte and
// no released file carries it.
func (l *Ledger) workState() (uint64, c1zstore.LedgerQueuePhase, error) {
	value, closer, err := l.e.db.Get(rawdb.LedgerWorkStateKey())
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, c1zstore.LedgerQueueAbsent, nil
	}
	if err != nil {
		return 0, c1zstore.LedgerQueueAbsent, err
	}
	defer closer.Close()
	if len(value) != 10 || value[0] != workStateVersion {
		return 0, c1zstore.LedgerQueueAbsent, errors.New("invalid pending-work state")
	}
	phase := c1zstore.LedgerQueuePhase(value[9])
	switch phase {
	case c1zstore.LedgerQueueCollecting, c1zstore.LedgerQueueExpanding, c1zstore.LedgerQueueSealing:
	default:
		return 0, c1zstore.LedgerQueueAbsent, errors.New("invalid pending-work phase")
	}
	return binary.BigEndian.Uint64(value[1:9]), phase, nil
}

func stageWorkState(batch *rawdb.RecordBatch, id uint64, phase c1zstore.LedgerQueuePhase) error {
	value := binary.BigEndian.AppendUint64([]byte{workStateVersion}, id)
	return batch.StageLedgerWorkState(append(value, byte(phase)))
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

func (l *Ledger) BeginCollecting(ctx context.Context, actions []c1zstore.LedgerWork, facts map[string]string) error {
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
		if record.GetEndedAt() != nil {
			return errors.New("a finished sync begins its next pass through BeginPass")
		}
		_, phase, err := l.workState()
		if err != nil {
			return err
		}
		if phase != c1zstore.LedgerQueueAbsent {
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
		batch := l.e.db.NewRecordBatch()
		defer batch.Close()
		if err := l.stageMarkInFlight(batch); err != nil {
			return err
		}
		if err := stageInitialWork(batch, l.e.CurrentSyncID(), actions, c1zstore.LedgerQueueCollecting); err != nil {
			return err
		}
		for name, value := range facts {
			if err := batch.StageLedgerFactValue(encodeLedgerFactKey(name), value); err != nil {
				return err
			}
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
		l.inFlight.Store(true)
		return nil
	})
}

// BeginExpanding moves a Collecting declaration to Expanding: collection is
// done, and the only pending entry is the expansion action. No page commits
// after this; the expansion entry completes through CompletePendingWork.
func (l *Ledger) BeginExpanding(ctx context.Context) error {
	return l.e.withWrite(func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := l.e.requireCurrentSync(); err != nil {
			return err
		}
		last, phase, err := l.workState()
		if err != nil {
			return err
		}
		if phase != c1zstore.LedgerQueueCollecting {
			return fmt.Errorf("BeginExpanding: pending-work declaration is %s", phase)
		}
		pending, _, err := l.readPendingWork(ctx, 0, 0, 2, false)
		if err != nil {
			return err
		}
		if len(pending) != 1 || pending[0].Action.Identity.Op != ledgerExpansionOp {
			return errors.New("BeginExpanding: the pending range must hold exactly the expansion entry")
		}
		batch := l.e.db.NewRecordBatch()
		defer batch.Close()
		if err := l.stageMarkInFlight(batch); err != nil {
			return err
		}
		if err := stageWorkState(batch, last, c1zstore.LedgerQueueExpanding); err != nil {
			return err
		}
		if hook := l.e.test.ledgerBeginExpandingHook; hook != nil {
			if err := hook(); err != nil {
				return err
			}
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
		l.inFlight.Store(true)
		return nil
	})
}

// The syncer's op string for grant expansion (pkg/sync SyncGrantExpansionOp).
const ledgerExpansionOp = "grant-expansion"

func (l *Ledger) PendingWork(ctx context.Context, beforeID uint64, limit int) ([]c1zstore.LedgerWork, c1zstore.LedgerQueuePhase, error) {
	return l.readPendingWork(ctx, beforeID, 0, limit, false)
}

func (l *Ledger) PendingWorkAfter(ctx context.Context, afterID uint64, limit int) ([]c1zstore.LedgerWork, c1zstore.LedgerQueuePhase, error) {
	return l.readPendingWork(ctx, 0, afterID, limit, true)
}

func (l *Ledger) readPendingWork(ctx context.Context, beforeID, afterID uint64, limit int, ascending bool) ([]c1zstore.LedgerWork, c1zstore.LedgerQueuePhase, error) {
	absent := c1zstore.LedgerQueueAbsent
	if err := ctx.Err(); err != nil {
		return nil, absent, err
	}
	if limit < 1 || limit > maxPendingWorkRead {
		return nil, absent, errors.New("pending work read limit must be between 1 and 100")
	}
	_, phase, err := l.workState()
	if err != nil || phase == absent {
		return nil, phase, err
	}
	lo, hi := rawdb.LedgerPendingBounds()
	if beforeID != 0 {
		hi = pendingWorkKey(beforeID)
	}
	if ascending && afterID == math.MaxUint64 {
		return nil, phase, nil
	}
	if ascending {
		lo = pendingWorkKey(afterID + 1)
	}
	it, err := l.e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, phase, err
	}
	defer it.Close()
	result := make([]c1zstore.LedgerWork, 0, limit)
	first, step := it.Last, it.Prev
	if ascending {
		first, step = it.First, it.Next
	}
	for valid := first(); valid && len(result) < limit; valid = step() {
		if err := ctx.Err(); err != nil {
			return nil, phase, err
		}
		var work c1zstore.LedgerWork
		if !utf8.Valid(it.Value()) {
			return nil, phase, errors.New("pending work contains invalid UTF-8")
		}
		if err := json.Unmarshal(it.Value(), &work); err != nil {
			return nil, phase, err
		}
		if len(it.Key()) != len(rawdb.LedgerPendingPrefix())+8 || work.ID == 0 || binary.BigEndian.Uint64(it.Key()[len(rawdb.LedgerPendingPrefix()):]) != work.ID {
			return nil, phase, errors.New("invalid pending-work identity")
		}
		result = append(result, work)
	}
	return result, phase, it.Error()
}

// Under the write lock. Only pebble reads.
func (l *Ledger) hasPendingWorkLocked() (bool, error) {
	lo, hi := rawdb.LedgerPendingBounds()
	it, err := l.e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return false, err
	}
	found := it.First()
	return found, errors.Join(it.Error(), it.Close())
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
	last, phase, err := l.workState()
	if err != nil {
		return err
	}
	switch phase {
	case c1zstore.LedgerQueueAbsent:
		return errors.New("pending work has no allocator state")
	case c1zstore.LedgerQueueSealing:
		return fmt.Errorf("%w: %s", ErrLedgerQueuePhase, phase)
	case c1zstore.LedgerQueueExpanding:
		// Only the expansion entry may complete; it carries no continuation
		// or children, and pageUnit.Commit refused any page already.
		if row.GetNextPageToken() != "" || len(row.GetChildren()) != 0 {
			return fmt.Errorf("%w: %s", ErrLedgerQueuePhase, phase)
		}
	case c1zstore.LedgerQueueCollecting:
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
		return stageWorkState(batch, last, c1zstore.LedgerQueueCollecting)
	}
	return nil
}

func stageInitialWork(batch *rawdb.RecordBatch, syncID string, actions []c1zstore.LedgerWork, phase c1zstore.LedgerQueuePhase) error {
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
	return stageWorkState(batch, uint64(len(actions)), phase)
}

type pendingWorkSeed struct {
	token string
	work  []c1zstore.LedgerWork
	phase c1zstore.LedgerQueuePhase
}

func (l *Ledger) BeginFromToken(
	ctx context.Context, runID, expectedToken string, facts map[string]string, counters c1zstore.LedgerCounters, work []c1zstore.LedgerWork, phase c1zstore.LedgerQueuePhase,
) (string, error) {
	if expectedToken == "" {
		return "", errors.New("pending-work takeover requires a decoded checkpoint")
	}
	switch phase {
	case c1zstore.LedgerQueueCollecting:
	case c1zstore.LedgerQueueExpanding:
		if len(work) != 1 || work[0].Action.Identity.Op != ledgerExpansionOp {
			return "", errors.New("BeginFromToken: expanding requires exactly the expansion entry")
		}
	case c1zstore.LedgerQueueAbsent, c1zstore.LedgerQueueSealing:
		return "", fmt.Errorf("BeginFromToken: cannot seed phase %s", phase)
	}
	return l.takeover(ctx, runID, facts, counters, &pendingWorkSeed{token: expectedToken, work: work, phase: phase})
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
