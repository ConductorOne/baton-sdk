package pebble

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

type Ledger struct {
	e            *Engine
	retainTokens atomic.Bool
	mismatches   atomic.Uint64
	// inFlight mirrors the keyspaceVersionLedgerInFlight stamp.
	inFlight atomic.Bool
}

func (e *Engine) Ledger() *Ledger { return &e.ledger }

// 128 bits: a collision costs a re-run (the identity compare fails), never a skip.
const ledgerTokenHashLen = 16

func ledgerTokenHash(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:ledgerTokenHashLen]
}

func encodeLedgerKey(id c1zstore.LedgerActionIdentity) []byte {
	return encodeLedgerKeyWithHash(id, ledgerTokenHash(id.PageToken))
}

func encodeLedgerKeyWithHash(id c1zstore.LedgerActionIdentity, hash []byte) []byte {
	buf := make([]byte, 0, 64+len(id.Op)+len(id.ResourceTypeID)+len(id.ResourceID)+
		len(id.ParentResourceTypeID)+len(id.ParentResourceID))
	buf = append(buf, rawdb.LedgerKeyPrefix()...)
	buf = codec.AppendTupleStrings(buf, id.Op, id.ResourceTypeID, id.ResourceID,
		id.ParentResourceTypeID, id.ParentResourceID)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleBool(buf, id.TypeScoped)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleBytes(buf, hash)
}

func ledgerLowerBound() []byte { lo, _ := rawdb.LedgerBounds(); return lo }
func ledgerUpperBound() []byte { _, hi := rawdb.LedgerBounds(); return hi }

func ledgerIdentityToProto(id c1zstore.LedgerActionIdentity) *v3.LedgerActionIdentity {
	return v3.LedgerActionIdentity_builder{
		Op:                   id.Op,
		ResourceTypeId:       id.ResourceTypeID,
		ResourceId:           id.ResourceID,
		ParentResourceTypeId: id.ParentResourceTypeID,
		ParentResourceId:     id.ParentResourceID,
		PageToken:            id.PageToken,
		PageTokenHash:        ledgerTokenHash(id.PageToken),
		TypeScoped:           id.TypeScoped,
	}.Build()
}

func ledgerIdentityFromProto(p *v3.LedgerActionIdentity) c1zstore.LedgerActionIdentity {
	return c1zstore.LedgerActionIdentity{
		Op:                   p.GetOp(),
		ResourceTypeID:       p.GetResourceTypeId(),
		ResourceID:           p.GetResourceId(),
		ParentResourceTypeID: p.GetParentResourceTypeId(),
		ParentResourceID:     p.GetParentResourceId(),
		PageToken:            p.GetPageToken(),
		TypeScoped:           p.GetTypeScoped(),
	}
}

// A row failing this compare is treated as absent: a key collision costs a
// re-run, never a skip. After a scrub the token compares by hash.
func ledgerIdentityMatches(want c1zstore.LedgerActionIdentity, got *v3.LedgerActionIdentity, scrubbed bool) bool {
	if got.GetOp() != want.Op ||
		got.GetResourceTypeId() != want.ResourceTypeID ||
		got.GetResourceId() != want.ResourceID ||
		got.GetParentResourceTypeId() != want.ParentResourceTypeID ||
		got.GetParentResourceId() != want.ParentResourceID ||
		got.GetTypeScoped() != want.TypeScoped {
		return false
	}
	if !scrubbed {
		return got.GetPageToken() == want.PageToken
	}
	return bytes.Equal(got.GetPageTokenHash(), ledgerTokenHash(want.PageToken))
}

func encodeLedgerFactKey(name string) []byte {
	buf := append([]byte{}, rawdb.LedgerFactPrefix()...)
	return codec.AppendTupleStrings(buf, name)
}

func encodeLedgerCounterKey(runID string, worker uint32) []byte {
	buf := append([]byte{}, rawdb.LedgerCounterPrefix()...)
	buf = codec.AppendTupleStrings(buf, runID)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleUint32(buf, worker)
}

func (l *Ledger) Facts(ctx context.Context) (map[string]string, error) {
	lo, hi := rawdb.LedgerFactBounds()
	iter, err := l.e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	facts := map[string]string{}
	for iter.First(); iter.Valid(); iter.Next() {
		tail := iter.Key()[len(lo):]
		name, _, err := codec.DecodeTupleStringTo(nil, tail, 0)
		if err != nil {
			return nil, fmt.Errorf("ledger facts: decode %x: %w", iter.Key(), err)
		}
		facts[string(name)] = rawdb.DecodeLedgerFactValue(iter.Value())
	}
	return facts, iter.Error()
}

// SetRetainTokens keeps verbatim page tokens in the sealed artifact. The
// default scrubs them: a page token can carry a credential.
func (l *Ledger) SetRetainTokens(retain bool) {
	l.retainTokens.Store(retain)
}

// Retention declared in memory or by the durable fact; the fact is what
// survives a crash into the sealing process. An unreadable fact fails the
// seal rather than scrubbing tokens a retain-tokens sync asked to keep.
func (l *Ledger) sealScrubsTokens() (bool, error) {
	if l.retainTokens.Load() {
		return false, nil
	}
	_, closer, err := l.e.db.Get(encodeLedgerFactKey(c1zstore.LedgerFactRetainTokens))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return true, nil
		}
		return true, err
	}
	defer closer.Close()
	return false, nil
}

func (l *Ledger) sealDiscardsRows() (bool, error) {
	_, closer, err := l.e.db.Get(encodeLedgerFactKey(c1zstore.LedgerFactDiscardOnSeal))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, closer.Close()
}

func scrubLedgerRow(row *v3.LedgerRow) bool {
	if row.GetScrubbed() {
		return false
	}
	if id := row.GetIdentity(); id != nil {
		if len(id.GetPageTokenHash()) == 0 {
			id.SetPageTokenHash(ledgerTokenHash(id.GetPageToken()))
		}
		id.SetPageToken("")
	}
	if len(row.GetNextPageTokenHash()) == 0 {
		row.SetNextPageTokenHash(ledgerTokenHash(row.GetNextPageToken()))
	}
	row.SetNextPageToken("")
	for _, c := range row.GetChildren() {
		id := c.GetIdentity()
		if len(id.GetPageTokenHash()) == 0 {
			id.SetPageTokenHash(ledgerTokenHash(id.GetPageToken()))
		}
		id.SetPageToken("")
	}
	row.SetScrubbed(true)
	return true
}

const ledgerScrubBatchBytes = 16 << 20

// Idempotent: scrubbed rows are skipped, so a crash mid-scrub and a re-run
// EndSync finish the job.
func (l *Ledger) scrubTokens(ctx context.Context) error {
	return l.e.withWriteAllowSealed(func() error {
		lo, hi := rawdb.LedgerRowBounds()
		iter, err := l.e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
		if err != nil {
			return err
		}
		defer iter.Close()

		batch := l.e.db.NewRecordBatch()
		defer func() { _ = batch.Close() }()
		flush := func() error {
			if batch.Empty() {
				return nil
			}
			if err := batch.Commit(writeOpts(l.e.opts.durability)); err != nil {
				return err
			}
			if err := batch.Close(); err != nil {
				return err
			}
			batch = l.e.db.NewRecordBatch()
			return nil
		}

		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			row := &v3.LedgerRow{}
			if err := unmarshalRecord(iter.Value(), row); err != nil {
				return fmt.Errorf("scrubTokens: unmarshal: %w", err)
			}
			if !scrubLedgerRow(row) {
				continue
			}
			val, err := marshalRecord(row)
			if err != nil {
				return err
			}
			if err := batch.StageLedgerRow(bytes.Clone(iter.Key()), val); err != nil {
				return err
			}
			if batch.Len() >= ledgerScrubBatchBytes {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		if err := iter.Error(); err != nil {
			return err
		}
		if err := l.scrubFrontierLocked(ctx, batch); err != nil {
			return err
		}
		return flush()
	})
}

// The frontier is not a ledger row and holds the taken-over token JSON
// verbatim, every action of it carrying a page_token; the row scrub does
// not reach it.
func (l *Ledger) scrubFrontierLocked(ctx context.Context, batch *rawdb.RecordBatch) error {
	f, found, err := l.Frontier(ctx)
	if err != nil {
		return fmt.Errorf("scrubTokens: read frontier: %w", err)
	}
	if !found || f.State == "" {
		return nil
	}
	blanked := v3.LedgerFrontier_builder{Attempt: f.Attempt}.Build()
	if !f.TakenOverAt.IsZero() {
		blanked.SetTakenOverAt(timestamppb.New(f.TakenOverAt))
	}
	val, err := marshalRecord(blanked)
	if err != nil {
		return err
	}
	return batch.StageLedgerFrontier(val)
}

// The scrub writes new versions; the pre-scrub rows stay in the SSTs they
// were flushed to and CheckpointTo hard-links SSTs as they are, so without
// this compaction the verbatim tokens ship as byte-level residue.
// SyncRunKey's range is included: CheckpointSync before a takeover left
// superseded sync-run versions carrying tokens, in SSTs that need not
// overlap the ledger range.
func (l *Ledger) purgeResidue(ctx context.Context) error {
	return l.e.withWriteAllowSealed(func() error {
		return l.compactForResidueLocked(ctx, ledgerResidueSpans())
	})
}

func ledgerResidueSpans() []pebble.KeyRange {
	lo, hi := rawdb.LedgerBounds()
	runKey := rawdb.SyncRunKey()
	return []pebble.KeyRange{
		{Start: lo, End: hi},
		{Start: runKey, End: upperBoundOf(runKey)},
	}
}

// Runs regardless of the retain fact: the residue belongs to a ledger that
// was dropped, and compaction never removes live rows.
func (l *Ledger) purgeMarkedResidue(ctx context.Context) error {
	armed, err := l.residuePending()
	if err != nil {
		return fmt.Errorf("purgeMarkedResidue: read marker: %w", err)
	}
	if !armed {
		return nil
	}
	// One critical section, so the marker cannot be consumed for a compaction a
	// concurrent Close cut off.
	return l.e.withWriteAllowSealed(func() error {
		if err := l.compactForResidueLocked(ctx, ledgerResidueSpans()); err != nil {
			return err
		}
		if err := l.e.db.MetaDelete(encodeLedgerResiduePendingKey(), pebble.Sync); err != nil {
			return fmt.Errorf("purgeMarkedResidue: consume marker: %w", err)
		}
		return nil
	})
}

// Marks that ledger bytes remain in SSTs with no ledger row left to infer it
// from. Drop arms it before deleting; purgeMarkedResidue consumes it only
// after its compaction succeeds, so an interrupted purge is retried by the
// next seal.
func encodeLedgerResiduePendingKey() []byte {
	buf := make([]byte, 0, 2+len("ledger_residue_pending"))
	buf = append(buf, versionV3, typeEngineMeta)
	return codec.AppendTupleStrings(buf, "ledger_residue_pending")
}

func (l *Ledger) markResiduePending() error {
	return l.e.withWriteAllowSealed(func() error {
		if err := l.e.db.MetaSet(encodeLedgerResiduePendingKey(), []byte{1}, pebble.Sync); err != nil {
			return fmt.Errorf("arm ledger-residue marker: %w", err)
		}
		return nil
	})
}

func (l *Ledger) residuePending() (bool, error) {
	_, closer, err := l.e.db.Get(encodeLedgerResiduePendingKey())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	closer.Close()
	return true, nil
}

// Under writeMu so Close waits: pebble.DB.Compact panics on a closed DB.
func (l *Ledger) compactForResidueLocked(ctx context.Context, spans []pebble.KeyRange) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// seal() leaves the scheduler paused and a manual compaction needs a grant.
	// Re-pause only what was un-paused, or an unsealed engine hangs at
	// L0StopWritesThreshold.
	if l.e.compactionScheduler != nil && l.e.compactionScheduler.paused.Load() {
		l.e.resumeCompactions()
		defer l.e.pauseCompactions()
	}
	l.e.test.ledgerResiduePurges.Add(1)
	for _, span := range spans {
		if err := l.e.db.Compact(ctx, span.Start, span.End, true); err != nil {
			return fmt.Errorf("purgeResidue: [%x, %x): %w", span.Start, span.End, err)
		}
	}
	return nil
}

// Drop removes the ledger and keeps the sync (compaction outputs, the
// sanitizer, the syncer's rebind of a finished sync).
func (l *Ledger) Drop(ctx context.Context) error {
	ledgeredBeforeDrop, err := l.active()
	if err != nil {
		return fmt.Errorf("Drop: check ledger presence: %w", err)
	}
	if ledgeredBeforeDrop {
		if err := l.markResiduePending(); err != nil {
			return fmt.Errorf("Drop: %w", err)
		}
	}
	if err := l.e.withWriteAllowSealed(func() error {
		lo, hi := rawdb.LedgerBounds()
		if err := l.e.db.DropKeyRange(lo, hi, writeOpts(l.e.opts.durability)); err != nil {
			return err
		}
		// After the drop: if the clear fails the rows are gone and the file still
		// refuses a token, the safe way round.
		return l.clearInFlightLocked()
	}); err != nil {
		return err
	}
	// Inline so a file dropped and shipped without a seal is clean; on failure
	// the marker stays armed for the next seal.
	return l.purgeMarkedResidue(ctx)
}

func cloneLedgerRow(row *v3.LedgerRow) *v3.LedgerRow {
	return proto.Clone(row).(*v3.LedgerRow)
}

// Stamped before the unit's batch, as its own synced write, so a token-only
// SDK refuses the file in every image that holds a row.
func (l *Ledger) markInFlightLocked() error {
	if l.inFlight.Load() {
		return nil
	}
	if err := l.e.stampKeyspaceVersionValueLocked(keyspaceVersionLedgerInFlight); err != nil {
		return fmt.Errorf("pebble: stamp ledger in-flight: %w", err)
	}
	l.inFlight.Store(true)
	return nil
}

func (l *Ledger) clearInFlightLocked() error {
	if !l.inFlight.Load() {
		return nil
	}
	if err := l.e.stampKeyspaceVersionValueLocked(keyspaceVersion); err != nil {
		return fmt.Errorf("pebble: clear ledger in-flight stamp: %w", err)
	}
	l.inFlight.Store(false)
	return nil
}

// The stamp alone is not enough: clearInFlightLocked runs before the
// ended_at stamp, so a failed or crashed finalize leaves rows with the flag
// false. Rows outlive the stamp, so rows are what the gate asks about.
func (l *Ledger) active() (bool, error) {
	if l.inFlight.Load() {
		return true, nil
	}
	lo, hi := rawdb.LedgerBounds()
	iter, err := l.e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return false, err
	}
	defer iter.Close()
	return iter.First(), iter.Error()
}

// A row at id's key echoing another identity reads as absent, and is counted.
func (l *Ledger) GetRow(ctx context.Context, id c1zstore.LedgerActionIdentity) (*c1zstore.LedgerRow, bool, error) {
	key := encodeLedgerKey(id)
	val, closer, err := l.e.db.Get(key)
	if err != nil {
		if !errors.Is(err, pebble.ErrNotFound) {
			return nil, false, err
		}
		iter, iterErr := l.e.db.NewIter(&pebble.IterOptions{LowerBound: key, UpperBound: rawdb.UpperBound(key)})
		if iterErr != nil {
			return nil, false, iterErr
		}
		if !iter.First() {
			return nil, false, errors.Join(iter.Error(), iter.Close())
		}
		if len(iter.Key()) != len(key) && len(iter.Key()) != len(key)+16 {
			return nil, false, iter.Close()
		}
		val, closer = iter.Value(), iter
	}
	defer closer.Close()
	row := &v3.LedgerRow{}
	if err := unmarshalRecord(val, row); err != nil {
		return nil, false, fmt.Errorf("GetRow: unmarshal: %w", err)
	}
	if !ledgerIdentityMatches(id, row.GetIdentity(), row.GetScrubbed()) {
		l.mismatches.Add(1)
		ctxzap.Extract(ctx).Warn("pebble ledger: identity mismatch at requested key",
			zap.String("op", id.Op),
			zap.String("resource_type_id", id.ResourceTypeID),
			zap.String("resource_id", id.ResourceID),
			zap.String("row_op", row.GetIdentity().GetOp()),
			zap.String("row_resource_type_id", row.GetIdentity().GetResourceTypeId()),
			zap.String("row_resource_id", row.GetIdentity().GetResourceId()),
		)
		return nil, false, nil
	}
	return ledgerRowFromProto(row), true, nil
}

func (l *Ledger) Counters(ctx context.Context) (c1zstore.LedgerCounters, error) {
	counters, _, err := l.countersExcept(ctx, nil, nil)
	return counters, err
}

func (l *Ledger) countersExcept(ctx context.Context, currentPrefix []byte, deleteBucket func([]byte) error) (c1zstore.LedgerCounters, bool, error) {
	lo, hi := rawdb.LedgerCounterBounds()
	iter, err := l.e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return c1zstore.LedgerCounters{}, false, err
	}
	defer iter.Close()
	needsFold := false
	foldedKey := foldedLedgerCounterKey()
	sum := map[string]uint64{}
	var flags uint64
	calls := map[string]*v3.CallStat{}
	sessions := map[string]*v3.CallStat{}
	durations := map[string]int64{}
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return c1zstore.LedgerCounters{}, false, err
		}
		if len(currentPrefix) > 0 && bytes.HasPrefix(iter.Key(), currentPrefix) {
			continue
		}
		needsFold = needsFold || !bytes.Equal(iter.Key(), foldedKey)
		b := &v3.LedgerCounterBucket{}
		if err := unmarshalRecord(iter.Value(), b); err != nil {
			return c1zstore.LedgerCounters{}, false, fmt.Errorf("ledger counters: unmarshal %x: %w", iter.Key(), err)
		}
		for k, v := range b.GetCounters() {
			sum[k] += v
		}
		flags |= b.GetFlags()
		calls = c1zstore.FoldCallStats(calls, b.GetConnectorCalls())
		sessions = c1zstore.FoldCallStats(sessions, b.GetSessionCalls())
		durations = c1zstore.FoldDurations(durations, b.GetStepDurationsMs())
		if deleteBucket != nil {
			if err := deleteBucket(iter.Key()); err != nil {
				return c1zstore.LedgerCounters{}, false, err
			}
		}
	}
	if err := iter.Error(); err != nil {
		return c1zstore.LedgerCounters{}, false, err
	}
	return ledgerCountersFromProto(v3.LedgerCounterBucket_builder{
		Counters:        sum,
		Flags:           flags,
		ConnectorCalls:  calls,
		SessionCalls:    sessions,
		StepDurationsMs: durations,
	}.Build()), needsFold, nil
}

func (l *Ledger) Frontier(ctx context.Context) (*c1zstore.LedgerFrontier, bool, error) {
	val, closer, err := l.e.db.Get(rawdb.LedgerFrontierKey())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer closer.Close()
	f := &v3.LedgerFrontier{}
	if err := unmarshalRecord(val, f); err != nil {
		return nil, false, fmt.Errorf("ledger frontier: unmarshal: %w", err)
	}
	out := &c1zstore.LedgerFrontier{State: f.GetState(), Attempt: f.GetAttempt()}
	if ts := f.GetTakenOverAt(); ts != nil {
		out.TakenOverAt = ts.AsTime()
	}
	return out, true, nil
}

// Holds lifecycleMu across the read and the commit: a CheckpointSync landing
// between them would be reverted by the blind-set of the sync-run key, and an
// endSync that snapshotted the record first would write the pre-takeover token
// back beside a live frontier.
func (l *Ledger) Takeover(ctx context.Context, runID string, facts []string, counters c1zstore.LedgerCounters) (string, error) {
	return l.takeover(ctx, runID, facts, counters, nil)
}

func (l *Ledger) takeover(ctx context.Context, runID string, facts []string, counters c1zstore.LedgerCounters, seed *pendingWorkSeed) (string, error) {
	l.e.lifecycleMu.Lock()
	defer l.e.lifecycleMu.Unlock()
	syncID := l.e.CurrentSyncID()
	if syncID == "" {
		return "", errors.New("Takeover: no open sync")
	}
	rec, err := l.e.GetSyncRunRecord(ctx, syncID)
	if err != nil {
		return "", err
	}
	state := rec.GetSyncToken()
	if state == "" {
		return "", nil
	}
	if seed != nil && state != seed.token {
		return "", errors.New("checkpoint changed before pending-work takeover")
	}
	frontier := v3.LedgerFrontier_builder{State: state, Attempt: syncID, TakenOverAt: timestamppb.Now()}.Build()
	fv, err := marshalRecord(frontier)
	if err != nil {
		return "", err
	}
	updated := proto.Clone(rec).(*v3.SyncRunRecord)
	updated.SetSyncToken("")
	rv, err := marshalRecord(updated)
	if err != nil {
		return "", err
	}
	var bv []byte
	if !counters.IsZero() {
		if bv, err = marshalRecord(ledgerCountersToProto(counters)); err != nil {
			return "", err
		}
	}
	err = l.e.withWrite(func() error {
		if err := l.markInFlightLocked(); err != nil {
			return err
		}
		batch := l.e.db.NewRecordBatch()
		defer batch.Close()
		if seed != nil {
			_, initialized, err := l.workState()
			if err != nil {
				return err
			}
			if initialized {
				return errors.New("checkpoint conflicts with initialized pending work")
			}
			if err := stageInitialWork(batch, syncID, seed.work); err != nil {
				return err
			}
		}
		if err := batch.StageLedgerTakeover(fv, rv); err != nil {
			return err
		}
		for _, f := range facts {
			if err := batch.StageLedgerFact(encodeLedgerFactKey(f)); err != nil {
				return err
			}
		}
		if l.retainTokens.Load() {
			if err := batch.StageLedgerFact(encodeLedgerFactKey(c1zstore.LedgerFactRetainTokens)); err != nil {
				return err
			}
		}
		if bv != nil {
			key := encodeLedgerCounterKey(runID, c1zstore.TakeoverBucketWorker)
			if err := batch.StageLedgerCounterBucket(key, bv); err != nil {
				return err
			}
		}
		return batch.Commit(pebble.Sync)
	})
	if err != nil {
		return "", err
	}
	return state, nil
}

func (l *Ledger) PutCounterBucket(ctx context.Context, runID string, worker uint32, counters c1zstore.LedgerCounters) error {
	if runID == "" {
		return errors.New("PutCounterBucket: empty run id")
	}
	val, err := marshalRecord(ledgerCountersToProto(counters))
	if err != nil {
		return err
	}
	return l.e.withWrite(func() error {
		if err := l.markInFlightLocked(); err != nil {
			return err
		}
		batch := l.e.db.NewRecordBatch()
		defer batch.Close()
		if err := batch.StageLedgerCounterBucket(encodeLedgerCounterKey(runID, worker), val); err != nil {
			return err
		}
		return batch.Commit(pebble.Sync)
	})
}

func (l *Ledger) ClearRows(ctx context.Context, clearFacts []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.e.lifecycleMu.Lock()
	defer l.e.lifecycleMu.Unlock()
	syncID := l.e.CurrentSyncID()
	if syncID == "" {
		return errors.New("ClearRows: no bound sync")
	}
	record, err := l.e.GetSyncRunRecord(ctx, syncID)
	if err != nil {
		return err
	}
	if record.GetEndedAt() == nil {
		return errors.New("ClearRows: bound sync is unfinished")
	}
	_, workOpen, err := l.workState()
	if err != nil {
		return err
	}
	if workOpen {
		return errors.New("ClearRows: processing pass is unfinished")
	}
	keys := make([][]byte, 0, len(clearFacts))
	for _, fact := range clearFacts {
		keys = append(keys, encodeLedgerFactKey(fact))
	}
	if err := l.markResiduePending(); err != nil {
		return err
	}
	return l.e.withWrite(func() error {
		if err := l.markInFlightLocked(); err != nil {
			return err
		}
		if hook := l.e.test.ledgerClearRowsHook; hook != nil {
			if err := hook("stamped"); err != nil {
				return err
			}
		}
		batch := l.e.db.NewRecordBatch()
		defer batch.Close()
		if err := batch.StageLedgerClearRows(keys); err != nil {
			return err
		}
		if hook := l.e.test.ledgerClearRowsHook; hook != nil {
			if err := hook("staged"); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
		if hook := l.e.test.ledgerClearRowsHook; hook != nil {
			return hook("committed")
		}
		return nil
	})
}
