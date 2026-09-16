package pebble

// The page ledger: one row per committed syncer page, written in the
// SAME batch as the page's records (docs/tasks/sound-syncs-solutions-
// brief.md §3). This file owns the key encoding, the value codec, the
// read side (with the identity compare that turns a key collision into
// a re-run instead of a silent skip), the seal-time token scrub, and
// the drop. The only writer of ledger rows is the page unit
// (page_unit.go) — see rawdb.StageLedgerRow for why there is no
// standalone writer.
//
// Key shape (single-sync file; no sync id, like every v3 key):
//
//	v3 | TypeLedger | 0x00 | op | rt | rid | parent_rt | parent_rid |
//	    type_scoped | hash16(page_token)
//
// Tuple-encoded so a by-value prefix scan over (op), (op, rt) or
// (op, rt, rid) is a range scan ("every grants page of this resource").
// The token rides as a fixed 16-byte hash so key size is bounded
// regardless of token size; the verbatim token is in the value.

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

// Ledger is the page ledger of the engine's bound sync: one row per
// committed page, facts, counter buckets and the takeover frontier
// (docs/tasks/sound-syncs-solutions-brief.md §3). It writes through the
// engine's lock and stagers; the engine's seal calls scrubTokens and
// purgeResidue.
type Ledger struct {
	e *Engine
	// retainTokens opts OUT of the token scrub the seal performs; the
	// zero value is the safe one.
	retainTokens atomic.Bool
	// mismatches counts read-side identity-compare failures (a key-function
	// bug signal, never data loss: the page re-runs).
	mismatches atomic.Uint64
	// inFlight mirrors the keyspaceVersionLedgerInFlight stamp
	// (keyspace_version.go): set on Open when the file carries it, by the
	// first page commit, cleared at seal.
	inFlight atomic.Bool
}

// Ledger returns the engine's page ledger.
func (e *Engine) Ledger() *Ledger { return &e.ledger }

// ledgerIdentity is the engine-side view of a syncer action's semantic
// identity (brief §3.2): the flat tuple that determines the connector
// request the page makes. Equal identity ⇒ equal work.
type ledgerIdentity struct {
	Op                   string
	ResourceTypeID       string
	ResourceID           string
	ParentResourceTypeID string
	ParentResourceID     string
	PageToken            string
	TypeScoped           bool
	// Spawned rides in the value and the compare, not the key.
	Spawned bool
}

// ledgerTokenHashLen is the truncated SHA-256 width carried in keys
// and in the *_hash value fields. 128 bits: a collision's consequence
// is a re-run (the identity compare fails), never a skip.
const ledgerTokenHashLen = 16

func ledgerTokenHash(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:ledgerTokenHashLen]
}

// encodeLedgerKey builds the ledger key for id. Paired with
// encodeLedgerPrefix* (by-value prefixes).
func encodeLedgerKey(id ledgerIdentity) []byte {
	buf := make([]byte, 0, 64+len(id.Op)+len(id.ResourceTypeID)+len(id.ResourceID)+
		len(id.ParentResourceTypeID)+len(id.ParentResourceID))
	buf = append(buf, rawdb.LedgerKeyPrefix()...)
	buf = codec.AppendTupleStrings(buf, id.Op, id.ResourceTypeID, id.ResourceID,
		id.ParentResourceTypeID, id.ParentResourceID)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleBool(buf, id.TypeScoped)
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleBytes(buf, ledgerTokenHash(id.PageToken))
}

// encodeLedgerPrefixOp bounds every row of one op (by-value prefix:
// trailing separator is load-bearing, see keys.go).
func encodeLedgerPrefixOp(op string) []byte {
	buf := append([]byte{}, rawdb.LedgerKeyPrefix()...)
	buf = codec.AppendTupleStrings(buf, op)
	return codec.AppendTupleSeparator(buf)
}

// encodeLedgerPrefixResource bounds every row of one op on one
// resource (all pages, all parents).
func encodeLedgerPrefixResource(op, resourceTypeID, resourceID string) []byte {
	buf := append([]byte{}, rawdb.LedgerKeyPrefix()...)
	buf = codec.AppendTupleStrings(buf, op, resourceTypeID, resourceID)
	return codec.AppendTupleSeparator(buf)
}

// ledgerLowerBound / ledgerUpperBound bound the whole family (wipe,
// compaction, family-bounded readers).
func ledgerLowerBound() []byte { lo, _ := rawdb.LedgerBounds(); return lo }
func ledgerUpperBound() []byte { _, hi := rawdb.LedgerBounds(); return hi }

// ledgerIdentityToProto / ledgerIdentityFromProto convert between the
// engine view and the stored echo. The stored form always carries the
// token hash so the compare survives a scrub.
func ledgerIdentityToProto(id ledgerIdentity) *v3.LedgerActionIdentity {
	return v3.LedgerActionIdentity_builder{
		Op:                   id.Op,
		ResourceTypeId:       id.ResourceTypeID,
		ResourceId:           id.ResourceID,
		ParentResourceTypeId: id.ParentResourceTypeID,
		ParentResourceId:     id.ParentResourceID,
		PageToken:            id.PageToken,
		PageTokenHash:        ledgerTokenHash(id.PageToken),
		TypeScoped:           id.TypeScoped,
		Spawned:              id.Spawned,
	}.Build()
}

func ledgerIdentityFromProto(p *v3.LedgerActionIdentity) ledgerIdentity {
	return ledgerIdentity{
		Op:                   p.GetOp(),
		ResourceTypeID:       p.GetResourceTypeId(),
		ResourceID:           p.GetResourceId(),
		ParentResourceTypeID: p.GetParentResourceTypeId(),
		ParentResourceID:     p.GetParentResourceId(),
		PageToken:            p.GetPageToken(),
		TypeScoped:           p.GetTypeScoped(),
		Spawned:              p.GetSpawned(),
	}
}

// ledgerIdentityMatches is the read-side identity compare (brief §3.2):
// the structured fields must be equal and the token must match — by
// value when the row still carries it, by hash after a scrub. A row
// that fails this compare is treated as ABSENT by the walk, so a key
// collision costs a re-run of the page, never a skip.
func ledgerIdentityMatches(want ledgerIdentity, got *v3.LedgerActionIdentity, scrubbed bool) bool {
	if got.GetOp() != want.Op ||
		got.GetResourceTypeId() != want.ResourceTypeID ||
		got.GetResourceId() != want.ResourceID ||
		got.GetParentResourceTypeId() != want.ParentResourceTypeID ||
		got.GetParentResourceId() != want.ParentResourceID ||
		got.GetTypeScoped() != want.TypeScoped ||
		got.GetSpawned() != want.Spawned {
		return false
	}
	if !scrubbed {
		return got.GetPageToken() == want.PageToken
	}
	return bytes.Equal(got.GetPageTokenHash(), ledgerTokenHash(want.PageToken))
}

// ErrLedgerIdentityMismatch is returned by GetRow when a row
// exists at the identity's key but echoes a different identity: the
// key function lost a distinguishing field, or two actions collided.
// Callers treat it as "no row" (re-run the page) and it is counted so
// the collision is never silent.
var ErrLedgerIdentityMismatch = errors.New("pebble ledger: row at key echoes a different identity")

// getRowRecord reads the completion row for id. Returns
// pebble.ErrNotFound when the page never committed and
// ErrLedgerIdentityMismatch (also counted in Ledger.mismatches) when a
// row exists but belongs to another identity. Reads never write.
func (l *Ledger) getRowRecord(ctx context.Context, id ledgerIdentity) (*v3.LedgerRow, error) {
	key := encodeLedgerKey(id)
	val, closer, err := l.e.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	row := &v3.LedgerRow{}
	if err := unmarshalRecord(val, row); err != nil {
		return nil, fmt.Errorf("GetRow: unmarshal: %w", err)
	}
	if !ledgerIdentityMatches(id, row.GetIdentity(), row.GetScrubbed()) {
		l.mismatches.Add(1)
		ctxzap.Extract(ctx).Warn("pebble ledger: identity mismatch at key; treating page as not committed",
			zap.String("op", id.Op),
			zap.String("resource_type_id", id.ResourceTypeID),
			zap.String("resource_id", id.ResourceID),
			zap.String("row_op", row.GetIdentity().GetOp()),
			zap.String("row_resource_type_id", row.GetIdentity().GetResourceTypeId()),
			zap.String("row_resource_id", row.GetIdentity().GetResourceId()),
		)
		return nil, ErrLedgerIdentityMismatch
	}
	return row, nil
}

// mismatchCount reports how many GetRow calls found a row
// echoing a different identity since Open. Nonzero is a key-function
// bug to investigate, not a data-loss event (the page re-ran).
func (l *Ledger) mismatchCount() uint64 { return l.mismatches.Load() }

// iterate yields every page row in key order (op, then resource).
// Rows only: facts, counter buckets and the frontier are sibling
// sub-families (rawdb keyspace.go) with their own readers below.
func (l *Ledger) iterate(ctx context.Context, yield func(*v3.LedgerRow) bool) error {
	lo, hi := rawdb.LedgerRowBounds()
	return l.iterateRange(lo, hi, yield)
}

// === facts, counter buckets, frontier (brief §3.6, §3.8) ===

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

// Facts returns every fact set by a committed page (or the
// takeover), name → value ("" for a bare fact). A fact absent here was
// never durably established.
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

// sumCounters folds every (run, worker) bucket into one: counters
// summed by name, flags OR'd. This is the sync-level value.
func (l *Ledger) sumCounters(ctx context.Context) (*v3.LedgerCounterBucket, error) {
	lo, hi := rawdb.LedgerCounterBounds()
	iter, err := l.e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	sum := map[string]uint64{}
	var flags uint64
	calls := map[string]*v3.CallStat{}
	sessions := map[string]*v3.CallStat{}
	durations := map[string]int64{}
	for iter.First(); iter.Valid(); iter.Next() {
		b := &v3.LedgerCounterBucket{}
		if err := unmarshalRecord(iter.Value(), b); err != nil {
			return nil, fmt.Errorf("ledger counters: unmarshal %x: %w", iter.Key(), err)
		}
		for k, v := range b.GetCounters() {
			sum[k] += v
		}
		flags |= b.GetFlags()
		calls = c1zstore.FoldCallStats(calls, b.GetConnectorCalls())
		sessions = c1zstore.FoldCallStats(sessions, b.GetSessionCalls())
		durations = c1zstore.FoldDurations(durations, b.GetStepDurationsMs())
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return v3.LedgerCounterBucket_builder{
		Counters:        sum,
		Flags:           flags,
		ConnectorCalls:  calls,
		SessionCalls:    sessions,
		StepDurationsMs: durations,
	}.Build(), nil
}

// counterBucketCount reports how many buckets exist (tests).
func (l *Ledger) counterBucketCount(ctx context.Context) (int, error) {
	lo, hi := rawdb.LedgerCounterBounds()
	iter, err := l.e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	n := 0
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}
	return n, iter.Error()
}

// getFrontier returns the takeover record, if the sync began
// under a token-only SDK and was taken over.
func (l *Ledger) getFrontier(ctx context.Context) (*v3.LedgerFrontier, bool, error) {
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
	return f, true, nil
}

// takeoverRecord migrates a token-only sync to the ledger (brief §3.8):
// in ONE batch it writes the frontier record (the token's state, now
// the ledger's), the given facts, an initial counter bucket under
// runID (the token's counters, so the sync-level sum stays exact), and
// the sync-run record with its token cleared. After this the token is
// empty and the frontier is the only copy of the stack; a crash before
// the commit leaves the token intact and the frontier absent, so the
// resumed sync takes over again. Returns the state string it moved.
//
// Holds lifecycleMu for the whole read-check-write, the same as
// CheckpointSync and endSync: this is exactly the sequence that mutex
// documents (see engine.go). withWrite serializes only the commit, not
// the GetSyncRunRecord above it, and StageLedgerTakeover blind-sets the
// whole sync-run key. Without the mutex, a CheckpointSync landing between
// the read and the commit is silently reverted, and an endSync that
// snapshotted the record first writes the pre-takeover token back —
// resurrecting a verbatim checkpoint token beside a live frontier, the
// two-authority state ErrLedgeredSyncWritesNoToken exists to prevent.
func (l *Ledger) takeoverRecord(ctx context.Context, runID string, facts []string, counters *v3.LedgerCounterBucket) (string, error) {
	l.e.lifecycleMu.Lock()
	defer l.e.lifecycleMu.Unlock()
	syncID := l.e.CurrentSyncID()
	if syncID == "" {
		return "", errors.New("takeoverRecord: no open sync")
	}
	rec, err := l.e.GetSyncRunRecord(ctx, syncID)
	if err != nil {
		return "", err
	}
	state := rec.GetSyncToken()
	if state == "" {
		return "", nil
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
	if counters != nil {
		if bv, err = marshalRecord(counters); err != nil {
			return "", err
		}
	}
	err = l.e.withWrite(func() error {
		if err := l.markInFlightLocked(); err != nil {
			return err
		}
		batch := l.e.db.NewRecordBatch()
		defer batch.Close()
		if err := batch.StageLedgerTakeover(fv, rv); err != nil {
			return err
		}
		for _, f := range facts {
			if err := batch.StageLedgerFact(encodeLedgerFactKey(f)); err != nil {
				return err
			}
		}
		// Same durable declaration the page batch makes. The takeover needs
		// its own because it can be the first thing to write the ledger,
		// before any page exists to carry the fact.
		if l.retainTokens.Load() {
			if err := batch.StageLedgerFact(encodeLedgerFactKey(c1zstore.LedgerFactRetainTokens)); err != nil {
				return err
			}
		}
		if bv != nil {
			// A reserved index, not 0: worker 0 is a real page worker and
			// blind-writes its own whole total at (runID, 0), so it would
			// overwrite the counters this takeover just migrated.
			key := encodeLedgerCounterKey(runID, c1zstore.TakeoverBucketWorker)
			if err := batch.StageLedgerCounterBucket(key, bv); err != nil {
				return err
			}
		}
		// Sync: the token is gone after this; the frontier must be at
		// least as durable as the token it replaces.
		return batch.Commit(pebble.Sync)
	})
	if err != nil {
		return "", err
	}
	return state, nil
}

// putCounterBucketRecord blind-writes one (run, worker) bucket outside a
// page (brief §3.13): the run's whole cumulative value for that index.
// The syncer uses it for the run's reserved stats bucket — phase
// durations and session-store calls, which are not page-shaped — at
// phase boundaries, stop and seal, best-effort. The value is the whole
// bucket, so the last write wins and the fold never double counts.
func (l *Ledger) putCounterBucketRecord(ctx context.Context, runID string, worker uint32, bucket *v3.LedgerCounterBucket) error {
	if runID == "" {
		return errors.New("putCounterBucketRecord: empty run id")
	}
	if bucket == nil {
		return errors.New("putCounterBucketRecord: nil bucket")
	}
	val, err := marshalRecord(bucket)
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
		// Sync: this is usually the attempt's last word before it exits.
		return batch.Commit(pebble.Sync)
	})
}

// iterateByOp yields every row of one op.
func (l *Ledger) iterateByOp(ctx context.Context, op string, yield func(*v3.LedgerRow) bool) error {
	lo := encodeLedgerPrefixOp(op)
	return l.iterateRange(lo, upperBoundOf(lo), yield)
}

// iterateByResource yields every row of one op on one resource.
func (l *Ledger) iterateByResource(ctx context.Context, op, resourceTypeID, resourceID string, yield func(*v3.LedgerRow) bool) error {
	lo := encodeLedgerPrefixResource(op, resourceTypeID, resourceID)
	return l.iterateRange(lo, upperBoundOf(lo), yield)
}

func (l *Ledger) iterateRange(lo, hi []byte, yield func(*v3.LedgerRow) bool) error {
	iter, err := l.e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		row := &v3.LedgerRow{}
		if err := unmarshalRecord(iter.Value(), row); err != nil {
			return fmt.Errorf("iterate ledger: %w", err)
		}
		if !yield(row) {
			return nil
		}
	}
	return iter.Error()
}

// rowCount counts rows (the trace's size; tests and Stats).
func (l *Ledger) rowCount(ctx context.Context) (uint64, error) {
	var n uint64
	err := l.iterate(ctx, func(*v3.LedgerRow) bool { n++; return true })
	return n, err
}

// SetRetainTokens keeps this sync's page tokens verbatim in the
// sealed artifact, opting out of the scrub EndSync otherwise performs
// before the ended_at stamp (brief §3.12).
//
// The default scrubs, and the default is the one that needs no thought:
// a page token can carry a credential, and the ledger is the first thing
// in a c1z to store one durably. Declaring safety per connector would
// mean every connector that never considers the question ships
// credentials in its artifacts, so the flag records the exception.
//
// Nothing needs the verbatim token after the seal — the ledger keys and
// compares by hash (encodeLedgerKey, ledgerIdentityMatches), and the one
// reader of a token, a resume fetching the next page, cannot exist on a
// sealed sync. This is for reading a finished file by hand.
func (l *Ledger) SetRetainTokens(retain bool) {
	l.retainTokens.Store(retain)
}

// retainTokensFlag reports the flag.
func (l *Ledger) retainTokensFlag() bool { return l.retainTokens.Load() }

// sealScrubsTokens reports whether the seal must scrub. It scrubs unless
// retention was declared, in memory OR by the durable fact a page batch
// wrote.
//
// The fact is what makes the opt-out work across processes: the flag is
// set by whoever starts the sync, while the seal runs wherever the sync
// finishes, which after a crash is a different process. Called once per
// seal.
//
// Both ways this can go wrong are safe, by different routes. An absent
// fact yields true, so the seal scrubs. An unreadable one fails the seal:
// endSyncFinalize returns on the error without looking at the bool, which
// leaves the sync unfinished and the resumed EndSync re-reads the fact.
// Scrubbing on a failed read would be the wrong call — it would destroy
// the verbatim tokens of a retain-tokens sync whose intent this could not
// read.
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

// scrubLedgerRow blanks every verbatim token on the row, keeping the
// hashes, and marks it scrubbed. Returns false if the row was already
// scrubbed (nothing to write).
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
		if len(c.GetPageTokenHash()) == 0 {
			c.SetPageTokenHash(ledgerTokenHash(c.GetPageToken()))
		}
		c.SetPageToken("")
	}
	row.SetScrubbed(true)
	return true
}

// ledgerScrubBatchBytes bounds one scrub batch so a large ledger does
// not build a single multi-GB batch at seal.
const ledgerScrubBatchBytes = 16 << 20

// scrubTokens rewrites every unscrubbed ledger row to hash-only
// tokens. Idempotent: already-scrubbed rows are skipped, so a crash
// mid-scrub and a re-run EndSync finish the job. Runs on the sealed
// lifecycle path (EndSync's finalize) and so takes the AllowSealed
// barrier; the rows' records are already durable, only token fields
// change. Rows are read through an iterator whose snapshot predates
// the batches it feeds, which is fine: nothing else writes the ledger
// once the engine is sealed.
func (l *Ledger) scrubTokens(ctx context.Context) error {
	return l.e.withWriteAllowSealed(func() error {
		lo, hi := rawdb.LedgerRowBounds()
		iter, err := l.e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
		if err != nil {
			return err
		}
		defer iter.Close()

		batch := l.e.db.NewRecordBatch()
		// Closure, not `defer batch.Close()`: commit re-mints the batch,
		// and a direct defer would bind the first one.
		defer func() { _ = batch.Close() }()
		commit := func() error {
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
				if err := commit(); err != nil {
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
		return commit()
	})
}

// scrubFrontierLocked blanks the frontier's verbatim state. The
// caller holds the write barrier and commits the batch.
//
// The frontier is not a ledger row and so is not in the loop above: it
// lives at its own kind (0x03) outside LedgerRowBounds. It needs its own
// scrub because takeoverRecord stores the taken-over sync token JSON
// verbatim, and every Action in that JSON carries a page_token field. So
// a sync that began token-only and was taken over would seal with those
// tokens readable in the artifact even though the seal scrubbed every
// row. The token-only path never had this exposure: its stack is empty
// by the time it seals.
//
// Attempt and taken_over_at stay, so the ledger still records that a
// takeover happened and when. Only the state goes. Nothing needs it
// after the resume that consumed it, and a sealed sync has no resume.
func (l *Ledger) scrubFrontierLocked(ctx context.Context, batch *rawdb.RecordBatch) error {
	frontier, found, err := l.getFrontier(ctx)
	if err != nil {
		return fmt.Errorf("scrubTokens: read frontier: %w", err)
	}
	if !found || frontier.GetState() == "" {
		return nil
	}
	frontier.SetState("")
	val, err := marshalRecord(frontier)
	if err != nil {
		return err
	}
	return batch.StageLedgerFrontier(val)
}

// purgeResidue rewrites the SSTs overlapping the ledger family so
// that superseded row versions are physically gone. scrubTokens
// alone is not enough: pebble never overwrites in place, the scrubbed
// rows flush to a new SST and the pre-scrub rows stay in the SSTs they
// were flushed to. Nothing on the seal-to-save path compacts (seal()
// pauses the scheduler, CompactAllRanges refuses on a sealed engine,
// and save's CheckpointTo hard-links the SSTs as they are), so without
// this the verbatim token would ride along in the shipped c1z as
// byte-level residue — query-invisible, `strings`-visible.
//
// Cost is proportional to the bytes in SSTs overlapping the family — the
// ledger's own plus the L0 files it is interleaved with. Compact flushes
// an overlapping memtable first, so the compacted output covers every
// version that ever landed.
//
// Two ranges: the ledger family, and SyncRunKey's own single-key range.
// takeoverRecord clears sync_token by rewriting the sync-run record at
// v3|TypeSyncRun (0x06), so each CheckpointSync before the takeover left a
// superseded version of that key carrying a page token verbatim. Those
// versions sit in whatever SSTs 0x06 was flushed to, and with TypeIndex
// and four other families between 0x06 and the ledger at 0x0C, a
// compacted L1+ file holding them need not overlap the ledger range at
// all. TestLedgerScrubLeavesNoSSTResidue's takeover arm pins the end-to-end
// property; the second range is what makes it hold at scale rather than
// at fixture size.
func (l *Ledger) purgeResidue(ctx context.Context) error {
	// AllowSealed: the seal's residue purge runs on a sealed engine.
	return l.e.withWriteAllowSealed(func() error {
		return l.compactForResidueLocked(ctx, ledgerResidueSpans())
	})
}

// ledgerResidueSpans: the ledger family and SyncRunKey's single-key range,
// for both purge paths. The second selects the files whose bounds contain
// 0x06 — at most one per L1+ level plus overlapping L0s — so its cost is
// independent of the ledger's size.
func ledgerResidueSpans() []pebble.KeyRange {
	lo, hi := rawdb.LedgerBounds()
	runKey := rawdb.SyncRunKey()
	return []pebble.KeyRange{
		{Start: lo, End: hi},
		{Start: runKey, End: upperBoundOf(runKey)},
	}
}

// purgeMarkedResidue compacts ledgerResidueSpans if the marker is
// armed, and consumes it. A no-op when nothing is armed.
//
// It runs regardless of the retain-tokens fact, unlike the scrub. The
// residue belongs to the sync whose ledger was deleted, whose retention
// intent went with it, and a compaction never removes live rows — so this
// cannot destroy the verbatim tokens of a sync that did ask to keep them.
func (l *Ledger) purgeMarkedResidue(ctx context.Context) error {
	armed, err := l.residuePending()
	if err != nil {
		return fmt.Errorf("purgeMarkedResidue: read marker: %w", err)
	}
	if !armed {
		return nil
	}
	// One critical section for the purge and the consume, so the marker
	// cannot be consumed for a compaction that a concurrent Close cut off.
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

// encodeLedgerResiduePendingKey is the durable marker that ledger bytes are
// still physically in the SSTs with no ledger left in the keyspace to infer
// it from — the state Drop leaves behind. DropKeyRange takes the rows
// out of the keyspace and not out of the SSTs, and db.Compact selects files
// by their bounds, which still cover the ledger range, so compacting that
// range is what reaches the bytes. (ResetForNewSync needs no marker: its
// excise spans the whole keyspace, so no SST survives to hold residue.)
//
// Drop arms it before deleting, since the deletion is what destroys
// the evidence. purgeMarkedResidue consumes it, and only after its
// compaction succeeds, so a failed or interrupted purge is retried by the
// next seal instead of shipping tokens.
func encodeLedgerResiduePendingKey() []byte {
	buf := make([]byte, 0, 2+len("ledger_residue_pending"))
	buf = append(buf, versionV3, typeEngineMeta)
	return codec.AppendTupleStrings(buf, "ledger_residue_pending")
}

// markResiduePending is fsync'd because the state it records outlives
// the process that created it: an interrupted sync's bytes are purged by
// whichever later seal reads the marker.
func (l *Ledger) markResiduePending() error {
	// AllowSealed: Drop arms it on a finished sync.
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

// compactForResidueLocked runs under writeMu like CompactAllRanges,
// so Close waits for the in-flight Compact (pebble.DB.Compact panics on a
// closed DB).
func (l *Ledger) compactForResidueLocked(ctx context.Context, spans []pebble.KeyRange) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// A manual compaction needs a scheduler grant like any other and seal()
	// leaves the scheduler paused, so the pause is lifted for the call; an
	// automatic compaction that starts in that window runs to completion.
	// Re-pause only what we un-paused: on an unsealed engine (direct
	// callers, tests) leaving the scheduler paused is the documented
	// L0StopWritesThreshold hang.
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

// Drop removes the whole ledger family (rows, facts, buckets,
// frontier). The single-sync wipe (ResetForNewSync) covers the family
// with everything else; this is for callers that keep the sync and drop
// only its trace (compaction outputs, the sanitizer's drop policy, the
// syncer's rebind of a finished sync).
func (l *Ledger) Drop(ctx context.Context) error {
	ledgeredBeforeDrop, err := l.active()
	if err != nil {
		return fmt.Errorf("Drop: check ledger presence: %w", err)
	}
	// Armed before the drop, because the drop is what destroys the evidence:
	// DropKeyRange takes the rows out of the keyspace and not out of the
	// SSTs, so afterwards neither active nor anything else can tell
	// that the verbatim page tokens are still there. Without the marker a
	// failed or interrupted purge below is permanent — the retry reads no
	// ledger and returns, and endSyncFinalize's gate finds none either, so
	// the tokens ship. compactPebbleFold is this exact shape: drop the
	// ledger, then seal.
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
		// The stamp classifies the file as mid-ledgered-sync, so it has to
		// go with the rows it describes. Left standing on a drop that runs
		// before the seal, it refuses the replacement sync a checkpoint
		// token AND a plain seal, on a file with no ledger, and still
		// reads as an unsupported layout to a token-only SDK.
		//
		// After the drop, not before: if the clear fails, the rows are
		// gone but the file still refuses a token, which is the safe way
		// round. Clearing first would leave a window where the stamp says
		// token-only over a ledger that is still there.
		return l.clearInFlightLocked()
	}); err != nil {
		return err
	}
	// Inline rather than left to the seal, so a file dropped and shipped
	// without one is clean. On failure the marker stays armed and the next
	// seal retries; the error is returned so the caller sees it too.
	return l.purgeMarkedResidue(ctx)
}

// cloneLedgerRow is the deep copy used by the page unit so a caller
// mutating its LedgerRow after Commit cannot alias the stored bytes'
// source.
func cloneLedgerRow(row *v3.LedgerRow) *v3.LedgerRow {
	return proto.Clone(row).(*v3.LedgerRow)
}

// markInFlightLocked stamps keyspaceVersionLedgerInFlight, once per
// open. Called by pageUnit.Commit BEFORE the unit's batch, synced, so
// the stamp is durable in every image the row is durable in.
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

// clearInFlightLocked restores keyspaceVersion at seal. Idempotent; a
// crash between it and the ended_at stamp leaves an unfinished v2 file
// with rows, which the syncer's attempt guard tolerates.
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

// active reports whether this sync must be treated as ledgered:
// the in-flight stamp is set, OR the ledger family holds a key.
//
// The stamp alone is not enough. Ledger.clearInFlightLocked runs before the
// ended_at stamp, and it drops both the durable stamp and the in-memory
// flag, so two states have a ledger while the flag says otherwise:
//
//   - endSyncFinalize fails after the clear (PutSyncRunRecord IO error).
//     EndSync unseals and the caller may keep writing or retry, with the
//     flag now false.
//   - a crash in the same window. The next open reads a v2 stamp and
//     sets the flag false, over rows that are still there.
//
// In both, gating on the flag alone would let CheckpointSync write a
// token beside a live ledger — the second, lagging authority that
// ErrLedgeredSyncWritesNoToken exists to refuse. Rows outlive the stamp,
// so rows are what the gate asks about. Called from CheckpointSync and
// EndSync only, never per record.
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
