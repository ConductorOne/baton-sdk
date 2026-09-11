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

// LedgerIdentity is the engine-side view of a syncer action's semantic
// identity (brief §3.2): the flat tuple that determines the connector
// request the page makes. Equal identity ⇒ equal work.
type LedgerIdentity struct {
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
func encodeLedgerKey(id LedgerIdentity) []byte {
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

// LedgerLowerBound / LedgerUpperBound bound the whole family (wipe,
// compaction, family-bounded readers).
func LedgerLowerBound() []byte { lo, _ := rawdb.LedgerBounds(); return lo }
func LedgerUpperBound() []byte { _, hi := rawdb.LedgerBounds(); return hi }

// ledgerIdentityToProto / ledgerIdentityFromProto convert between the
// engine view and the stored echo. The stored form always carries the
// token hash so the compare survives a scrub.
func ledgerIdentityToProto(id LedgerIdentity) *v3.LedgerActionIdentity {
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

func ledgerIdentityFromProto(p *v3.LedgerActionIdentity) LedgerIdentity {
	return LedgerIdentity{
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
func ledgerIdentityMatches(want LedgerIdentity, got *v3.LedgerActionIdentity, scrubbed bool) bool {
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

// ErrLedgerIdentityMismatch is returned by GetLedgerRow when a row
// exists at the identity's key but echoes a different identity: the
// key function lost a distinguishing field, or two actions collided.
// Callers treat it as "no row" (re-run the page) and it is counted so
// the collision is never silent.
var ErrLedgerIdentityMismatch = errors.New("pebble ledger: row at key echoes a different identity")

// GetLedgerRowRecord reads the completion row for id. Returns
// pebble.ErrNotFound when the page never committed and
// ErrLedgerIdentityMismatch (also counted in ledgerMismatches) when a
// row exists but belongs to another identity. Reads never write.
func (e *Engine) GetLedgerRowRecord(ctx context.Context, id LedgerIdentity) (*v3.LedgerRow, error) {
	key := encodeLedgerKey(id)
	val, closer, err := e.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	row := &v3.LedgerRow{}
	if err := unmarshalRecord(val, row); err != nil {
		return nil, fmt.Errorf("GetLedgerRow: unmarshal: %w", err)
	}
	if !ledgerIdentityMatches(id, row.GetIdentity(), row.GetScrubbed()) {
		e.ledgerMismatches.Add(1)
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

// LedgerMismatches reports how many GetLedgerRow calls found a row
// echoing a different identity since Open. Nonzero is a key-function
// bug to investigate, not a data-loss event (the page re-ran).
func (e *Engine) LedgerMismatches() uint64 { return e.ledgerMismatches.Load() }

// IterateLedger yields every page row in key order (op, then resource).
// Rows only: facts, counter buckets and the frontier are sibling
// sub-families (rawdb keyspace.go) with their own readers below.
func (e *Engine) IterateLedger(ctx context.Context, yield func(*v3.LedgerRow) bool) error {
	lo, hi := rawdb.LedgerRowBounds()
	return e.iterateLedgerRange(lo, hi, yield)
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

// LedgerFacts returns every fact set by a committed page (or the
// takeover), name → value ("" for a bare fact). A fact absent here was
// never durably established.
func (e *Engine) LedgerFacts(ctx context.Context) (map[string]string, error) {
	lo, hi := rawdb.LedgerFactBounds()
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
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

// SumLedgerCounters folds every (run, worker) bucket into one: counters
// summed by name, flags OR'd. This is the sync-level value.
func (e *Engine) SumLedgerCounters(ctx context.Context) (*v3.LedgerCounterBucket, error) {
	lo, hi := rawdb.LedgerCounterBounds()
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
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

// LedgerCounterBucketCount reports how many buckets exist (tests).
func (e *Engine) LedgerCounterBucketCount(ctx context.Context) (int, error) {
	lo, hi := rawdb.LedgerCounterBounds()
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
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

// GetLedgerFrontier returns the takeover record, if the sync began
// under a token-only SDK and was taken over.
func (e *Engine) GetLedgerFrontier(ctx context.Context) (*v3.LedgerFrontier, bool, error) {
	val, closer, err := e.db.Get(rawdb.LedgerFrontierKey())
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

// takeoverToken migrates a token-only sync to the ledger (brief §3.8):
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
func (e *Engine) takeoverToken(ctx context.Context, runID string, facts []string, counters *v3.LedgerCounterBucket) (string, error) {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return "", errors.New("takeoverToken: no open sync")
	}
	rec, err := e.GetSyncRunRecord(ctx, syncID)
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
	err = e.withWrite(func() error {
		if err := e.markLedgerInFlight(); err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
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
		if e.retainLedgerTokens.Load() {
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

// PutLedgerCounterBucket blind-writes one (run, worker) bucket outside a
// page (brief §3.13): the run's whole cumulative value for that index.
// The syncer uses it for the run's reserved stats bucket — phase
// durations and session-store calls, which are not page-shaped — at
// phase boundaries, stop and seal, best-effort. The value is the whole
// bucket, so the last write wins and the fold never double counts.
func (e *Engine) PutLedgerCounterBucket(ctx context.Context, runID string, worker uint32, bucket *v3.LedgerCounterBucket) error {
	if runID == "" {
		return errors.New("PutLedgerCounterBucket: empty run id")
	}
	if bucket == nil {
		return errors.New("PutLedgerCounterBucket: nil bucket")
	}
	val, err := marshalRecord(bucket)
	if err != nil {
		return err
	}
	return e.withWrite(func() error {
		if err := e.markLedgerInFlight(); err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
		defer batch.Close()
		if err := batch.StageLedgerCounterBucket(encodeLedgerCounterKey(runID, worker), val); err != nil {
			return err
		}
		// Sync: this is usually the attempt's last word before it exits.
		return batch.Commit(pebble.Sync)
	})
}

// IterateLedgerByOp yields every row of one op.
func (e *Engine) IterateLedgerByOp(ctx context.Context, op string, yield func(*v3.LedgerRow) bool) error {
	lo := encodeLedgerPrefixOp(op)
	return e.iterateLedgerRange(lo, upperBoundOf(lo), yield)
}

// IterateLedgerByResource yields every row of one op on one resource.
func (e *Engine) IterateLedgerByResource(ctx context.Context, op, resourceTypeID, resourceID string, yield func(*v3.LedgerRow) bool) error {
	lo := encodeLedgerPrefixResource(op, resourceTypeID, resourceID)
	return e.iterateLedgerRange(lo, upperBoundOf(lo), yield)
}

func (e *Engine) iterateLedgerRange(lo, hi []byte, yield func(*v3.LedgerRow) bool) error {
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
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

// LedgerRowCount counts rows (the trace's size; tests and Stats).
func (e *Engine) LedgerRowCount(ctx context.Context) (uint64, error) {
	var n uint64
	err := e.IterateLedger(ctx, func(*v3.LedgerRow) bool { n++; return true })
	return n, err
}

// SetRetainLedgerTokens keeps this sync's page tokens verbatim in the
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
func (e *Engine) SetRetainLedgerTokens(retain bool) {
	e.retainLedgerTokens.Store(retain)
}

// RetainLedgerTokens reports the flag.
func (e *Engine) RetainLedgerTokens() bool { return e.retainLedgerTokens.Load() }

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
func (e *Engine) sealScrubsTokens() (bool, error) {
	if e.retainLedgerTokens.Load() {
		return false, nil
	}
	_, closer, err := e.db.Get(encodeLedgerFactKey(c1zstore.LedgerFactRetainTokens))
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

// ScrubLedgerTokens rewrites every unscrubbed ledger row to hash-only
// tokens. Idempotent: already-scrubbed rows are skipped, so a crash
// mid-scrub and a re-run EndSync finish the job. Runs on the sealed
// lifecycle path (EndSync's finalize) and so takes the AllowSealed
// barrier; the rows' records are already durable, only token fields
// change. Rows are read through an iterator whose snapshot predates
// the batches it feeds, which is fine: nothing else writes the ledger
// once the engine is sealed.
func (e *Engine) ScrubLedgerTokens(ctx context.Context) error {
	return e.withWriteAllowSealed(func() error {
		lo, hi := rawdb.LedgerRowBounds()
		iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
		if err != nil {
			return err
		}
		defer iter.Close()

		batch := e.db.NewRecordBatch()
		// Closure, not `defer batch.Close()`: commit re-mints the batch,
		// and a direct defer would bind the first one.
		defer func() { _ = batch.Close() }()
		commit := func() error {
			if batch.Empty() {
				return nil
			}
			if err := batch.Commit(writeOpts(e.opts.durability)); err != nil {
				return err
			}
			if err := batch.Close(); err != nil {
				return err
			}
			batch = e.db.NewRecordBatch()
			return nil
		}

		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			row := &v3.LedgerRow{}
			if err := unmarshalRecord(iter.Value(), row); err != nil {
				return fmt.Errorf("ScrubLedgerTokens: unmarshal: %w", err)
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
		if err := e.scrubLedgerFrontierLocked(ctx, batch); err != nil {
			return err
		}
		return commit()
	})
}

// scrubLedgerFrontierLocked blanks the frontier's verbatim state. The
// caller holds the write barrier and commits the batch.
//
// The frontier is not a ledger row and so is not in the loop above: it
// lives at its own kind (0x03) outside LedgerRowBounds. It needs its own
// scrub because takeoverToken stores the taken-over sync token JSON
// verbatim, and every Action in that JSON carries a page_token field. So
// a sync that began token-only and was taken over would seal with those
// tokens readable in the artifact even though the seal scrubbed every
// row. The token-only path never had this exposure: its stack is empty
// by the time it seals.
//
// Attempt and taken_over_at stay, so the ledger still records that a
// takeover happened and when. Only the state goes. Nothing needs it
// after the resume that consumed it, and a sealed sync has no resume.
func (e *Engine) scrubLedgerFrontierLocked(ctx context.Context, batch *rawdb.RecordBatch) error {
	frontier, found, err := e.GetLedgerFrontier(ctx)
	if err != nil {
		return fmt.Errorf("ScrubLedgerTokens: read frontier: %w", err)
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

// PurgeLedgerResidue rewrites the SSTs overlapping the ledger family so
// that superseded row versions are physically gone. ScrubLedgerTokens
// alone is not enough: pebble never overwrites in place, the scrubbed
// rows flush to a new SST and the pre-scrub rows stay in the SSTs they
// were flushed to. Nothing on the seal-to-save path compacts (seal()
// pauses the scheduler, CompactAllRanges refuses on a sealed engine,
// and save's CheckpointTo hard-links the SSTs as they are), so without
// this the verbatim token would ride along in the shipped c1z as
// byte-level residue — query-invisible, `strings`-visible.
//
// Runs on the sealed lifecycle path. The scheduler is paused under
// seal and a manual compaction needs a grant like any other, so the
// pause is lifted for the duration of the call (any automatic
// compaction that slips in runs to completion; bounded, and the window
// is short). Cost is proportional to the bytes in SSTs overlapping the
// family — the ledger's own plus the L0 files it is interleaved with.
// Compact flushes an overlapping memtable first, so the compacted
// output covers every version that ever landed.
//
// The range is the ledger family alone, and one thing sits outside it:
// takeoverToken clears sync_token by rewriting the sync-run record at
// v3|TypeSyncRun (0x06), so each CheckpointSync before the takeover left
// a superseded version of that key carrying a page token verbatim. Those
// versions do go, but incidentally — little enough lives between 0x06 and
// the ledger family that the SSTs overlapping one overlap the other, so
// this compaction rewrites them anyway. TestLedgerScrubLeavesNoSSTResidue's
// takeover arm pins the end-to-end property. Compacting SyncRunKey's own
// single-key range here would make it structural instead, which is the fix
// if a keyspace change ever puts enough between the two to split them and
// that arm fails.
func (e *Engine) PurgeLedgerResidue(ctx context.Context) error {
	lo, hi := rawdb.LedgerBounds()
	return e.compactForLedgerResidue(ctx, lo, hi)
}

// purgeMarkedLedgerResidue compacts whatever range the armed marker calls
// for and consumes it. A no-op when nothing is armed.
//
// It runs regardless of the retain-tokens fact, unlike the scrub. The
// residue belongs to the sync whose ledger was deleted, whose retention
// intent went with it, and a compaction never removes live rows — so this
// cannot destroy the verbatim tokens of a sync that did ask to keep them.
func (e *Engine) purgeMarkedLedgerResidue(ctx context.Context) error {
	kind, err := e.ledgerResidueKind()
	if err != nil {
		return fmt.Errorf("purgeMarkedLedgerResidue: read marker: %w", err)
	}
	if kind == 0 {
		return nil
	}
	lo, hi := rawdb.LedgerBounds()
	if kind == residueExcised {
		lo, hi = []byte{versionV3}, []byte{versionV3 + 1}
	}
	if err := e.compactForLedgerResidue(ctx, lo, hi); err != nil {
		return err
	}
	if err := e.db.MetaDelete(encodeLedgerResiduePendingKey(), pebble.Sync); err != nil {
		return fmt.Errorf("purgeMarkedLedgerResidue: consume marker: %w", err)
	}
	return nil
}

// encodeLedgerResiduePendingKey is the durable marker that ledger bytes are
// still physically in the SSTs with no ledger left in the keyspace to infer
// it from — the state both ways of deleting the family leave behind. It is
// an engine-meta key because ResetForNewSync's excise spans
// typeResourceType..typeEngineMeta, so engine-meta is the one family that
// survives the wipe that creates this state.
//
// DropLedger and ResetForNewSync arm it before deleting, since the deletion
// is what destroys the evidence. purgeMarkedLedgerResidue consumes it, and
// only after its compaction succeeds, so a failed or interrupted purge is
// retried by the next seal instead of shipping tokens.
func encodeLedgerResiduePendingKey() []byte {
	buf := make([]byte, 0, 2+len("ledger_residue_pending"))
	buf = append(buf, versionV3, typeEngineMeta)
	return codec.AppendTupleStrings(buf, "ledger_residue_pending")
}

// How the rows were deleted, which decides the range the purge has to
// compact. db.Compact selects files by their bounds: DropKeyRange leaves the
// old versions in files whose bounds still cover the ledger range, so
// compacting that range rewrites them, but ExciseRange narrows those files
// into virtual ones whose bounds exclude it, and then only a compaction wide
// enough to still overlap them reaches the bytes.
//
// The distinction is worth keeping because DropLedger runs on every
// compaction fold, where the wide compaction would rewrite the whole
// artifact.
const (
	residueTombstoned = 't'
	residueExcised    = 'x'
)

// markLedgerResiduePending is fsync'd because the state it records outlives
// the process that created it: an interrupted sync's bytes are purged by
// whichever later seal reads the marker.
//
// An excised marker is never downgraded to a tombstoned one. A file can
// collect both — a reset, then a ledgered sync, then a drop — and the narrow
// compaction the tombstoned kind asks for would leave the excised bytes and
// consume the marker that was standing for them.
func (e *Engine) markLedgerResiduePending(kind byte) error {
	if kind == residueTombstoned {
		standing, err := e.ledgerResidueKind()
		if err != nil {
			return fmt.Errorf("arm ledger-residue marker: read standing kind: %w", err)
		}
		if standing == residueExcised {
			return nil
		}
	}
	if err := e.db.MetaSet(encodeLedgerResiduePendingKey(), []byte{kind}, pebble.Sync); err != nil {
		return fmt.Errorf("arm ledger-residue marker: %w", err)
	}
	return nil
}

// ledgerResidueKind returns 0 when no marker is armed.
func (e *Engine) ledgerResidueKind() (byte, error) {
	val, closer, err := e.db.Get(encodeLedgerResiduePendingKey())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	defer closer.Close()
	if len(val) != 1 {
		// Written by markLedgerResiduePending alone, so this is corruption.
		// Treat it as the wider kind: the marker's whole purpose is that the
		// bytes cannot be found any other way.
		return residueExcised, nil
	}
	return val[0], nil
}

func (e *Engine) compactForLedgerResidue(ctx context.Context, lo, hi []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.checkWritableAllowSealed(); err != nil {
		return err
	}
	// Same Close-race guard as CompactAllRanges: hold writeWG so Close
	// waits for the in-flight Compact (pebble.DB.Compact panics on a
	// closed DB). No writeMu: pebble's compaction is concurrency-safe
	// with foreground writes.
	e.writeWG.Add(1)
	defer e.writeWG.Done()
	if e.closing.Load() {
		return ErrEngineClosing
	}
	// Re-pause only what we un-paused: on an unsealed engine (direct
	// callers, tests) leaving the scheduler paused is the documented
	// L0StopWritesThreshold hang.
	if e.compactionScheduler != nil && e.compactionScheduler.paused.Load() {
		e.resumeCompactions()
		defer e.pauseCompactions()
	}
	e.test.ledgerResiduePurges.Add(1)
	if err := e.db.Compact(ctx, lo, hi, true); err != nil {
		return fmt.Errorf("PurgeLedgerResidue: %w", err)
	}
	return nil
}

// DropLedger removes the whole ledger family (rows, facts, buckets,
// frontier). The single-sync wipe (ResetForNewSync) covers the family
// through scopedRanges; this is for callers that keep the sync and drop
// only its trace (compaction outputs, the sanitizer's drop policy, the
// syncer's rebind of a finished sync).
func (e *Engine) DropLedger(ctx context.Context) error {
	ledgeredBeforeDrop, err := e.ledgerActive()
	if err != nil {
		return fmt.Errorf("DropLedger: check ledger presence: %w", err)
	}
	// Armed before the drop, because the drop is what destroys the evidence:
	// DropKeyRange takes the rows out of the keyspace and not out of the
	// SSTs, so afterwards neither ledgerActive nor anything else can tell
	// that the verbatim page tokens are still there. Without the marker a
	// failed or interrupted purge below is permanent — the retry reads no
	// ledger and returns, and endSyncFinalize's gate finds none either, so
	// the tokens ship. compactPebbleFold is this exact shape: drop the
	// ledger, then seal.
	if ledgeredBeforeDrop {
		if err := e.markLedgerResiduePending(residueTombstoned); err != nil {
			return fmt.Errorf("DropLedger: %w", err)
		}
	}
	if err := e.withWriteAllowSealed(func() error {
		lo, hi := rawdb.LedgerBounds()
		if err := e.db.DropKeyRange(lo, hi, writeOpts(e.opts.durability)); err != nil {
			return err
		}
		// The stamp classifies the file as mid-ledgered-sync, so it has to
		// go with the rows it describes. Left standing on a drop that runs
		// before the seal, it refuses the replacement sync a checkpoint
		// token AND a plain seal, on a file with no ledger, and still
		// reads as an unsupported layout to a token-only SDK. Same defect
		// ResetForNewSync had, in the other place that deletes these rows.
		//
		// After the drop, not before: if the clear fails, the rows are
		// gone but the file still refuses a token, which is the safe way
		// round. Clearing first would leave a window where the stamp says
		// token-only over a ledger that is still there.
		return e.clearLedgerInFlight()
	}); err != nil {
		return err
	}
	// Inline rather than left to the seal, so a file dropped and shipped
	// without one is clean. On failure the marker stays armed and the next
	// seal retries; the error is returned so the caller sees it too.
	return e.purgeMarkedLedgerResidue(ctx)
}

// ResetLedger implements c1zstore.PageLedgerStore.
func (e *Engine) ResetLedger(ctx context.Context) error {
	return e.DropLedger(ctx)
}

// BoundSyncFinished implements c1zstore.PageLedgerStore.
func (e *Engine) BoundSyncFinished(ctx context.Context) (bool, error) {
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return false, nil
	}
	rec, err := e.GetSyncRunRecord(ctx, syncID)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return rec.GetEndedAt() != nil, nil
}

// cloneLedgerRow is the deep copy used by the page unit so a caller
// mutating its LedgerRow after Commit cannot alias the stored bytes'
// source.
func cloneLedgerRow(row *v3.LedgerRow) *v3.LedgerRow {
	return proto.Clone(row).(*v3.LedgerRow)
}
