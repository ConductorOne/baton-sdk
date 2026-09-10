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
func (e *Engine) takeoverToken(ctx context.Context, runID string, facts []string, counters *v3.LedgerCounterBucket) (string, error) {
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
		// Same durable declaration as the page batch makes. The takeover
		// needs its own: it writes a frontier holding a verbatim token
		// before any page exists, so a crash right after it would leave
		// that token with no fact to tell the sealing process to scrub it.
		if e.ledgerTokensSensitive.Load() {
			if err := batch.StageLedgerFact(encodeLedgerFactKey(c1zstore.LedgerFactTokensSensitive)); err != nil {
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

// SetLedgerTokensSensitive declares that this sync's connector returns
// page tokens that may carry credentials (brief §3.12). When set, the
// seal (EndSync) rewrites every ledger row to hash-only tokens before
// the ended_at stamp, so no sealed artifact carries a verbatim token.
// The syncer sets it from the connector's capabilities at sync start.
func (e *Engine) SetLedgerTokensSensitive(sensitive bool) {
	e.ledgerTokensSensitive.Store(sensitive)
}

// LedgerTokensSensitive reports the flag.
func (e *Engine) LedgerTokensSensitive() bool { return e.ledgerTokensSensitive.Load() }

// ledgerTokensSensitiveDurable reports whether the seal must scrub: the
// in-memory declaration OR the durable fact the page batch wrote.
//
// The fact is what makes this correct across processes. The flag is set
// by whoever starts the sync; the seal can run in a different process
// after a crash, and that process has no way to know the connector's
// tokens were sensitive. Reading the fact means the obligation travels
// with the file instead of with the goroutine that created it. Called
// once per seal.
func (e *Engine) ledgerTokensSensitiveDurable() (bool, error) {
	if e.ledgerTokensSensitive.Load() {
		return true, nil
	}
	_, closer, err := e.db.Get(encodeLedgerFactKey(c1zstore.LedgerFactTokensSensitive))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	defer closer.Close()
	return true, nil
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
// on a connector that declared its tokens sensitive, a sync that began
// token-only and was taken over would seal with those tokens readable in
// the artifact — exactly what SetLedgerTokensSensitive promises cannot
// happen. The token-only path never had this exposure: its stack is
// empty by the time it seals.
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
func (e *Engine) PurgeLedgerResidue(ctx context.Context) error {
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
	lo, hi := rawdb.LedgerBounds()
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
	return e.withWriteAllowSealed(func() error {
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
	})
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
