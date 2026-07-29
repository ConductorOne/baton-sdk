package pebble

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// PutGrantRecord writes a grant record + its remaining secondary index entries,
// atomically in a single pebble.Batch.
//
// This is the engine's canonical write path; other record types
// follow the same shape (read the previous primary if any → delete
// its index entries → write the new primary → write the new index
// entries → commit).
func (e *Engine) PutGrantRecord(ctx context.Context, r *v3.GrantRecord) error {
	if r == nil {
		return errors.New("PutGrantRecord: nil record")
	}
	return e.PutGrantRecords(ctx, r)
}

// PutGrantRecords writes N grants through a single RecordBatch —
// StageGrantPutInline derives the index obligations inside rawdb, so
// primary and index keys land in one atomic commit. This is the bulk
// path the adapter's PutGrants uses.
//
// Mutation safety. Connectors can legitimately emit the same
// external_id twice within a single sync (paginated sources,
// deduplication bugs in upstream APIs, etc.). To prevent orphan
// index entries from earlier duplicates, we pre-scan records to find
// the latest occurrence of each external_id and process only those —
// earlier duplicates are dropped before any batch byte is written.
// db.Get doesn't see in-batch writes either way, so this dedup pass is
// the load-bearing safety net that neither the old read-before-write
// path nor a pure skip-Get path provides.
//
// Read-before-write overwrite probe. On a NON-fresh sync the engine
// must Get the prior primary key so StageGrantPutInline can stage the
// overwrite obligations — grant index keys are KEY-derived (identity
// key), so the probe is an existence check, not a value read: the
// stale-index cleanup and digest invalidation the deriver stages are
// keyed off the same identity. On a FRESH sync the keyspace is empty
// by construction (StartNewSync excises it) so the Get is guaranteed
// to return ErrNotFound and we skip it — saves an LSM lookup per
// record on the bulk path.
//
// One RecordBatch. Primary and index keys are staged into a single
// batch via StageGrantPutInline, which derives the index obligations
// inside rawdb — one commit, primary+index atomic. (This path
// historically split primary and index keys into two batches to
// preserve each key family's natural sort order for Pebble's
// flushable-batch promotion; the choke-point migration collapsed
// them, trading that micro-optimization for atomicity and
// can't-forget index derivation.)
//
// Fresh-sync still uses pebble.NoSync — EndFreshSync does one
// Flush+fsync at sync end to harden the data.
func (e *Engine) PutGrantRecords(ctx context.Context, records ...*v3.GrantRecord) error {
	if len(records) == 0 {
		return nil
	}
	return e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
		defer batch.Close()

		fresh := e.IsFreshSync()
		// skipGet fires exactly once per fresh sync — only the first
		// PutGrantRecords call sees the keyspace empty by construction.
		// Subsequent calls in the same fresh sync still need
		// read-before-write to clean up index entries that the prior
		// in-sync calls already committed (e.g. paginated sources
		// emitting an external_id on two pages).
		skipGet := e.takeFreshGrantsEmpty()

		// Dedup pre-pass: keep only the LAST occurrence of each
		// structured grant identity. The map value is the records[]
		// index — when we re-iterate, we process record i only if
		// dedup[ext] == i.
		type dedupKey struct {
			id grantIdentity
		}
		var dedup map[dedupKey]int
		if len(records) > 1 {
			dedup = make(map[dedupKey]int, len(records))
			for i, r := range records {
				if r == nil {
					continue
				}
				id, err := grantIdentityFromRecord(r)
				if err != nil {
					return err
				}
				dedup[dedupKey{id}] = i
			}
		}

		for i, r := range records {
			if r == nil {
				continue
			}
			id, err := grantIdentityFromRecord(r)
			if err != nil {
				return err
			}
			if dedup != nil {
				if dedup[dedupKey{id}] != i {
					continue
				}
			}
			key := encodeGrantIdentityKey(id)
			val, err := marshalRecord(r)
			if err != nil {
				return err
			}
			hadOld := false
			if !skipGet {
				_, closer, getErr := e.db.Get(key)
				switch {
				case getErr == nil:
					hadOld = true
					closer.Close()
				case errors.Is(getErr, pebble.ErrNotFound):
					// no prior record — write unconditionally
				default:
					return fmt.Errorf("PutGrantRecords: get old: %w", getErr)
				}
			}
			// One typed op stages the row and everything it owes:
			// prior-row index cleanup, by_principal, needs_expansion,
			// digest invalidation. Index keys derive from the primary
			// key (identity-encoded), so the prior VALUE is not needed
			// for cleanup — the Get above reduces to an existence probe.
			if err := batch.StageGrantPutInline(key, val, hadOld, r.GetNeedsExpansion()); err != nil {
				return err
			}
		}
		opts := writeOpts(e.opts.durability)
		if fresh {
			opts = pebble.NoSync
		}
		// One atomic commit: primary rows and their index/invalidation
		// obligations ride the same batch, so a primary commit landing
		// without its index entries is unexpressible.
		return batch.Commit(opts)
	})
}

// PutExpandedGrantRecords is the grant-expander write path — the
// engine side of GrantStore.StoreExpandedGrants, and its only caller.
//
// Two properties distinguish it from PutGrantRecords:
//
//   - Single read-before-write. The expander must preserve each
//     grant's existing Expansion / NeedsExpansion / DiscoveredAt
//     side-state, which requires reading the prior primary value. That
//     same read also yields the bytes needed to delete the prior
//     value's stale index entries. The old path did BOTH a
//     GetGrantRecord in the adapter (to preserve side-state) AND a
//     db.Get here (to clean indexes) — two point lookups per grant.
//     This path issues one and uses it for both.
//
//   - NoSync commit. Expanded grants are fully regenerable from the
//     sync (the expander recomputes them from the entitlement graph),
//     so a per-batch fsync buys nothing. Writes commit with
//     pebble.NoSync and are hardened by the single Flush at sync end
//     (EndFreshSync) or Close — the same bargain the
//     fresh-sync fast path strikes, extended to resumed syncs where
//     IsFreshSync() is false.
//
// records arrive as freshly translated v3 GrantRecords with NO
// preservation or discovered_at stamping applied; this method performs
// the merge so the read it already issues does double duty.
func (e *Engine) PutExpandedGrantRecords(ctx context.Context, records []*v3.GrantRecord) error {
	if len(records) == 0 {
		return nil
	}
	e.expandedWriteCalls.Add(1)
	e.expandedWriteRows.Add(int64(len(records)))
	return e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
		defer batch.Close()

		now := timestamppb.Now()

		// Dedup pre-pass: keep only the LAST occurrence of each
		// structured grant identity. The expander appends the same deterministic
		// grant id more than once when it merges sources for a
		// principal across a source page, so without this the earlier
		// occurrences would leak orphan index entries (db.Get can't see
		// in-batch writes). Same safety net as PutGrantRecords.
		var dedup map[grantIdentity]int
		if len(records) > 1 {
			dedup = make(map[grantIdentity]int, len(records))
			for i, r := range records {
				if r == nil {
					continue
				}
				id, err := grantIdentityFromRecord(r)
				if err != nil {
					return err
				}
				dedup[id] = i
			}
		}

		// Scratch buffers reused across every record. pebble.Batch.Set
		// and Delete copy their key/value arguments, so one buffer can
		// back the primary key and the marshaled value in turn — the
		// per-record key/marshal allocations were the engine's hottest
		// allocator on the expansion write path. (Index-key scratch
		// lives inside the RecordBatch; the typed ops splice index keys
		// from the primary key.)
		var keyScratch, valScratch []byte
		var prior v3.GrantRecord
		for i, r := range records {
			if r == nil {
				continue
			}
			id, err := grantIdentityFromRecord(r)
			if err != nil {
				return err
			}
			if dedup != nil && dedup[id] != i {
				continue
			}
			ext := r.GetExternalId()
			keyScratch = appendGrantIdentityKey(keyScratch[:0], id)

			hadOld := false
			oldVal, closer, getErr := e.db.Get(keyScratch)
			switch {
			case getErr == nil:
				// Preserve the prior record's expansion side-state +
				// discovered_at (StoreExpandedGrants contract). The prior
				// value's index cleanup is the typed op's obligation.
				hadOld = true
				prior.Reset()
				if err := unmarshalRecord(oldVal, &prior); err != nil {
					closer.Close()
					return fmt.Errorf("PutExpandedGrantRecords: unmarshal prior %q: %w", ext, err)
				}
				r.SetExpansion(prior.GetExpansion())
				r.SetNeedsExpansion(prior.GetNeedsExpansion())
				r.SetDiscoveredAt(prior.GetDiscoveredAt())
				closer.Close()
			case errors.Is(getErr, pebble.ErrNotFound):
				// No prior record: discovered_at is stamped below unless the
				// translation already carried one.
			default:
				return fmt.Errorf("PutExpandedGrantRecords: get old %q: %w", ext, getErr)
			}
			if r.GetDiscoveredAt() == nil {
				r.SetDiscoveredAt(now)
			}

			val, err := marshalRecordAppend(valScratch[:0], r)
			if err != nil {
				return err
			}
			valScratch = val
			// Deferred regime: arms the rebuild marker, cleans the prior
			// needs_expansion entry (by_principal is excised+rebuilt at
			// seal), stages row + conditional needs_expansion + digest
			// invalidation.
			if err := batch.StageGrantPutDeferred(keyScratch, val, hadOld, r.GetNeedsExpansion()); err != nil {
				return err
			}
		}

		if e.test.recordCommitHook != nil {
			if err := e.test.recordCommitHook(); err != nil {
				return err
			}
		}
		// One atomic commit: rows and their obligations ride the same
		// batch, so a primary commit landing without its index entries
		// is unexpressible.
		return batch.Commit(pebble.NoSync)
	})
}

// PutSynthesizedGrantRecords writes expander-synthesized grants that the caller
// guarantees are brand-new by structured grant identity. It skips the
// read-before-write Get in PutExpandedGrantRecords because there is no prior
// value whose Expansion/NeedsExpansion/DiscoveredAt or index entries must be
// preserved/cleaned.
func (e *Engine) PutSynthesizedGrantRecords(ctx context.Context, records []*v3.GrantRecord) error {
	if len(records) == 0 {
		return nil
	}
	e.synthesizedWriteCalls.Add(1)
	e.synthesizedWriteRows.Add(int64(len(records)))
	return e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
		defer batch.Close()

		now := timestamppb.Now()
		var keyScratch, valScratch []byte
		for _, r := range records {
			if r == nil {
				continue
			}
			id, err := grantIdentityFromRecord(r)
			if err != nil {
				return err
			}
			keyScratch = appendGrantIdentityKey(keyScratch[:0], id)
			if r.GetDiscoveredAt() == nil {
				r.SetDiscoveredAt(now)
			}
			val, err := marshalRecordAppend(valScratch[:0], r)
			if err != nil {
				return err
			}
			valScratch = val
			// Deferred regime; the caller guarantees brand-new identities,
			// so there is no prior row to clean (hadOldVal=false).
			if err := batch.StageGrantPutDeferred(keyScratch, val, false, r.GetNeedsExpansion()); err != nil {
				return err
			}
		}
		if e.test.recordCommitHook != nil {
			if err := e.test.recordCommitHook(); err != nil {
				return err
			}
		}
		// One atomic commit: rows and their obligations ride the same
		// batch, so a primary commit landing without its index entries
		// is unexpressible.
		return batch.Commit(pebble.NoSync)
	})
}

type synthesizedGrantRecord struct {
	id          grantIdentity
	entitlement *v3.EntitlementRef
	principal   *v3.PrincipalRef
	sources     batonGrant.Sources
}

// PutSynthesizedGrantContributions batch-writes one destination's synthesized
// contributions. It is the fallback for stores/engines that cannot run a
// layer-scoped layer session (see BeginSynthesizedGrantLayer); the layer path
// is preferred because it publishes sorted SSTs instead of out-of-order batch
// commits.
func (e *Engine) PutSynthesizedGrantContributions(ctx context.Context, records []synthesizedGrantRecord) error {
	if len(records) == 0 {
		return nil
	}
	e.synthesizedWriteCalls.Add(1)
	e.synthesizedWriteRows.Add(int64(len(records)))
	return e.putSynthesizedGrantContributionsBatch(ctx, records)
}

// synthLayerSegmentRows is the default row count at which an open layer
// session cuts its current segment and hands it to the background worker for
// merge + SST ingest. Cutting mid-layer is safe: nothing reads a layer's rows
// until the next layer begins, and keys are globally unique, so segments may
// cover overlapping key ranges without conflict. Segments keep the merge
// fan-in small, bound temp-disk usage, and overlap merge/ingest work with the
// producer's compute instead of serializing it at the layer boundary.
const synthLayerSegmentRows = 8_000_000

func synthLayerSegmentLimit() int64 {
	if raw := os.Getenv("BATON_PEBBLE_SYNTH_LAYER_SEGMENT_ROWS"); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil && parsed > 0 {
			return parsed
		}
	}
	return synthLayerSegmentRows
}

// synthLayerSegment is one finalized batch of sorted spill chunks awaiting
// merge + ingest on the session's background worker.
type synthLayerSegment struct {
	name   string
	chunks []string
}

// synthGrantLayerSession accumulates one topological layer's synthesized grant
// rows as encoded (key, value) pairs in a background-sorted spill sorter.
// Every segLimit rows the current sorter is finalized into a segment and
// queued for the background worker, which k-way merges the segment's chunks
// into one SST and ingests it while the producer keeps encoding. Finish
// flushes the tail segment and waits the worker out. Encoding happens at Add
// time, so the session never retains references into the caller's reused
// buffers. One session at a time; the expansion driver is the single
// producer.
type synthGrantLayerSession struct {
	dir      string
	sorter   *spillSorter
	sortSem  chan struct{}
	segIdx   int
	segRows  int64
	segLimit int64

	// segCh carries finalized segments to the worker; its capacity bounds
	// how many merged-but-uningested segments can stack up before Add blocks
	// (backpressure). segErr holds the worker's first failure, surfaced on
	// the next Add/Finish.
	segCh  chan synthLayerSegment
	segWG  sync.WaitGroup
	segMu  sync.Mutex
	segErr error

	// arenaFree recycles the 128MiB chunk arenas across all of the
	// session's segment sorters (see spillArenaFreeList).
	arenaFree *spillArenaFreeList

	now        *timestamppb.Timestamp
	keyScratch []byte
	valScratch []byte
	srcScratch batonGrant.Sources
	rec        *v3.GrantRecord // reused across rows; see fillSynthGrantRecord
	rows       int64
}

func (s *synthGrantLayerSession) setErr(err error) {
	s.segMu.Lock()
	if s.segErr == nil {
		s.segErr = err
	}
	s.segMu.Unlock()
}

func (s *synthGrantLayerSession) takeErr() error {
	s.segMu.Lock()
	defer s.segMu.Unlock()
	return s.segErr
}

func (s *synthGrantLayerSession) segName() string {
	return fmt.Sprintf("synth-grant-layer-seg%03d", s.segIdx)
}

// cutSegment finalizes the current sorter into a segment for the worker and
// starts a fresh sorter for the next segment. Blocks when the worker is more
// than one segment behind.
func (s *synthGrantLayerSession) cutSegment() error {
	chunks, err := s.sorter.finalize()
	if err != nil {
		return err
	}
	name := s.segName()
	s.segIdx++
	s.segRows = 0
	s.sorter = newSpillSorter(s.dir, s.segName(), s.sortSem, deferredIndexSpillChunkBytes)
	s.sorter.free = s.arenaFree
	if len(chunks) == 0 {
		return nil
	}
	s.segCh <- synthLayerSegment{name: name, chunks: chunks}
	return nil
}

// BeginSynthesizedGrantLayer opens a layer-scoped layer session. The ingested
// SSTs carry primary rows only; the by_principal index is always rebuilt at
// EndSync, so no inline index maintenance is skipped by taking this path.
// The boolean is part of the store-level contract (non-Pebble stores report
// false and callers fall back to StoreNewExpandedGrantContributions).
func (e *Engine) BeginSynthesizedGrantLayer(ctx context.Context) (bool, error) {
	if err := e.checkWritable(); err != nil {
		return false, err
	}
	if err := e.requireCurrentSync(); err != nil {
		return false, err
	}
	e.synthLayerMu.Lock()
	defer e.synthLayerMu.Unlock()
	if e.synthLayer != nil {
		return false, errors.New("pebble: synthesized grant layer session already open")
	}
	e.synthLayer = &synthGrantLayerSession{
		now:      timestamppb.Now(),
		rec:      &v3.GrantRecord{},
		segLimit: synthLayerSegmentLimit(),
	}
	return true, nil
}

// loadSynthLayer returns the open layer session (or nil) under synthLayerMu.
func (e *Engine) loadSynthLayer() *synthGrantLayerSession {
	e.synthLayerMu.Lock()
	defer e.synthLayerMu.Unlock()
	return e.synthLayer
}

// takeSynthLayer detaches and returns the open layer session (or nil) under
// synthLayerMu, so Finish/Abort can tear it down without racing each other
// or Close.
func (e *Engine) takeSynthLayer() *synthGrantLayerSession {
	e.synthLayerMu.Lock()
	defer e.synthLayerMu.Unlock()
	s := e.synthLayer
	e.synthLayer = nil
	return s
}

// initSynthLayerSession lazily allocates the session's temp dir, first
// sorter, and background merge+ingest worker on the first Add.
func (e *Engine) initSynthLayerSession(ctx context.Context, s *synthGrantLayerSession) error {
	dir, err := e.prepareStagingDir("", "pebble-synth-grant-layer-")
	if err != nil {
		return fmt.Errorf("synth grant layer: mkdir temp: %w", err)
	}
	s.dir = dir
	sorters := min(4, max(2, runtime.GOMAXPROCS(0)/2))
	s.sortSem = make(chan struct{}, sorters)
	s.arenaFree = newSpillArenaFreeList(deferredIndexSpillChunkBytes, sorters+2)
	// deferredIndexSpillChunkBytes, not bulkSpillKeyChunkBytes: like the
	// deferred index build, the layer session is one producer with one live
	// sorter carrying full grant records. 8MiB chunks turned one whale layer
	// into a 3,400-way merge with ~3.4GB of read buffers; 128MiB chunks keep
	// each segment's merge fan-in small.
	s.sorter = newSpillSorter(dir, s.segName(), s.sortSem, deferredIndexSpillChunkBytes)
	s.sorter.free = s.arenaFree
	s.segCh = make(chan synthLayerSegment, 1)
	s.segWG.Add(1)
	go func() {
		defer s.segWG.Done()
		for seg := range s.segCh {
			// After a failure (or abort), drain remaining segments without
			// touching the DB; Finish surfaces the stored error.
			if s.takeErr() != nil {
				continue
			}
			if err := e.ingestSynthLayerSegment(ctx, s.dir, seg); err != nil {
				s.setErr(err)
			}
		}
	}()
	return nil
}

// ingestSynthLayerSegment merges one segment's sorted chunks into an SST and
// ingests it. Chunk files are deleted once merged; the SST path is left for
// the session's final dir cleanup (Pebble links/copies it on ingest).
//
// Runs on the session's background worker, which deliberately bypasses the
// engine write barrier (an Add holding writeMu can block on the bounded
// segment channel waiting for this worker, so worker-takes-writeMu would
// deadlock). The db itself stays valid for the worker's whole life — Close
// drains the worker via AbortSynthesizedGrantLayer before tearing the db
// down — but CheckpointTo's Flush→Checkpoint→WAL-truncate window must not
// see an ingest land in the middle: pebble's flushable-ingest path writes a
// WAL record referencing the ingested SSTs, and truncateCheckpointWALs would
// discard it from the snapshot. checkpointMu is that barrier.
func (e *Engine) ingestSynthLayerSegment(ctx context.Context, dir string, seg synthLayerSegment) error {
	sstPath := filepath.Join(dir, seg.name+".sst")
	if err := mergeSortedSpillChunksToSST(ctx, e.fs(), sstPath, seg.name, seg.chunks); err != nil {
		return err
	}
	for _, chunk := range seg.chunks {
		_ = os.Remove(chunk)
	}
	e.checkpointMu.RLock()
	defer e.checkpointMu.RUnlock()
	if err := e.db.IngestSSTs(ctx, []string{sstPath}); err != nil {
		return fmt.Errorf("synth grant layer: ingest segment %s: %w", seg.name, err)
	}
	return nil
}

// AddSynthesizedGrantLayerContributions encodes records into the open layer
// session. Rows become readable as their segment is ingested; callers must
// not rely on visibility before FinishSynthesizedGrantLayer returns.
func (e *Engine) AddSynthesizedGrantLayerContributions(ctx context.Context, records []synthesizedGrantRecord) error {
	if len(records) == 0 {
		return nil
	}
	return e.withWrite(func() error {
		s := e.loadSynthLayer()
		if s == nil {
			return errors.New("pebble: no open synthesized grant layer session")
		}
		if err := s.takeErr(); err != nil {
			return err
		}
		if s.sorter == nil {
			// Segment SSTs carry primary rows only; make sure EndSync builds
			// the deferred by_principal index even if no batch write set the
			// flag. Arm the marker BEFORE initializing the session: if the
			// marker lands but init fails, the worst case is a spurious
			// (cheap) rebuild at EndSync. The reverse order could leave an
			// initialized session whose later Adds skip this block, ingesting
			// rows with the rebuild unarmed.
			if err := e.db.ArmDeferredGrantIndex(); err != nil {
				return err
			}
			if err := e.initSynthLayerSession(ctx, s); err != nil {
				return err
			}
		}
		e.synthesizedWriteCalls.Add(1)
		e.synthesizedWriteRows.Add(int64(len(records)))
		for i := range records {
			rec := &records[i]
			if rec.entitlement == nil || rec.principal == nil || len(rec.sources) == 0 {
				continue
			}
			s.keyScratch = appendGrantIdentityKey(s.keyScratch[:0], rec.id)
			// external_id (field 2) is hand-encoded FIRST — it is the
			// lowest-numbered field these records carry, so emitting it
			// before the base marshal preserves canonical field order
			// without materializing a per-row concat string.
			s.valScratch = appendSynthGrantExternalIDWire(s.valScratch[:0], rec)
			fillSynthGrantRecord(s.rec, rec, s.now)
			val, err := marshalRecordAppend(s.valScratch, s.rec)
			if err != nil {
				return err
			}
			// sources (field 9) is the record's highest field, so appending
			// it after the base marshal matches the deterministic byte order.
			val, s.srcScratch = appendGrantSourcesWire(val, s.srcScratch, rec.sources)
			s.valScratch = val
			if err := s.sorter.add(s.keyScratch, val); err != nil {
				return err
			}
			s.rows++
			s.segRows++
		}
		if s.segLimit > 0 && s.segRows >= s.segLimit {
			if err := s.cutSegment(); err != nil {
				return err
			}
		}
		return nil
	})
}

// FinishSynthesizedGrantLayer flushes the session's tail segment, waits for
// the background worker to merge and ingest every queued segment, and closes
// the session. No-op if no session is open or the session saw no rows.
func (e *Engine) FinishSynthesizedGrantLayer(ctx context.Context) error {
	return e.withWrite(func() error {
		s := e.takeSynthLayer()
		if s == nil {
			return nil
		}
		if s.dir != "" {
			defer e.removeStagingDir(s.dir)
		}
		if s.sorter == nil {
			return nil
		}
		var finishErr error
		chunks, err := s.sorter.finalize()
		if err != nil {
			finishErr = err
		} else if len(chunks) > 0 {
			s.segCh <- synthLayerSegment{name: s.segName(), chunks: chunks}
		}
		close(s.segCh)
		s.segWG.Wait()
		if err := s.takeErr(); err != nil && finishErr == nil {
			finishErr = err
		}
		return finishErr
	})
}

// AbortSynthesizedGrantLayer discards an in-flight layer session: already
// ingested segments remain in the DB (their rows are idempotent overwrites on
// retry), staged chunks are dropped. Safe to call with no open session.
//
// Deliberately does NOT take the engine write barrier — Close calls it after
// setting the closing flag (withWrite would refuse), and it must stay callable
// as a cleanup path when a writer holding writeMu panicked. The synthLayerMu
// take keeps the pointer handoff race-free against Begin/Add/Finish/Close.
func (e *Engine) AbortSynthesizedGrantLayer(ctx context.Context) error {
	s := e.takeSynthLayer()
	if s == nil {
		return nil
	}
	if s.sorter != nil {
		s.sorter.abort()
	}
	if s.segCh != nil {
		// Make the worker drain queued segments without ingesting them.
		s.setErr(errors.New("pebble: synthesized grant layer session aborted"))
		close(s.segCh)
		s.segWG.Wait()
	}
	if s.dir != "" {
		e.removeStagingDir(s.dir)
	}
	return nil
}

func (e *Engine) putSynthesizedGrantContributionsBatch(ctx context.Context, records []synthesizedGrantRecord) error {
	return e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
		defer batch.Close()

		now := timestamppb.Now()
		var keyScratch, valScratch []byte
		var srcScratch batonGrant.Sources
		r := &v3.GrantRecord{} // reused across rows; see fillSynthGrantRecord
		for i := range records {
			rec := &records[i]
			if rec.entitlement == nil || rec.principal == nil || len(rec.sources) == 0 {
				continue
			}
			keyScratch = appendGrantIdentityKey(keyScratch[:0], rec.id)
			// external_id first; see AddSynthesizedGrantLayerContributions.
			valScratch = appendSynthGrantExternalIDWire(valScratch[:0], rec)
			fillSynthGrantRecord(r, rec, now)
			val, err := marshalRecordAppend(valScratch, r)
			if err != nil {
				return err
			}
			// sources (field 9) is the record's highest field, so appending
			// it after the base marshal matches the deterministic byte order.
			val, srcScratch = appendGrantSourcesWire(val, srcScratch, rec.sources)
			valScratch = val
			// Deferred regime: synthesized grants are brand-new (no prior
			// row) and never expandable (needsExpansion=false).
			if err := batch.StageGrantPutDeferred(keyScratch, val, false, false); err != nil {
				return err
			}
		}
		if e.test.recordCommitHook != nil {
			if err := e.test.recordCommitHook(); err != nil {
				return err
			}
		}
		// One atomic commit: rows and their obligations ride the same
		// batch, so a primary commit landing without its index entries
		// is unexpressible.
		return batch.Commit(pebble.NoSync)
	})
}

// UnsafePutUniqueGrantRecords is the trusted-import write path: it writes
// records unconditionally, with NO read-before-write and NO dedup pass. Do not
// use it for live connector output. The engine must currently be in fresh-sync
// mode, and the caller must guarantee each external_id appears at most once
// across the whole sync (not just within this batch). Primary + index key/value
// encoding — including the proto marshal — runs in parallel across GOMAXPROCS
// workers; a single goroutine then stages the pre-encoded bytes into a single
// RecordBatch and commits it (NoSync during a fresh sync).
//
// Unlike PutGrantRecords this skips the per-record db.Get that PutGrantRecords
// performs on every batch after the first of a fresh sync. That read-before-
// write only exists to clean up stale index entries when an external_id is
// rewritten within a sync — impossible when the caller guarantees global
// uniqueness for the imported sync.
func (e *Engine) UnsafePutUniqueGrantRecords(ctx context.Context, records ...*v3.GrantRecord) error {
	if len(records) == 0 {
		return nil
	}
	return e.withWrite(func() error {
		if !e.IsFreshSync() {
			return errors.New("UnsafePutUniqueGrantRecords: sync is not fresh")
		}
		// Fail-loud defense (review suggestion): on the fresh syncs this
		// path requires, digest state is provably absent (StartNewSync's
		// ResetForNewSync excised it), so the typed ops' digest
		// invalidation is a no-op. If digests ever read as present here,
		// the freshness contract itself is broken — refuse rather than
		// silently tombstone the digest keyspace of a trusted import.
		if e.db.GrantDigestsPresent() {
			return errors.New("UnsafePutUniqueGrantRecords: digest state present on a fresh sync; freshness contract violated")
		}
		// Consume the fresh-grants-empty bit: the keyspace is no longer
		// provably empty, so a subsequent PutGrantRecords in this sync must
		// take its read-before-write path to clean up index entries on
		// overwrites of identities written here.
		_ = e.takeFreshGrantsEmpty()

		type encoded struct {
			priKey         []byte
			priVal         []byte
			needsExpansion bool
		}
		enc := make([]encoded, len(records))

		workers := runtime.GOMAXPROCS(0)
		if workers < 1 {
			workers = 1
		}
		chunk := (len(records) + workers - 1) / workers
		var (
			wg     sync.WaitGroup
			encErr error
			errMu  sync.Mutex
			failed atomic.Bool
		)
		for w := 0; w < workers; w++ {
			lo := w * chunk
			hi := lo + chunk
			if hi > len(records) {
				hi = len(records)
			}
			if lo >= hi {
				break
			}
			wg.Add(1)
			go func(lo, hi int) {
				defer wg.Done()
				for i := lo; i < hi; i++ {
					if failed.Load() {
						return
					}
					r := records[i]
					if r == nil {
						continue
					}
					val, err := marshalRecord(r)
					if err != nil {
						errMu.Lock()
						if encErr == nil {
							encErr = err
						}
						errMu.Unlock()
						failed.Store(true)
						return
					}
					id, err := grantIdentityFromRecord(r)
					if err != nil {
						errMu.Lock()
						if encErr == nil {
							encErr = err
						}
						errMu.Unlock()
						failed.Store(true)
						return
					}
					enc[i] = encoded{
						priKey:         encodeGrantIdentityKey(id),
						priVal:         val,
						needsExpansion: r.GetNeedsExpansion(),
					}
				}
			}(lo, hi)
		}
		wg.Wait()
		if encErr != nil {
			return encErr
		}

		batch := e.db.NewRecordBatch()
		defer batch.Close()
		for i := range enc {
			if enc[i].priKey == nil {
				continue
			}
			// Inline regime, brand-new rows (caller-guaranteed unique):
			// the typed op splices index keys from the primary key on the
			// staging goroutine — cheaper than the encode-from-identity
			// the parallel workers used to do, and unforgettable.
			if err := batch.StageGrantPutInline(enc[i].priKey, enc[i].priVal, false, enc[i].needsExpansion); err != nil {
				return err
			}
		}

		opts := writeOpts(e.opts.durability)
		if e.IsFreshSync() {
			opts = pebble.NoSync
		}
		// One atomic commit: rows and their obligations ride the same
		// batch, so a primary commit landing without its index entries
		// is unexpressible.
		return batch.Commit(opts)
	})
}

// GetGrantRecord fetches a grant record by its raw public id via the
// bare-id lookup (candidate-split probing, exactly-one rule — lookup.go).
func (e *Engine) GetGrantRecord(ctx context.Context, externalID string) (*v3.GrantRecord, error) {
	id, err := e.resolveGrantIdentityByExternalID(ctx, externalID)
	if err != nil {
		return nil, err
	}
	key := encodeGrantIdentityKey(id)
	val, closer, err := e.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	r := &v3.GrantRecord{}
	if err := unmarshalRecord(val, r); err != nil {
		return nil, fmt.Errorf("GetGrantRecord: unmarshal: %w", err)
	}
	return r, nil
}

// DeleteGrantRecord removes a grant and its index entries by raw public id.
// A missing/unresolvable id is a no-op; an ambiguous id is an error (a
// lossy string must never guess a delete).
func (e *Engine) DeleteGrantRecord(ctx context.Context, externalID string) error {
	return e.withWrite(func() error {
		id, err := e.resolveGrantIdentityByExternalID(ctx, externalID)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil
			}
			return err
		}
		return e.deleteGrantByIdentityLocked(id)
	})
}

// DeleteGrantByIdentityRefs removes a grant addressed by its structural
// refs — the exact delete path for callers that hold the full grant. No
// lossy id string is involved. Deleting a non-existent grant is a no-op.
func (e *Engine) DeleteGrantByIdentityRefs(ctx context.Context, r *v3.GrantRecord) error {
	id, err := grantIdentityFromRecord(r)
	if err != nil {
		return fmt.Errorf("DeleteGrantByIdentityRefs: %w", err)
	}
	return e.withWrite(func() error {
		return e.deleteGrantByIdentityLocked(id)
	})
}

// deleteGrantByIdentityLocked deletes one grant row and its index entries.
// Caller holds the engine write lock (withWrite).
func (e *Engine) deleteGrantByIdentityLocked(id grantIdentity) error {
	key := encodeGrantIdentityKey(id)

	// Existence probe only: delete of non-existent stays a no-op, and
	// the typed op derives all cleanup keys from the primary key.
	_, closer, err := e.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil
		}
		return err
	}
	closer.Close()

	batch := e.db.NewRecordBatch()
	defer batch.Close()
	if err := batch.StageGrantDelete(key); err != nil {
		return err
	}
	return batch.Commit(writeOpts(e.opts.durability))
}

// grantIndexKeys returns the secondary-index keys for r. Folded families
// (by_entitlement, by_entitlement_resource, and by_principal_resource_type) are
// served by primary or by_principal prefix scans and are deliberately not
// written.
func grantIndexKeys(r *v3.GrantRecord) [][]byte {
	id, err := grantIdentityFromRecord(r)
	if err != nil {
		return nil
	}
	keys := make([][]byte, 0, 2)
	keys = append(keys, encodeGrantByPrincipalIdentityIndexKey(id))
	if r.GetNeedsExpansion() {
		keys = append(keys, encodeGrantByNeedsExpansionIdentityIndexKey(id))
	}
	return keys
}

// NOTE (2b): the per-path index/invalidation helper quartet
// (writeGrantIndexes, writeGrantIndexesScratch,
// writeGrantIndexesForIdentityScratch, deleteGrantIndexesScratch,
// deleteGrantIndexesRaw) is GONE. Their obligations moved into rawdb's
// typed record ops (StageGrantPutInline / StageGrantPutDeferred /
// StageGrantDelete), which derive index keys from the primary key by
// pinned byte splices and stage digest invalidation + the deferred
// marker through the Open-wired RecordDerivers — a caller can no
// longer stage a grant row and forget what it owes.

// The deferred-marker arm/clear contract (CAS + durable key agreement
// on both edges) lives on rawdb (ArmDeferredGrantIndex /
// ClearDeferredGrantIndexMarker): the marker is write-side crash
// state the typed record ops consume directly.

// clearDeferredIdxPending drops both forms of the marker after a
// successful rebuild, under the engine write barrier so the clear
// can't interleave with a concurrent writer's arm (which would leave
// the atomic flag armed but the durable marker deleted, or vice
// versa). Ordering and failure semantics live in rawdb's
// ClearDeferredGrantIndexMarker.
func (e *Engine) clearDeferredIdxPending() error {
	// AllowSealed: runs inside EndSync's sealed finalize window, right
	// after the deferred build (see Adapter.EndSync).
	return e.withWriteAllowSealed(func() error {
		return e.db.ClearDeferredGrantIndexMarker()
	})
}

// IterateGrants iterates all grants in primary-key order. yield returns
// false to stop iteration.
func (e *Engine) IterateGrants(ctx context.Context, yield func(*v3.GrantRecord) bool) error {
	prefix := encodeGrantPrefix()
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		r := &v3.GrantRecord{}
		if err := unmarshalRecord(iter.Value(), r); err != nil {
			return fmt.Errorf("iterate grants: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

// IterateGrantsByEntitlement iterates the primary grant keyspace for the given
// raw entitlement id, yielding each grant in encoded principal-key order.
// The id resolves through the bare-id lookup; an id matching no entitlement
// iterates nothing. yield returns false to stop.
func (e *Engine) IterateGrantsByEntitlement(ctx context.Context, entitlementID string, yield func(*v3.GrantRecord) bool) error {
	entID, err := e.resolveGrantScanEntitlementIdentity(ctx, entitlementID)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil
		}
		return err
	}
	indexPrefix := encodeGrantPrimaryEntitlementPrefix(entID)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		r := &v3.GrantRecord{}
		if err := unmarshalRecord(iter.Value(), r); err != nil {
			return fmt.Errorf("iterate by entitlement: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

// IterateGrantsByPrincipal iterates the by_principal index.
func (e *Engine) IterateGrantsByPrincipal(ctx context.Context, principalRT, principalID string, yield func(*v3.GrantRecord) bool) error {
	indexPrefix := encodeGrantByPrincipalPrefix(principalRT, principalID)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		components, ok := decodeTupleComponents(iter.Key(), indexPrefix, 4)
		if !ok {
			continue
		}
		r, err := getGrantByIdentity(ctx, e.db, grantIdentity{
			entitlement: entitlementIdentity{
				resourceTypeID: components[0],
				resourceID:     components[1],
				stripped:       components[2] == idFlagStripped,
				tail:           components[3],
			},
			principalTypeID: principalRT,
			principalID:     principalID,
		})
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return err
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

// IterateGrantsByPrincipalResourceType iterates the by_principal index narrowed
// to a principal resource type. Yields each grant whose principal carries the
// given resource_type. Stops when yield returns false.
func (e *Engine) IterateGrantsByPrincipalResourceType(ctx context.Context, principalRT string, yield func(*v3.GrantRecord) bool) error {
	indexPrefix := encodeGrantByPrincipalResourceTypeIdentityPrefix(principalRT)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		components, ok := decodeTupleComponents(iter.Key(), indexPrefix, 5)
		if !ok {
			continue
		}
		r, err := getGrantByIdentity(ctx, e.db, grantIdentity{
			entitlement: entitlementIdentity{
				resourceTypeID: components[1],
				resourceID:     components[2],
				stripped:       components[3] == idFlagStripped,
				tail:           components[4],
			},
			principalTypeID: principalRT,
			principalID:     components[0],
		})
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return fmt.Errorf("iterate by principal_rt: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

// IterateGrantsByNeedsExpansion iterates the needs_expansion index,
// yielding each grant whose NeedsExpansion flag is currently set.
// yield returns false to stop.
//
// Pebble-equivalent of the SQLite partial index
// `WHERE needs_expansion = 1`. Backs PendingExpansionPage on the
// grant store.
func (e *Engine) IterateGrantsByNeedsExpansion(ctx context.Context, yield func(*v3.GrantRecord) bool) error {
	indexPrefix := encodeGrantByNeedsExpansionPrefix()
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		components, ok := decodeTupleComponents(iter.Key(), indexPrefix, 6)
		if !ok {
			continue
		}
		r, err := getGrantByIdentity(ctx, e.db, grantIdentity{
			entitlement: entitlementIdentity{
				resourceTypeID: components[0],
				resourceID:     components[1],
				stripped:       components[2] == idFlagStripped,
				tail:           components[3],
			},
			principalTypeID: components[4],
			principalID:     components[5],
		})
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return fmt.Errorf("iterate needs_expansion: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

// Tuple-component decoders for index-key tails. Thin wrappers over
// codec.DecodeTupleStringAlias so the engine and the canonical codec
// share one implementation of the escape rules, and the no-escape
// common path avoids the intermediate []byte allocation before the
// string conversion. The previous hand-rolled versions silently
// swallowed malformed escape sequences (returning truncated strings);
// these report decode failure by returning the zero value, which iter
// callers treat as "skip this entry" — same observable behavior on
// well-formed keys, fail-safe on corruption.

// decodeTwoTupleComponents decodes the first two tuple-encoded string
// components from an index key relative to its prefix. Components are
// separated by a single 0x00 byte; intra-component NUL/0x01 bytes are
// escape-encoded per codec.AppendTupleString. Returns (a, b, true) on
// success or ("", "", false) if the tail is empty, malformed, or has
// fewer than two components.
func decodeTwoTupleComponents(key, prefix []byte) (string, string, bool) {
	components, ok := decodeTupleComponents(key, prefix, 2)
	if !ok {
		return "", "", false
	}
	return components[0], components[1], true
}

func decodeGrantIdentityKey(key []byte) (grantIdentity, bool) {
	prefix := []byte{versionV3, typeGrant, 0x00}
	return decodeGrantIdentityTail(key, prefix)
}

func decodeTupleComponents(key, prefix []byte, want int) ([]string, bool) {
	if want <= 0 || len(key) <= len(prefix) {
		return nil, false
	}
	tail := key[len(prefix):]
	out := make([]string, 0, want)
	off := 0
	for off <= len(tail) && len(out) < want {
		decoded, next, ok := codec.DecodeTupleStringAlias(tail, off)
		if !ok {
			return nil, false
		}
		out = append(out, string(decoded))
		if next >= len(tail) {
			break
		}
		off = next + 1
	}
	if len(out) != want {
		return nil, false
	}
	return out, true
}

func decodeGrantIdentityTail(key, prefix []byte) (grantIdentity, bool) {
	if len(key) <= len(prefix) {
		return grantIdentity{}, false
	}
	tail := key[len(prefix):]
	off := 0
	nextString := func() (string, bool) {
		decoded, next, ok := codec.DecodeTupleStringAlias(tail, off)
		if !ok {
			return "", false
		}
		off = next + 1
		return string(decoded), true
	}
	entRT, ok := nextString()
	if !ok {
		return grantIdentity{}, false
	}
	entRID, ok := nextString()
	if !ok {
		return grantIdentity{}, false
	}
	entFlag, ok := nextString()
	if !ok {
		return grantIdentity{}, false
	}
	entTail, ok := nextString()
	if !ok {
		return grantIdentity{}, false
	}
	principalRT, ok := nextString()
	if !ok {
		return grantIdentity{}, false
	}
	principalID, ok := nextString()
	if !ok {
		return grantIdentity{}, false
	}
	return grantIdentity{
		entitlement: entitlementIdentity{
			resourceTypeID: entRT,
			resourceID:     entRID,
			stripped:       entFlag == idFlagStripped,
			tail:           entTail,
		},
		principalTypeID: principalRT,
		principalID:     principalID,
	}, true
}
