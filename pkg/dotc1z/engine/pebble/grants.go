package pebble

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// PutGrantRecord writes a grant record + its by_entitlement and
// by_principal index entries, atomically in a single pebble.Batch.
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

// PutGrantRecords writes N grants in a single pebble.Batch, fsyncing
// once at the end. This is the bulk path the adapter's PutGrants uses.
//
// Read-before-write index cleanup runs unconditionally — even during
// a fresh sync, a connector that emits the same external_id twice (a
// real bug class, e.g. paginated source where the same record appears
// on two pages) would otherwise leak orphan index entries pointing at
// stale entitlement/principal values. The Get cost is small (memtable
// hit during a sync's hot phase) and the integrity guarantee is worth
// it.
//
// Fresh-sync still uses pebble.NoSync for batch commits — that's the
// larger perf win (skips per-batch fsync). EndFreshSync does one
// Flush+fsync at sync end to harden the data.
func (e *Engine) PutGrantRecords(ctx context.Context, records ...*v3.GrantRecord) error {
	if len(records) == 0 {
		return nil
	}
	return e.withWrite(func() error {
		// Two batches: primaries vs indexes. Pebble's flushable-batch
		// promotion (triggered when batch > MemTableSize) sort.Sorts every
		// entry to make the batch behave like a memtable. When the input
		// is already key-sorted, pdqsort short-circuits to nearly O(N).
		// Connector grants typically arrive in external_id order (paginated
		// sources walk in stable order), so the primary key stream
		// (v3|G|sync|external_id) is nearly always pre-sorted by
		// construction. Splitting the index writes (which interleave
		// unsorted entitlement_id / principal_id) into a separate batch
		// lets the primary batch's sort take that fast path. Indexes are
		// still committed atomically per-batch; cross-batch consistency
		// for fresh-sync is acceptable because fresh-sync replays from the
		// connector on crash.
		// Pre-size the batches. Each grant contributes ~600 bytes to the
		// primary batch (key + marshaled GrantRecord value + entry header)
		// and ~140 bytes to the index batch (2 index keys + headers).
		// Pre-sizing avoids the internal grow-by-2x reallocations Pebble
		// would otherwise do as the batch fills. NewBatchWithSize accepts
		// the hint as a starting allocation size.
		priBatch := e.db.NewBatchWithSize(len(records) * 600)
		defer priBatch.Close()
		idxBatch := e.db.NewBatchWithSize(len(records) * 140)
		defer idxBatch.Close()

		fresh := e.IsFreshSync()
		// Skip-Get fast path. When this is the first PutGrantRecords call
		// in a fresh sync, the grant keyspace for this sync is provably
		// empty (MarkFreshSync just created it). The per-record
		// e.db.Get(key) cannot find anything — querying the DB returns
		// ErrNotFound for every record, and the batch's pending writes
		// aren't visible to db.Get until Commit. So 1M point lookups are
		// pure overhead in this case. Subsequent PutGrantRecords calls
		// within the same sync still need the Get for cross-call
		// duplicate detection (e.g. paginated connector sources emitting
		// the same external_id on two pages). NB: within-call duplicates
		// are still NOT detected here — a connector that emits the same
		// external_id twice in one PutGrants call already leaves stale
		// indexes under the previous code, so this is no behavioral change.
		skipGet := e.takeFreshGrantsEmpty()

		// Parallel-build threshold. Below this, goroutine setup (~5 µs
		// per goroutine + sync.WaitGroup) exceeds the loop-body savings;
		// the sequential path wins for tiny batches.
		const parallelMinRecords = 256

		switch {
		case skipGet && len(records) >= parallelMinRecords:
			// Parallel build path: with no read-before-write Get, the two
			// batches have no shared mutable state — priBatch holds only
			// primary writes, idxBatch holds only fresh index writes (no
			// old-index Deletes since skipGet implies provably-empty
			// keyspace). We can build them on two goroutines in parallel,
			// roughly halving the loop-body wallclock on a multi-core host.
			// Each goroutine has its own scratch buffers and its own
			// per-call sync_id cache. The proto.Marshal of GrantRecord is
			// concurrent-safe (read-only on the message); the appendGrantKey
			// / appendGrantBy*IndexKey encoders are pure functions.
			var wg sync.WaitGroup
			var priErr, idxErr error
			wg.Add(2)
			go func() {
				defer wg.Done()
				priErr = e.buildPriBatchSkipGet(priBatch, records)
			}()
			go func() {
				defer wg.Done()
				idxErr = e.buildIdxBatchSkipGet(idxBatch, records)
			}()
			wg.Wait()
			if priErr != nil {
				return priErr
			}
			if idxErr != nil {
				return idxErr
			}
		case skipGet:
			// Below the parallel threshold, but still skipGet — use the
			// sequential path without the Get to retain the skip-Get win.
			if err := e.buildBothBatchesSequentialSkipGet(priBatch, idxBatch, records); err != nil {
				return err
			}
		default:
			if err := e.buildBothBatchesSequential(priBatch, idxBatch, records); err != nil {
				return err
			}
		}
		opts := writeOpts(e.opts.durability)
		if fresh {
			opts = pebble.NoSync
		}
		if err := priBatch.Commit(opts); err != nil {
			return err
		}
		if err := idxBatch.Commit(opts); err != nil {
			return err
		}
		if skipGet {
			// Once any grant batch commits, future PutGrantRecords calls in
			// the same fresh sync need the Get for cross-call dup detection.
			e.clearFreshGrantsEmpty()
		}
		return nil
	})
}

// buildPriBatchSkipGetIntoLocal is the per-shard worker for the
// parallel-build path. Each goroutine writes Sets into its own private
// pebble.Batch (no shared state), which the caller then Applies into the
// final priBatch.
func (e *Engine) buildPriBatchSkipGetIntoLocal(local *pebble.Batch, records []*v3.GrantRecord) error {
	keyBuf := make([]byte, 0, 64)
	valBuf := make([]byte, 0, 512)
	marshalOpts := proto.MarshalOptions{Deterministic: true}
	var (
		lastSyncIDStr string
		lastIDBytes   []byte
		haveLast      bool
	)
	for _, r := range records {
		if r == nil {
			continue
		}
		var (
			idBytes []byte
			err     error
		)
		if sid := r.GetSyncId(); haveLast && sid == lastSyncIDStr {
			idBytes = lastIDBytes
		} else {
			idBytes, err = e.resolveSyncBytes(sid)
			if err != nil {
				return err
			}
			lastSyncIDStr = sid
			lastIDBytes = idBytes
			haveLast = true
		}
		keyBuf = appendGrantKey(keyBuf[:0], idBytes, r.GetExternalId())
		valBuf, err = marshalOpts.MarshalAppend(valBuf[:0], r)
		if err != nil {
			return err
		}
		if err := local.Set(keyBuf, valBuf, nil); err != nil {
			return err
		}
	}
	return nil
}

// buildPriBatchSkipGet shards records across N goroutines, each building
// a local pebble.Batch. The resulting batches are Apply'd into the final
// priBatch in order. proto.Marshal of GrantRecords is the long pole on
// the parallel-build path; sharding it across cores cuts wallclock on
// the goroutine A side roughly N×, at the cost of an Apply memcpy
// (~530 MB total for 1M records, ≈50 ms at 10 GB/s).
func (e *Engine) buildPriBatchSkipGet(priBatch *pebble.Batch, records []*v3.GrantRecord) error {
	// Heuristic: shard count = min(4, runtime.GOMAXPROCS/4). On a
	// 16-core box this is 4. Each shard handles ~250k records of ~530 B
	// for 1M-grant workloads. Below ~1000 records per shard the Apply
	// memcpy overhead dominates, so bail to the single-pass path.
	const targetShards = 4
	const minShardSize = 1024

	shards := targetShards
	if n := len(records) / minShardSize; n < shards {
		shards = n
	}
	if shards < 2 {
		return e.buildPriBatchSkipGetIntoLocal(priBatch, records)
	}

	// Build sub-batches in parallel.
	locals := make([]*pebble.Batch, shards)
	errs := make([]error, shards)
	var wg sync.WaitGroup
	wg.Add(shards)
	chunkSize := (len(records) + shards - 1) / shards
	for s := 0; s < shards; s++ {
		start := s * chunkSize
		end := start + chunkSize
		if end > len(records) {
			end = len(records)
		}
		s := s
		slice := records[start:end]
		local := e.db.NewBatchWithSize(len(slice) * 600)
		locals[s] = local
		go func() {
			defer wg.Done()
			errs[s] = e.buildPriBatchSkipGetIntoLocal(local, slice)
		}()
	}
	wg.Wait()
	defer func() {
		for _, l := range locals {
			if l != nil {
				_ = l.Close()
			}
		}
	}()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	// Concatenate into the final priBatch in shard order. Apply does a
	// single memcpy per call.
	for _, l := range locals {
		if err := priBatch.Apply(l, nil); err != nil {
			return err
		}
	}
	return nil
}

// buildIdxBatchSkipGetUnsortedInto emits both by_entitlement and
// by_principal index Sets to the given batch in record-iteration order
// (i.e. unsorted by key). Used as a fallback for small workloads where
// the parallel-sort+merge overhead isn't justified.
func (e *Engine) buildIdxBatchSkipGetUnsortedInto(batch *pebble.Batch, records []*v3.GrantRecord) error {
	idx1Buf := make([]byte, 0, 96)
	idx2Buf := make([]byte, 0, 96)
	var (
		lastSyncIDStr string
		lastIDBytes   []byte
		haveLast      bool
	)
	for _, r := range records {
		if r == nil {
			continue
		}
		var (
			idBytes []byte
			err     error
		)
		if sid := r.GetSyncId(); haveLast && sid == lastSyncIDStr {
			idBytes = lastIDBytes
		} else {
			idBytes, err = e.resolveSyncBytes(sid)
			if err != nil {
				return err
			}
			lastSyncIDStr = sid
			lastIDBytes = idBytes
			haveLast = true
		}
		idx1Buf, idx2Buf, err = appendGrantIndexes(batch, idBytes, r, idx1Buf[:0], idx2Buf[:0])
		if err != nil {
			return err
		}
	}
	return nil
}

// keyArena holds many variable-length keys in a single underlying
// []byte (data) with one (start, end) offset pair per key (bounds).
// Two GC objects regardless of how many keys — replaces a [][]byte of
// N entries (N+1 GC objects, 24*N bytes of slice headers) and lets the
// GC scanner skip the per-key walk. bounds is sortable in-place; the
// data arena stays put after sort.
type keyArena struct {
	data   []byte
	bounds [][2]uint32 // bounds[i] = [start, end) of key i in data
}

func (a *keyArena) key(i int) []byte {
	return a.data[a.bounds[i][0]:a.bounds[i][1]]
}

func (a *keyArena) keyCount() int { return len(a.bounds) }

// collectAndSortIdxKeys gathers all by_entitlement + by_principal
// index keys for the given record slice into a keyArena, then sorts
// bounds in place by bytewise key comparison. Used by the parallel-
// sort+merge idxBatch builder. The arena pattern matters at this scale
// because the prior [][]byte approach allocated ~500k slice headers
// per shard, which dominated GC scan cost (~670 ms in the v8 profile).
func (e *Engine) collectAndSortIdxKeys(records []*v3.GrantRecord) (*keyArena, error) {
	// Pre-size: typical keys are ~60 bytes. Pre-allocating avoids the
	// arena's grow-by-2x reallocs as it fills.
	const estKeyBytes = 80
	arena := &keyArena{
		data:   make([]byte, 0, 2*len(records)*estKeyBytes),
		bounds: make([][2]uint32, 0, 2*len(records)),
	}
	var (
		lastSyncIDStr string
		lastIDBytes   []byte
		haveLast      bool
	)
	for _, r := range records {
		if r == nil {
			continue
		}
		var (
			idBytes []byte
			err     error
		)
		if sid := r.GetSyncId(); haveLast && sid == lastSyncIDStr {
			idBytes = lastIDBytes
		} else {
			idBytes, err = e.resolveSyncBytes(sid)
			if err != nil {
				return nil, err
			}
			lastSyncIDStr = sid
			lastIDBytes = idBytes
			haveLast = true
		}
		ent := r.GetEntitlement()
		princ := r.GetPrincipal()
		ext := r.GetExternalId()
		// uint32 conversions are bounded: per-shard arena is at most
		// ~200 MiB worth of keys (well under 2³² bytes), and a single
		// PutGrants call won't hand us 4 billion records.
		if ent != nil && princ != nil {
			start := uint32(len(arena.data)) //nolint:gosec // bounded; see above
			arena.data = appendGrantByEntitlementIndexKey(arena.data,
				idBytes,
				ent.GetEntitlementId(),
				princ.GetResourceTypeId(),
				princ.GetResourceId(),
				ext,
			)
			arena.bounds = append(arena.bounds, [2]uint32{start, uint32(len(arena.data))}) //nolint:gosec // bounded
		}
		if princ != nil {
			start := uint32(len(arena.data)) //nolint:gosec // bounded
			arena.data = appendGrantByPrincipalIndexKey(arena.data,
				idBytes,
				princ.GetResourceTypeId(),
				princ.GetResourceId(),
				ext,
			)
			arena.bounds = append(arena.bounds, [2]uint32{start, uint32(len(arena.data))}) //nolint:gosec // bounded
		}
	}
	// Capture data once so the closure doesn't re-deref arena every cmp.
	data := arena.data
	slices.SortFunc(arena.bounds, func(x, y [2]uint32) int {
		return bytes.Compare(data[x[0]:x[1]], data[y[0]:y[1]])
	})
	return arena, nil
}

// buildIdxBatchSkipGet builds the index batch with entries in sorted
// key order, so the flushable-batch promotion sort (when batch >
// largeBatchThreshold ≈ 32 MiB) takes pdqsort's already-sorted
// short-circuit. The idx keys interleave by_entitlement (sorted by
// entitlement_id) and by_principal (sorted by principal_id, different
// from iteration order). Without pre-sorting, the flushable-batch
// sort over ~2M entries was the single largest remaining cost in the
// 1M-grant workload (≈630 ms in cmpbody+Less per profile).
//
// Strategy: shard records, each shard sorts its own ~500k keys in
// parallel (slices.SortFunc with bytes.Compare — pdqsort), then a
// 4-way merge writes them to idxBatch in global sort order. Cost ~250 ms
// wallclock vs ~630 ms saved on the flushable-batch sort. Memory
// peak: ~170 MB temporary (2M []byte key copies).
func (e *Engine) buildIdxBatchSkipGet(idxBatch *pebble.Batch, records []*v3.GrantRecord) error {
	const targetShards = 4
	const minShardSize = 1024

	shards := targetShards
	if n := len(records) / minShardSize; n < shards {
		shards = n
	}
	if shards < 2 {
		return e.buildIdxBatchSkipGetUnsortedInto(idxBatch, records)
	}

	// Step 1: shard records, each goroutine collects + sorts into its
	// own arena.
	type shardResult struct {
		arena *keyArena
		err   error
	}
	results := make([]shardResult, shards)
	var wg sync.WaitGroup
	wg.Add(shards)
	chunkSize := (len(records) + shards - 1) / shards
	for s := 0; s < shards; s++ {
		start := s * chunkSize
		end := start + chunkSize
		if end > len(records) {
			end = len(records)
		}
		s := s
		slice := records[start:end]
		go func() {
			defer wg.Done()
			arena, err := e.collectAndSortIdxKeys(slice)
			results[s].arena = arena
			results[s].err = err
		}()
	}
	wg.Wait()
	for _, r := range results {
		if r.err != nil {
			return r.err
		}
	}

	// Step 2: k-way merge across the per-shard sorted arenas into
	// idxBatch. Naive linear scan picks the min head among (at most 4)
	// shards — cheap and branch-predictable for k≤4 and avoids
	// container/heap allocation overhead.
	idx := make([]int, shards)
	counts := make([]int, shards)
	for s := 0; s < shards; s++ {
		counts[s] = results[s].arena.keyCount()
	}
	for {
		minShard := -1
		var minKey []byte
		for s := 0; s < shards; s++ {
			if idx[s] >= counts[s] {
				continue
			}
			k := results[s].arena.key(idx[s])
			if minShard == -1 || bytes.Compare(k, minKey) < 0 {
				minShard = s
				minKey = k
			}
		}
		if minShard == -1 {
			break // all shards drained
		}
		if err := idxBatch.Set(minKey, nil, nil); err != nil {
			return err
		}
		idx[minShard]++
	}
	return nil
}

// buildBothBatchesSequentialSkipGet is the small-batch fresh-sync path.
// Same as Sequential but without the per-record db.Get — cheaper for
// tiny batches where parallel goroutine setup would dominate.
func (e *Engine) buildBothBatchesSequentialSkipGet(priBatch, idxBatch *pebble.Batch, records []*v3.GrantRecord) error {
	keyBuf := make([]byte, 0, 64)
	idx1Buf := make([]byte, 0, 96)
	idx2Buf := make([]byte, 0, 96)
	valBuf := make([]byte, 0, 512)
	marshalOpts := proto.MarshalOptions{Deterministic: true}
	var (
		lastSyncIDStr string
		lastIDBytes   []byte
		haveLast      bool
	)
	for _, r := range records {
		if r == nil {
			continue
		}
		var (
			idBytes []byte
			err     error
		)
		if sid := r.GetSyncId(); haveLast && sid == lastSyncIDStr {
			idBytes = lastIDBytes
		} else {
			idBytes, err = e.resolveSyncBytes(sid)
			if err != nil {
				return err
			}
			lastSyncIDStr = sid
			lastIDBytes = idBytes
			haveLast = true
		}
		keyBuf = appendGrantKey(keyBuf[:0], idBytes, r.GetExternalId())
		valBuf, err = marshalOpts.MarshalAppend(valBuf[:0], r)
		if err != nil {
			return err
		}
		if err := priBatch.Set(keyBuf, valBuf, nil); err != nil {
			return err
		}
		idx1Buf, idx2Buf, err = appendGrantIndexes(idxBatch, idBytes, r, idx1Buf[:0], idx2Buf[:0])
		if err != nil {
			return err
		}
	}
	return nil
}

// buildBothBatchesSequential is the safe sequential path. Used when
// skipGet is false (resume / mid-sync put), so the per-record db.Get
// might find a prior record whose old indexes must be Deleted in
// idxBatch — not safe to interleave with parallel idx writes.
func (e *Engine) buildBothBatchesSequential(priBatch, idxBatch *pebble.Batch, records []*v3.GrantRecord) error {
	keyBuf := make([]byte, 0, 64)
	idx1Buf := make([]byte, 0, 96)
	idx2Buf := make([]byte, 0, 96)
	valBuf := make([]byte, 0, 512)
	marshalOpts := proto.MarshalOptions{Deterministic: true}
	var (
		lastSyncIDStr string
		lastIDBytes   []byte
		haveLast      bool
	)
	for _, r := range records {
		if r == nil {
			continue
		}
		var (
			idBytes []byte
			err     error
		)
		if sid := r.GetSyncId(); haveLast && sid == lastSyncIDStr {
			idBytes = lastIDBytes
		} else {
			idBytes, err = e.resolveSyncBytes(sid)
			if err != nil {
				return err
			}
			lastSyncIDStr = sid
			lastIDBytes = idBytes
			haveLast = true
		}
		keyBuf = appendGrantKey(keyBuf[:0], idBytes, r.GetExternalId())
		valBuf, err = marshalOpts.MarshalAppend(valBuf[:0], r)
		if err != nil {
			return err
		}
		oldVal, closer, getErr := e.db.Get(keyBuf)
		switch {
		case getErr == nil:
			old := &v3.GrantRecord{}
			if err := proto.Unmarshal(oldVal, old); err != nil {
				closer.Close()
				return fmt.Errorf("PutGrantRecords: unmarshal old %q: %w", r.GetExternalId(), err)
			}
			if err := e.deleteGrantIndexes(idxBatch, idBytes, old); err != nil {
				closer.Close()
				return err
			}
			closer.Close()
		case errors.Is(getErr, pebble.ErrNotFound):
			// no prior record — write unconditionally
		default:
			return fmt.Errorf("PutGrantRecords: get old: %w", getErr)
		}
		if err := priBatch.Set(keyBuf, valBuf, nil); err != nil {
			return err
		}
		idx1Buf, idx2Buf, err = appendGrantIndexes(idxBatch, idBytes, r, idx1Buf[:0], idx2Buf[:0])
		if err != nil {
			return err
		}
	}
	return nil
}

// appendGrantIndexes is the scratch-buffer variant of writeGrantIndexes,
// used by the hot PutGrantRecords path. Caller supplies two pre-truncated
// scratch buffers; returned buffers carry the underlying capacity for
// the next iteration.
func appendGrantIndexes(batch *pebble.Batch, syncIDBytes []byte, r *v3.GrantRecord, idx1, idx2 []byte) ([]byte, []byte, error) {
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()
	ext := r.GetExternalId()

	if ent != nil && princ != nil {
		idx1 = appendGrantByEntitlementIndexKey(idx1,
			syncIDBytes,
			ent.GetEntitlementId(),
			princ.GetResourceTypeId(),
			princ.GetResourceId(),
			ext,
		)
		if err := batch.Set(idx1, nil, nil); err != nil {
			return idx1, idx2, err
		}
	}
	if princ != nil {
		idx2 = appendGrantByPrincipalIndexKey(idx2,
			syncIDBytes,
			princ.GetResourceTypeId(),
			princ.GetResourceId(),
			ext,
		)
		if err := batch.Set(idx2, nil, nil); err != nil {
			return idx1, idx2, err
		}
	}
	return idx1, idx2, nil
}

// GetGrantRecord fetches a grant record by sync_id + external_id.
// syncID may be empty to use the engine's currently-set sync.
func (e *Engine) GetGrantRecord(ctx context.Context, syncID, externalID string) (*v3.GrantRecord, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, err
	}
	key := encodeGrantKey(idBytes, externalID)
	val, closer, err := e.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	r := &v3.GrantRecord{}
	if err := proto.Unmarshal(val, r); err != nil {
		return nil, fmt.Errorf("GetGrantRecord: unmarshal: %w", err)
	}
	return r, nil
}

// DeleteGrantRecord removes a grant and its index entries.
func (e *Engine) DeleteGrantRecord(ctx context.Context, syncID, externalID string) error {
	return e.withWrite(func() error {
		idBytes, err := e.resolveSyncBytes(syncID)
		if err != nil {
			return err
		}
		key := encodeGrantKey(idBytes, externalID)

		batch := e.db.NewBatch()
		defer batch.Close()

		oldVal, closer, err := e.db.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil // delete of non-existent is a no-op
			}
			return err
		}
		old := &v3.GrantRecord{}
		if err := proto.Unmarshal(oldVal, old); err != nil {
			closer.Close()
			return fmt.Errorf("DeleteGrantRecord: unmarshal old %q: %w", externalID, err)
		}
		{
			if err := e.deleteGrantIndexes(batch, idBytes, old); err != nil {
				closer.Close()
				return err
			}
		}
		closer.Close()

		if err := batch.Delete(key, nil); err != nil {
			return err
		}
		return batch.Commit(writeOpts(e.opts.durability))
	})
}

// writeGrantIndexes adds index entries for r to batch.
func (e *Engine) writeGrantIndexes(batch *pebble.Batch, syncIDBytes []byte, r *v3.GrantRecord) error {
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()
	ext := r.GetExternalId()

	if ent != nil && princ != nil {
		k := encodeGrantByEntitlementIndexKey(
			syncIDBytes,
			ent.GetEntitlementId(),
			princ.GetResourceTypeId(),
			princ.GetResourceId(),
			ext,
		)
		if err := batch.Set(k, nil, nil); err != nil {
			return err
		}
	}
	if princ != nil {
		k := encodeGrantByPrincipalIndexKey(
			syncIDBytes,
			princ.GetResourceTypeId(),
			princ.GetResourceId(),
			ext,
		)
		if err := batch.Set(k, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

// deleteGrantIndexes mirrors writeGrantIndexes but with batch.Delete.
func (e *Engine) deleteGrantIndexes(batch *pebble.Batch, syncIDBytes []byte, r *v3.GrantRecord) error {
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()
	ext := r.GetExternalId()

	if ent != nil && princ != nil {
		k := encodeGrantByEntitlementIndexKey(
			syncIDBytes,
			ent.GetEntitlementId(),
			princ.GetResourceTypeId(),
			princ.GetResourceId(),
			ext,
		)
		if err := batch.Delete(k, nil); err != nil {
			return err
		}
	}
	if princ != nil {
		k := encodeGrantByPrincipalIndexKey(
			syncIDBytes,
			princ.GetResourceTypeId(),
			princ.GetResourceId(),
			ext,
		)
		if err := batch.Delete(k, nil); err != nil {
			return err
		}
	}
	return nil
}

// IterateGrantsBySync iterates all grants in a sync in primary-key
// order. yield returns false to stop iteration.
func (e *Engine) IterateGrantsBySync(ctx context.Context, syncID string, yield func(*v3.GrantRecord) bool) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	prefix := encodeGrantPrefix(idBytes)
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
		if err := proto.Unmarshal(iter.Value(), r); err != nil {
			return fmt.Errorf("iterate grants: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

// IterateGrantsByEntitlement iterates the by_entitlement index for
// the given entitlement_id, yielding each grant in encoded principal-
// key order. yield returns false to stop.
func (e *Engine) IterateGrantsByEntitlement(ctx context.Context, syncID, entitlementID string, yield func(*v3.GrantRecord) bool) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	indexPrefix := encodeGrantByEntitlementPrefix(idBytes, entitlementID)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		// Index key tail is the grant's external_id. Need to decode it
		// to look up the primary record. The tail is the last
		// tuple-encoded component in the index key.
		externalID := lastTupleComponent(iter.Key(), indexPrefix)
		if externalID == "" {
			continue
		}
		key := encodeGrantKey(idBytes, externalID)
		val, closer, err := e.db.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				// Index entry references a primary that's gone — a
				// transient orphan during overwrite, or a real
				// consistency issue. Skip; fsck reconciles.
				continue
			}
			return err
		}
		r := &v3.GrantRecord{}
		err = proto.Unmarshal(val, r)
		closer.Close()
		if err != nil {
			return fmt.Errorf("iterate by entitlement: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

// IterateGrantsByPrincipal iterates the by_principal index.
func (e *Engine) IterateGrantsByPrincipal(ctx context.Context, syncID, principalRT, principalID string, yield func(*v3.GrantRecord) bool) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	indexPrefix := encodeGrantByPrincipalPrefix(idBytes, principalRT, principalID)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		externalID := lastTupleComponent(iter.Key(), indexPrefix)
		if externalID == "" {
			continue
		}
		key := encodeGrantKey(idBytes, externalID)
		val, closer, err := e.db.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return err
		}
		r := &v3.GrantRecord{}
		err = proto.Unmarshal(val, r)
		closer.Close()
		if err != nil {
			return err
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

// decodeTwoTupleComponents decodes the last two tuple-encoded string
// components from an index key relative to its prefix. The components
// are separated by a single 0x00 byte; embedded NULs in the components
// are escape-encoded per the tuple encoder. Returns (a, b, true) on
// success.
func decodeTwoTupleComponents(key, prefix []byte) (string, string, bool) {
	if len(key) <= len(prefix) {
		return "", "", false
	}
	tail := key[len(prefix):]
	// Find the separator between the two components. We can't just
	// scan for 0x00 because intra-component NULs are escaped — but in
	// the escape sequence 0x01 0x01 the 0x01 comes first, so a bare
	// 0x00 is always a separator.
	first := make([]byte, 0, len(tail)/2)
	i := 0
	for i < len(tail) {
		b := tail[i]
		if b == 0x00 {
			// Found separator.
			second := decodeOneTupleComponent(tail[i+1:])
			return string(first), second, true
		}
		if b == 0x01 && i+1 < len(tail) {
			switch tail[i+1] {
			case 0x01:
				first = append(first, 0x00)
			case 0x02:
				first = append(first, 0x01)
			default:
				return "", "", false
			}
			i += 2
			continue
		}
		first = append(first, b)
		i++
	}
	return "", "", false
}

func decodeOneTupleComponent(b []byte) string {
	out := make([]byte, 0, len(b))
	i := 0
	for i < len(b) {
		c := b[i]
		if c == 0x01 && i+1 < len(b) {
			switch b[i+1] {
			case 0x01:
				out = append(out, 0x00)
			case 0x02:
				out = append(out, 0x01)
			default:
				return string(out)
			}
			i += 2
			continue
		}
		if c == 0x00 {
			// Should not happen in a single component — return what we have.
			return string(out)
		}
		out = append(out, c)
		i++
	}
	return string(out)
}

// lastTupleComponent returns the decoded string of the last component
// in an index key relative to its prefix. Returns empty string if the
// key doesn't extend past the prefix.
func lastTupleComponent(key, prefix []byte) string {
	if len(key) <= len(prefix) {
		return ""
	}
	tail := key[len(prefix):]
	// The tail may have intermediate components; for our two indexes
	// the tail is exactly one tuple-encoded string (the external_id),
	// terminated by EOF (no trailing separator).
	out := make([]byte, 0, len(tail))
	i := 0
	for i < len(tail) {
		b := tail[i]
		if b == 0x00 {
			// Separator — shouldn't appear inside the last component;
			// indicates intermediate components remain. Skip past it
			// and continue with the next component.
			out = out[:0]
			i++
			continue
		}
		if b == 0x01 && i+1 < len(tail) {
			switch tail[i+1] {
			case 0x01:
				out = append(out, 0x00)
			case 0x02:
				out = append(out, 0x01)
			default:
				return string(out)
			}
			i += 2
			continue
		}
		out = append(out, b)
		i++
	}
	return string(out)
}
