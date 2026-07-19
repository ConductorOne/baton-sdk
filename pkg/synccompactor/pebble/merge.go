package pebble

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// SourceSync names one input to a merge: its open Pebble engine and the
// sync id whose records should be folded into the destination.
type SourceSync struct {
	Engine *enginepkg.Engine
	SyncID string
}

// FoldStats reports what a MergeInto call overrode in the destination
// keyspace. DeadBytes is the exact raw size (keys + values) of the
// incumbent records — and their derived index keys — that the fold
// shadowed: because the envelope save splices the base's compressed
// frames verbatim, those bytes stay in the output as dead weight.
// Callers accumulate this into the manifest's fold_dead_bytes so the
// auto cutover can force a rebuild once waste crosses its threshold.
type FoldStats struct {
	// OverriddenRecords counts incumbents replaced by a strictly-newer
	// source record. Byte-identical resubmissions are no-ops and do
	// not count.
	OverriddenRecords int64
	// DeadBytes sums len(key)+len(value) of each overridden incumbent
	// plus len(key) of each of its stale derived index keys.
	DeadBytes int64
	// AddedByBucket counts records admitted with no incumbent, keyed by
	// bucket name (resource_types, resources, entitlements, grants).
	AddedByBucket map[string]int64
	// ReplacedByBucket counts strictly-newer overrides per bucket name.
	// Sums to OverriddenRecords.
	ReplacedByBucket map[string]int64
	// GrantWrites counts every grant record actually written to dest —
	// both fresh admissions (no incumbent at the key) and strictly-
	// newer overrides. Byte-identical resubmissions and not-strictly-
	// newer candidates are NOT counted (mergeBucketRawIfNewer's no-op
	// fast paths). The fold compactor uses this to decide whether the
	// grants keyspace changed at all: when it's zero across a whole
	// fold, the base's already-correct grant-digest state was never
	// touched and needs no rebuild (see compactPebbleFold).
	GrantWrites int64
	// TouchedGrantPartitions is the set of entitlement partitions
	// (enginepkg.GrantPartitionFromPrimaryKey) that had at least one
	// grant actually written to dest, under the same counting rule as
	// GrantWrites. The fold compactor uses this to invalidate and
	// repair EXACTLY the entitlements a fold touched
	// (Engine.InvalidateGrantDigestPartitions +
	// Engine.RepairMissingGrantDigests), instead of the whole file.
	TouchedGrantPartitions map[string]struct{}
}

func (s *FoldStats) Add(o FoldStats) {
	s.OverriddenRecords += o.OverriddenRecords
	s.DeadBytes += o.DeadBytes
	for bucket, n := range o.AddedByBucket {
		s.bumpAdded(bucket, n)
	}
	for bucket, n := range o.ReplacedByBucket {
		s.bumpReplaced(bucket, n)
	}
	s.GrantWrites += o.GrantWrites
	for p := range o.TouchedGrantPartitions {
		if s.TouchedGrantPartitions == nil {
			s.TouchedGrantPartitions = make(map[string]struct{}, len(o.TouchedGrantPartitions))
		}
		s.TouchedGrantPartitions[p] = struct{}{}
	}
}

func (s *FoldStats) bumpAdded(bucket string, n int64) {
	if s.AddedByBucket == nil {
		s.AddedByBucket = make(map[string]int64, 4)
	}
	s.AddedByBucket[bucket] += n
}

func (s *FoldStats) bumpReplaced(bucket string, n int64) {
	if s.ReplacedByBucket == nil {
		s.ReplacedByBucket = make(map[string]int64, 4)
	}
	s.ReplacedByBucket[bucket] += n
}

// MergeInto folds every source's primary records into dest under
// destSyncID. Across all inputs (and any records already present under
// destSyncID), the newest record per logical key (by discovered_at,
// ties keep the incumbent) survives, and dest's derived indexes are
// maintained by the raw keep-newer merge (mergeBucketRawIfNewer).
//
// The merge is byte-level end to end: v3 keys and values carry no
// sync_id, so a source record is copied into dest verbatim — no proto
// decode/re-encode — and a source record byte-identical to the
// incumbent is a pure no-op.
//
// destSyncID must already exist in dest. It may be non-empty: the
// in-place fold compaction merges partial syncs directly into the base
// sync's keyspace, resolving conflicts against pre-existing records
// via the keep-newer rule. MergeInto binds the dest engine's current
// sync to destSyncID for engine bookkeeping.
//
// Only the four primary record buckets are copied: resource_types,
// resources, entitlements, grants. Assets are intentionally NOT copied
// — this matches the SQLite compaction path, which folds only those
// four tables and drops assets from the compacted sync; copying assets
// here would give Pebble compacted syncs asset access the SQLite path
// does not have. Index keyspaces are maintained per record (replaced
// records get point deletes for their stale index keys).
//
// Sources are applied in the given order. The dedup keeps the record
// with the strictly-greater discovered_at; on an equal discovered_at
// the already-written (earlier source, or pre-existing dest) record is
// kept. Callers pass sources newest-first so the tie winner matches the
// SQLite fold.
//
// MergeInto does NOT touch grant-digest state itself. The fold dest
// starts as a byte copy of a SEALED base, which carries an already-
// CORRECT grant hash index and digests; this merge mutates grants
// through the raw DB handle without maintaining either (nothing does,
// outside the seal-time build), so once ANY grant write lands the
// dest's digest state is stale relative to it. Rather than drop it
// pre-emptively on every call — which would force a full from-scratch
// digest rebuild even for a fold that changes nothing, defeating
// fold's whole point of doing O(partials) work, not O(base) — callers
// inspect the returned FoldStats.GrantWrites: zero means the grants
// keyspace was never touched and the base's digest state is still
// exactly correct, left alone; nonzero means the caller must rebuild
// (Engine.BuildGrantDigests rebuilds both keyspaces atomically from
// scratch, so no separate drop is needed even then — see
// compactPebbleFold).
func MergeInto(ctx context.Context, dest *enginepkg.Engine, sources []SourceSync, destSyncID string) (FoldStats, error) {
	var stats FoldStats
	if dest == nil {
		return stats, errors.New("synccompactor/pebble.MergeInto: dest engine is nil")
	}
	if destSyncID == "" {
		return stats, errors.New("synccompactor/pebble.MergeInto: destSyncID is required")
	}
	if err := dest.SetCurrentSync(ctx, destSyncID); err != nil {
		return stats, fmt.Errorf("synccompactor/pebble.MergeInto: bind dest sync: %w", err)
	}
	// Folds write the dest keyspace through the raw DB handle; invalidate
	// the engine's bare-id lookup state on the way out (even on error —
	// earlier sources may already have been folded in).
	defer dest.InvalidateBareIDLookups()
	for i := range sources {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		s := sources[i]
		if s.Engine == nil || s.SyncID == "" {
			continue
		}
		srcStats, err := mergeOneSource(ctx, dest, s, destSyncID)
		stats.Add(srcStats)
		if err != nil {
			return stats, fmt.Errorf("merge source %s: %w", s.SyncID, err)
		}
	}
	return stats, nil
}

// mergeRawFlushRecords bounds how many records accumulate in one raw
// write batch before it commits. Matches the overlay writer's chunk
// size; peak batch memory stays O(chunk), not O(bucket).
const mergeRawFlushRecords = 32768

// mergeOneSource streams every primary record of the source and folds
// it into dest with keep-newer semantics, entirely at the byte level:
// source keys and values are already in final dest form (v3 keys and
// values carry no sync_id), so nothing is proto-decoded or re-encoded.
// Per record:
//
//   - no incumbent at the key → raw copy + derived index keys;
//   - incumbent byte-identical → pure no-op (no tombstones, no index
//     churn — the common case when overlapping partials resubmit
//     unchanged records);
//   - otherwise a shallow discovered_at comparison decides: strictly
//     newer wins, replacing the value and swapping the incumbent's
//     derived index keys for the new value's (point deletes
//     proportional to overridden records only). Ties keep the
//     incumbent, mirroring the engine's Put*RecordsIfNewer rule —
//     missing discovered_at scans as 0, reproducing its nil-timestamp
//     ordering ("never overwrite an incumbent, always fill a hole").
func mergeOneSource(ctx context.Context, dest *enginepkg.Engine, s SourceSync, destSyncID string) (FoldStats, error) {
	var stats FoldStats
	for _, bucket := range allBuckets() {
		bucketStats, err := mergeBucketRawIfNewer(ctx, dest, s.Engine, bucket)
		stats.Add(bucketStats)
		if err != nil {
			return stats, fmt.Errorf("merge %s: %w", bucket.name, err)
		}
	}
	return stats, nil
}

func mergeBucketRawIfNewer(ctx context.Context, dest *enginepkg.Engine, src *enginepkg.Engine, bucket bucketSpec) (FoldStats, error) {
	var stats FoldStats
	lower, upper := bucket.syncRange()
	iter, err := src.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return stats, err
	}
	defer func() { _ = iter.Close() }()

	batch := dest.NewFoldBatch()
	defer func() { _ = batch.Close() }()
	pending := 0
	var scratch rawIndexScratch
	// The closures see batch by reference, so flush's reassignment
	// routes subsequent index writes to the fresh batch.
	setIndexKey := func(key []byte) error { return batch.Set(key, nil) }
	// Each deleted index key is an incumbent's stale index entry: dead
	// weight in the spliced base frames, counted toward FoldStats.
	delIndexKey := func(key []byte) error {
		stats.DeadBytes += int64(len(key))
		return batch.Delete(key)
	}
	flush := func() error {
		if batch.Empty() {
			return nil
		}
		// NoSync: the fold's envelope save checkpoints (which flushes
		// and fsyncs) before anything depends on these writes.
		if err := batch.Commit(pebble.NoSync); err != nil {
			return err
		}
		_ = batch.Close()
		batch = dest.NewFoldBatch()
		pending = 0
		return nil
	}

	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		key, value := iter.Key(), iter.Value()
		// Point-read the committed incumbent. Safe against the pending
		// batch: a sync holds one record per key, so no key repeats
		// within this loop, and earlier sources were fully flushed.
		oldVal, closer, getErr := dest.Get(key)
		switch {
		case getErr == nil:
			if bytes.Equal(oldVal, value) {
				closer.Close()
				continue
			}
			newTs, err := discoveredAtNanosFromRaw(bucket, value)
			if err != nil {
				closer.Close()
				return stats, err
			}
			oldTs, err := discoveredAtNanosFromRaw(bucket, oldVal)
			if err != nil {
				closer.Close()
				return stats, err
			}
			if newTs <= oldTs {
				closer.Close()
				continue
			}
			// The incumbent loses: its key and value go dead inside the
			// spliced base frames. Its stale index keys are counted by
			// delIndexKey as forEachIndexKeyFromRaw enumerates them.
			stats.OverriddenRecords++
			stats.bumpReplaced(bucket.name, 1)
			stats.DeadBytes += int64(len(key)) + int64(len(oldVal))
			if err := forEachIndexKeyFromRaw(bucket, key, lower, oldVal, &scratch, nil, delIndexKey); err != nil {
				closer.Close()
				return stats, err
			}
			closer.Close()
		case errors.Is(getErr, pebble.ErrNotFound):
			stats.bumpAdded(bucket.name, 1)
		default:
			return stats, fmt.Errorf("get incumbent: %w", getErr)
		}
		if err := batch.Set(key, value); err != nil {
			return stats, err
		}
		if bucket.id == runBucketGrants {
			stats.GrantWrites++
			if partition, ok := enginepkg.GrantPartitionFromPrimaryKey(key); ok {
				if stats.TouchedGrantPartitions == nil {
					stats.TouchedGrantPartitions = map[string]struct{}{}
				}
				stats.TouchedGrantPartitions[partition] = struct{}{}
			}
		}
		if err := forEachIndexKeyFromRaw(bucket, key, lower, value, &scratch, nil, setIndexKey); err != nil {
			return stats, err
		}
		pending++
		if pending >= mergeRawFlushRecords {
			if err := flush(); err != nil {
				return stats, err
			}
		}
	}
	if err := iter.Error(); err != nil {
		return stats, err
	}
	return stats, flush()
}
