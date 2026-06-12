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
func MergeInto(ctx context.Context, dest *enginepkg.Engine, sources []SourceSync, destSyncID string) error {
	if dest == nil {
		return errors.New("synccompactor/pebble.MergeInto: dest engine is nil")
	}
	if destSyncID == "" {
		return errors.New("synccompactor/pebble.MergeInto: destSyncID is required")
	}
	if err := dest.SetCurrentSync(destSyncID); err != nil {
		return fmt.Errorf("synccompactor/pebble.MergeInto: bind dest sync: %w", err)
	}
	for i := range sources {
		if err := ctx.Err(); err != nil {
			return err
		}
		s := sources[i]
		if s.Engine == nil || s.SyncID == "" {
			continue
		}
		if err := mergeOneSource(ctx, dest, s, destSyncID); err != nil {
			return fmt.Errorf("merge source %s: %w", s.SyncID, err)
		}
	}
	return nil
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
func mergeOneSource(ctx context.Context, dest *enginepkg.Engine, s SourceSync, destSyncID string) error {
	srcDB := s.Engine.DB()
	if srcDB == nil {
		return errors.New("source engine has no DB (closed?)")
	}
	for _, bucket := range allBuckets() {
		if err := mergeBucketRawIfNewer(ctx, dest, srcDB, bucket); err != nil {
			return fmt.Errorf("merge %s: %w", bucket.name, err)
		}
	}
	return nil
}

func mergeBucketRawIfNewer(ctx context.Context, dest *enginepkg.Engine, src *pebble.DB, bucket bucketSpec) error {
	lower, upper := bucket.syncRange()
	iter, err := src.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	destDB := dest.DB()
	batch := destDB.NewBatch()
	defer func() { _ = batch.Close() }()
	pending := 0
	var scratch rawIndexScratch
	// The closures see batch by reference, so flush's reassignment
	// routes subsequent index writes to the fresh batch.
	setIndexKey := func(key []byte) error { return batch.Set(key, nil, nil) }
	delIndexKey := func(key []byte) error { return batch.Delete(key, nil) }
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
		batch = destDB.NewBatch()
		pending = 0
		return nil
	}

	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key, value := iter.Key(), iter.Value()
		// Point-read the committed incumbent. Safe against the pending
		// batch: a sync holds one record per key, so no key repeats
		// within this loop, and earlier sources were fully flushed.
		oldVal, closer, getErr := destDB.Get(key)
		switch {
		case getErr == nil:
			if bytes.Equal(oldVal, value) {
				closer.Close()
				continue
			}
			newTs, err := discoveredAtNanosFromRaw(bucket, value)
			if err != nil {
				closer.Close()
				return err
			}
			oldTs, err := discoveredAtNanosFromRaw(bucket, oldVal)
			if err != nil {
				closer.Close()
				return err
			}
			if newTs <= oldTs {
				closer.Close()
				continue
			}
			if err := forEachIndexKeyFromRaw(bucket, key, lower, oldVal, &scratch, nil, delIndexKey); err != nil {
				closer.Close()
				return err
			}
			closer.Close()
		case errors.Is(getErr, pebble.ErrNotFound):
		default:
			return fmt.Errorf("get incumbent: %w", getErr)
		}
		if err := batch.Set(key, value, nil); err != nil {
			return err
		}
		if err := forEachIndexKeyFromRaw(bucket, key, lower, value, &scratch, nil, setIndexKey); err != nil {
			return err
		}
		pending++
		if pending >= mergeRawFlushRecords {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	return flush()
}
