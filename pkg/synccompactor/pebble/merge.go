package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
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
// maintained by the keep-newer write path.
//
// destSyncID must already exist in dest. It may be non-empty: the
// in-place fold compaction merges partial syncs directly into the base
// sync's keyspace, relying on the Put*RecordsIfNewer semantics to
// resolve conflicts against pre-existing records. MergeInto binds the
// dest engine's current sync to destSyncID — the engine's write
// methods key off the current sync, not any per-record field.
//
// Only the four primary record buckets are copied: resource_types,
// resources, entitlements, grants. Assets are intentionally NOT copied
// — this matches the SQLite compaction path, which folds only those
// four tables and drops assets from the compacted sync; copying assets
// here would give Pebble compacted syncs asset access the SQLite path
// does not have. Index keyspaces are not copied verbatim either; the
// per-record write path maintains them.
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

// mergeBatchSize bounds how many decoded records are held in memory
// before a flush into the keep-newer put path. A whole bucket can hold
// millions of records at the large-connector scale compaction targets,
// so the merge streams in fixed-size batches rather than materializing
// the bucket — peak heap stays O(mergeBatchSize), not O(bucket).
const mergeBatchSize = 1000

// mergeOneSource streams every primary record under s.SyncID, re-keys
// it to destSyncID, and writes it into dest via the engine's keep-newer
// put path (which dedups by discovered_at and rebuilds indexes). Each
// bucket is drained in fixed-size batches to bound peak memory.
func mergeOneSource(ctx context.Context, dest *enginepkg.Engine, s SourceSync, destSyncID string) error {
	srcBytes, err := codec.EncodeSyncID(s.SyncID)
	if err != nil {
		return err
	}
	srcDB := s.Engine.DB()
	if srcDB == nil {
		return errors.New("source engine has no DB (closed?)")
	}

	if err := streamBucket(ctx, srcDB,
		enginepkg.ResourceTypeSyncLowerBound(srcBytes), enginepkg.ResourceTypeSyncUpperBound(srcBytes),
		func() *v3.ResourceTypeRecord { return &v3.ResourceTypeRecord{} },
		dest.PutResourceTypeRecordsIfNewer,
	); err != nil {
		return fmt.Errorf("merge resource_types: %w", err)
	}

	if err := streamBucket(ctx, srcDB,
		enginepkg.ResourceSyncLowerBound(srcBytes), enginepkg.ResourceSyncUpperBound(srcBytes),
		func() *v3.ResourceRecord { return &v3.ResourceRecord{} },
		dest.PutResourceRecordsIfNewer,
	); err != nil {
		return fmt.Errorf("merge resources: %w", err)
	}

	if err := streamBucket(ctx, srcDB,
		enginepkg.EntitlementSyncLowerBound(srcBytes), enginepkg.EntitlementSyncUpperBound(srcBytes),
		func() *v3.EntitlementRecord { return &v3.EntitlementRecord{} },
		dest.PutEntitlementRecordsIfNewer,
	); err != nil {
		return fmt.Errorf("merge entitlements: %w", err)
	}

	if err := streamBucket(ctx, srcDB,
		enginepkg.GrantSyncLowerBound(srcBytes), enginepkg.GrantSyncUpperBound(srcBytes),
		func() *v3.GrantRecord { return &v3.GrantRecord{} },
		dest.PutGrantRecordsIfNewer,
	); err != nil {
		return fmt.Errorf("merge grants: %w", err)
	}

	return nil
}

// streamBucket drains a primary bucket's [lower, upper) key range,
// unmarshalling each value into a fresh T (the stored value is a
// deterministic proto marshal of the v3 record) and flushing fixed-size
// batches into put. The destination sync id is supplied by the engine's
// current-sync key context; data record values do not carry sync_id.
// Peak memory is bounded by mergeBatchSize rather than the bucket size.
func streamBucket[T proto.Message](
	ctx context.Context,
	db *pebble.DB,
	lower, upper []byte,
	mk func() T,
	put func(context.Context, ...T) error,
) error {
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	batch := make([]T, 0, mergeBatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := put(ctx, batch...); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec := mk()
		if err := proto.Unmarshal(iter.Value(), rec); err != nil {
			return fmt.Errorf("unmarshal record: %w", err)
		}
		batch = append(batch, rec)
		if len(batch) >= mergeBatchSize {
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
