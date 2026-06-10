package pebble

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2"

	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// ErrEmptySync is returned when Compact is called with a sync_id that
// has no records in the source engine. The caller can treat this as a
// no-op or as an error depending on context.
var ErrEmptySync = errors.New("synccompactor/pebble: source has no records under the given sync_id")

// Compactor merges sync_run data between two Pebble engines using
// pebble.DB.IngestAndExcise:
//
//  1. For each bucket of the applied sync, iterate its records in the
//     source engine's key order and accumulate them into an SST file
//     on disk.
//  2. Call base.DB.IngestAndExcise with the new SST and the key range
//     covering the applied sync_id. Pebble atomically (a) excises
//     every key in the span from base — old sync_id rows go away in
//     one shot — and (b) ingests the SST as a new L6 file (or
//     flushable, depending on Pebble's choice) under that range.
//
// The net effect is a byte-level merge: zero proto encode/decode per
// record, zero LSM compaction churn from a record-by-record Put loop.
// Each Compact call replaces exactly one sync_id range in the
// destination; multi-sync rollup is a higher-level loop calling
// Compact in sequence.
//
// Reusable across many Compact calls; safe for sequential use only
// (not concurrent).
type Compactor struct {
	base   *enginepkg.Engine
	tmpDir string
}

// NewCompactor builds a compactor that writes its merge into base. The
// tmpDir is where intermediate SST files are written; it must be on
// the same filesystem as the engine's data directory so Pebble can
// hard-link (a different FS would force a copy and break atomicity).
// If tmpDir is empty, os.TempDir is used (acceptable for tests but not
// production).
func NewCompactor(base *enginepkg.Engine, tmpDir string) (*Compactor, error) {
	if base == nil {
		return nil, errors.New("synccompactor/pebble: base engine is nil")
	}
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, fmt.Errorf("synccompactor/pebble: mkdir tmpDir: %w", err)
	}
	return &Compactor{base: base, tmpDir: tmpDir}, nil
}

// Compact merges all records belonging to syncID in `source` into the
// base engine. Any pre-existing data in base under that syncID is
// excised before the new data is ingested — base ends up with exactly
// source's view of that sync.
//
// Atomicity: per-bucket atomic, NOT whole-Compact atomic. Each
// IngestAndExcise call (one per record-type bucket: grants,
// by_entitlement index, by_principal index) is atomic from base's
// perspective — a concurrent reader sees the old-or-new state of that
// bucket, never a mixture. However, the multi-bucket loop is NOT
// transactional as a whole: a crash or hard cancellation mid-loop
// leaves base with new data in some buckets and old data in others
// (and the same for the DeleteRange-only path used for empty
// buckets). Recovery is "re-run Compact for the same syncID" — every
// step is idempotent (excise + ingest the same SSTs again converges
// to the source's view).
//
// Caller is responsible for quiescing writes to `source` for the
// duration of the call (an active syncer would invalidate the SST as
// it's being built).
func (c *Compactor) Compact(ctx context.Context, source *enginepkg.Engine, syncID string) error {
	if source == nil {
		return errors.New("synccompactor/pebble.Compact: source engine is nil")
	}
	if syncID == "" {
		return errors.New("synccompactor/pebble.Compact: syncID is required")
	}

	srcDB := source.DB()
	if srcDB == nil {
		return errors.New("synccompactor/pebble.Compact: source engine has no DB (closed?)")
	}
	dstDB := c.base.DB()
	if dstDB == nil {
		return errors.New("synccompactor/pebble.Compact: base engine has no DB (closed?)")
	}

	syncIDBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return fmt.Errorf("synccompactor/pebble.Compact: encode syncID: %w", err)
	}

	// The full key range that belongs to this sync across all record
	// types and indexes. The v3 key layout puts sync_id inside each
	// type bucket, so a single range does not cover all sync-owned
	// keys. Instead we walk each bucket and emit one SST per bucket,
	// excising each bucket's [sync_lo, sync_hi) range.
	plans := buildBucketPlans(syncIDBytes)

	type pendingIngest struct {
		sstPath    string
		exciseSpan pebble.KeyRange
		keyCount   int
	}
	var pending []pendingIngest

	// Always clean up SSTs on the way out; pebble's IngestAndExcise
	// links them into its own object store on success, so it's fine to
	// remove the path either way (Pebble retains its own ref).
	defer func() {
		for _, p := range pending {
			_ = os.Remove(p.sstPath)
		}
	}()

	totalKeys := 0
	for _, plan := range plans {
		if err := ctx.Err(); err != nil {
			return err
		}
		iter, err := srcDB.NewIter(&pebble.IterOptions{
			LowerBound: plan.lower,
			UpperBound: plan.upper,
		})
		if err != nil {
			return fmt.Errorf("compact: source iter for %s: %w", plan.name, err)
		}

		sstPath := filepath.Join(c.tmpDir, fmt.Sprintf("compact-%s-%x.sst", plan.name, syncIDBytes[:4]))
		b, err := buildSSTFromIter(ctx, sstPath, iter)
		_ = iter.Close()
		if err != nil {
			return fmt.Errorf("compact: build SST for %s: %w", plan.name, err)
		}

		// Empty bucket: nothing to ingest, but we still excise the
		// destination's range so that the resulting state matches the
		// source exactly (if the source had no grants under this sync,
		// the destination's pre-existing grants under this sync must
		// also go).
		if b.KeyCount() == 0 {
			// Tiny excise-only: no SST to ingest but still need to
			// clear destination. Pebble doesn't let us IngestAndExcise
			// with zero SSTs (the ingest must reference at least one
			// file), so we fall back to a RangeDelete + Flush.
			_ = os.Remove(sstPath)
			if err := dstDB.DeleteRange(plan.lower, plan.upper, pebble.Sync); err != nil {
				return fmt.Errorf("compact: excise-only DeleteRange for %s: %w", plan.name, err)
			}
			continue
		}

		pending = append(pending, pendingIngest{
			sstPath: sstPath,
			exciseSpan: pebble.KeyRange{
				Start: plan.lower,
				End:   plan.upper,
			},
			keyCount: b.KeyCount(),
		})
		totalKeys += b.KeyCount()
	}

	if len(pending) == 0 && totalKeys == 0 {
		// Source had nothing for this sync. Destination's pre-existing
		// data (if any) was cleared via DeleteRange in the empty
		// branches above. Return ErrEmptySync so callers can
		// distinguish a "true no-op" from a successful merge.
		return ErrEmptySync
	}

	// One IngestAndExcise per bucket. We can't bundle them into a
	// single call because each bucket has a distinct excise span and
	// Pebble takes only one span per call.
	for _, p := range pending {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := dstDB.IngestAndExcise(
			ctx,
			[]string{p.sstPath},
			nil,
			nil,
			p.exciseSpan,
		); err != nil {
			return fmt.Errorf("compact: IngestAndExcise: %w", err)
		}
	}

	return nil
}
