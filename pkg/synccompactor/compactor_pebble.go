package synccompactor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	mergepkg "github.com/conductorone/baton-sdk/pkg/synccompactor/pebble"
)

// WithEngine selects the storage engine for the compacted output.
// The default (unset) is sqlite, which is byte-identical to the
// historical compactor. EnginePebble produces a v3 Pebble c1z via a
// native record merge.
//
// This is the only supported way to choose the engine; an engine
// passed through WithC1ZOptions does not select the compaction
// strategy and is overridden.
func WithEngine(engine dotc1z.Engine) Option {
	return func(c *Compactor) {
		c.engine = engine
	}
}

// PebbleCompactorMode selects the record-merge strategy for Pebble
// (v3) compaction.
type PebbleCompactorMode string

const (
	// PebbleCompactorModeAuto (the zero value) picks the best strategy.
	// Currently that is overlay: it won or tied every measured shape
	// (same-size and skewed inputs at 10/50/500 sources) and degrades
	// gracefully — buckets whose estimated keys exceed its in-memory
	// seen-set bound are routed to the kway path inside the merge.
	PebbleCompactorModeAuto PebbleCompactorMode = ""

	// PebbleCompactorModeKWay forces the bounded-fan-in external merge
	// sort. Never the fastest on its own, but it is the machinery
	// overlay falls back to for oversized buckets; forcing it is for
	// debugging and benchmarks.
	PebbleCompactorModeKWay PebbleCompactorMode = "kway"

	// PebbleCompactorModeOverlay forces the seen-set overlay merge.
	PebbleCompactorModeOverlay PebbleCompactorMode = "overlay"
)

// WithPebbleCompactorMode overrides the Pebble merge strategy. The
// default (PebbleCompactorModeAuto) chooses for you; passing an
// explicit mode is intended for benchmarks and debugging, not normal
// operation. No-op for the SQLite engine.
func WithPebbleCompactorMode(mode PebbleCompactorMode) Option {
	return func(c *Compactor) {
		c.pebbleMode = mode
	}
}

// WithOverlaySeenKeyLimit overrides the overlay merge's per-bucket
// seen-set cap: buckets whose estimated key count exceeds it route to
// the K-way run-file fallback. Zero (the default) uses the merge's
// built-in production value. Intended for tests and benchmarks; the
// cap bounds merge memory (~40B per seen key).
func WithOverlaySeenKeyLimit(n int64) Option {
	return func(c *Compactor) {
		c.overlaySeenKeyLimit = n
	}
}

// WithOverlayRecordChunkSize overrides how many winner records the
// overlay merge buffers per raw write batch. Zero (the default) uses
// the merge's built-in production value.
func WithOverlayRecordChunkSize(n int) Option {
	return func(c *Compactor) {
		c.overlayRecordChunkSize = n
	}
}

// resolvedPebbleMode returns the effective merge strategy: an explicit
// WithPebbleCompactorMode wins, and Auto resolves to overlay (see
// PebbleCompactorModeAuto for why).
func (c *Compactor) resolvedPebbleMode() PebbleCompactorMode {
	switch c.pebbleMode {
	case PebbleCompactorModeKWay, PebbleCompactorModeOverlay:
		return c.pebbleMode
	default:
		return PebbleCompactorModeOverlay
	}
}

// compactableV3SyncType reports whether a v3 sync type is a compactable
// snapshot type. Diff syncs (partial_upserts / partial_deletions) are
// excluded — compaction folds full / resources-only / partial snapshots
// only, matching the sqlite source selection.
func compactableV3SyncType(t v3.SyncType) bool {
	switch t {
	case v3.SyncType_SYNC_TYPE_FULL,
		v3.SyncType_SYNC_TYPE_RESOURCES_ONLY,
		v3.SyncType_SYNC_TYPE_PARTIAL:
		return true
	default:
		return false
	}
}

// unionV3SyncType folds two sync types to the wider one: full beats
// resources-only beats partial. Mirrors the sqlite union rule.
func unionV3SyncType(a, b v3.SyncType) v3.SyncType {
	switch {
	case a == v3.SyncType_SYNC_TYPE_FULL || b == v3.SyncType_SYNC_TYPE_FULL:
		return v3.SyncType_SYNC_TYPE_FULL
	case a == v3.SyncType_SYNC_TYPE_RESOURCES_ONLY || b == v3.SyncType_SYNC_TYPE_RESOURCES_ONLY:
		return v3.SyncType_SYNC_TYPE_RESOURCES_ONLY
	default:
		return v3.SyncType_SYNC_TYPE_PARTIAL
	}
}

// manifestSourceSelection is the result of picking a compaction source
// sync from the envelope manifest's sync-run projection.
type manifestSourceSelection struct {
	syncID   string
	syncType v3.SyncType
	endedAt  time.Time
	stats    *reader_v2.SyncStats
}

// selectSourceSyncFromManifest reads the v3 envelope header (manifest
// only — no payload unpack, no zstd decode) and picks the latest
// finished compactable sync from the manifest's sync_runs projection.
// The selection rule mirrors Engine.LatestFinishedSyncRecord: newest
// ended_at, ties broken by the greater sync_id (KSUIDs sort by time).
//
// Returns ok=false when the file predates the projection (no sync_runs
// recorded), isn't a readable v3 envelope, or has no finished
// compactable sync — callers fall back to the unpack path, which
// produces the canonical errors.
func selectSourceSyncFromManifest(path string) (manifestSourceSelection, bool) {
	f, err := os.Open(path)
	if err != nil {
		return manifestSourceSelection{}, false
	}
	defer f.Close()
	m, err := formatv3.ReadManifestHeader(f)
	if err != nil {
		return manifestSourceSelection{}, false
	}
	var best *c1zv3.SyncRunSummary
	for _, r := range m.GetSyncRuns() {
		if r == nil || r.GetEndedAt() == nil || !compactableV3SyncType(r.GetType()) {
			continue
		}
		if best == nil {
			best = r
			continue
		}
		curEnd, bestEnd := r.GetEndedAt().AsTime(), best.GetEndedAt().AsTime()
		switch {
		case curEnd.After(bestEnd):
			best = r
		case curEnd.Equal(bestEnd) && r.GetSyncId() > best.GetSyncId():
			best = r
		}
	}
	if best == nil {
		return manifestSourceSelection{}, false
	}
	sel := manifestSourceSelection{
		syncID:   best.GetSyncId(),
		syncType: best.GetType(),
		endedAt:  best.GetEndedAt().AsTime(),
	}
	if s := best.GetStats(); s != nil {
		sel.stats = enginepkg.SyncStatsFromRecord(s)
	}
	return sel, true
}

// compactPebble folds every input into the empty newSyncId on the
// Pebble output via a native record merge: each input is opened, its
// latest finished compactable sync is selected (diff syncs excluded),
// and all are merged keeping the newest record per key. The output
// sync_run's type and ended_at are then set to the union / max across
// the inputs (mirroring the sqlite UpdateSync), and its stats are
// recomputed. Inputs are merged in reverse entry order so the tie
// winner matches the sqlite fold.
func (c *Compactor) compactPebble(ctx context.Context, newSyncId string) error {
	l := ctxzap.Extract(ctx)

	destEng, ok := enginepkg.AsEngine(c.compactedC1z)
	if !ok {
		return errors.New("compactPebble: compacted store is not a pebble engine")
	}

	pebbleCompactorMode := c.resolvedPebbleMode()
	useOverlay := pebbleCompactorMode == PebbleCompactorModeOverlay
	sources := make([]mergepkg.SourceFile, 0, len(c.entries))
	unionType := v3.SyncType_SYNC_TYPE_PARTIAL
	var maxEnded time.Time

	for i := len(c.entries) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		cs := c.entries[i]

		// Fast path: read the latest finished compactable sync (and its
		// cached stats) from the envelope manifest's sync-run projection
		// — a header read, no payload unpack. Files written before the
		// projection existed fall back to the unpack path below.
		if sel, ok := selectSourceSyncFromManifest(cs.FilePath); ok {
			sources = append(sources, mergepkg.SourceFile{Path: cs.FilePath, SyncID: sel.syncID, Stats: sel.stats, DecoderPool: c.decoderPool})
			unionType = unionV3SyncType(unionType, sel.syncType)
			if sel.endedAt.After(maxEnded) {
				maxEnded = sel.endedAt
			}
			continue
		}

		source, syncType, endedAt, err := func() (mergepkg.SourceFile, v3.SyncType, time.Time, error) {
			var zeroSource mergepkg.SourceFile
			w, err := dotc1z.NewStore(ctx, cs.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(c.tmpDir), dotc1z.WithDecoderPool(c.decoderPool))
			if err != nil {
				return zeroSource, v3.SyncType_SYNC_TYPE_UNSPECIFIED, time.Time{}, fmt.Errorf("compactPebble: open input %s: %w", cs.FilePath, err)
			}
			defer func() {
				if cerr := w.Close(ctx); cerr != nil {
					l.Error("compactPebble: error closing source store", zap.Error(cerr), zap.String("file", cs.FilePath))
				}
			}()

			srcEng, ok := enginepkg.AsEngine(w)
			if !ok {
				return zeroSource, v3.SyncType_SYNC_TYPE_UNSPECIFIED, time.Time{}, fmt.Errorf("compactPebble: input %s is not a pebble c1z", cs.FilePath)
			}
			rec, err := srcEng.LatestFinishedSyncRecord(ctx, compactableV3SyncType)
			if err != nil {
				return zeroSource, v3.SyncType_SYNC_TYPE_UNSPECIFIED, time.Time{}, fmt.Errorf("compactPebble: select source sync for %s: %w", cs.FilePath, err)
			}
			if rec == nil {
				return zeroSource, v3.SyncType_SYNC_TYPE_UNSPECIFIED, time.Time{}, fmt.Errorf("compactPebble: input %s has no finished compactable sync (diff syncs are not compactable)", cs.FilePath)
			}

			// Record only (Path, SyncID, Stats) and fully close the store,
			// removing its unpacked directory. The merge re-unpacks each
			// source when its chunk is processed and removes it when the
			// chunk closes, so peak disk is O(fan-in) source directories,
			// not O(len(entries)). The fallback unpacks one source at a
			// time and pays one extra unpack per source (selection + merge).
			source := mergepkg.SourceFile{Path: cs.FilePath, SyncID: rec.GetSyncId(), DecoderPool: c.decoderPool}
			if useOverlay {
				stats, ok, err := enginepkg.CachedSyncStats(ctx, srcEng, rec.GetSyncId())
				if err != nil {
					return zeroSource, v3.SyncType_SYNC_TYPE_UNSPECIFIED, time.Time{}, fmt.Errorf("compactPebble: cached stats for %s: %w", cs.FilePath, err)
				}
				if ok {
					source.Stats = stats
				}
			}
			var endedAt time.Time
			if ts := rec.GetEndedAt(); ts != nil {
				endedAt = ts.AsTime()
			}
			return source, rec.GetType(), endedAt, nil
		}()
		if err != nil {
			return err
		}

		sources = append(sources, source)
		unionType = unionV3SyncType(unionType, syncType)
		if endedAt.After(maxEnded) {
			maxEnded = endedAt
		}
	}

	var statsRec *v3.SyncStatsRecord
	var err error
	switch pebbleCompactorMode {
	case PebbleCompactorModeOverlay:
		// Oversized buckets are routed to the K-way run-file path inside
		// the overlay merge itself (overlayPlanBuckets), so there is no
		// whole-merge fallback here.
		var overlayOpts []mergepkg.OverlayOption
		if c.overlaySeenKeyLimit > 0 {
			overlayOpts = append(overlayOpts, mergepkg.WithOverlaySeenKeyLimit(c.overlaySeenKeyLimit))
		}
		if c.overlayRecordChunkSize > 0 {
			overlayOpts = append(overlayOpts, mergepkg.WithOverlayRecordChunkSize(c.overlayRecordChunkSize))
		}
		statsRec, err = mergepkg.MergeFilesIntoOverlay(ctx, destEng, sources, newSyncId, c.tmpDir, overlayOpts...)
	default:
		statsRec, err = mergepkg.MergeFilesInto(ctx, destEng, sources, newSyncId, c.tmpDir)
	}
	if err != nil {
		return fmt.Errorf("compactPebble: merge: %w", err)
	}

	// Set the compacted sync_run's type + ended_at to the union / max
	// across the inputs so downstream gating (e.g. grant expansion)
	// behaves identically to the sqlite path, then recompute stats.
	rec, err := destEng.GetSyncRunRecord(ctx, newSyncId)
	if err != nil {
		return fmt.Errorf("compactPebble: load dest sync_run: %w", err)
	}
	rec.SetType(unionType)
	if !maxEnded.IsZero() {
		rec.SetEndedAt(timestamppb.New(maxEnded))
	}
	if err := destEng.PutSyncRunRecord(ctx, rec); err != nil {
		return fmt.Errorf("compactPebble: persist dest sync_run: %w", err)
	}
	// The merge accumulated the dest stats while writing winners, so
	// persist those instead of re-scanning the freshly written output.
	if statsRec != nil {
		if err := destEng.PersistComputedSyncStats(ctx, newSyncId, statsRec); err != nil {
			return fmt.Errorf("compactPebble: persist stats: %w", err)
		}
		return nil
	}
	if err := destEng.PersistSyncStats(ctx, newSyncId); err != nil {
		return fmt.Errorf("compactPebble: persist stats: %w", err)
	}
	return nil
}
