package synccompactor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	mergepkg "github.com/conductorone/baton-sdk/pkg/synccompactor/pebble"
)

// WithEngine selects the storage engine for the compacted output.
// The default (unset) is sqlite, which is byte-identical to the
// historical compactor. EnginePebble produces a v3 Pebble c1z via a
// native record merge whose strategy (overlay / fold / kway) is
// resolved per run by resolvePebbleMode.
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

	// PebbleCompactorModeFold forces in-place fold into the base sync.
	// Note: the output ADOPTS the base sync's id (no fresh sync id),
	// which C1's compaction bookkeeping currently mishandles — auto
	// mode never selects fold for that reason; forcing it is on the
	// caller to know their consumer tolerates a reused sync id.
	PebbleCompactorModeFold PebbleCompactorMode = "fold"
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

// WithOverlayBufferFactor overrides the overlay merge's soft→hard
// seen-set limit multiplier (default 1.25). A source that crosses the
// soft limit mid-scan may finish inside the buffer and resume via the
// K-way path at the source boundary; crossing the hard limit
// mid-source restarts the bucket through the blind K-way path. Zero
// (the default) uses the merge's built-in production value.
func WithOverlayBufferFactor(f float64) Option {
	return func(c *Compactor) {
		c.overlayBufferFactor = f
	}
}

// WithOverlayGateFraction overrides the overlay merge's statless
// pre-source gate, expressed as a fraction of the soft seen-set limit
// (default 0.9): when a source has no cached stats and the seen set is
// already past the gate, the bucket degrades to the K-way path before
// scanning the source. Zero (the default) uses the merge's built-in
// production value.
func WithOverlayGateFraction(f float64) Option {
	return func(c *Compactor) {
		c.overlayGateFraction = f
	}
}

// Fold cutover thresholds. The auto cutover is currently DISABLED
// (see resolvePebbleMode) — the gate only logs when fold would have
// been picked. Fold's win is conditional on
// the base dominating total input volume: it pays a fixed per-source
// cost (envelope + pebble open, ~10ms each) plus per-record point
// writes for every partial record, but does ZERO work for base records
// and splices the base's unchanged frames at save. Measured at a 1.1GB
// base + 50 small partials, fold is ~3x faster than overlay (~20s vs
// ~60s); at fixture scale (MB bases) overlay wins every shape.
//
// The ratio comes from the crossover sweep
// (TestProdScaleFoldOverlayCrossover): overlay's cost is nearly flat
// in partial volume (it streams the base regardless) while fold's is
// linear (~0.2s per MB of partials), so they cross where fold's
// partial-write cost eats its base savings — ~9% partial volume at a
// 117MB base, extrapolating to ~16% at 1.1GB. 10% keeps fold ahead of
// overlay across the whole gated range; past it the win shrinks
// toward a loss, and overlay is never badly wrong.
const (
	defaultFoldMinBaseBytes      int64 = 256 << 20 // 256 MiB
	defaultFoldMaxPartialPercent int64 = 10        // partials ≤ 10% of base size
)

// foldMinBaseBytes is the smallest base input (envelope file size) for
// which the (currently disabled) auto cutover would pick fold.
// Tunable via BATON_PEBBLE_FOLD_MIN_BASE_BYTES.
func foldMinBaseBytes() int64 {
	if raw := os.Getenv("BATON_PEBBLE_FOLD_MIN_BASE_BYTES"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err == nil && n > 0 {
			return n
		}
	}
	return defaultFoldMinBaseBytes
}

// foldMaxPartialPercent caps the summed partial file sizes, as a
// percent of the base file size, for which the (currently disabled)
// auto cutover would pick fold.
// Tunable via BATON_PEBBLE_FOLD_MAX_PARTIAL_PCT.
func foldMaxPartialPercent() int64 {
	if raw := os.Getenv("BATON_PEBBLE_FOLD_MAX_PARTIAL_PCT"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err == nil && n > 0 {
			return n
		}
	}
	return defaultFoldMaxPartialPercent
}

// resolvePebbleMode picks the Pebble compaction strategy. An explicit
// mode (WithPebbleCompactorMode or BATON_EXPERIMENTAL_PEBBLE_COMPACTOR
// = "overlay" / "kway" / "fold") is honored as-is. Otherwise overlay
// is always selected.
//
// Auto-selection of fold is DISABLED: fold adopts the base sync's id
// instead of minting a fresh one, and C1's compaction bookkeeping
// treats an unchanged LatestCompactedSyncId as "no new compaction",
// silently skipping follow-up work like uplift. Until that is resolved
// on the C1 side, fold runs only when requested explicitly. The size
// gate still evaluates and logs when fold would have won (large base,
// partials a small fraction of it — measured ~3x faster there) so the
// cutover can be re-enabled with confidence. The gate reads only file
// sizes (os.Stat); nothing is unpacked.
func (c *Compactor) resolvePebbleMode(ctx context.Context) PebbleCompactorMode {
	l := ctxzap.Extract(ctx)
	switch c.pebbleMode {
	case PebbleCompactorModeKWay, PebbleCompactorModeOverlay, PebbleCompactorModeFold:
		return c.pebbleMode
	case PebbleCompactorModeAuto:
	}
	switch mode := PebbleCompactorMode(os.Getenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR")); mode {
	case PebbleCompactorModeOverlay, PebbleCompactorModeKWay, PebbleCompactorModeFold:
		l.Info("pebble compactor: mode forced by env", zap.String("mode", string(mode)))
		return mode
	case PebbleCompactorModeAuto:
	default:
		l.Warn("pebble compactor: unrecognized BATON_EXPERIMENTAL_PEBBLE_COMPACTOR value, auto-selecting",
			zap.String("value", string(mode)))
	}

	baseBytes := fileSizeOrZero(c.entries[0].FilePath)
	var partialBytes int64
	for _, e := range c.entries[1:] {
		partialBytes += fileSizeOrZero(e.FilePath)
	}
	minBase := foldMinBaseBytes()
	maxPct := foldMaxPartialPercent()
	mode := PebbleCompactorModeOverlay
	if baseBytes >= minBase && partialBytes*100 <= baseBytes*maxPct {
		l.Info("pebble compactor: fold gate matched but auto-fold is disabled (duplicate sync id breaks C1 compaction bookkeeping); using overlay",
			zap.Int64("base_bytes", baseBytes),
			zap.Int64("partial_bytes", partialBytes),
			zap.Int64("fold_min_base_bytes", minBase),
			zap.Int64("fold_max_partial_pct", maxPct),
		)
	}
	l.Info("pebble compactor: auto-selected mode",
		zap.String("mode", string(mode)),
		zap.Int64("base_bytes", baseBytes),
		zap.Int64("partial_bytes", partialBytes),
		zap.Int("partials", len(c.entries)-1),
	)
	return mode
}

// fileSizeOrZero returns the file's size, or 0 when it can't be
// stat'ed; a missing/unreadable input fails later with a real error,
// the gate just declines the fold cutover.
func fileSizeOrZero(path string) int64 {
	fi, err := os.Stat(path) // #nosec G703 - path is a caller-provided compaction input, not untrusted user input.
	if err != nil {
		return 0
	}
	return fi.Size()
}

// ensurePebbleRegistered is kept for tests and setup paths from the
// pre-static-registration era. Pebble is now registered by dotc1z init, so this
// is a cheap sanity check.
func ensurePebbleRegistered() error {
	if _, ok := dotc1z.EngineDriverFor(dotc1z.EnginePebble); ok {
		return nil
	}
	return dotc1z.ErrEngineNotAvailable
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
	f, err := os.Open(path) // #nosec G703 - path is a caller-provided compaction input, not untrusted user input.
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

// compactPebbleFold is the in-place fold strategy (auto-selected for
// large-base + small-partial inputs, or forced via
// BATON_EXPERIMENTAL_PEBBLE_COMPACTOR=fold): the dest store is a copy
// of the base input (c.entries[0]), and the compacted output ADOPTS the
// base sync's id instead of re-keying everything under a fresh sync.
//
//   - Base primary and index keys: zero writes — already correct under
//     their own sync id. Work is O(partials), not O(base).
//   - Partial winners are merged into the base keyspace via the
//     engine's keep-newer path (Put*RecordsIfNewer), which compares
//     discovered_at against the incumbent and maintains indexes with
//     point tombstones proportional to overridden records only.
//   - Tie semantics: a partial record with discovered_at EQUAL to the
//     base's keeps the base record (strictly-newer required). Among
//     partials, newest-first application means the newest partial wins
//     ties — matching the K-way/sqlite fold. The base-vs-partial tie
//     differs from K-way (which would prefer the partial); cross-sync
//     equal timestamps do not occur with SDK-stamped discovered_at.
//
// Returns the adopted sync id. Sync-id continuity is deliberate: diff
// chains and parent_sync_id links into the base stay valid.
func (c *Compactor) compactPebbleFold(ctx context.Context) (string, error) {
	l := ctxzap.Extract(ctx)
	foldStart := time.Now()
	destEng, ok := enginepkg.AsEngine(c.compactedC1z)
	if !ok {
		return "", errors.New("compactPebbleFold: compacted store is not a pebble engine")
	}
	baseRec, err := destEng.LatestFinishedSyncRecord(ctx, compactableV3SyncType)
	if err != nil {
		return "", fmt.Errorf("compactPebbleFold: select base sync: %w", err)
	}
	if baseRec == nil {
		return "", fmt.Errorf("compactPebbleFold: base input %s has no finished compactable sync", c.entries[0].FilePath)
	}
	baseSyncID := baseRec.GetSyncId()
	l.Info("compactPebbleFold: folding partials into base sync",
		zap.String("base_sync_id", baseSyncID),
		zap.Int("partials", len(c.entries)-1),
	)
	unionType := baseRec.GetType()
	maxEnded := baseRec.GetEndedAt().AsTime()

	// Apply partials newest-first (reverse entry order, excluding the
	// base at entries[0]); strictly-newer-wins makes earlier
	// applications take precedence on ties.
	for i := len(c.entries) - 1; i >= 1; i-- {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		cs := c.entries[i]
		srcSyncID := ""
		if sel, ok := selectSourceSyncFromManifest(cs.FilePath); ok {
			srcSyncID = sel.syncID
			unionType = unionV3SyncType(unionType, sel.syncType)
			if sel.endedAt.After(maxEnded) {
				maxEnded = sel.endedAt
			}
		}
		w, err := dotc1z.NewStore(ctx, cs.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(c.tmpDir))
		if err != nil {
			return "", fmt.Errorf("compactPebbleFold: open input %s: %w", cs.FilePath, err)
		}
		srcEng, ok := enginepkg.AsEngine(w)
		if !ok {
			_ = w.Close(ctx)
			return "", fmt.Errorf("compactPebbleFold: input %s is not a pebble c1z", cs.FilePath)
		}
		if srcSyncID == "" {
			rec, err := srcEng.LatestFinishedSyncRecord(ctx, compactableV3SyncType)
			if err != nil || rec == nil {
				_ = w.Close(ctx)
				return "", fmt.Errorf("compactPebbleFold: input %s has no finished compactable sync: %w", cs.FilePath, err)
			}
			srcSyncID = rec.GetSyncId()
			unionType = unionV3SyncType(unionType, rec.GetType())
			if ts := rec.GetEndedAt(); ts != nil && ts.AsTime().After(maxEnded) {
				maxEnded = ts.AsTime()
			}
		}
		mergeErr := mergepkg.MergeInto(ctx, destEng, []mergepkg.SourceSync{{Engine: srcEng, SyncID: srcSyncID}}, baseSyncID)
		if cerr := w.Close(ctx); cerr != nil {
			l.Error("compactPebbleFold: error closing source store", zap.Error(cerr), zap.String("file", cs.FilePath))
		}
		if mergeErr != nil {
			return "", fmt.Errorf("compactPebbleFold: merge %s: %w", cs.FilePath, mergeErr)
		}
	}

	baseRec.SetType(unionType)
	if !maxEnded.IsZero() {
		baseRec.SetEndedAt(timestamppb.New(maxEnded))
	}
	if err := destEng.PutSyncRunRecord(ctx, baseRec); err != nil {
		return "", fmt.Errorf("compactPebbleFold: persist base sync_run: %w", err)
	}
	// The fold mutates an existing sync in place, so the cached stats
	// sidecar is stale. Recompute (key-range counts, not full
	// unmarshals); a future delta-accumulating IfNewer path could make
	// this O(partials) too.
	if err := destEng.PersistSyncStats(ctx, baseSyncID); err != nil {
		return "", fmt.Errorf("compactPebbleFold: persist stats: %w", err)
	}
	// All writes above went through the engine directly; flip the
	// store's dirty bit so Close saves the envelope.
	if !enginepkg.MarkStoreDirty(c.compactedC1z) {
		return "", errors.New("compactPebbleFold: could not mark store dirty")
	}
	l.Info("compactPebbleFold: done",
		zap.String("base_sync_id", baseSyncID),
		zap.Duration("elapsed", time.Since(foldStart)),
	)
	return baseSyncID, nil
}

// runPebbleRebuild is the standard (non-fold) Pebble compaction: start
// an empty partial sync on the dest and merge every input into it.
// compactPebble updates the sync's type to the union of the input
// types after the merge.
func (c *Compactor) runPebbleRebuild(ctx context.Context, runCtx context.Context) (string, error) {
	newSyncId, err := c.compactedC1z.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	if err != nil {
		return "", fmt.Errorf("failed to start new sync: %w", err)
	}
	if err := c.compactedC1z.EndSync(ctx); err != nil {
		return "", fmt.Errorf("failed to end sync: %w", err)
	}
	if err := c.compactPebble(runCtx, newSyncId); err != nil {
		return "", err
	}
	return newSyncId, nil
}

// copyFileForFold copies the base input to the dest path so the fold
// mutates a private working copy; the original input is never touched.
func copyFileForFold(src, dst string) error {
	in, err := os.Open(src) // #nosec G703 - src is a caller-provided compaction input path, not untrusted user input.
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
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

	pebbleCompactorMode := c.pebbleMode
	if pebbleCompactorMode == PebbleCompactorModeAuto {
		pebbleCompactorMode = c.resolvePebbleMode(ctx)
	}
	useOverlay := pebbleCompactorMode == PebbleCompactorModeOverlay
	sources := make([]mergepkg.SourceFile, 0, len(c.entries))
	unionType := v3.SyncType_SYNC_TYPE_PARTIAL
	var maxEnded time.Time

	manifestSelected := 0
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
			manifestSelected++
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
	// unpack_selected > 0 means inputs predate the manifest sync-run
	// projection and pay a full unpack just to pick a sync — a fleet
	// signal that those files should be regenerated by a current SDK.
	l.Info("compactPebble: source selection",
		zap.Int("sources", len(sources)),
		zap.Int("manifest_selected", manifestSelected),
		zap.Int("unpack_selected", len(sources)-manifestSelected),
		zap.String("mode", string(pebbleCompactorMode)),
	)

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
		if c.overlayBufferFactor > 0 {
			overlayOpts = append(overlayOpts, mergepkg.WithOverlayBufferFactor(c.overlayBufferFactor))
		}
		if c.overlayGateFraction > 0 {
			overlayOpts = append(overlayOpts, mergepkg.WithOverlayGateFraction(c.overlayGateFraction))
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
