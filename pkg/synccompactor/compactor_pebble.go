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
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	sdksync "github.com/conductorone/baton-sdk/pkg/sync"
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
func WithEngine(engine c1zstore.Engine) Option {
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
	// The folded output is re-keyed to a FRESH sync id
	// (compactPebbleFold renames the single sync-run record), so it is
	// safe for auto mode to select — the size gate does exactly that
	// for a large base with small partials.
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

// WithFoldMaxWastePercent overrides the fold waste cutover: when the
// base input's accumulated fold dead bytes (the envelope manifest's
// fold_dead_bytes counter) exceed this percent of its live payload
// bytes, auto mode forces an overlay rebuild instead of another fold,
// reclaiming the waste. Zero (the default) uses
// defaultFoldMaxWastePercent; negative disables the waste cutover.
func WithFoldMaxWastePercent(pct int64) Option {
	return func(c *Compactor) {
		c.foldMaxWastePct = pct
	}
}

// foldMaxWastePercent resolves the configured waste cutover, treating
// zero as the default.
func (c *Compactor) foldMaxWastePercent() int64 {
	if c.foldMaxWastePct != 0 {
		return c.foldMaxWastePct
	}
	return defaultFoldMaxWastePercent
}

// Fold cutover thresholds gate the auto fold/overlay choice in
// resolvePebbleMode. Fold's win is conditional on the base dominating
// total input volume: it pays a fixed per-source cost (envelope +
// pebble open, ~10ms each) plus per-record raw keep-newer writes for
// every partial record, but does ZERO work for base records and
// splices the base's unchanged frames at save.
//
// The thresholds were re-derived after the fold merge went raw
// (byte-level keep-newer, no proto round-trip — mergeBucketRawIfNewer)
// and the overlay's whole-source path switched to verbatim index
// copies — both strategies got faster, and the crossover sweep
// (TestProdScaleFoldOverlayCrossover, 117MB base) puts the crossing
// at ~7% partial byte volume: fold wins at 4.9% (4.8s vs 6.1s) and
// loses mildly from 8.9% up (6.2s vs 5.5s), with overlay's lead
// roughly flat (~15%) through 41%. Fold's output also grows with the
// override ratio — replaced records leave dead bytes inside the
// spliced base frames (+18% at 8.9%, +58% at 41%) — so past the
// crossing overlay is better on both axes.
//
// At the production shape the gate exists for (partials ≲ 5% of a
// large base) fold wins at every measured scale: GB-scale skewed
// (~1% ratio) 11.3s vs overlay's 20.8s, and fixture-scale bases all
// the way down to KBs. The old 256MiB min-base requirement is
// therefore gone — fold's win comes from splicing the base's
// unchanged compressed frames at save instead of re-encoding the
// whole output envelope, which holds at any base size.
//
// The waste cutover bounds how much dead weight consecutive folds may
// accumulate before auto mode forces a rebuild. Every record a fold
// overrides leaves its raw bytes shadowed inside the spliced base
// frames (the crossover sweep measured +18% output at an 8.9%
// override ratio, +58% at 41%), and that waste taxes every subsequent
// download, open, and save until a rebuild reclaims it. The fold
// merge counts the shadowed bytes exactly (FoldStats.DeadBytes — the
// incumbent is already in hand at override time), the counter is
// carried in the envelope manifest (fold_dead_bytes) across
// consecutive folds, and a rebuild resets it to zero. 15% sits below
// the band where the sweep showed overlay winning on both axes
// (~18% bloat) while still amortizing one rebuild over several folds
// at production override ratios. Override via WithFoldMaxWastePercent.
const (
	defaultFoldMinBaseBytes      int64 = 0  // no minimum — fold wins at every measured base size
	defaultFoldMaxPartialPercent int64 = 10 // partials ≤ 10% of base size
	defaultFoldMaxWastePercent   int64 = 15 // accumulated dead bytes ≤ 15% of live payload bytes
)

// foldMinBaseBytes is the smallest base input (envelope file size) for
// which the auto cutover picks fold.
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
// percent of the base file size, for which the auto cutover picks fold.
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
// = "overlay" / "kway" / "fold") is honored as-is. Otherwise the size
// gate selects fold for a large base with small partials and overlay
// for everything else.
//
// Fold is auto-selected again now that a folded c1z gets a fresh sync
// id (compactPebbleFold renames the single sync-run record — a
// metadata-only write since keys carry no sync_id), so C1's compaction
// bookkeeping sees a new LatestCompactedSyncId rather than mistaking the
// fold for "no new compaction". The gate reads only file sizes
// (os.Stat); nothing is unpacked.
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
	// baseBytes > 0 keeps an unstat-able base (size 0) out of the fold
	// path so the open fails later with the canonical error.
	if baseBytes > 0 && baseBytes >= minBase && partialBytes*100 <= baseBytes*maxPct {
		mode = PebbleCompactorModeFold
	}
	var deadBytes, liveBytes int64
	wastePct := c.foldMaxWastePercent()
	if mode == PebbleCompactorModeFold && wastePct > 0 {
		var exceeded bool
		deadBytes, liveBytes, exceeded = foldWaste(c.entries[0].FilePath)
		// Division form of deadBytes*100 > liveBytes*wastePct: the
		// cross-multiplied version overflows int64 around 92 PB
		// (deadBytes*100) / 615 PB (liveBytes*wastePct), so divide
		// instead. wastePct > 0 is guaranteed by the enclosing
		// condition; integer truncation is immaterial at byte scale.
		if exceeded || (liveBytes > 0 && deadBytes/wastePct > liveBytes/100) {
			// Accumulated fold waste has crossed the cutover: rebuild
			// to reclaim it instead of folding more dead weight in.
			mode = PebbleCompactorModeOverlay
		}
	}
	l.Info("pebble compactor: auto-selected mode",
		zap.String("mode", string(mode)),
		zap.Int64("base_bytes", baseBytes),
		zap.Int64("partial_bytes", partialBytes),
		zap.Int64("fold_min_base_bytes", minBase),
		zap.Int64("fold_max_partial_pct", maxPct),
		zap.Int64("fold_dead_bytes", deadBytes),
		zap.Int64("fold_live_bytes", liveBytes),
		zap.Int64("fold_max_waste_pct", wastePct),
		zap.Int("partials", len(c.entries)-1),
	)
	return mode
}

// foldWaste reads the base envelope's accumulated fold waste. The
// first return is the manifest's fold_dead_bytes counter; the second
// is the live payload bytes — the indexed trailer's total_raw_size
// minus the dead bytes (three small preads, no payload unpack). The
// bool short-circuits the degenerate cases: when dead bytes are
// recorded but live bytes can't be computed (non-indexed payload — no
// cheap raw-size source — or a corrupt trailer) it returns true,
// erring toward the rebuild, which is the safe direction (a rebuild
// is always correct, just slower). A base with no recorded waste, or
// one that isn't a readable v3 envelope, returns all zeros — the gate
// stays on fold and a genuinely bad file fails later with the
// canonical open error.
func foldWaste(path string) (int64, int64, bool) {
	f, err := os.Open(path) // #nosec G703 - path is a caller-provided compaction input, not untrusted user input.
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()
	m, err := formatv3.ReadManifestHeader(f)
	if err != nil {
		return 0, 0, false
	}
	dead := m.GetFoldDeadBytes()
	if dead <= 0 {
		return dead, 0, false
	}
	idx, err := formatv3.ReadIndexedFrameIndex(f)
	if err != nil {
		return dead, 0, true
	}
	live := idx.GetTotalRawSize() - dead
	if live <= 0 {
		return dead, live, true
	}
	return dead, live, false
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
	if _, ok := dotc1z.EngineDriverFor(c1zstore.EnginePebble); ok {
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
// of the base input (c.entries[0]) into which the partials' winners are
// merged, and the result is stamped with a FRESH sync id.
//
//   - Base primary and index keys: zero writes — the data keyspace
//     carries no sync_id, so folding and the final rename touch none of
//     them. Work is O(partials), not O(base).
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
//   - The single sync-run record is overwritten with the fresh id
//     (union type, max ended_at, no parent link), and the stats
//     sidecar is recomputed under it.
//
// Returns the fresh sync id.
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
	var foldStats mergepkg.FoldStats
	var partialSyncIDs []string
	var partialTokens []string
	// SQLite/v1 partials are converted to Pebble in the tmp dir before being
	// folded in; their converted copies are removed when this run completes.
	var convertedInputs []string
	defer func() {
		for _, path := range convertedInputs {
			_ = os.Remove(path)
		}
	}()
	for i := len(c.entries) - 1; i >= 1; i-- {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		cs := c.entries[i]
		sourcePath := cs.FilePath
		format, err := readCompactionInputFormat(sourcePath)
		if err != nil {
			return "", err
		}
		if format == dotc1z.C1ZFormatV1 {
			convertedPath, err := c.convertSQLiteInputToPebble(ctx, cs)
			if err != nil {
				return "", err
			}
			convertedInputs = append(convertedInputs, convertedPath)
			sourcePath = convertedPath
		}
		srcSyncID := ""
		if sel, ok := selectSourceSyncFromManifest(sourcePath); ok {
			srcSyncID = sel.syncID
			unionType = unionV3SyncType(unionType, sel.syncType)
			if sel.endedAt.After(maxEnded) {
				maxEnded = sel.endedAt
			}
		}
		w, err := dotc1z.NewStore(ctx, sourcePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(c.tmpDir), dotc1z.WithDecoderPool(c.decoderPool))
		if err != nil {
			return "", fmt.Errorf("compactPebbleFold: open input %s: %w", sourcePath, err)
		}
		srcEng, ok := enginepkg.AsEngine(w)
		if !ok {
			_ = w.Close(ctx)
			return "", fmt.Errorf("compactPebbleFold: input %s is not a pebble c1z", sourcePath)
		}
		if srcSyncID == "" {
			rec, err := srcEng.LatestFinishedSyncRecord(ctx, compactableV3SyncType)
			if err != nil {
				_ = w.Close(ctx)
				return "", fmt.Errorf("compactPebbleFold: input %s: select compactable sync: %w", sourcePath, err)
			}
			if rec == nil {
				_ = w.Close(ctx)
				return "", fmt.Errorf("compactPebbleFold: input %s has no finished compactable sync", sourcePath)
			}
			srcSyncID = rec.GetSyncId()
			unionType = unionV3SyncType(unionType, rec.GetType())
			if ts := rec.GetEndedAt(); ts != nil && ts.AsTime().After(maxEnded) {
				maxEnded = ts.AsTime()
			}
		}
		partialSyncIDs = append(partialSyncIDs, srcSyncID)
		partialTokens = append(partialTokens, readSourceSyncToken(ctx, srcEng, srcSyncID))

		mergeStats, mergeErr := mergepkg.MergeInto(ctx, destEng, []mergepkg.SourceSync{{Engine: srcEng, SyncID: srcSyncID}}, baseSyncID)
		foldStats.Add(mergeStats)
		if cerr := w.Close(ctx); cerr != nil {
			l.Error("compactPebbleFold: error closing source store", zap.Error(cerr), zap.String("file", sourcePath))
		}
		if mergeErr != nil {
			return "", fmt.Errorf("compactPebbleFold: merge %s: %w", sourcePath, mergeErr)
		}
	}

	// Record the bytes this fold shadowed in the base keyspace. The
	// store inherited the base manifest's running fold_dead_bytes at
	// open (the dest is a byte copy of the base), so adding the delta
	// keeps the manifest counter cumulative across consecutive folds;
	// resolvePebbleMode's waste cutover reads it to force the eventual
	// rebuild that reclaims the dead weight.
	if foldStats.DeadBytes > 0 {
		if !enginepkg.AddFoldDeadBytes(c.compactedC1z, foldStats.DeadBytes) {
			return "", errors.New("compactPebbleFold: could not record fold dead bytes")
		}
	}

	// Only touch grant digests for the entitlement partitions this fold
	// actually wrote a grant into. The dest started as a byte copy of a
	// sealed base, whose digest state is already exactly correct for
	// every OTHER entitlement; invalidating (and later recalling
	// RepairMissingGrantDigests to rebuild) the whole file on every
	// fold — even one where a single entitlement out of thousands
	// changed — would reintroduce the O(base) cost fold exists to
	// avoid. InvalidateGrantDigestPartitions drops exactly the touched
	// partitions (+ the now-stale whole-file root);
	// RepairMissingGrantDigests then rebuilds exactly what's missing,
	// each from a targeted scan of just that entitlement's own grants —
	// never a full-file scan — and recomputes the whole-file root from
	// the (small) digest keyspace itself. A fold whose partials add
	// nothing new (a common steady-state case — re-running compaction
	// with no new data, or partials that only touch
	// resources/entitlements) touches nothing at all.
	//
	// This result is FINAL only when the caller (Compact) goes on to
	// skip grant expansion (WithSkipGrantExpansion, or a partial-typed
	// union) — nothing else touches c.compactedC1z before Close in that
	// path (GetSync is a pure read; Cleanup is a hard no-op for the
	// Pebble engine). When expansion runs instead, its own grant writes
	// use dedicated write paths (PutExpandedGrantRecords /
	// PutSynthesizedGrantRecords / the layer-session ingest, not
	// PutGrantRecords) whose invalidation correctness doesn't matter
	// here: expandGrants' syncer.Sync ALWAYS calls store.EndSync
	// afterward — even for a no-op expansion — and Adapter.EndSync's
	// finalize unconditionally runs a FULL digest rebuild
	// (BuildDeferredGrantIndexes or BuildGrantDigests) whenever the
	// digest index is enabled, with no branch that skips both. So
	// whatever this targeted repair produces gets unconditionally
	// superseded by a full, correct rebuild the moment expansion runs —
	// safe, but this optimization's actual win is scoped to
	// skip-expansion compactions; a fold whose base is a full sync
	// (expansion NOT skipped) pays for both the targeted repair AND the
	// subsequent full rebuild. See
	// TestCompactPebbleFoldWithExpansionRebuildsFullyRegardless.
	//
	// When the dest engine has the digest index DISABLED, touched
	// digests must be dropped instead of repaired: the byte copy
	// carried the sealed base's digest state unconditionally, readers
	// serve whatever is stored regardless of this writer's flag
	// (grantDigestsPresent is probed from the keyspace at Open), and
	// every rebuild path — this one, EndSync's finalize,
	// RepairMissingGrantDigests itself — gates on the same flag, so
	// stale digests would ship as present-but-wrong with nothing left
	// to heal them. Absent is always safe (present-means-exact).
	// Dropping only on a grant write, rather than skipping the digest
	// bucket copy up front, keeps the no-grant-write fold preserving
	// the base's still-exact digests for free even on a disabled-index
	// engine. See TestCompactPebbleFoldDigestIndexDisabledDropsDigests.
	if len(foldStats.TouchedGrantPartitions) > 0 {
		if !destEng.GrantDigestIndexEnabled() {
			if err := destEng.DropAllGrantDigestState(ctx); err != nil {
				return "", fmt.Errorf("compactPebbleFold: drop grant digest state (digest index disabled): %w", err)
			}
			l.Info("compactPebbleFold: grant writes with digest index disabled; dropped the base's copied digest state",
				zap.Int("touched_partitions", len(foldStats.TouchedGrantPartitions)))
		} else {
			partitions := make([]string, 0, len(foldStats.TouchedGrantPartitions))
			for p := range foldStats.TouchedGrantPartitions {
				partitions = append(partitions, p)
			}
			if err := destEng.InvalidateGrantDigestPartitions(ctx, partitions); err != nil {
				return "", fmt.Errorf("compactPebbleFold: invalidate grant digest partitions: %w", err)
			}
			if err := destEng.RepairMissingGrantDigests(ctx); err != nil {
				return "", fmt.Errorf("compactPebbleFold: repair grant digests: %w", err)
			}
			l.Info("compactPebbleFold: repaired grant digests for touched entitlements",
				zap.Int("touched_partitions", len(partitions)))
		}
	} else {
		l.Info("compactPebbleFold: no grant writes; base grant digest state left untouched")
	}

	// Optionally compact the folded LSM before save. Off by default:
	// a compaction rewrites the SSTs that overlap the partials' writes
	// — for scattered overrides that is most of the base — which both
	// costs O(base) on the critical path and makes those files no
	// longer byte-identical to the source envelope's frames, so the
	// splice-at-save degrades to a full re-encode. The payoff is an
	// output with zero shadowed records (overrides otherwise leave
	// dead bytes inside the spliced base frames). Experimental knob
	// for measuring that trade-off; the long-term plan for reclaiming
	// accumulated fold bloat is routing the occasional compaction to
	// the rebuild path instead.
	if os.Getenv("BATON_EXPERIMENTAL_FOLD_COMPACT") == "1" {
		compactStart := time.Now()
		if err := destEng.CompactAllRanges(ctx); err != nil {
			return "", fmt.Errorf("compactPebbleFold: compact ranges: %w", err)
		}
		l.Info("compactPebbleFold: compacted LSM before save",
			zap.Duration("elapsed", time.Since(compactStart)))
	}

	// Mint a fresh sync id for the folded output. Renaming a v3 c1z's
	// sync is a metadata-only write now that keys carry no sync_id (the
	// records merged into the base keyspace are untouched by the
	// rename), so the historical objection to auto-fold — that adopting
	// the base id left C1's LatestCompactedSyncId unchanged and looked
	// like "no new compaction" — no longer applies. ParentSyncId is
	// cleared: the base sync's record is overwritten by this rename, so
	// a lineage link would dangle, and the rebuild path's compacted
	// output carries no parent either.
	newSyncID := ksuid.New().String()
	baseRec.SetSyncId(newSyncID)
	baseRec.SetParentSyncId("")
	baseRec.SetType(unionType)
	if !maxEnded.IsZero() {
		baseRec.SetEndedAt(timestamppb.New(maxEnded))
	}
	// PutSyncRunRecord overwrites the single fixed sync-run key, so the
	// file's one sync-run record now carries newSyncID. (The compactor
	// GetSync's this id right after and asserts it matches — the
	// engine's GetSyncRunRecord id-match guard enforces it.)
	if err := destEng.PutSyncRunRecord(ctx, baseRec); err != nil {
		return "", fmt.Errorf("compactPebbleFold: persist folded sync_run: %w", err)
	}
	// The fold rewrote the keyspace, so the cached stats sidecar is
	// stale. Recompute under the NEW id so the sidecar's SyncId — and
	// the envelope manifest's sync-run projection built from it at save
	// — match the renamed sync. Key-range counts, not full unmarshals.
	if err := destEng.PersistSyncStats(ctx, newSyncID); err != nil {
		return "", fmt.Errorf("compactPebbleFold: persist stats: %w", err)
	}
	// Rewrite the token with compaction provenance: the base token's
	// timing stats describe the base sync's collection run, so the
	// section re-attributes them (stats_sync_id) and adds what this fold
	// merged. Provenance is best-effort — it never fails the compaction.
	outputStats, statsErr := enginepkg.ReadSyncStatsRecord(ctx, destEng, newSyncID)
	if statsErr != nil {
		l.Warn("compactPebbleFold: could not read output stats for provenance", zap.Error(statsErr))
	}
	compactedToken, tokenErr := sdksync.BuildCompactedToken(baseRec.GetSyncToken(), sdksync.CompactionTokenInput{
		Mode:           string(PebbleCompactorModeFold),
		BaseSyncID:     baseSyncID,
		PartialSyncIDs: partialSyncIDs,
		PartialTokens:  partialTokens,
		RecordCounts:   compactionRecordCounts(outputStats, &foldStats),
	})
	if tokenErr != nil {
		l.Warn("compactPebbleFold: could not build compaction provenance token", zap.Error(tokenErr))
	} else {
		baseRec.SetSyncToken(compactedToken)
		if err := destEng.PutSyncRunRecord(ctx, baseRec); err != nil {
			return "", fmt.Errorf("compactPebbleFold: persist provenance token: %w", err)
		}
	}
	// All writes above went through the engine directly; flip the
	// store's dirty bit so Close saves the envelope.
	if !enginepkg.MarkStoreDirty(c.compactedC1z) {
		return "", errors.New("compactPebbleFold: could not mark store dirty")
	}
	l.Info("compactPebbleFold: done",
		zap.String("base_sync_id", baseSyncID),
		zap.String("folded_sync_id", newSyncID),
		zap.Int64("overridden_records", foldStats.OverriddenRecords),
		zap.Int64("dead_bytes", foldStats.DeadBytes),
		zap.Duration("elapsed", time.Since(foldStart)),
	)
	return newSyncID, nil
}

// rebuildCompactedGrantDigests builds the by_entitlement_principal_hash
// index and per-entitlement grant digests (plus the whole-file root,
// see manifest.go) for a compacted output, once the merge has finished
// writing its final grant keyspace — so a compacted file carries built
// digests indistinguishable from a seal-time build, instead of shipping
// with none (every merge strategy writes the final grant records
// through its own path — raw keep-newer for fold, run-file/SST
// materialization for k-way and overlay — and none of them maintains
// the hash index or digests inline).
//
// This reuses Engine.BuildGrantDigests verbatim: the SAME standalone
// build EndSync runs when a sync's deferred index pass never fired
// (grants already have a correct by_principal / by_needs_expansion,
// nothing to rebuild there). It scans the primary grants exactly once —
// it does NOT re-derive by_principal or rewrite the primary grant
// keyspace, so it does not repeat the O(grants) rebuild work the merge
// strategies already did. Because every hash-index row and digest node
// is recomputed from scratch off the FINAL winner records, there is no
// verbatim-copy path that could carry forward a superseded row's stale
// content hash — unlike a fused emit-during-merge design, this build
// has no notion of "copy an index entry from a source", so the classic
// hazard (two sources sharing a grant identity but different source
// sets, whose content hash covers the source set) cannot arise: every
// row is derived from the one winner value that survived the merge.
//
// Failure policy matches the seal build exactly (same function): an
// error other than context cancellation drops all digest state and
// logs, and returns nil — a compacted file with no digests is safe
// (present-means-exact), so a digest-build hiccup must never fail the
// compaction. Interrupt/restart safety is free: this runs once, after
// the merge's writes are already durably committed, so a crash or
// cancellation before this call simply leaves the output with no
// digest state at all (never a partial one) — the conservative design
// the digest package's present-means-exact contract calls for.
//
// No-op when the destination engine was opened with the digest index
// disabled (WithGrantDigestIndex(false)), matching EndSync's own gate
// (GrantDigestIndexEnabled).
//
// Callers: only k-way and overlay use this (unconditionally) — those
// modes materialize every record fresh on every run regardless of
// digests, so a full rebuild is proportional to work already
// unavoidable. Fold does NOT call this: its dest starts as a byte copy
// of a sealed base whose digest state is already correct for every
// untouched entitlement, so a full rebuild on every fold — even one
// where a handful of entitlements out of thousands changed — would
// reintroduce the O(base) cost fold exists to avoid. Fold instead
// calls Engine.InvalidateGrantDigestPartitions +
// Engine.RepairMissingGrantDigests to invalidate and rebuild EXACTLY
// the entitlements it touched (see compactPebbleFold).
func rebuildCompactedGrantDigests(ctx context.Context, destEng *enginepkg.Engine) error {
	if !destEng.GrantDigestIndexEnabled() {
		return nil
	}
	if err := destEng.BuildGrantDigests(ctx); err != nil {
		return fmt.Errorf("build grant digests: %w", err)
	}
	return nil
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

// readSourceSyncToken returns a source sync's marshalled token, or "" when
// the record is unavailable (e.g. converted sqlite inputs carry none).
func readSourceSyncToken(ctx context.Context, eng *enginepkg.Engine, syncID string) string {
	rec, err := eng.GetSyncRunRecord(ctx, syncID)
	if err != nil || rec == nil {
		return ""
	}
	return rec.GetSyncToken()
}

// compactionRecordCounts renders per-type provenance counts for the token's
// compaction section. fold carries added/replaced attribution (fold mode
// only); rebuild modes pass nil and report output totals alone.
func compactionRecordCounts(output *v3.SyncStatsRecord, fold *mergepkg.FoldStats) map[string]sdksync.CompactionRecordCounts {
	if output == nil {
		return nil
	}
	totals := map[string]int64{
		"resource_types": output.GetResourceTypes(),
		"resources":      output.GetResources(),
		"entitlements":   output.GetEntitlements(),
		"grants":         output.GetGrants(),
	}
	out := make(map[string]sdksync.CompactionRecordCounts, len(totals))
	for bucket, total := range totals {
		counts := sdksync.CompactionRecordCounts{Output: total}
		if fold != nil {
			counts.Added = fold.AddedByBucket[bucket]
			counts.Replaced = fold.ReplacedByBucket[bucket]
			carried := total - counts.Added - counts.Replaced
			if carried < 0 {
				carried = 0
			}
			counts.Carried = carried
		}
		out[bucket] = counts
	}
	return out
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

// readCompactionInputFormat reads the c1z header of path and returns its
// on-disk format, rejecting anything that is not a supported v1/v3 c1z.
func readCompactionInputFormat(path string) (dotc1z.C1ZFormat, error) {
	f, err := os.Open(path) // #nosec G304 - compaction inputs are caller-provided c1z paths.
	if err != nil {
		return dotc1z.C1ZFormatUnknown, fmt.Errorf("compactPebble: open input header %s: %w", path, err)
	}
	defer f.Close()
	format, err := dotc1z.ReadHeaderFormat(f)
	if err != nil {
		return dotc1z.C1ZFormatUnknown, fmt.Errorf("compactPebble: read input header %s: %w", path, err)
	}
	switch format {
	case dotc1z.C1ZFormatV1, dotc1z.C1ZFormatV3:
		return format, nil
	default:
		return dotc1z.C1ZFormatUnknown, fmt.Errorf("compactPebble: unsupported c1z format %s for %s", format, path)
	}
}

func resolveSQLiteCompactionSyncID(ctx context.Context, store *dotc1z.C1File, explicitSyncID string) (string, error) {
	if explicitSyncID != "" {
		return explicitSyncID, nil
	}

	candidates := []connectorstore.SyncType{
		connectorstore.SyncTypeFull,
		connectorstore.SyncTypeResourcesOnly,
		connectorstore.SyncTypePartial,
	}
	var best *reader_v2.SyncRun
	for _, st := range candidates {
		resp, err := store.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{
			SyncType: string(st),
		}.Build())
		if err != nil {
			return "", err
		}
		sync := resp.GetSync()
		if sync == nil {
			continue
		}
		syncEndedAt := sync.GetEndedAt().AsTime()
		bestEndedAt := time.Time{}
		if best != nil {
			bestEndedAt = best.GetEndedAt().AsTime()
		}
		if best == nil || syncEndedAt.After(bestEndedAt) || (syncEndedAt.Equal(bestEndedAt) && sync.GetId() > best.GetId()) {
			best = sync
		}
	}
	if best == nil {
		return "", fmt.Errorf(
			"no finished compactable sync found in sqlite input (diff sync types %q/%q are not compactable)",
			string(connectorstore.SyncTypePartialUpserts),
			string(connectorstore.SyncTypePartialDeletions),
		)
	}
	return best.GetId(), nil
}

// convertSQLiteInputToPebble converts a v1/SQLite compaction input into a
// freshly-written Pebble (v3) c1z in the compactor tmp dir and returns the
// path to the converted file. The caller owns the returned path and must
// remove it when the compaction run completes.
func (c *Compactor) convertSQLiteInputToPebble(ctx context.Context, cs *CompactableSync) (string, error) {
	if cs == nil {
		return "", errors.New("compactPebble: nil compactable sync")
	}
	tmp, err := os.CreateTemp(c.tmpDir, "sqlite-input-*.pebble.c1z")
	if err != nil {
		return "", fmt.Errorf("compactPebble: create conversion temp file: %w", err)
	}
	convertedPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(convertedPath)
		return "", fmt.Errorf("compactPebble: close conversion temp file: %w", err)
	}
	// ToPebble requires the destination path to not exist.
	if err := os.Remove(convertedPath); err != nil {
		return "", fmt.Errorf("compactPebble: remove conversion temp placeholder: %w", err)
	}

	store, err := dotc1z.NewStore(ctx, cs.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(c.tmpDir))
	if err != nil {
		return "", fmt.Errorf("compactPebble: open sqlite input for conversion %s: %w", cs.FilePath, err)
	}
	sqliteStore, ok := dotc1z.AsSQLiteStore(store)
	if !ok {
		_ = store.Close(ctx)
		return "", fmt.Errorf("compactPebble: expected SQLite input while converting %s, got %T", cs.FilePath, store)
	}
	syncID, err := resolveSQLiteCompactionSyncID(ctx, sqliteStore, cs.SyncID)
	if err != nil {
		_ = store.Close(ctx)
		_ = os.Remove(convertedPath)
		return "", fmt.Errorf("compactPebble: select sqlite input sync %s: %w", cs.FilePath, err)
	}
	if _, err := sqliteStore.ToPebble(ctx, convertedPath, syncID, dotc1z.WithConvertTmpDir(c.tmpDir)); err != nil {
		_ = store.Close(ctx)
		_ = os.Remove(convertedPath)
		return "", fmt.Errorf("compactPebble: convert sqlite input %s to pebble: %w", cs.FilePath, err)
	}
	if err := store.Close(ctx); err != nil {
		_ = os.Remove(convertedPath)
		return "", fmt.Errorf("compactPebble: close sqlite input after conversion %s: %w", cs.FilePath, err)
	}
	return convertedPath, nil
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

	// runPebbleRebuild's StartNewSync→EndSync left the dest engine SEALED
	// (writes refused, compactions paused). The merge below writes the whole
	// compacted dataset — overlay mode through raw memtable batches — so
	// bind the sync first: unseals the engine and resumes the compaction
	// scheduler. Without this, L0 accumulates with no compactions granted
	// until pebble stalls writes at L0StopWritesThreshold, permanently
	// (nothing else resumes the scheduler mid-merge).
	//
	// Yes, "SetCurrentSync to restart compactions" is an odd spelling. It
	// is deliberate: unseal/resume is not a public engine operation, because
	// the sealed state exists precisely to guarantee "no record writes
	// without a bound sync". Binding the sync we're about to write under is
	// the one sanctioned way to declare that intent, and unseal+resume ride
	// along as consequences (see Engine.SetCurrentSync / Engine.seal). An
	// exported ResumeCompactions-style escape hatch would let callers write
	// on a sealed engine again, recreating the very hang this fixes.
	if err := destEng.SetCurrentSync(ctx, newSyncId); err != nil {
		return fmt.Errorf("compactPebble: bind dest sync: %w", err)
	}

	pebbleCompactorMode := c.pebbleMode
	if pebbleCompactorMode == PebbleCompactorModeAuto {
		pebbleCompactorMode = c.resolvePebbleMode(ctx)
	}
	useOverlay := pebbleCompactorMode == PebbleCompactorModeOverlay
	sources := make([]mergepkg.SourceFile, 0, len(c.entries))
	unionType := v3.SyncType_SYNC_TYPE_PARTIAL
	var maxEnded time.Time
	rebuildBaseSyncID := ""
	var rebuildPartialSyncIDs []string

	manifestSelected := 0
	// SQLite/v1 inputs are converted to Pebble in the tmp dir before being
	// merged; their converted copies are removed when this run completes.
	var convertedInputs []string
	defer func() {
		for _, path := range convertedInputs {
			_ = os.Remove(path)
		}
	}()
	for i := len(c.entries) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		cs := c.entries[i]
		sourcePath := cs.FilePath
		format, err := readCompactionInputFormat(sourcePath)
		if err != nil {
			return err
		}
		if format == dotc1z.C1ZFormatV1 {
			convertedPath, err := c.convertSQLiteInputToPebble(ctx, cs)
			if err != nil {
				return err
			}
			convertedInputs = append(convertedInputs, convertedPath)
			sourcePath = convertedPath
		}

		// Fast path: read the latest finished compactable sync (and its
		// cached stats) from the envelope manifest's sync-run projection
		// — a header read, no payload unpack. Files written before the
		// projection existed fall back to the unpack path below.
		if sel, ok := selectSourceSyncFromManifest(sourcePath); ok {
			manifestSelected++
			sources = append(sources, mergepkg.SourceFile{Path: sourcePath, SyncID: sel.syncID, Stats: sel.stats, DecoderPool: c.decoderPool})
			unionType = unionV3SyncType(unionType, sel.syncType)
			if sel.endedAt.After(maxEnded) {
				maxEnded = sel.endedAt
			}
			continue
		}

		source, syncType, endedAt, err := func() (mergepkg.SourceFile, v3.SyncType, time.Time, error) {
			var zeroSource mergepkg.SourceFile
			w, err := dotc1z.NewStore(ctx, sourcePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(c.tmpDir), dotc1z.WithDecoderPool(c.decoderPool))
			if err != nil {
				return zeroSource, v3.SyncType_SYNC_TYPE_UNSPECIFIED, time.Time{}, fmt.Errorf("compactPebble: open input %s: %w", sourcePath, err)
			}
			defer func() {
				if cerr := w.Close(ctx); cerr != nil {
					l.Error("compactPebble: error closing source store", zap.Error(cerr), zap.String("file", sourcePath))
				}
			}()

			srcEng, ok := enginepkg.AsEngine(w)
			if !ok {
				return zeroSource, v3.SyncType_SYNC_TYPE_UNSPECIFIED, time.Time{}, fmt.Errorf("compactPebble: input %s is not a pebble c1z", sourcePath)
			}
			rec, err := srcEng.LatestFinishedSyncRecord(ctx, compactableV3SyncType)
			if err != nil {
				return zeroSource, v3.SyncType_SYNC_TYPE_UNSPECIFIED, time.Time{}, fmt.Errorf("compactPebble: select source sync for %s: %w", sourcePath, err)
			}
			if rec == nil {
				return zeroSource, v3.SyncType_SYNC_TYPE_UNSPECIFIED, time.Time{}, fmt.Errorf("compactPebble: input %s has no finished compactable sync (diff syncs are not compactable)", sourcePath)
			}

			// Record only (Path, SyncID, Stats) and fully close the store,
			// removing its unpacked directory. The merge re-unpacks each
			// source when its chunk is processed and removes it when the
			// chunk closes, so peak disk is O(fan-in) source directories,
			// not O(len(entries)). The fallback unpacks one source at a
			// time and pays one extra unpack per source (selection + merge).
			source := mergepkg.SourceFile{Path: sourcePath, SyncID: rec.GetSyncId(), DecoderPool: c.decoderPool}
			if useOverlay {
				stats, ok, err := enginepkg.CachedSyncStats(ctx, srcEng, rec.GetSyncId())
				if err != nil {
					return zeroSource, v3.SyncType_SYNC_TYPE_UNSPECIFIED, time.Time{}, fmt.Errorf("compactPebble: cached stats for %s: %w", sourcePath, err)
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
	for i, source := range sources {
		// The loop above appends in reverse entry order, so the base
		// (entries[0]) is the last-appended source.
		if i == len(sources)-1 {
			rebuildBaseSyncID = source.SyncID
			continue
		}
		rebuildPartialSyncIDs = append(rebuildPartialSyncIDs, source.SyncID)
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

	if err := rebuildCompactedGrantDigests(ctx, destEng); err != nil {
		return fmt.Errorf("compactPebble: %w", err)
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
	// The merge accumulated the dest stats while writing winners, so
	// persist those instead of re-scanning the freshly written output.
	if statsRec != nil {
		if err := destEng.PersistComputedSyncStats(ctx, newSyncId, statsRec); err != nil {
			return fmt.Errorf("compactPebble: persist stats: %w", err)
		}
	} else {
		if err := destEng.PersistSyncStats(ctx, newSyncId); err != nil {
			return fmt.Errorf("compactPebble: persist stats: %w", err)
		}
		recomputed, statsErr := enginepkg.ReadSyncStatsRecord(ctx, destEng, newSyncId)
		if statsErr != nil {
			l.Warn("compactPebble: could not read output stats for provenance", zap.Error(statsErr))
		} else {
			statsRec = recomputed
		}
	}
	// Stamp compaction provenance on the (otherwise empty) rebuild token.
	// Rebuild merges lose per-source attribution in their run-file paths,
	// so record counts carry output totals only, and the partials' timing
	// aggregate is fold-only — collecting rebuild source tokens would pay
	// a second envelope unpack per source. Best-effort: provenance never
	// fails the compaction.
	mode := PebbleCompactorModeKWay
	if useOverlay {
		mode = PebbleCompactorModeOverlay
	}
	compactedToken, tokenErr := sdksync.BuildCompactedToken(rec.GetSyncToken(), sdksync.CompactionTokenInput{
		Mode:           string(mode),
		BaseSyncID:     rebuildBaseSyncID,
		PartialSyncIDs: rebuildPartialSyncIDs,
		RecordCounts:   compactionRecordCounts(statsRec, nil),
	})
	if tokenErr != nil {
		l.Warn("compactPebble: could not build compaction provenance token", zap.Error(tokenErr))
	} else {
		rec.SetSyncToken(compactedToken)
	}
	if err := destEng.PutSyncRunRecord(ctx, rec); err != nil {
		return fmt.Errorf("compactPebble: persist dest sync_run: %w", err)
	}
	return nil
}
