// Package pebble implements the record-merge strategies behind v3
// (Pebble-engine) c1z compaction.
//
// Three strategies live here. Which one runs is decided per compaction
// by synccompactor.resolvePebbleMode. Fold is auto-selected for the
// large-base/small-partials shape (summed partials ≤ 10% of the base
// file, no minimum base size) now that a folded c1z gets a fresh sync
// id — compactPebbleFold renames the single sync-run record, a
// metadata-only write since keys carry no sync_id, so C1's compaction
// bookkeeping sees a new LatestCompactedSyncId instead of mistaking the
// fold for "no new compaction". Overlay is the default for every other
// shape, and it also reclaims accumulated fold bloat: once a base's
// recorded fold_dead_bytes crosses the waste cutover
// (synccompactor.WithFoldMaxWastePercent, default 15% of live payload),
// the gate flips a fold-shaped input back to overlay to rebuild it.
// Kway is never auto-selected on its own: it is overlay's internal
// fallback and an operational escape hatch
// (BATON_EXPERIMENTAL_PEBBLE_COMPACTOR=kway). The fold cutover
// thresholds and the measured crossover data live next to that gate.
//
// # Overlay merge (MergeFilesIntoOverlay) — the default rebuild
//
// A sqlite-shaped "newest wins" merge into a fresh dest sync. Sources
// are scanned newest-to-oldest with a bounded in-memory seen set per
// bucket (128-bit suffix hash → discovered_at; see seenSuffixSet), and
// only winners are written, through raw Pebble batches that
// materialize each record and its derived index keys exactly once.
// The last source scanned (the oldest — in the production skewed
// shape, the large base) can skip the record write path entirely: its
// bucket is materialized as SST files filtered against the seen set
// (every base key not overridden by a newer source) and ingested
// wholesale (overlayWholeSourceWorthIt gates this on bucket size).
// Buckets whose estimated key count exceeds the seen-set memory bound
// (overlaySeenKeyLimit) are routed to the kway run-file path by
// overlayPlanBuckets, so overlay's worst case is kway plus a planning
// pass. Cost: O(total input volume), with the cheapest per-record
// path of the rebuild strategies; wins or ties every measured shape.
//
// # K-way merge (MergeFilesInto) — overlay's fallback
//
// A bounded-fan-in external merge sort into a fresh dest sync. Sources
// are processed in fan-in-sized chunks: each chunk's buckets are
// streamed into a sorted run file (≤ fanIn sources merge directly into
// Pebble with no run files at all); run files are then merged fanIn at
// a time per round until one generation remains, and the final
// generation is deduped (newest discovered_at wins; runRecordIsNewer)
// and materialized into SSTs that are ingested into dest. Memory stays
// bounded regardless of input count or size and there is no reliance
// on in-memory dedup state — the price is writing and re-reading every
// record through run files, strictly more I/O than overlay. Kept
// because overlay's oversized buckets need it, and as a forced-mode
// backup if overlay misbehaves in production.
//
// # In-place fold (MergeInto) — explicit-only, fastest for large bases
//
// Not a rebuild. The dest store starts as a byte copy of the base
// input, the output adopts the base sync's id, and each partial's
// records are streamed into the base keyspace through the engine's
// keep-newer puts (Put*RecordsIfNewer), which resolve conflicts
// against incumbents by discovered_at and maintain indexes with point
// tombstones for overridden records only. Base records are never
// read, decoded, or rewritten, and the envelope save splices the
// base's unchanged zstd frames instead of re-encoding them — total
// cost is O(partial volume) plus a fixed per-source open, versus
// O(total input) for the rebuilds (measured ~3x faster at a 1.1GB
// base with small partials). It loses when partial volume grows:
// every partial record pays a point read-modify-write (~0.2s/MB), so
// the (dormant) auto gate caps partials at a small fraction of the
// base. Overridden incumbents leave their bytes shadowed inside the
// spliced base frames; the merge counts them exactly (FoldStats) and
// the compactor accumulates the total in the envelope manifest's
// fold_dead_bytes, forcing an overlay rebuild once waste crosses the
// cutover (synccompactor.WithFoldMaxWastePercent, default 15% of
// live payload bytes).
//
// The package also hosts an IngestAndExcise-based Compactor, a
// byte-level primitive that atomically replaces one sync_id range in a
// destination engine with a source's view of it (see Compactor).
package pebble
