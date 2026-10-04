package pebble

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (e *Engine) migrateIDIndexFormatToStructuredV1(ctx context.Context) error {
	start := time.Now()
	l := ctxzap.Extract(ctx)
	dir, err := e.prepareStagingDir("", "pebble-id-index-migration-")
	if err != nil {
		return fmt.Errorf("id-index migration: mkdir temp: %w", err)
	}
	defer e.removeStagingDir(dir)

	sortSem := make(chan struct{}, 4)
	// 128MiB chunks (deferredIndexSpillChunkBytes): the merge holds a
	// 1MiB read buffer per chunk, so chunk size bounds the fan-in. At
	// 8MiB a 150M-grant file would merge ~4,000 chunks (~4GB of
	// buffers); at 128MiB it stays in the low hundreds. The arena
	// freelist recycles the big chunks across all four sorters.
	arenaFree := newSpillArenaFreeList(deferredIndexSpillChunkBytes, 6)
	grantPrimary := newSpillSorter(dir, "grant-primary", sortSem, deferredIndexSpillChunkBytes)
	entitlementPrimary := newSpillSorter(dir, "entitlement-primary", sortSem, deferredIndexSpillChunkBytes)
	byPrincipal := newSpillSorter(dir, "idx-grant-by-principal", sortSem, deferredIndexSpillChunkBytes)
	byNeedsExpansion := newSpillSorter(dir, "idx-grant-by-needs-expansion", sortSem, deferredIndexSpillChunkBytes)
	sorters := []*spillSorter{grantPrimary, entitlementPrimary, byPrincipal, byNeedsExpansion}
	for _, s := range sorters {
		s.free = arenaFree
	}
	defer func() {
		for _, s := range sorters {
			s.abort()
		}
	}()

	entitlementRows, err := e.emitStructuredEntitlementMigration(ctx, entitlementPrimary)
	if err != nil {
		return err
	}
	grantRows, err := e.emitStructuredGrantMigration(ctx, grantPrimary)
	if err != nil {
		return err
	}
	scanDone := time.Now()

	type replacement struct {
		name         string
		sorter       *spillSorter
		lower, upper []byte
	}
	replacements := []replacement{
		{name: "entitlement-primary", sorter: entitlementPrimary, lower: EntitlementLowerBound(), upper: EntitlementUpperBound()},
		{name: "grant-primary", sorter: grantPrimary, lower: GrantLowerBound(), upper: GrantUpperBound()},
		{name: "idx-grant-by-principal", sorter: byPrincipal, lower: GrantByPrincipalLowerBound(), upper: GrantByPrincipalUpperBound()},
		{name: "idx-grant-by-needs-expansion", sorter: byNeedsExpansion, lower: GrantByNeedsExpansionLowerBound(), upper: GrantByNeedsExpansionUpperBound()},
	}

	for _, r := range replacements {
		var path string
		var err error
		if r.name == "grant-primary" {
			path, err = finalizeGrantPrimaryMigrationSorter(ctx, e.fs(), dir, r.name, r.sorter, byPrincipal, byNeedsExpansion)
		} else {
			path, err = finalizeMigrationSorter(ctx, e.fs(), dir, r.name, r.sorter)
		}
		if err != nil {
			return err
		}
		if err := e.replaceRangeWithSST(ctx, r.lower, r.upper, path); err != nil {
			return fmt.Errorf("id-index migration: replace %s: %w", r.name, err)
		}
	}

	for _, r := range [][2][]byte{
		{EntitlementByResourceLowerBound(), EntitlementByResourceUpperBound()},
		{GrantByEntitlementLowerBound(), GrantByEntitlementUpperBound()},
		{GrantByPrincipalResourceTypeLowerBound(), GrantByPrincipalResourceTypeUpperBound()},
		{GrantByEntitlementResourceLowerBound(), GrantByEntitlementResourceUpperBound()},
	} {
		if err := e.db.DropKeyRange(r[0], r[1], pebble.Sync); err != nil {
			return fmt.Errorf("id-index migration: delete dropped range: %w", err)
		}
	}

	if err := e.recomputeStatsAfterIDIndexMigration(ctx); err != nil {
		return err
	}
	e.noteEntitlementKeyspaceWrite()
	if err := e.withWriteAllowSealed(func() error { return e.writeIDIndexFormat(idIndexFormatCurrent) }); err != nil {
		return err
	}
	e.migratedOnOpen = true
	l.Info("id-index migration: structured identity re-key complete",
		zap.Int64("grants", grantRows),
		zap.Int64("entitlements", entitlementRows),
		zap.Duration("scan", scanDone.Sub(start)),
		zap.Duration("total", time.Since(start)),
	)
	return nil
}

func (e *Engine) recomputeStatsAfterIDIndexMigration(ctx context.Context) error {
	var syncID string
	err := e.IterateAllSyncRuns(ctx, func(r *v3.SyncRunRecord) bool {
		syncID = r.GetSyncId()
		return false
	})
	if err != nil {
		return fmt.Errorf("id-index migration: find sync for stats: %w", err)
	}
	if syncID == "" {
		return nil
	}
	if err := e.PersistSyncStats(ctx, syncID); err != nil {
		return fmt.Errorf("id-index migration: recompute sync stats: %w", err)
	}
	return nil
}

// emitStructuredEntitlementMigration re-keys every legacy entitlement row
// under its structural identity, derived from the record's structured
// resource fields plus the byte-prefix compression rule — the same single
// derivation every reader and write path uses, so primary/index divergence
// is impossible by construction. Values are never modified: external ids
// are an external-consumer contract and migrate byte-identical. Rows whose
// resource ref is missing cannot be represented in the structured keyspace
// at all and are dropped with a warning.
func (e *Engine) emitStructuredEntitlementMigration(ctx context.Context, out *spillSorter) (int64, error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: EntitlementLowerBound(),
		UpperBound: EntitlementUpperBound(),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	var rows, skippedMissingResource int64
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return rows, err
		}
		rt, rid, externalID, err := scanEntitlementIdentityFieldsRaw(iter.Value())
		if err != nil {
			return rows, fmt.Errorf("id-index migration: scan entitlement: %w", err)
		}
		if rt == "" || rid == "" {
			skippedMissingResource++
			continue
		}
		id := entitlementIdentityFromParts(rt, rid, externalID)
		if err := out.add(encodeEntitlementIdentityKey(id), iter.Value()); err != nil {
			return rows, err
		}
		rows++
	}
	if skippedMissingResource > 0 {
		ctxzap.Extract(ctx).Warn("id-index migration: dropped legacy entitlements with no resource ref; they cannot be keyed in the structured layout",
			zap.Int64("dropped", skippedMissingResource),
		)
	}
	return rows, iter.Error()
}

// emitStructuredGrantMigration re-keys every legacy grant row under its
// structural identity from the record's ref fields, with the same
// no-value-rewrite contract as emitStructuredEntitlementMigration. Rows
// missing entitlement or principal ref fields cannot be represented in the
// structured keyspace and are dropped with a warning.
func (e *Engine) emitStructuredGrantMigration(ctx context.Context, primary *spillSorter) (int64, error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: GrantLowerBound(),
		UpperBound: GrantUpperBound(),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	// Periodic progress logging: the migration runs inside Open and can
	// take minutes on very large files — a silent multi-minute open looks
	// hung and invites an operator (or a startup probe) to kill it,
	// restarting the migration from scratch. Mirrors the deferred index
	// build's 15s cadence.
	l := ctxzap.Extract(ctx)
	start := time.Now()
	lastLog := start
	var rows, skippedMissingRefs int64
	for iter.First(); iter.Valid(); iter.Next() {
		if rows&0xFFFF == 0 {
			if err := ctx.Err(); err != nil {
				return rows, err
			}
			if now := time.Now(); now.Sub(lastLog) >= 15*time.Second {
				l.Info("id-index migration: re-keying grants",
					zap.Int64("rows", rows),
					zap.Duration("elapsed", now.Sub(start)),
				)
				lastLog = now
			}
		}
		entRT, entRID, entID, principalRT, principalID, _, err := scanGrantIndexFieldsRaw(iter.Value())
		if err != nil {
			return rows, fmt.Errorf("id-index migration: scan grant: %w", err)
		}
		if entRT == "" || entRID == "" || entID == "" || principalRT == "" || principalID == "" {
			skippedMissingRefs++
			continue
		}
		id := grantIdentity{
			entitlement:     entitlementIdentityFromParts(entRT, entRID, entID),
			principalTypeID: principalRT,
			principalID:     principalID,
		}
		if err := primary.add(encodeGrantIdentityKey(id), iter.Value()); err != nil {
			return rows, err
		}
		rows++
	}
	if skippedMissingRefs > 0 {
		ctxzap.Extract(ctx).Warn("id-index migration: dropped legacy grants with missing entitlement/principal refs; they cannot be keyed in the structured layout",
			zap.Int64("dropped", skippedMissingRefs),
		)
	}
	return rows, iter.Error()
}

func finalizeMigrationSorter(ctx context.Context, fs vfs.FS, dir, name string, sorter *spillSorter) (string, error) {
	chunks, err := sorter.finalize()
	if err != nil {
		return "", err
	}
	if len(chunks) == 0 {
		return "", nil
	}
	path := filepath.Join(dir, name+".sst")
	if err := mergeSortedSpillChunksToSST(ctx, fs, path, name, chunks); err != nil {
		return "", err
	}
	return path, nil
}

func (e *Engine) replaceRangeWithSST(ctx context.Context, lower, upper []byte, path string) error {
	if path == "" {
		return e.db.DropKeyRange(lower, upper, pebble.Sync)
	}
	err := e.db.ReplaceRangeWithSSTs(ctx, []string{path}, pebble.KeyRange{Start: lower, End: upper})
	return err
}

func finalizeGrantPrimaryMigrationSorter(ctx context.Context, fs vfs.FS, dir, name string, sorter, byPrincipal, byNeedsExpansion *spillSorter) (string, error) {
	chunks, err := sorter.finalize()
	if err != nil {
		return "", err
	}
	if len(chunks) == 0 {
		return "", nil
	}
	path := filepath.Join(dir, name+".sst")
	if err := mergeGrantPrimaryMigrationChunksToSST(ctx, fs, path, name, chunks, byPrincipal, byNeedsExpansion); err != nil {
		return "", err
	}
	return path, nil
}

func mergeGrantPrimaryMigrationChunksToSST(ctx context.Context, fs vfs.FS, sstPath, name string, chunks []string, byPrincipal, byNeedsExpansion *spillSorter) error {
	cursors, err := openSpillChunks(chunks)
	if err != nil {
		return err
	}
	defer cursors.closeAll()
	h := &spillChunkHeap{}
	for i := range chunks {
		ok, err := cursors.advance(i)
		if err != nil {
			return err
		}
		if ok {
			h.push(spillChunkItem{chunkIdx: i, key: append([]byte(nil), cursors.key(i)...), val: append([]byte(nil), cursors.val(i)...)})
		}
	}

	w, err := newBulkSSTWriter(fs, filepath.Dir(sstPath), name)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if !success {
			_ = w.finish()
			_ = fs.Remove(w.path)
		}
	}()

	var duplicateGroups, duplicateRowsMerged int64
	var idxKeyScratch []byte
	var rowsProcessed int64
	for len(*h) > 0 {
		rowsProcessed++
		if rowsProcessed&0xFFFF == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		// Heap items are owned copies (pushed via append([]byte(nil), ...)
		// in the initial fill and advanceMigrationChunk), so no re-copy is
		// needed to hold them across chunk advances.
		first := h.pop()
		key := first.key
		values := [][]byte{first.val}
		if err := advanceMigrationChunk(h, cursors, first.chunkIdx); err != nil {
			return err
		}
		for len(*h) > 0 && bytes.Equal((*h)[0].key, key) {
			item := h.pop()
			values = append(values, item.val)
			if err := advanceMigrationChunk(h, cursors, item.chunkIdx); err != nil {
				return err
			}
		}
		if len(values) > 1 {
			duplicateGroups++
			duplicateRowsMerged += int64(len(values) - 1)
		}
		merged, err := mergeDuplicateGrantValues(values)
		if err != nil {
			return fmt.Errorf("id-index migration: merge duplicate grant key %x: %w", key, err)
		}
		if err := w.add(key, merged); err != nil {
			return err
		}
		// The identity is already fully encoded in the primary key being
		// written, so both index keys derive from it at the byte level —
		// no proto unmarshal per row (the dominant per-row cost at whale
		// scale and beyond). by_principal is the pinned segment
		// permutation; by_needs_expansion shares the primary's exact tail
		// (header swap only). Only the flag needs a value read, via a
		// single-field shallow scan.
		idxKey, ok := rawdb.AppendGrantByPrincipalKeyFromPrimary(idxKeyScratch[:0], key)
		idxKeyScratch = idxKey
		if !ok {
			return fmt.Errorf("id-index migration: grant primary key %x did not decode as a 6-segment identity", key)
		}
		if err := byPrincipal.add(idxKey, nil); err != nil {
			return err
		}
		needsExpansion, err := scanGrantNeedsExpansionRaw(merged)
		if err != nil {
			return fmt.Errorf("id-index migration: scan needs_expansion for key %x: %w", key, err)
		}
		if needsExpansion {
			idxKeyScratch = append(idxKeyScratch[:0], versionV3, typeIndex, idxGrantByNeedsExpansion)
			// The primary's sep+tail IS the identity index tail, byte-identical
			// (pinned by TestNeedsExpansionKeyHeaderSpliceFromPrimary).
			idxKeyScratch = append(idxKeyScratch, key[2:]...)
			if err := byNeedsExpansion.add(idxKeyScratch, nil); err != nil {
				return err
			}
		}
	}
	if err := w.finish(); err != nil {
		return err
	}
	if w.path != sstPath {
		// Engine FS, not os: the writer created this SST through fs and
		// the pebble.DB will read it at IngestAndExcise through fs.
		if err := fs.Rename(w.path, sstPath); err != nil {
			return err
		}
	}
	if duplicateGroups > 0 {
		ctxzap.Extract(ctx).Warn("id-index migration: merged legacy grant rows that share one structural identity",
			zap.Int64("identities_with_duplicates", duplicateGroups),
			zap.Int64("rows_merged_away", duplicateRowsMerged),
		)
	}
	success = true
	return nil
}

func advanceMigrationChunk(h *spillChunkHeap, cursors *spillChunkCursors, idx int) error {
	ok, err := cursors.advance(idx)
	if err != nil {
		return err
	}
	if ok {
		h.push(spillChunkItem{chunkIdx: idx, key: append([]byte(nil), cursors.key(idx)...), val: append([]byte(nil), cursors.val(idx)...)})
	}
	return nil
}

// mergeDuplicateGrantValues folds N marshaled grant rows that share one
// structural identity into a single record. Callers hand values over in
// FOLD ORDER, which is not stable: the in-place migration and the bulk
// import both pop duplicate groups off a k-way heap whose tie-break is
// chunk index — spill-chunk creation order, which varies run to run.
//
// INVARIANT: the merge of every field must therefore be fold-order
// independent, so the merged record's bytes are reproducible regardless of
// which duplicate arrives first. external_id/discovered_at use the
// commutative winner rule (recordIdentityInfoWins), needs_expansion is an
// OR, sources merge by map key, expansion ids union-sort, and annotations
// sort the merged union by (TypeUrl, Value). A new field added here must
// come with an order-independent merge rule.
//
// The fold is one pass: a group's size comes from the input file, and
// re-merging every accumulated field per row made a group cost quadratic
// time.
func mergeDuplicateGrantValues(values [][]byte) ([]byte, error) {
	if len(values) == 1 {
		return values[0], nil
	}
	var f grantRecordFold
	for _, value := range values {
		rec := &v3.GrantRecord{}
		if err := unmarshalRecord(value, rec); err != nil {
			return nil, err
		}
		f.add(rec)
	}
	return marshalRecord(f.result())
}

// grantRecordFold accumulates mergeDuplicateGrantValues' field rules over a
// group, one row at a time. A field that only one row carries keeps that
// row's value as stored; the rule's union runs only once a second row
// contributes.
type grantRecordFold struct {
	// rec is the first row. The fields the fold does not touch are equal
	// across the group, which shares one primary key.
	rec            *v3.GrantRecord
	externalID     string
	discoveredAt   *timestamppb.Timestamp
	needsExpansion bool

	annotations []*anypb.Any
	// annotationKeys is nil until a second row contributes annotations.
	annotationKeys map[string]struct{}

	sources map[string]*v3.GrantSourceRecord

	// expansion is the only contributor's, until a second one arrives and
	// the fold switches to the id sets.
	expansion        *v3.GrantExpandableRecord
	expansionIDs     map[string]struct{}
	expansionRTs     map[string]struct{}
	expansionShallow bool
}

func (f *grantRecordFold) add(rec *v3.GrantRecord) {
	if f.rec == nil || recordIdentityInfoWins(rec.GetDiscoveredAt(), rec.GetExternalId(), f.discoveredAt, f.externalID) {
		f.externalID, f.discoveredAt = rec.GetExternalId(), rec.GetDiscoveredAt()
	}
	if f.rec == nil {
		f.rec = rec
	}
	f.needsExpansion = f.needsExpansion || rec.GetNeedsExpansion()
	f.addAnnotations(rec.GetAnnotations())
	f.addSources(rec.GetSources())
	f.addExpansion(rec.GetExpansion())
}

// addAnnotations dedupes by (TypeUrl, Value). The union is sorted in result,
// because rows arrive in heap order, whose chunk tie-break varies run to run.
func (f *grantRecordFold) addAnnotations(anns []*anypb.Any) {
	switch {
	case len(anns) == 0:
		return
	case len(f.annotations) == 0:
		f.annotations, f.annotationKeys = anns, nil
		return
	case f.annotationKeys == nil:
		only := f.annotations
		f.annotations, f.annotationKeys = nil, make(map[string]struct{}, len(only)+len(anns))
		f.collectAnnotations(only)
	}
	f.collectAnnotations(anns)
}

func (f *grantRecordFold) collectAnnotations(anns []*anypb.Any) {
	for _, a := range anns {
		if a == nil {
			continue
		}
		key := a.GetTypeUrl() + "\x00" + string(a.GetValue())
		if _, ok := f.annotationKeys[key]; ok {
			continue
		}
		f.annotationKeys[key] = struct{}{}
		f.annotations = append(f.annotations, a)
	}
}

// addSources merges by map key. Colliding values fold field-wise with
// commutative, associative rules: IsDirect is an OR, and each ref field
// takes the smallest non-empty value. Preferring the direct value's fields
// would not be order-independent, since IsDirect ORs into the accumulator
// and loses which row was direct.
func (f *grantRecordFold) addSources(srcs map[string]*v3.GrantSourceRecord) {
	for key, src := range srcs {
		if src == nil {
			continue
		}
		if f.sources == nil {
			f.sources = make(map[string]*v3.GrantSourceRecord, len(srcs))
		}
		cur, ok := f.sources[key]
		if !ok {
			f.sources[key] = src
			continue
		}
		f.sources[key] = v3.GrantSourceRecord_builder{
			ResourceTypeId: minNonEmptyString(cur.GetResourceTypeId(), src.GetResourceTypeId()),
			ResourceId:     minNonEmptyString(cur.GetResourceId(), src.GetResourceId()),
			EntitlementId:  minNonEmptyString(cur.GetEntitlementId(), src.GetEntitlementId()),
			IsDirect:       cur.GetIsDirect() || src.GetIsDirect(),
		}.Build()
	}
}

// addExpansion unions the non-empty ids and ANDs Shallow.
func (f *grantRecordFold) addExpansion(exp *v3.GrantExpandableRecord) {
	switch {
	case exp == nil:
		return
	case f.expansion == nil && f.expansionIDs == nil:
		f.expansion = exp
		return
	case f.expansionIDs == nil:
		only := f.expansion
		f.expansion = nil
		f.expansionIDs, f.expansionRTs = map[string]struct{}{}, map[string]struct{}{}
		f.expansionShallow = only.GetShallow()
		f.collectExpansion(only)
	}
	f.collectExpansion(exp)
	f.expansionShallow = f.expansionShallow && exp.GetShallow()
}

func (f *grantRecordFold) collectExpansion(exp *v3.GrantExpandableRecord) {
	for _, id := range exp.GetEntitlementIds() {
		if id != "" {
			f.expansionIDs[id] = struct{}{}
		}
	}
	for _, id := range exp.GetResourceTypeIds() {
		if id != "" {
			f.expansionRTs[id] = struct{}{}
		}
	}
}

func (f *grantRecordFold) result() *v3.GrantRecord {
	out := f.rec
	out.SetExternalId(f.externalID)
	out.SetDiscoveredAt(f.discoveredAt)
	out.SetNeedsExpansion(f.needsExpansion)
	if f.annotationKeys != nil {
		slices.SortFunc(f.annotations, func(a, b *anypb.Any) int {
			return cmp.Or(strings.Compare(a.GetTypeUrl(), b.GetTypeUrl()), bytes.Compare(a.GetValue(), b.GetValue()))
		})
	}
	out.SetAnnotations(f.annotations)
	out.SetSources(f.sources)
	if f.expansionIDs != nil {
		f.expansion = v3.GrantExpandableRecord_builder{
			EntitlementIds:  slices.Sorted(maps.Keys(f.expansionIDs)),
			ResourceTypeIds: slices.Sorted(maps.Keys(f.expansionRTs)),
			Shallow:         f.expansionShallow,
		}.Build()
	}
	out.SetExpansion(f.expansion)
	return out
}

// recordIdentityInfoWins is the shared winner rule for duplicate-identity
// rows: earliest discovered_at wins; ties break to the smallest external id.
func recordIdentityInfoWins(candidateDiscovered *timestamppb.Timestamp, candidateExternalID string, incumbentDiscovered *timestamppb.Timestamp, incumbentExternalID string) bool {
	switch {
	case candidateDiscovered != nil && incumbentDiscovered == nil:
		return true
	case candidateDiscovered == nil && incumbentDiscovered != nil:
		return false
	case candidateDiscovered != nil && incumbentDiscovered != nil:
		ct, it := candidateDiscovered.AsTime(), incumbentDiscovered.AsTime()
		if !ct.Equal(it) {
			return ct.Before(it)
		}
	}
	return candidateExternalID < incumbentExternalID
}

// minNonEmptyString returns the lexicographically smallest non-empty
// argument, or "" when both are empty. Commutative and associative, with
// "" as the identity — folding it over N values yields the global smallest
// non-empty value regardless of order.
func minNonEmptyString(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	case a < b:
		return a
	default:
		return b
	}
}
