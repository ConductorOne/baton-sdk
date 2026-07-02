package pebble

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/cockroachdb/pebble/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func (e *Engine) migrateIDIndexFormatToStructuredV1(ctx context.Context) error {
	dir, err := os.MkdirTemp("", "pebble-id-index-migration-")
	if err != nil {
		return fmt.Errorf("id-index migration: mkdir temp: %w", err)
	}
	defer os.RemoveAll(dir)

	sortSem := make(chan struct{}, 4)
	grantPrimary := newSpillSorter(dir, "grant-primary", sortSem, bulkSpillKeyChunkBytes)
	entitlementPrimary := newSpillSorter(dir, "entitlement-primary", sortSem, bulkSpillKeyChunkBytes)
	byPrincipal := newSpillSorter(dir, "idx-grant-by-principal", sortSem, bulkSpillKeyChunkBytes)
	byNeedsExpansion := newSpillSorter(dir, "idx-grant-by-needs-expansion", sortSem, bulkSpillKeyChunkBytes)
	sorters := []*spillSorter{grantPrimary, entitlementPrimary, byPrincipal, byNeedsExpansion}
	defer func() {
		for _, s := range sorters {
			s.abort()
		}
	}()

	if err := e.emitStructuredEntitlementMigration(ctx, entitlementPrimary); err != nil {
		return err
	}
	if err := e.emitStructuredGrantMigration(ctx, grantPrimary); err != nil {
		return err
	}

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
			path, err = finalizeGrantPrimaryMigrationSorter(ctx, dir, r.name, r.sorter, byPrincipal, byNeedsExpansion)
		} else {
			path, err = finalizeMigrationSorter(ctx, dir, r.name, r.sorter)
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
		if err := e.db.DeleteRange(r[0], r[1], pebble.Sync); err != nil {
			return fmt.Errorf("id-index migration: delete dropped range: %w", err)
		}
	}

	if err := e.recomputeStatsAfterIDIndexMigration(ctx); err != nil {
		return err
	}
	return e.writeIDIndexFormat(idIndexFormatStructuredV1)
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

func (e *Engine) emitStructuredEntitlementMigration(ctx context.Context, out *spillSorter) error {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: EntitlementLowerBound(),
		UpperBound: EntitlementUpperBound(),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		rt, rid, externalID, err := scanEntitlementIdentityFieldsRaw(iter.Value())
		if err != nil {
			return fmt.Errorf("id-index migration: scan entitlement: %w", err)
		}
		id := entitlementIdentityFromLegacyParts(rt, rid, externalID)
		if err := out.add(encodeEntitlementIdentityKey(id), iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (e *Engine) emitStructuredGrantMigration(ctx context.Context, primary *spillSorter) error {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: GrantLowerBound(),
		UpperBound: GrantUpperBound(),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		entRT, entRID, entID, principalRT, principalID, _, err := scanGrantIndexFieldsRaw(iter.Value())
		if err != nil {
			return fmt.Errorf("id-index migration: scan grant: %w", err)
		}
		id := grantIdentity{
			entitlement:     entitlementIdentityFromLegacyParts(entRT, entRID, entID),
			principalTypeID: principalRT,
			principalID:     principalID,
		}
		if err := primary.add(encodeGrantIdentityKey(id), iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}

func finalizeMigrationSorter(ctx context.Context, dir, name string, sorter *spillSorter) (string, error) {
	chunks, err := sorter.finalize()
	if err != nil {
		return "", err
	}
	if len(chunks) == 0 {
		return "", nil
	}
	path := filepath.Join(dir, name+".sst")
	if err := mergeSortedSpillChunksToSST(ctx, path, name, chunks); err != nil {
		return "", err
	}
	return path, nil
}

func (e *Engine) replaceRangeWithSST(ctx context.Context, lower, upper []byte, path string) error {
	if path == "" {
		return e.db.DeleteRange(lower, upper, pebble.Sync)
	}
	_, err := e.db.IngestAndExcise(ctx, []string{path}, nil, nil, pebble.KeyRange{Start: lower, End: upper})
	return err
}

func finalizeGrantPrimaryMigrationSorter(ctx context.Context, dir, name string, sorter, byPrincipal, byNeedsExpansion *spillSorter) (string, error) {
	chunks, err := sorter.finalize()
	if err != nil {
		return "", err
	}
	if len(chunks) == 0 {
		return "", nil
	}
	path := filepath.Join(dir, name+".sst")
	if err := mergeGrantPrimaryMigrationChunksToSST(ctx, path, name, chunks, byPrincipal, byNeedsExpansion); err != nil {
		return "", err
	}
	return path, nil
}

func mergeGrantPrimaryMigrationChunksToSST(ctx context.Context, sstPath, name string, chunks []string, byPrincipal, byNeedsExpansion *spillSorter) error {
	readers := make([]*os.File, 0, len(chunks))
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	bufReaders := make([]*bufio.Reader, len(chunks))
	keyBufs := make([][]byte, len(chunks))
	valBufs := make([][]byte, len(chunks))
	h := &spillChunkHeap{}
	var lenBuf [4]byte
	for i, chunk := range chunks {
		f, err := os.Open(chunk) // #nosec G304 - staged under migration temp dir.
		if err != nil {
			return err
		}
		readers = append(readers, f)
		bufReaders[i] = bufio.NewReaderSize(f, bulkSpillBufferSize)
		ok, err := readSpillEntry(bufReaders[i], &keyBufs[i], &valBufs[i], &lenBuf)
		if err != nil {
			return err
		}
		if ok {
			h.push(spillChunkItem{chunkIdx: i, key: append([]byte(nil), keyBufs[i]...), val: append([]byte(nil), valBufs[i]...)})
		}
	}

	w, err := newBulkSSTWriter(filepath.Dir(sstPath), name)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if !success {
			_ = w.finish()
			_ = os.Remove(w.path)
		}
	}()

	for len(*h) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		first := h.pop()
		key := append([]byte(nil), first.key...)
		values := [][]byte{append([]byte(nil), first.val...)}
		if err := advanceMigrationChunk(h, bufReaders, keyBufs, valBufs, &lenBuf, first.chunkIdx); err != nil {
			return err
		}
		for len(*h) > 0 && bytes.Equal((*h)[0].key, key) {
			item := h.pop()
			values = append(values, append([]byte(nil), item.val...))
			if err := advanceMigrationChunk(h, bufReaders, keyBufs, valBufs, &lenBuf, item.chunkIdx); err != nil {
				return err
			}
		}
		merged, err := mergeDuplicateGrantValues(values)
		if err != nil {
			return fmt.Errorf("id-index migration: merge duplicate grant key %x: %w", key, err)
		}
		if err := w.add(key, merged); err != nil {
			return err
		}
		var rec v3.GrantRecord
		if err := unmarshalRecord(merged, &rec); err != nil {
			return err
		}
		id, err := grantIdentityFromRecord(&rec)
		if err != nil {
			return err
		}
		if err := byPrincipal.add(encodeGrantByPrincipalIdentityIndexKey(id), nil); err != nil {
			return err
		}
		if rec.GetNeedsExpansion() {
			if err := byNeedsExpansion.add(encodeGrantByNeedsExpansionIdentityIndexKey(id), nil); err != nil {
				return err
			}
		}
	}
	if err := w.finish(); err != nil {
		return err
	}
	if w.path != sstPath {
		if err := os.Rename(w.path, sstPath); err != nil {
			return err
		}
	}
	success = true
	return nil
}

func advanceMigrationChunk(h *spillChunkHeap, readers []*bufio.Reader, keyBufs, valBufs [][]byte, lenBuf *[4]byte, idx int) error {
	ok, err := readSpillEntry(readers[idx], &keyBufs[idx], &valBufs[idx], lenBuf)
	if err != nil {
		return err
	}
	if ok {
		h.push(spillChunkItem{chunkIdx: idx, key: append([]byte(nil), keyBufs[idx]...), val: append([]byte(nil), valBufs[idx]...)})
	}
	return nil
}

func mergeDuplicateGrantValues(values [][]byte) ([]byte, error) {
	if len(values) == 1 {
		return values[0], nil
	}
	var out *v3.GrantRecord
	for _, value := range values {
		var rec v3.GrantRecord
		if err := unmarshalRecord(value, &rec); err != nil {
			return nil, err
		}
		if out == nil {
			cp := proto.Clone(&rec).(*v3.GrantRecord)
			out = cp
			continue
		}
		mergeGrantRecordInto(out, &rec)
	}
	return marshalRecord(out)
}

func mergeGrantRecordInto(dst, src *v3.GrantRecord) {
	if grantRecordIdentityInfoWins(src, dst) {
		dst.SetExternalId(src.GetExternalId())
		dst.SetDiscoveredAt(src.GetDiscoveredAt())
	}
	dst.SetNeedsExpansion(dst.GetNeedsExpansion() || src.GetNeedsExpansion())
	dst.SetAnnotations(mergeGrantAnnotations(dst.GetAnnotations(), src.GetAnnotations()))
	dst.SetSources(mergeGrantSources(dst.GetSources(), src.GetSources()))
	dst.SetExpansion(mergeGrantExpansion(dst.GetExpansion(), src.GetExpansion()))
}

func grantRecordIdentityInfoWins(candidate, incumbent *v3.GrantRecord) bool {
	cdt, idt := candidate.GetDiscoveredAt(), incumbent.GetDiscoveredAt()
	switch {
	case cdt != nil && idt == nil:
		return true
	case cdt == nil && idt != nil:
		return false
	case cdt != nil && idt != nil:
		ct, it := cdt.AsTime(), idt.AsTime()
		if !ct.Equal(it) {
			return ct.Before(it)
		}
	}
	return candidate.GetExternalId() < incumbent.GetExternalId()
}

func mergeGrantAnnotations(a, b []*anypb.Any) []*anypb.Any {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	out := make([]*anypb.Any, 0, len(a)+len(b))
	seen := make(map[string]struct{}, len(a)+len(b))
	add := func(items []*anypb.Any) {
		for _, item := range items {
			if item == nil {
				continue
			}
			key := item.GetTypeUrl() + "\x00" + string(item.GetValue())
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, item)
		}
	}
	add(a)
	add(b)
	return out
}

func mergeGrantSources(a, b map[string]*v3.GrantSourceRecord) map[string]*v3.GrantSourceRecord {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	out := make(map[string]*v3.GrantSourceRecord, len(a)+len(b))
	for key, value := range a {
		out[key] = proto.Clone(value).(*v3.GrantSourceRecord)
	}
	for key, value := range b {
		if value == nil {
			continue
		}
		existing := out[key]
		if existing == nil {
			out[key] = proto.Clone(value).(*v3.GrantSourceRecord)
			continue
		}
		rt, rid, eid := existing.GetResourceTypeId(), existing.GetResourceId(), existing.GetEntitlementId()
		if value.GetIsDirect() {
			if value.GetResourceTypeId() != "" {
				rt = value.GetResourceTypeId()
			}
			if value.GetResourceId() != "" {
				rid = value.GetResourceId()
			}
			if value.GetEntitlementId() != "" {
				eid = value.GetEntitlementId()
			}
		} else {
			if rt == "" {
				rt = value.GetResourceTypeId()
			}
			if rid == "" {
				rid = value.GetResourceId()
			}
			if eid == "" {
				eid = value.GetEntitlementId()
			}
		}
		out[key] = v3.GrantSourceRecord_builder{
			ResourceTypeId: rt,
			ResourceId:     rid,
			EntitlementId:  eid,
			IsDirect:       existing.GetIsDirect() || value.GetIsDirect(),
		}.Build()
	}
	return out
}

func mergeGrantExpansion(a, b *v3.GrantExpandableRecord) *v3.GrantExpandableRecord {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return v3.GrantExpandableRecord_builder{
		EntitlementIds:  unionSortedStrings(a.GetEntitlementIds(), b.GetEntitlementIds()),
		ResourceTypeIds: unionSortedStrings(a.GetResourceTypeIds(), b.GetResourceTypeIds()),
		Shallow:         a.GetShallow() && b.GetShallow(),
	}.Build()
}

func unionSortedStrings(a, b []string) []string {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(a)+len(b))
	for _, v := range a {
		if v != "" {
			seen[v] = struct{}{}
		}
	}
	for _, v := range b {
		if v != "" {
			seen[v] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
