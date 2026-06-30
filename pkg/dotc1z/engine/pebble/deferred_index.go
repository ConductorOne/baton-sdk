package pebble

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// BuildDeferredGrantIndexes rebuilds the three grant index families that the
// expansion write path skips when deferGrantIndexes is enabled —
// by_entitlement_resource, by_principal, by_principal_resource_type — by
// scanning the by_entitlement index keys once and ingesting one sorted SST per
// family. It is the "build" half of the defer-then-sort strategy: it converts
// the out-of-order, compaction-churning inline writes of the scattered families
// into a single external sort + ingest each.
//
// Correctness notes:
//   - These families are NOT read during expansion (the merge reads only
//     by_entitlement), so deferring them to EndSync changes no expansion read.
//   - They are derived from the by_entitlement index (which carries
//     (entitlement_id, principal_rt, principal_id, external_id) in its key) plus
//     a one-time entitlement->resource map for by_entitlement_resource — so no
//     grant primary is decoded, and the rebuild is a pure function of durable,
//     checkpoint-captured state (resumable: re-run from by_entitlement).
//   - Each key carries the grant's external_id as its tail, and every primary
//     has a unique external_id, so the emitted keys are unique within a family —
//     no duplicate-key conflict in the spill merge.
//   - Ingesting into a keyspace that already holds base-grant entries (written
//     inline by the connector sync) is fine: identical keys are shadowed by the
//     higher-seqnum ingest, and the family SSTs have disjoint key ranges so a
//     single Ingest is legal.
//
// EndSync gates the call on deferredIdxPending so a non-expansion sync skips it.
func (e *Engine) BuildDeferredGrantIndexes(ctx context.Context) error {
	l := ctxzap.Extract(ctx)
	start := time.Now()

	dir, err := os.MkdirTemp("", "pebble-deferred-idx-")
	if err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: mkdir temp: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	sorters := min(4, max(2, runtime.GOMAXPROCS(0)/2))
	sem := make(chan struct{}, sorters)

	entRes := newSpillSorter(dir, fmt.Sprintf("index-%02x", idxGrantByEntitlementResource), sem, bulkSpillKeyChunkBytes)
	prin := newSpillSorter(dir, fmt.Sprintf("index-%02x", idxGrantByPrincipal), sem, bulkSpillKeyChunkBytes)
	prinRT := newSpillSorter(dir, fmt.Sprintf("index-%02x", idxGrantByPrincipalResourceType), sem, bulkSpillKeyChunkBytes)

	// All three deferred families are derived from the by_entitlement index —
	// written inline and therefore checkpoint-captured — with no grant primary
	// decode. The key's (principal_rt, principal_id, external_id) tail directly
	// yields by_principal and by_principal_resource_type. by_entitlement_resource
	// needs the entitlement's split (resource_type, resource_id); the
	// entitlement_id in the key is the lossy ":"-join, so we resolve it from a
	// one-time scan of the (small) entitlement keyspace — any handy index — rather
	// than parsing it.
	entResByID, err := e.entitlementResourceMap(ctx)
	if err != nil {
		return err
	}
	mapDone := time.Now()

	prefix := []byte{versionV3, typeIndex, idxGrantByEntitlement}
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upperBoundOf(prefix)})
	if err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: iter: %w", err)
	}
	defer iter.Close()

	var scratch []byte
	var scanned int64
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		entID, pRT, pID, ext, ok := decodeGrantByEntitlementIndexKey(iter.Key())
		if !ok {
			continue
		}
		// Byte-slice appenders + a string()-keyed map lookup (compiler-elided
		// alloc) keep this loop allocation-free over 162M keys.
		scratch = AppendGrantByPrincipalIndexKeyRawBytes(scratch[:0], pRT, pID, ext)
		if err := prin.add(scratch, nil); err != nil {
			return err
		}
		scratch = AppendGrantByPrincipalResourceTypeIndexKeyRawBytes(scratch[:0], pRT, ext)
		if err := prinRT.add(scratch, nil); err != nil {
			return err
		}
		if er, found := entResByID[string(entID)]; found && len(er.resourceID) > 0 {
			scratch = AppendGrantByEntitlementResourceIndexKeyRawBytes(scratch[:0], er.resourceTypeID, er.resourceID, ext)
			if err := entRes.add(scratch, nil); err != nil {
				return err
			}
		}
		scanned++
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: iter error: %w", err)
	}
	scanDone := time.Now()

	units := []struct {
		name string
		s    *spillSorter
	}{
		{fmt.Sprintf("index-%02x", idxGrantByEntitlementResource), entRes},
		{fmt.Sprintf("index-%02x", idxGrantByPrincipal), prin},
		{fmt.Sprintf("index-%02x", idxGrantByPrincipalResourceType), prinRT},
	}
	// The families are independent, so finalize + k-way merge each into its SST
	// in parallel (mirrors BulkSyncImport.Finish).
	results := make([]struct {
		path string
		err  error
	}, len(units))
	var wg sync.WaitGroup
	for i, u := range units {
		wg.Add(1)
		go func(i int, name string, s *spillSorter) {
			defer wg.Done()
			chunks, err := s.finalize()
			if err != nil {
				results[i].err = err
				return
			}
			if len(chunks) == 0 {
				return
			}
			sstPath := filepath.Join(dir, name+".sst")
			if err := mergeSortedSpillChunksToSST(ctx, sstPath, name, chunks); err != nil {
				results[i].err = err
				return
			}
			results[i].path = sstPath
		}(i, u.name, u.s)
	}
	wg.Wait()
	paths := make([]string, 0, len(units))
	for _, r := range results {
		if r.err != nil {
			return r.err
		}
		if r.path != "" {
			paths = append(paths, r.path)
		}
	}
	mergeDone := time.Now()

	if len(paths) == 0 {
		return nil
	}
	if err := e.withWrite(func() error {
		return e.db.Ingest(ctx, paths)
	}); err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: ingest: %w", err)
	}

	l.Info("deferred grant index build complete",
		zap.Int64("index_keys_scanned", scanned),
		zap.Int("entitlements_mapped", len(entResByID)),
		zap.Int("families", len(paths)),
		zap.Duration("ent_map", mapDone.Sub(start)),
		zap.Duration("scan", scanDone.Sub(mapDone)),
		zap.Duration("merge", mergeDone.Sub(scanDone)),
		zap.Duration("ingest", time.Since(mergeDone)),
		zap.Duration("total", time.Since(start)),
	)
	return nil
}

// entResource is the entitlement's split resource identity as raw bytes, so the
// by_entitlement_resource build can use the byte-slice key appenders and skip a
// per-grant string conversion. Allocated once per entitlement (reused across
// every grant on it), not per grant.
type entResource struct {
	resourceTypeID []byte
	resourceID     []byte
}

// entitlementResourceMap scans the entitlement keyspace once and maps each
// entitlement_id (external id) to its resource's (type, id). The entitlement
// set is small relative to grants, so this is cheap and avoids decoding any
// grant primary.
func (e *Engine) entitlementResourceMap(ctx context.Context) (map[string]entResource, error) {
	lo := []byte{versionV3, typeEntitlement}
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: upperBoundOf(lo)})
	if err != nil {
		return nil, fmt.Errorf("entitlementResourceMap: iter: %w", err)
	}
	defer iter.Close()
	out := make(map[string]entResource)
	var rec v3.EntitlementRecord
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rec.Reset()
		if err := unmarshalRecord(iter.Value(), &rec); err != nil {
			return nil, fmt.Errorf("entitlementResourceMap: unmarshal: %w", err)
		}
		res := rec.GetResource()
		if res == nil {
			continue
		}
		out[rec.GetExternalId()] = entResource{
			resourceTypeID: []byte(res.GetResourceTypeId()),
			resourceID:     []byte(res.GetResourceId()),
		}
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("entitlementResourceMap: iter error: %w", err)
	}
	return out, nil
}

// decodeGrantByEntitlementIndexKey decodes a by_entitlement grant index key into
// its (entitlement_id, principal_rt, principal_id, external_id) components. The
// key layout is the 3-byte family header, a leading tuple separator, then the
// four escaped tuple components. Returns ok=false for any key that does not
// match that shape.
// Returned slices alias key (no allocation) and are only valid until the
// iterator that produced key advances; callers must copy/encode before Next.
func decodeGrantByEntitlementIndexKey(key []byte) (entID, prinRT, prinID, ext []byte, ok bool) {
	const headerLen = 3 // versionV3 | typeIndex | idxGrantByEntitlement
	if len(key) <= headerLen {
		return nil, nil, nil, nil, false
	}
	tail := key[headerLen:]
	// tail[0] is the leading tuple separator written by AppendTupleSeparator;
	// the first component begins at offset 1.
	a, n1, ok1 := codec.DecodeTupleStringAlias(tail, 1)
	if !ok1 || n1 >= len(tail) {
		return nil, nil, nil, nil, false
	}
	b, n2, ok2 := codec.DecodeTupleStringAlias(tail, n1+1)
	if !ok2 || n2 >= len(tail) {
		return nil, nil, nil, nil, false
	}
	c, n3, ok3 := codec.DecodeTupleStringAlias(tail, n2+1)
	if !ok3 || n3 >= len(tail) {
		return nil, nil, nil, nil, false
	}
	d, _, ok4 := codec.DecodeTupleStringAlias(tail, n3+1)
	if !ok4 {
		return nil, nil, nil, nil, false
	}
	return a, b, c, d, true
}
