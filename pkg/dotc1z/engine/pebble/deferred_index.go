package pebble

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// deferredIndexSpillChunkBytes is the spill-chunk arena size for the deferred
// by_principal index build. The shared bulkSpillKeyChunkBytes (8MiB) is sized
// for the bulk import, where lanes × index-families sorters are alive at once
// and small arenas bound aggregate memory. The deferred build is the opposite
// shape — one producer, one sorter, nothing else running — and with 8MiB
// chunks a whale (57M+ index keys ≈ 6.4GB) produced an 801-way final merge:
// ~10 heap comparisons per entry plus 801 open chunk files with 1MiB readers
// (~800MB of buffers). 128MiB chunks cut that to ~50 runs. Peak memory is the
// active arena plus up to `sorters` chunks being sorted in background
// (~640MB), in the same ballpark as what the wide merge's read buffers used.
const deferredIndexSpillChunkBytes = 128 << 20

// appendGrantByPrincipalKeyFromPrimary builds the by_principal index key
// directly from a primary grant key by permuting its tuple segments, into
// dst. The primary tail is ent_rt|ent_rid|ent_kind|ent_name|p_rt|p_id and the
// index tail is p_rt|p_id|ent_rt|ent_rid|ent_kind|ent_name — the segments are
// already escaped, and the tuple escaping is canonical, so splicing the raw
// bytes is byte-identical to decode + re-encode
// (encodeGrantByPrincipalIdentityIndexKey ∘ decodeGrantIdentityKey) while
// skipping six string allocations and a fresh key buffer per row. That
// decode/re-encode pair was ~7GB of allocations per whale deferred build.
// Returns ok=false for keys that do not have exactly six segments.
//
// Pinned against the decode+re-encode path by
// TestAppendGrantByPrincipalKeyFromPrimary.
func appendGrantByPrincipalKeyFromPrimary(dst, primaryKey []byte) ([]byte, bool) {
	const prefixLen = 3 // versionV3 | typeGrant | separator
	if len(primaryKey) < prefixLen || primaryKey[0] != versionV3 || primaryKey[1] != typeGrant || primaryKey[2] != 0 {
		return dst, false
	}
	tail := primaryKey[prefixLen:]
	// Split into exactly six escaped segments on bare separator bytes (the
	// escape rules guarantee segments contain no bare 0x00).
	var segs [6][]byte
	rest := tail
	for i := 0; i < 5; i++ {
		sep := bytes.IndexByte(rest, 0)
		if sep < 0 {
			return dst, false
		}
		segs[i] = rest[:sep]
		rest = rest[sep+1:]
	}
	if bytes.IndexByte(rest, 0) >= 0 {
		return dst, false
	}
	segs[5] = rest

	dst = append(dst, versionV3, typeIndex, idxGrantByPrincipal, 0)
	for i, idx := range [6]int{4, 5, 0, 1, 2, 3} { // p_rt, p_id, ent_rt, ent_rid, ent_kind, ent_name
		if i > 0 {
			dst = append(dst, 0)
		}
		dst = append(dst, segs[idx]...)
	}
	return dst, true
}

// deferredGrantStats carries the grant-keyspace stats accumulated during the
// BuildDeferredGrantIndexes scan: the same numbers computeSyncStats derives
// from its own full grant scan. Fusing them into the index scan removes a
// second O(grants) pass at EndSync.
type deferredGrantStats struct {
	syncID                string
	grants                int64
	grantsByEntitlementRT map[string]int64
}

// BuildDeferredGrantIndexes rebuilds the remaining scattered expansion index
// family, by_principal, from entitlement-first primary grant keys. The expansion
// write path can skip by_principal inline because expansion reads by entitlement
// only; this method rewrites the whole by_principal range as one sorted SST at
// EndSync.
//
// The scan also accumulates the grant portion of the sync-stats sidecar
// (total count + per-entitlement-resource-type grouping) and stashes it so
// the subsequent PersistSyncStats call can skip its own full grant scan. The
// stats accumulation is best-effort: a value that fails the shallow field
// scan just disables the stash and PersistSyncStats falls back to scanning.
func (e *Engine) BuildDeferredGrantIndexes(ctx context.Context) error {
	start := time.Now()
	l := ctxzap.Extract(ctx)
	dir, err := os.MkdirTemp("", "pebble-deferred-idx-")
	if err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: mkdir temp: %w", err)
	}
	defer os.RemoveAll(dir)

	sorters := min(4, max(2, runtime.GOMAXPROCS(0)/2))
	sem := make(chan struct{}, sorters)
	principal := newSpillSorter(dir, fmt.Sprintf("index-%02x", idxGrantByPrincipal), sem, deferredIndexSpillChunkBytes)

	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: GrantLowerBound(),
		UpperBound: GrantUpperBound(),
	})
	if err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: iter: %w", err)
	}
	defer iter.Close()

	statsOK := true
	var totalKeys int64
	grantsByEntRTPtr := map[string]*int64{}

	var scanned int64
	var idxKeyScratch []byte
	lastLog := start
	for iter.First(); iter.Valid(); iter.Next() {
		totalKeys++
		if statsOK {
			// Same shallow scan + grouping computeSyncStats performs; see
			// sync_stats_sidecar.go for the map[string]*int64 rationale.
			if entRT, serr := scanGrantEntitlementResourceTypeRaw(iter.Value()); serr != nil {
				statsOK = false
			} else if p, ok := grantsByEntRTPtr[string(entRT)]; ok {
				*p++
			} else {
				n := int64(1)
				grantsByEntRTPtr[string(entRT)] = &n
			}
		}
		idxKey, ok := appendGrantByPrincipalKeyFromPrimary(idxKeyScratch[:0], iter.Key())
		idxKeyScratch = idxKey
		if !ok {
			continue
		}
		if err := principal.add(idxKey, nil); err != nil {
			return err
		}
		scanned++
		// Throttle per-row bookkeeping; ctx.Err and time.Now are measurable
		// at 57M+ rows.
		if scanned&0xFFFF == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if now := time.Now(); now.Sub(lastLog) >= 15*time.Second {
				l.Info("deferred grant index build: scanning primary grants",
					zap.Int64("index_keys_added", scanned),
					zap.Duration("elapsed", now.Sub(start)),
				)
				lastLog = now
			}
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: iter error: %w", err)
	}
	if statsOK {
		if syncID := codec.DecodeSyncID(e.currentSyncBytes()); syncID != "" {
			grantsByEntitlementRT := make(map[string]int64, len(grantsByEntRTPtr))
			for rt, p := range grantsByEntRTPtr {
				grantsByEntitlementRT[rt] = *p
			}
			e.stashDeferredGrantStats(&deferredGrantStats{
				syncID:                syncID,
				grants:                totalKeys,
				grantsByEntitlementRT: grantsByEntitlementRT,
			})
		}
	}
	scanDone := time.Now()
	l.Info("deferred grant index build: scan complete",
		zap.Int64("index_keys_added", scanned),
		zap.Duration("elapsed", scanDone.Sub(start)),
	)

	chunks, err := principal.finalize()
	if err != nil {
		return err
	}
	if len(chunks) == 0 {
		l.Info("deferred grant index build: no index chunks to ingest",
			zap.Int64("index_keys_added", scanned),
			zap.Duration("total", time.Since(start)),
		)
		return nil
	}
	l.Info("deferred grant index build: merging sorted chunks",
		zap.Int("chunks", len(chunks)),
		zap.Int64("index_keys_added", scanned),
		zap.Duration("scan", scanDone.Sub(start)),
	)
	sstPath := filepath.Join(dir, fmt.Sprintf("index-%02x.sst", idxGrantByPrincipal))
	if err := mergeSortedSpillChunksToSST(ctx, sstPath, fmt.Sprintf("index-%02x", idxGrantByPrincipal), chunks); err != nil {
		return err
	}
	mergeDone := time.Now()

	_, err = e.db.IngestAndExcise(ctx, []string{sstPath}, nil, nil, pebble.KeyRange{
		Start: GrantByPrincipalLowerBound(),
		End:   GrantByPrincipalUpperBound(),
	})
	if err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: ingest/excise: %w", err)
	}

	ctxzap.Extract(ctx).Info("deferred grant index build complete",
		zap.Int64("index_keys_scanned", scanned),
		zap.Duration("scan", scanDone.Sub(start)),
		zap.Duration("merge", mergeDone.Sub(scanDone)),
		zap.Duration("ingest", time.Since(mergeDone)),
		zap.Duration("total", time.Since(start)),
	)
	return nil
}
