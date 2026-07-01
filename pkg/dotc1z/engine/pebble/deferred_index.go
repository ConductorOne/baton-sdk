package pebble

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// BuildDeferredGrantIndexes rebuilds the remaining scattered expansion index
// family, by_principal, from entitlement-first primary grant keys. The expansion
// write path can skip by_principal inline because expansion reads by entitlement
// only; this method rewrites the whole by_principal range as one sorted SST at
// EndSync.
func (e *Engine) BuildDeferredGrantIndexes(ctx context.Context) error {
	start := time.Now()
	dir, err := os.MkdirTemp("", "pebble-deferred-idx-")
	if err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: mkdir temp: %w", err)
	}
	defer os.RemoveAll(dir)

	sorters := min(4, max(2, runtime.GOMAXPROCS(0)/2))
	sem := make(chan struct{}, sorters)
	principal := newSpillSorter(dir, fmt.Sprintf("index-%02x", idxGrantByPrincipal), sem, bulkSpillKeyChunkBytes)

	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: GrantLowerBound(),
		UpperBound: GrantUpperBound(),
	})
	if err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: iter: %w", err)
	}
	defer iter.Close()

	var scanned int64
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		id, ok := decodeGrantIdentityKey(iter.Key())
		if !ok {
			continue
		}
		if err := principal.add(encodeGrantByPrincipalIdentityIndexKey(id), nil); err != nil {
			return err
		}
		scanned++
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("BuildDeferredGrantIndexes: iter error: %w", err)
	}
	scanDone := time.Now()

	chunks, err := principal.finalize()
	if err != nil {
		return err
	}
	if len(chunks) == 0 {
		return nil
	}
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
