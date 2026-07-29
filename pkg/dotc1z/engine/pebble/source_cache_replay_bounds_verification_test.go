package pebble

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// C10/C12: the replay commit seam supplies deterministic evidence that live
// batch cardinality is fixed, and lets retry be cut after one landed chunk.
func TestVerificationReplayBatchBoundAndInterruptedRetry(t *testing.T) {
	ctx := t.Context()
	const rows = replayBatchRows + 1

	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	const writeChunk = 1_000
	for start := 0; start < rows; start += writeChunk {
		end := min(start+writeChunk, rows)
		batch := make([]*v3.ResourceRecord, 0, end-start)
		for i := start; i < end; i++ {
			batch = append(batch, v3.ResourceRecord_builder{
				ResourceTypeId: "user",
				ResourceId:     fmt.Sprintf("user-%05d", i),
				SourceScopeKey: "scope-a",
			}.Build())
		}
		require.NoError(t, prev.PebbleEngine().PutResourceRecords(ctx, batch...))
	}

	bounded := newAdapter(t)
	_, err = bounded.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	var commitCalls, highWater int
	bounded.PebbleEngine().test.sourceCacheReplayCommitHook = func(kind string, batchRows int, _ bool) error {
		require.Equal(t, "resources", kind)
		commitCalls++
		highWater = max(highWater, batchRows)
		return nil
	}
	res, err := bounded.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	require.Equal(t, int64(rows), res.Rows)
	require.Equal(t, 2, commitCalls)
	require.Equal(t, replayBatchRows, highWater)
	require.Equal(t, rows, countKeys(t, bounded.PebbleEngine(), encodeResourcePrefix()))

	interruptedEngine, interruptedDir := newTestEngine(t)
	interrupted := NewAdapter(interruptedEngine)
	interruptedSyncID, err := interrupted.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	injected := errors.New("verification replay cut")
	commitCalls = 0
	interrupted.PebbleEngine().test.sourceCacheReplayCommitHook = func(_ string, _ int, _ bool) error {
		commitCalls++
		if commitCalls == 2 {
			return injected
		}
		return nil
	}
	_, err = interrupted.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
	require.ErrorIs(t, err, injected)
	require.Equal(t, replayBatchRows, countKeys(t, interrupted.PebbleEngine(), encodeResourcePrefix()),
		"only the first complete replay chunk may be visible after the injected cut")
	require.Equal(t, replayBatchRows, countKeys(t, interrupted.PebbleEngine(), ResourceBySourceScopeLowerBound()))
	require.Equal(t, rows, countKeys(t, prev.PebbleEngine(), encodeResourcePrefix()), "failed replay mutated source primaries")
	require.NoError(t, auditSourceScopeBiconditional(prev.PebbleEngine()))

	require.NoError(t, interrupted.PebbleEngine().Close())
	reopened, err := Open(ctx, filepath.Join(interruptedDir, "db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	interrupted = NewAdapter(reopened)
	require.NoError(t, interrupted.SetCurrentSync(ctx, interruptedSyncID))
	res, err = interrupted.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	require.Equal(t, int64(rows), res.Rows)
	require.Equal(t, rows, countKeys(t, interrupted.PebbleEngine(), encodeResourcePrefix()))
	require.Equal(t, rows, countKeys(t, interrupted.PebbleEngine(), ResourceBySourceScopeLowerBound()))
	require.NoError(t, auditSourceScopeBiconditional(interrupted.PebbleEngine()))

	cancelled := newAdapter(t)
	_, err = cancelled.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	cancelCtx, cancel := context.WithCancel(ctx)
	cancelled.PebbleEngine().test.sourceCacheReplayCommitHook = func(_ string, _ int, final bool) error {
		if !final {
			cancel()
		}
		return nil
	}
	_, err = cancelled.PebbleEngine().ReplaySourceCacheResources(cancelCtx, prev.PebbleEngine(), "scope-a")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, replayBatchRows, countKeys(t, cancelled.PebbleEngine(), encodeResourcePrefix()))
	require.Equal(t, rows, countKeys(t, prev.PebbleEngine(), encodeResourcePrefix()), "cancelled replay mutated source primaries")
	require.NoError(t, auditSourceScopeBiconditional(prev.PebbleEngine()))
	cancelled.PebbleEngine().test.sourceCacheReplayCommitHook = nil
	res, err = cancelled.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	require.Equal(t, int64(rows), res.Rows)
	require.Equal(t, rows, countKeys(t, cancelled.PebbleEngine(), encodeResourcePrefix()))
	require.NoError(t, auditSourceScopeBiconditional(cancelled.PebbleEngine()))

	readFailed := newAdapter(t)
	_, err = readFailed.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	readErr := errors.New("verification source iterator failure")
	readFailed.PebbleEngine().test.sourceCacheReplayReadHook = func(kind string, row int) error {
		require.Equal(t, "resources", kind)
		if row == replayBatchRows {
			return readErr
		}
		return nil
	}
	_, err = readFailed.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
	require.ErrorIs(t, err, readErr)
	require.Equal(t, replayBatchRows, countKeys(t, readFailed.PebbleEngine(), encodeResourcePrefix()))
	require.Equal(t, rows, countKeys(t, prev.PebbleEngine(), encodeResourcePrefix()), "source iterator failure mutated source")
	readFailed.PebbleEngine().test.sourceCacheReplayReadHook = nil
	res, err = readFailed.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	require.Equal(t, int64(rows), res.Rows)
	require.Equal(t, rows, countKeys(t, readFailed.PebbleEngine(), encodeResourcePrefix()))
	require.NoError(t, auditSourceScopeBiconditional(readFailed.PebbleEngine()))
}
