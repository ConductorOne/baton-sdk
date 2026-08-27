package pebble

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

func TestOverlayFoldBatchLifecycleFailureCuts(t *testing.T) {
	injected := errors.New("injected fold commit failure")
	tests := []struct {
		name       string
		failCommit int
		cancel     bool
		discard    bool
		wantErr    error
		wantCalls  int
	}{
		{name: "primary commit", failCommit: 1, wantErr: injected, wantCalls: 1},
		{name: "index commit after primary", failCommit: 2, wantErr: injected, wantCalls: 2},
		{name: "cancel with pending writes", cancel: true, wantErr: context.Canceled},
		{name: "successful commit close remint", wantCalls: 2},
		{name: "restart discard remint", discard: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e, err := enginepkg.Open(t.Context(), filepath.Join(t.TempDir(), "dest"))
			require.NoError(t, err)

			commitCalls := 0
			beforeCommit := func() error {
				commitCalls++
				if commitCalls == tc.failCommit {
					return injected
				}
				return nil
			}

			w := newOverlayBucketRawWriter(e, allBuckets()[0], newMergeStatsAccumulator(), 10)
			require.NoError(t, w.primary.Set([]byte("primary"), []byte("value")))
			require.NoError(t, w.index.Set([]byte("index"), nil))
			w.count = 1

			if tc.discard {
				w.discard()
			} else {
				ctx := t.Context()
				if tc.cancel {
					cancelCtx, cancel := context.WithCancel(ctx)
					cancel()
					ctx = cancelCtx
				}
				err = w.flushWithCommitFailure(ctx, beforeCommit)
				if tc.wantErr != nil {
					require.ErrorIs(t, err, tc.wantErr)
				} else {
					require.NoError(t, err)
				}
			}
			require.Equal(t, tc.wantCalls, commitCalls)

			w.cleanup()
			require.NoError(t, e.Close(), "every cut must release current and replacement batches")
		})
	}
}

func TestMergeFoldCommitFailureRetryConvergesAndClosesCleanly(t *testing.T) {
	ctx := t.Context()
	open := func(name string) *enginepkg.Engine {
		t.Helper()
		e, err := enginepkg.Open(ctx, filepath.Join(t.TempDir(), name))
		require.NoError(t, err)
		return e
	}
	src, dst := open("src"), open("dst")
	srcSyncID, dstSyncID := ksuid.New().String(), ksuid.New().String()
	require.NoError(t, src.SetCurrentSync(ctx, srcSyncID))
	require.NoError(t, dst.SetCurrentSync(ctx, dstSyncID))
	require.NoError(t, src.PutGrantRecords(ctx, grantAt(srcSyncID, "grant", time.Unix(1, 0).UTC())))

	var grantBucket bucketSpec
	for _, candidate := range allBuckets() {
		if candidate.id == runBucketGrants {
			grantBucket = candidate
			break
		}
	}
	require.Equal(t, runBucketGrants, grantBucket.id)

	injected := errors.New("injected merge fold commit failure")
	_, err := mergeBucketRawIfNewerWithCommitFailure(
		ctx,
		dst,
		src,
		grantBucket,
		false,
		func() error { return injected },
	)
	require.ErrorIs(t, err, injected)

	_, err = mergeBucketRawIfNewerWithCommitFailure(ctx, dst, src, grantBucket, false, nil)
	require.NoError(t, err)
	require.Equal(t, 1, countGrants(t, dst, dstSyncID))

	require.NoError(t, src.Close())
	require.NoError(t, dst.Close(), "failed and retried merge must leave no fold batches outstanding")
}

func TestOverlayRestartCommitFailureReleasesBatches(t *testing.T) {
	e, err := enginepkg.Open(t.Context(), filepath.Join(t.TempDir(), "dest"))
	require.NoError(t, err)
	bucket := allBuckets()[0]
	stats := newMergeStatsAccumulator()
	w := newOverlayBucketRawWriter(e, bucket, stats, 10)
	require.NoError(t, w.primary.Set([]byte("pending"), []byte("value")))
	w.count = 1

	injected := errors.New("injected restart commit failure")
	err = overlayRestartBucketWithCommitFailure(
		t.Context(),
		e,
		bucket,
		w,
		stats,
		func() error { return injected },
	)
	require.ErrorIs(t, err, injected)

	w.cleanup()
	require.NoError(t, e.Close(), "restart failure must release discarded and replacement batches")
}
