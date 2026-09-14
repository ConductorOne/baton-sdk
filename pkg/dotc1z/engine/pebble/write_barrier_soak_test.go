package pebble

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

// TestWriteBarrierSoak runs writers, a checkpointer, and a sealer against
// one engine from independent goroutines, then Closes while they are still
// running. Every call must return nil or a lifecycle error; a panic
// (WaitGroup misuse, nil db) or a hang (test timeout) fails it. Run with
// -race and -count to sample schedules.
func TestWriteBarrierSoak(t *testing.T) {
	skipOnWindowsMemFS(t)
	const window = 300 * time.Millisecond
	ctx := context.Background()
	memFS := vfs.NewMem()
	e, err := Open(ctx, "soak-db", WithVFS(memFS))
	require.NoError(t, err)
	syncID, err := NewAdapter(e).StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	sid := sessions.WithSyncID(syncID)

	tolerated := func(err error) bool {
		return err == nil ||
			errors.Is(err, ErrEngineClosing) ||
			errors.Is(err, ErrEngineSealed) ||
			errors.Is(err, ErrNoCurrentSync) ||
			errors.Is(err, fs.ErrExist) ||
			errors.Is(err, os.ErrExist)
	}
	var (
		mu         sync.Mutex
		unexpected []error
	)
	report := func(op string, err error) {
		mu.Lock()
		defer mu.Unlock()
		unexpected = append(unexpected, fmt.Errorf("%s: %w", op, err))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var writes, checkpoints, seals atomic.Int64
	for w := 0; w < 8; w++ {
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				var err error
				if i%2 == 0 {
					err = e.SessionSet(ctx, fmt.Sprintf("k-%d-%d", w, i), []byte("v"), sid)
				} else {
					err = e.PutResourceTypeRecord(ctx, v3.ResourceTypeRecord_builder{
						ExternalId:   fmt.Sprintf("rt-%d-%d", w, i),
						DisplayName:  "soak",
						DiscoveredAt: timestamppb.Now(),
					}.Build())
				}
				if !tolerated(err) {
					report("write", err)
					return
				}
				if err == nil {
					writes.Add(1)
				}
			}
		})
	}
	wg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			dir := fmt.Sprintf("soak-ckpt-%d", i)
			err := e.CheckpointTo(ctx, dir)
			if !tolerated(err) {
				report("checkpoint", err)
				return
			}
			if err == nil {
				checkpoints.Add(1)
				_ = memFS.RemoveAll(dir)
			}
		}
	})
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			e.seal()
			seals.Add(1)
			runtime.Gosched()
			e.unseal()
		}
	})

	time.Sleep(window)
	require.NoError(t, e.Close())
	require.NoError(t, e.Close(), "second Close")
	close(stop)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Empty(t, unexpected)
	t.Logf("writes=%d checkpoints=%d seals=%d", writes.Load(), checkpoints.Load(), seals.Load())
	require.Positive(t, writes.Load())
	require.Positive(t, checkpoints.Load())
}
