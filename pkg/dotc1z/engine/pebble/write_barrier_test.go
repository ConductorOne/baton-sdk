package pebble

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// parkWriter runs a writeMu holder that blocks until the returned release
// channel is closed and returns once the holder is inside the lock. The
// second channel receives the holder's result.
func parkWriter(t *testing.T, e *Engine) (chan struct{}, chan error) {
	t.Helper()
	release := make(chan struct{})
	done := make(chan error, 1)
	entered := make(chan struct{})
	go func() {
		done <- e.withWriteAllowSealed(func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	return release, done
}

func notReturned[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s returned while a writer held writeMu", what)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestCloseAndCheckpointWaitForWriter: Close and CheckpointTo do not
// proceed while a writer is inside writeMu, and complete once it leaves.
func TestCloseAndCheckpointWaitForWriter(t *testing.T) {
	skipOnWindowsMemFS(t)
	ctx := context.Background()
	e, err := Open(ctx, "barrier-db", WithVFS(vfs.NewMem()))
	require.NoError(t, err)
	_, err = NewAdapter(e).StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	release, writer := parkWriter(t, e)
	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()
	ckpt := make(chan error, 1)
	go func() { ckpt <- e.CheckpointTo(ctx, "barrier-ckpt") }()
	notReturned(t, closed, "Close")
	notReturned(t, ckpt, "CheckpointTo")

	close(release)
	require.NoError(t, <-writer)
	require.NoError(t, <-closed)
	// writeMu hands off in no fixed order: the checkpoint either ran before
	// Close (nil) or found the engine closed.
	if err := <-ckpt; err != nil {
		require.ErrorIs(t, err, ErrEngineClosing)
	}
}

// TestWritesAfterCloseReturnErrEngineClosing: every writeMu entry point
// refuses with ErrEngineClosing after Close, and a write queued behind Close
// either lands before it or is refused — never a nil-db panic.
func TestWritesAfterCloseReturnErrEngineClosing(t *testing.T) {
	skipOnWindowsMemFS(t)
	ctx := context.Background()
	e, err := Open(ctx, "closing-db", WithVFS(vfs.NewMem()))
	require.NoError(t, err)
	_, err = NewAdapter(e).StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	release, writer := parkWriter(t, e)
	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()
	notReturned(t, closed, "Close")
	queued := make(chan error, 1)
	go func() {
		queued <- e.PutResourceTypeRecord(ctx, v3.ResourceTypeRecord_builder{
			ExternalId: "queued", DiscoveredAt: timestamppb.Now(),
		}.Build())
	}()
	close(release)
	require.NoError(t, <-writer)
	require.NoError(t, <-closed)
	if err := <-queued; err != nil {
		require.ErrorIs(t, err, ErrEngineClosing)
	}

	rt := v3.ResourceTypeRecord_builder{ExternalId: "late", DiscoveredAt: timestamppb.Now()}.Build()
	for name, err := range map[string]error{
		"PutResourceTypeRecord": e.PutResourceTypeRecord(ctx, rt),
		"withWriteAllowSealed":  e.withWriteAllowSealed(func() error { return errors.New("ran") }),
		"CheckpointTo":          e.CheckpointTo(ctx, "late-ckpt"),
		"Flush":                 e.Flush(ctx),
		"CompactAllRanges":      e.CompactAllRanges(ctx),
	} {
		require.ErrorIs(t, err, ErrEngineClosing, name)
	}
	_, err = e.BeginSynthesizedGrantLayer(ctx)
	require.ErrorIs(t, err, ErrEngineClosing, "BeginSynthesizedGrantLayer")
	require.NoError(t, e.AbortSynthesizedGrantLayer(ctx), "Abort touches no DB and stays callable")
	require.NoError(t, e.Close(), "second Close")
}
