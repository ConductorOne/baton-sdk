package cli

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWatchParentStdinFlushesOnceBeforeExit covers the connector service's
// exit path: when stdin closes, telemetry is flushed before exiting, and the
// deferred flush that may also run does not shut otel down a second time.
func TestWatchParentStdinFlushesOnceBeforeExit(t *testing.T) {
	var events []string
	shutdowns := 0
	hasDeadline := false
	exitCode := -1
	flush := onceOtelShutdown(func(ctx context.Context) error {
		_, hasDeadline = ctx.Deadline()
		shutdowns++
		events = append(events, "flush")
		return nil
	})
	exit := func(code int) { events = append(events, "exit"); exitCode = code }

	r, w := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchParentStdin(r, flush, exit)
	}()
	require.NoError(t, w.Close()) // the parent closes the service's stdin
	<-done
	require.True(t, hasDeadline)
	require.Equal(t, 0, exitCode)

	flush() // the command's deferred flush
	require.Equal(t, []string{"flush", "exit"}, events)
	require.Equal(t, 1, shutdowns)
}

func TestWatchParentStdinExitsAfterDataThenEOF(t *testing.T) {
	var events []string
	exitCode := -1
	r, w := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchParentStdin(r, func() { events = append(events, "flush") }, func(code int) {
			events = append(events, "exit")
			exitCode = code
		})
	}()
	_, err := w.Write([]byte("stray"))
	require.NoError(t, err)
	_, err = w.Write([]byte("more"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	<-done
	require.Equal(t, 0, exitCode)
	require.Equal(t, []string{"flush", "exit"}, events)
}

func TestOnceOtelShutdownNil(t *testing.T) {
	require.NotPanics(t, onceOtelShutdown(nil))
}
