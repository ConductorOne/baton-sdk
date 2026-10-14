package cli

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWatchParentStdinFlushesOnceBeforeExit covers the connector service's
// exit path: when stdin closes, telemetry is flushed before exiting, and the
// deferred flush that may also run does not shut otel down a second time.
func TestWatchParentStdinFlushesOnceBeforeExit(t *testing.T) {
	var events []string
	shutdowns := 0
	flush := onceOtelShutdown(func(ctx context.Context) error {
		_, hasDeadline := ctx.Deadline()
		require.True(t, hasDeadline)
		shutdowns++
		events = append(events, "flush")
		return nil
	})
	exit := func(code int) { events = append(events, "exit"); require.Equal(t, 0, code) }

	r, w := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchParentStdin(r, flush, exit)
	}()
	require.NoError(t, w.Close()) // the parent closes the service's stdin
	<-done

	flush() // the command's deferred flush
	require.Equal(t, []string{"flush", "exit"}, events)
	require.Equal(t, 1, shutdowns)
}

func TestWatchParentStdinIgnoresData(t *testing.T) {
	called := false
	watchParentStdin(strings.NewReader("x"), func() { called = true }, func(int) { called = true })
	require.False(t, called)
}

func TestOnceOtelShutdownNil(t *testing.T) {
	require.NotPanics(t, onceOtelShutdown(nil))
}
