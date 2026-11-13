package connector

import (
	"bufio"
	"context"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// childModeEnv makes the re-executed test binary act as the connector
// service subprocess instead of running tests.
const childModeEnv = "BATON_CONNECTOR_SHUTDOWN_TEST_CHILD"

const (
	childExitOnEOF   = "exit-on-eof"   // exits as soon as stdin closes, like the real service
	childIgnoreEOF   = "ignore-eof"    // keeps running after stdin closes, like a service stuck flushing
	childStartupWait = 5 * time.Second // generous bound for the child to start
)

func TestMain(m *testing.M) {
	switch os.Getenv(childModeEnv) {
	case childExitOnEOF:
		runChild(false)
	case childIgnoreEOF:
		runChild(true)
	}
	os.Exit(m.Run())
}

// runChild reads the server config line the parent writes, then waits for
// stdin to close. With ignoreEOF it ignores both EOF and SIGINT/SIGTERM and
// sleeps, so only a kill stops it.
func runChild(ignoreEOF bool) {
	signal.Ignore(os.Interrupt, syscall.SIGTERM)
	r := bufio.NewReader(os.Stdin)
	_, _ = r.ReadString('\n')
	_, _ = r.ReadByte() // returns at EOF
	if ignoreEOF {
		time.Sleep(time.Hour)
	}
	os.Exit(0)
}

func startChild(t *testing.T, ctx context.Context, mode string) *wrapper {
	t.Helper()
	t.Setenv(childModeEnv, mode)
	cw := &wrapper{now: time.Now}
	_, err := cw.runServer(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, cw.serverDone)
	return cw
}

func requireExited(t *testing.T, done <-chan struct{}, within time.Duration) {
	t.Helper()
	select {
	case <-done:
		return
	default:
	}
	select {
	case <-done:
	case <-time.After(within):
		t.Fatalf("connector service subprocess still running after %v", within)
	}
}

// TestCloseWaitsForServiceExit is the happy path: closing the wrapper closes
// the service's stdin, and Close returns once the service has exited.
func TestCloseWaitsForServiceExit(t *testing.T) {
	cw := startChild(t, t.Context(), childExitOnEOF)
	done := cw.serverDone

	start := time.Now()
	require.NoError(t, cw.Close())
	require.Less(t, time.Since(start), childStartupWait)
	requireExited(t, done, 0)
}

// TestStuckServiceIsKilledAfterGrace is the fault case: a service that never
// exits after stdin closes (for example stuck exporting to an unreachable
// collector) must not hold up the parent beyond the grace period, and is
// killed once the run context ends and the grace elapses.
func TestStuckServiceIsKilledAfterGrace(t *testing.T) {
	prev := serviceShutdownGrace
	serviceShutdownGrace = 500 * time.Millisecond
	t.Cleanup(func() { serviceShutdownGrace = prev })

	ctx, cancel := context.WithCancel(t.Context())
	cw := startChild(t, ctx, childIgnoreEOF)
	done := cw.serverDone

	// Shutdown order of the runner: the run context ends, then Close.
	cancel()
	start := time.Now()
	require.NoError(t, cw.Close())
	elapsed := time.Since(start)
	require.Less(t, elapsed, serviceShutdownGrace+time.Second, "Close blocked past the grace period")

	// exec kills the child WaitDelay (= grace) after the cancel.
	requireExited(t, done, serviceShutdownGrace+2*time.Second)
}

// TestCloseStopsServiceWhenConnCloseFails: an error closing the client
// connection is reported but does not skip stopping the service.
func TestCloseStopsServiceWhenConnCloseFails(t *testing.T) {
	cw := startChild(t, t.Context(), childExitOnEOF)
	done := cw.serverDone

	conn, err := grpc.NewClient("127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	cw.conn = conn // closing it again fails

	err = cw.Close()
	require.ErrorContains(t, err, "error closing client connection")
	requireExited(t, done, 0)
	require.Nil(t, cw.serverStdin)
}

// TestCloseDoesNotHoldLockWhileWaiting: while Close waits for the service to
// exit, the wrapper lock is free and the fields are already cleared.
func TestCloseDoesNotHoldLockWhileWaiting(t *testing.T) {
	prev := serviceShutdownGrace
	serviceShutdownGrace = 2 * time.Second
	t.Cleanup(func() { serviceShutdownGrace = prev })

	cw := &wrapper{now: time.Now, serverDone: make(chan struct{})} // never closed
	closed := make(chan error, 1)
	go func() { closed <- cw.Close() }()

	require.Eventually(t, func() bool {
		if !cw.mtx.TryRLock() {
			return false
		}
		defer cw.mtx.RUnlock()
		return cw.serverDone == nil && cw.client == nil && cw.conn == nil
	}, serviceShutdownGrace/2, 5*time.Millisecond, "lock held or state not cleared while Close waits")

	select {
	case <-closed:
		t.Fatal("Close returned before the grace period elapsed")
	default:
	}
	require.NoError(t, <-closed)
}
