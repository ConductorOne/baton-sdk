package grpc

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestTransportStreamSetHeaderRejectsHeadersSentAfterUnlockedCheck(t *testing.T) {
	stream := NewTransportStream(&grpc.MethodDesc{MethodName: "Test"})
	stream.mtx.Lock()

	errCh := make(chan error, 1)
	go func() {
		errCh <- stream.SetHeader(metadata.Pairs("late-header", "value"))
	}()

	waitForTransportStreamMutexWait(t, "SetHeader")
	stream.headersSent.Store(true)
	stream.mtx.Unlock()

	require.ErrorIs(t, <-errCh, ErrIllegalHeaderWrite)
	require.Empty(t, stream.headers.Get("late-header"))
}

func TestTransportStreamSetTrailerRejectsHeadersSentAfterUnlockedCheck(t *testing.T) {
	stream := NewTransportStream(&grpc.MethodDesc{MethodName: "Test"})
	stream.mtx.Lock()

	errCh := make(chan error, 1)
	go func() {
		errCh <- stream.SetTrailer(metadata.Pairs("late-trailer", "value"))
	}()

	waitForTransportStreamMutexWait(t, "SetTrailer")
	stream.headersSent.Store(true)
	stream.mtx.Unlock()

	require.ErrorIs(t, <-errCh, ErrIllegalHeaderWrite)
	require.Empty(t, stream.trailers.Get("late-trailer"))
}

func waitForTransportStreamMutexWait(t *testing.T, method string) {
	t.Helper()

	methodFrame := "github.com/conductorone/baton-sdk/pkg/lambda/grpc.(*TransportStream)." + method
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var stack [1 << 20]byte
		n := runtime.Stack(stack[:], true)
		stacks := string(stack[:n])
		if strings.Contains(stacks, methodFrame) && strings.Contains(stacks, "sync.(*Mutex).Lock") {
			return
		}
		runtime.Gosched()
	}

	t.Fatalf("timed out waiting for %s to block on TransportStream mutex", method)
}
