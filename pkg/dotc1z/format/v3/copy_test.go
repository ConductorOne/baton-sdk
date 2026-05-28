package v3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCtxCopyHappyPath(t *testing.T) {
	src := bytes.NewReader(make([]byte, 17))
	var dst bytes.Buffer
	n, err := CtxCopy(t.Context(), &dst, src)
	require.NoError(t, err)
	require.Equal(t, int64(17), n)
	require.Equal(t, 17, dst.Len())
}

// TestCtxCopyCancelBeforeStart makes sure a context that's already
// cancelled bails out immediately without copying any bytes.
func TestCtxCopyCancelBeforeStart(t *testing.T) {
	src := bytes.NewReader(make([]byte, CtxCopyChunkSize*3))
	var dst bytes.Buffer
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	n, err := CtxCopy(ctx, &dst, src)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(0), n)
	require.Equal(t, 0, dst.Len())
}

// TestCtxCopyCancelMidStream cancels the context after the first
// 4 MiB chunk completes, then asserts the next iteration's ctx.Err()
// check fires and we bail out without draining the remaining four
// chunks of the source. Cancellation granularity is per-chunk, not
// per-byte; ~4 MiB worst-case latency is documented on CtxCopy.
func TestCtxCopyCancelMidStream(t *testing.T) {
	src := bytes.NewReader(make([]byte, CtxCopyChunkSize*5))
	ctx, cancel := context.WithCancel(t.Context())
	cw := &cancelAfterBytes{threshold: CtxCopyChunkSize, cancel: cancel}
	n, err := CtxCopy(ctx, cw, src)
	require.ErrorIs(t, err, context.Canceled)
	// First chunk delivered fully; subsequent chunks were skipped.
	require.Equal(t, int64(CtxCopyChunkSize), n)
	require.True(t, cw.canceled, "cancel was not triggered by the writer threshold")
}

// TestCtxCopyPropagatesSourceError keeps the io.Copy contract: a
// non-EOF error from the source is returned to the caller and aborts
// the loop.
func TestCtxCopyPropagatesSourceError(t *testing.T) {
	src := &errOnSecondReader{data: make([]byte, CtxCopyChunkSize), err: errors.New("disk eof")}
	var dst bytes.Buffer
	_, err := CtxCopy(t.Context(), &dst, src)
	require.ErrorContains(t, err, "disk eof")
}

// cancelAfterBytes is an io.Writer that fires the cancel func once
// total Write traffic crosses the configured threshold. Mirrors the
// real-world shape: the upstream cancel arrives partway through a
// large copy, after some bytes have already been delivered.
type cancelAfterBytes struct {
	threshold int
	written   int
	canceled  bool
	cancel    context.CancelFunc
}

func (c *cancelAfterBytes) Write(p []byte) (int, error) {
	c.written += len(p)
	if !c.canceled && c.written >= c.threshold {
		c.canceled = true
		c.cancel()
	}
	return len(p), nil
}

// errOnSecondReader returns data once, then a non-EOF error on the
// next Read. Used to verify CtxCopy propagates non-EOF reader errors.
type errOnSecondReader struct {
	data    []byte
	err     error
	emitted bool
}

func (r *errOnSecondReader) Read(p []byte) (int, error) {
	if !r.emitted {
		r.emitted = true
		return copy(p, r.data), nil
	}
	return 0, r.err
}

// Static interface checks: keep the test helpers honest.
var (
	_ io.Writer = (*cancelAfterBytes)(nil)
	_ io.Reader = (*errOnSecondReader)(nil)
)
