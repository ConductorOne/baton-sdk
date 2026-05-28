package iox

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

// Source that yields exactly 0 bytes — exercises the empty-payloadDir
// downstream case where writeZstdTar invokes CtxCopy against a zero-
// byte file (or a directory walk produces no regular files).
func TestCtxCopyEmptySource(t *testing.T) {
	var dst bytes.Buffer
	n, err := CtxCopy(t.Context(), &dst, bytes.NewReader(nil))
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
	require.Equal(t, 0, dst.Len())
}

// TestCtxCopyCancelBeforeStart makes sure a context that's already
// cancelled bails out immediately without copying any bytes.
func TestCtxCopyCancelBeforeStart(t *testing.T) {
	src := bytes.NewReader(make([]byte, ctxCopyChunkSize*3))
	var dst bytes.Buffer
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	n, err := CtxCopy(ctx, &dst, src)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "copied 0 bytes")
	require.Equal(t, int64(0), n)
	require.Equal(t, 0, dst.Len())
}

// TestCtxCopyCancelMidStream cancels the context after the first
// 4 MiB chunk completes, then asserts the next iteration's ctx.Err()
// check fires and we bail out without draining the remaining four
// chunks of the source. Cancellation granularity is per-chunk, not
// per-byte; ~4 MiB worst-case latency is documented on CtxCopy.
func TestCtxCopyCancelMidStream(t *testing.T) {
	src := &countingReader{r: bytes.NewReader(make([]byte, ctxCopyChunkSize*5))}
	ctx, cancel := context.WithCancel(t.Context())
	cw := &cancelAfterBytes{threshold: ctxCopyChunkSize, cancel: cancel}
	n, err := CtxCopy(ctx, cw, src)
	require.ErrorIs(t, err, context.Canceled)
	// First chunk delivered fully; subsequent chunks were skipped.
	require.Equal(t, int64(ctxCopyChunkSize), n)
	require.True(t, cw.canceled, "cancel was not triggered by the writer threshold")
	// The source must NOT have been fully drained: prove it by
	// asserting fewer bytes were read than the full source size.
	require.Less(t, src.bytesRead, int64(ctxCopyChunkSize*5), "source was over-read past the cancel boundary")
}

// TestCtxCopyPropagatesSourceError keeps the io.Copy contract: a
// non-EOF error from the source is returned to the caller and aborts
// the loop. Also asserts the partial-byte count is preserved.
func TestCtxCopyPropagatesSourceError(t *testing.T) {
	payload := bytes.Repeat([]byte{0xAB}, 16)
	src := &errOnSecondReader{data: payload, err: errors.New("disk eof")}
	var dst bytes.Buffer
	n, err := CtxCopy(t.Context(), &dst, src)
	require.ErrorContains(t, err, "disk eof")
	require.Equal(t, int64(len(payload)), n, "partial bytes copied before the error must be reported")
	require.Equal(t, payload, dst.Bytes())
}

func TestCtxCopyPropagatesDstWriteError(t *testing.T) {
	src := bytes.NewReader(make([]byte, 64))
	dst := &errOnWrite{err: errors.New("disk full")}
	_, err := CtxCopy(t.Context(), dst, src)
	require.ErrorContains(t, err, "disk full")
}

func TestCtxCopyPropagatesDstShortWrite(t *testing.T) {
	src := bytes.NewReader(make([]byte, 64))
	dst := &shortWriter{}
	_, err := CtxCopy(t.Context(), dst, src)
	require.ErrorIs(t, err, io.ErrShortWrite)
}

// cancelAfterBytes is an io.Writer that fires the cancel func once
// total Write traffic crosses the configured threshold.
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

// errOnSecondReader returns its full data on the first Read, then
// the configured error on every subsequent call. Sized small enough
// (16 bytes) that a single Read drains it regardless of io.Copy's
// internal buffer size.
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

type errOnWrite struct{ err error }

func (e *errOnWrite) Write(p []byte) (int, error) { return 0, e.err }

// shortWriter returns less than len(p) without an error — io.Copy
// turns this into io.ErrShortWrite per its contract.
type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) / 2, nil }

// countingReader tracks bytes read so tests can prove the source was
// not over-consumed when a cancel was supposed to stop us early.
type countingReader struct {
	r         io.Reader
	bytesRead int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.bytesRead += int64(n)
	return n, err
}

// Static interface checks.
var (
	_ io.Writer = (*cancelAfterBytes)(nil)
	_ io.Reader = (*errOnSecondReader)(nil)
	_ io.Writer = (*errOnWrite)(nil)
	_ io.Writer = shortWriter{}
	_ io.Reader = (*countingReader)(nil)
)
