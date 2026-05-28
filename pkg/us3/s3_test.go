package us3

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountingReader(t *testing.T) {
	const payload = "abcdefghij"
	src := strings.NewReader(payload)
	cr := &countingReader{r: src}

	// Drain through io.Copy into io.Discard so we exercise the buffer
	// loop, not just a single Read call.
	n, err := io.Copy(io.Discard, cr)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), n)
	require.Equal(t, int64(len(payload)), cr.BytesRead())
}

func TestCountingReaderRecordsErrorPath(t *testing.T) {
	// First Read returns 3 bytes + a non-EOF error: counter must still
	// reflect the 3 bytes that were successfully delivered.
	src := &readerWithError{data: []byte("abc"), err: errors.New("boom")}
	cr := &countingReader{r: src}
	buf := make([]byte, 8)
	n, err := cr.Read(buf)
	require.Equal(t, 3, n)
	require.ErrorContains(t, err, "boom")
	require.Equal(t, int64(3), cr.BytesRead())
}

type readerWithError struct {
	data []byte
	err  error
	pos  int
}

func (r *readerWithError) Read(p []byte) (int, error) {
	n := copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos >= len(r.data) {
		return n, r.err
	}
	return n, nil
}

// deleterRecorder captures invocations so tests can assert that the
// best-effort cleanup fired with the expected reason / sizes.
type deleterRecorder struct {
	calls []deleterCall
}

type deleterCall struct {
	ctx       context.Context
	key       string
	reason    string
	bytesRead int64
	expected  int64
}

func (r *deleterRecorder) fn(ctx context.Context, key, reason string, br, exp int64) {
	r.calls = append(r.calls, deleterCall{ctx: ctx, key: key, reason: reason, bytesRead: br, expected: exp})
}

// headRecorder counts and remembers the args of headObject calls so
// happy-path tests can verify the HeadObject step actually ran (not
// just the absence of a deleter call).
type headRecorder struct {
	calls   int
	lastKey string
}

func (h *headRecorder) returning(contentLength int64) func(ctx context.Context, key string) (int64, error) {
	return func(_ context.Context, key string) (int64, error) {
		h.calls++
		h.lastKey = key
		return contentLength, nil
	}
}

func TestVerifyUploadHappy(t *testing.T) {
	rec := &deleterRecorder{}
	head := &headRecorder{}
	headLen, err := verifyUpload(
		t.Context(),
		"objects/example.c1z",
		int64(1024),
		int64(1024),
		head.returning(1024),
		rec.fn,
	)
	require.NoError(t, err)
	require.Equal(t, int64(1024), headLen)
	require.Empty(t, rec.calls)
	require.Equal(t, 1, head.calls, "HeadObject must be called on the happy path")
	require.Equal(t, "objects/example.c1z", head.lastKey)
}

func TestVerifyUploadHappyEmptyBody(t *testing.T) {
	// expectedSize=0 is a real known size for an empty body, distinct
	// from the -1 sentinel. Must NOT trip the bytes-vs-expected check.
	rec := &deleterRecorder{}
	head := &headRecorder{}
	headLen, err := verifyUpload(
		t.Context(),
		"objects/empty.c1z",
		int64(0),
		int64(0),
		head.returning(0),
		rec.fn,
	)
	require.NoError(t, err)
	require.Equal(t, int64(0), headLen)
	require.Empty(t, rec.calls)
	require.Equal(t, 1, head.calls)
}

func TestVerifyUploadEmptyExpectedButGotBytes(t *testing.T) {
	// expectedSize=0 but bytesRead>0 — the over-read branch must
	// fire, NOT a misleading "short read".
	rec := &deleterRecorder{}
	_, err := verifyUpload(
		t.Context(),
		"objects/empty.c1z",
		int64(7),
		int64(0),
		func(_ context.Context, _ string) (int64, error) {
			t.Fatalf("HeadObject must not be called when source over-read is detected first")
			return 0, nil
		},
		rec.fn,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "over-read")
	require.ErrorContains(t, err, "read 7, expected 0")
	require.Len(t, rec.calls, 1)
	require.Equal(t, "source_over_read", rec.calls[0].reason)
}

func TestVerifyUploadExpectedSizeMissing(t *testing.T) {
	// expectedSize = -1 means "don't assert against the source": only
	// the HeadObject vs bytesRead invariant is checked.
	rec := &deleterRecorder{}
	headLen, err := verifyUpload(
		t.Context(),
		"objects/example.c1z",
		int64(100),
		int64(-1),
		func(ctx context.Context, key string) (int64, error) { return 100, nil },
		rec.fn,
	)
	require.NoError(t, err)
	require.Equal(t, int64(100), headLen)
	require.Empty(t, rec.calls)
}

func TestVerifyUploadSourceShortRead(t *testing.T) {
	rec := &deleterRecorder{}
	parentCtx := t.Context()
	_, err := verifyUpload(
		parentCtx,
		"objects/example.c1z",
		int64(900),
		int64(1024),
		func(ctx context.Context, key string) (int64, error) {
			t.Fatalf("HeadObject must not be called when source short-read is detected first")
			return 0, nil
		},
		rec.fn,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "short read")
	require.ErrorContains(t, err, "read 900, expected 1024")

	require.Len(t, rec.calls, 1)
	require.Equal(t, "source_short_read", rec.calls[0].reason)
	require.Equal(t, int64(900), rec.calls[0].bytesRead)
	require.Equal(t, int64(1024), rec.calls[0].expected)
	// verifyUpload should forward the parent context to the deleter
	// unchanged. The detach (context.WithoutCancel + timeout) lives
	// inside deleteAfterTruncation, not verifyUpload — that
	// separation is load-bearing for the "best-effort cleanup runs
	// even when the caller has been canceled" contract.
	require.Same(t, parentCtx, rec.calls[0].ctx)
}

func TestVerifyUploadHeadObjectError(t *testing.T) {
	rec := &deleterRecorder{}
	_, err := verifyUpload(
		t.Context(),
		"objects/example.c1z",
		int64(1024),
		int64(1024),
		func(ctx context.Context, key string) (int64, error) {
			return 0, errors.New("s3 head failed: 500")
		},
		rec.fn,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "HeadObject verify failed")

	require.Len(t, rec.calls, 1)
	require.Equal(t, "head_object_failed", rec.calls[0].reason)
	require.Equal(t, int64(-1), rec.calls[0].expected)
}

func TestVerifyUploadHeadSizeMismatch(t *testing.T) {
	// This is the canonical truncated-multipart case: our reader sent
	// 1024 bytes, but the committed object only holds 900. Multipart
	// said success; the size cross-check is what catches it.
	rec := &deleterRecorder{}
	_, err := verifyUpload(
		t.Context(),
		"objects/example.c1z",
		int64(1024),
		int64(1024),
		func(ctx context.Context, key string) (int64, error) { return 900, nil },
		rec.fn,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "uploaded object size mismatch")
	require.ErrorContains(t, err, "HeadObject=900")
	require.ErrorContains(t, err, "bytesRead=1024")

	require.Len(t, rec.calls, 1)
	require.Equal(t, "head_object_size_mismatch", rec.calls[0].reason)
	require.Equal(t, int64(1024), rec.calls[0].bytesRead)
	require.Equal(t, int64(900), rec.calls[0].expected)
}

func TestVerifyUploadNoExpectedSizeButMismatchedHead(t *testing.T) {
	// expectedSize unknown (-1) — source check is skipped, but head
	// vs bytesRead still catches the truncated commit.
	rec := &deleterRecorder{}
	_, err := verifyUpload(
		t.Context(),
		"objects/example.c1z",
		int64(1024),
		int64(-1),
		func(ctx context.Context, key string) (int64, error) { return 1000, nil },
		rec.fn,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "uploaded object size mismatch")
	require.Len(t, rec.calls, 1)
	require.Equal(t, "head_object_size_mismatch", rec.calls[0].reason)
}
