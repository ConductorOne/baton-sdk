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
	key       string
	reason    string
	bytesRead int64
	expected  int64
}

func (r *deleterRecorder) fn(_ context.Context, key, reason string, br, exp int64) {
	r.calls = append(r.calls, deleterCall{key: key, reason: reason, bytesRead: br, expected: exp})
}

func TestVerifyUploadHappy(t *testing.T) {
	rec := &deleterRecorder{}
	headLen, err := verifyUpload(
		t.Context(),
		"objects/example.c1z",
		int64(1024),
		int64(1024),
		func(ctx context.Context, key string) (int64, error) { return 1024, nil },
		rec.fn,
	)
	require.NoError(t, err)
	require.Equal(t, int64(1024), headLen)
	require.Empty(t, rec.calls)
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
	_, err := verifyUpload(
		t.Context(),
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
	require.ErrorContains(t, err, "source short read")
	require.ErrorContains(t, err, "read 900, expected 1024")

	require.Len(t, rec.calls, 1)
	require.Equal(t, "source_short_read", rec.calls[0].reason)
	require.Equal(t, int64(900), rec.calls[0].bytesRead)
	require.Equal(t, int64(1024), rec.calls[0].expected)
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
