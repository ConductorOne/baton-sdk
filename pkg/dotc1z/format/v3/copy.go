package v3

import (
	"context"
	"errors"
	"io"
)

// CtxCopyChunkSize is the granularity at which CtxCopy checks the
// context. 4 MiB is large enough that the per-chunk select overhead is
// negligible against typical c1z write throughput (tens of MiB/s),
// while small enough that a cancelled ctx is honored within a fraction
// of a second on any reasonable workload.
const CtxCopyChunkSize = 4 << 20

// CtxCopy is io.Copy with context cancellation. Reads up to
// CtxCopyChunkSize bytes per iteration, then checks ctx.Done() before
// the next read. Returns ctx.Err() with the number of bytes copied so
// far if the context is cancelled; otherwise returns the same outcome
// as io.Copy.
//
// Use this instead of io.Copy anywhere a c1z byte stream is being
// produced or consumed: a context cancel mid-stream must surface as an
// error to upstream consumers (specifically, to io.Pipe-based encryption
// goroutines whose CloseWithError signals s3manager to abort instead of
// CompleteMultipartUpload-ing a truncated body).
func CtxCopy(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := io.CopyN(dst, src, CtxCopyChunkSize)
		total += n
		if errors.Is(err, io.EOF) {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}
