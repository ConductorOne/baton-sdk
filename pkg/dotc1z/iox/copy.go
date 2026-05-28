// Package iox holds I/O helpers shared across the dotc1z subtree.
// It exists as a leaf of pkg/dotc1z so that sibling subpackages (e.g.
// format/v3, engine/pebble, manager/s3) can import the helpers
// without dragging in the rest of dotc1z.
package iox

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// ctxCopyChunkSize is the granularity at which CtxCopy checks the
// context. 4 MiB is large enough that the per-iteration select
// overhead is negligible against typical c1z write throughput, while
// small enough that a cancelled ctx is honored within a fraction of a
// second on any reasonable workload. The constant only controls
// poll frequency — it is NOT the working buffer size. CtxCopy holds
// a single 32 KiB buffer across iterations via io.CopyBuffer.
const ctxCopyChunkSize = 4 << 20

// ctxCopyBufferSize matches io.Copy's internal default (32 KiB) so
// the per-iteration read/write pattern is identical to plain io.Copy
// — only the cancellation poll is added.
const ctxCopyBufferSize = 32 * 1024

// CtxCopy is io.Copy with context cancellation. Reads up to
// ctxCopyChunkSize bytes per iteration, then checks ctx.Err() before
// the next read. Returns ctx.Err() (wrapped with the byte count
// copied so far) if the context is cancelled; otherwise returns the
// same outcome as io.Copy.
//
// Use this instead of io.Copy anywhere a c1z byte stream is being
// produced or consumed: a context cancel mid-stream must surface as
// an error to upstream consumers (specifically, to io.Pipe-based
// encryption goroutines whose CloseWithError signals s3manager to
// abort instead of CompleteMultipartUpload-ing a truncated body).
func CtxCopy(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, ctxCopyBufferSize)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, fmt.Errorf("iox.CtxCopy: copied %d bytes: %w", total, err)
		}
		n, err := io.CopyBuffer(dst, io.LimitReader(src, ctxCopyChunkSize), buf)
		total += n
		if errors.Is(err, io.EOF) {
			return total, nil
		}
		if err != nil {
			return total, err
		}
		// LimitReader returned because either the limit was hit
		// (n == chunk) or the source EOF'd within the chunk
		// (n < chunk). The latter case is the "source drained"
		// signal — io.CopyBuffer returns nil err on a short read
		// at EOF, so detect it via the byte count.
		if n < ctxCopyChunkSize {
			return total, nil
		}
	}
}
