package dotc1z

import (
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// TestSaveC1z_SetsFrameContentSize verifies that saveC1z records the
// decompressed db size in the zstd Frame_Content_Size (FCS) field. This lets
// readers learn the decoded size without decompressing the stream — which
// closes the gap that forced us to run the decoder against the max-size cap
// to discover how big a c1z actually is.
func TestSaveC1z_SetsFrameContentSize(t *testing.T) {
	tmpDir := t.TempDir()

	// Write a non-trivial db file so the size is distinguishable.
	const dbSize = 123_456
	dbPath := filepath.Join(tmpDir, "src.db")
	data := make([]byte, dbSize)
	for i := range data {
		data[i] = byte(i % 251)
	}
	require.NoError(t, os.WriteFile(dbPath, data, 0600))

	c1zPath := filepath.Join(tmpDir, "out.c1z")
	require.NoError(t, saveC1z(dbPath, c1zPath, 1))

	// Open the c1z, skip the 5-byte C1ZF magic, and parse the zstd frame
	// header directly. FCS should now be populated with the exact db size.
	raw, err := os.ReadFile(c1zPath)
	require.NoError(t, err)
	require.Greater(t, len(raw), len(C1ZFileHeader))
	zstdStream := raw[len(C1ZFileHeader):]

	header := zstd.Header{}
	require.NoError(t, header.Decode(zstdStream))

	require.True(t, header.HasFCS, "zstd frame must advertise Frame_Content_Size after save")
	require.Equal(t, uint64(dbSize), header.FrameContentSize,
		"FCS in header must equal the decompressed db size")
}

// TestC1ZDecoder_WindowSizeExceeded verifies that a sufficiently large c1z
// triggers ErrWindowSizeExceeded when the decoder memory budget is too small
// for the encoder's sliding window. This previously lived inline in
// TestC1ZDecoder but moved here after saveC1z started setting FCS — small c1z
// frames now use SingleSegment mode and fit inside modest memory budgets, so
// exercising the window-size limit requires a content larger than the
// encoder's max single-segment size (128 MiB spec limit, but klauspost
// switches to a sliding window at ~8 MiB by default).
func TestC1ZDecoder_WindowSizeExceeded(t *testing.T) {
	tmpDir := t.TempDir()

	// Write ~16 MiB of semi-random data — random enough to not fully
	// compress to nothing, and large enough to force the encoder off of
	// single-segment mode and onto a sliding window that a 1 MiB decoder
	// memory budget cannot hold.
	const dbSize = 16 * 1024 * 1024
	dbPath := filepath.Join(tmpDir, "large.db")
	data := make([]byte, dbSize)
	//nolint:gosec // deterministic test fixture; no security context.
	rng := rand.New(rand.NewSource(42))
	_, err := rng.Read(data)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dbPath, data, 0600))

	c1zPath := filepath.Join(tmpDir, "large.c1z")
	require.NoError(t, saveC1z(dbPath, c1zPath, 1))

	f, err := os.Open(c1zPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	// 1 MiB decoder memory budget — too small for the encoder's window.
	d, err := NewDecoder(f, WithDecoderMaxMemory(1*1024*1024))
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })

	_, err = io.Copy(io.Discard, d)
	require.ErrorIs(t, err, ErrWindowSizeExceeded)
}
