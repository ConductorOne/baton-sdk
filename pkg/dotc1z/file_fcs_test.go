package dotc1z

import (
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

