package dotc1z

import (
	"errors"
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

// TestDecoder_DeclaredDecodedSize verifies the consumer-side FCS peek:
// DeclaredDecodedSize() returns the exact db size advertised in the frame
// header for c1zs saved by the current producer, without decompressing.
func TestDecoder_DeclaredDecodedSize(t *testing.T) {
	tmpDir := t.TempDir()
	const dbSize = 271_828 // non-round, distinguishable from any default
	dbPath := filepath.Join(tmpDir, "src.db")
	data := make([]byte, dbSize)
	for i := range data {
		data[i] = byte(i % 251)
	}
	require.NoError(t, os.WriteFile(dbPath, data, 0600))

	c1zPath := filepath.Join(tmpDir, "out.c1z")
	require.NoError(t, saveC1z(dbPath, c1zPath, 1))

	f, err := os.Open(c1zPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	d, err := NewDecoder(f)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })

	// No Read yet — DeclaredDecodedSize must trigger header parsing.
	size, ok := d.DeclaredDecodedSize()
	require.True(t, ok, "FCS must be declared for c1zs saved by the current producer")
	require.Equal(t, uint64(dbSize), size)

	// Subsequent Read must still decompress the full stream.
	decoded, err := io.ReadAll(d)
	require.NoError(t, err)
	require.Equal(t, data, decoded)

	// Second call returns the same value (idempotent).
	size2, ok2 := d.DeclaredDecodedSize()
	require.True(t, ok2)
	require.Equal(t, size, size2)
}

// TestDecoder_DeclaredDecodedSize_LegacyWithoutFCS verifies the backward-compat
// path: a c1z produced before the FCS change (hand-built here without
// ResetContentSize) returns (0, false) from DeclaredDecodedSize, and reads
// normally.
func TestDecoder_DeclaredDecodedSize_LegacyWithoutFCS(t *testing.T) {
	tmpDir := t.TempDir()

	// Build a legacy-format c1z directly: C1ZF magic + zstd frame WITHOUT
	// FCS. The default klauspost encoder (no ResetContentSize call) produces
	// exactly this layout.
	payload := []byte("legacy c1z payload without FCS")
	c1zPath := filepath.Join(tmpDir, "legacy.c1z")
	f, err := os.Create(c1zPath)
	require.NoError(t, err)
	_, err = f.Write(C1ZFileHeader)
	require.NoError(t, err)
	enc, err := zstd.NewWriter(f, zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	_, err = enc.Write(payload)
	require.NoError(t, err)
	require.NoError(t, enc.Close())
	require.NoError(t, f.Close())

	rf, err := os.Open(c1zPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rf.Close() })

	d, err := NewDecoder(rf)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })

	size, ok := d.DeclaredDecodedSize()
	require.False(t, ok, "legacy c1z must not advertise FCS")
	require.Zero(t, size)

	decoded, err := io.ReadAll(d)
	require.NoError(t, err)
	require.Equal(t, payload, decoded)
}

// TestDecoder_DeclaredSizeExceedsCap_FailsFast verifies the fail-fast path: if
// the frame's declared size exceeds the caller's DecoderMaxDecodedSize, the
// very first Read returns ErrMaxSizeExceeded with the declared size embedded
// in the error message, before any decompression happens.
func TestDecoder_DeclaredSizeExceedsCap_FailsFast(t *testing.T) {
	tmpDir := t.TempDir()
	const dbSize = 1 << 20 // 1 MiB
	dbPath := filepath.Join(tmpDir, "src.db")
	data := make([]byte, dbSize)
	for i := range data {
		data[i] = byte(i * 7 % 251)
	}
	require.NoError(t, os.WriteFile(dbPath, data, 0600))

	c1zPath := filepath.Join(tmpDir, "out.c1z")
	require.NoError(t, saveC1z(dbPath, c1zPath, 1))

	f, err := os.Open(c1zPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	// Cap set below the declared size — init should surface the error on the
	// very first Read without the decoder ever producing a byte.
	d, err := NewDecoder(f, WithDecoderMaxDecodedSize(dbSize/2))
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })

	// DeclaredDecodedSize still reports the declared size — the inspection
	// path is still useful even when the cap rejects the stream.
	size, ok := d.DeclaredDecodedSize()
	require.True(t, ok)
	require.Equal(t, uint64(dbSize), size)

	var buf [64]byte
	n, err := d.Read(buf[:])
	require.Equal(t, 0, n, "no bytes must be produced when the cap rejects declared size")
	require.ErrorIs(t, err, ErrMaxSizeExceeded)
	require.True(t, errors.Is(err, ErrMaxSizeExceeded))
	require.Contains(t, err.Error(), "declared decoded size")
}

// TestDecoder_FCSFailFastKillSwitch verifies that flipping
// fcsFailFastDisabled (driven by BATON_DISABLE_FCS_FAIL_FAST=1 at process
// start) bypasses the init-time pre-check and falls back to the pre-FCS
// behavior: Read runs the decoder and trips ErrMaxSizeExceeded only after
// decoded bytes pass the cap.
func TestDecoder_FCSFailFastKillSwitch(t *testing.T) {
	tmpDir := t.TempDir()
	const dbSize = 1 << 20 // 1 MiB
	dbPath := filepath.Join(tmpDir, "src.db")
	data := make([]byte, dbSize)
	for i := range data {
		data[i] = byte(i * 11 % 251)
	}
	require.NoError(t, os.WriteFile(dbPath, data, 0600))

	c1zPath := filepath.Join(tmpDir, "out.c1z")
	require.NoError(t, saveC1z(dbPath, c1zPath, 1))

	f, err := os.Open(c1zPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	// Flip the switch for the duration of this test.
	orig := fcsFailFastDisabled
	fcsFailFastDisabled = true
	defer func() { fcsFailFastDisabled = orig }()

	// Cap below declared size — with fail-fast disabled, init must succeed
	// and Read must decode until decodedBytes > cap.
	d, err := NewDecoder(f, WithDecoderMaxDecodedSize(dbSize/2))
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })

	// DeclaredDecodedSize still works — the kill-switch only gates the
	// pre-check, not the header peek.
	size, ok := d.DeclaredDecodedSize()
	require.True(t, ok)
	require.Equal(t, uint64(dbSize), size)

	// Streaming into io.Discard must eventually fail with the overrun error,
	// and the error must NOT contain the fail-fast "declared decoded size"
	// phrase (that only appears on the init-time pre-check).
	_, err = io.Copy(io.Discard, d)
	require.ErrorIs(t, err, ErrMaxSizeExceeded)
	require.NotContains(t, err.Error(), "declared decoded size",
		"kill-switch must route through the overrun path, not the pre-check")
}
