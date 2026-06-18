package v3

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	"github.com/klauspost/compress/zstd"
)

// buildPoolTestEnvelope writes a tiny TAR_ZSTD envelope and returns its
// bytes.
func buildPoolTestEnvelope(t *testing.T) []byte {
	t.Helper()
	srcDir := filepath.Join(t.TempDir(), "src")
	require.NoError(t, os.MkdirAll(srcDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "CURRENT"), []byte("MANIFEST-000001\n"), 0o600))
	m := &c1zv3.C1ZManifestV3{
		Engine:          "pebble",
		PayloadEncoding: c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD,
	}
	var buf bytes.Buffer
	require.NoError(t, WriteEnvelope(&buf, m, srcDir), "WriteEnvelope")
	return buf.Bytes()
}

// TestDecoderPoolScopedReuse locks in the pool's lifecycle contract:
// an envelope returns its decoder to the pool at Close, the next read
// drains the idle decoder instead of constructing one, and Close
// releases everything — decoders returned after Close are destroyed,
// never re-pooled.
func TestDecoderPoolScopedReuse(t *testing.T) {
	data := buildPoolTestEnvelope(t)
	pool := NewDecoderPool()

	env1, err := ReadEnvelopeHeaderWithPool(bytes.NewReader(data), pool)
	require.NoError(t, err, "ReadEnvelopeHeaderWithPool")
	require.NoError(t, ExtractZstdTar(env1.PayloadReader, t.TempDir()), "ExtractZstdTar")
	require.NoError(t, env1.Close())
	require.Len(t, pool.idle, 1, "idle decoders after first close")

	env2, err := ReadEnvelopeHeaderWithPool(bytes.NewReader(data), pool)
	require.NoError(t, err, "ReadEnvelopeHeaderWithPool (reuse)")
	require.Empty(t, pool.idle, "idle decoders during second read (reuse expected)")
	require.NoError(t, ExtractZstdTar(env2.PayloadReader, t.TempDir()), "ExtractZstdTar (reuse)")
	require.NoError(t, env2.Close())
	require.Len(t, pool.idle, 1, "idle decoders after second close")

	pool.Close()
	require.Empty(t, pool.idle, "idle decoders after pool close")

	// An envelope closed after the pool destroys its decoder instead
	// of re-pooling it.
	env3, err := ReadEnvelopeHeaderWithPool(bytes.NewReader(data), pool)
	require.NoError(t, err, "ReadEnvelopeHeaderWithPool (after pool close)")
	require.NoError(t, env3.Close())
	require.Empty(t, pool.idle, "idle decoders after late close (pool is closed)")

	// Nil pool is valid everywhere.
	var nilPool *DecoderPool
	env4, err := ReadEnvelopeHeaderWithPool(bytes.NewReader(data), nilPool)
	require.NoError(t, err, "ReadEnvelopeHeaderWithPool (nil pool)")
	require.NoError(t, env4.Close())
	nilPool.Close()
}

// writePoolTestEnvelopeFile writes the tiny TAR_ZSTD envelope to disk;
// ExtractEnvelopePayload requires an *os.File.
func writePoolTestEnvelopeFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pool.c1z")
	require.NoError(t, os.WriteFile(path, buildPoolTestEnvelope(t), 0o600))
	return path
}

// TestExtractEnvelopePayloadDecoderPoolReuse pins that the Pebble open
// path's extraction (ExtractEnvelopePayload) draws its tar_zstd
// streaming decoder from a caller-scoped pool and returns it: one
// decoder is constructed for the first open and reused (not duplicated)
// by the second.
func TestExtractEnvelopePayloadDecoderPoolReuse(t *testing.T) {
	path := writePoolTestEnvelopeFile(t)
	pool := NewDecoderPool()
	defer pool.Close()

	for i := 0; i < 2; i++ {
		f, err := os.Open(path)
		require.NoError(t, err)
		_, _, err = ExtractEnvelopePayload(f, t.TempDir(), WithPayloadDecoderPool(pool))
		_ = f.Close()
		require.NoError(t, err, "ExtractEnvelopePayload (open %d)", i)
		// Exactly one idle decoder after each extraction: the first
		// open constructs and parks it; the second drains and re-parks
		// the same one. Two idle decoders would mean the pool was
		// bypassed on get and only used on put.
		require.Len(t, pool.idle, 1, "idle decoders after open %d", i)
	}
}

// TestExtractEnvelopePayloadCustomMemoryCapBypassesPool pins the
// pool-compatibility gate: a caller-specific decoder memory cap must be
// honored, which means constructing a fresh decoder instead of using a
// pooled one (whose cap is baked in at construction). The tiny cap also
// proves the custom cap took effect.
func TestExtractEnvelopePayloadCustomMemoryCapBypassesPool(t *testing.T) {
	path := writePoolTestEnvelopeFile(t)
	pool := NewDecoderPool()
	defer pool.Close()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	_, _, err = ExtractEnvelopePayload(f, t.TempDir(),
		WithPayloadDecoderPool(pool),
		WithMaxDecoderMemoryBytes(1),
	)
	// Depending on the frame shape, the 1-byte cap surfaces as either the
	// window-size or decoded-size violation; both prove the custom cap
	// reached the decoder.
	require.True(t,
		errors.Is(err, zstd.ErrWindowSizeExceeded) || errors.Is(err, zstd.ErrDecoderSizeExceeded),
		"ExtractEnvelopePayload error = %v, want a zstd memory-cap violation (custom cap must be honored)", err,
	)
	require.Empty(t, pool.idle, "idle decoders (custom-cap decoder must not be pooled)")
}
