package v3

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	"github.com/klauspost/compress/zstd"
)

// buildPoolTestEnvelope writes a tiny TAR_ZSTD envelope and returns its
// bytes.
func buildPoolTestEnvelope(t *testing.T) []byte {
	t.Helper()
	srcDir := filepath.Join(t.TempDir(), "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "CURRENT"), []byte("MANIFEST-000001\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	m := &c1zv3.C1ZManifestV3{
		Engine:          "pebble",
		PayloadEncoding: c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD,
	}
	var buf bytes.Buffer
	if err := WriteEnvelope(&buf, m, srcDir); err != nil {
		t.Fatalf("WriteEnvelope: %v", err)
	}
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
	if err != nil {
		t.Fatalf("ReadEnvelopeHeaderWithPool: %v", err)
	}
	if err := ExtractZstdTar(env1.PayloadReader, t.TempDir()); err != nil {
		t.Fatalf("ExtractZstdTar: %v", err)
	}
	if err := env1.Close(); err != nil {
		t.Fatal(err)
	}
	if got := len(pool.idle); got != 1 {
		t.Fatalf("idle decoders after first close = %d, want 1", got)
	}

	env2, err := ReadEnvelopeHeaderWithPool(bytes.NewReader(data), pool)
	if err != nil {
		t.Fatalf("ReadEnvelopeHeaderWithPool (reuse): %v", err)
	}
	if got := len(pool.idle); got != 0 {
		t.Fatalf("idle decoders during second read = %d, want 0 (reuse expected)", got)
	}
	if err := ExtractZstdTar(env2.PayloadReader, t.TempDir()); err != nil {
		t.Fatalf("ExtractZstdTar (reuse): %v", err)
	}
	if err := env2.Close(); err != nil {
		t.Fatal(err)
	}
	if got := len(pool.idle); got != 1 {
		t.Fatalf("idle decoders after second close = %d, want 1", got)
	}

	pool.Close()
	if got := len(pool.idle); got != 0 {
		t.Fatalf("idle decoders after pool close = %d, want 0", got)
	}

	// An envelope closed after the pool destroys its decoder instead
	// of re-pooling it.
	env3, err := ReadEnvelopeHeaderWithPool(bytes.NewReader(data), pool)
	if err != nil {
		t.Fatalf("ReadEnvelopeHeaderWithPool (after pool close): %v", err)
	}
	if err := env3.Close(); err != nil {
		t.Fatal(err)
	}
	if got := len(pool.idle); got != 0 {
		t.Fatalf("idle decoders after late close = %d, want 0 (pool is closed)", got)
	}

	// Nil pool is valid everywhere.
	var nilPool *DecoderPool
	env4, err := ReadEnvelopeHeaderWithPool(bytes.NewReader(data), nilPool)
	if err != nil {
		t.Fatalf("ReadEnvelopeHeaderWithPool (nil pool): %v", err)
	}
	if err := env4.Close(); err != nil {
		t.Fatal(err)
	}
	nilPool.Close()
}

// writePoolTestEnvelopeFile writes the tiny TAR_ZSTD envelope to disk;
// ExtractEnvelopePayload requires an *os.File.
func writePoolTestEnvelopeFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pool.c1z")
	if err := os.WriteFile(path, buildPoolTestEnvelope(t), 0o600); err != nil {
		t.Fatal(err)
	}
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
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = ExtractEnvelopePayload(f, t.TempDir(), WithPayloadDecoderPool(pool))
		_ = f.Close()
		if err != nil {
			t.Fatalf("ExtractEnvelopePayload (open %d): %v", i, err)
		}
		// Exactly one idle decoder after each extraction: the first
		// open constructs and parks it; the second drains and re-parks
		// the same one. Two idle decoders would mean the pool was
		// bypassed on get and only used on put.
		if got := len(pool.idle); got != 1 {
			t.Fatalf("idle decoders after open %d = %d, want 1", i, got)
		}
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
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, _, err = ExtractEnvelopePayload(f, t.TempDir(),
		WithPayloadDecoderPool(pool),
		WithMaxDecoderMemoryBytes(1),
	)
	// Depending on the frame shape, the 1-byte cap surfaces as either the
	// window-size or decoded-size violation; both prove the custom cap
	// reached the decoder.
	if !errors.Is(err, zstd.ErrWindowSizeExceeded) && !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		t.Fatalf("ExtractEnvelopePayload error = %v, want a zstd memory-cap violation (custom cap must be honored)", err)
	}
	if got := len(pool.idle); got != 0 {
		t.Fatalf("idle decoders = %d, want 0 (custom-cap decoder must not be pooled)", got)
	}
}
