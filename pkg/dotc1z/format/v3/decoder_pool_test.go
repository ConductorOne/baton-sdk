package v3

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
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
