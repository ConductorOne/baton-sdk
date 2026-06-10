package v3

import (
	"bytes"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	"github.com/klauspost/compress/zstd"
)

func writeTestPayloadDir(t *testing.T, files map[string][]byte) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		target := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func indexedManifest() *c1zv3.C1ZManifestV3 {
	return c1zv3.C1ZManifestV3_builder{
		Engine:          "pebble",
		PayloadEncoding: c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD,
	}.Build()
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// TestIndexedRoundTrip writes an indexed envelope and extracts it,
// verifying contents (including a nested path and an empty file) and
// that the reuse table is populated.
func TestIndexedRoundTrip(t *testing.T) {
	files := map[string][]byte{
		"000001.sst":      randomBytes(t, 256<<10),
		"000002.sst":      randomBytes(t, 64),
		"MANIFEST-000001": []byte("manifest contents"),
		"empty.log":       {},
		"nested/OPTIONS":  []byte("opts"),
	}
	dir := writeTestPayloadDir(t, files)

	envPath := filepath.Join(t.TempDir(), "indexed.c1z")
	out, err := os.Create(envPath)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := WriteEnvelopeWithReuse(out, indexedManifest(), dir, nil)
	if err != nil {
		t.Fatalf("WriteEnvelopeWithReuse: %v", err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	if stats.EncodedFrames != len(files) || stats.SplicedFrames != 0 {
		t.Fatalf("stats = %+v, want %d encoded / 0 spliced", stats, len(files))
	}

	f, err := os.Open(envPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dest := t.TempDir()
	m, reuse, err := ExtractEnvelopePayload(f, dest)
	if err != nil {
		t.Fatalf("ExtractEnvelopePayload: %v", err)
	}
	if m.GetPayloadEncoding() != c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD {
		t.Fatalf("encoding = %v", m.GetPayloadEncoding())
	}
	if reuse == nil || len(reuse.byName) != len(files) {
		t.Fatalf("reuse table missing or wrong size: %v", reuse)
	}
	for name, want := range files {
		got, err := os.ReadFile(filepath.Join(dest, filepath.FromSlash(name)))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: content mismatch (%d vs %d bytes)", name, len(got), len(want))
		}
	}
}

func TestIndexedRespectsMaxDecodedPayloadLimit(t *testing.T) {
	files := map[string][]byte{
		"a.sst": randomBytes(t, 128),
		"b.sst": randomBytes(t, 128),
	}
	dir := writeTestPayloadDir(t, files)
	envPath := filepath.Join(t.TempDir(), "indexed.c1z")
	out, err := os.Create(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteEnvelopeWithReuse(out, indexedManifest(), dir, nil); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(envPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, _, err = ExtractEnvelopePayload(f, t.TempDir(), WithMaxDecodedPayloadBytes(255))
	if !errors.Is(err, ErrMaxSizeExceeded) {
		t.Fatalf("ExtractEnvelopePayload error = %v, want ErrMaxSizeExceeded", err)
	}
}

func TestIndexedDecodedPayloadFailFastKillSwitch(t *testing.T) {
	files := map[string][]byte{
		"a.sst": randomBytes(t, 128),
		"b.sst": randomBytes(t, 128),
	}
	dir := writeTestPayloadDir(t, files)
	envPath := filepath.Join(t.TempDir(), "indexed.c1z")
	out, err := os.Create(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteEnvelopeWithReuse(out, indexedManifest(), dir, nil); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}

	orig := fcsFailFastDisabled
	fcsFailFastDisabled = true
	defer func() { fcsFailFastDisabled = orig }()

	f, err := os.Open(envPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, _, err = ExtractEnvelopePayload(f, t.TempDir(), WithMaxDecodedPayloadBytes(255))
	if !errors.Is(err, ErrMaxSizeExceeded) {
		t.Fatalf("ExtractEnvelopePayload error = %v, want ErrMaxSizeExceeded", err)
	}
	if err != nil && bytes.Contains([]byte(err.Error()), []byte("indexed payload exceeds")) {
		t.Fatalf("kill switch should avoid header-size fail-fast error, got %v", err)
	}
}

func TestIndexedRespectsMaxDecodedPayloadEnv(t *testing.T) {
	t.Setenv(maxDecodedSizeEnvVar, "1")
	files := map[string][]byte{
		"too-big.sst": randomBytes(t, 1<<20+1),
	}
	dir := writeTestPayloadDir(t, files)
	envPath := filepath.Join(t.TempDir(), "indexed.c1z")
	out, err := os.Create(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteEnvelopeWithReuse(out, indexedManifest(), dir, nil); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(envPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, _, err = ExtractEnvelopePayload(f, t.TempDir())
	if !errors.Is(err, ErrMaxSizeExceeded) {
		t.Fatalf("ExtractEnvelopePayload error = %v, want ErrMaxSizeExceeded", err)
	}
}

func TestIndexedRespectsMaxDecoderMemory(t *testing.T) {
	files := map[string][]byte{
		"needs-window.sst": bytes.Repeat([]byte("0123456789abcdef"), 256<<10),
	}
	dir := writeTestPayloadDir(t, files)
	envPath := filepath.Join(t.TempDir(), "indexed.c1z")
	out, err := os.Create(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteEnvelopeWithReuse(out, indexedManifest(), dir, nil); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(envPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, _, err = ExtractEnvelopePayload(f, t.TempDir(), WithMaxDecoderMemoryBytes(1))
	if !errors.Is(err, zstd.ErrWindowSizeExceeded) {
		t.Fatalf("ExtractEnvelopePayload error = %v, want zstd.ErrWindowSizeExceeded", err)
	}
}

// TestIndexedSpliceReuse models the fold save: extract an envelope,
// hard-link most files into a "checkpoint" dir (what pebble's
// checkpoint does for immutable SSTs), change one and add one, then
// rewrite with reuse. Unchanged files must splice; changed/new files
// must encode; the result must extract byte-identically.
func TestIndexedSpliceReuse(t *testing.T) {
	files := map[string][]byte{
		"000001.sst":      randomBytes(t, 128<<10),
		"000002.sst":      randomBytes(t, 96<<10),
		"000003.sst":      randomBytes(t, 32<<10),
		"MANIFEST-000001": []byte("old manifest"),
	}
	dir := writeTestPayloadDir(t, files)
	srcPath := filepath.Join(t.TempDir(), "src.c1z")
	out, err := os.Create(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteEnvelopeWithReuse(out, indexedManifest(), dir, nil); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	extracted := t.TempDir()
	_, reuse, err := ExtractEnvelopePayload(f, extracted)
	if err != nil {
		t.Fatal(err)
	}

	// Build the "checkpoint": hard links for the three SSTs, a
	// rewritten MANIFEST, and a brand-new SST.
	checkpoint := t.TempDir()
	for _, name := range []string{"000001.sst", "000002.sst", "000003.sst"} {
		if err := os.Link(filepath.Join(extracted, name), filepath.Join(checkpoint, name)); err != nil {
			t.Skipf("hard links unavailable on this filesystem: %v", err)
		}
	}
	newManifest := []byte("NEW manifest, longer than before")
	if err := os.WriteFile(filepath.Join(checkpoint, "MANIFEST-000001"), newManifest, 0o600); err != nil {
		t.Fatal(err)
	}
	newSST := randomBytes(t, 16<<10)
	if err := os.WriteFile(filepath.Join(checkpoint, "000004.sst"), newSST, 0o600); err != nil {
		t.Fatal(err)
	}

	dstPath := filepath.Join(t.TempDir(), "dst.c1z")
	dst, err := os.Create(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := WriteEnvelopeWithReuse(dst, indexedManifest(), checkpoint, reuse)
	if err != nil {
		t.Fatalf("WriteEnvelopeWithReuse: %v", err)
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
	if stats.SplicedFrames != 3 {
		t.Fatalf("spliced frames = %d, want 3 (the hard-linked SSTs)", stats.SplicedFrames)
	}
	if stats.EncodedFrames != 2 {
		t.Fatalf("encoded frames = %d, want 2 (new manifest + new sst)", stats.EncodedFrames)
	}

	df, err := os.Open(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	dest := t.TempDir()
	if _, _, err := ExtractEnvelopePayload(df, dest); err != nil {
		t.Fatalf("extract spliced envelope: %v", err)
	}
	expect := map[string][]byte{
		"000001.sst":      files["000001.sst"],
		"000002.sst":      files["000002.sst"],
		"000003.sst":      files["000003.sst"],
		"MANIFEST-000001": newManifest,
		"000004.sst":      newSST,
	}
	for name, want := range expect {
		got, err := os.ReadFile(filepath.Join(dest, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: content mismatch after splice", name)
		}
	}
}

// TestIndexedSpliceHashFallback covers the no-hard-link path: the
// checkpoint file is a fresh copy (different inode), so SameFile
// fails and the hash fallback must still splice it.
func TestIndexedSpliceHashFallback(t *testing.T) {
	content := randomBytes(t, 64<<10)
	dir := writeTestPayloadDir(t, map[string][]byte{"000001.sst": content})
	srcPath := filepath.Join(t.TempDir(), "src.c1z")
	out, err := os.Create(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteEnvelopeWithReuse(out, indexedManifest(), dir, nil); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, _, err := ExtractEnvelopePayload(f, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	// Re-extract for the reuse table, then copy (not link) into the
	// checkpoint dir so inodes differ.
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	_, reuse, err := ExtractEnvelopePayload(f, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := t.TempDir()
	if err := os.WriteFile(filepath.Join(checkpoint, "000001.sst"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	dst, err := os.Create(filepath.Join(t.TempDir(), "dst.c1z"))
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	stats, err := WriteEnvelopeWithReuse(dst, indexedManifest(), checkpoint, reuse)
	if err != nil {
		t.Fatal(err)
	}
	if stats.SplicedFrames != 1 || stats.EncodedFrames != 0 {
		t.Fatalf("stats = %+v, want 1 spliced / 0 encoded via hash fallback", stats)
	}
}

// TestIndexedRequiresSeekableWriter locks in the WriteSeeker
// requirement for the indexed encoding.
func TestIndexedRequiresSeekableWriter(t *testing.T) {
	var buf bytes.Buffer
	_, err := WriteEnvelopeWithReuse(&buf, indexedManifest(), t.TempDir(), nil)
	if err == nil {
		t.Fatal("expected error for non-seekable writer")
	}
}
