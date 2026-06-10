package v3

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v3pb "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

func TestEnvelopeRoundtrip(t *testing.T) {
	tmp := t.TempDir()
	srcDir := filepath.Join(tmp, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Populate src with a small synthetic Pebble-like directory.
	for name, content := range map[string]string{
		"CURRENT":         "MANIFEST-000001\n",
		"MANIFEST-000001": "manifest bytes",
		"000005.sst":      "sst contents — these would be real SSTs in production",
	} {
		path := filepath.Join(srcDir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	manifest := &c1zv3.C1ZManifestV3{
		Engine:              "pebble",
		EngineSchemaVersion: 17,
		PayloadEncoding:     c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD,
	}

	envFile := filepath.Join(tmp, "out.c1z3")
	f, err := os.Create(envFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteEnvelope(f, manifest, srcDir); err != nil {
		t.Fatalf("WriteEnvelope: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Read it back.
	rf, err := os.Open(envFile)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()
	env, err := ReadEnvelope(rf)
	if err != nil {
		t.Fatalf("ReadEnvelope: %v", err)
	}
	defer env.Close()

	if env.Manifest.GetEngine() != "pebble" {
		t.Errorf("engine: got %q want pebble", env.Manifest.GetEngine())
	}
	if env.Manifest.GetEngineSchemaVersion() != 17 {
		t.Errorf("engine_schema_version: got %d", env.Manifest.GetEngineSchemaVersion())
	}
	if env.Manifest.GetPayloadEncoding() != c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD {
		t.Errorf("payload_encoding: got %v", env.Manifest.GetPayloadEncoding())
	}

	// Extract payload and verify contents.
	dest := filepath.Join(tmp, "extracted")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ExtractZstdTar(env.PayloadReader, dest); err != nil {
		t.Fatalf("ExtractZstdTar: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "CURRENT"))
	if err != nil {
		t.Fatalf("read extracted CURRENT: %v", err)
	}
	if string(got) != "MANIFEST-000001\n" {
		t.Errorf("CURRENT roundtrip: got %q", got)
	}
}

func TestReadEnvelopeBadMagic(t *testing.T) {
	bad := bytes.NewReader([]byte("XXXX\x00trailing bytes"))
	_, err := ReadEnvelope(bad)
	if !errors.Is(err, ErrInvalidV3Magic) {
		t.Fatalf("expected ErrInvalidV3Magic, got %v", err)
	}
}

func TestReadEnvelopeTruncated(t *testing.T) {
	// Magic only, no length.
	_, err := ReadEnvelope(bytes.NewReader(C1Z3Magic))
	if !errors.Is(err, ErrEnvelopeTruncated) {
		t.Errorf("expected ErrEnvelopeTruncated, got %v", err)
	}
}

func TestMarshalManifestEmptyEngineFails(t *testing.T) {
	_, err := MarshalManifest(&c1zv3.C1ZManifestV3{})
	if !errors.Is(err, ErrManifestEmptyEngine) {
		t.Fatalf("expected ErrManifestEmptyEngine, got %v", err)
	}
}

func TestUnmarshalManifestEmptyEngineFails(t *testing.T) {
	// An empty byte sequence unmarshals to a manifest with zero
	// values; UnmarshalManifest must reject because engine is empty.
	_, err := UnmarshalManifest([]byte{})
	if !errors.Is(err, ErrManifestEmptyEngine) {
		t.Fatalf("expected ErrManifestEmptyEngine on empty bytes, got %v", err)
	}
}

func TestVerifyDescriptorClosureCatchesMissing(t *testing.T) {
	// Hand-craft an incomplete set: file A depends on B, but only A is present.
	a := &fileDescriptorProtoForTest{Name: "a.proto", Dependency: []string{"b.proto"}}
	set := newFileDescriptorSetForTest(a)
	if err := VerifyDescriptorClosure(set); !errors.Is(err, ErrManifestIncompleteDescriptors) {
		t.Fatalf("expected ErrManifestIncompleteDescriptors, got %v", err)
	}
	// Closing the closure makes it pass.
	b := &fileDescriptorProtoForTest{Name: "b.proto"}
	set = newFileDescriptorSetForTest(a, b)
	if err := VerifyDescriptorClosure(set); err != nil {
		t.Fatalf("expected closure-OK, got %v", err)
	}
}

func TestBuildDescriptorClosureIncludesStorageV3(t *testing.T) {
	// Touch the storage.v3 package so its descriptors are registered.
	_ = &v3pb.GrantRecord{}

	set, err := BuildDescriptorClosure()
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyDescriptorClosure(set); err != nil {
		t.Fatalf("closure built but verify failed: %v", err)
	}
	// Closure must include records.proto.
	want := "c1/storage/v3/records.proto"
	found := false
	for _, f := range set.File {
		if f.GetName() == want {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("closure missing %s; got %d files", want, len(set.File))
	}
}

// envelope_test helpers below — tiny stand-ins so the test file is self-contained.

func TestEnvelopePayloadAtEnd(t *testing.T) {
	// Verify the writer doesn't leave dangling bytes after the payload.
	// We do this by checking the file size matches header + length + manifest + payload.
	tmp := t.TempDir()
	srcDir := filepath.Join(tmp, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "f"), []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}

	envFile := filepath.Join(tmp, "out.c1z3")
	f, err := os.Create(envFile)
	if err != nil {
		t.Fatal(err)
	}
	m := &c1zv3.C1ZManifestV3{Engine: "pebble", PayloadEncoding: c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD}
	if err := WriteEnvelope(f, m, srcDir); err != nil {
		t.Fatal(err)
	}
	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	st, _ := os.Stat(envFile)
	if st.Size() < int64(len(C1Z3Magic)+4) {
		t.Errorf("envelope too small: %d bytes", st.Size())
	}

	// Read and verify payload streams to EOF cleanly.
	rf, _ := os.Open(envFile)
	defer rf.Close()
	env, err := ReadEnvelope(rf)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	_, err = io.Copy(io.Discard, env.PayloadReader)
	if err != nil {
		t.Fatalf("draining payload: %v", err)
	}
}

// TestReadEnvelopeDecodedSizeBudget verifies the payload reader fails
// with ErrMaxSizeExceeded once the decoded payload crosses the
// configured budget — the decompression-bomb guard for untrusted v3
// files.
func TestReadEnvelopeDecodedSizeBudget(t *testing.T) {
	t.Setenv("BATON_DECODER_MAX_DECODED_SIZE_MB", "1")

	tmp := t.TempDir()
	srcDir := filepath.Join(tmp, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// 2 MiB of zeros decodes past the 1 MiB budget but compresses tiny.
	if err := os.WriteFile(filepath.Join(srcDir, "big.sst"), make([]byte, 2<<20), 0o600); err != nil {
		t.Fatal(err)
	}

	envPath := filepath.Join(tmp, "out.c1z3")
	f, err := os.Create(envPath)
	if err != nil {
		t.Fatal(err)
	}
	m := &c1zv3.C1ZManifestV3{Engine: "pebble", PayloadEncoding: c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD}
	if err := WriteEnvelope(f, m, srcDir); err != nil {
		t.Fatalf("WriteEnvelope: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	rf, err := os.Open(envPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()
	env, err := ReadEnvelope(rf)
	if err != nil {
		t.Fatalf("ReadEnvelope: %v", err)
	}
	defer env.Close()

	err = ExtractZstdTar(env.PayloadReader, filepath.Join(tmp, "extracted"))
	if !errors.Is(err, ErrMaxSizeExceeded) {
		t.Fatalf("expected ErrMaxSizeExceeded, got %v", err)
	}
}

// TestExtractZstdTarStreamsLargeEntry covers the inline-streaming path
// for entries larger than inlineCopyThresholdBytes, which bypasses the
// in-memory worker-pool buffers.
func TestExtractZstdTarStreamsLargeEntry(t *testing.T) {
	tmp := t.TempDir()
	srcDir := filepath.Join(tmp, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	big := make([]byte, inlineCopyThresholdBytes+1)
	for i := range big {
		big[i] = byte(i % 251)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "big.sst"), big, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "small.sst"), []byte("small"), 0o600); err != nil {
		t.Fatal(err)
	}

	envPath := filepath.Join(tmp, "out.c1z3")
	f, err := os.Create(envPath)
	if err != nil {
		t.Fatal(err)
	}
	m := &c1zv3.C1ZManifestV3{Engine: "pebble", PayloadEncoding: c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD}
	if err := WriteEnvelope(f, m, srcDir); err != nil {
		t.Fatalf("WriteEnvelope: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	rf, err := os.Open(envPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()
	env, err := ReadEnvelope(rf)
	if err != nil {
		t.Fatalf("ReadEnvelope: %v", err)
	}
	defer env.Close()

	dest := filepath.Join(tmp, "extracted")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ExtractZstdTar(env.PayloadReader, dest); err != nil {
		t.Fatalf("ExtractZstdTar: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "big.sst"))
	if err != nil {
		t.Fatalf("read extracted big.sst: %v", err)
	}
	if !bytes.Equal(got, big) {
		t.Fatalf("big.sst roundtrip mismatch: got %d bytes", len(got))
	}
	small, err := os.ReadFile(filepath.Join(dest, "small.sst"))
	if err != nil {
		t.Fatalf("read extracted small.sst: %v", err)
	}
	if string(small) != "small" {
		t.Fatalf("small.sst roundtrip: got %q", small)
	}
}

// TestLimitedPayloadReaderExactLimit verifies a payload of exactly the
// budget succeeds and reaches EOF — only crossing the budget fails.
func TestLimitedPayloadReaderExactLimit(t *testing.T) {
	lr := &limitedPayloadReader{r: bytes.NewReader(make([]byte, 64)), limit: 64}
	got, err := io.ReadAll(lr)
	if err != nil {
		t.Fatalf("exact-limit payload should succeed, got %v", err)
	}
	if len(got) != 64 {
		t.Fatalf("expected 64 bytes, got %d", len(got))
	}

	lr = &limitedPayloadReader{r: bytes.NewReader(make([]byte, 65)), limit: 64}
	got, err = io.ReadAll(lr)
	if !errors.Is(err, ErrMaxSizeExceeded) {
		t.Fatalf("expected ErrMaxSizeExceeded, got %v", err)
	}
	if len(got) > 64 {
		t.Fatalf("reader leaked %d bytes past the budget", len(got)-64)
	}
}

func TestExtractZstdTarRejectsOversizedEntry(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{
		Name: "too-large.sst",
		Mode: 0o644,
		Size: maxTarEntryBytes + 1,
	}); err != nil {
		t.Fatal(err)
	}

	err := ExtractZstdTar(&buf, t.TempDir())
	if err == nil {
		t.Fatal("expected oversized tar entry error")
	}
}

func TestTarFileMode(t *testing.T) {
	tests := []struct {
		name    string
		mode    int64
		mask    os.FileMode
		want    os.FileMode
		wantErr bool
	}{
		{
			name: "regular file mode is masked",
			mode: 0o666,
			mask: 0o644,
			want: 0o644,
		},
		{
			name: "directory mode is masked",
			mode: 0o777,
			mask: 0o755,
			want: 0o755,
		},
		{
			name:    "negative mode is rejected",
			mode:    -1,
			mask:    0o644,
			wantErr: true,
		},
		{
			name:    "non-permission bits are rejected",
			mode:    0o1000,
			mask:    0o644,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tarFileMode(tt.mode, tt.mask)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("tarFileMode: %v", err)
			}
			if got != tt.want {
				t.Fatalf("tarFileMode(%#o, %#o) = %#o, want %#o", tt.mode, tt.mask, got, tt.want)
			}
		})
	}
}

// === S1 — selectable PayloadEncoding ===

// roundTripEnvelope writes a tiny synthetic payload dir under the
// given encoding then reads it back, returning the resulting
// PayloadEncoding from the manifest and the round-tripped CURRENT
// file contents.
func roundTripEnvelope(t *testing.T, enc c1zv3.PayloadEncoding) (c1zv3.PayloadEncoding, string) {
	t.Helper()
	tmp := t.TempDir()
	srcDir := filepath.Join(tmp, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"CURRENT":         "MANIFEST-000001\n",
		"MANIFEST-000001": "manifest bytes",
		"000005.sst":      "fake sst",
	} {
		if err := os.WriteFile(filepath.Join(srcDir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	manifest := &c1zv3.C1ZManifestV3{
		Engine:              "pebble",
		EngineSchemaVersion: 1,
		PayloadEncoding:     enc,
	}

	envPath := filepath.Join(tmp, "out.c1z3")
	f, err := os.Create(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteEnvelope(f, manifest, srcDir); err != nil {
		t.Fatalf("WriteEnvelope: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	rf, err := os.Open(envPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()
	env, err := ReadEnvelope(rf)
	if err != nil {
		t.Fatalf("ReadEnvelope: %v", err)
	}
	defer env.Close()

	dest := filepath.Join(tmp, "extracted")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ExtractZstdTar(env.PayloadReader, dest); err != nil {
		t.Fatalf("ExtractZstdTar: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "CURRENT"))
	if err != nil {
		t.Fatalf("read CURRENT: %v", err)
	}
	return env.Manifest.GetPayloadEncoding(), string(got)
}

// TestEnvelopeRoundtripTar exercises PAYLOAD_ENCODING_TAR — no outer
// zstd compression. The payload reader hands the tar bytes through
// directly; ExtractZstdTar handles them as plain tar.
func TestEnvelopeRoundtripTar(t *testing.T) {
	got, content := roundTripEnvelope(t, c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR)
	if got != c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR {
		t.Errorf("manifest payload_encoding: got %v, want TAR", got)
	}
	if content != "MANIFEST-000001\n" {
		t.Errorf("CURRENT roundtrip: got %q", content)
	}
}

// TestEnvelopeRoundtripTarZstd exercises the explicit TAR_ZSTD path.
func TestEnvelopeRoundtripTarZstd(t *testing.T) {
	got, content := roundTripEnvelope(t, c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD)
	if got != c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD {
		t.Errorf("manifest payload_encoding: got %v, want TAR_ZSTD", got)
	}
	if content != "MANIFEST-000001\n" {
		t.Errorf("CURRENT roundtrip: got %q", content)
	}
}

// TestEnvelopeUnspecifiedDefaultsToTarZstd verifies that a manifest
// with the zero-value PayloadEncoding gets upgraded to TAR_ZSTD by
// WriteEnvelope, and reads back with that value.
func TestEnvelopeUnspecifiedDefaultsToTarZstd(t *testing.T) {
	got, content := roundTripEnvelope(t, c1zv3.PayloadEncoding_PAYLOAD_ENCODING_UNSPECIFIED)
	if got != c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD {
		t.Errorf("manifest payload_encoding: got %v, want TAR_ZSTD (default)", got)
	}
	if content != "MANIFEST-000001\n" {
		t.Errorf("CURRENT roundtrip: got %q", content)
	}
}
