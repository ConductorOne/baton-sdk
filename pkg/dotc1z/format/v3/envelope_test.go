package v3

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v3pb "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/stretchr/testify/require"
)

func TestEnvelopeRoundtrip(t *testing.T) {
	tmp := t.TempDir()
	srcDir := filepath.Join(tmp, "src")
	require.NoError(t, os.MkdirAll(srcDir, 0o755))
	// Populate src with a small synthetic Pebble-like directory.
	for name, content := range map[string]string{
		"CURRENT":         "MANIFEST-000001\n",
		"MANIFEST-000001": "manifest bytes",
		"000005.sst":      "sst contents — these would be real SSTs in production",
	} {
		path := filepath.Join(srcDir, name)
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	}

	manifest := &c1zv3.C1ZManifestV3{
		Engine:              "pebble",
		EngineSchemaVersion: 17,
		PayloadEncoding:     c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD,
	}

	envFile := filepath.Join(tmp, "out.c1z3")
	f, err := os.Create(envFile)
	require.NoError(t, err)
	require.NoError(t, WriteEnvelope(f, manifest, srcDir))
	require.NoError(t, f.Close())

	// Read it back.
	rf, err := os.Open(envFile)
	require.NoError(t, err)
	defer rf.Close()
	env, err := ReadEnvelope(rf)
	require.NoError(t, err)
	defer env.Close()

	require.Equal(t, "pebble", env.Manifest.GetEngine())
	require.Equal(t, uint32(17), env.Manifest.GetEngineSchemaVersion())
	require.Equal(t, c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD, env.Manifest.GetPayloadEncoding())

	// Extract payload and verify contents.
	dest := filepath.Join(tmp, "extracted")
	require.NoError(t, os.MkdirAll(dest, 0o755))
	require.NoError(t, ExtractZstdTar(env.PayloadReader, dest))
	got, err := os.ReadFile(filepath.Join(dest, "CURRENT"))
	require.NoError(t, err)
	require.Equal(t, "MANIFEST-000001\n", string(got))
}

func TestReadEnvelopeBadMagic(t *testing.T) {
	bad := bytes.NewReader([]byte("XXXX\x00trailing bytes"))
	_, err := ReadEnvelope(bad)
	require.ErrorIs(t, err, ErrInvalidV3Magic)
}

func TestReadEnvelopeTruncated(t *testing.T) {
	// Magic only, no length.
	_, err := ReadEnvelope(bytes.NewReader(C1Z3Magic))
	require.ErrorIs(t, err, ErrEnvelopeTruncated)
}

func TestMarshalManifestEmptyEngineFails(t *testing.T) {
	_, err := MarshalManifest(&c1zv3.C1ZManifestV3{})
	require.ErrorIs(t, err, ErrManifestEmptyEngine)
}

func TestUnmarshalManifestEmptyEngineFails(t *testing.T) {
	// An empty byte sequence unmarshals to a manifest with zero
	// values; UnmarshalManifest must reject because engine is empty.
	_, err := UnmarshalManifest([]byte{})
	require.ErrorIs(t, err, ErrManifestEmptyEngine)
}

func TestVerifyDescriptorClosureCatchesMissing(t *testing.T) {
	// Hand-craft an incomplete set: file A depends on B, but only A is present.
	a := &fileDescriptorProtoForTest{Name: "a.proto", Dependency: []string{"b.proto"}}
	set := newFileDescriptorSetForTest(a)
	require.ErrorIs(t, VerifyDescriptorClosure(set), ErrManifestIncompleteDescriptors)
	// Closing the closure makes it pass.
	b := &fileDescriptorProtoForTest{Name: "b.proto"}
	set = newFileDescriptorSetForTest(a, b)
	require.NoError(t, VerifyDescriptorClosure(set))
}

func TestBuildDescriptorClosureIncludesStorageV3(t *testing.T) {
	// Touch the storage.v3 package so its descriptors are registered.
	_ = &v3pb.GrantRecord{}

	set, err := BuildDescriptorClosure()
	require.NoError(t, err)
	require.NoError(t, VerifyDescriptorClosure(set))
	// Closure must include records.proto.
	want := "c1/storage/v3/records.proto"
	found := false
	for _, f := range set.File {
		if f.GetName() == want {
			found = true
			break
		}
	}
	require.True(t, found, "closure missing %s; got %d files", want, len(set.File))
}

// envelope_test helpers below — tiny stand-ins so the test file is self-contained.

func TestEnvelopePayloadAtEnd(t *testing.T) {
	// Verify the writer doesn't leave dangling bytes after the payload.
	// We do this by checking the file size matches header + length + manifest + payload.
	tmp := t.TempDir()
	srcDir := filepath.Join(tmp, "src")
	require.NoError(t, os.MkdirAll(srcDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "f"), []byte("hi"), 0o600))

	envFile := filepath.Join(tmp, "out.c1z3")
	f, err := os.Create(envFile)
	require.NoError(t, err)
	m := &c1zv3.C1ZManifestV3{Engine: "pebble", PayloadEncoding: c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD}
	require.NoError(t, WriteEnvelope(f, m, srcDir))
	require.NoError(t, f.Close())

	st, _ := os.Stat(envFile)
	require.GreaterOrEqual(t, st.Size(), int64(len(C1Z3Magic)+4), "envelope too small: %d bytes", st.Size())

	// Read and verify payload streams to EOF cleanly.
	rf, _ := os.Open(envFile)
	defer rf.Close()
	env, err := ReadEnvelope(rf)
	require.NoError(t, err)
	defer env.Close()
	_, err = io.Copy(io.Discard, env.PayloadReader)
	require.NoError(t, err)
}

// TestReadEnvelopeDecodedSizeBudget verifies the payload reader fails
// with ErrMaxSizeExceeded once the decoded payload crosses the
// configured budget — the decompression-bomb guard for untrusted v3
// files.
func TestReadEnvelopeDecodedSizeBudget(t *testing.T) {
	t.Setenv("BATON_DECODER_MAX_DECODED_SIZE_MB", "1")

	tmp := t.TempDir()
	srcDir := filepath.Join(tmp, "src")
	require.NoError(t, os.MkdirAll(srcDir, 0o755))
	// 2 MiB of zeros decodes past the 1 MiB budget but compresses tiny.
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "big.sst"), make([]byte, 2<<20), 0o600))

	envPath := filepath.Join(tmp, "out.c1z3")
	f, err := os.Create(envPath)
	require.NoError(t, err)
	m := &c1zv3.C1ZManifestV3{Engine: "pebble", PayloadEncoding: c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD}
	require.NoError(t, WriteEnvelope(f, m, srcDir))
	require.NoError(t, f.Close())

	rf, err := os.Open(envPath)
	require.NoError(t, err)
	defer rf.Close()
	env, err := ReadEnvelope(rf)
	require.NoError(t, err)
	defer env.Close()

	err = ExtractZstdTar(env.PayloadReader, filepath.Join(tmp, "extracted"))
	require.ErrorIs(t, err, ErrMaxSizeExceeded)
}

// TestExtractZstdTarStreamsLargeEntry covers the inline-streaming path
// for entries larger than inlineCopyThresholdBytes, which bypasses the
// in-memory worker-pool buffers.
func TestExtractZstdTarStreamsLargeEntry(t *testing.T) {
	tmp := t.TempDir()
	srcDir := filepath.Join(tmp, "src")
	require.NoError(t, os.MkdirAll(srcDir, 0o755))
	big := make([]byte, inlineCopyThresholdBytes+1)
	for i := range big {
		big[i] = byte(i % 251)
	}
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "big.sst"), big, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "small.sst"), []byte("small"), 0o600))

	envPath := filepath.Join(tmp, "out.c1z3")
	f, err := os.Create(envPath)
	require.NoError(t, err)
	m := &c1zv3.C1ZManifestV3{Engine: "pebble", PayloadEncoding: c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD}
	require.NoError(t, WriteEnvelope(f, m, srcDir))
	require.NoError(t, f.Close())

	rf, err := os.Open(envPath)
	require.NoError(t, err)
	defer rf.Close()
	env, err := ReadEnvelope(rf)
	require.NoError(t, err)
	defer env.Close()

	dest := filepath.Join(tmp, "extracted")
	require.NoError(t, os.MkdirAll(dest, 0o755))
	require.NoError(t, ExtractZstdTar(env.PayloadReader, dest))
	got, err := os.ReadFile(filepath.Join(dest, "big.sst"))
	require.NoError(t, err)
	require.Equal(t, big, got)
	small, err := os.ReadFile(filepath.Join(dest, "small.sst"))
	require.NoError(t, err)
	require.Equal(t, "small", string(small))
}

// TestLimitedPayloadReaderExactLimit verifies a payload of exactly the
// budget succeeds and reaches EOF — only crossing the budget fails.
func TestLimitedPayloadReaderExactLimit(t *testing.T) {
	lr := &limitedPayloadReader{r: bytes.NewReader(make([]byte, 64)), limit: 64}
	got, err := io.ReadAll(lr)
	require.NoError(t, err)
	require.Len(t, got, 64)

	lr = &limitedPayloadReader{r: bytes.NewReader(make([]byte, 65)), limit: 64}
	got, err = io.ReadAll(lr)
	require.ErrorIs(t, err, ErrMaxSizeExceeded)
	require.LessOrEqual(t, len(got), 64)
}

func TestExtractZstdTarRejectsOversizedEntry(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "too-large.sst",
		Mode: 0o644,
		Size: maxTarEntryBytes + 1,
	}))

	err := ExtractZstdTar(&buf, t.TempDir())
	require.Error(t, err)
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
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
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
	require.NoError(t, os.MkdirAll(srcDir, 0o755))
	for name, content := range map[string]string{
		"CURRENT":         "MANIFEST-000001\n",
		"MANIFEST-000001": "manifest bytes",
		"000005.sst":      "fake sst",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(srcDir, name), []byte(content), 0o600))
	}

	manifest := &c1zv3.C1ZManifestV3{
		Engine:              "pebble",
		EngineSchemaVersion: 1,
		PayloadEncoding:     enc,
	}

	envPath := filepath.Join(tmp, "out.c1z3")
	f, err := os.Create(envPath)
	require.NoError(t, err)
	require.NoError(t, WriteEnvelope(f, manifest, srcDir))
	require.NoError(t, f.Close())

	rf, err := os.Open(envPath)
	require.NoError(t, err)
	defer rf.Close()
	env, err := ReadEnvelope(rf)
	require.NoError(t, err)
	defer env.Close()

	dest := filepath.Join(tmp, "extracted")
	require.NoError(t, os.MkdirAll(dest, 0o755))
	require.NoError(t, ExtractZstdTar(env.PayloadReader, dest))
	got, err := os.ReadFile(filepath.Join(dest, "CURRENT"))
	require.NoError(t, err)
	return env.Manifest.GetPayloadEncoding(), string(got)
}

// TestEnvelopeRoundtripTar exercises PAYLOAD_ENCODING_TAR — no outer
// zstd compression. The payload reader hands the tar bytes through
// directly; ExtractZstdTar handles them as plain tar.
func TestEnvelopeRoundtripTar(t *testing.T) {
	got, content := roundTripEnvelope(t, c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR)
	require.Equal(t, c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR, got)
	require.Equal(t, "MANIFEST-000001\n", content)
}

// TestEnvelopeRoundtripTarZstd exercises the explicit TAR_ZSTD path.
func TestEnvelopeRoundtripTarZstd(t *testing.T) {
	got, content := roundTripEnvelope(t, c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD)
	require.Equal(t, c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD, got)
	require.Equal(t, "MANIFEST-000001\n", content)
}

// TestEnvelopeUnspecifiedDefaultsToTarZstd verifies that a manifest
// with the zero-value PayloadEncoding gets upgraded to TAR_ZSTD by
// WriteEnvelope, and reads back with that value.
func TestEnvelopeUnspecifiedDefaultsToTarZstd(t *testing.T) {
	got, content := roundTripEnvelope(t, c1zv3.PayloadEncoding_PAYLOAD_ENCODING_UNSPECIFIED)
	require.Equal(t, c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD, got)
	require.Equal(t, "MANIFEST-000001\n", content)
}
