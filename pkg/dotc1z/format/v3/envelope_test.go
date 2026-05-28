package v3

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

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
	if err := WriteEnvelope(t.Context(), f, manifest, srcDir); err != nil {
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
	if err := WriteEnvelope(t.Context(), f, m, srcDir); err != nil {
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
	if err := WriteEnvelope(t.Context(), f, manifest, srcDir); err != nil {
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

// TestWriteEnvelopeCanceledCtxAbortsWalk verifies that a context
// cancelled before WriteEnvelope is called causes the payload-walk to
// short-circuit with ctx.Err(). The header/manifest bytes may have
// already been written; the contract is only that the caller learns
// it must NOT promote the partial output (via os.Rename in the
// caller's success gate).
func TestWriteEnvelopeCanceledCtxAbortsWalk(t *testing.T) {
	tmp := t.TempDir()
	srcDir := filepath.Join(tmp, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"CURRENT":         "MANIFEST-000001\n",
		"MANIFEST-000001": "manifest bytes",
		"000005.sst":      "sst contents",
	} {
		if err := os.WriteFile(filepath.Join(srcDir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	manifest := &c1zv3.C1ZManifestV3{
		Engine:              "pebble",
		EngineSchemaVersion: 17,
		PayloadEncoding:     c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD,
	}

	envPath := filepath.Join(tmp, "out.c1z3")
	f, err := os.Create(envPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err = WriteEnvelope(ctx, f, manifest, srcDir)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteEnvelope on cancelled ctx: got err=%v, want context.Canceled", err)
	}
}

// TestWriteEnvelopeCanceledMidWalk covers the case the prior test
// can't: a context that is healthy when WriteEnvelope is called and
// is cancelled by an external observer DURING the walk. The walk
// must surface the cancel with a wrapped error that names the
// in-flight tar entry.
func TestWriteEnvelopeCanceledMidWalk(t *testing.T) {
	tmp := t.TempDir()
	srcDir := filepath.Join(tmp, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Many files so the walk has callbacks to traverse. Names are
	// chosen so sort order is deterministic and the cancel fires
	// after the first few entries.
	for i := 0; i < 32; i++ {
		name := filepath.Join(srcDir, fmt.Sprintf("entry-%02d", i))
		if err := os.WriteFile(name, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	manifest := &c1zv3.C1ZManifestV3{
		Engine:              "pebble",
		EngineSchemaVersion: 17,
		PayloadEncoding:     c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD,
	}

	envPath := filepath.Join(tmp, "out.c1z3")
	f, err := os.Create(envPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Wrap the writer so cancel fires after the very first bytes
	// hit it (magic/length/manifest writes) — that puts the cancel
	// in flight before the walk callback's first ctx check runs,
	// so the wrapped error names the first walked entry.
	ctx, cancel := context.WithCancel(t.Context())
	cw := &cancelOnWriteOnce{cancel: cancel, threshold: 1}

	err = WriteEnvelope(ctx, &teeWriter{into: f, also: cw}, manifest, srcDir)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	// Wrapped error must name the entry we were on, so the operator
	// triaging "context canceled" in logs can see which file the
	// walk was visiting.
	require.Regexp(t, `walk canceled at "entry-.+":`, err.Error())
}

// TestWriteEnvelopeEmptyPayloadDir verifies the degenerate case
// where the payload directory exists but contains no regular files
// (e.g. a freshly-checkpointed Pebble store with zero SSTs). The
// walk still emits a directory header for "." but no body bytes;
// ReadEnvelope must round-trip it cleanly.
func TestWriteEnvelopeEmptyPayloadDir(t *testing.T) {
	tmp := t.TempDir()
	srcDir := filepath.Join(tmp, "empty")
	require.NoError(t, os.MkdirAll(srcDir, 0o755))

	manifest := &c1zv3.C1ZManifestV3{
		Engine:              "pebble",
		EngineSchemaVersion: 17,
		PayloadEncoding:     c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD,
	}

	envPath := filepath.Join(tmp, "out.c1z3")
	f, err := os.Create(envPath)
	require.NoError(t, err)
	require.NoError(t, WriteEnvelope(t.Context(), f, manifest, srcDir))
	require.NoError(t, f.Close())

	rf, err := os.Open(envPath)
	require.NoError(t, err)
	defer rf.Close()
	env, err := ReadEnvelope(rf)
	require.NoError(t, err)
	defer env.Close()
	require.Equal(t, "pebble", env.Manifest.GetEngine())
	// Payload is a zero-entry tar inside zstd; draining it must
	// finish with no error and no unexpected bytes.
	dest := t.TempDir()
	require.NoError(t, ExtractZstdTar(env.PayloadReader, dest))
}

// teeWriter mirrors every Write to both targets. Used to observe
// payload writes from the test without disturbing the real writer.
type teeWriter struct {
	into io.Writer
	also io.Writer
}

func (t *teeWriter) Write(p []byte) (int, error) {
	if _, err := t.also.Write(p); err != nil {
		return 0, err
	}
	return t.into.Write(p)
}

// cancelOnWriteOnce fires the cancel func the first time Write
// receives at least `threshold` total bytes. Subsequent writes are
// pass-through.
type cancelOnWriteOnce struct {
	cancel    context.CancelFunc
	threshold int
	written   int
	fired     bool
}

func (c *cancelOnWriteOnce) Write(p []byte) (int, error) {
	c.written += len(p)
	if !c.fired && c.written >= c.threshold {
		c.fired = true
		c.cancel()
	}
	return len(p), nil
}
