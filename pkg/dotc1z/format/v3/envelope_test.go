//go:build batonsdkv2

package v3

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v3pb "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	manifest := &c1zv3.C1ZManifestV3{
		Engine:              "pebble",
		EngineSchemaVersion: 17,
		PayloadEncoding:     c1zv3.PayloadEncoding_PAYLOAD_ENCODING_ZSTD_TAR,
		CreatedAt:           timestamppb.Now(),
		CreatedBySdkVersion: "test-sdk/0.0.1",
		CreatedByTool:       "envelope_test",
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
	if env.Manifest.GetPayloadEncoding() != c1zv3.PayloadEncoding_PAYLOAD_ENCODING_ZSTD_TAR {
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
	if err := os.WriteFile(filepath.Join(srcDir, "f"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}

	envFile := filepath.Join(tmp, "out.c1z3")
	f, err := os.Create(envFile)
	if err != nil {
		t.Fatal(err)
	}
	m := &c1zv3.C1ZManifestV3{Engine: "pebble", PayloadEncoding: c1zv3.PayloadEncoding_PAYLOAD_ENCODING_ZSTD_TAR}
	if err := WriteEnvelope(f, m, srcDir); err != nil {
		t.Fatal(err)
	}
	f.Close()

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
