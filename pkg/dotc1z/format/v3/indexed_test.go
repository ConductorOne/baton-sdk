package v3

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
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

// writeIndexedEnvelope writes an indexed envelope for dir into a temp
// file and returns its path.
func writeIndexedEnvelope(t *testing.T, dir string) string {
	t.Helper()
	envPath := filepath.Join(t.TempDir(), "indexed.c1z")
	out, err := os.Create(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteEnvelopeWithReuse(out, indexedManifest(), dir, nil); err != nil {
		t.Fatalf("WriteEnvelopeWithReuse: %v", err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	return envPath
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

// writerOnly hides every method except Write, modeling a non-seekable
// network sink.
type writerOnly struct{ io.Writer }

// TestIndexedStreamingWrite locks in the single-pass property: the
// indexed encoding must accept a non-seekable writer (nothing is
// backpatched), and the result must extract byte-identically. This is
// the "encode straight to an upload" path.
func TestIndexedStreamingWrite(t *testing.T) {
	files := map[string][]byte{
		"000001.sst": randomBytes(t, 64<<10),
		"000002.sst": randomBytes(t, 8<<10),
	}
	dir := writeTestPayloadDir(t, files)

	envPath := filepath.Join(t.TempDir(), "streamed.c1z")
	out, err := os.Create(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteEnvelopeWithReuse(writerOnly{out}, indexedManifest(), dir, nil); err != nil {
		t.Fatalf("WriteEnvelopeWithReuse via non-seekable writer: %v", err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(envPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dest := t.TempDir()
	if _, _, err := ExtractEnvelopePayload(f, dest); err != nil {
		t.Fatalf("ExtractEnvelopePayload: %v", err)
	}
	for name, want := range files {
		got, err := os.ReadFile(filepath.Join(dest, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: content mismatch", name)
		}
	}
}

// TestIndexedFrameIndex exercises the trailer as the chunk-store
// planning primitive: ReadIndexedFrameIndex must return every frame's
// name, byte range, sizes, hashes, and payload totals — and each byte
// range must be independently decodable as a bare zstd stream with
// Frame_Content_Size advertised (the standalone-object property).
func TestIndexedFrameIndex(t *testing.T) {
	files := map[string][]byte{
		"000001.sst":      randomBytes(t, 128<<10),
		"000002.sst":      randomBytes(t, 4<<10),
		"MANIFEST-000001": []byte("manifest contents"),
		"empty.log":       {},
	}
	dir := writeTestPayloadDir(t, files)
	envPath := writeIndexedEnvelope(t, dir)

	f, err := os.Open(envPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	idx, err := ReadIndexedFrameIndex(f)
	if err != nil {
		t.Fatalf("ReadIndexedFrameIndex: %v", err)
	}
	if len(idx.GetEntries()) != len(files) {
		t.Fatalf("index has %d entries, want %d", len(idx.GetEntries()), len(files))
	}

	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()

	var totalRaw, totalComp int64
	for _, e := range idx.GetEntries() {
		want, ok := files[e.GetName()]
		if !ok {
			t.Fatalf("index names unknown frame %q", e.GetName())
		}
		if e.GetRawSize() != int64(len(want)) {
			t.Fatalf("%s: raw size %d, want %d", e.GetName(), e.GetRawSize(), len(want))
		}
		wantSha := sha256.Sum256(want)
		if !bytes.Equal(e.GetRawSha256(), wantSha[:]) {
			t.Fatalf("%s: sha256 mismatch", e.GetName())
		}

		// The byte range alone must be a complete, standalone zstd
		// stream — exactly what a chunk store preads or PUTs. Even
		// zero-length files must produce a real frame (WithZeroFrames),
		// or their chunk objects would be undecodable.
		if e.GetCompressedSize() <= 0 {
			t.Fatalf("%s: frame is empty (compressed size %d)", e.GetName(), e.GetCompressedSize())
		}
		frame := make([]byte, e.GetCompressedSize())
		if _, err := f.ReadAt(frame, e.GetFrameOffset()); err != nil {
			t.Fatalf("%s: pread frame: %v", e.GetName(), err)
		}
		// FCS is advertised for frames >= 256 bytes; the zstd frame
		// header cannot represent smaller sizes without the
		// single-segment flag, which the encoder only sets for
		// mid-sized payloads. raw_size in the index is authoritative
		// either way.
		if len(want) >= 256 {
			var hdr zstd.Header
			if err := hdr.Decode(frame); err != nil {
				t.Fatalf("%s: decode frame header: %v", e.GetName(), err)
			}
			if !hdr.HasFCS || hdr.FrameContentSize != uint64(len(want)) {
				t.Fatalf("%s: FCS missing or wrong (has=%v fcs=%d want=%d)", e.GetName(), hdr.HasFCS, hdr.FrameContentSize, len(want))
			}
		}
		if err := dec.Reset(bytes.NewReader(frame)); err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(dec)
		if err != nil {
			t.Fatalf("%s: standalone decode: %v", e.GetName(), err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: standalone decode mismatch (%d vs %d bytes)", e.GetName(), len(got), len(want))
		}
		totalRaw += e.GetRawSize()
		totalComp += e.GetCompressedSize()
	}
	if idx.GetTotalRawSize() != totalRaw {
		t.Fatalf("total raw = %d, entries sum to %d", idx.GetTotalRawSize(), totalRaw)
	}
	if idx.GetTotalCompressedSize() != totalComp {
		t.Fatalf("total compressed = %d, entries sum to %d", idx.GetTotalCompressedSize(), totalComp)
	}
}

// TestIndexedTrailerCorruption covers the corruption-class failure
// modes: a truncated footer, a flipped index byte (caught by the
// footer's xxh64 of the index), and a flipped frame byte (caught at
// extract).
func TestIndexedTrailerCorruption(t *testing.T) {
	content := randomBytes(t, 32<<10)
	newEnv := func(t *testing.T) string {
		dir := writeTestPayloadDir(t, map[string][]byte{"000001.sst": content})
		return writeIndexedEnvelope(t, dir)
	}
	flipByteAt := func(t *testing.T, path string, off int64) {
		t.Helper()
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		var b [1]byte
		if _, err := f.ReadAt(b[:], off); err != nil {
			t.Fatal(err)
		}
		b[0] ^= 0xFF
		if _, err := f.WriteAt(b[:], off); err != nil {
			t.Fatal(err)
		}
	}
	extract := func(t *testing.T, path string) error {
		t.Helper()
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		_, _, err = ExtractEnvelopePayload(f, t.TempDir())
		return err
	}

	t.Run("truncated footer", func(t *testing.T) {
		p := newEnv(t)
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Truncate(p, st.Size()-1); err != nil {
			t.Fatal(err)
		}
		if err := extract(t, p); !errors.Is(err, ErrEnvelopeTruncated) {
			t.Fatalf("error = %v, want ErrEnvelopeTruncated", err)
		}
	})

	t.Run("corrupt index byte", func(t *testing.T) {
		p := newEnv(t)
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		var footer [indexedFooterLen]byte
		if _, err := f.ReadAt(footer[:], st.Size()-indexedFooterLen); err != nil {
			t.Fatal(err)
		}
		f.Close()
		indexOff := int64(binary.BigEndian.Uint64(footer[0:8])) //nolint:gosec // test reads back the offset our writer wrote.
		flipByteAt(t, p, indexOff)
		err = extract(t, p)
		if err == nil || !strings.Contains(err.Error(), "hash mismatch") {
			t.Fatalf("error = %v, want index hash mismatch", err)
		}
	})

	// A flipped bit inside the manifest's descriptor closure is
	// invisible to the header-only manifest decode (which skips that
	// field) — the index's manifest hash must catch it.
	t.Run("corrupt manifest byte", func(t *testing.T) {
		dir := writeTestPayloadDir(t, map[string][]byte{"000001.sst": content})
		sentinel := "zz-hash-sentinel.proto"
		m := c1zv3.C1ZManifestV3_builder{
			Engine:          "pebble",
			PayloadEncoding: c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD,
			Descriptors: &descriptorpb.FileDescriptorSet{
				File: []*descriptorpb.FileDescriptorProto{{Name: proto.String(sentinel)}},
			},
		}.Build()
		p := filepath.Join(t.TempDir(), "m.c1z")
		out, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := WriteEnvelopeWithReuse(out, m, dir, nil); err != nil {
			t.Fatal(err)
		}
		if err := out.Close(); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		i := bytes.Index(raw, []byte(sentinel))
		if i < 0 {
			t.Fatal("sentinel not found in manifest bytes")
		}
		// Flip a content byte inside the sentinel string: the wire
		// structure stays valid, only the bytes change.
		flipByteAt(t, p, int64(i)+2)
		err = extract(t, p)
		if err == nil || !strings.Contains(err.Error(), "manifest bytes hash mismatch") {
			t.Fatalf("error = %v, want manifest hash mismatch", err)
		}
	})

	// rewriteTrailer rebuilds the envelope's trailer — mutated index,
	// recomputed footer — so subtests can craft hostile-but-well-formed
	// indexes that pass the footer hash and fail entry validation.
	rewriteTrailer := func(t *testing.T, p string, mutate func(idx *c1zv3.IndexedFrameIndex)) {
		t.Helper()
		f, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		idx, err := ReadIndexedFrameIndex(f)
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var footer [indexedFooterLen]byte
		copy(footer[:], raw[len(raw)-indexedFooterLen:])
		indexOff := int64(binary.BigEndian.Uint64(footer[0:8])) //nolint:gosec // offset our writer wrote.

		mutate(idx)
		ib, err := proto.Marshal(idx)
		if err != nil {
			t.Fatal(err)
		}
		out := append([]byte{}, raw[:indexOff]...)
		out = append(out, ib...)
		binary.BigEndian.PutUint64(footer[0:8], uint64(indexOff)) //nolint:gosec // non-negative offset.
		binary.BigEndian.PutUint32(footer[8:12], uint32(len(ib))) //nolint:gosec // small test index.
		binary.BigEndian.PutUint64(footer[12:20], xxhash.Sum64(ib))
		out = append(out, footer[:]...)
		//nolint:gosec // p is a t.TempDir path created by this test.
		if err := os.WriteFile(p, out, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// A well-formed footer over a hostile index with duplicate names
	// must be rejected: duplicates would race parallel extraction
	// workers onto the same target file.
	t.Run("duplicate entry name", func(t *testing.T) {
		p := newEnv(t)
		rewriteTrailer(t, p, func(idx *c1zv3.IndexedFrameIndex) {
			idx.SetEntries(append(idx.GetEntries(), idx.GetEntries()[0]))
		})
		err := extract(t, p)
		if err == nil || !strings.Contains(err.Error(), "duplicate") {
			t.Fatalf("error = %v, want duplicate entry rejection", err)
		}
	})

	// compSize == 0 can't come from our writer (WithZeroFrames makes
	// even empty files encode to a complete frame), so the reader
	// treats it as index corruption.
	t.Run("zero-length frame range", func(t *testing.T) {
		p := newEnv(t)
		rewriteTrailer(t, p, func(idx *c1zv3.IndexedFrameIndex) {
			idx.GetEntries()[0].SetCompressedSize(0)
		})
		err := extract(t, p)
		if err == nil || !strings.Contains(err.Error(), "sizes out of range") {
			t.Fatalf("error = %v, want size-range rejection", err)
		}
	})

	t.Run("corrupt frame byte", func(t *testing.T) {
		p := newEnv(t)
		f, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		idx, err := ReadIndexedFrameIndex(f)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		e := idx.GetEntries()[0]
		flipByteAt(t, p, e.GetFrameOffset()+e.GetCompressedSize()/2)
		if err := extract(t, p); err == nil {
			t.Fatal("extraction of corrupt frame succeeded")
		}
	})
}

// TestIndexedSpliceCarriesSHA256 verifies content identities survive
// the splice path: spliced frames keep their SHA-256 from the source
// index, fresh frames get newly computed ones, and a reuse table with
// missing identities forces recomputation rather than emitting empty
// hashes.
func TestIndexedSpliceCarriesSHA256(t *testing.T) {
	files := map[string][]byte{
		"000001.sst": randomBytes(t, 64<<10),
		"000002.sst": randomBytes(t, 32<<10),
	}
	dir := writeTestPayloadDir(t, files)
	srcPath := writeIndexedEnvelope(t, dir)

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
	for _, e := range reuse.byName {
		if len(e.SHA256) != sha256.Size {
			t.Fatalf("%s: reuse entry missing sha256 from trailer", e.Name)
		}
	}
	// Wipe the identities to model a caller-constructed reuse table:
	// the writer must recompute, not emit empty hashes.
	for _, e := range reuse.byName {
		e.SHA256 = nil
	}

	checkpoint := t.TempDir()
	for name := range files {
		if err := os.Link(filepath.Join(extracted, name), filepath.Join(checkpoint, name)); err != nil {
			t.Skipf("hard links unavailable on this filesystem: %v", err)
		}
	}
	newSST := randomBytes(t, 16<<10)
	if err := os.WriteFile(filepath.Join(checkpoint, "000003.sst"), newSST, 0o600); err != nil {
		t.Fatal(err)
	}

	dstPath := filepath.Join(t.TempDir(), "dst.c1z")
	dst, err := os.Create(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := WriteEnvelopeWithReuse(dst, indexedManifest(), checkpoint, reuse)
	if err != nil {
		t.Fatal(err)
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
	if stats.SplicedFrames != 2 || stats.EncodedFrames != 1 {
		t.Fatalf("stats = %+v, want 2 spliced / 1 encoded", stats)
	}

	expect := map[string][]byte{
		"000001.sst": files["000001.sst"],
		"000002.sst": files["000002.sst"],
		"000003.sst": newSST,
	}
	df, err := os.Open(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	idx, err := ReadIndexedFrameIndex(df)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.GetEntries()) != len(expect) {
		t.Fatalf("index has %d entries, want %d", len(idx.GetEntries()), len(expect))
	}
	for _, e := range idx.GetEntries() {
		want := sha256.Sum256(expect[e.GetName()])
		if !bytes.Equal(e.GetRawSha256(), want[:]) {
			t.Fatalf("%s: sha256 mismatch in spliced envelope's index", e.GetName())
		}
	}
}

// TestIndexedEmptyPayload locks in the degenerate case: an envelope
// with zero payload files still carries a valid (empty) index and
// extracts cleanly.
func TestIndexedEmptyPayload(t *testing.T) {
	envPath := writeIndexedEnvelope(t, t.TempDir())
	f, err := os.Open(envPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	idx, err := ReadIndexedFrameIndex(f)
	if err != nil {
		t.Fatalf("ReadIndexedFrameIndex: %v", err)
	}
	if len(idx.GetEntries()) != 0 || idx.GetTotalRawSize() != 0 || idx.GetTotalCompressedSize() != 0 {
		t.Fatalf("empty payload index not empty: %v", idx)
	}
	if _, _, err := ExtractEnvelopePayload(f, t.TempDir()); err != nil {
		t.Fatalf("ExtractEnvelopePayload: %v", err)
	}
}
