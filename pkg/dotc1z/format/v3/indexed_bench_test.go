package v3

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// benchPayloadDir writes n files of size bytes each. Content is
// half-compressible (random first half mirrored into the second) so
// the zstd encoder does representative work — all-random data turns
// the encoder into a memcpy and all-zero data overstates it.
func benchPayloadDir(b *testing.B, n, size int) string {
	b.Helper()
	dir := b.TempDir()
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic bench data.
	buf := make([]byte, size)
	for i := range n {
		half := buf[:size/2]
		_, _ = rng.Read(half)
		copy(buf[size/2:], half)
		name := filepath.Join(dir, fmt.Sprintf("%06d.sst", i+1))
		if err := os.WriteFile(name, buf, 0o600); err != nil {
			b.Fatal(err)
		}
	}
	return dir
}

func benchEnvelope(b *testing.B, dir string) string {
	b.Helper()
	envPath := filepath.Join(b.TempDir(), "bench.c1z")
	out, err := os.Create(envPath)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := WriteEnvelopeWithReuse(out, indexedManifest(), dir, nil); err != nil {
		b.Fatal(err)
	}
	if err := out.Close(); err != nil {
		b.Fatal(err)
	}
	return envPath
}

// BenchmarkReadIndexedFrameIndex measures the chunk-planning
// primitive at a production-shaped frame count: the whole frame table
// (names, ranges, hashes) from three preads, no frame bytes touched.
func BenchmarkReadIndexedFrameIndex(b *testing.B) {
	dir := benchPayloadDir(b, 1024, 4<<10)
	envPath := benchEnvelope(b, dir)
	f, err := os.Open(envPath)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	b.ReportAllocs()
	for b.Loop() {
		idx, err := ReadIndexedFrameIndex(f)
		if err != nil {
			b.Fatal(err)
		}
		if len(idx.GetEntries()) != 1024 {
			b.Fatalf("entries = %d", len(idx.GetEntries()))
		}
	}
}

// BenchmarkIndexedWriteFresh is the cold-save path: every frame is
// compressed (and xxh64+SHA-256 hashed). The single-pass writer needs
// no seeking, so the sink is io.Discard.
func BenchmarkIndexedWriteFresh(b *testing.B) {
	const n, size = 32, 256 << 10
	dir := benchPayloadDir(b, n, size)
	b.SetBytes(int64(n * size))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := WriteEnvelopeWithReuse(io.Discard, indexedManifest(), dir, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkIndexedWriteSpliced is the incremental-save path: every
// frame matches the source envelope via the os.SameFile fast path and
// is copied as compressed bytes. The fresh/spliced delta is the win
// the fold compactor (and a chunked uploader) banks on.
func BenchmarkIndexedWriteSpliced(b *testing.B) {
	const n, size = 32, 256 << 10
	dir := benchPayloadDir(b, n, size)
	envPath := benchEnvelope(b, dir)

	f, err := os.Open(envPath)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	extracted := b.TempDir()
	_, reuse, err := ExtractEnvelopePayload(f, extracted)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(n * size))
	b.ReportAllocs()
	for b.Loop() {
		stats, err := WriteEnvelopeWithReuse(io.Discard, indexedManifest(), extracted, reuse)
		if err != nil {
			b.Fatal(err)
		}
		if stats.SplicedFrames != n {
			b.Fatalf("spliced %d frames, want %d", stats.SplicedFrames, n)
		}
	}
}

// BenchmarkIndexedExtract is the open path: parallel pread +
// decompress of every frame into a fresh directory.
func BenchmarkIndexedExtract(b *testing.B) {
	const n, size = 64, 64 << 10
	dir := benchPayloadDir(b, n, size)
	envPath := benchEnvelope(b, dir)
	f, err := os.Open(envPath)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	root := b.TempDir()
	i := 0
	b.SetBytes(int64(n * size))
	b.ReportAllocs()
	for b.Loop() {
		dest := filepath.Join(root, strconv.Itoa(i))
		i++
		if err := os.Mkdir(dest, 0o755); err != nil {
			b.Fatal(err)
		}
		if _, _, err := ExtractEnvelopePayload(f, dest); err != nil {
			b.Fatal(err)
		}
	}
}

// benchManifestHead builds a realistic envelope head: a manifest
// carrying a full descriptor closure (every linked proto file, the
// dominant manifest cost) and the single sync run the pebble contract
// allows, prefixed by the v3 magic and length.
func benchManifestHead(b *testing.B) []byte {
	b.Helper()
	fds := &descriptorpb.FileDescriptorSet{}
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		fds.File = append(fds.File, protodesc.ToFileDescriptorProto(fd))
		return true
	})
	now := time.Now()
	m := c1zv3.C1ZManifestV3_builder{
		Engine:              "pebble",
		EngineSchemaVersion: 1,
		PayloadEncoding:     c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD,
		Descriptors:         fds,
		SyncRuns: []*c1zv3.SyncRunSummary{
			c1zv3.SyncRunSummary_builder{
				SyncId:    "01J0000000000000000000SYNC",
				StartedAt: timestamppb.New(now.Add(-time.Minute)),
				EndedAt:   timestamppb.New(now),
			}.Build(),
		},
	}.Build()
	mb, err := MarshalManifest(m)
	if err != nil {
		b.Fatal(err)
	}
	head := bytes.NewBuffer(nil)
	_, _ = head.Write(C1Z3Magic)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(mb))) //nolint:gosec // bench manifest is small.
	_, _ = head.Write(lenBuf[:])
	_, _ = head.Write(mb)
	return head.Bytes()
}

// BenchmarkReadManifestHeader measures the header-only open path:
// the hand-rolled field skip must stay cheap even though the manifest
// bytes are dominated by the descriptor closure it never decodes.
func BenchmarkReadManifestHeader(b *testing.B) {
	head := benchManifestHead(b)
	b.SetBytes(int64(len(head)))
	b.ReportAllocs()
	for b.Loop() {
		m, err := ReadManifestHeader(bytes.NewReader(head))
		if err != nil {
			b.Fatal(err)
		}
		if len(m.GetSyncRuns()) != 1 {
			b.Fatal("sync runs not decoded")
		}
	}
}
