package v3

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

// TestSecurity_IndexedExtractionBudgetBoundsEntryCardinality: an indexed
// envelope whose entries all point at one empty zstd frame (raw_size 0,
// valid checksums) costs nothing in decoded bytes, so before entries were
// charged a per-entry overhead it extracted every file under any budget.
// With a 1-byte budget the extraction must fail before creating a file,
// whether the header fail-fast or the decode-time budget catches it.
func TestSecurity_IndexedExtractionBudgetBoundsEntryCardinality(t *testing.T) {
	const (
		numEntries = 5000
		entryName  = "security-cardinality-%06d.sst"
	)

	// One shared empty zstd frame (WithZeroFrames) — genuinely decodes to zero
	// bytes with a well-formed frame header, so every per-entry checksum below
	// is internally consistent (an attacker can produce this in a real .c1z).
	enc, err := zstd.NewWriter(nil, zstd.WithZeroFrames(true), zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}
	frame := enc.EncodeAll(nil, nil)
	if err := enc.Close(); err != nil {
		t.Fatalf("zstd close: %v", err)
	}
	if len(frame) == 0 || len(frame) > 64 {
		t.Fatalf("empty frame not minimal: %d bytes", len(frame))
	}
	emptySum := sha256.Sum256(nil)

	// Minimal manifest: engine "pebble3", payload_encoding = INDEXED_ZSTD (5).
	// Built with protowire helpers — no raw tag/length bytes, no
	// int->uint conversions for the wire framing.
	var manifest []byte
	manifest = protowire.AppendTag(manifest, 1, protowire.BytesType)
	manifest = protowire.AppendString(manifest, "pebble3")
	manifest = protowire.AppendTag(manifest, 4, protowire.VarintType)
	manifest = protowire.AppendVarint(manifest, 5)

	// Envelope: 5-byte magic + u32 manifest length + manifest + shared frame.
	var env bytes.Buffer
	env.Write([]byte("C1Z3\x00"))
	mlen := make([]byte, 4)
	manifestLen := len(manifest)
	if manifestLen > math.MaxUint32 {
		t.Fatalf("manifest too large")
	}
	binary.BigEndian.PutUint32(mlen, uint32(manifestLen)) // #nosec G115 -- bounded to MaxUint32 by the check above.
	env.Write(mlen)
	env.Write(manifest)
	frameOffset := int64(env.Len())
	env.Write(frame)

	// IndexedFrameIndex (pb/c1/c1z/v3): repeated entries field 1; each entry
	// carries name=1(bytes), frame_offset=2(varint), compressed_size=3(varint),
	// raw_size=4(varint), raw_xxh64=5(fixed64), raw_sha256=6(bytes).
	// All N entries share the one empty frame, raw_size == 0, with the
	// checksums of the (empty) decoded content.
	var indexProto []byte
	for i := range numEntries {
		var entry []byte
		name := fmt.Sprintf(entryName, i)
		entry = protowire.AppendTag(entry, 1, protowire.BytesType)
		entry = protowire.AppendString(entry, name)
		entry = protowire.AppendTag(entry, 2, protowire.VarintType)
		entry = protowire.AppendVarint(entry, uint64(frameOffset)) // #nosec G115 -- frameOffset is a small in-memory buffer position, non-negative.
		entry = protowire.AppendTag(entry, 3, protowire.VarintType)
		entry = protowire.AppendVarint(entry, uint64(len(frame)))
		entry = protowire.AppendTag(entry, 4, protowire.VarintType)
		entry = protowire.AppendVarint(entry, 0) // raw_size == 0
		entry = protowire.AppendTag(entry, 5, protowire.Fixed64Type)
		var xxh [8]byte
		binary.LittleEndian.PutUint64(xxh[:], xxhash.Sum64(nil))
		entry = append(entry, xxh[:]...)
		entry = protowire.AppendTag(entry, 6, protowire.BytesType)
		entry = protowire.AppendBytes(entry, emptySum[:])

		indexProto = protowire.AppendTag(indexProto, 1, protowire.BytesType)
		indexProto = protowire.AppendBytes(indexProto, entry)
	}
	indexBytes := indexProto

	// Footer: indexOffset(u64 BE) + indexLen(u32 BE) + xxh64(index)(u64 BE)
	// + "C1ZIDX1\x00".
	indexOffset := int64(env.Len())
	env.Write(indexBytes)
	footer := make([]byte, 0, 28)
	if indexOffset < 0 {
		t.Fatalf("negative index offset")
	}
	var off [8]byte
	binary.BigEndian.PutUint64(off[:], uint64(indexOffset)) // #nosec G115 -- bounded non-negative by the check above.
	footer = append(footer, off[:]...)
	if len(indexBytes) > math.MaxUint32 {
		t.Fatalf("index too large")
	}
	var ilen [4]byte
	binary.BigEndian.PutUint32(ilen[:], uint32(len(indexBytes))) // #nosec G115 -- bounded to MaxUint32 by the check above.
	footer = append(footer, ilen[:]...)
	var ixxh [8]byte
	binary.BigEndian.PutUint64(ixxh[:], xxhash.Sum64(indexBytes))
	footer = append(footer, ixxh[:]...)
	footer = append(footer, []byte("C1ZIDX1\x00")...)
	env.Write(footer)

	envPath := filepath.Join(t.TempDir(), "hostile-indexed.c1z")
	require.NoError(t, os.WriteFile(envPath, env.Bytes(), 0o600))

	for _, failFastDisabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("failFastDisabled=%t", failFastDisabled), func(t *testing.T) {
			orig := fcsFailFastDisabled
			fcsFailFastDisabled = failFastDisabled
			t.Cleanup(func() { fcsFailFastDisabled = orig })

			f, err := os.Open(envPath)
			require.NoError(t, err)
			defer f.Close()
			dest := t.TempDir()
			_, _, err = ExtractEnvelopePayload(f, dest, WithMaxDecodedPayloadBytes(1))
			require.ErrorIs(t, err, ErrMaxSizeExceeded)
			require.Zero(t, countFiles(t, dest), "no entry may be extracted once the budget is exceeded")
		})
	}
}

func countFiles(t *testing.T, root string) int {
	t.Helper()
	n := 0
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk dest: %v", err)
	}
	return n
}
