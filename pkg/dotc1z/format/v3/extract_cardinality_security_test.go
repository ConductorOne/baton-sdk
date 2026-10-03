// SPDX-License-Identifier: Apache-2.0

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

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zstd"
)

// TestSecurity_IndexedExtractionBudgetBoundsEntryCardinality guards the
// decoded-byte budget's cardinality axis for PAYLOAD_ENCODING_INDEXED_ZSTD.
//
// Finding: indexed-zstd-decoded-budget-cardinality-bypass
//
// The budget is charged only with decoded frame bytes
// (budgetedFrameWriter.Write -> decodedBudget.take). Trailer entries with
// raw_size == 0 are legal ("rawSize 0 is legal", indexed.go) and an empty zstd
// frame decodes to zero bytes, so a hostile envelope filled with empty-frame
// entries extracted unlimited files/directories at zero budget cost while the
// tar encodings charge >=512 decoded bytes per entry header.
//
// This test pins the INVARIANT the fix must enforce (the budget bounds
// extraction cardinality), not a specific implementation: build a minimal
// indexed envelope whose N entries all share one empty frame with valid
// checksums, extract with a tiny budget, and assert the extraction is
// rejected (or the created-file count stays within a budget-consistent
// bound) rather than creating all N files.
//
// Pre-fix (red): extraction succeeds creating all N files at budget=1.
// Post-fix (green): typed budget error (or bounded file count) before
// unbounded file creation.
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
	if err := os.WriteFile(envPath, env.Bytes(), 0o600); err != nil {
		t.Fatalf("write envelope: %v", err)
	}
	f, err := os.Open(envPath)
	if err != nil {
		t.Fatalf("open envelope: %v", err)
	}
	defer f.Close()

	dest := t.TempDir()
	_, _, err = ExtractEnvelopePayload(f, dest, WithMaxDecodedPayloadBytes(1))

	// The invariant: a 1-byte budget must NOT permit bulk extraction. Count
	// what actually landed regardless of the error, so the assertion holds
	// whether the fix rejects at parse (typed budget error) or bounds the
	// per-entry filesystem work against the budget.
	files := countFiles(t, dest)

	if err == nil && files >= numEntries {
		t.Fatalf("cardinality bypass: budget=1 extracted %d files from %d empty-frame entries (err=nil)", files, numEntries)
	}
	if err == nil && files > 64 {
		t.Fatalf("budget=1 extraction created %d files with err=nil; extraction cost is not bounded by the decoded budget", files)
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
