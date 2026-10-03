// SPDX-License-Identifier: Apache-2.0

package v3

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

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
	var manifest bytes.Buffer
	manifest.Write([]byte{0x0a, byte(len("pebble3"))})
	manifest.WriteString("pebble3")
	manifest.Write([]byte{0x20, 0x05}) // field 4 varint 5

	// Envelope: 5-byte magic + u32 manifest length + manifest + shared frame.
	var env bytes.Buffer
	env.Write([]byte("C1Z3\x00"))
	mlen := make([]byte, 4)
	binary.BigEndian.PutUint32(mlen, uint32(manifest.Len()))
	env.Write(mlen)
	env.Write(manifest.Bytes())
	frameOffset := int64(env.Len())
	env.Write(frame)

	// IndexedFrameIndex (pb/c1/c1z/v3): repeated entries field 1; each entry
	// carries name=1(bytes), frame_offset=2(varint), compressed_size=3(varint),
	// raw_size=4(varint), raw_xxh64=5(fixed64), raw_sha256=6(bytes).
	// All N entries share the one empty frame, raw_size == 0, with the
	// checksums of the (empty) decoded content.
	var indexProto bytes.Buffer
	for i := range numEntries {
		var entry bytes.Buffer
		name := fmt.Sprintf(entryName, i)
		entry.WriteByte(0x0a)
		entry.WriteByte(byte(len(name)))
		entry.WriteString(name)
		entry.WriteByte(0x10)
		entry.Write(binary.AppendUvarint(nil, uint64(frameOffset)))
		entry.WriteByte(0x18)
		entry.Write(binary.AppendUvarint(nil, uint64(len(frame))))
		entry.WriteByte(0x20)
		entry.Write(binary.AppendUvarint(nil, 0)) // raw_size == 0
		entry.WriteByte(0x29)                     // field 5, wire type 1 (fixed64)
		xxh := make([]byte, 8)
		binary.LittleEndian.PutUint64(xxh, xxhash.Sum64(nil))
		entry.Write(xxh)
		entry.WriteByte(0x32) // field 6, wire type 2 (bytes)
		entry.Write(binary.AppendUvarint(nil, sha256.Size))
		entry.Write(emptySum[:])

		indexProto.WriteByte(0x0a) // entries field 1
		indexProto.Write(binary.AppendUvarint(nil, uint64(entry.Len())))
		indexProto.Write(entry.Bytes())
	}
	indexBytes := indexProto.Bytes()

	// Footer: indexOffset(u64 BE) + indexLen(u32 BE) + xxh64(index)(u64 BE)
	// + "C1ZIDX1\x00".
	indexOffset := int64(env.Len())
	env.Write(indexBytes)
	footer := make([]byte, 0, 28)
	var off [8]byte
	binary.BigEndian.PutUint64(off[:], uint64(indexOffset))
	footer = append(footer, off[:]...)
	var ilen [4]byte
	binary.BigEndian.PutUint32(ilen[:], uint32(len(indexBytes)))
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