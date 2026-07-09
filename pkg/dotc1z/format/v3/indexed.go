package v3

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

// PAYLOAD_ENCODING_INDEXED_ZSTD payload. Layout, immediately after
// the manifest:
//
//	payload := frame* | index | footer
//	footer  := u64 indexOffset | u32 indexLen | u64 xxh64(index) | "C1ZIDX1\0"
//
// All integers big-endian, matching the envelope's length prefix.
// Each frame is the bare zstd stream of one payload file — no
// per-frame framing of our own — with Frame_Content_Size advertised,
// so a frame's byte range is independently meaningful: pread it and
// you hold a self-describing zstd stream any vanilla consumer can
// decode. The index is the sole frame table: a binary-proto
// c1zv3.IndexedFrameIndex carrying, per frame, the slash-separated
// relative path (filepath.ToSlash on write, FromSlash on extract, so
// payloads are portable across macOS/Linux/Windows), the absolute
// byte offset and compressed size, the raw size, an xxh64 of the raw
// bytes (fast corruption check at extract), and a SHA-256 of the raw
// bytes (content-address identity for chunked object storage). The
// proto envelope is the extension point for future per-frame
// metadata (codec, dictionary, encryption) without a layout rev.
//
// The whole table is fetchable in O(1) reads — fixed-size footer,
// then index — which is what makes ranged reads over object storage
// practical. There is deliberately no streaming-scannable framing: a
// payload without a valid footer is corruption-class
// (ErrEnvelopeTruncated; invalidate and resync), the same policy as
// a torn tar payload.
//
// This layout is what enables:
//
//   - parallel decode at open: workers pread their frame's byte range
//     (io.ReaderAt is safe for concurrent use on all three supported
//     platforms) and stream-decompress, instead of the single-stream
//     encodings' one-core sequential decode;
//   - frame splicing at save: an unchanged payload file's compressed
//     frame is copied verbatim from the source envelope. Pebble SSTs
//     are immutable and checkpoints hard-link them, so incremental
//     rewrites (the fold compactor) reuse almost every frame and pay
//     compression only for the handful of new files; and
//   - single-pass writes: nothing is backpatched, so an envelope can
//     be encoded straight to a non-seekable sink (a network upload),
//     and a chunked store can reconstruct the envelope byte range by
//     byte range from the index alone.

const maxIndexedNameBytes = 4096

// indexedFooterMagic ends an indexed payload. 8 bytes so the whole
// footer stays u64-aligned; the trailing "1" is the trailer version.
var indexedFooterMagic = []byte("C1ZIDX1\x00")

// indexedFooterLen is u64 indexOffset + u32 indexLen + u64 xxh64(index) + magic.
const indexedFooterLen = 8 + 4 + 8 + 8

// maxIndexedIndexBytes caps the trailer index size we accept on read.
// ~80 bytes/entry means even a million-frame payload stays an order
// of magnitude below this; protects against a hostile footer
// claiming a multi-GiB index.
const maxIndexedIndexBytes = 64 << 20

// ReuseEntry describes one frame of a source envelope: where its
// compressed bytes live in the source file, what they decode to, and
// where the extractor materialized them.
type ReuseEntry struct {
	Name           string
	FrameOffset    int64 // offset of the compressed bytes in the source file
	CompressedSize int64
	RawSize        int64
	XXH64          uint64
	// SHA256 is the content-address identity of the raw bytes, carried
	// from the source envelope's trailer index. The splice writer
	// recomputes it from the local payload file if a caller hands it an
	// entry without one.
	SHA256 []byte
	// ExtractedPath is where extraction wrote the raw bytes. Used for
	// the zero-read os.SameFile identity check at save time: a
	// checkpoint hard-link of the extracted file is provably the same
	// content. Empty when unknown.
	ExtractedPath string
}

// PayloadReuse indexes a source envelope's frames for splice-at-save.
// Returned by ExtractEnvelopePayload for indexed payloads; passed to
// WriteEnvelopeWithReuse when rewriting a store opened from that
// source.
type PayloadReuse struct {
	srcPath string
	byName  map[string]*ReuseEntry
}

// SourcePath returns the envelope file the reuse entries point into.
func (r *PayloadReuse) SourcePath() string {
	if r == nil {
		return ""
	}
	return r.srcPath
}

// match returns the reusable entry for a payload file, or nil when the
// file is new or changed. The fast path is the os.SameFile identity
// check against the extracted file (zero reads — covers checkpoint
// hard-links); the fallback hashes the candidate's raw bytes, covering
// filesystems where hard-linking wasn't possible and pebble copied.
func (r *PayloadReuse) match(name string, info os.FileInfo, path string) *ReuseEntry {
	if r == nil {
		return nil
	}
	e := r.byName[name]
	if e == nil || e.RawSize != info.Size() {
		return nil
	}
	if e.ExtractedPath != "" {
		if ei, err := os.Stat(e.ExtractedPath); err == nil && os.SameFile(ei, info) {
			return e
		}
	}
	h, err := xxh64File(path)
	if err != nil || h != e.XXH64 {
		return nil
	}
	return e
}

func xxh64File(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	h := xxhash.New()
	if _, err := io.Copy(h, f); err != nil {
		return 0, err
	}
	return h.Sum64(), nil
}

func sha256File(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

// SpliceStats reports how a WriteEnvelopeWithReuse call split between
// verbatim frame copies and fresh compression. Exposed so callers can
// log reuse effectiveness in production.
type SpliceStats struct {
	SplicedFrames int
	SplicedBytes  int64 // compressed bytes copied verbatim
	EncodedFrames int
	EncodedBytes  int64 // raw bytes freshly compressed
}

// writeIndexedZstd writes the indexed payload for dir to w in a
// single pass: frames back-to-back, then the index, then the footer.
// Nothing is backpatched, so w can be any io.Writer — including a
// non-seekable network sink. payloadStart is the absolute file
// offset of the first payload byte (magic + manifest length prefix +
// manifest); index offsets are absolute so a ranged reader needs no
// other context. manifestXXH64 is the hash of the marshaled manifest
// bytes, recorded in the index so the otherwise-unprotected envelope
// head is covered too.
func writeIndexedZstd(w io.Writer, payloadStart int64, manifestXXH64 uint64, dir string, reuse *PayloadReuse) (SpliceStats, error) {
	var stats SpliceStats

	var paths []string
	walkErr := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			paths = append(paths, path)
		}
		return nil
	})
	if walkErr != nil {
		return stats, walkErr
	}
	sort.Strings(paths)

	var srcFile *os.File
	if reuse != nil && reuse.srcPath != "" {
		f, err := os.Open(reuse.srcPath)
		if err != nil {
			return stats, fmt.Errorf("c1z v3: open splice source: %w", err)
		}
		srcFile = f
		defer srcFile.Close()
	}

	// WithZeroFrames is pinned (it is the library default today, but
	// documented as optional): zero-length payload files must still
	// produce complete zstd frames, or the "every byte range is a
	// standalone decodable stream" property breaks for empty files.
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithZeroFrames(true))
	if err != nil {
		return stats, err
	}
	defer enc.Close()

	var idxEntries []*c1zv3.IndexedFrameEntry
	var totalRaw, totalComp int64

	// All payload writes go through cw so frame offsets are known
	// without seeking.
	cw := &countingW{w: w}
	for _, path := range paths {
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return stats, err
		}
		name := filepath.ToSlash(rel)
		if len(name) > maxIndexedNameBytes {
			return stats, fmt.Errorf("c1z v3: payload name too long: %q", name)
		}
		info, err := os.Stat(path)
		if err != nil {
			return stats, err
		}
		frameOff := payloadStart + cw.n

		if e := reuse.match(name, info, path); e != nil && srcFile != nil {
			if _, err := io.Copy(cw, io.NewSectionReader(srcFile, e.FrameOffset, e.CompressedSize)); err != nil {
				return stats, fmt.Errorf("c1z v3: splice frame %q: %w", name, err)
			}
			sha := e.SHA256
			if len(sha) == 0 {
				// The local file is proven byte-identical to the spliced
				// frame's raw bytes (match above), so hash it directly.
				if sha, err = sha256File(path); err != nil {
					return stats, err
				}
			}
			idxEntries = append(idxEntries, c1zv3.IndexedFrameEntry_builder{
				Name:           name,
				FrameOffset:    frameOff,
				CompressedSize: e.CompressedSize,
				RawSize:        e.RawSize,
				RawXxh64:       e.XXH64,
				RawSha256:      sha,
			}.Build())
			totalRaw += e.RawSize
			totalComp += e.CompressedSize
			stats.SplicedFrames++
			stats.SplicedBytes += e.CompressedSize
			continue
		}

		f, err := os.Open(path)
		if err != nil {
			return stats, err
		}
		hasher := xxhash.New()
		shaHasher := sha256.New()
		// ResetContentSize advertises Frame_Content_Size in the frame
		// header, making each frame self-describing to any vanilla zstd
		// consumer; Close errors if the bytes copied disagree with the
		// stat size, catching a file mutated mid-encode. (Frames under
		// 256 bytes omit FCS — the zstd header can't represent sizes
		// below 256 without the single-segment flag — so the index's
		// raw_size is the authoritative size everywhere.)
		enc.ResetContentSize(cw, info.Size())
		rawN, err := io.Copy(io.MultiWriter(enc, hasher, shaHasher), f)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return stats, err
		}
		if err := enc.Close(); err != nil {
			return stats, err
		}
		compN := payloadStart + cw.n - frameOff
		idxEntries = append(idxEntries, c1zv3.IndexedFrameEntry_builder{
			Name:           name,
			FrameOffset:    frameOff,
			CompressedSize: compN,
			RawSize:        rawN,
			RawXxh64:       hasher.Sum64(),
			RawSha256:      shaHasher.Sum(nil),
		}.Build())
		totalRaw += rawN
		totalComp += compN
		stats.EncodedFrames++
		stats.EncodedBytes += rawN
	}

	// Index, then footer. See the layout comment at the top of this
	// file.
	ib, err := proto.Marshal(c1zv3.IndexedFrameIndex_builder{
		Entries:             idxEntries,
		TotalRawSize:        totalRaw,
		TotalCompressedSize: totalComp,
		ManifestXxh64:       manifestXXH64,
	}.Build())
	if err != nil {
		return stats, err
	}
	if len(ib) > maxIndexedIndexBytes {
		return stats, fmt.Errorf("c1z v3: trailer index is %d bytes, exceeds %d", len(ib), maxIndexedIndexBytes)
	}
	indexOff := payloadStart + cw.n
	if _, err := cw.Write(ib); err != nil {
		return stats, err
	}
	var footer [indexedFooterLen]byte
	binary.BigEndian.PutUint64(footer[0:8], uint64(indexOff)) //nolint:gosec // file offsets are non-negative.
	binary.BigEndian.PutUint32(footer[8:12], uint32(len(ib))) //nolint:gosec // capped at maxIndexedIndexBytes above.
	binary.BigEndian.PutUint64(footer[12:20], xxhash.Sum64(ib))
	copy(footer[20:28], indexedFooterMagic)
	if _, err := cw.Write(footer[:]); err != nil {
		return stats, err
	}
	return stats, nil
}

type countingW struct {
	w io.Writer
	n int64
}

func (c *countingW) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

type decodedBudget struct {
	mu        sync.Mutex
	remaining uint64
	limit     uint64
}

func newDecodedBudget(limit uint64) *decodedBudget {
	return &decodedBudget{remaining: limit, limit: limit}
}

func (b *decodedBudget) take(n int) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.remaining == 0 {
		return 0, b.err()
	}
	if uint64(n) > b.remaining { //nolint:gosec // n is len(p), always non-negative.
		allowed := int(b.remaining) //nolint:gosec // allowed only feeds a slice bound after b.remaining < uint64(len(p)).
		b.remaining = 0
		return allowed, b.err()
	}
	b.remaining -= uint64(n) //nolint:gosec // n is len(p), always non-negative.
	return n, nil
}

func (b *decodedBudget) err() error {
	return fmt.Errorf("c1z v3: indexed decoded bytes exceed %d bytes: %w", b.limit, ErrMaxSizeExceeded)
}

type budgetedFrameWriter struct {
	budget *decodedBudget
	out    io.Writer
	hash   io.Writer
}

func (w budgetedFrameWriter) Write(p []byte) (int, error) {
	n, err := w.budget.take(len(p))
	if n > 0 {
		if _, werr := w.out.Write(p[:n]); werr != nil {
			return 0, werr
		}
		if _, werr := w.hash.Write(p[:n]); werr != nil {
			return 0, werr
		}
	}
	if err != nil {
		return n, err
	}
	return n, nil
}

// readIndexedTrailer reads the trailer index — the payload's only
// frame table — without touching any frame bytes: one pread for the
// fixed-size footer, one for the index. This is the O(1)-round-trip
// shape that scales to ranged reads over object storage.
//
// A missing or mangled footer is corruption-class
// (ErrEnvelopeTruncated): there is no other framing to fall back to,
// by design, and the caller's corruption classifier should
// invalidate the file and trigger a resync.
func readIndexedTrailer(f *os.File, payloadStart int64) (*c1zv3.IndexedFrameIndex, []*ReuseEntry, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}
	size := st.Size()
	if size < payloadStart+indexedFooterLen {
		return nil, nil, fmt.Errorf("%w: indexed payload too small for trailer footer", ErrEnvelopeTruncated)
	}
	var footer [indexedFooterLen]byte
	if _, err := f.ReadAt(footer[:], size-indexedFooterLen); err != nil {
		return nil, nil, err
	}
	if !bytes.Equal(footer[20:28], indexedFooterMagic) {
		return nil, nil, fmt.Errorf("%w: indexed payload trailer magic missing (truncated file, or not an indexed payload)", ErrEnvelopeTruncated)
	}
	indexOff := int64(binary.BigEndian.Uint64(footer[0:8]))  //nolint:gosec // bounds-checked against the file size below.
	indexLen := int64(binary.BigEndian.Uint32(footer[8:12])) // u32, cannot overflow int64.
	if indexLen > maxIndexedIndexBytes {
		return nil, nil, fmt.Errorf("c1z v3: trailer index claims %d bytes, exceeds cap %d", indexLen, maxIndexedIndexBytes)
	}
	if indexOff < payloadStart || indexOff+indexLen+indexedFooterLen != size {
		return nil, nil, fmt.Errorf("c1z v3: trailer index does not fit the file (off=%d len=%d size=%d)", indexOff, indexLen, size)
	}
	ib := make([]byte, indexLen)
	if _, err := f.ReadAt(ib, indexOff); err != nil {
		return nil, nil, fmt.Errorf("%w: trailer index: %w", ErrEnvelopeTruncated, err)
	}
	if got, want := xxhash.Sum64(ib), binary.BigEndian.Uint64(footer[12:20]); got != want {
		return nil, nil, errors.New("c1z v3: trailer index hash mismatch (corrupt index)")
	}
	idx := &c1zv3.IndexedFrameIndex{}
	if err := proto.Unmarshal(ib, idx); err != nil {
		return nil, nil, fmt.Errorf("c1z v3: unmarshal trailer index: %w", err)
	}
	entries := make([]*ReuseEntry, 0, len(idx.GetEntries()))
	seen := make(map[string]struct{}, len(idx.GetEntries()))
	for _, e := range idx.GetEntries() {
		name := e.GetName()
		if name == "" || len(name) > maxIndexedNameBytes {
			return nil, nil, fmt.Errorf("c1z v3: trailer index entry name invalid: %q", name)
		}
		// Duplicate names would have parallel extraction workers
		// racing writes to the same target file.
		if _, dup := seen[name]; dup {
			return nil, nil, fmt.Errorf("c1z v3: trailer index has duplicate entry %q", name)
		}
		seen[name] = struct{}{}
		// rawSize 0 is legal (empty payload file), but compSize must be
		// positive: WithZeroFrames guarantees even an empty file encodes
		// to a complete zstd frame, so a zero-length frame range can
		// only come from a corrupt or hand-mangled index.
		//
		// There is deliberately NO upper bound on either size: a single
		// Pebble SST can legitimately exceed any fixed cap (whale-scale
		// grants buckets are written as one whole-bucket SST), and the
		// bomb protections live elsewhere — compSize is bounds-checked
		// against the real file layout just below, and total decoded
		// output is enforced by the extraction budget
		// (BATON_DECODER_MAX_DECODED_SIZE_MB) regardless of what
		// raw_size claims.
		rawSize, compSize := e.GetRawSize(), e.GetCompressedSize()
		if rawSize < 0 || compSize <= 0 {
			return nil, nil, fmt.Errorf("c1z v3: trailer index entry %q sizes out of range (raw=%d comp=%d)", name, rawSize, compSize)
		}
		// Overflow-safe form of off+compSize > indexOff: with no upper
		// bound on compSize, the addition could wrap negative for a
		// hostile compressed_size near MaxInt64 and slip past the
		// comparison. Subtraction can't wrap here (off >= payloadStart
		// >= 0 and indexOff fits the file), and when off > indexOff the
		// negative difference rejects too, as it must.
		off := e.GetFrameOffset()
		if off < payloadStart || compSize > indexOff-off {
			return nil, nil, fmt.Errorf("c1z v3: trailer index entry %q frame range out of bounds (off=%d comp=%d)", name, off, compSize)
		}
		if len(e.GetRawSha256()) != sha256.Size {
			return nil, nil, fmt.Errorf("c1z v3: trailer index entry %q sha256 is %d bytes, want %d", name, len(e.GetRawSha256()), sha256.Size)
		}
		entries = append(entries, &ReuseEntry{
			Name:           name,
			FrameOffset:    off,
			CompressedSize: compSize,
			RawSize:        rawSize,
			XXH64:          e.GetRawXxh64(),
			SHA256:         e.GetRawSha256(),
		})
	}
	return idx, entries, nil
}

// ReadIndexedFrameIndex returns the frame table of an INDEXED_ZSTD
// envelope using three small preads (head, footer, index) — no frame
// bytes are read and the manifest body is never parsed. This is the
// planning primitive for chunked object storage: every frame's name,
// absolute byte range, sizes, and content identity (SHA-256 of raw
// bytes), plus payload totals, without rematerializing the store.
//
// Frame offsets are absolute file offsets, so entries can drive
// ranged reads (or object reassembly) directly. Returns
// ErrEnvelopeTruncated-wrapped errors for missing trailers and
// explicit errors for corrupt ones; returns an error for non-v3
// files. The caller retains ownership of f.
func ReadIndexedFrameIndex(f *os.File) (*c1zv3.IndexedFrameIndex, error) {
	head := make([]byte, len(C1Z3Magic)+4)
	if _, err := f.ReadAt(head, 0); err != nil {
		return nil, fmt.Errorf("%w: reading magic: %w", ErrEnvelopeTruncated, err)
	}
	if !bytes.Equal(head[:len(C1Z3Magic)], C1Z3Magic) {
		return nil, ErrInvalidV3Magic
	}
	mlen := binary.BigEndian.Uint32(head[len(C1Z3Magic):])
	if mlen > maxManifestBytes {
		return nil, fmt.Errorf("c1z v3: manifest claims %d bytes, exceeds cap %d", mlen, maxManifestBytes)
	}
	payloadStart := int64(len(C1Z3Magic)) + 4 + int64(mlen)
	idx, _, err := readIndexedTrailer(f, payloadStart)
	return idx, err
}

// extractIndexedZstd materializes an indexed payload into destDir,
// decoding frames in parallel. Returns the PayloadReuse map so a later
// save can splice unchanged frames. manifestXXH64 is the hash of the
// manifest bytes the caller just read; it is checked against the hash
// the index recorded at write time, closing the envelope-head
// integrity gap (header-only manifest decode skips the descriptor
// closure, so a flipped bit there would otherwise go unnoticed).
func extractIndexedZstd(f *os.File, payloadStart int64, manifestXXH64 uint64, destDir string, maxDecodedBytes uint64, maxMemoryBytes uint64, disableSizeFailFast bool) (*PayloadReuse, error) {
	idx, entries, err := readIndexedTrailer(f, payloadStart)
	if err != nil {
		return nil, err
	}
	if h := idx.GetManifestXxh64(); h != 0 && h != manifestXXH64 {
		return nil, fmt.Errorf("c1z v3: manifest bytes hash mismatch (index records %016x, head hashes to %016x): corrupt envelope head", h, manifestXXH64)
	}
	if !disableSizeFailFast {
		var totalRaw uint64
		for _, e := range entries {
			if e.RawSize < 0 {
				return nil, fmt.Errorf("c1z v3: indexed entry %q raw size is negative: %d", e.Name, e.RawSize)
			}
			raw := uint64(e.RawSize)
			if raw > maxDecodedBytes || totalRaw > maxDecodedBytes-raw {
				return nil, fmt.Errorf("c1z v3: indexed payload exceeds %d bytes: %w", maxDecodedBytes, ErrMaxSizeExceeded)
			}
			totalRaw += raw
		}
	}
	budget := newDecodedBudget(maxDecodedBytes)

	// Directories first, on one goroutine, so workers never race a
	// parent-dir creation.
	for _, e := range entries {
		if !filepath.IsLocal(filepath.FromSlash(e.Name)) {
			return nil, fmt.Errorf("c1z v3: unsafe indexed entry path: %q", e.Name)
		}
		target := filepath.Join(destDir, filepath.FromSlash(e.Name))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return nil, err
		}
		e.ExtractedPath = target
	}

	workers := runtime.GOMAXPROCS(0)
	if workers > 8 {
		workers = 8
	}
	if workers < 1 {
		workers = 1
	}
	jobs := make(chan *ReuseEntry, workers)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	var failed atomic.Bool
	setErr := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
		failed.Store(true)
	}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			dec, err := zstd.NewReader(nil,
				zstd.WithDecoderConcurrency(1),
				zstd.WithDecoderMaxMemory(maxMemoryBytes),
			)
			if err != nil {
				setErr(err)
				return
			}
			defer dec.Close()
			for e := range jobs {
				// The whole extraction fails on firstErr, so once any
				// worker has failed, drain the queue instead of decoding
				// frames whose output will be discarded.
				if failed.Load() {
					continue
				}
				if err := extractOneFrame(f, e, dec, budget); err != nil {
					setErr(fmt.Errorf("c1z v3: extract %q: %w", e.Name, err))
				}
			}
		}()
	}
	for _, e := range entries {
		jobs <- e
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	reuse := &PayloadReuse{
		srcPath: f.Name(),
		byName:  make(map[string]*ReuseEntry, len(entries)),
	}
	for _, e := range entries {
		reuse.byName[e.Name] = e
	}
	return reuse, nil
}

func extractOneFrame(f *os.File, e *ReuseEntry, dec *zstd.Decoder, budget *decodedBudget) error {
	out, err := os.OpenFile(e.ExtractedPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	// io.NewSectionReader over the shared *os.File preads the frame's
	// byte range; concurrent ReadAt is safe on macOS/Linux/Windows.
	if err := dec.Reset(io.NewSectionReader(f, e.FrameOffset, e.CompressedSize)); err != nil {
		_ = out.Close()
		return err
	}
	hasher := xxhash.New()
	n, err := io.CopyN(budgetedFrameWriter{budget: budget, out: out, hash: hasher}, dec, e.RawSize+1)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if n != e.RawSize {
		return fmt.Errorf("decoded %d bytes, header says %d", n, e.RawSize)
	}
	if hasher.Sum64() != e.XXH64 {
		return fmt.Errorf("raw bytes hash mismatch (corrupt frame)")
	}
	return nil
}

// ExtractEnvelopePayload opens, validates, and extracts a v3
// envelope's payload from f into destDir, dispatching on the
// manifest's payload encoding. For indexed payloads it decodes frames
// in parallel and returns a PayloadReuse for splice-at-save; the tar
// encodings extract sequentially and return a nil reuse.
//
// f must be positioned anywhere (it is rewound); it stays open and
// owned by the caller.
func ExtractEnvelopePayload(f *os.File, destDir string, opts ...PayloadOption) (*c1zv3.C1ZManifestV3, *PayloadReuse, error) {
	cfg := resolvePayloadOptions(opts...)
	// With no explicit budget from the caller, scale the decoded-byte
	// budget with the envelope's own size: the flat default would
	// refuse any legitimately large file (one WE wrote), while the
	// scaled budget still bounds a hostile envelope's disk consumption
	// proportionally to its actual size. The env var, when set, wins
	// inside payloadBudgetForFileSize.
	if !cfg.budgetExplicit {
		if st, err := f.Stat(); err == nil {
			cfg.maxDecodedPayloadBytes = payloadBudgetForFileSize(st.Size())
		}
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, nil, err
	}
	mb, err := readManifestBytes(f)
	if err != nil {
		return nil, nil, err
	}
	m, err := unmarshalManifestHeader(mb)
	if err != nil {
		return nil, nil, err
	}
	payloadStart := int64(len(C1Z3Magic)) + 4 + int64(len(mb))
	switch m.GetPayloadEncoding() {
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD:
		reuse, err := extractIndexedZstd(f, payloadStart, xxhash.Sum64(mb), destDir, cfg.maxDecodedPayloadBytes, cfg.maxDecoderMemoryBytes, cfg.disableSizeFailFast)
		if err != nil {
			return nil, nil, err
		}
		return m, reuse, nil
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD:
		// Draw the streaming decoder from the caller's pool only when the
		// resolved memory cap matches the pool's standard construction —
		// a pooled decoder's cap is baked in at construction, so honoring
		// a custom cap requires a fresh decoder (mirrors the v1 decoder's
		// pool-compatibility gate in pkg/dotc1z/decoder.go).
		var zr *zstd.Decoder
		usePool := cfg.pool != nil && cfg.maxDecoderMemoryBytes == decoderMaxMemoryBytes()
		if usePool {
			zr, err = cfg.pool.get(f)
		} else {
			zr, err = zstd.NewReader(f, zstd.WithDecoderMaxMemory(cfg.maxDecoderMemoryBytes))
		}
		if err != nil {
			return nil, nil, err
		}
		defer func() {
			if usePool {
				cfg.pool.put(zr)
			} else {
				zr.Close()
			}
		}()
		limited := &limitedPayloadReader{r: zr, limit: cfg.maxDecodedPayloadBytes}
		if err := ExtractZstdTar(limited, destDir); err != nil {
			return nil, nil, err
		}
		return m, nil, nil
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR:
		limited := &limitedPayloadReader{r: f, limit: cfg.maxDecodedPayloadBytes}
		if err := ExtractZstdTar(limited, destDir); err != nil {
			return nil, nil, err
		}
		return m, nil, nil
	default:
		return nil, nil, fmt.Errorf("c1z v3: unsupported payload encoding %v", m.GetPayloadEncoding())
	}
}
