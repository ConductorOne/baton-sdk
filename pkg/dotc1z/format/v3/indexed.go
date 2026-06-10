package v3

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"
	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	"github.com/klauspost/compress/zstd"
)

// PAYLOAD_ENCODING_INDEXED_ZSTD frame stream. Layout, immediately
// after the manifest:
//
//	entry    := u32 nameLen | name | u64 rawSize | u64 xxh64 | u64 compSize | <compSize zstd bytes>
//	stream   := entry* | u32 0 (terminator)
//
// All integers big-endian, matching the envelope's length prefix.
// Names are slash-separated relative paths (filepath.ToSlash on
// write, FromSlash on extract) so payloads are portable across
// macOS/Linux/Windows. xxh64 is over the RAW (decompressed) bytes.
//
// Each file is an independent zstd frame, which is what enables:
//
//   - parallel decode at open: workers pread their frame's byte range
//     (io.ReaderAt is safe for concurrent use on all three supported
//     platforms) and stream-decompress, instead of the single-stream
//     encodings' one-core sequential decode; and
//   - frame splicing at save: an unchanged payload file's compressed
//     frame is copied verbatim from the source envelope. Pebble SSTs
//     are immutable and checkpoints hard-link them, so incremental
//     rewrites (the fold compactor) reuse almost every frame and pay
//     compression only for the handful of new files.

const maxIndexedNameBytes = 4096

// indexedEntryFixedTail is rawSize + xxh64 + compSize.
const indexedEntryFixedTail = 8 + 8 + 8

// ReuseEntry describes one frame of a source envelope: where its
// compressed bytes live in the source file, what they decode to, and
// where the extractor materialized them.
type ReuseEntry struct {
	Name           string
	FrameOffset    int64 // offset of the compressed bytes in the source file
	CompressedSize int64
	RawSize        int64
	XXH64          uint64
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

// SpliceStats reports how a WriteEnvelopeWithReuse call split between
// verbatim frame copies and fresh compression. Exposed so callers can
// log reuse effectiveness in production.
type SpliceStats struct {
	SplicedFrames int
	SplicedBytes  int64 // compressed bytes copied verbatim
	EncodedFrames int
	EncodedBytes  int64 // raw bytes freshly compressed
}

// writeIndexedZstd writes the indexed frame stream for dir to w.
// w must be seekable: per-frame headers carry the compressed size and
// raw-bytes hash, which are only known after encoding, so the header
// is backpatched. The envelope save path always writes to a temp
// *os.File, satisfying this.
func writeIndexedZstd(w io.WriteSeeker, dir string, reuse *PayloadReuse) (SpliceStats, error) {
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

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return stats, err
	}
	defer enc.Close()

	var lenBuf [4]byte
	var tail [indexedEntryFixedTail]byte
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

		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(name))) //nolint:gosec // capped above.
		if _, err := w.Write(lenBuf[:]); err != nil {
			return stats, err
		}
		if _, err := io.WriteString(w, name); err != nil {
			return stats, err
		}

		if e := reuse.match(name, info, path); e != nil && srcFile != nil {
			binary.BigEndian.PutUint64(tail[0:8], uint64(e.RawSize)) //nolint:gosec // sizes validated on extract.
			binary.BigEndian.PutUint64(tail[8:16], e.XXH64)
			binary.BigEndian.PutUint64(tail[16:24], uint64(e.CompressedSize)) //nolint:gosec // sizes validated on extract.
			if _, err := w.Write(tail[:]); err != nil {
				return stats, err
			}
			if _, err := io.Copy(w, io.NewSectionReader(srcFile, e.FrameOffset, e.CompressedSize)); err != nil {
				return stats, fmt.Errorf("c1z v3: splice frame %q: %w", name, err)
			}
			stats.SplicedFrames++
			stats.SplicedBytes += e.CompressedSize
			continue
		}

		// Fresh encode: header tail is backpatched once the hash and
		// compressed size are known.
		tailPos, err := w.Seek(0, io.SeekCurrent)
		if err != nil {
			return stats, err
		}
		for i := range tail {
			tail[i] = 0
		}
		if _, err := w.Write(tail[:]); err != nil {
			return stats, err
		}
		f, err := os.Open(path)
		if err != nil {
			return stats, err
		}
		hasher := xxhash.New()
		counter := &countingW{w: w}
		enc.Reset(counter)
		rawN, err := io.Copy(io.MultiWriter(enc, hasher), f)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return stats, err
		}
		if err := enc.Close(); err != nil {
			return stats, err
		}
		end, err := w.Seek(0, io.SeekCurrent)
		if err != nil {
			return stats, err
		}
		binary.BigEndian.PutUint64(tail[0:8], uint64(rawN)) //nolint:gosec // io.Copy result is non-negative.
		binary.BigEndian.PutUint64(tail[8:16], hasher.Sum64())
		binary.BigEndian.PutUint64(tail[16:24], uint64(counter.n)) //nolint:gosec // byte count is non-negative.
		if _, err := w.Seek(tailPos, io.SeekStart); err != nil {
			return stats, err
		}
		if _, err := w.Write(tail[:]); err != nil {
			return stats, err
		}
		if _, err := w.Seek(end, io.SeekStart); err != nil {
			return stats, err
		}
		stats.EncodedFrames++
		stats.EncodedBytes += rawN
	}

	// Terminator.
	binary.BigEndian.PutUint32(lenBuf[:], 0)
	if _, err := w.Write(lenBuf[:]); err != nil {
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

// scanIndexedEntries reads the frame headers without touching frame
// bytes: one small read per entry plus a relative seek over the
// compressed data.
func scanIndexedEntries(f *os.File, payloadStart int64) ([]*ReuseEntry, error) {
	if _, err := f.Seek(payloadStart, io.SeekStart); err != nil {
		return nil, err
	}
	r := f
	var entries []*ReuseEntry
	var lenBuf [4]byte
	var tail [indexedEntryFixedTail]byte
	nameBuf := make([]byte, 0, 256)
	for {
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return nil, fmt.Errorf("%w: indexed entry header: %w", ErrEnvelopeTruncated, err)
		}
		nameLen := binary.BigEndian.Uint32(lenBuf[:])
		if nameLen == 0 {
			return entries, nil
		}
		if nameLen > maxIndexedNameBytes {
			return nil, fmt.Errorf("c1z v3: indexed entry name length %d exceeds cap", nameLen)
		}
		if cap(nameBuf) < int(nameLen) {
			nameBuf = make([]byte, nameLen)
		}
		nameBuf = nameBuf[:nameLen]
		if _, err := io.ReadFull(r, nameBuf); err != nil {
			return nil, fmt.Errorf("%w: indexed entry name: %w", ErrEnvelopeTruncated, err)
		}
		if _, err := io.ReadFull(r, tail[:]); err != nil {
			return nil, fmt.Errorf("%w: indexed entry sizes: %w", ErrEnvelopeTruncated, err)
		}
		rawSize := int64(binary.BigEndian.Uint64(tail[0:8]))    //nolint:gosec // validated below.
		compSize := int64(binary.BigEndian.Uint64(tail[16:24])) //nolint:gosec // validated below.
		if rawSize < 0 || rawSize > maxTarEntryBytes || compSize < 0 || compSize > maxTarEntryBytes {
			return nil, fmt.Errorf("c1z v3: indexed entry %q sizes out of range (raw=%d comp=%d)", nameBuf, rawSize, compSize)
		}
		offset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		entries = append(entries, &ReuseEntry{
			Name:           string(nameBuf),
			FrameOffset:    offset,
			CompressedSize: compSize,
			RawSize:        rawSize,
			XXH64:          binary.BigEndian.Uint64(tail[8:16]),
		})
		if _, err := f.Seek(compSize, io.SeekCurrent); err != nil {
			return nil, err
		}
	}
}

// extractIndexedZstd materializes an indexed payload into destDir,
// decoding frames in parallel. Returns the PayloadReuse map so a later
// save can splice unchanged frames.
func extractIndexedZstd(f *os.File, payloadStart int64, destDir string, maxDecodedBytes uint64, maxMemoryBytes uint64, disableSizeFailFast bool) (*PayloadReuse, error) {
	entries, err := scanIndexedEntries(f, payloadStart)
	if err != nil {
		return nil, err
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
	setErr := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
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
		reuse, err := extractIndexedZstd(f, payloadStart, destDir, cfg.maxDecodedPayloadBytes, cfg.maxDecoderMemoryBytes, cfg.disableSizeFailFast)
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
