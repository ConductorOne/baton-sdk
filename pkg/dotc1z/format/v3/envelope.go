package v3

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"
	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// C1Z3Magic is the 5-byte header that identifies a v3 c1z file. Same
// value as dotc1z.C1Z3FileHeader; duplicated here so the format package
// doesn't import its parent (it's the layer below).
var C1Z3Magic = []byte("C1Z3\x00")

// ErrInvalidV3Magic is returned by ReadEnvelope when the first 5 bytes
// don't match C1Z3Magic. Callers that want to dispatch across v1 and v3
// should use dotc1z.ReadHeaderFormat (which delegates to this package
// for the v3 detection only after a v1/v3 magic-byte choice).
var ErrInvalidV3Magic = errors.New("c1z v3: invalid magic; not a v3 file")

// ErrEnvelopeTruncated is returned when the reader hits EOF mid-header
// or mid-manifest. This is a corruption-class error; the C1 corruption
// classifier should auto-invalidate the live c1z and trigger a re-sync.
var ErrEnvelopeTruncated = errors.New("c1z v3: envelope truncated")

// Maximum manifest size we accept on read. Real manifests are ~50-200 KiB
// (mostly the descriptor closure). A 16 MiB cap is far above legitimate
// usage and protects against a malicious file claiming a billion-byte
// manifest length.
const maxManifestBytes = 16 << 20
const maxTarEntryBytes int64 = 4 << 30

// Tar entries larger than this are streamed straight to disk on the
// reader goroutine instead of being buffered in memory for the writer
// worker pool. Pebble's typical 2 MiB FlushSplitBytes keeps the common
// case well below this, preserving the parallel-write win while
// bounding peak extraction memory at roughly
// (extractWorkerCount + channel buffer) × inlineCopyThresholdBytes.
const inlineCopyThresholdBytes int64 = 8 << 20

// Payload decode limits. Defaults and env var names mirror the v1
// decoder in pkg/dotc1z/decoder.go so operators tune one knob for both
// formats. Duplicated here (like C1Z3Magic) because this package is the
// layer below dotc1z and cannot import it.
const (
	defaultMaxDecodedPayloadBytes uint64 = 10 << 30  // 10 GiB
	defaultDecoderMaxMemoryBytes  uint64 = 128 << 20 // 128 MiB zstd window cap
	maxDecodedSizeEnvVar                 = "BATON_DECODER_MAX_DECODED_SIZE_MB"
	maxDecoderMemorySizeEnvVar           = "BATON_DECODER_MAX_MEMORY_MB"
	fcsFailFastDisableEnvVar             = "BATON_DISABLE_FCS_FAIL_FAST"
)

var fcsFailFastDisabled = os.Getenv(fcsFailFastDisableEnvVar) == "1"

// ErrMaxSizeExceeded is returned (wrapped) when a payload decodes to
// more bytes than the configured cap. Guards against decompression
// bombs in untrusted v3 c1z files.
var ErrMaxSizeExceeded = fmt.Errorf("c1z v3: max decoded payload size exceeded, increase via the %s environment variable", maxDecodedSizeEnvVar)

// envSizeBytes reads an env var holding a size in MiB and converts it
// to bytes, falling back to def when unset, unparsable, zero, or large
// enough to overflow the MiB→bytes conversion.
func envSizeBytes(envVar string, def uint64) uint64 {
	v := os.Getenv(envVar)
	if v == "" {
		return def
	}
	mb, err := strconv.ParseUint(v, 10, 64)
	if err != nil || mb == 0 || mb > (1<<63)>>20 {
		return def
	}
	return mb << 20
}

func maxDecodedPayloadBytes() uint64 {
	return envSizeBytes(maxDecodedSizeEnvVar, defaultMaxDecodedPayloadBytes)
}

func decoderMaxMemoryBytes() uint64 {
	return envSizeBytes(maxDecoderMemorySizeEnvVar, defaultDecoderMaxMemoryBytes)
}

type payloadOptions struct {
	maxDecodedPayloadBytes uint64
	maxDecoderMemoryBytes  uint64
	disableSizeFailFast    bool
	pool                   *DecoderPool
}

type PayloadOption func(*payloadOptions)

// WithMaxDecodedPayloadBytes sets the decoded payload byte cap for v3 payload
// extraction. A zero value means use BATON_DECODER_MAX_DECODED_SIZE_MB or the
// built-in default.
func WithMaxDecodedPayloadBytes(n uint64) PayloadOption {
	return func(o *payloadOptions) {
		o.maxDecodedPayloadBytes = n
	}
}

// WithMaxDecoderMemoryBytes sets the zstd decoder memory cap for v3 payload
// extraction. A zero value means use BATON_DECODER_MAX_MEMORY_MB or the
// built-in default.
func WithMaxDecoderMemoryBytes(n uint64) PayloadOption {
	return func(o *payloadOptions) {
		o.maxDecoderMemoryBytes = n
	}
}

// WithPayloadDecoderPool scopes zstd payload-decoder reuse to the caller's
// pool for the tar_zstd encoding (the single-stream decode that dominates
// when many envelopes are opened in a loop, e.g. compaction source opens).
//
// The pool only engages when the resolved decoder memory cap equals the
// pool's standard construction cap: a pooled decoder's WithDecoderMaxMemory
// is baked in at construction, so a caller-specific cap must construct a
// fresh decoder to be honored. The indexed encoding never draws from the
// pool — its parallel frame workers use cheap concurrency-1 decoders whose
// settings don't match the pool's. Nil is valid and means no reuse.
func WithPayloadDecoderPool(pool *DecoderPool) PayloadOption {
	return func(o *payloadOptions) {
		o.pool = pool
	}
}

func resolvePayloadOptions(opts ...PayloadOption) payloadOptions {
	out := payloadOptions{
		maxDecodedPayloadBytes: maxDecodedPayloadBytes(),
		maxDecoderMemoryBytes:  decoderMaxMemoryBytes(),
		disableSizeFailFast:    fcsFailFastDisabled,
	}
	for _, opt := range opts {
		opt(&out)
	}
	if out.maxDecodedPayloadBytes == 0 {
		out.maxDecodedPayloadBytes = maxDecodedPayloadBytes()
	}
	if out.maxDecoderMemoryBytes == 0 {
		out.maxDecoderMemoryBytes = decoderMaxMemoryBytes()
	}
	return out
}

// limitedPayloadReader passes through up to limit bytes and then fails
// with ErrMaxSizeExceeded. Unlike io.LimitReader, exceeding the budget
// is an error, not a silent EOF — a truncated tar stream must not look
// like a well-formed short one.
type limitedPayloadReader struct {
	r     io.Reader
	read  uint64
	limit uint64
}

func (l *limitedPayloadReader) Read(p []byte) (int, error) {
	if l.read > l.limit {
		return 0, l.limitErr()
	}
	n, err := l.r.Read(p)
	//nolint:gosec // n is always >= 0.
	l.read += uint64(n)
	// Clip bytes that crossed the cap in this single Read so callers
	// never see data past the budget; exactly-limit payloads still
	// succeed and reach EOF normally.
	if l.read > l.limit {
		over := l.read - l.limit
		//nolint:gosec // over <= n by construction (we just added n and went over).
		if uint64(n) >= over {
			n -= int(over)
		} else {
			n = 0
		}
		return n, l.limitErr()
	}
	return n, err
}

func (l *limitedPayloadReader) limitErr() error {
	return fmt.Errorf("c1z v3: payload exceeds %d bytes: %w", l.limit, ErrMaxSizeExceeded)
}

// WriteEnvelope writes a complete v3 envelope to w:
//
//  1. The 5-byte C1Z3 magic.
//  2. A uint32 BE length prefix for the marshaled manifest.
//  3. The marshaled manifest bytes.
//  4. The payload, per the manifest's PayloadEncoding.
//
// The manifest's PayloadEncoding field selects the payload format:
//
//   - PAYLOAD_ENCODING_TAR_ZSTD (1): default; tar then zstd. The
//     manifest can leave PayloadEncoding as the zero value
//     (UNSPECIFIED) and WriteEnvelope will write TAR_ZSTD and patch
//     the manifest in place so the reader sees the same value.
//   - PAYLOAD_ENCODING_TAR (2): uncompressed tar.
//   - PAYLOAD_ENCODING_INDEXED_ZSTD (5): per-file zstd frames with a
//     trailing frame index (see indexed.go for the layout).
//   - PAYLOAD_ENCODING_UNSPECIFIED (0): treated as TAR_ZSTD.
//
// Any other value (including the reserved 3 and 4) returns an error
// before any bytes are written to w.
//
// payloadDir is walked in sorted lexical order; file mtimes are NOT
// normalized (the RFC documents tar as not byte-stable). w is typically
// a *os.File created via os.CreateTemp in the same directory as the
// final destination so an atomic rename can finalize the write.
func WriteEnvelope(w io.Writer, m *c1zv3.C1ZManifestV3, payloadDir string) error {
	_, err := WriteEnvelopeWithReuse(w, m, payloadDir, nil)
	return err
}

// WriteEnvelopeWithReuse is WriteEnvelope plus frame splicing for the
// INDEXED_ZSTD encoding: payload files proven byte-identical to a
// frame in reuse's source envelope are copied as compressed bytes
// instead of re-encoded. reuse is ignored (and stats zero) for the tar
// encodings. Every encoding writes in a single pass, so w may be any
// io.Writer — including a non-seekable network sink.
func WriteEnvelopeWithReuse(w io.Writer, m *c1zv3.C1ZManifestV3, payloadDir string, reuse *PayloadReuse) (SpliceStats, error) {
	var stats SpliceStats
	if m == nil {
		return stats, errors.New("c1z v3: WriteEnvelope: nil manifest")
	}

	// Resolve the encoding. Validate before we write any bytes so a
	// bad PayloadEncoding doesn't leave a partial file on disk.
	enc := m.GetPayloadEncoding()
	if enc == c1zv3.PayloadEncoding_PAYLOAD_ENCODING_UNSPECIFIED {
		enc = c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD
		m.SetPayloadEncoding(enc)
	}
	switch enc {
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD,
		c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR,
		c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD:
		// ok
	default:
		return stats, fmt.Errorf("c1z v3: WriteEnvelope: unsupported payload encoding %v", enc)
	}

	if _, err := w.Write(C1Z3Magic); err != nil {
		return stats, fmt.Errorf("c1z v3: write magic: %w", err)
	}
	mb, err := MarshalManifest(m)
	if err != nil {
		return stats, fmt.Errorf("c1z v3: marshal manifest: %w", err)
	}
	if len(mb) > maxManifestBytes {
		return stats, fmt.Errorf("c1z v3: manifest is %d bytes, exceeds %d", len(mb), maxManifestBytes)
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(mb))) //nolint:gosec // len(mb) is capped at maxManifestBytes above.
	if _, err := w.Write(lenBuf[:]); err != nil {
		return stats, fmt.Errorf("c1z v3: write manifest length: %w", err)
	}
	if _, err := w.Write(mb); err != nil {
		return stats, fmt.Errorf("c1z v3: write manifest: %w", err)
	}
	switch enc {
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD:
		if err := writeZstdTar(w, payloadDir); err != nil {
			return stats, fmt.Errorf("c1z v3: write tar_zstd payload: %w", err)
		}
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR:
		if err := writeTar(w, payloadDir); err != nil {
			return stats, fmt.Errorf("c1z v3: write tar payload: %w", err)
		}
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD:
		payloadStart := int64(len(C1Z3Magic)) + 4 + int64(len(mb))
		stats, err = writeIndexedZstd(w, payloadStart, xxhash.Sum64(mb), payloadDir, reuse)
		if err != nil {
			return stats, fmt.Errorf("c1z v3: write indexed_zstd payload: %w", err)
		}
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_UNSPECIFIED:
		// Unreachable: the validation block above normalises
		// UNSPECIFIED to TAR_ZSTD before this point.
		return stats, fmt.Errorf("c1z v3: WriteEnvelope: payload encoding was not resolved")
	}
	return stats, nil
}

// Envelope is the parsed result of ReadEnvelope. Manifest holds the
// decoded manifest; PayloadReader yields the zstd-tar payload bytes
// (or whatever PayloadEncoding the manifest declared). Callers must
// Close the envelope when done.
type Envelope struct {
	Manifest      *c1zv3.C1ZManifestV3
	PayloadReader io.Reader

	zstdReader *zstd.Decoder
	pool       *DecoderPool
}

// Close returns the payload decoder to the envelope's DecoderPool (when
// one was supplied) or destroys it.
func (e *Envelope) Close() error {
	if e.zstdReader != nil {
		if e.pool != nil {
			e.pool.put(e.zstdReader)
		} else {
			e.zstdReader.Close()
		}
		e.zstdReader = nil
		e.pool = nil
	}
	return nil
}

// DecoderPool reuses zstd payload decoders across envelope reads. A
// decoder retains its window/history buffers across Reset, so reuse
// avoids both the construction cost (worker goroutine spin-up, buffer
// allocation) and the per-open garbage when many envelopes are read in
// a loop — the compactor opens one envelope per source per merge.
//
// The pool is deliberately NOT process-global: a pooled decoder keeps
// whatever buffers its largest stream grew, so the owner scopes the
// pool to the operation that benefits (one pool per compaction) and
// calls Close when done, releasing everything deterministically
// instead of waiting out sync.Pool's GC-paced eviction in a long-lived
// worker process. Callers that open a single envelope pass a nil pool
// and get a one-shot decoder destroyed at Envelope.Close.
type DecoderPool struct {
	mu     sync.Mutex
	idle   []*zstd.Decoder
	closed bool
}

// NewDecoderPool returns an empty pool. The caller owns its lifetime
// and must call Close to release idle decoders.
func NewDecoderPool() *DecoderPool {
	return &DecoderPool{}
}

// get returns an idle decoder reset onto r, or constructs one (with the
// standard decoder memory cap). Nil-receiver safe: a nil pool always
// constructs, and the Envelope destroys the decoder at Close.
func (p *DecoderPool) get(r io.Reader) (*zstd.Decoder, error) {
	if p != nil {
		p.mu.Lock()
		if n := len(p.idle); n > 0 {
			dec := p.idle[n-1]
			p.idle = p.idle[:n-1]
			p.mu.Unlock()
			if err := dec.Reset(r); err != nil {
				dec.Close()
				return nil, err
			}
			return dec, nil
		}
		p.mu.Unlock()
	}
	return zstd.NewReader(r, zstd.WithDecoderMaxMemory(decoderMaxMemoryBytes()))
}

// put detaches dec from its reader and parks it for reuse. Decoders
// returned after Close (an envelope outliving the pool) are destroyed
// rather than leaked into a closed pool.
func (p *DecoderPool) put(dec *zstd.Decoder) {
	if dec == nil {
		return
	}
	if p == nil {
		dec.Close()
		return
	}
	if err := dec.Reset(nil); err != nil {
		dec.Close()
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		dec.Close()
		return
	}
	p.idle = append(p.idle, dec)
	p.mu.Unlock()
}

// Close releases every idle decoder and marks the pool closed; later
// puts destroy their decoders. Safe to call on a nil pool.
func (p *DecoderPool) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	idle := p.idle
	p.idle = nil
	p.closed = true
	p.mu.Unlock()
	for _, dec := range idle {
		dec.Close()
	}
}

// ReadEnvelope reads a v3 envelope from r. r must be positioned at the
// start of the file (the C1Z3 magic). Returns an Envelope whose
// PayloadReader streams the uncompressed tar bytes for both
// PAYLOAD_ENCODING_TAR_ZSTD (transparently decoding zstd first) and
// PAYLOAD_ENCODING_TAR (passing through unchanged). For
// PAYLOAD_ENCODING_INDEXED_ZSTD the payload is not a tar stream and
// PayloadReader is nil — use ExtractEnvelopePayload (random access
// over the frame table) or ReadIndexedFrameIndex instead. The
// reserved values 3 and 4 return an error.
//
// The payload is treated as untrusted: the zstd decoder's window
// memory is capped (BATON_DECODER_MAX_MEMORY_MB, default 128 MiB) and
// PayloadReader fails with ErrMaxSizeExceeded once the decoded payload
// exceeds the configured budget (BATON_DECODER_MAX_DECODED_SIZE_MB,
// default 10 GiB) — the same knobs the v1 decoder honors.
func ReadEnvelope(r io.Reader) (*Envelope, error) {
	return readEnvelope(r, false, nil)
}

// ReadEnvelopeHeader is the low-allocation open path for engine dispatch.
// It parses only the cheap manifest fields (engine, engine_schema_version,
// payload_encoding, sync_runs) and skips the descriptor closure. Call
// ReadEnvelope when callers need the full self-describing descriptor set.
func ReadEnvelopeHeader(r io.Reader) (*Envelope, error) {
	return readEnvelope(r, true, nil)
}

// ReadEnvelopeHeaderWithPool is ReadEnvelopeHeader with a caller-scoped
// payload decoder pool: the envelope draws its zstd decoder from pool
// and returns it on Close instead of destroying it. Used by callers
// that open many envelopes in a loop (compaction source opens). A nil
// pool behaves exactly like ReadEnvelopeHeader.
func ReadEnvelopeHeaderWithPool(r io.Reader, pool *DecoderPool) (*Envelope, error) {
	return readEnvelope(r, true, pool)
}

// ReadManifestHeader parses only the cheap manifest fields and stops
// before the payload: no zstd decoder is constructed and not a single
// payload byte is read. This is the "shallow unpack" path for callers
// that want envelope metadata — sync run summaries with their cached
// stats — without rematerializing the Pebble directory (compaction
// source selection, overlay bucket planning, tooling).
func ReadManifestHeader(r io.Reader) (*c1zv3.C1ZManifestV3, error) {
	mb, err := readManifestBytes(r)
	if err != nil {
		return nil, err
	}
	return unmarshalManifestHeader(mb)
}

// readManifestBytes consumes the magic, the manifest length prefix, and
// the manifest bytes, leaving r positioned at the first payload byte.
func readManifestBytes(r io.Reader) ([]byte, error) {
	magic := make([]byte, len(C1Z3Magic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, fmt.Errorf("%w: reading magic: %w", ErrEnvelopeTruncated, err)
	}
	if !bytes.Equal(magic, C1Z3Magic) {
		return nil, ErrInvalidV3Magic
	}
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("%w: reading manifest length: %w", ErrEnvelopeTruncated, err)
	}
	mlen := binary.BigEndian.Uint32(lenBuf[:])
	if mlen > maxManifestBytes {
		return nil, fmt.Errorf("c1z v3: manifest claims %d bytes, exceeds cap %d", mlen, maxManifestBytes)
	}
	mb := make([]byte, mlen)
	if _, err := io.ReadFull(r, mb); err != nil {
		return nil, fmt.Errorf("%w: reading manifest: %w", ErrEnvelopeTruncated, err)
	}
	return mb, nil
}

func readEnvelope(r io.Reader, headerOnly bool, pool *DecoderPool) (*Envelope, error) {
	mb, err := readManifestBytes(r)
	if err != nil {
		return nil, err
	}
	var m *c1zv3.C1ZManifestV3
	if headerOnly {
		m, err = unmarshalManifestHeader(mb)
	} else {
		m, err = UnmarshalManifest(mb)
	}
	if err != nil {
		return nil, err
	}
	// 4. Payload. The reader is positioned at the first payload byte.
	env := &Envelope{Manifest: m}
	switch m.GetPayloadEncoding() {
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD:
		zr, err := pool.get(r)
		if err != nil {
			return nil, fmt.Errorf("c1z v3: zstd reader: %w", err)
		}
		env.zstdReader = zr
		env.pool = pool
		env.PayloadReader = &limitedPayloadReader{r: zr, limit: maxDecodedPayloadBytes()}
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR:
		env.PayloadReader = &limitedPayloadReader{r: r, limit: maxDecodedPayloadBytes()}
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD:
		// Indexed payloads are not a tar stream; extraction goes
		// through ExtractEnvelopePayload (random access over the
		// frame table). PayloadReader stays nil.
	default:
		return nil, fmt.Errorf("c1z v3: unsupported payload encoding %v", m.GetPayloadEncoding())
	}
	return env, nil
}

// unmarshalManifestHeader decodes the cheap manifest fields by hand:
// engine (1), engine_schema_version (2), payload_encoding (4), and the
// sync_runs projection (40). The descriptor closure (field 10) — by far
// the largest field — is skipped, which is what makes header reads
// cheap enough for engine dispatch on every open. Sync run summaries
// are small and few (bounded by the sync retention limit), so decoding
// them here costs a handful of allocations.
func unmarshalManifestHeader(b []byte) (*c1zv3.C1ZManifestV3, error) {
	out := &c1zv3.C1ZManifestV3{}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		b = b[n:]
		switch num {
		case 1:
			if typ != protowire.BytesType {
				return nil, fmt.Errorf("c1z v3: manifest engine has wire type %v", typ)
			}
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			out.SetEngine(v)
			b = b[n:]
		case 2:
			if typ != protowire.VarintType {
				return nil, fmt.Errorf("c1z v3: manifest engine_schema_version has wire type %v", typ)
			}
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			if v > uint64(^uint32(0)) {
				return nil, fmt.Errorf("c1z v3: manifest engine_schema_version overflow: %d", v)
			}
			out.SetEngineSchemaVersion(uint32(v))
			b = b[n:]
		case 4:
			if typ != protowire.VarintType {
				return nil, fmt.Errorf("c1z v3: manifest payload_encoding has wire type %v", typ)
			}
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			if v > uint64(1<<31-1) {
				return nil, fmt.Errorf("c1z v3: manifest payload_encoding overflow: %d", v)
			}
			out.SetPayloadEncoding(c1zv3.PayloadEncoding(v))
			b = b[n:]
		case 40:
			if typ != protowire.BytesType {
				return nil, fmt.Errorf("c1z v3: manifest sync_runs has wire type %v", typ)
			}
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			summary := &c1zv3.SyncRunSummary{}
			if err := proto.Unmarshal(v, summary); err != nil {
				return nil, fmt.Errorf("%w: sync_runs entry: %w", ErrManifestInvalid, err)
			}
			out.SetSyncRuns(append(out.GetSyncRuns(), summary))
			b = b[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return out, nil
}

// writeZstdTar walks dir in sorted order, writing each entry into a
// tar stream that is itself zstd-compressed and written to w.
func writeZstdTar(w io.Writer, dir string) error {
	zw, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return err
	}
	tw := tar.NewWriter(zw)

	walkErr := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = rel
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			f, err := os.Open(path) // #nosec G122 -- path is from filepath.WalkDir over a Pebble checkpoint directory we own.
			if err != nil {
				return err
			}
			_, err = io.Copy(tw, f)
			if closeErr := f.Close(); err == nil {
				err = closeErr
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	if walkErr != nil {
		if err := tw.Close(); err != nil {
			walkErr = errors.Join(walkErr, fmt.Errorf("close tar writer: %w", err))
		}
		if err := zw.Close(); err != nil {
			walkErr = errors.Join(walkErr, fmt.Errorf("close zstd writer: %w", err))
		}
		return walkErr
	}
	if err := tw.Close(); err != nil {
		if closeErr := zw.Close(); closeErr != nil {
			return errors.Join(fmt.Errorf("close tar writer: %w", err), fmt.Errorf("close zstd writer: %w", closeErr))
		}
		return fmt.Errorf("close tar writer: %w", err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("close zstd writer: %w", err)
	}
	return nil
}

// writeTar walks dir in sorted order, writing each entry directly to
// w as a tar stream (no compression). Used when PayloadEncoding is
// PAYLOAD_ENCODING_TAR. The shape mirrors writeZstdTar so any future
// refactor can lift the common walk-and-emit body into a helper.
func writeTar(w io.Writer, dir string) error {
	tw := tar.NewWriter(w)

	walkErr := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = rel
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			f, err := os.Open(path) // #nosec G122 -- path is from filepath.Walk over a Pebble checkpoint directory we own.
			if err != nil {
				return err
			}
			_, err = io.Copy(tw, f)
			if closeErr := f.Close(); err == nil {
				err = closeErr
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	if walkErr != nil {
		if err := tw.Close(); err != nil {
			walkErr = errors.Join(walkErr, fmt.Errorf("close tar writer: %w", err))
		}
		return walkErr
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("close tar writer: %w", err)
	}
	return nil
}

// ExtractZstdTar reads a zstd-tar payload stream from r and unpacks
// it into destDir. destDir must exist. Used by the engine to
// rematerialize a Pebble directory at open time.
//
// Despite the name, the function accepts ANY tar stream — the
// Envelope's payload reader has already transparently decoded the
// outer zstd layer when the encoding was TAR_ZSTD, or is a plain tar
// reader when the encoding was TAR. Same downstream code path.
//
// Parallelism: the tar reader pulls bytes from the underlying stream
// serially in this goroutine (tar framing is sequential). Regular-file
// entries up to inlineCopyThresholdBytes are read into a
// freshly-allocated buffer and dispatched (target, mode, buffer) to a
// writer worker pool; workers perform the per-file open/write/close
// syscalls in parallel. Larger entries are streamed straight to disk
// on this goroutine so a hostile archive full of multi-GiB entries
// can't drive memory to extractWorkerCount × maxTarEntryBytes. Memory
// peak is bounded by (extractWorkerCount + channel buffer) ×
// inlineCopyThresholdBytes; at Pebble's typical 2 MiB FlushSplitBytes
// nearly every entry takes the parallel path — the per-entry
// parallelism win compounds at production-scale c1z files (100s GB).
//
// Aggregate extraction is bounded too: when r is an Envelope's
// PayloadReader, the decoded-byte budget (file contents AND tar
// headers, so entry count as well) fails the extraction with
// ErrMaxSizeExceeded once exceeded.
//
// Directory creation stays on the main goroutine because tar entries
// are emitted in walk order — a TypeDir must finish before a TypeReg
// child can be written.
func ExtractZstdTar(r io.Reader, destDir string) error {
	const extractWorkerCount = 4

	type writeJob struct {
		target string
		mode   os.FileMode
		data   []byte
	}
	jobs := make(chan writeJob, extractWorkerCount)

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
	wg.Add(extractWorkerCount)
	for w := 0; w < extractWorkerCount; w++ {
		go func() {
			defer wg.Done()
			for j := range jobs {
				f, err := os.OpenFile(j.target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, j.mode)
				if err != nil {
					setErr(err)
					continue
				}
				_, werr := f.Write(j.data)
				if cerr := f.Close(); werr == nil {
					werr = cerr
				}
				if werr != nil {
					setErr(werr)
				}
			}
		}()
	}

	tr := tar.NewReader(r)
	var readErr error
entryLoop:
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			readErr = fmt.Errorf("c1z v3: tar Next: %w", err)
			break
		}
		if !filepath.IsLocal(hdr.Name) {
			readErr = fmt.Errorf("c1z v3: unsafe tar entry path: %q", hdr.Name)
			break
		}
		target := filepath.Join(destDir, hdr.Name) //nolint:gosec // hdr.Name is guarded by filepath.IsLocal above.
		switch hdr.Typeflag {
		case tar.TypeDir:
			mode, err := tarFileMode(hdr.Mode, 0o755)
			if err != nil {
				readErr = err
				break entryLoop
			}
			if err := os.MkdirAll(target, mode); err != nil {
				readErr = err
				break entryLoop
			}
		case tar.TypeReg:
			if hdr.Size < 0 || hdr.Size > maxTarEntryBytes {
				readErr = fmt.Errorf("c1z v3: tar entry %q size %d exceeds cap %d", hdr.Name, hdr.Size, maxTarEntryBytes)
				break entryLoop
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				readErr = err
				break entryLoop
			}
			mode, err := tarFileMode(hdr.Mode, 0o644)
			if err != nil {
				readErr = err
				break entryLoop
			}
			if hdr.Size > inlineCopyThresholdBytes {
				if err := streamTarEntry(tr, target, mode); err != nil {
					readErr = fmt.Errorf("c1z v3: tar stream %q: %w", hdr.Name, err)
					break entryLoop
				}
				continue
			}
			buf := make([]byte, hdr.Size)
			if _, err := io.ReadFull(tr, buf); err != nil {
				readErr = fmt.Errorf("c1z v3: tar read %q: %w", hdr.Name, err)
				break entryLoop
			}
			jobs <- writeJob{target: target, mode: mode, data: buf}
		default:
			// Skip other types (symlinks, etc.) — Pebble directories
			// contain only directories and regular files.
		}
	}
	close(jobs)
	wg.Wait()
	if readErr != nil {
		return readErr
	}
	return firstErr
}

// streamTarEntry copies one large tar entry from tr to target without
// buffering it in memory. The tar reader bounds the copy at the entry's
// declared size.
func streamTarEntry(tr *tar.Reader, target string, mode os.FileMode) error {
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, tr)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func tarFileMode(mode int64, mask os.FileMode) (os.FileMode, error) {
	if mode < 0 || mode > 0o777 {
		return 0, fmt.Errorf("c1z v3: unsafe tar mode %d", mode)
	}
	return os.FileMode(mode) & mask, nil
}
