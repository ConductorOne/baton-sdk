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

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	"github.com/klauspost/compress/zstd"
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

// WriteEnvelope writes a complete v3 envelope to w:
//
//  1. The 5-byte C1Z3 magic.
//  2. A uint32 BE length prefix for the marshaled manifest.
//  3. The marshaled manifest bytes.
//  4. The zstd-compressed tar of payloadDir.
//
// payloadDir is walked in sorted lexical order; file mtimes are NOT
// normalized (the RFC documents tar as not byte-stable). w is typically
// a *os.File created via os.CreateTemp in the same directory as the
// final destination so an atomic rename can finalize the write.
func WriteEnvelope(w io.Writer, m *c1zv3.C1ZManifestV3, payloadDir string) error {
	if m == nil {
		return errors.New("c1z v3: WriteEnvelope: nil manifest")
	}
	if _, err := w.Write(C1Z3Magic); err != nil {
		return fmt.Errorf("c1z v3: write magic: %w", err)
	}
	mb, err := MarshalManifest(m)
	if err != nil {
		return fmt.Errorf("c1z v3: marshal manifest: %w", err)
	}
	if len(mb) > maxManifestBytes {
		return fmt.Errorf("c1z v3: manifest is %d bytes, exceeds %d", len(mb), maxManifestBytes)
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(mb))) //nolint:gosec // len(mb) is capped at maxManifestBytes above.
	if _, err := w.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("c1z v3: write manifest length: %w", err)
	}
	if _, err := w.Write(mb); err != nil {
		return fmt.Errorf("c1z v3: write manifest: %w", err)
	}
	if err := writeZstdTar(w, payloadDir); err != nil {
		return fmt.Errorf("c1z v3: write payload: %w", err)
	}
	return nil
}

// Envelope is the parsed result of ReadEnvelope. Manifest holds the
// decoded manifest; PayloadReader yields the zstd-tar payload bytes
// (or whatever PayloadEncoding the manifest declared). Callers must
// Close the envelope when done.
type Envelope struct {
	Manifest      *c1zv3.C1ZManifestV3
	PayloadReader io.Reader

	zstdReader *zstd.Decoder
}

// Close releases any pooled decoder resources held by PayloadReader.
func (e *Envelope) Close() error {
	if e.zstdReader != nil {
		e.zstdReader.Close()
		e.zstdReader = nil
	}
	return nil
}

// ReadEnvelope reads a v3 envelope from r. r must be positioned at the
// start of the file (the C1Z3 magic). Returns an Envelope whose
// PayloadReader streams the uncompressed tar bytes when the manifest
// declares PAYLOAD_ENCODING_ZSTD_TAR, or the raw bytes for
// PAYLOAD_ENCODING_RAW / PAYLOAD_ENCODING_ZSTD.
func ReadEnvelope(r io.Reader) (*Envelope, error) {
	// 1. Magic.
	magic := make([]byte, len(C1Z3Magic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, fmt.Errorf("%w: reading magic: %w", ErrEnvelopeTruncated, err)
	}
	if !bytes.Equal(magic, C1Z3Magic) {
		return nil, ErrInvalidV3Magic
	}
	// 2. Manifest length.
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("%w: reading manifest length: %w", ErrEnvelopeTruncated, err)
	}
	mlen := binary.BigEndian.Uint32(lenBuf[:])
	if mlen > maxManifestBytes {
		return nil, fmt.Errorf("c1z v3: manifest claims %d bytes, exceeds cap %d", mlen, maxManifestBytes)
	}
	// 3. Manifest bytes.
	mb := make([]byte, mlen)
	if _, err := io.ReadFull(r, mb); err != nil {
		return nil, fmt.Errorf("%w: reading manifest: %w", ErrEnvelopeTruncated, err)
	}
	m, err := UnmarshalManifest(mb)
	if err != nil {
		return nil, err
	}
	// 4. Payload. The reader is positioned at the first payload byte.
	env := &Envelope{Manifest: m}
	switch m.GetPayloadEncoding() {
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_RAW:
		env.PayloadReader = r
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_ZSTD,
		c1zv3.PayloadEncoding_PAYLOAD_ENCODING_ZSTD_TAR:
		zr, err := zstd.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("c1z v3: zstd reader: %w", err)
		}
		env.zstdReader = zr
		env.PayloadReader = zr
	default:
		return nil, fmt.Errorf("c1z v3: unsupported payload encoding %v", m.GetPayloadEncoding())
	}
	return env, nil
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
			f, err := os.Open(path)
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

// ExtractZstdTar reads a zstd-tar payload stream from r and unpacks
// it into destDir. destDir must exist. Used by the engine to
// rematerialize a Pebble directory at open time.
func ExtractZstdTar(r io.Reader, destDir string) error {
	// r already came through Envelope's zstd decoder; we just need
	// to walk the tar entries.
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("c1z v3: tar Next: %w", err)
		}
		if !filepath.IsLocal(hdr.Name) {
			return fmt.Errorf("c1z v3: unsafe tar entry path: %q", hdr.Name)
		}
		target := filepath.Join(destDir, hdr.Name) //nolint:gosec // hdr.Name is guarded by filepath.IsLocal above.
		switch hdr.Typeflag {
		case tar.TypeDir:
			mode, err := tarFileMode(hdr.Mode, 0o755)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(target, mode); err != nil {
				return err
			}
		case tar.TypeReg:
			if hdr.Size < 0 || hdr.Size > maxTarEntryBytes {
				return fmt.Errorf("c1z v3: tar entry %q size %d exceeds cap %d", hdr.Name, hdr.Size, maxTarEntryBytes)
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			mode, err := tarFileMode(hdr.Mode, 0o644)
			if err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
			if err != nil {
				return err
			}
			n, err := io.Copy(f, io.LimitReader(tr, hdr.Size))
			if closeErr := f.Close(); err == nil {
				err = closeErr
			}
			if err != nil {
				return err
			}
			if n != hdr.Size {
				return io.ErrUnexpectedEOF
			}
		default:
			// Skip other types (symlinks, etc.) — Pebble directories
			// contain only directories and regular files.
		}
	}
}

func tarFileMode(mode int64, mask os.FileMode) (os.FileMode, error) {
	if mode < 0 || mode > 0o777 {
		return 0, fmt.Errorf("c1z v3: unsafe tar mode %d", mode)
	}
	return os.FileMode(mode) & mask, nil
}
