package codec

import (
	"bytes"
	"encoding/binary"
	"strings"
)

// Tuple encoding for v3 storage keys. Mirrors FoundationDB's tuple
// layer — every encoded element is self-delimiting so concatenation
// preserves the lexicographic ordering of the natural element order.
//
// Escape rules for string and bytes (the only place 0x00 / 0x01 can
// appear in untrusted input):
//
//	0x00 -> 0x01 0x01
//	0x01 -> 0x01 0x02
//
// Separator between elements: a single 0x00 byte. The escape rules
// guarantee no element's encoded bytes contain a bare 0x00, so the
// separator is unambiguous.
//
// Property-tested in codec_test.go over a 40k-pair grid including
// embedded NUL, all-NUL components, and binary edge cases.

const (
	tupleSeparator byte = 0x00
	tupleEscape    byte = 0x01
	escapedNUL     byte = 0x01 // 0x01 0x01
	escapedEscape  byte = 0x02 // 0x01 0x02
)

// AppendTupleString writes a tuple-encoded string component (no
// trailing separator). The caller is responsible for emitting the
// separator between successive components.
func AppendTupleString(dst []byte, s string) []byte {
	return appendEscapedString(dst, s)
}

// AppendTupleBytes writes a tuple-encoded raw-bytes component. Same
// escape rules as strings — needed because some connectors emit
// external IDs as opaque bytes that may contain embedded NUL.
func AppendTupleBytes(dst []byte, b []byte) []byte {
	return appendEscaped(dst, b)
}

func appendEscaped(dst []byte, src []byte) []byte {
	// Fast path: real-world ids almost never contain 0x00/0x01, and
	// SIMD IndexByte beats a byte-at-a-time loop by an order of
	// magnitude. Profiled at ~4% of total sqlite→pebble conversion CPU
	// before this (every key component of every record and index entry
	// passes through here).
	for len(src) > 0 {
		i := bytes.IndexByte(src, tupleSeparator)
		if j := bytes.IndexByte(src, tupleEscape); j >= 0 && (i < 0 || j < i) {
			i = j
		}
		if i < 0 {
			return append(dst, src...)
		}
		dst = append(dst, src[:i]...)
		if src[i] == tupleSeparator {
			dst = append(dst, tupleEscape, escapedNUL)
		} else {
			dst = append(dst, tupleEscape, escapedEscape)
		}
		src = src[i+1:]
	}
	return dst
}

// appendEscapedString is appendEscaped for a string source. It is
// byte-for-byte identical to appendEscaped([]byte(src)) but avoids
// allocating a []byte copy of the string — append and strings.IndexByte
// both operate on the string directly. Key encoding is the hottest
// allocator in the engine (every component of every record and index
// key flows through here), so the string callers route through this to
// skip the per-component copy.
func appendEscapedString(dst []byte, src string) []byte {
	for len(src) > 0 {
		i := strings.IndexByte(src, tupleSeparator)
		if j := strings.IndexByte(src, tupleEscape); j >= 0 && (i < 0 || j < i) {
			i = j
		}
		if i < 0 {
			return append(dst, src...)
		}
		dst = append(dst, src[:i]...)
		if src[i] == tupleSeparator {
			dst = append(dst, tupleEscape, escapedNUL)
		} else {
			dst = append(dst, tupleEscape, escapedEscape)
		}
		src = src[i+1:]
	}
	return dst
}

// AppendTupleSeparator writes a single separator byte between
// elements. Callers emit this themselves so the encoder is composable
// — e.g. a record's primary-key emission appends version + type +
// sync_id + separator + external_id with no separator at the end.
func AppendTupleSeparator(dst []byte) []byte {
	return append(dst, tupleSeparator)
}

// AppendTupleStrings tuple-encodes each string in s and interleaves
// the tuple separator between successive elements. Equivalent to
// calling AppendTupleString in a loop with AppendTupleSeparator
// between calls — but in one place, so key-encoding sites can't
// silently drift on "did I emit one too many / one too few
// separators?".
//
// No leading or trailing separator is emitted. Callers that need a
// leading separator (e.g. to delimit the raw sync_id bytes that
// precede the tuple tail in every Pebble v3 key) or a trailing
// separator (e.g. to make a by-value range-scan prefix unambiguous
// — see keys.go's convention doc) must add it themselves.
//
// For a single string, AppendTupleStrings(dst, s) is exactly
// equivalent to AppendTupleString(dst, s).
func AppendTupleStrings(dst []byte, s ...string) []byte {
	for i, x := range s {
		if i > 0 {
			dst = append(dst, tupleSeparator)
		}
		dst = appendEscapedString(dst, x)
	}
	return dst
}

// AppendTupleInt32 writes a sign-flipped big-endian 4-byte int32.
// Sign-flipping puts negative numbers before non-negative in
// bytewise comparison, matching natural int order.
func AppendTupleInt32(dst []byte, n int32) []byte {
	var buf [4]byte
	// XOR with the sign bit is the intentional FoundationDB tuple
	// encoding pattern; the int32→uint32 reinterpret-cast is safe.
	binary.BigEndian.PutUint32(buf[:], uint32(n)^0x80000000) //nolint:gosec // intentional sign-flip cast
	return append(dst, buf[:]...)
}

// AppendTupleInt64 writes a sign-flipped big-endian 8-byte int64.
func AppendTupleInt64(dst []byte, n int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(n)^0x8000000000000000) //nolint:gosec // intentional sign-flip cast
	return append(dst, buf[:]...)
}

// AppendTupleUint32 writes a big-endian 4-byte uint32 (no sign flip).
func AppendTupleUint32(dst []byte, n uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], n)
	return append(dst, buf[:]...)
}

// AppendTupleUint64 writes a big-endian 8-byte uint64 (no sign flip).
func AppendTupleUint64(dst []byte, n uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], n)
	return append(dst, buf[:]...)
}

// AppendTupleBool writes 0x26 for false, 0x27 for true. These bytes
// sort false-before-true and never collide with the separator (0x00)
// or escape (0x01).
func AppendTupleBool(dst []byte, b bool) []byte {
	if b {
		return append(dst, 0x27)
	}
	return append(dst, 0x26)
}

// DecodeTupleStringAlias decodes a single tuple-encoded string component from
// src starting at offset off. It is the zero-alloc counterpart to
// DecodeTupleStringTo for read-only callers on the hot path:
//
//   - When the component contains no escape byte (the overwhelmingly common
//     case for ids), the returned slice ALIASES src — no allocation. It is only
//     valid until src is mutated or its backing iterator advances.
//   - When the component contains an escape sequence, a decoded copy is
//     allocated, identical to DecodeTupleStringTo(nil, ...).
//
// The second return is the offset of the terminating separator byte, or
// len(src) if the component runs to end-of-input — same convention as
// DecodeTupleStringTo. The bool is false only when the input ends inside an
// escape sequence (malformed).
//
// Finding the component end by scanning for the next 0x00 is correct because
// the escape rules guarantee no component's encoded bytes contain a bare 0x00.
func DecodeTupleStringAlias(src []byte, off int) ([]byte, int, bool) {
	if off > len(src) {
		return nil, 0, false
	}
	end := len(src)
	if rel := bytes.IndexByte(src[off:], tupleSeparator); rel >= 0 {
		end = off + rel
	}
	comp := src[off:end]
	if bytes.IndexByte(comp, tupleEscape) < 0 {
		return comp, end, true
	}
	decoded, _, err := DecodeTupleStringTo(nil, src, off)
	if err != nil {
		return nil, 0, false
	}
	return decoded, end, true
}

// KeyUpperBound returns the lexicographically smallest key strictly greater
// than every key carrying prefix: the prefix with its last non-0xff byte
// incremented and any trailing 0xff bytes dropped. It returns nil when prefix
// is empty or all 0xff — there is no finite upper bound, so a range scan should
// run to the end of the keyspace. The input is not modified.
func KeyUpperBound(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// DecodeTupleStringTo decodes a single tuple-encoded string from src
// starting at offset off. Returns the decoded string, the offset
// immediately after the consumed bytes (pointing at the separator or
// end-of-input), and any error. If the input ends inside an escape
// sequence, returns ErrInvalidTuple.
func DecodeTupleStringTo(dst []byte, src []byte, off int) ([]byte, int, error) {
	i := off
	for i < len(src) {
		b := src[i]
		if b == tupleSeparator {
			return dst, i, nil
		}
		if b == tupleEscape {
			if i+1 >= len(src) {
				return nil, 0, ErrInvalidTuple
			}
			switch src[i+1] {
			case escapedNUL:
				dst = append(dst, tupleSeparator)
			case escapedEscape:
				dst = append(dst, tupleEscape)
			default:
				return nil, 0, ErrInvalidTuple
			}
			i += 2
			continue
		}
		dst = append(dst, b)
		i++
	}
	return dst, i, nil
}
