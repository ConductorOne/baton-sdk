package pebble

import (
	"encoding/base64"
	"encoding/binary"
	"strings"
	"testing"
)

// decodeBatchCursor parses attacker-controlled page tokens. A combined
// `len(raw) < lenU+4` guard wraps for lenU >= MaxUint64-3 and lets raw[:lenU]
// panic; the two-term guard must return a typed error for every wrap-band
// token instead.
func TestSecurity_DecodeBatchCursorRejectsUint64WrapTokens(t *testing.T) {
	const maxUint64 = ^uint64(0)

	makeToken := func(lenU uint64, tailLen int) string {
		buf := binary.AppendUvarint(nil, 0) // idx
		buf = binary.AppendUvarint(buf, lenU)
		for range tailLen {
			buf = append(buf, 0xAA)
		}
		return base64.RawURLEncoding.EncodeToString(buf)
	}

	for _, lenU := range []uint64{maxUint64, maxUint64 - 1, maxUint64 - 2, maxUint64 - 3} {
		for _, tailLen := range []int{0, 1, 2, 3, 4, 8} {
			token := makeToken(lenU, tailLen)
			t.Run(token, func(t *testing.T) {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("decodeBatchCursor panicked on lenU=%d tail=%d: %v", lenU, tailLen, r)
					}
				}()
				_, _, err := decodeBatchCursor(token, 0)
				if err == nil {
					t.Fatalf("decodeBatchCursor accepted wrap-band lenU=%d tail=%d; want typed error", lenU, tailLen)
				}
				if !strings.Contains(err.Error(), "truncated") {
					t.Fatalf("decodeBatchCursor lenU=%d tail=%d: unexpected error: %v", lenU, tailLen, err)
				}
			})
		}
	}

	// Just outside the wrap band the sum does not wrap, so a short payload is
	// a plain truncation error.
	_, _, err := decodeBatchCursor(makeToken(maxUint64-4, 4), 0)
	if err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("lenU=MaxUint64-4 with 4-byte tail: want truncation error, got %v", err)
	}

	// Checksum mismatch restarts from the beginning rather than erroring.
	_, _, err = decodeBatchCursor(makeToken(0, 4), 0x11223344)
	if err != nil {
		t.Fatalf("checksum-mismatch restart returned error: %v", err)
	}
}
