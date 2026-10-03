// SPDX-License-Identifier: Apache-2.0

package dotc1z

import (
	"encoding/base64"
	"encoding/binary"
	"strings"
	"testing"
)

// TestSecurity_DecodeBatchCursorRejectsUint64WrapTokens guards against the
// uint64 length-wrap panic in the SQLite engine's ListGrantsForEntitlements
// page-token decoder (decodeBatchCursor).
//
// Finding: dotc1z/sqlite-decodeBatchCursor-lenU-uint64-wrap
//
// The old guard `uint64(len(raw)) < lenU+4` performed the attacker-controlled
// + constant addition before comparison: for lenU >= MaxUint64-3 the sum wraps
// to <=3, the guard passed a short payload, and the subsequent raw[:lenU]
// slice panicked with a runtime slice-bounds error, crashing any process
// serving a SQLite-engine c1z through the connectorstore.Reader surface.
//
// The Pebble engine's twin decoder (pkg/dotc1z/engine/pebble) already uses the
// wrap-proof two-term guard; this test pins the same behavior here: every
// wrap-band token must return a typed error, never a panic.
//
// Pre-fix (red): the wrap-band cases panic with
// "runtime error: slice bounds out of range [:18446744073709551615]".
// Post-fix (green): typed "cursor truncated" errors.
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

	// Wrap band: lenU+4 wraps to <=3 so a combined comparison passes and the
	// raw[:lenU] slice is out of range. Each of these must be a typed error,
	// not a panic.
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

	// Just outside the wrap band: the sum does not wrap, so a short payload is
	// (and always was) a clean truncation error.
	_, _, err := decodeBatchCursor(makeToken(maxUint64-4, 4), 0)
	if err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("lenU=MaxUint64-4 with 4-byte tail: want truncation error, got %v", err)
	}

	// Sanity: the checksum-mismatch path still restarts cleanly rather than
	// erroring (stale cursor semantics).
	_, _, err = decodeBatchCursor(makeToken(0, 4), 0x11223344)
	if err != nil {
		t.Fatalf("checksum-mismatch restart returned error: %v", err)
	}
}