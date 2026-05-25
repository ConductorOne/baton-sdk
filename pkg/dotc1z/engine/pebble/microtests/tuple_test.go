// Micro-test 3 — validates the production tuple key encoding's
// prefix-free property. The encoder's escape rule:
//
//	0x00 → 0x01 0x01
//	0x01 → 0x01 0x02
//	terminator between elements: 0x00
//
// Property: for any tuples a and b,
//
//	bytewise_compare(encode(a), encode(b)) == tuple_compare(a, b)
//
// This is the property real range scans depend on. If it breaks for
// bytes containing embedded NUL — and external IDs from SCIM /
// Okta / Entra often do — range scans return wrong results.
//
// This file tests pkg/dotc1z/engine/pebble/codec.AppendTupleBytes
// directly, not a parallel re-implementation.

package microtests

import (
	"bytes"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// tupleEncodeBytes encodes a sequence of byte slices via the
// production encoder. The separator is codec.AppendTupleSeparator.
func tupleEncodeBytes(parts ...[]byte) []byte {
	var out []byte
	for i, p := range parts {
		out = codec.AppendTupleBytes(out, p)
		if i < len(parts)-1 {
			out = codec.AppendTupleSeparator(out)
		}
	}
	return out
}

func tupleCompareBytes(a, b [][]byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if c := bytes.Compare(a[i], b[i]); c != 0 {
			return c
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}

func TestTupleEncodingPrefixFree(t *testing.T) {
	cases := [][][]byte{
		{[]byte("user"), []byte("alice")},
		{[]byte("user"), []byte("alice\x00bob")},
		{[]byte("user\x00prefix"), []byte("alice")},
		{[]byte("user\x01"), []byte("\x00alice")},
		{[]byte("user"), []byte("")},
		{[]byte(""), []byte("alice")},
		{[]byte("\x00\x00\x00"), []byte("alice")},
		{[]byte("user"), []byte("\x01\x01\x01")},
		{[]byte{0xff, 0xfe}, []byte{0x00, 0x01}},
	}

	for _, c := range cases {
		enc := tupleEncodeBytes(c...)
		separatorCount := bytes.Count(enc, []byte{0x00})
		if separatorCount != len(c)-1 {
			t.Errorf("tuple %v: %d separators, want %d (enc=%x)",
				c, separatorCount, len(c)-1, enc)
		}
	}

	for i, a := range cases {
		for j, b := range cases {
			tc := tupleCompareBytes(a, b)
			bc := bytes.Compare(tupleEncodeBytes(a...), tupleEncodeBytes(b...))
			if bc < 0 {
				bc = -1
			} else if bc > 0 {
				bc = 1
			}
			if tc != bc {
				t.Errorf("compare mismatch case[%d]=%v case[%d]=%v: tuple=%d bytewise=%d",
					i, a, j, b, tc, bc)
			}
		}
	}

	alphabet := []byte{0x00, 0x01, 0x02, 0xff}
	const randomCases = 200
	random := make([][][]byte, 0, randomCases)
	for i := 0; i < randomCases; i++ {
		nParts := 1 + (i % 3)
		tu := make([][]byte, nParts)
		for k := 0; k < nParts; k++ {
			n := i%5 + 1
			p := make([]byte, n)
			for b := 0; b < n; b++ {
				p[b] = alphabet[(i*17+k*5+b)%len(alphabet)]
			}
			tu[k] = p
		}
		random = append(random, tu)
	}
	for i, a := range random {
		for j, b := range random {
			tc := tupleCompareBytes(a, b)
			bc := bytes.Compare(tupleEncodeBytes(a...), tupleEncodeBytes(b...))
			if bc < 0 {
				bc = -1
			} else if bc > 0 {
				bc = 1
			}
			if tc != bc {
				t.Errorf("random[%d]=%v vs random[%d]=%v: tuple=%d bytewise=%d  enc_a=%x enc_b=%x",
					i, a, j, b, tc, bc,
					tupleEncodeBytes(a...), tupleEncodeBytes(b...))
				return
			}
		}
	}
}
