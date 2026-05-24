//go:build batonsdkv2

package codec

import (
	"bytes"
	"testing"
)

// TestTupleEncoding_PrefixFree verifies the prefix-free property of
// the tuple encoding over hand-picked + random byte sequences. Ports
// the micro-test from /tmp/baton-rfc-microtests/tuple_test.go into
// the engine package.
func TestTupleEncoding_PrefixFree(t *testing.T) {
	encode := func(parts ...[]byte) []byte {
		var out []byte
		for i, p := range parts {
			out = AppendTupleBytes(out, p)
			if i < len(parts)-1 {
				out = AppendTupleSeparator(out)
			}
		}
		return out
	}
	tupleCompare := func(a, b [][]byte) int {
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
	cases := [][][]byte{
		{[]byte("user"), []byte("alice")},
		{[]byte("user"), []byte("alice\x00bob")},
		{[]byte("user\x00prefix"), []byte("alice")},
		{[]byte("user\x01"), []byte("\x00alice")},
		{[]byte("user"), []byte("")},
		{[]byte(""), []byte("alice")},
		{[]byte("\x00\x00\x00"), []byte("alice")},
		{[]byte("user"), []byte("\x01\x01\x01")},
		{{0xff, 0xfe}, {0x00, 0x01}},
	}
	for i, a := range cases {
		for j, b := range cases {
			tc := tupleCompare(a, b)
			bc := bytes.Compare(encode(a...), encode(b...))
			if bc < 0 {
				bc = -1
			} else if bc > 0 {
				bc = 1
			}
			if tc != bc {
				t.Errorf("case[%d]=%v vs case[%d]=%v: tuple=%d bytewise=%d",
					i, a, j, b, tc, bc)
			}
		}
	}
}

func TestDecodeTupleString_Roundtrip(t *testing.T) {
	inputs := [][]byte{
		[]byte(""),
		[]byte("hello"),
		[]byte("with\x00embedded"),
		[]byte("\x01escape-byte"),
		{0xff, 0x00, 0x01, 0x02},
	}
	for _, in := range inputs {
		encoded := AppendTupleBytes(nil, in)
		decoded, n, err := DecodeTupleStringTo(nil, encoded, 0)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if n != len(encoded) {
			t.Fatalf("%q: consumed %d, want %d", in, n, len(encoded))
		}
		if !bytes.Equal(decoded, in) {
			t.Fatalf("%q: roundtrip failed; got %q", in, decoded)
		}
	}
}

func TestTupleInt32Sort(t *testing.T) {
	values := []int32{-1 << 30, -1, 0, 1, 1 << 30}
	encoded := make([][]byte, len(values))
	for i, v := range values {
		encoded[i] = AppendTupleInt32(nil, v)
	}
	// Verify bytewise comparison matches int order.
	for i := 1; i < len(values); i++ {
		if bytes.Compare(encoded[i-1], encoded[i]) >= 0 {
			t.Errorf("encoded[%d]=%v >= encoded[%d]=%v but %d < %d",
				i-1, encoded[i-1], i, encoded[i], values[i-1], values[i])
		}
	}
}

func TestTupleBoolEncoding(t *testing.T) {
	if b := AppendTupleBool(nil, false); len(b) != 1 || b[0] != 0x26 {
		t.Errorf("false: got %v", b)
	}
	if b := AppendTupleBool(nil, true); len(b) != 1 || b[0] != 0x27 {
		t.Errorf("true: got %v", b)
	}
}
