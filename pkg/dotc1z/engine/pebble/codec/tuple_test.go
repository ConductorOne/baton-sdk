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

// TestAppendTupleString_MatchesBytes pins the string-input encoder to
// the []byte-input encoder: AppendTupleString(s) must be byte-for-byte
// identical to AppendTupleBytes([]byte(s)) for every input, including
// the escape-triggering bytes. This guards the no-copy string fast path
// against drifting from the byte path.
func TestAppendTupleString_MatchesBytes(t *testing.T) {
	inputs := []string{
		"",
		"alice",
		"user\x00bob",
		"\x01escape",
		"trailing\x00",
		"\x00leading",
		"\x00\x00\x00",
		"\x01\x01\x01",
		"mix\x00ed\x01bytes\x00",
		string([]byte{0xff, 0xfe, 0x00, 0x01, 0x02}),
	}
	for _, s := range inputs {
		gotString := AppendTupleString(nil, s)
		gotBytes := AppendTupleBytes(nil, []byte(s))
		if !bytes.Equal(gotString, gotBytes) {
			t.Errorf("input %q: AppendTupleString=%v, AppendTupleBytes=%v", s, gotString, gotBytes)
		}
		// And it must decode back to the original.
		decoded, _, err := DecodeTupleStringTo(nil, gotString, 0)
		if err != nil {
			t.Errorf("input %q: decode error %v", s, err)
		}
		if string(decoded) != s {
			t.Errorf("input %q: roundtrip got %q", s, decoded)
		}
	}
}

// TestAppendTupleStrings_NoAlloc verifies the string encoder doesn't
// allocate a []byte copy per component when the destination buffer has
// capacity: the whole point of the string fast path. One amortized
// alloc for dst growth is fine; per-component string copies are not.
func TestAppendTupleStrings_NoAlloc(t *testing.T) {
	dst := make([]byte, 0, 256)
	allocs := testing.AllocsPerRun(100, func() {
		_ = AppendTupleStrings(dst[:0], "entitlement-id", "user", "alice", "grant-external-id")
	})
	if allocs != 0 {
		t.Errorf("AppendTupleStrings allocated %v times, want 0", allocs)
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

// TestDecodeTupleStringAlias_MatchesDecodeTupleStringTo verifies the aliasing
// decoder returns the same bytes and next-offset as DecodeTupleStringTo across
// single- and multi-component keys, including escape-triggering inputs.
func TestDecodeTupleStringAlias_MatchesDecodeTupleStringTo(t *testing.T) {
	components := [][]string{
		{"alice"},
		{"user", "alice"},
		{"entitlement-id", "user", "alice", "grant-external-id"},
		{"", "alice"},
		{"user", ""},
		{"has\x00nul", "has\x01esc"},
		{"\x00\x00", "\x01\x01", "tail"},
		{string([]byte{0xff, 0xfe, 0x00, 0x01}), "x"},
	}
	for _, parts := range components {
		key := AppendTupleStrings(nil, parts...)
		off := 0
		for idx, want := range parts {
			wantDecoded, wantNext, err := DecodeTupleStringTo(nil, key, off)
			if err != nil {
				t.Fatalf("%q[%d]: DecodeTupleStringTo error %v", parts, idx, err)
			}
			gotDecoded, gotNext, ok := DecodeTupleStringAlias(key, off)
			if !ok {
				t.Fatalf("%q[%d]: DecodeTupleStringAlias not ok", parts, idx)
			}
			if !bytes.Equal(gotDecoded, wantDecoded) {
				t.Errorf("%q[%d]: alias=%q want %q", parts, idx, gotDecoded, wantDecoded)
			}
			if gotNext != wantNext {
				t.Errorf("%q[%d]: alias next=%d want %d", parts, idx, gotNext, wantNext)
			}
			if string(gotDecoded) != want {
				t.Errorf("%q[%d]: alias roundtrip got %q", parts, idx, gotDecoded)
			}
			off = gotNext + 1
		}
	}
}

// TestDecodeTupleStringAlias_NoAllocOnEscapeFree verifies the common no-escape
// path aliases src without allocating; the escaped path is allowed to copy.
func TestDecodeTupleStringAlias_NoAllocOnEscapeFree(t *testing.T) {
	key := AppendTupleStrings(nil, "user", "alice")
	allocs := testing.AllocsPerRun(100, func() {
		if _, _, ok := DecodeTupleStringAlias(key, 0); !ok {
			t.Fatal("not ok")
		}
	})
	if allocs != 0 {
		t.Errorf("DecodeTupleStringAlias allocated %v times on escape-free input, want 0", allocs)
	}
}

// TestDecodeTupleStringAlias_Malformed verifies a key ending inside an escape
// sequence is rejected.
func TestDecodeTupleStringAlias_Malformed(t *testing.T) {
	if _, _, ok := DecodeTupleStringAlias([]byte{tupleEscape}, 0); ok {
		t.Error("dangling escape: got ok, want false")
	}
}

func TestKeyUpperBound(t *testing.T) {
	cases := []struct {
		in   []byte
		want []byte
	}{
		{[]byte{0x03, 0x07, 0x01, 0x00}, []byte{0x03, 0x07, 0x01, 0x01}},
		{[]byte{0x01, 0xff}, []byte{0x02}},
		{[]byte{0xff, 0xff}, nil},
		{[]byte{}, nil},
	}
	for _, tc := range cases {
		got := KeyUpperBound(tc.in)
		if !bytes.Equal(got, tc.want) {
			t.Errorf("KeyUpperBound(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
	// Must not mutate the input.
	in := []byte{0x01, 0x02}
	_ = KeyUpperBound(in)
	if !bytes.Equal(in, []byte{0x01, 0x02}) {
		t.Errorf("KeyUpperBound mutated input to %v", in)
	}
}
