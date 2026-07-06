package pebble

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/cespare/xxhash/v2"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// The seal-time index build uses byte-view, buffer-reusing variants of
// the row primitives (principalBucketHash64, grantContentHash64,
// appendGrantByEntPrincHashIndexKeyBytes). Their output is on-disk ABI:
// a divergence from the canonical definitions would make two SDK builds
// hash identical grants differently, which the digest comparison would
// read as "everything differs". Pin each variant to an INDEPENDENT
// reference implementation (the pre-optimization code), not to the
// wrapper that now delegates to it.
func TestGrantDigestByteVariantsMatchReference(t *testing.T) {
	cases := []struct {
		name               string
		ent, prt, pid, ext string
		srcs               []string
	}{
		{name: "plain", ent: "ent-1", prt: "user", pid: "user-42", ext: "grant-1"},
		{name: "sources", ent: "ent-1", prt: "user", pid: "user-42", ext: "grant-1", srcs: []string{"a", "b", "c"}},
		{name: "embedded NUL", ent: "ent\x00x", prt: "us\x00er", pid: "id\x00", ext: "g\x00\x00", srcs: []string{"s\x00rc"}},
		{name: "escape byte", ent: "ent\x01x", prt: "us\x01er", pid: "\x01id", ext: "g\x01", srcs: []string{"\x01", "\x00"}},
		{name: "empty fields", ent: "e", prt: "u", pid: "p", ext: "", srcs: []string{""}},
		{name: "unicode", ent: "entitlé", prt: "usér", pid: "ид-42", ext: "грант"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Reference content hash: tuple-encode the field list with
			// the string codec, hash the framing.
			fields := append([]string{tc.ent, tc.prt, tc.pid, tc.ext}, tc.srcs...)
			wantCH := xxhash.Sum64(codec.AppendTupleStrings(nil, fields...))

			srcs := make([][]byte, len(tc.srcs))
			for i := range tc.srcs {
				srcs[i] = []byte(tc.srcs[i])
			}
			// Seed the buffer with junk to prove the [:0] reuse contract.
			gotCH, _ := grantContentHash64([]byte("junk"), []byte(tc.ent), []byte(tc.prt), []byte(tc.pid), []byte(tc.ext), srcs)
			if gotCH != wantCH {
				t.Errorf("grantContentHash64 = %x, reference = %x", gotCH, wantCH)
			}
			if got := grantContentHashFromParts(tc.ent, tc.prt, tc.pid, tc.ext, tc.srcs); binary.BigEndian.Uint64(got) != wantCH {
				t.Errorf("grantContentHashFromParts = %x, reference = %x", got, wantCH)
			}

			// Reference bucket hash: the streaming form.
			h := xxhash.New()
			_, _ = h.WriteString(tc.prt)
			_, _ = h.Write([]byte{0})
			_, _ = h.WriteString(tc.pid)
			wantBH := h.Sum64()
			gotBH, _ := principalBucketHash64([]byte("junk"), []byte(tc.prt), []byte(tc.pid))
			if gotBH != wantBH {
				t.Errorf("principalBucketHash64 = %x, reference = %x", gotBH, wantBH)
			}
			var full [8]byte
			binary.BigEndian.PutUint64(full[:], wantBH)
			if got := principalBucketHash(tc.prt, tc.pid); !bytes.Equal(got, full[:digestBucketHashLen]) {
				t.Errorf("principalBucketHash = %x, reference top bytes = %x", got, full[:digestBucketHashLen])
			}

			// Index key: bytes variant against the string encoder.
			bh := principalBucketHash(tc.prt, tc.pid)
			want := encodeGrantByEntPrincHashIndexKey(tc.ent, bh, tc.prt, tc.pid, tc.ext)
			got := appendGrantByEntPrincHashIndexKeyBytes([]byte("junk")[:0], []byte(tc.ent), bh, []byte(tc.prt), []byte(tc.pid), []byte(tc.ext))
			if !bytes.Equal(got, want) {
				t.Errorf("appendGrantByEntPrincHashIndexKeyBytes = %x, encode = %x", got, want)
			}
		})
	}
}
