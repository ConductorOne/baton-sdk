package pebble

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// FuzzGrantContentHashRawMatchesDecoded is the byte-fuzzing counterpart to
// TestGrantContentHashRawMatchesDecodedOnDuplicateWireFields
// (raw_records_dupfield_test.go): rather than a fixed table of
// duplicate-field shapes, it hands proto.Unmarshal arbitrary wire bytes and,
// for every input proto.Unmarshal accepts and that carries a hashable grant
// identity, checks that the raw-scan content-hash path
// (scanGrantContentFactsRawBytes -> sortGrantSourceFacts ->
// grantContentHash64) agrees with the decode-then-hash reference path
// (proto.Unmarshal -> grantContentHashForRecord) bit for bit.
//
// The oracle is "what proto.Unmarshal accepts": malformed bytes, or bytes
// proto rejects (e.g. invalid UTF-8 in a string field), are out of scope
// and skipped. Follows the byte-based fuzzer style of FuzzCondenseFWBW_FromBytes
// (pkg/sync/expand/scc/scc_fuzz_test.go).
func FuzzGrantContentHashRawMatchesDecoded(f *testing.F) {
	base, err := marshalRecord(baseDupFieldGrantRecord())
	require.NoError(f, err)
	f.Add(base)

	for _, tc := range dupFieldTestCases() {
		f.Add(append(append([]byte(nil), base...), tc.extra...))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		decoded := &v3.GrantRecord{}
		if err := proto.Unmarshal(data, decoded); err != nil {
			t.Skip()
		}

		id, err := grantIdentityFromRecord(decoded)
		if err != nil {
			t.Skip()
		}

		// Reference path: proto.Unmarshal, then the from-record hash.
		want, err := grantContentHashForRecord(decoded)
		require.NoError(t, err)

		// Raw path: exactly what the seal-time build and repair paths do
		// (see appendGrantHashIndexRow / repairOneGrantDigestPartitionLocked).
		isImm, srcs, err := scanGrantContentFactsRawBytes(data, nil)
		if err != nil {
			// proto.Unmarshal accepted these bytes above; the raw scanner
			// erroring on the same bytes is itself the divergence under
			// test.
			t.Fatalf("raw scan rejected bytes proto.Unmarshal accepted: %v\ndata: %x", err, data)
		}
		srcs = sortGrantSourceFacts(srcs)

		// Diagnostics: compare the intermediate facts before the final
		// hash comparison, so a divergence names the specific fact that
		// differed rather than just "hash mismatch".
		wantImm := annsContainType(decoded.GetAnnotations(), grantImmutableAnnotationTypeName)
		require.Equal(t, wantImm, isImm, "isImmutable fact diverged\ndata: %x", data)

		srcMap := decoded.GetSources()
		wantSrcs := make([]grantSourceFact, 0, len(srcMap))
		for k, v := range srcMap {
			wantSrcs = append(wantSrcs, grantSourceFact{key: []byte(k), isDirect: v.GetIsDirect()})
		}
		wantSrcs = sortGrantSourceFacts(wantSrcs)

		require.Equal(t, len(wantSrcs), len(srcs), "source fact count diverged\ndata: %x", data)
		for i := range wantSrcs {
			require.Truef(t, bytes.Equal(wantSrcs[i].key, srcs[i].key),
				"source key %d diverged: want %q got %q\ndata: %x", i, wantSrcs[i].key, srcs[i].key, data)
			require.Equal(t, wantSrcs[i].isDirect, srcs[i].isDirect,
				"source is_direct %d diverged\ndata: %x", i, data)
		}

		key := encodeGrantIdentityKey(id)
		gotU64, _ := grantContentHash64(nil, key[grantPrimaryKeyPrefixLen:], isImm, srcs)
		got := make([]byte, hashLen)
		binary.BigEndian.PutUint64(got, gotU64)

		require.Equal(t, want, got, "raw-scan content hash must match decode-then-hash reference\ndata: %x", data)
	})
}
