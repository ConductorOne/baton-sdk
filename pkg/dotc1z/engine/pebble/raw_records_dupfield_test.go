package pebble

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// --- wire-fragment builders -------------------------------------------
//
// These append raw protobuf field occurrences with protowire, deliberately
// bypassing proto.Marshal so a field number can be duplicated in the wire
// bytes — a legal (if unusual) shape proto.Unmarshal must still parse
// according to the documented merge rules (scalars: last wins; embedded
// messages: merge; map entries: last entry for a given key wins).

// appendBoolField appends one bool-valued varint field occurrence.
func appendBoolField(b []byte, num protowire.Number, v bool) []byte {
	b = protowire.AppendTag(b, num, protowire.VarintType)
	val := uint64(0)
	if v {
		val = 1
	}
	return protowire.AppendVarint(b, val)
}

// appendStringField appends one string-valued length-delimited field
// occurrence.
func appendStringField(b []byte, num protowire.Number, s string) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendString(b, s)
}

// appendBytesField appends one length-delimited field occurrence whose
// payload is an already-marshaled submessage (or any raw byte string).
func appendBytesField(b []byte, num protowire.Number, payload []byte) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, payload)
}

// grantSourceRecordBytes marshals a GrantSourceRecord (field 4 = is_direct)
// with one field-4 occurrence per entry in directs, in order — lets a
// case put is_direct twice in one fragment.
func grantSourceRecordBytes(directs ...bool) []byte {
	var b []byte
	for _, d := range directs {
		b = appendBoolField(b, 4, d)
	}
	return b
}

// grantSourcesMapEntryBytes builds one `sources` map entry (key = field 1,
// value = field 2) whose value payload is exactly valueBytes (already a
// marshaled GrantSourceRecord fragment, possibly with duplicated fields,
// or nil/empty for "no is_direct present").
func grantSourcesMapEntryBytes(key string, valueBytes []byte) []byte {
	entry := appendStringField(nil, 1, key)
	return appendBytesField(entry, 2, valueBytes)
}

// grantSourcesMapEntryTwoValueOccurrences builds one `sources` map entry
// whose value (field 2) appears TWICE inside the entry — a duplicate
// embedded-message field within a single map-entry occurrence, which
// proto semantics MERGE (unlike duplicate map keys across two entries,
// which replace).
func grantSourcesMapEntryTwoValueOccurrences(key string, first, second []byte) []byte {
	entry := appendStringField(nil, 1, key)
	entry = appendBytesField(entry, 2, first)
	entry = appendBytesField(entry, 2, second)
	return entry
}

// anyBytesWithTypeURLs builds one google.protobuf.Any fragment (field 1 =
// type_url) with one field-1 occurrence per entry in urls, in order.
func anyBytesWithTypeURLs(urls ...string) []byte {
	var b []byte
	for _, u := range urls {
		b = appendStringField(b, 1, u)
	}
	return b
}

const dupFieldTestImmutableTypeURL = "type.googleapis.com/" + grantImmutableAnnotationTypeName
const dupFieldTestOtherTypeURL = "type.googleapis.com/c1.connector.v2.SomeOtherAnnotation"

// baseDupFieldGrantRecord is the identity-only record every case appends
// duplicate-field fragments onto. Its identity fields are never touched by
// the appended fragments, so the primary-key tail is stable across cases.
func baseDupFieldGrantRecord() *v3.GrantRecord {
	return v3.GrantRecord_builder{
		ExternalId: "grant-dupfield-test",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app",
			ResourceId:     "github",
			EntitlementId:  "ent-1",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user",
			ResourceId:     "user-42",
		}.Build(),
	}.Build()
}

// dupFieldTestCase describes one duplicate-wire-field scenario: extra bytes
// appended after the base record's marshaled bytes (see
// baseDupFieldGrantRecord) to produce the final wire bytes under test.
// Shared by the table test below and the byte-fuzzer's seed corpus
// (raw_records_fuzz_test.go).
type dupFieldTestCase struct {
	name string
	// extra is appended after the base record's marshaled bytes to
	// produce the final wire bytes under test.
	extra []byte
	// wantSrcCount, when >= 0, asserts the raw scanner's post-sort
	// source-fact count in a separate subtest (cases c/d: two map
	// entries sharing one key must collapse to one source fact).
	wantSrcCount int
}

// dupFieldTestCases is the duplicate-wire-field scenario table shared by
// TestGrantContentHashRawMatchesDecodedOnDuplicateWireFields and the fuzz
// seed corpus in raw_records_fuzz_test.go.
func dupFieldTestCases() []dupFieldTestCase {
	return []dupFieldTestCase{
		{
			name:         "source is_direct duplicated true-then-false",
			extra:        appendBytesField(nil, 9, grantSourcesMapEntryBytes("src-a", grantSourceRecordBytes(true, false))),
			wantSrcCount: -1,
		},
		{
			name:         "source is_direct duplicated false-then-true",
			extra:        appendBytesField(nil, 9, grantSourcesMapEntryBytes("src-a", grantSourceRecordBytes(false, true))),
			wantSrcCount: -1,
		},
		{
			name: "duplicate map key, first true then empty",
			extra: func() []byte {
				var b []byte
				b = appendBytesField(b, 9, grantSourcesMapEntryBytes("src-a", grantSourceRecordBytes(true)))
				b = appendBytesField(b, 9, grantSourcesMapEntryBytes("src-a", nil))
				return b
			}(),
			wantSrcCount: 1,
		},
		{
			name: "duplicate map key, first empty then true",
			extra: func() []byte {
				var b []byte
				b = appendBytesField(b, 9, grantSourcesMapEntryBytes("src-a", nil))
				b = appendBytesField(b, 9, grantSourcesMapEntryBytes("src-a", grantSourceRecordBytes(true)))
				return b
			}(),
			wantSrcCount: 1,
		},
		{
			name: "map entry value submessage duplicated, true-then-empty",
			extra: appendBytesField(nil, 9, grantSourcesMapEntryTwoValueOccurrences(
				"src-a", grantSourceRecordBytes(true), nil)),
			wantSrcCount: -1,
		},
		{
			name:         "annotation any type_url duplicated, other-then-immutable",
			extra:        appendBytesField(nil, 8, anyBytesWithTypeURLs(dupFieldTestOtherTypeURL, dupFieldTestImmutableTypeURL)),
			wantSrcCount: -1,
		},
		{
			name:         "annotation any type_url duplicated, immutable-then-other",
			extra:        appendBytesField(nil, 8, anyBytesWithTypeURLs(dupFieldTestImmutableTypeURL, dupFieldTestOtherTypeURL)),
			wantSrcCount: -1,
		},
		{
			name: "control: no duplicates",
			extra: func() []byte {
				var b []byte
				b = appendBytesField(b, 9, grantSourcesMapEntryBytes("src-a", grantSourceRecordBytes(true)))
				b = appendBytesField(b, 8, anyBytesWithTypeURLs(dupFieldTestImmutableTypeURL))
				return b
			}(),
			wantSrcCount: 1,
		},
	}
}

// TestGrantContentHashRawMatchesDecodedOnDuplicateWireFields is a
// differential test: for wire bytes that legally duplicate a field number
// somewhere inside a marshaled GrantRecord (a shape proto.Marshal never
// produces, but proto.Unmarshal must still parse per the documented merge
// rules), the raw-scan content-hash path (scanGrantContentFactsRawBytes +
// sortGrantSourceFacts + grantContentHash64) must agree with the
// decode-then-hash reference path (proto.Unmarshal + grantContentHashForRecord)
// bit for bit.
//
// The oracle is proto.Unmarshal's actual behavior — no case hardcodes
// which duplicate should "win"; only path equality is asserted.
func TestGrantContentHashRawMatchesDecodedOnDuplicateWireFields(t *testing.T) {
	for _, tc := range dupFieldTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			base, err := marshalRecord(baseDupFieldGrantRecord())
			require.NoError(t, err)
			wire := append(append([]byte(nil), base...), tc.extra...)

			// Reference path: proto.Unmarshal, then the from-record hash.
			decoded := &v3.GrantRecord{}
			require.NoError(t, proto.Unmarshal(wire, decoded))
			want, err := grantContentHashForRecord(decoded)
			require.NoError(t, err)

			// Raw path: exactly what the seal-time build and repair paths
			// do (see appendGrantHashIndexRow / repairOneGrantDigestPartitionLocked).
			id, err := grantIdentityFromRecord(decoded)
			require.NoError(t, err)
			key := encodeGrantIdentityKey(id)

			isImm, srcs, err := scanGrantContentFactsRawBytes(wire, nil)
			require.NoError(t, err)
			srcs = sortGrantSourceFacts(srcs)
			gotU64, _ := grantContentHash64(nil, key[grantPrimaryKeyPrefixLen:], isImm, srcs)
			got := make([]byte, hashLen)
			binary.BigEndian.PutUint64(got, gotU64)

			require.Equal(t, want, got, "raw-scan content hash must match decode-then-hash reference")

			if tc.wantSrcCount >= 0 {
				t.Run("source_fact_count_after_dedupe", func(t *testing.T) {
					require.Len(t, srcs, tc.wantSrcCount, "duplicate map keys must collapse to one source fact")
				})
			}
		})
	}
}
