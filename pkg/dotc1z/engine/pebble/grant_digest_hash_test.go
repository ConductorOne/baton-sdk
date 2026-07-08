package pebble

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// The seal-time digest build never decodes anything: the hash-index key
// is spliced out of the grant primary key, the bucket hash is computed
// over a raw sub-slice of it, the content hash over the primary tail
// plus raw-scanned source keys. All of that is on-disk ABI, so each
// splice/byte form is pinned here against the from-decoded-record
// reference path (encode identity → hash), the way
// TestAppendGrantByPrincipalKeyFromPrimary pins the by_principal
// splice. A divergence would make two SDK builds hash identical grants
// differently, which the digest comparison reads as "everything
// differs".
func TestGrantDigestSpliceMatchesEncode(t *testing.T) {
	cases := []struct {
		name                 string
		entRT, entRID, entID string
		prt, pid, ext        string
		srcs                 []string
	}{
		{name: "plain opaque ent id", entRT: "app", entRID: "github", entID: "ent-1", prt: "user", pid: "user-42", ext: "grant-1"},
		{name: "stripped ent id", entRT: "app", entRID: "github", entID: "app:github:member", prt: "user", pid: "user-42", ext: "app:github:member:user:user-42"},
		{name: "sources", entRT: "app", entRID: "github", entID: "ent-1", prt: "user", pid: "user-42", ext: "g", srcs: []string{"c-src", "a-src", "b-src"}},
		{name: "embedded NUL", entRT: "a\x00pp", entRID: "git\x00hub", entID: "ent\x00x", prt: "us\x00er", pid: "id\x00", ext: "g", srcs: []string{"s\x00rc", "\x00"}},
		{name: "escape byte", entRT: "a\x01pp", entRID: "hub", entID: "ent\x01x", prt: "us\x01er", pid: "\x01id", ext: "g", srcs: []string{"\x01", "\x00"}},
		{name: "unicode", entRT: "приложение", entRID: "гитхаб", entID: "entitlé", prt: "usér", pid: "ид-42", ext: "грант"},
		{name: "duplicate-source keys impossible but sorted singleton", entRT: "app", entRID: "gh", entID: "e", prt: "u", pid: "p", ext: "", srcs: []string{"only"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := v3.GrantRecord_builder{
				ExternalId: tc.ext,
				Entitlement: v3.EntitlementRef_builder{
					ResourceTypeId: tc.entRT,
					ResourceId:     tc.entRID,
					EntitlementId:  tc.entID,
				}.Build(),
				Principal: v3.PrincipalRef_builder{
					ResourceTypeId: tc.prt,
					ResourceId:     tc.pid,
				}.Build(),
			}.Build()
			if len(tc.srcs) > 0 {
				m := make(map[string]*v3.GrantSourceRecord, len(tc.srcs))
				for _, s := range tc.srcs {
					m[s] = v3.GrantSourceRecord_builder{}.Build()
				}
				rec.SetSources(m)
			}

			id, err := grantIdentityFromRecord(rec)
			require.NoError(t, err)
			priKey := encodeGrantIdentityKey(id)
			val, err := marshalRecord(rec)
			require.NoError(t, err)

			sep4, ok := splitGrantPrimaryKey(priKey)
			require.True(t, ok, "splitGrantPrimaryKey")

			// The partition region of the primary key is exactly the
			// encoded entitlement identity tail.
			partition := appendEntitlementIdentityTail(nil, id.entitlement)
			require.Equal(t, partition, priKey[grantPrimaryKeyPrefixLen:sep4], "partition region")
			require.Equal(t, string(partition), digestPartitionForEntitlement(id.entitlement))

			// Bucket hash over the spliced principal region == bucket hash
			// over freshly encoded principal segments.
			wantBH64 := xxhash.Sum64(codec.AppendTupleStrings(nil, tc.prt, tc.pid))
			require.Equal(t, wantBH64, grantPrincipalBucketHash64(priKey[sep4+1:]), "bucket hash from key splice")
			var full [8]byte
			binary.BigEndian.PutUint64(full[:], wantBH64)
			require.Equal(t, full[:digestBucketHashLen], principalBucketHash(tc.prt, tc.pid), "principalBucketHash top bytes")

			// Content hash from raw key+value bytes == content hash from
			// the decoded record.
			srcs, err := scanGrantSourceKeysRawBytes(val, nil)
			require.NoError(t, err)
			sortByteSlices(srcs)
			ch64, _ := grantContentHash64(nil, priKey[grantPrimaryKeyPrefixLen:], srcs)
			fromRecord, err := grantContentHashForRecord(rec)
			require.NoError(t, err)
			require.Equal(t, fromRecord, binary.BigEndian.AppendUint64(nil, ch64), "content hash: raw scan vs decoded record")

			// Index key splice == reference built entirely from encoders.
			idxKey := appendGrantHashIndexKeyFromPrimary(nil, priKey, sep4, wantBH64)
			ref := encodeGrantByEntPrincHashEntPrefix(string(partition))
			ref = append(ref, principalBucketHash(tc.prt, tc.pid)...)
			ref = codec.AppendTupleStrings(ref, tc.prt, tc.pid)
			require.Equal(t, ref, idxKey, "index key: splice vs encode")

			// Round trip: removing the hash region reconstructs the
			// primary key byte-exactly.
			back, ok := grantPrimaryKeyFromHashIndexKey(nil, idxKey)
			require.True(t, ok, "grantPrimaryKeyFromHashIndexKey")
			require.Equal(t, priKey, back, "primary key round trip")

			// The merge-side splitter agrees on the partition and bucket.
			gotPartition, bucket, ok := splitGrantHashIndexKey(idxKey)
			require.True(t, ok, "splitGrantHashIndexKey")
			require.Equal(t, partition, gotPartition)
			require.Equal(t, binary.BigEndian.Uint16(full[:digestBucketHashLen]), bucket)
		})
	}
}

// TestGrantDigestPartitionPrefixFree pins the property bucketBounds and
// the partition-contiguity of the index rest on: no partition's index
// prefix is a byte-prefix of another's, even for entitlements whose
// tails extend each other or contain separator-adjacent escapes.
func TestGrantDigestPartitionPrefixFree(t *testing.T) {
	ids := []entitlementIdentity{
		entitlementIdentityFromParts("app", "github", "ent"),
		entitlementIdentityFromParts("app", "github", "ent-1"),
		entitlementIdentityFromParts("app", "github", "app:github:ent"),
		entitlementIdentityFromParts("app", "github", "ent\x00"),
		entitlementIdentityFromParts("app", "github", "ent\x01"),
		entitlementIdentityFromParts("app", "gith", "ub:ent"),
		entitlementIdentityFromParts("ap", "pgithub", "ent"),
	}
	prefixes := make([][]byte, len(ids))
	for i, id := range ids {
		prefixes[i] = encodeGrantByEntPrincHashEntPrefix(digestPartitionForEntitlement(id))
	}
	for i := range prefixes {
		for j := range prefixes {
			if i == j {
				continue
			}
			require.False(t, bytes.HasPrefix(prefixes[i], prefixes[j]),
				"partition prefix %d (%x) extends %d (%x)", i, prefixes[i], j, prefixes[j])
		}
	}
}
