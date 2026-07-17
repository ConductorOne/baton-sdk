package pebble

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// TestAppendGrantByPrincipalKeyFromPrimary pins the raw-splice fast path to
// the decode + re-encode reference: for any well-formed primary grant key the
// spliced by_principal index key must be byte-identical to
// encodeGrantByPrincipalIdentityIndexKey(decodeGrantIdentityKey(key)).
func TestAppendGrantByPrincipalKeyFromPrimary(t *testing.T) {
	ids := []grantIdentity{
		{
			entitlement:     entitlementIdentity{resourceTypeID: "group", resourceID: "g1", stripped: true, tail: "member"},
			principalTypeID: "user",
			principalID:     "u1",
		},
		// Empty components.
		{
			entitlement:     entitlementIdentity{resourceTypeID: "", resourceID: "g1", tail: "member"},
			principalTypeID: "user",
			principalID:     "",
		},
		// Escape-triggering bytes (0x00 and 0x01) in components: the splice
		// must carry the escaped forms through untouched.
		{
			entitlement:     entitlementIdentity{resourceTypeID: "gr\x00oup", resourceID: "g\x011", tail: "\x00k\x00\x01"},
			principalTypeID: "us\x01er",
			principalID:     "u\x00\x001",
		},
		// High bytes / unicode.
		{
			entitlement:     entitlementIdentity{resourceTypeID: "grüppe", resourceID: string([]byte{0xff, 0xfe}), stripped: true, tail: "членство"},
			principalTypeID: "user",
			principalID:     "u-é",
		},
	}

	for _, id := range ids {
		primary := encodeGrantIdentityKey(id)

		decoded, ok := decodeGrantIdentityKey(primary)
		require.True(t, ok, "%+v: decodeGrantIdentityKey failed", id)
		want := encodeGrantByPrincipalIdentityIndexKey(decoded)

		got, ok := rawdb.AppendGrantByPrincipalKeyFromPrimary(nil, primary)
		require.True(t, ok, "%+v: splice failed", id)
		require.Equal(t, want, got, "%+v: spliced index key diverged from decode+re-encode", id)

		// Scratch reuse must not change the output.
		scratch := make([]byte, 0, 256)
		got2, ok := rawdb.AppendGrantByPrincipalKeyFromPrimary(scratch[:0], primary)
		require.True(t, ok)
		require.Equal(t, want, got2)
	}
}

// TestNeedsExpansionKeyHeaderSpliceFromPrimary pins the byte splice the
// id-index migration merge uses instead of decode+re-encode: the
// by_needs_expansion identity index key is exactly the 3-byte index header
// followed by the primary key's sep+tail (the two keys share the identical
// 6-segment tuple encoding).
func TestNeedsExpansionKeyHeaderSpliceFromPrimary(t *testing.T) {
	ids := []grantIdentity{
		{
			entitlement:     entitlementIdentity{resourceTypeID: "group", resourceID: "g1", stripped: true, tail: "member"},
			principalTypeID: "user",
			principalID:     "u1",
		},
		{
			entitlement:     entitlementIdentity{resourceTypeID: "gr\x00oup", resourceID: "g\x011", tail: "\x00k\x00\x01"},
			principalTypeID: "us\x01er",
			principalID:     "u\x00\x001",
		},
		{
			entitlement:     entitlementIdentity{resourceTypeID: "", resourceID: "g1", tail: "member"},
			principalTypeID: "user",
			principalID:     "",
		},
	}
	for _, id := range ids {
		primary := encodeGrantIdentityKey(id)
		spliced := append([]byte{versionV3, typeIndex, idxGrantByNeedsExpansion}, primary[2:]...)
		require.Equal(t, encodeGrantByNeedsExpansionIdentityIndexKey(id), spliced, "identity %+v", id)
	}
}

func TestAppendGrantByPrincipalKeyFromPrimaryRejectsMalformed(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		{versionV3},
		{versionV3, typeGrant},
		{versionV3, typeIndex, 0}, // wrong record type
		{versionV3, typeGrant, 1}, // missing separator
		// Too few segments (three instead of six).
		encodeGrantPrimaryEntitlementResourcePrefix("rt", "rid"),
	}
	for _, key := range cases {
		_, ok := rawdb.AppendGrantByPrincipalKeyFromPrimary(nil, key)
		require.Falsef(t, ok, "key %x: expected splice to reject malformed input", key)
	}
}
