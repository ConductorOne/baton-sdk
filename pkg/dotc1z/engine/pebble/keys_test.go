package pebble

import (
	"bytes"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// TestEncodersOmitSyncID pins the single-sync contract at the byte
// level: the key encoders no longer embed the 20-byte sync_id, so
// encoding the same logical key under two different sync_ids yields
// identical bytes, and a primary key is exactly header + 0x00 separator
// + tuple tail.
func TestEncodersOmitSyncID(t *testing.T) {
	syncA, err := codec.EncodeSyncID(ksuid.New().String())
	require.NoError(t, err)
	syncB, err := codec.EncodeSyncID(ksuid.New().String())
	require.NoError(t, err)
	require.False(t, bytes.Equal(syncA, syncB), "sanity: two fresh KSUIDs encoded equal")

	// Primary grant key: v3 | typeGrant | 0x00 | tuple("ext-1"). No
	// 20-byte sync_id region, and sync-independent.
	keyA := encodeGrantKey("ext-1")
	keyB := encodeGrantKey("ext-1")
	require.True(t, bytes.Equal(keyA, keyB), "grant key depends on sync_id: %x vs %x", keyA, keyB)
	require.Equal(t, 2+1+len("ext-1"), len(keyA), "grant key length (header|0x00|tail, no sync_id)")
	require.Equal(t, []byte{versionV3, typeGrant, 0x00}, keyA[:3], "grant key prefix")

	// Index keys are likewise sync-independent.
	require.True(t, bytes.Equal(
		encodeGrantByEntitlementIndexKey("ent", "user", "u1", "ext-1"),
		encodeGrantByEntitlementIndexKey("ent", "user", "u1", "ext-1"),
	), "by_entitlement index key depends on sync_id")

	// Sync-run and stats-sidecar keys collapse to fixed keys.
	require.True(t, bytes.Equal(encodeSyncRunKey(), encodeSyncRunKey()), "sync-run key depends on sync_id")
	require.True(t, bytes.Equal(encodeSyncStatsKey(), encodeSyncStatsKey()), "stats sidecar key depends on sync_id")
}

func TestUpperBoundOf(t *testing.T) {
	tests := []struct {
		name   string
		prefix []byte
		want   []byte
	}{
		{
			name:   "increments last byte",
			prefix: []byte{0x03, 0x10, 0x20},
			want:   []byte{0x03, 0x10, 0x21},
		},
		{
			name:   "carries through trailing ff bytes",
			prefix: []byte{0x03, 0x10, 0xff, 0xff},
			want:   []byte{0x03, 0x11},
		},
		{
			name:   "all ff has no finite upper bound",
			prefix: []byte{0xff, 0xff},
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := upperBoundOf(tt.prefix)
			require.Equal(t, tt.want, got, "upperBoundOf(%x)", tt.prefix)
		})
	}
}
