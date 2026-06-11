package pebble

import (
	"bytes"
	"testing"

	"github.com/segmentio/ksuid"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// TestEncodersOmitSyncID pins the single-sync contract at the byte
// level: the key encoders no longer embed the 20-byte sync_id, so
// encoding the same logical key under two different sync_ids yields
// identical bytes, and a primary key is exactly header + 0x00 separator
// + tuple tail.
func TestEncodersOmitSyncID(t *testing.T) {
	syncA, err := codec.EncodeSyncID(ksuid.New().String())
	if err != nil {
		t.Fatal(err)
	}
	syncB, err := codec.EncodeSyncID(ksuid.New().String())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(syncA, syncB) {
		t.Fatal("sanity: two fresh KSUIDs encoded equal")
	}

	// Primary grant key: v3 | typeGrant | 0x00 | tuple("ext-1"). No
	// 20-byte sync_id region, and sync-independent.
	keyA := encodeGrantKey(syncA, "ext-1")
	keyB := encodeGrantKey(syncB, "ext-1")
	if !bytes.Equal(keyA, keyB) {
		t.Errorf("grant key depends on sync_id: %x vs %x", keyA, keyB)
	}
	if got, want := len(keyA), 2+1+len("ext-1"); got != want {
		t.Errorf("grant key length = %d, want %d (header|0x00|tail, no sync_id)", got, want)
	}
	if keyA[0] != versionV3 || keyA[1] != typeGrant || keyA[2] != 0x00 {
		t.Errorf("grant key prefix = %x, want %x", keyA[:3], []byte{versionV3, typeGrant, 0x00})
	}

	// Index keys are likewise sync-independent.
	if !bytes.Equal(
		encodeGrantByEntitlementIndexKey(syncA, "ent", "user", "u1", "ext-1"),
		encodeGrantByEntitlementIndexKey(syncB, "ent", "user", "u1", "ext-1"),
	) {
		t.Error("by_entitlement index key depends on sync_id")
	}

	// Sync-run and stats-sidecar keys collapse to fixed keys.
	if !bytes.Equal(encodeSyncRunKey(syncA), encodeSyncRunKey(syncB)) {
		t.Error("sync-run key depends on sync_id")
	}
	if !bytes.Equal(encodeSyncStatsKey(syncA), encodeSyncStatsKey(syncB)) {
		t.Error("stats sidecar key depends on sync_id")
	}
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
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("upperBoundOf(%x) = %x, want %x", tt.prefix, got, tt.want)
			}
		})
	}
}
