package pebble

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyEncoder_EncodeResourceTypeKey(t *testing.T) {
	ke := NewKeyEncoder()

	tests := []struct {
		name       string
		syncID     string
		externalID string
		want       []byte
	}{
		{
			name:       "simple key",
			syncID:     "sync1",
			externalID: "rt1",
			want:       []byte{0x01, 0x00, 0x01, 0x00, 's', 'y', 'n', 'c', '1', 0x00, 'r', 't', '1'},
		},
		{
			name:       "empty strings",
			syncID:     "",
			externalID: "",
			want:       []byte{0x01, 0x00, 0x01, 0x00, 0x00},
		},
		{
			name:       "with special characters",
			syncID:     "sync-1",
			externalID: "rt_1",
			want:       []byte{0x01, 0x00, 0x01, 0x00, 's', 'y', 'n', 'c', '-', '1', 0x00, 'r', 't', '_', '1'},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ke.EncodeResourceTypeKey(tt.syncID, tt.externalID)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestKeyEncoder_EncodeResourceKey(t *testing.T) {
	ke := NewKeyEncoder()

	tests := []struct {
		name           string
		syncID         string
		resourceTypeID string
		resourceID     string
		want           []byte
	}{
		{
			name:           "simple key",
			syncID:         "sync1",
			resourceTypeID: "rt1",
			resourceID:     "res1",
			want:           []byte{0x01, 0x00, 0x02, 0x00, 's', 'y', 'n', 'c', '1', 0x00, 'r', 't', '1', 0x00, 'r', 'e', 's', '1'},
		},
		{
			name:           "with UUID",
			syncID:         "sync1",
			resourceTypeID: "rt1",
			resourceID:     "550e8400-e29b-41d4-a716-446655440000",
			want:           []byte{0x01, 0x00, 0x02, 0x00, 's', 'y', 'n', 'c', '1', 0x00, 'r', 't', '1', 0x00, '5', '5', '0', 'e', '8', '4', '0', '0', '-', 'e', '2', '9', 'b', '-', '4', '1', 'd', '4', '-', 'a', '7', '1', '6', '-', '4', '4', '6', '6', '5', '5', '4', '4', '0', '0', '0', '0'},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ke.EncodeResourceKey(tt.syncID, tt.resourceTypeID, tt.resourceID)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestKeyEncoder_EscapeSequences(t *testing.T) {
	ke := NewKeyEncoder()

	tests := []struct {
		name       string
		syncID     string
		externalID string
	}{
		{
			name:       "null byte in sync ID",
			syncID:     "sync\x00id",
			externalID: "rt1",
		},
		{
			name:       "escape byte in external ID",
			syncID:     "sync1",
			externalID: "rt\x01id",
		},
		{
			name:       "both null and escape bytes",
			syncID:     "sync\x00\x01id",
			externalID: "rt\x01\x00id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode the key
			encoded := ke.EncodeResourceTypeKey(tt.syncID, tt.externalID)

			// Decode the key
			decoded, err := ke.DecodeKey(encoded)
			require.NoError(t, err)

			// Verify the components match
			assert.Equal(t, KeyTypeResourceType, decoded.KeyType)
			assert.Equal(t, tt.syncID, decoded.GetSyncID())
			assert.Equal(t, tt.externalID, decoded.GetExternalID())
		})
	}
}

func TestKeyEncoder_DecodeKey(t *testing.T) {
	ke := NewKeyEncoder()

	tests := []struct {
		name    string
		key     []byte
		want    *DecodedKey
		wantErr bool
	}{
		{
			name: "resource type key",
			key:  []byte{0x01, 0x00, 0x01, 0x00, 's', 'y', 'n', 'c', '1', 0x00, 'r', 't', '1'},
			want: &DecodedKey{
				Version:    0x01,
				KeyType:    KeyTypeResourceType,
				Components: []string{"sync1", "rt1"},
			},
		},
		{
			name: "resource key",
			key:  []byte{0x01, 0x00, 0x02, 0x00, 's', 'y', 'n', 'c', '1', 0x00, 'r', 't', '1', 0x00, 'r', 'e', 's', '1'},
			want: &DecodedKey{
				Version:    0x01,
				KeyType:    KeyTypeResource,
				Components: []string{"sync1", "rt1", "res1"},
			},
		},
		{
			name:    "key too short",
			key:     []byte{0x01, 0x00},
			wantErr: true,
		},
		{
			name:    "invalid version",
			key:     []byte{0x02, 0x00, 0x01, 0x00, 's', 'y', 'n', 'c', '1'},
			wantErr: true,
		},
		{
			name:    "missing separator after version",
			key:     []byte{0x01, 0x01, 0x01, 0x00, 's', 'y', 'n', 'c', '1'},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ke.DecodeKey(tt.key)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want.Version, got.Version)
			assert.Equal(t, tt.want.KeyType, got.KeyType)
			assert.Equal(t, tt.want.Components, got.Components)
		})
	}
}

func TestKeyEncoder_RoundTrip(t *testing.T) {
	ke := NewKeyEncoder()

	tests := []struct {
		name     string
		keyType  KeyType
		syncID   string
		parts    []string
		encoder  func() []byte
		verifier func(*DecodedKey) bool
	}{
		{
			name:    "resource type",
			keyType: KeyTypeResourceType,
			syncID:  "sync1",
			parts:   []string{"rt1"},
			encoder: func() []byte { return ke.EncodeResourceTypeKey("sync1", "rt1") },
			verifier: func(dk *DecodedKey) bool {
				return dk.GetSyncID() == "sync1" && dk.GetExternalID() == "rt1"
			},
		},
		{
			name:    "resource",
			keyType: KeyTypeResource,
			syncID:  "sync1",
			parts:   []string{"rt1", "res1"},
			encoder: func() []byte { return ke.EncodeResourceKey("sync1", "rt1", "res1") },
			verifier: func(dk *DecodedKey) bool {
				return dk.GetSyncID() == "sync1" && dk.GetResourceTypeID() == "rt1" && dk.GetResourceID() == "res1"
			},
		},
		{
			name:    "entitlement",
			keyType: KeyTypeEntitlement,
			syncID:  "sync1",
			parts:   []string{"ent1"},
			encoder: func() []byte { return ke.EncodeEntitlementKey("sync1", "ent1") },
			verifier: func(dk *DecodedKey) bool {
				return dk.GetSyncID() == "sync1" && dk.GetExternalID() == "ent1"
			},
		},
		{
			name:    "grant",
			keyType: KeyTypeGrant,
			syncID:  "sync1",
			parts:   []string{"grant1"},
			encoder: func() []byte { return ke.EncodeGrantKey("sync1", "grant1") },
			verifier: func(dk *DecodedKey) bool {
				return dk.GetSyncID() == "sync1" && dk.GetExternalID() == "grant1"
			},
		},
		{
			name:    "asset",
			keyType: KeyTypeAsset,
			syncID:  "sync1",
			parts:   []string{"asset1"},
			encoder: func() []byte { return ke.EncodeAssetKey("sync1", "asset1") },
			verifier: func(dk *DecodedKey) bool {
				return dk.GetSyncID() == "sync1" && dk.GetExternalID() == "asset1"
			},
		},
		{
			name:    "sync run",
			keyType: KeyTypeSyncRun,
			syncID:  "sync1",
			parts:   []string{},
			encoder: func() []byte { return ke.EncodeSyncRunKey("sync1") },
			verifier: func(dk *DecodedKey) bool {
				return dk.GetSyncID() == "sync1" && dk.GetExternalID() == "sync1"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := tt.encoder()

			// Decode
			decoded, err := ke.DecodeKey(encoded)
			require.NoError(t, err)

			// Verify
			assert.Equal(t, tt.keyType, decoded.KeyType)
			assert.True(t, tt.verifier(decoded), "verifier failed for decoded key: %+v", decoded)
		})
	}
}

func TestKeyEncoder_SortOrder(t *testing.T) {
	ke := NewKeyEncoder()

	// Create keys that should be in lexicographic order
	keys := [][]byte{
		ke.EncodeResourceTypeKey("sync1", "a"),
		ke.EncodeResourceTypeKey("sync1", "b"),
		ke.EncodeResourceTypeKey("sync1", "c"),
		ke.EncodeResourceTypeKey("sync2", "a"),
		ke.EncodeResourceKey("sync1", "rt1", "a"),
		ke.EncodeResourceKey("sync1", "rt1", "b"),
		ke.EncodeResourceKey("sync1", "rt2", "a"),
		ke.EncodeEntitlementKey("sync1", "a"),
		ke.EncodeGrantKey("sync1", "a"),
		ke.EncodeAssetKey("sync1", "a"),
		ke.EncodeSyncRunKey("sync1"),
	}

	// Verify they are in ascending order
	err := ke.ValidateKeyOrder(keys)
	assert.NoError(t, err)

	// Shuffle and sort to verify sort stability
	shuffled := make([][]byte, len(keys))
	copy(shuffled, keys)

	// Reverse the order to test sorting
	for i := 0; i < len(shuffled)/2; i++ {
		j := len(shuffled) - 1 - i
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	// Sort using bytes.Compare
	sort.Slice(shuffled, func(i, j int) bool {
		return bytes.Compare(shuffled[i], shuffled[j]) < 0
	})

	// Should match original order
	for i, key := range keys {
		assert.Equal(t, key, shuffled[i], "key at index %d doesn't match after sorting", i)
	}
}

func TestKeyEncoder_PrefixKeys(t *testing.T) {
	ke := NewKeyEncoder()

	tests := []struct {
		name     string
		keyType  KeyType
		syncID   string
		expected []byte
	}{
		{
			name:     "resource type prefix",
			keyType:  KeyTypeResourceType,
			syncID:   "sync1",
			expected: []byte{0x01, 0x00, 0x01, 0x00, 's', 'y', 'n', 'c', '1', 0x00},
		},
		{
			name:     "resource prefix",
			keyType:  KeyTypeResource,
			syncID:   "sync1",
			expected: []byte{0x01, 0x00, 0x02, 0x00, 's', 'y', 'n', 'c', '1', 0x00},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ke.EncodePrefixKey(tt.keyType, tt.syncID)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestKeyEncoder_ResourcePrefixKey(t *testing.T) {
	ke := NewKeyEncoder()

	syncID := "sync1"
	resourceTypeID := "rt1"

	prefix := ke.EncodeResourcePrefixKey(syncID, resourceTypeID)

	// Create some resource keys with this prefix
	res1 := ke.EncodeResourceKey(syncID, resourceTypeID, "res1")
	res2 := ke.EncodeResourceKey(syncID, resourceTypeID, "res2")
	res3 := ke.EncodeResourceKey(syncID, "rt2", "res1") // Different resource type

	// Verify that res1 and res2 have the prefix, but res3 doesn't
	assert.True(t, bytes.HasPrefix(res1, prefix))
	assert.True(t, bytes.HasPrefix(res2, prefix))
	assert.False(t, bytes.HasPrefix(res3, prefix))
}

func TestKeyEncoder_IndexKeys(t *testing.T) {
	ke := NewKeyEncoder()

	tests := []struct {
		name       string
		indexType  IndexType
		components []string
		expected   []byte
	}{
		{
			name:       "entitlements by resource",
			indexType:  IndexEntitlementsByResource,
			components: []string{"sync1", "rt1", "res1", "ent1"},
			expected:   []byte{0x01, 0x00, 0x07, 0x00, 0x01, 0x00, 's', 'y', 'n', 'c', '1', 0x00, 'r', 't', '1', 0x00, 'r', 'e', 's', '1', 0x00, 'e', 'n', 't', '1'},
		},
		{
			name:       "grants by principal",
			indexType:  IndexGrantsByPrincipal,
			components: []string{"sync1", "user", "user1", "grant1"},
			expected:   []byte{0x01, 0x00, 0x07, 0x00, 0x03, 0x00, 's', 'y', 'n', 'c', '1', 0x00, 'u', 's', 'e', 'r', 0x00, 'u', 's', 'e', 'r', '1', 0x00, 'g', 'r', 'a', 'n', 't', '1'},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ke.EncodeIndexKey(tt.indexType, tt.components...)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestKeyEncoder_TimestampEncoding(t *testing.T) {
	ke := NewKeyEncoder()

	now := time.Now()
	earlier := now.Add(-time.Hour)
	later := now.Add(time.Hour)

	// Test normal timestamp encoding
	nowBytes := ke.EncodeTimestamp(now)
	earlierBytes := ke.EncodeTimestamp(earlier)
	laterBytes := ke.EncodeTimestamp(later)

	// Earlier timestamps should be lexicographically smaller
	assert.True(t, bytes.Compare(earlierBytes, nowBytes) < 0)
	assert.True(t, bytes.Compare(nowBytes, laterBytes) < 0)

	// Test round-trip
	decodedNow, err := ke.DecodeTimestamp(nowBytes)
	require.NoError(t, err)
	assert.Equal(t, now.UnixNano(), decodedNow.UnixNano())
}

func TestKeyEncoder_UUIDEncoding(t *testing.T) {
	ke := NewKeyEncoder()

	tests := []struct {
		name    string
		uuid    string
		wantErr bool
	}{
		{
			name: "valid UUID",
			uuid: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name: "nil UUID",
			uuid: "00000000-0000-0000-0000-000000000000",
		},
		{
			name:    "invalid UUID",
			uuid:    "invalid-uuid",
			wantErr: true,
		},
		{
			name:    "empty string",
			uuid:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := ke.EncodeUUID(tt.uuid)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Len(t, encoded, 16)

			// Test round-trip
			decoded, err := ke.DecodeUUID(encoded)
			require.NoError(t, err)
			assert.Equal(t, tt.uuid, decoded)
		})
	}
}


func TestKeyEncoder_IndexPrefixKeys(t *testing.T) {
	ke := NewKeyEncoder()

	// Test entitlements by resource prefix
	prefix := ke.EncodeIndexPrefixKey(IndexEntitlementsByResource, "sync1", "rt1", "res1")

	// Create some index keys with this prefix
	ent1 := ke.EncodeEntitlementsByResourceIndexKey("sync1", "rt1", "res1", "ent1")
	ent2 := ke.EncodeEntitlementsByResourceIndexKey("sync1", "rt1", "res1", "ent2")
	ent3 := ke.EncodeEntitlementsByResourceIndexKey("sync1", "rt1", "res2", "ent1") // Different resource

	// Verify that ent1 and ent2 have the prefix, but ent3 doesn't
	assert.True(t, bytes.HasPrefix(ent1, prefix))
	assert.True(t, bytes.HasPrefix(ent2, prefix))
	assert.False(t, bytes.HasPrefix(ent3, prefix))
}

func TestKeyEncoder_LargeDataset(t *testing.T) {
	ke := NewKeyEncoder()

	// Generate a large number of keys to test performance and correctness
	const numKeys = 10000
	keys := make([][]byte, numKeys)

	for i := 0; i < numKeys; i++ {
		syncID := fmt.Sprintf("sync%d", i%100)
		externalID := fmt.Sprintf("rt%d", i)
		keys[i] = ke.EncodeResourceTypeKey(syncID, externalID)
	}

	// Verify all keys can be decoded
	for i, key := range keys {
		decoded, err := ke.DecodeKey(key)
		require.NoError(t, err, "failed to decode key at index %d", i)
		assert.Equal(t, KeyTypeResourceType, decoded.KeyType)
		assert.NotEmpty(t, decoded.GetSyncID())
		assert.NotEmpty(t, decoded.GetExternalID())
	}

	// Verify keys are sortable
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	err := ke.ValidateKeyOrder(keys)
	assert.NoError(t, err)
}

func TestKeyEncoder_EdgeCases(t *testing.T) {
	ke := NewKeyEncoder()

	t.Run("very long strings", func(t *testing.T) {
		longString := string(make([]byte, 10000))
		for i := range longString {
			longString = longString[:i] + "a" + longString[i+1:]
		}

		key := ke.EncodeResourceTypeKey(longString, "rt1")
		decoded, err := ke.DecodeKey(key)
		require.NoError(t, err)
		assert.Equal(t, longString, decoded.GetSyncID())
	})

	t.Run("unicode strings", func(t *testing.T) {
		unicodeSync := "sync-测试-🚀"
		unicodeID := "资源类型-1"

		key := ke.EncodeResourceTypeKey(unicodeSync, unicodeID)
		decoded, err := ke.DecodeKey(key)
		require.NoError(t, err)
		assert.Equal(t, unicodeSync, decoded.GetSyncID())
		assert.Equal(t, unicodeID, decoded.GetExternalID())
	})

	t.Run("all possible escape sequences", func(t *testing.T) {
		// String containing all bytes that need escaping
		testString := "\x00\x01mixed\x00content\x01here"

		key := ke.EncodeResourceTypeKey(testString, testString)
		decoded, err := ke.DecodeKey(key)
		require.NoError(t, err)
		assert.Equal(t, testString, decoded.GetSyncID())
		assert.Equal(t, testString, decoded.GetExternalID())
	})
}

func TestKeyType_String(t *testing.T) {
	tests := []struct {
		keyType KeyType
		want    string
	}{
		{KeyTypeResourceType, "resource_type"},
		{KeyTypeResource, "resource"},
		{KeyTypeEntitlement, "entitlement"},
		{KeyTypeGrant, "grant"},
		{KeyTypeAsset, "asset"},
		{KeyTypeSyncRun, "sync_run"},
		{KeyTypeIndex, "index"},
		{KeyTypeCounter, "counter"},
		{KeyType(99), "unknown(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.keyType.String())
		})
	}
}

func TestIndexType_String(t *testing.T) {
	tests := []struct {
		indexType IndexType
		want      string
	}{
		{IndexEntitlementsByResource, "entitlements_by_resource"},
		{IndexGrantsByResource, "grants_by_resource"},
		{IndexGrantsByPrincipal, "grants_by_principal"},
		{IndexGrantsByEntitlement, "grants_by_entitlement"},
		{IndexType(99), "unknown(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.indexType.String())
		})
	}
}

func TestDecodedKey_String(t *testing.T) {
	ke := NewKeyEncoder()

	key := ke.EncodeResourceKey("sync1", "rt1", "res1")
	decoded, err := ke.DecodeKey(key)
	require.NoError(t, err)

	expected := "v1|resource|sync1|rt1|res1"
	assert.Equal(t, expected, decoded.String())
}

// Benchmark tests
func BenchmarkKeyEncoder_EncodeResourceTypeKey(b *testing.B) {
	ke := NewKeyEncoder()
	syncID := "sync1"
	externalID := "rt1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ke.EncodeResourceTypeKey(syncID, externalID)
	}
}

func BenchmarkKeyEncoder_DecodeKey(b *testing.B) {
	ke := NewKeyEncoder()
	key := ke.EncodeResourceTypeKey("sync1", "rt1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ke.DecodeKey(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKeyEncoder_EncodeResourceKey(b *testing.B) {
	ke := NewKeyEncoder()
	syncID := "sync1"
	resourceTypeID := "rt1"
	resourceID := "res1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ke.EncodeResourceKey(syncID, resourceTypeID, resourceID)
	}
}
