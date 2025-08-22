package pebble

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// KeyEncoder handles binary key encoding/decoding with proper sorting and prefix support.
type KeyEncoder struct {
	version byte // v1 = 0x01
}

// NewKeyEncoder creates a new KeyEncoder instance.
func NewKeyEncoder() *KeyEncoder {
	return &KeyEncoder{
		version: 0x01, // v1
	}
}

// KeyType represents the type of entity being stored.
type KeyType byte

const (
	KeyTypeResourceType KeyType = 0x01 // rt
	KeyTypeResource     KeyType = 0x02 // rs
	KeyTypeEntitlement  KeyType = 0x03 // en
	KeyTypeGrant        KeyType = 0x04 // gr
	KeyTypeAsset        KeyType = 0x05 // as
	KeyTypeSyncRun      KeyType = 0x06 // sr
	KeyTypeIndex        KeyType = 0x07 // ix
	KeyTypeCounter      KeyType = 0x08 // ct
)

// IndexType represents the type of secondary index.
type IndexType byte

const (
	IndexEntitlementsByResource IndexType = 0x01 // en_by_res
	IndexGrantsByResource       IndexType = 0x02 // gr_by_res
	IndexGrantsByPrincipal      IndexType = 0x03 // gr_by_prn
	IndexGrantsByEntitlement    IndexType = 0x04 // gr_by_ent
)

const (
	separator = byte(0x00)
	escape    = byte(0x01)
	escapeNul = byte(0x01)
	escapeEsc = byte(0x02)
)

// EncodeResourceTypeKey encodes a key for a resource type.
// Format: v1|rt|{sync_id}|{external_id}
func (ke *KeyEncoder) EncodeResourceTypeKey(syncID, externalID string) []byte {
	var buf bytes.Buffer
	buf.WriteByte(ke.version)
	buf.WriteByte(separator)
	buf.WriteByte(byte(KeyTypeResourceType))
	buf.WriteByte(separator)
	ke.writeEscapedString(&buf, syncID)
	buf.WriteByte(separator)
	ke.writeEscapedString(&buf, externalID)
	return buf.Bytes()
}

// EncodeResourceKey encodes a key for a resource.
// Format: v1|rs|{sync_id}|{resource_type_id}|{resource_id}
func (ke *KeyEncoder) EncodeResourceKey(syncID, resourceTypeID, resourceID string) []byte {
	var buf bytes.Buffer
	buf.WriteByte(ke.version)
	buf.WriteByte(separator)
	buf.WriteByte(byte(KeyTypeResource))
	buf.WriteByte(separator)
	ke.writeEscapedString(&buf, syncID)
	buf.WriteByte(separator)
	ke.writeEscapedString(&buf, resourceTypeID)
	buf.WriteByte(separator)
	ke.writeEscapedString(&buf, resourceID)
	return buf.Bytes()
}

// EncodeEntitlementKey encodes a key for an entitlement.
// Format: v1|en|{sync_id}|{external_id}
func (ke *KeyEncoder) EncodeEntitlementKey(syncID, externalID string) []byte {
	var buf bytes.Buffer
	buf.WriteByte(ke.version)
	buf.WriteByte(separator)
	buf.WriteByte(byte(KeyTypeEntitlement))
	buf.WriteByte(separator)
	ke.writeEscapedString(&buf, syncID)
	buf.WriteByte(separator)
	ke.writeEscapedString(&buf, externalID)
	return buf.Bytes()
}

// EncodeGrantKey encodes a key for a grant.
// Format: v1|gr|{sync_id}|{external_id}
func (ke *KeyEncoder) EncodeGrantKey(syncID, externalID string) []byte {
	var buf bytes.Buffer
	buf.WriteByte(ke.version)
	buf.WriteByte(separator)
	buf.WriteByte(byte(KeyTypeGrant))
	buf.WriteByte(separator)
	ke.writeEscapedString(&buf, syncID)
	buf.WriteByte(separator)
	ke.writeEscapedString(&buf, externalID)
	return buf.Bytes()
}

// EncodeAssetKey encodes a key for an asset.
// Format: v1|as|{sync_id}|{external_id}
func (ke *KeyEncoder) EncodeAssetKey(syncID, externalID string) []byte {
	var buf bytes.Buffer
	buf.WriteByte(ke.version)
	buf.WriteByte(separator)
	buf.WriteByte(byte(KeyTypeAsset))
	buf.WriteByte(separator)
	ke.writeEscapedString(&buf, syncID)
	buf.WriteByte(separator)
	ke.writeEscapedString(&buf, externalID)
	return buf.Bytes()
}

// EncodeSyncRunKey encodes a key for a sync run.
// Format: v1|sr|{sync_id}
func (ke *KeyEncoder) EncodeSyncRunKey(syncID string) []byte {
	var buf bytes.Buffer
	buf.WriteByte(ke.version)
	buf.WriteByte(separator)
	buf.WriteByte(byte(KeyTypeSyncRun))
	buf.WriteByte(separator)
	ke.writeEscapedString(&buf, syncID)
	return buf.Bytes()
}

// EncodeIndexKey encodes a key for a secondary index.
func (ke *KeyEncoder) EncodeIndexKey(indexType IndexType, components ...string) []byte {
	var buf bytes.Buffer
	buf.WriteByte(ke.version)
	buf.WriteByte(separator)
	buf.WriteByte(byte(KeyTypeIndex))
	buf.WriteByte(separator)
	buf.WriteByte(byte(indexType))

	for _, component := range components {
		buf.WriteByte(separator)
		ke.writeEscapedString(&buf, component)
	}

	return buf.Bytes()
}

// EncodeEntitlementsByResourceIndexKey encodes an index key for entitlements by resource.
// Format: v1|ix|en_by_res|{sync_id}|{resource_type_id}|{resource_id}|{external_id}
func (ke *KeyEncoder) EncodeEntitlementsByResourceIndexKey(syncID, resourceTypeID, resourceID, externalID string) []byte {
	return ke.EncodeIndexKey(IndexEntitlementsByResource, syncID, resourceTypeID, resourceID, externalID)
}

// EncodeGrantsByResourceIndexKey encodes an index key for grants by resource.
// Format: v1|ix|gr_by_res|{sync_id}|{resource_type_id}|{resource_id}|{external_id}
func (ke *KeyEncoder) EncodeGrantsByResourceIndexKey(syncID, resourceTypeID, resourceID, externalID string) []byte {
	return ke.EncodeIndexKey(IndexGrantsByResource, syncID, resourceTypeID, resourceID, externalID)
}

// EncodeGrantsByPrincipalIndexKey encodes an index key for grants by principal.
// Format: v1|ix|gr_by_prn|{sync_id}|{principal_type}|{principal_id}|{external_id}
func (ke *KeyEncoder) EncodeGrantsByPrincipalIndexKey(syncID, principalType, principalID, externalID string) []byte {
	return ke.EncodeIndexKey(IndexGrantsByPrincipal, syncID, principalType, principalID, externalID)
}

// EncodeGrantsByEntitlementIndexKey encodes an index key for grants by entitlement.
// Format: v1|ix|gr_by_ent|{sync_id}|{entitlement_id}|{principal_type}|{principal_id}|{external_id}
func (ke *KeyEncoder) EncodeGrantsByEntitlementIndexKey(syncID, entitlementID, principalType, principalID, externalID string) []byte {
	return ke.EncodeIndexKey(IndexGrantsByEntitlement, syncID, entitlementID, principalType, principalID, externalID)
}


// EncodePrefixKey encodes a prefix key for range scans.
func (ke *KeyEncoder) EncodePrefixKey(keyType KeyType, syncID string) []byte {
	var buf bytes.Buffer
	buf.WriteByte(ke.version)
	buf.WriteByte(separator)
	buf.WriteByte(byte(keyType))
	buf.WriteByte(separator)
	ke.writeEscapedString(&buf, syncID)
	buf.WriteByte(separator)
	return buf.Bytes()
}

// EncodeResourcePrefixKey encodes a prefix key for resources of a specific type.
func (ke *KeyEncoder) EncodeResourcePrefixKey(syncID, resourceTypeID string) []byte {
	var buf bytes.Buffer
	buf.WriteByte(ke.version)
	buf.WriteByte(separator)
	buf.WriteByte(byte(KeyTypeResource))
	buf.WriteByte(separator)
	ke.writeEscapedString(&buf, syncID)
	buf.WriteByte(separator)
	ke.writeEscapedString(&buf, resourceTypeID)
	buf.WriteByte(separator)
	return buf.Bytes()
}

// EncodeIndexPrefixKey encodes a prefix key for index range scans.
func (ke *KeyEncoder) EncodeIndexPrefixKey(indexType IndexType, components ...string) []byte {
	var buf bytes.Buffer
	buf.WriteByte(ke.version)
	buf.WriteByte(separator)
	buf.WriteByte(byte(KeyTypeIndex))
	buf.WriteByte(separator)
	buf.WriteByte(byte(indexType))

	for _, component := range components {
		buf.WriteByte(separator)
		ke.writeEscapedString(&buf, component)
	}

	buf.WriteByte(separator)
	return buf.Bytes()
}

// DecodeKey decodes a key and returns its components.
func (ke *KeyEncoder) DecodeKey(key []byte) (*DecodedKey, error) {
	if len(key) < 3 {
		return nil, fmt.Errorf("key too short")
	}

	if key[0] != ke.version {
		return nil, fmt.Errorf("unsupported key version: %d", key[0])
	}

	if key[1] != separator {
		return nil, fmt.Errorf("invalid key format: missing separator after version")
	}

	keyType := KeyType(key[2])

	// Check for separator after key type
	if len(key) < 4 || key[3] != separator {
		return nil, fmt.Errorf("invalid key format: missing separator after key type")
	}

	var components []string
	var err error

	if keyType == KeyTypeIndex {
		// Index keys have an additional indexType byte
		if len(key) < 6 || key[5] != separator {
			return nil, fmt.Errorf("invalid index key format: missing separator after index type")
		}
		// Skip version, separator, keyType, separator, indexType, separator
		components, err = ke.splitKey(key[6:])
	} else {
		// Regular keys
		components, err = ke.splitKey(key[4:])
	}

	if err != nil {
		return nil, fmt.Errorf("failed to split key components: %w", err)
	}

	return &DecodedKey{
		Version:    key[0],
		KeyType:    keyType,
		Components: components,
	}, nil
}

// DecodedKey represents a decoded key with its components.
type DecodedKey struct {
	Version    byte
	KeyType    KeyType
	Components []string
}

// GetSyncID returns the sync ID from the decoded key.
func (dk *DecodedKey) GetSyncID() string {
	if len(dk.Components) > 0 {
		return dk.Components[0]
	}
	return ""
}

// GetExternalID returns the external ID from the decoded key.
func (dk *DecodedKey) GetExternalID() string {
	switch dk.KeyType {
	case KeyTypeResourceType, KeyTypeEntitlement, KeyTypeGrant, KeyTypeAsset:
		if len(dk.Components) > 1 {
			return dk.Components[1]
		}
	case KeyTypeResource:
		if len(dk.Components) > 2 {
			return dk.Components[2]
		}
	case KeyTypeSyncRun:
		if len(dk.Components) > 0 {
			return dk.Components[0] // For sync runs, the sync ID is the external ID
		}
	}
	return ""
}

// GetResourceTypeID returns the resource type ID from a resource key.
func (dk *DecodedKey) GetResourceTypeID() string {
	if dk.KeyType == KeyTypeResource && len(dk.Components) > 1 {
		return dk.Components[1]
	}
	return ""
}

// GetResourceID returns the resource ID from a resource key.
func (dk *DecodedKey) GetResourceID() string {
	if dk.KeyType == KeyTypeResource && len(dk.Components) > 2 {
		return dk.Components[2]
	}
	return ""
}

// writeEscapedString writes a string to the buffer with proper escaping.
func (ke *KeyEncoder) writeEscapedString(buf *bytes.Buffer, s string) {
	for i := 0; i < len(s); i++ {
		b := s[i]
		switch b {
		case separator:
			buf.WriteByte(escape)
			buf.WriteByte(escapeNul)
		case escape:
			buf.WriteByte(escape)
			buf.WriteByte(escapeEsc)
		default:
			buf.WriteByte(b)
		}
	}
}

// splitKey splits a key into components, handling escape sequences.
func (ke *KeyEncoder) splitKey(key []byte) ([]string, error) {
	var components []string
	var current bytes.Buffer

	i := 0
	for i < len(key) {
		if key[i] == separator {
			components = append(components, current.String())
			current.Reset()
			i++
			continue
		}

		if key[i] == escape {
			if i+1 >= len(key) {
				return nil, fmt.Errorf("incomplete escape sequence at end of key")
			}

			switch key[i+1] {
			case escapeNul:
				current.WriteByte(separator)
			case escapeEsc:
				current.WriteByte(escape)
			default:
				return nil, fmt.Errorf("invalid escape sequence: %02x %02x", key[i], key[i+1])
			}
			i += 2
			continue
		}

		current.WriteByte(key[i])
		i++
	}

	// Add the last component if there's remaining data
	if current.Len() > 0 {
		components = append(components, current.String())
	}

	return components, nil
}

// EncodeUUID encodes a UUID as a 16-byte binary representation for efficient storage.
func (ke *KeyEncoder) EncodeUUID(id string) ([]byte, error) {
	u, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("invalid UUID: %w", err)
	}
	return u[:], nil
}

// DecodeUUID decodes a 16-byte binary representation back to a UUID string.
func (ke *KeyEncoder) DecodeUUID(data []byte) (string, error) {
	if len(data) != 16 {
		return "", fmt.Errorf("invalid UUID data length: %d", len(data))
	}

	var u uuid.UUID
	copy(u[:], data)
	return u.String(), nil
}

// EncodeTimestamp encodes a timestamp as a big-endian int64 for proper sorting.
func (ke *KeyEncoder) EncodeTimestamp(t time.Time) []byte {
	timestamp := t.UnixNano()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(timestamp))
	return buf
}


// DecodeTimestamp decodes a big-endian int64 back to a timestamp.
func (ke *KeyEncoder) DecodeTimestamp(data []byte) (time.Time, error) {
	if len(data) != 8 {
		return time.Time{}, fmt.Errorf("invalid timestamp data length: %d", len(data))
	}

	timestamp := int64(binary.BigEndian.Uint64(data))
	return time.Unix(0, timestamp), nil
}


// ValidateKeyOrder validates that keys are in proper lexicographic order.
func (ke *KeyEncoder) ValidateKeyOrder(keys [][]byte) error {
	for i := 1; i < len(keys); i++ {
		if bytes.Compare(keys[i-1], keys[i]) >= 0 {
			return fmt.Errorf("keys not in ascending order at index %d", i)
		}
	}
	return nil
}

// KeyTypeString returns a string representation of the key type.
func (kt KeyType) String() string {
	switch kt {
	case KeyTypeResourceType:
		return "resource_type"
	case KeyTypeResource:
		return "resource"
	case KeyTypeEntitlement:
		return "entitlement"
	case KeyTypeGrant:
		return "grant"
	case KeyTypeAsset:
		return "asset"
	case KeyTypeSyncRun:
		return "sync_run"
	case KeyTypeIndex:
		return "index"
	case KeyTypeCounter:
		return "counter"
	default:
		return fmt.Sprintf("unknown(%d)", byte(kt))
	}
}

// IndexTypeString returns a string representation of the index type.
func (it IndexType) String() string {
	switch it {
	case IndexEntitlementsByResource:
		return "entitlements_by_resource"
	case IndexGrantsByResource:
		return "grants_by_resource"
	case IndexGrantsByPrincipal:
		return "grants_by_principal"
	case IndexGrantsByEntitlement:
		return "grants_by_entitlement"
	default:
		return fmt.Sprintf("unknown(%d)", byte(it))
	}
}

// String returns a human-readable representation of the decoded key.
func (dk *DecodedKey) String() string {
	var parts []string
	parts = append(parts, fmt.Sprintf("v%d", dk.Version))
	parts = append(parts, dk.KeyType.String())
	parts = append(parts, dk.Components...)
	return strings.Join(parts, "|")
}
