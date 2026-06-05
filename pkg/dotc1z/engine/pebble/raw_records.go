package pebble

import (
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	resourceTypeDiscoveredAtField protowire.Number = 6
	resourceDiscoveredAtField     protowire.Number = 8
	entitlementDiscoveredAtField  protowire.Number = 8
	grantDiscoveredAtField        protowire.Number = 5
)

func discoveredAtIsNewerThanRaw(incoming *timestamppb.Timestamp, existingValue []byte, field protowire.Number) (bool, error) {
	if incoming == nil {
		return false, nil
	}
	existing, ok, err := rawDiscoveredAtNanos(existingValue, field)
	if err != nil {
		return false, err
	}
	if !ok {
		return true, nil
	}
	return incoming.AsTime().UnixNano() > existing, nil
}

func rawDiscoveredAtNanos(value []byte, field protowire.Number) (int64, bool, error) {
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return 0, false, protowire.ParseError(n)
		}
		value = value[n:]
		if num != field {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return 0, false, protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		if typ != protowire.BytesType {
			return 0, false, fmt.Errorf("raw record: discovered_at has wire type %v", typ)
		}
		ts, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return 0, false, protowire.ParseError(n)
		}
		nanos, err := rawTimestampNanos(ts)
		return nanos, true, err
	}
	return 0, false, nil
}

func rawTimestampNanos(value []byte) (int64, error) {
	var seconds int64
	var nanos int32
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		value = value[n:]
		switch num {
		case 1:
			if typ != protowire.VarintType {
				return 0, fmt.Errorf("raw record: timestamp seconds has wire type %v", typ)
			}
			v, n := protowire.ConsumeVarint(value)
			if n < 0 {
				return 0, protowire.ParseError(n)
			}
			if v > math.MaxInt64 {
				return 0, fmt.Errorf("raw record: timestamp seconds exceeds int64: %d", v)
			}
			seconds = int64(v)
			value = value[n:]
		case 2:
			if typ != protowire.VarintType {
				return 0, fmt.Errorf("raw record: timestamp nanos has wire type %v", typ)
			}
			v, n := protowire.ConsumeVarint(value)
			if n < 0 {
				return 0, protowire.ParseError(n)
			}
			if v > math.MaxInt32 {
				return 0, fmt.Errorf("raw record: timestamp nanos exceeds int32: %d", v)
			}
			nanos = int32(v)
			value = value[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return 0, protowire.ParseError(n)
			}
			value = value[n:]
		}
	}
	if seconds > math.MaxInt64/int64(time.Second) {
		return 0, fmt.Errorf("raw record: timestamp seconds overflow: %d", seconds)
	}
	return seconds*int64(time.Second) + int64(nanos), nil
}

func (e *Engine) deleteResourceIndexesRaw(batch *pebble.Batch, syncIDBytes []byte, resourceTypeID string, resourceID string, value []byte) error {
	parentRT, parentID, err := scanResourceParentRaw(value)
	if err != nil {
		return err
	}
	if parentID == "" {
		return nil
	}
	return batch.Delete(encodeResourceByParentIndexKey(syncIDBytes, parentRT, parentID, resourceTypeID, resourceID), nil)
}

func (e *Engine) deleteEntitlementIndexesRaw(batch *pebble.Batch, syncIDBytes []byte, externalID string, value []byte) error {
	resourceRT, resourceID, err := scanEntitlementResourceRaw(value)
	if err != nil {
		return err
	}
	if resourceID == "" {
		return nil
	}
	return batch.Delete(encodeEntitlementByResourceIndexKey(syncIDBytes, resourceRT, resourceID, externalID), nil)
}

func (e *Engine) deleteGrantIndexesRaw(batch *pebble.Batch, syncIDBytes []byte, externalID string, value []byte) error {
	entRT, entRID, entID, principalRT, principalID, _, err := scanGrantIndexFieldsRaw(value)
	if err != nil {
		return err
	}
	if entID != "" && principalRT != "" && principalID != "" {
		if err := batch.Delete(encodeGrantByEntitlementIndexKey(syncIDBytes, entID, principalRT, principalID, externalID), nil); err != nil {
			return err
		}
	}
	if entRID != "" {
		if err := batch.Delete(encodeGrantByEntitlementResourceIndexKey(syncIDBytes, entRT, entRID, externalID), nil); err != nil {
			return err
		}
	}
	if principalRT != "" && principalID != "" {
		if err := batch.Delete(encodeGrantByPrincipalIndexKey(syncIDBytes, principalRT, principalID, externalID), nil); err != nil {
			return err
		}
		if err := batch.Delete(encodeGrantByPrincipalResourceTypeIndexKey(syncIDBytes, principalRT, externalID), nil); err != nil {
			return err
		}
	}
	return batch.Delete(encodeGrantByNeedsExpansionIndexKey(syncIDBytes, externalID), nil)
}

// scanGrantEntitlementResourceTypeRaw extracts only the entitlement's
// resource_type_id from a marshaled GrantRecord, borrowing the bytes
// from value. The stats grouping path needs just this one field;
// scanGrantIndexFieldsRaw materializes five strings per grant. Like
// the full scanner, the last occurrence of the entitlement field wins.
func scanGrantEntitlementResourceTypeRaw(value []byte) ([]byte, error) {
	var entRT []byte
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		value = value[n:]
		if num != 3 {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		if typ != protowire.BytesType {
			return nil, fmt.Errorf("raw record: grant entitlement has wire type %v", typ)
		}
		msg, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		if err := scanResourceRefRawBytes(msg, func(fnum protowire.Number, val []byte) {
			if fnum == 1 {
				entRT = val
			}
		}); err != nil {
			return nil, err
		}
		value = value[n:]
	}
	return entRT, nil
}

// scanResourceParentRaw and scanEntitlementResourceRaw keep the LAST
// occurrence of the target field, matching scanGrantIndexFieldsRaw and
// approximating proto merge semantics. Values written by this SDK carry
// at most one occurrence, so this only matters for foreign writers.
func scanResourceParentRaw(value []byte) (string, string, error) {
	var rt, id string
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return "", "", protowire.ParseError(n)
		}
		value = value[n:]
		if num != 6 {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return "", "", protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		if typ != protowire.BytesType {
			return "", "", fmt.Errorf("raw record: resource parent has wire type %v", typ)
		}
		msg, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return "", "", protowire.ParseError(n)
		}
		var err error
		rt, id, err = scanResourceRefRaw(msg)
		if err != nil {
			return "", "", err
		}
		value = value[n:]
	}
	return rt, id, nil
}

func scanEntitlementResourceRaw(value []byte) (string, string, error) {
	var rt, id string
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return "", "", protowire.ParseError(n)
		}
		value = value[n:]
		if num != 3 {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return "", "", protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		if typ != protowire.BytesType {
			return "", "", fmt.Errorf("raw record: entitlement resource has wire type %v", typ)
		}
		msg, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return "", "", protowire.ParseError(n)
		}
		var err error
		rt, id, err = scanResourceRefRaw(msg)
		if err != nil {
			return "", "", err
		}
		value = value[n:]
	}
	return rt, id, nil
}

func scanGrantIndexFieldsRaw(value []byte) (string, string, string, string, string, bool, error) {
	var entRT, entRID, entID, principalRT, principalID string
	var needsExpansion bool
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return "", "", "", "", "", false, protowire.ParseError(n)
		}
		value = value[n:]
		switch num {
		case 3:
			if typ != protowire.BytesType {
				return "", "", "", "", "", false, fmt.Errorf("raw record: grant entitlement has wire type %v", typ)
			}
			msg, n := protowire.ConsumeBytes(value)
			if n < 0 {
				return "", "", "", "", "", false, protowire.ParseError(n)
			}
			var err error
			entRT, entRID, entID, err = scanEntitlementRefRaw(msg)
			if err != nil {
				return "", "", "", "", "", false, err
			}
			value = value[n:]
		case 4:
			if typ != protowire.BytesType {
				return "", "", "", "", "", false, fmt.Errorf("raw record: grant principal has wire type %v", typ)
			}
			msg, n := protowire.ConsumeBytes(value)
			if n < 0 {
				return "", "", "", "", "", false, protowire.ParseError(n)
			}
			var err error
			principalRT, principalID, err = scanPrincipalRefRaw(msg)
			if err != nil {
				return "", "", "", "", "", false, err
			}
			value = value[n:]
		case 7:
			if typ != protowire.VarintType {
				return "", "", "", "", "", false, fmt.Errorf("raw record: grant needs_expansion has wire type %v", typ)
			}
			v, n := protowire.ConsumeVarint(value)
			if n < 0 {
				return "", "", "", "", "", false, protowire.ParseError(n)
			}
			needsExpansion = v != 0
			value = value[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return "", "", "", "", "", false, protowire.ParseError(n)
			}
			value = value[n:]
		}
	}
	return entRT, entRID, entID, principalRT, principalID, needsExpansion, nil
}

func scanResourceRefRaw(value []byte) (string, string, error) {
	var rt, id []byte
	err := scanResourceRefRawBytes(value, func(num protowire.Number, val []byte) {
		switch num {
		case 1:
			rt = val
		case 2:
			id = val
		default:
		}
	})
	return string(rt), string(id), err
}

func scanEntitlementRefRaw(value []byte) (string, string, string, error) {
	var rt, rid, eid []byte
	err := scanResourceRefRawBytes(value, func(num protowire.Number, val []byte) {
		switch num {
		case 1:
			rt = val
		case 2:
			rid = val
		case 3:
			eid = val
		default:
		}
	})
	return string(rt), string(rid), string(eid), err
}

func scanPrincipalRefRaw(value []byte) (string, string, error) {
	var rt, id []byte
	err := scanResourceRefRawBytes(value, func(num protowire.Number, val []byte) {
		switch num {
		case 1:
			rt = val
		case 2:
			id = val
		default:
		}
	})
	return string(rt), string(id), err
}

func scanResourceRefRawBytes(value []byte, set func(protowire.Number, []byte)) error {
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return protowire.ParseError(n)
		}
		value = value[n:]
		if typ != protowire.BytesType {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		b, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return protowire.ParseError(n)
		}
		switch num {
		case 1, 2, 3:
			set(num, b)
		default:
		}
		value = value[n:]
	}
	return nil
}
