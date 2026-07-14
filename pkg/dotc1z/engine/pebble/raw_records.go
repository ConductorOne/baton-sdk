package pebble

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
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

func (e *Engine) deleteResourceIndexesRaw(batch *pebble.Batch, resourceTypeID string, resourceID string, value []byte) error {
	parentRT, parentID, sourceScopeHash, err := scanResourceIndexFieldsRaw(value)
	if err != nil {
		return err
	}
	if parentID != "" {
		if err := batch.Delete(encodeResourceByParentIndexKey(parentRT, parentID, resourceTypeID, resourceID), nil); err != nil {
			return err
		}
	}
	if sourceScopeHash != "" {
		if err := batch.Delete(encodeResourceBySourceScopeIndexKey(sourceScopeHash, resourceTypeID, resourceID), nil); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) deleteGrantIndexesRaw(batch *pebble.Batch, externalID string, value []byte) error {
	entRT, entRID, entID, principalRT, principalID, _, sourceScopeHash, err := scanGrantIndexFieldsRaw(value)
	if err != nil {
		return err
	}
	if entID == "" || entRT == "" || entRID == "" || principalRT == "" || principalID == "" {
		return nil
	}
	id := grantIdentity{
		entitlement:     entitlementIdentityFromParts(entRT, entRID, entID),
		principalTypeID: principalRT,
		principalID:     principalID,
	}
	if err := batch.Delete(encodeGrantByPrincipalIdentityIndexKey(id), nil); err != nil {
		return err
	}
	// Post-seal mutation of a grant invalidates the touched entitlement's
	// digest + hash-index state (no-op unless digests exist — see
	// stageGrantDigestInvalidation).
	if err := e.stageGrantDigestInvalidation(batch, id.entitlement); err != nil {
		return err
	}
	if sourceScopeHash != "" {
		if err := batch.Delete(encodeGrantBySourceScopeIndexKey(sourceScopeHash, id), nil); err != nil {
			return err
		}
	}
	return batch.Delete(encodeGrantByNeedsExpansionIdentityIndexKey(id), nil)
}

// scanGrantExternalIDRaw extracts only the stored external_id (field 2)
// from a marshaled GrantRecord. Used by the bare-id grant lookup to check
// a probe hit's public id without a full unmarshal. Last occurrence wins,
// matching the full scanners.
func scanGrantExternalIDRaw(value []byte) (string, error) {
	var externalID string
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return "", protowire.ParseError(n)
		}
		value = value[n:]
		if num != 2 {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return "", protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		if typ != protowire.BytesType {
			return "", fmt.Errorf("raw record: grant external_id has wire type %v", typ)
		}
		v, n := protowire.ConsumeString(value)
		if n < 0 {
			return "", protowire.ParseError(n)
		}
		externalID = v
		value = value[n:]
	}
	return externalID, nil
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

// scanGrantSourceKeysRawBytes extracts the source-entitlement ID keys
// from a marshaled GrantRecord without a full unmarshal. Sources are
// field 9 (map<string, GrantSourceRecord>), encoded as repeated embedded
// messages each with sub-field 1 = key string. The keys are views
// borrowed from value (valid only while value's backing bytes are),
// appended to keys — pass a recycled keys[:0] to reuse its backing
// array across calls. The seal-time grant digest build calls this once
// per grant (see appendGrantHashIndexRow).
func scanGrantSourceKeysRawBytes(value []byte, keys [][]byte) ([][]byte, error) {
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		value = value[n:]
		if num != 9 {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		if typ != protowire.BytesType {
			return nil, fmt.Errorf("raw record: grant sources entry has wire type %v", typ)
		}
		entry, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		value = value[n:]
		for len(entry) > 0 {
			eNum, eTyp, en := protowire.ConsumeTag(entry)
			if en < 0 {
				return nil, protowire.ParseError(en)
			}
			entry = entry[en:]
			if eNum == 1 && eTyp == protowire.BytesType {
				k, kn := protowire.ConsumeBytes(entry)
				if kn < 0 {
					return nil, protowire.ParseError(kn)
				}
				keys = append(keys, k)
				entry = entry[kn:]
			} else {
				en = protowire.ConsumeFieldValue(eNum, eTyp, entry)
				if en < 0 {
					return nil, protowire.ParseError(en)
				}
				entry = entry[en:]
			}
		}
	}
	return keys, nil
}

// scanEntitlementResourceTypeRaw extracts only the entitlement's
// resource_type_id (its own resource's type) from a marshaled
// EntitlementRecord, borrowing the bytes from value. The stats grouping
// path needs just this one field; scanEntitlementResourceRaw
// materializes two strings per entitlement. Like the full scanner, the
// last occurrence of the resource field wins.
func scanEntitlementResourceTypeRaw(value []byte) ([]byte, error) {
	var rt []byte
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
			return nil, fmt.Errorf("raw record: entitlement resource has wire type %v", typ)
		}
		msg, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		if err := scanResourceRefRawBytes(msg, func(fnum protowire.Number, val []byte) {
			if fnum == 1 {
				rt = val
			}
		}); err != nil {
			return nil, err
		}
		value = value[n:]
	}
	return rt, nil
}

// scanResourceParentRaw and scanEntitlementResourceRaw keep the LAST
// occurrence of the target field, matching scanGrantIndexFieldsRaw and
// approximating proto merge semantics. Values written by this SDK carry
// at most one occurrence, so this only matters for foreign writers.
// scanResourceIndexFieldsRaw extracts the parent ref (field 6) and
// source_scope_hash (field 9) from a marshaled ResourceRecord — the
// fields that key resource secondary indexes.
func scanResourceIndexFieldsRaw(value []byte) (string, string, string, error) {
	var rt, id, sourceScopeHash string
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return "", "", "", protowire.ParseError(n)
		}
		value = value[n:]
		switch num {
		case 6:
			if typ != protowire.BytesType {
				return "", "", "", fmt.Errorf("raw record: resource parent has wire type %v", typ)
			}
			msg, n := protowire.ConsumeBytes(value)
			if n < 0 {
				return "", "", "", protowire.ParseError(n)
			}
			var err error
			rt, id, err = scanResourceRefRaw(msg)
			if err != nil {
				return "", "", "", err
			}
			value = value[n:]
		case 9:
			if typ != protowire.BytesType {
				return "", "", "", fmt.Errorf("raw record: resource source_scope_hash has wire type %v", typ)
			}
			s, n := protowire.ConsumeBytes(value)
			if n < 0 {
				return "", "", "", protowire.ParseError(n)
			}
			sourceScopeHash = string(s)
			value = value[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return "", "", "", protowire.ParseError(n)
			}
			value = value[n:]
		}
	}
	return rt, id, sourceScopeHash, nil
}

// scanEntitlementSourceScopeRaw extracts source_scope_hash (field 11)
// from a marshaled EntitlementRecord. Keys the entitlement
// by_source_scope index — the only entitlement secondary index.
func scanEntitlementSourceScopeRaw(value []byte) (string, error) {
	var sourceScopeHash string
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return "", protowire.ParseError(n)
		}
		value = value[n:]
		if num != 11 {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return "", protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		if typ != protowire.BytesType {
			return "", fmt.Errorf("raw record: entitlement source_scope_hash has wire type %v", typ)
		}
		s, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return "", protowire.ParseError(n)
		}
		sourceScopeHash = string(s)
		value = value[n:]
	}
	return sourceScopeHash, nil
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

func scanEntitlementIdentityFieldsRaw(value []byte) (string, string, string, error) {
	var externalID, rt, id string
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return "", "", "", protowire.ParseError(n)
		}
		value = value[n:]
		switch num {
		case 2:
			if typ != protowire.BytesType {
				return "", "", "", fmt.Errorf("raw record: entitlement external_id has wire type %v", typ)
			}
			v, n := protowire.ConsumeString(value)
			if n < 0 {
				return "", "", "", protowire.ParseError(n)
			}
			externalID = v
			value = value[n:]
		case 3:
			if typ != protowire.BytesType {
				return "", "", "", fmt.Errorf("raw record: entitlement resource has wire type %v", typ)
			}
			msg, n := protowire.ConsumeBytes(value)
			if n < 0 {
				return "", "", "", protowire.ParseError(n)
			}
			var err error
			rt, id, err = scanResourceRefRaw(msg)
			if err != nil {
				return "", "", "", err
			}
			value = value[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return "", "", "", protowire.ParseError(n)
			}
			value = value[n:]
		}
	}
	return rt, id, externalID, nil
}

func scanGrantIndexFieldsRaw(value []byte) (string, string, string, string, string, bool, string, error) {
	var entRT, entRID, entID, principalRT, principalID, sourceScopeHash string
	var needsExpansion bool
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return "", "", "", "", "", false, "", protowire.ParseError(n)
		}
		value = value[n:]
		switch num {
		case 3:
			if typ != protowire.BytesType {
				return "", "", "", "", "", false, "", fmt.Errorf("raw record: grant entitlement has wire type %v", typ)
			}
			msg, n := protowire.ConsumeBytes(value)
			if n < 0 {
				return "", "", "", "", "", false, "", protowire.ParseError(n)
			}
			var err error
			entRT, entRID, entID, err = scanEntitlementRefRaw(msg)
			if err != nil {
				return "", "", "", "", "", false, "", err
			}
			value = value[n:]
		case 4:
			if typ != protowire.BytesType {
				return "", "", "", "", "", false, "", fmt.Errorf("raw record: grant principal has wire type %v", typ)
			}
			msg, n := protowire.ConsumeBytes(value)
			if n < 0 {
				return "", "", "", "", "", false, "", protowire.ParseError(n)
			}
			var err error
			principalRT, principalID, err = scanPrincipalRefRaw(msg)
			if err != nil {
				return "", "", "", "", "", false, "", err
			}
			value = value[n:]
		case 7:
			if typ != protowire.VarintType {
				return "", "", "", "", "", false, "", fmt.Errorf("raw record: grant needs_expansion has wire type %v", typ)
			}
			v, n := protowire.ConsumeVarint(value)
			if n < 0 {
				return "", "", "", "", "", false, "", protowire.ParseError(n)
			}
			needsExpansion = v != 0
			value = value[n:]
		case 10:
			if typ != protowire.BytesType {
				return "", "", "", "", "", false, "", fmt.Errorf("raw record: grant source_scope_hash has wire type %v", typ)
			}
			s, n := protowire.ConsumeBytes(value)
			if n < 0 {
				return "", "", "", "", "", false, "", protowire.ParseError(n)
			}
			sourceScopeHash = string(s)
			value = value[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return "", "", "", "", "", false, "", protowire.ParseError(n)
			}
			value = value[n:]
		}
	}
	return entRT, entRID, entID, principalRT, principalID, needsExpansion, sourceScopeHash, nil
}

// scanGrantNeedsExpansionRaw extracts only the needs_expansion flag
// (GrantRecord field 7) with a shallow wire scan — for callers that already
// carry the identity in the key and need nothing else from the value.
func scanGrantNeedsExpansionRaw(value []byte) (bool, error) {
	var needsExpansion bool
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return false, protowire.ParseError(n)
		}
		value = value[n:]
		if num == 7 {
			if typ != protowire.VarintType {
				return false, fmt.Errorf("raw record: grant needs_expansion has wire type %v", typ)
			}
			v, n := protowire.ConsumeVarint(value)
			if n < 0 {
				return false, protowire.ParseError(n)
			}
			needsExpansion = v != 0
			value = value[n:]
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, value)
		if n < 0 {
			return false, protowire.ParseError(n)
		}
		value = value[n:]
	}
	return needsExpansion, nil
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

// Annotation message names (the tail of an Any type_url) that drive
// replay side effects. Matched on the segment after the last '/' so
// non-default type-url prefixes still resolve.
const (
	anyTypeChildResourceType        = "c1.connector.v2.ChildResourceType"
	anyTypeInsertResourceGrants     = "c1.connector.v2.InsertResourceGrants"
	anyTypeExternalResourceMatch    = "c1.connector.v2.ExternalResourceMatch"
	anyTypeExternalResourceMatchAll = "c1.connector.v2.ExternalResourceMatchAll"
	anyTypeExternalResourceMatchID  = "c1.connector.v2.ExternalResourceMatchID"
)

// scanAnnotationAnysRaw walks a record's repeated-Any annotations field
// (fieldNum) without unmarshaling the record, invoking visit with each
// Any's message type name and value bytes. Used by the source-cache
// replay paths to detect side-effect annotations (ChildResourceType,
// InsertResourceGrants, ExternalResourceMatch*) on raw-copied rows.
func scanAnnotationAnysRaw(value []byte, fieldNum protowire.Number, visit func(typeName string, payload []byte) error) error {
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return protowire.ParseError(n)
		}
		value = value[n:]
		if num != fieldNum {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		if typ != protowire.BytesType {
			return fmt.Errorf("raw record: annotations field has wire type %v", typ)
		}
		anyMsg, n := protowire.ConsumeBytes(value)
		if n < 0 {
			return protowire.ParseError(n)
		}
		value = value[n:]

		var typeURL string
		var payload []byte
		rest := anyMsg
		for len(rest) > 0 {
			anum, atyp, an := protowire.ConsumeTag(rest)
			if an < 0 {
				return protowire.ParseError(an)
			}
			rest = rest[an:]
			if atyp != protowire.BytesType {
				an = protowire.ConsumeFieldValue(anum, atyp, rest)
				if an < 0 {
					return protowire.ParseError(an)
				}
				rest = rest[an:]
				continue
			}
			b, an := protowire.ConsumeBytes(rest)
			if an < 0 {
				return protowire.ParseError(an)
			}
			switch anum {
			case 1:
				typeURL = string(b)
			case 2:
				payload = b
			default:
			}
			rest = rest[an:]
		}
		typeName := typeURL
		if i := strings.LastIndexByte(typeURL, '/'); i >= 0 {
			typeName = typeURL[i+1:]
		}
		if typeName == "" {
			continue
		}
		if err := visit(typeName, payload); err != nil {
			return err
		}
	}
	return nil
}

// scanResourceChildTypeIDsRaw returns the resource_type_ids named by
// ChildResourceType annotations on a marshaled ResourceRecord (field 7).
// Nil for the common case of a resource with no child types.
func scanResourceChildTypeIDsRaw(value []byte) ([]string, error) {
	var out []string
	err := scanAnnotationAnysRaw(value, 7, func(typeName string, payload []byte) error {
		if typeName != anyTypeChildResourceType {
			return nil
		}
		crt := &v2.ChildResourceType{}
		if err := proto.Unmarshal(payload, crt); err != nil {
			return fmt.Errorf("raw record: unmarshal ChildResourceType annotation: %w", err)
		}
		if id := crt.GetResourceTypeId(); id != "" {
			out = append(out, id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

const anyTypeEntitlementExclusionGroup = "c1.connector.v2.EntitlementExclusionGroup"

// scanEntitlementExclusionGroupsRaw returns the exclusion-group
// annotations on a marshaled EntitlementRecord (annotations field 7),
// paired with the row's identity so the syncer can validate them exactly
// like fresh response rows. Nil for the common unannotated case, with no
// identity scan performed.
func scanEntitlementExclusionGroupsRaw(value []byte) ([]ReplayedExclusionGroup, error) {
	var groups []*v2.EntitlementExclusionGroup
	err := scanAnnotationAnysRaw(value, 7, func(typeName string, payload []byte) error {
		if typeName != anyTypeEntitlementExclusionGroup {
			return nil
		}
		eg := &v2.EntitlementExclusionGroup{}
		if err := proto.Unmarshal(payload, eg); err != nil {
			return fmt.Errorf("raw record: unmarshal EntitlementExclusionGroup annotation: %w", err)
		}
		groups = append(groups, eg)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return nil, nil
	}
	rt, _, externalID, err := scanEntitlementIdentityFieldsRaw(value)
	if err != nil {
		return nil, err
	}
	out := make([]ReplayedExclusionGroup, 0, len(groups))
	for _, eg := range groups {
		out = append(out, ReplayedExclusionGroup{
			EntitlementID:  externalID,
			ResourceTypeID: rt,
			Group:          eg,
		})
	}
	return out, nil
}

// grantSourceCacheFlags reports which replay side-effect annotations a
// marshaled GrantRecord carries (annotations field 8).
type grantSourceCacheFlags struct {
	insertResourceGrants  bool
	externalResourceMatch bool
}

func scanGrantSourceCacheFlagsRaw(value []byte) (grantSourceCacheFlags, error) {
	var f grantSourceCacheFlags
	err := scanAnnotationAnysRaw(value, 8, func(typeName string, _ []byte) error {
		switch typeName {
		case anyTypeInsertResourceGrants:
			f.insertResourceGrants = true
		case anyTypeExternalResourceMatch, anyTypeExternalResourceMatchAll, anyTypeExternalResourceMatchID:
			f.externalResourceMatch = true
		}
		return nil
	})
	return f, err
}
