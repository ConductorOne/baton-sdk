package pebble

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
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

// NOTE (2b): deleteResourceIndexesRaw / deleteGrantIndexesRaw are GONE.
// Prior-row index cleanup is an obligation of rawdb's typed record ops
// (StageGrantPutInline/StageGrantDelete derive cleanup keys from the
// primary key; StageResourcePut/StageResourceDelete consume the prior
// value through the ResourceParent deriver).

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
		if err := rawdb.ScanResourceRefRawBytes(msg, func(fnum protowire.Number, val []byte) {
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

// grantImmutableAnnotationTypeName is the c1.connector.v2.GrantImmutable
// message's fully-qualified name — the tail of its anypb type URL
// ("type.googleapis.com/c1.connector.v2.GrantImmutable"). Matched by
// name, never by unmarshaling the annotation payload: existence is the
// only fact the content hash folds in (see grantContentHash64's ABI doc
// in grant_digest.go).
const grantImmutableAnnotationTypeName = "c1.connector.v2.GrantImmutable"

// scanGrantContentFactsRawBytes extracts the two grant-content facts a
// marshaled GrantRecord's value carries beyond its primary-key identity,
// in one raw field scan (no proto unmarshal):
//
//   - isImmutable: whether `annotations` (field 8, repeated
//     google.protobuf.Any) carries a GrantImmutable entry, checked
//     against the tail of the embedded Any's type_url (its own field 1)
//     without unmarshaling the annotation payload.
//   - sources: the `sources` map (field 9, map<string, GrantSourceRecord>)
//     as (key, is_direct) pairs — a map entry is a submessage with
//     sub-field 1 = key string, sub-field 2 = the GrantSourceRecord
//     value, whose own field 4 is is_direct.
//
// out is a recycled scratch slice (pass out[:0] to reuse its backing
// array across calls, as the seal-time grant digest build does — see
// appendGrantHashIndexRow); its key slices are views borrowed from
// value, valid only while value's backing bytes are. Sources are
// returned in encounter order, NOT sorted — callers sort by key
// themselves (sortGrantSourceFacts).
func scanGrantContentFactsRawBytes(value []byte, out []grantSourceFact) (bool, []grantSourceFact, error) {
	isImmutable := false
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return false, nil, protowire.ParseError(n)
		}
		value = value[n:]
		switch num {
		case 8:
			if typ != protowire.BytesType {
				return false, nil, fmt.Errorf("raw record: grant annotations entry has wire type %v", typ)
			}
			entry, en := protowire.ConsumeBytes(value)
			if en < 0 {
				return false, nil, protowire.ParseError(en)
			}
			value = value[en:]
			if !isImmutable {
				var err error
				isImmutable, err = scanAnyEntryIsTypeRaw(entry, grantImmutableAnnotationTypeName)
				if err != nil {
					return false, nil, err
				}
			}
		case 9:
			if typ != protowire.BytesType {
				return false, nil, fmt.Errorf("raw record: grant sources entry has wire type %v", typ)
			}
			entry, en := protowire.ConsumeBytes(value)
			if en < 0 {
				return false, nil, protowire.ParseError(en)
			}
			value = value[en:]
			var key []byte
			var isDirect bool
			for len(entry) > 0 {
				eNum, eTyp, ren := protowire.ConsumeTag(entry)
				if ren < 0 {
					return false, nil, protowire.ParseError(ren)
				}
				entry = entry[ren:]
				switch {
				case eNum == 1 && eTyp == protowire.BytesType:
					k, kn := protowire.ConsumeBytes(entry)
					if kn < 0 {
						return false, nil, protowire.ParseError(kn)
					}
					key = k
					entry = entry[kn:]
				case eNum == 2 && eTyp == protowire.BytesType:
					v, vn := protowire.ConsumeBytes(entry)
					if vn < 0 {
						return false, nil, protowire.ParseError(vn)
					}
					var err error
					isDirect, err = scanGrantSourceRecordIsDirectRaw(v)
					if err != nil {
						return false, nil, err
					}
					entry = entry[vn:]
				default:
					ren = protowire.ConsumeFieldValue(eNum, eTyp, entry)
					if ren < 0 {
						return false, nil, protowire.ParseError(ren)
					}
					entry = entry[ren:]
				}
			}
			out = append(out, grantSourceFact{key: key, isDirect: isDirect})
		default:
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return false, nil, protowire.ParseError(n)
			}
			value = value[n:]
		}
	}
	return isImmutable, out, nil
}

// scanAnyEntryIsTypeRaw reports whether one serialized google.protobuf.Any
// entry names typeName, checked against the tail of its type_url (field
// 1) without unmarshaling the payload (field 2).
//
// Stays on []byte throughout and keeps the final string(tail) conversion
// INLINE in the comparison: the compiler rewrites string(b) == s in that
// position to a non-allocating alias of b's backing array (OBYTES2STRTMP
// — safe because a comparison cannot retain its operands). Hoisting the
// conversion into a local would silently restore a copy, and
// protowire.ConsumeString is exactly that copy — it heap-allocates for
// anything over 32 bytes, which every real type URL is. The seal-time
// digest build calls this once per annotation per grant, and that path
// must not allocate per row (see grantHashRowScratch).
func scanAnyEntryIsTypeRaw(entry []byte, typeName string) (bool, error) {
	for len(entry) > 0 {
		num, typ, n := protowire.ConsumeTag(entry)
		if n < 0 {
			return false, protowire.ParseError(n)
		}
		entry = entry[n:]
		if num != 1 {
			n = protowire.ConsumeFieldValue(num, typ, entry)
			if n < 0 {
				return false, protowire.ParseError(n)
			}
			entry = entry[n:]
			continue
		}
		if typ != protowire.BytesType {
			return false, fmt.Errorf("raw record: any type_url has wire type %v", typ)
		}
		url, un := protowire.ConsumeBytes(entry)
		if un < 0 {
			return false, protowire.ParseError(un)
		}
		name := url
		if i := bytes.LastIndexByte(url, '/'); i >= 0 {
			name = url[i+1:]
		}
		return string(name) == typeName, nil
	}
	return false, nil
}

// scanGrantSourceRecordIsDirectRaw extracts is_direct (GrantSourceRecord
// field 4) from one marshaled map-entry value.
func scanGrantSourceRecordIsDirectRaw(value []byte) (bool, error) {
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return false, protowire.ParseError(n)
		}
		value = value[n:]
		if num != 4 {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return false, protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		if typ != protowire.VarintType {
			return false, fmt.Errorf("raw record: grant source is_direct has wire type %v", typ)
		}
		v, n := protowire.ConsumeVarint(value)
		if n < 0 {
			return false, protowire.ParseError(n)
		}
		return v != 0, nil
	}
	return false, nil
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
		if err := rawdb.ScanResourceRefRawBytes(msg, func(fnum protowire.Number, val []byte) {
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
		rt, id, err = rawdb.ScanResourceRefRaw(msg)
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
			rt, id, err = rawdb.ScanResourceRefRaw(msg)
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

func scanEntitlementRefRaw(value []byte) (string, string, string, error) {
	var rt, rid, eid []byte
	err := rawdb.ScanResourceRefRawBytes(value, func(num protowire.Number, val []byte) {
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
	err := rawdb.ScanResourceRefRawBytes(value, func(num protowire.Number, val []byte) {
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
