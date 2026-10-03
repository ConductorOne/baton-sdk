package pebble

import (
	"bytes"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

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
// returned in encounter order, NOT sorted, and NOT deduplicated — a
// map field with a duplicated key (legal but never proto.Marshal-
// produced wire bytes) comes back as two entries here; callers sort
// AND collapse duplicate keys themselves (sortGrantSourceFacts).
//
// Fields 8 and 9 carrying the wrong wire type are skipped as unknown
// data, matching protobuf-go's decoder rather than erroring.
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
				// A known field number carrying the wrong wire type is
				// unknown-field data to protobuf-go's decoder (it does not
				// error); skip it the same way so this scanner accepts
				// exactly what proto.Unmarshal accepts.
				n = protowire.ConsumeFieldValue(num, typ, value)
				if n < 0 {
					return false, nil, protowire.ParseError(n)
				}
				value = value[n:]
				continue
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
				// Same unknown-field treatment as field 8 above: a wrong
				// wire type on a known field number is skipped, not an
				// error, matching protobuf-go.
				n = protowire.ConsumeFieldValue(num, typ, value)
				if n < 0 {
					return false, nil, protowire.ParseError(n)
				}
				value = value[n:]
				continue
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
					fragDirect, present, err := scanGrantSourceRecordIsDirectRaw(v)
					if err != nil {
						return false, nil, err
					}
					// The map entry's value (field 2) is itself an
					// embedded message: proto merges repeated
					// occurrences of it field-by-field rather than
					// replacing wholesale, so a later fragment that
					// doesn't mention is_direct must not reset a true
					// carried over from an earlier one.
					if present {
						isDirect = fragDirect
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
// 1) without unmarshaling the payload (field 2). type_url is a scalar
// (string) field, so proto merge semantics say the LAST occurrence in
// the fragment wins — this scans to the end rather than returning on
// the first field-1 hit.
//
// Stays on []byte throughout and keeps each string(tail) conversion
// INLINE in the comparison: the compiler rewrites string(b) == s in that
// position to a non-allocating alias of b's backing array (OBYTES2STRTMP
// — safe because a comparison cannot retain its operands). Hoisting the
// conversion into a local would silently restore a copy, and
// protowire.ConsumeString is exactly that copy — it heap-allocates for
// anything over 32 bytes, which every real type URL is. The seal-time
// digest build calls this once per annotation per grant, and that path
// must not allocate per row (see grantHashRowScratch).
//
// field 1 carrying the wrong wire type is skipped as unknown data,
// matching protobuf-go's decoder rather than erroring.
func scanAnyEntryIsTypeRaw(entry []byte, typeName string) (bool, error) {
	isType := false
	for len(entry) > 0 {
		num, typ, n := protowire.ConsumeTag(entry)
		if n < 0 {
			return false, protowire.ParseError(n)
		}
		entry = entry[n:]
		if num != 1 || typ != protowire.BytesType {
			n = protowire.ConsumeFieldValue(num, typ, entry)
			if n < 0 {
				return false, protowire.ParseError(n)
			}
			entry = entry[n:]
			continue
		}
		url, un := protowire.ConsumeBytes(entry)
		if un < 0 {
			return false, protowire.ParseError(un)
		}
		name := url
		if i := bytes.LastIndexByte(url, '/'); i >= 0 {
			name = url[i+1:]
		}
		isType = string(name) == typeName
		entry = entry[un:]
	}
	return isType, nil
}

// scanGrantSourceRecordIsDirectRaw extracts is_direct (GrantSourceRecord
// field 4) from one marshaled map-entry value fragment. is_direct is a
// scalar field, so proto merge semantics say the LAST occurrence wins —
// this scans to the end of the fragment rather than returning on the
// first hit. present reports whether field 4 occurred at all in this
// fragment, so a caller merging this fragment's result into a value
// accumulated from an earlier fragment (a duplicated map-entry value
// submessage, itself a proto-merge case) can tell "this fragment said
// false" apart from "this fragment didn't mention it" — the latter must
// not stomp a true carried over from an earlier fragment.
//
// field 4 carrying the wrong wire type is skipped as unknown data,
// matching protobuf-go's decoder rather than erroring.
func scanGrantSourceRecordIsDirectRaw(value []byte) (bool, bool, error) {
	var isDirect, present bool
	for len(value) > 0 {
		num, typ, n := protowire.ConsumeTag(value)
		if n < 0 {
			return false, false, protowire.ParseError(n)
		}
		value = value[n:]
		if num != 4 || typ != protowire.VarintType {
			n = protowire.ConsumeFieldValue(num, typ, value)
			if n < 0 {
				return false, false, protowire.ParseError(n)
			}
			value = value[n:]
			continue
		}
		v, n := protowire.ConsumeVarint(value)
		if n < 0 {
			return false, false, protowire.ParseError(n)
		}
		isDirect, present = v != 0, true
		value = value[n:]
	}
	return isDirect, present, nil
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
