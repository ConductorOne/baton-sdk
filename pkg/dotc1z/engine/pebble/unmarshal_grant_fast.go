package pebble

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// unmarshalGrantRecordFast is a hand-rolled wire-format decoder for
// v3.GrantRecord scoped to the "simple grant" shape that dominates the
// paginated-read workload: sync_id + external_id + entitlement +
// principal (fields 1-4), no annotations/sources/discovered_at.
//
// Why exists:
//
//   - proto.Unmarshal calls the runtime's MessageInfo.unmarshalPointer
//     path, which fully handles all 9 GrantRecord fields, the unknown-
//     fields tail, repeated-message merge, map-decoding, etc. Per
//     profile, this is ≈500 ns/record dominated by table lookup +
//     nested-message allocation for EntitlementRef + PrincipalRef.
//
//   - The fast decoder uses arena-allocated nested struct slots (one
//     v3.EntitlementRef + one v3.PrincipalRef per arena index) so the
//     two nested allocations per record are eliminated — collapsed to
//     two large slice allocations per page (≈10k records). For 1 M-grant
//     bench, this is 2 M nested allocs → 200 slice allocs.
//
//   - Skipping table-lookup + reflection for the outer field-switch
//     trims the per-record decode CPU.
//
// Correctness guard: if the wire stream contains field 8 (annotations)
// or 9 (sources) — which v2.Grant translation reads — the fast decoder
// Reset()s the partially-decoded record and falls back to
// proto.Unmarshal. This preserves identical-to-proto output for any
// grant shape outside the fast path.
//
// Wire-format invariants (encoded inline because proto changes would
// break us silently otherwise):
//   - v3.GrantRecord fields 1=sync_id (string), 2=external_id (string),
//     3=entitlement (EntitlementRef msg), 4=principal (PrincipalRef msg).
//   - v3.EntitlementRef fields 1=resource_type_id, 2=resource_id,
//     3=entitlement_id (all strings).
//   - v3.PrincipalRef fields 1=resource_type_id, 2=resource_id
//     (both strings).
//
// If the proto IDL changes, the fast path needs updating. We rely on
// the protobuf-frozen IDL constraint from autoresearch.md
// (`proto/c1/storage/v3/` is off-limits).
// Pre-computed single-byte wire tags for v3.GrantRecord fields 1-4.
// Each is (fieldNum << 3) | wireType (2 = BytesType / length-delimited).
// For field numbers 1-15, the tag fits in a single varint byte, so we
// can compare data[0] directly without calling protowire.ConsumeTag
// (which does varint-decode + bit-split per field).
const (
	grantWireTagSyncID         byte = 0x0A // (1<<3)|2
	grantWireTagExternalID     byte = 0x12 // (2<<3)|2
	grantWireTagEntitlement    byte = 0x1A // (3<<3)|2
	grantWireTagPrincipal      byte = 0x22 // (4<<3)|2
	grantWireTagDiscoveredAt   byte = 0x2A // (5<<3)|2
	grantWireTagExpansion      byte = 0x32 // (6<<3)|2
	grantWireTagNeedsExpansion byte = 0x38 // (7<<3)|0 varint
	grantWireTagAnnotations    byte = 0x42 // (8<<3)|2
	grantWireTagSources        byte = 0x4A // (9<<3)|2
)

func unmarshalGrantRecordFast(
	data []byte,
	rec *v3.GrantRecord,
	ent *v3.EntitlementRef,
	princ *v3.PrincipalRef,
) error {
	full := data
	var sawEnt, sawPrinc bool
	for len(data) > 0 {
		// Fast tag-byte fast path: fields 1-15 have single-byte tags.
		// If the first byte's high bit is set, the tag is multi-byte
		// (field >= 16 or varint-encoded reserved range) — fall back
		// to protowire.ConsumeTag for general decoding.
		tagByte := data[0]
		if tagByte&0x80 != 0 {
			return fallbackUnmarshalGrant(full, rec)
		}
		switch tagByte {
		case grantWireTagAnnotations, grantWireTagSources:
			return fallbackUnmarshalGrant(full, rec)
		case grantWireTagSyncID:
			data = data[1:]
			// Skip the length-prefixed bytes without parsing the
			// string — no read-path consumer reads SyncId. Use
			// ConsumeBytes which does internal bounds checks and
			// returns the consumed byte count.
			_, m := protowire.ConsumeBytes(data)
			if m < 0 {
				return fallbackUnmarshalGrant(full, rec)
			}
			data = data[m:]
			continue
		case grantWireTagExternalID:
			data = data[1:]
			val, m := protowire.ConsumeString(data)
			if m < 0 {
				return fallbackUnmarshalGrant(full, rec)
			}
			data = data[m:]
			rec.SetExternalId(val)
			continue
		case grantWireTagEntitlement:
			data = data[1:]
			val, m := protowire.ConsumeBytes(data)
			if m < 0 {
				return fallbackUnmarshalGrant(full, rec)
			}
			data = data[m:]
			if err := unmarshalEntitlementRefFast(val, ent); err != nil {
				return fallbackUnmarshalGrant(full, rec)
			}
			rec.SetEntitlement(ent)
			sawEnt = true
			continue
		case grantWireTagPrincipal:
			data = data[1:]
			val, m := protowire.ConsumeBytes(data)
			if m < 0 {
				return fallbackUnmarshalGrant(full, rec)
			}
			data = data[m:]
			if err := unmarshalPrincipalRefFast(val, princ); err != nil {
				return fallbackUnmarshalGrant(full, rec)
			}
			rec.SetPrincipal(princ)
			sawPrinc = true
			continue
		case grantWireTagDiscoveredAt, grantWireTagExpansion:
			// Length-delimited skip via ConsumeBytes (bounds-checked).
			data = data[1:]
			_, m := protowire.ConsumeBytes(data)
			if m < 0 {
				return fallbackUnmarshalGrant(full, rec)
			}
			data = data[m:]
			continue
		case grantWireTagNeedsExpansion:
			// Varint skip: 1 byte tag + varint value
			data = data[1:]
			_, m := protowire.ConsumeVarint(data)
			if m < 0 {
				return fallbackUnmarshalGrant(full, rec)
			}
			data = data[m:]
			continue
		}
		// Unrecognized single-byte tag (fields 10-15 or reserved range
		// with high bit clear) — fall back to general loop.
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			rec.Reset()
			return proto.Unmarshal(full, rec)
		}
		data = data[n:]
		// Multi-byte tags (fields >= 16) or fields 10-15 with single
		// byte not matched above. Use general consume + skip.
		m := protowire.ConsumeFieldValue(num, typ, data)
		if m < 0 {
			return fallbackUnmarshalGrant(full, rec)
		}
		data = data[m:]
	}
	// Suppress "declared and not used" if compiler doesn't see the
	// branch paths. These bools are intentionally unused in the success
	// path but the structure documents what the decoder accepts.
	_ = sawEnt
	_ = sawPrinc
	return nil
}

func fallbackUnmarshalGrant(data []byte, rec *v3.GrantRecord) error {
	rec.Reset()
	return proto.Unmarshal(data, rec)
}

func unmarshalEntitlementRefFast(data []byte, ent *v3.EntitlementRef) error {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return fmt.Errorf("entitlement: bad tag")
		}
		data = data[n:]
		if typ != protowire.BytesType {
			// Force fallback by returning error.
			return fmt.Errorf("entitlement: unexpected wire type %d for field %d", typ, num)
		}
		val, m := protowire.ConsumeString(data)
		if m < 0 {
			return fmt.Errorf("entitlement: bad string field %d", num)
		}
		data = data[m:]
		switch num {
		case 1:
			ent.SetResourceTypeId(val)
		case 2:
			ent.SetResourceId(val)
		case 3:
			ent.SetEntitlementId(val)
		default:
			// Unknown field — ignore. proto.Unmarshal would store in
			// unknownFields, but EntitlementRef is a leaf shape that's
			// not expected to grow.
		}
	}
	return nil
}

func unmarshalPrincipalRefFast(data []byte, princ *v3.PrincipalRef) error {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return fmt.Errorf("principal: bad tag")
		}
		data = data[n:]
		if typ != protowire.BytesType {
			return fmt.Errorf("principal: unexpected wire type %d for field %d", typ, num)
		}
		val, m := protowire.ConsumeString(data)
		if m < 0 {
			return fmt.Errorf("principal: bad string field %d", num)
		}
		data = data[m:]
		switch num {
		case 1:
			princ.SetResourceTypeId(val)
		case 2:
			princ.SetResourceId(val)
		default:
			// Unknown field — ignore. PrincipalRef is a leaf shape.
		}
	}
	return nil
}
