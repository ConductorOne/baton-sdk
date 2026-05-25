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
func unmarshalGrantRecordFast(
	data []byte,
	rec *v3.GrantRecord,
	ent *v3.EntitlementRef,
	princ *v3.PrincipalRef,
) error {
	full := data
	var sawEnt, sawPrinc bool
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			rec.Reset()
			return proto.Unmarshal(full, rec)
		}
		data = data[n:]
		switch num {
		case 1: // sync_id (string)
			if typ != protowire.BytesType {
				return fallbackUnmarshalGrant(full, rec)
			}
			val, m := protowire.ConsumeString(data)
			if m < 0 {
				return fallbackUnmarshalGrant(full, rec)
			}
			data = data[m:]
			rec.SetSyncId(val)
		case 2: // external_id (string)
			if typ != protowire.BytesType {
				return fallbackUnmarshalGrant(full, rec)
			}
			val, m := protowire.ConsumeString(data)
			if m < 0 {
				return fallbackUnmarshalGrant(full, rec)
			}
			data = data[m:]
			rec.SetExternalId(val)
		case 3: // entitlement (EntitlementRef msg)
			if typ != protowire.BytesType {
				return fallbackUnmarshalGrant(full, rec)
			}
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
		case 4: // principal (PrincipalRef msg)
			if typ != protowire.BytesType {
				return fallbackUnmarshalGrant(full, rec)
			}
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
		case 8, 9: // annotations / sources — v2 translation reads these
			return fallbackUnmarshalGrant(full, rec)
		default:
			// Fields 5 (discovered_at), 6 (expansion), 7 (needs_expansion),
			// plus any unknown fields. v2 translation doesn't read these
			// for grants, so we safely skip. (Unknown fields go through
			// the unknownFields tail in proto.Unmarshal; the fast path
			// drops them, which is acceptable because callers don't
			// read unknown fields from v2.Grant.)
			m := protowire.ConsumeFieldValue(num, typ, data)
			if m < 0 {
				return fallbackUnmarshalGrant(full, rec)
			}
			data = data[m:]
		}
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
