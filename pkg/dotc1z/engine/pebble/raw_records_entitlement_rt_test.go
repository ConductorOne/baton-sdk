package pebble

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// TestScanEntitlementResourceTypeRaw covers the raw proto-wire scanner
// that extracts only an entitlement's own resource_type_id (field 3 →
// field 1) from a marshaled EntitlementRecord. This is the field the
// per-resource-type entitlement grouping reads on the sidecar compute
// path.
func TestScanEntitlementResourceTypeRaw(t *testing.T) {
	t.Run("resource present", func(t *testing.T) {
		rec := v3.EntitlementRecord_builder{
			ExternalId: "ent-1",
			Resource:   v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "g1"}.Build(),
		}.Build()
		val, err := marshalRecord(rec)
		require.NoError(t, err)

		rt, err := scanEntitlementResourceTypeRaw(val)
		require.NoError(t, err)
		require.Equal(t, "group", string(rt))
	})

	t.Run("no resource", func(t *testing.T) {
		rec := v3.EntitlementRecord_builder{ExternalId: "ent-noressource"}.Build()
		val, err := marshalRecord(rec)
		require.NoError(t, err)

		rt, err := scanEntitlementResourceTypeRaw(val)
		require.NoError(t, err)
		require.Empty(t, string(rt))
	})

	t.Run("resource with empty type", func(t *testing.T) {
		rec := v3.EntitlementRecord_builder{
			ExternalId: "ent-emptyrt",
			Resource:   v3.ResourceRef_builder{ResourceId: "r1"}.Build(),
		}.Build()
		val, err := marshalRecord(rec)
		require.NoError(t, err)

		rt, err := scanEntitlementResourceTypeRaw(val)
		require.NoError(t, err)
		require.Empty(t, string(rt))
	})

	t.Run("last occurrence wins", func(t *testing.T) {
		// Hand-assemble a record with the resource field (3) repeated.
		// proto merge semantics keep the last occurrence, which the
		// scanner must mirror.
		first := protowire.AppendTag(nil, 1, protowire.BytesType)
		first = protowire.AppendString(first, "user")
		second := protowire.AppendTag(nil, 1, protowire.BytesType)
		second = protowire.AppendString(second, "group")

		var val []byte
		val = protowire.AppendTag(val, 2, protowire.BytesType)
		val = protowire.AppendString(val, "ent-dup")
		val = protowire.AppendTag(val, 3, protowire.BytesType)
		val = protowire.AppendBytes(val, first)
		val = protowire.AppendTag(val, 3, protowire.BytesType)
		val = protowire.AppendBytes(val, second)

		rt, err := scanEntitlementResourceTypeRaw(val)
		require.NoError(t, err)
		require.Equal(t, "group", string(rt))
	})

	t.Run("malformed truncated input", func(t *testing.T) {
		// A bytes-typed resource field whose length prefix overruns the
		// buffer must surface a parse error, not panic.
		val := protowire.AppendTag(nil, 3, protowire.BytesType)
		val = append(val, 0x7f) // length 127, but no payload follows
		_, err := scanEntitlementResourceTypeRaw(val)
		require.Error(t, err)
	})

	t.Run("parity with full scanner", func(t *testing.T) {
		rec := v3.EntitlementRecord_builder{
			ExternalId: "ent-parity",
			Resource:   v3.ResourceRef_builder{ResourceTypeId: "app", ResourceId: "a1"}.Build(),
		}.Build()
		val, err := marshalRecord(rec)
		require.NoError(t, err)

		rtOnly, err := scanEntitlementResourceTypeRaw(val)
		require.NoError(t, err)
		fullRT, _, err := scanEntitlementResourceRaw(val)
		require.NoError(t, err)
		require.Equal(t, fullRT, string(rtOnly))
	})
}
