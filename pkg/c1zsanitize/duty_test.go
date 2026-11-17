package c1zsanitize

import (
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestDutyMappingsAnnotationHandler(t *testing.T) {
	s := newTestSanitizer(bytes32("duty-annos"))
	in := v2.DutyMappingsAnnotation_builder{
		Mappings: []*v2.DutyMapping{
			v2.DutyMapping_builder{
				DutyId:    "payment.release",
				Precision: v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_EXACT,
			}.Build(),
			v2.DutyMapping_builder{
				DutyId:      "software_change.approve",
				Precision:   v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_INCLUSIVE,
				Description: "Maintainer role on the acme/payments-service repository",
			}.Build(),
		},
	}.Build()

	out := s.transformAnnotations([]*anypb.Any{mustAny(t, in)}, newAssetRefSet())
	require.Len(t, out, 1)
	got := mustUnpack(t, out[0], &v2.DutyMappingsAnnotation{})
	require.NoError(t, got.ValidateAll(), "sanitized output satisfies the wire rules")
	require.Len(t, got.GetMappings(), 2)

	m0, m1 := got.GetMappings()[0], got.GetMappings()[1]
	require.Equal(t, s.id("payment.release"), m0.GetDutyId())
	require.Equal(t, v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_EXACT, m0.GetPrecision())
	require.Empty(t, m0.GetDescription(), "an empty description stays empty")
	require.Equal(t, s.id("software_change.approve"), m1.GetDutyId())
	require.Equal(t, v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_INCLUSIVE, m1.GetPrecision())
	require.Equal(t, redactedDutyDescription, m1.GetDescription())
}

func TestDutyMappingsAnnotationHandlerIDs(t *testing.T) {
	s := newTestSanitizer(bytes32("duty-ids"))
	ids := []string{
		"payment.release",
		"acme_ledger.post",
		"Acme-Ledger.Post",
		"acme.ledger.post.extra",
		"software_change.merge",
		strings.Repeat("x", 64),
	}
	list := make([]*v2.DutyMapping, 0, len(ids))
	for _, id := range ids {
		list = append(list, v2.DutyMapping_builder{
			DutyId:    id,
			Precision: v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_INCLUSIVE,
		}.Build())
	}
	in := v2.DutyMappingsAnnotation_builder{Mappings: list}.Build()
	require.NoError(t, in.ValidateAll())

	out := s.transformAnnotations([]*anypb.Any{mustAny(t, in)}, newAssetRefSet())
	require.Len(t, out, 1)
	got := mustUnpack(t, out[0], &v2.DutyMappingsAnnotation{})
	require.NoError(t, got.ValidateAll(), "sanitized IDs satisfy the wire rules")
	require.Len(t, got.GetMappings(), len(ids))

	again := mustUnpack(t, s.transformAnnotations([]*anypb.Any{mustAny(t, in)}, newAssetRefSet())[0], &v2.DutyMappingsAnnotation{})

	seen := map[string]struct{}{}
	for i, m := range got.GetMappings() {
		require.Equal(t, v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_INCLUSIVE, m.GetPrecision())
		require.NotEqual(t, ids[i], m.GetDutyId(), "every duty ID is replaced")
		require.Equal(t, s.id(ids[i]), m.GetDutyId())
		require.Equal(t, again.GetMappings()[i].GetDutyId(), m.GetDutyId(), "the same input maps to the same output")
		seen[m.GetDutyId()] = struct{}{}
	}
	require.Len(t, seen, len(ids), "distinct inputs stay distinct")
}
