package c1zsanitize

import (
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/duty"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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

	want := proto.Clone(in).(*v2.DutyMappingsAnnotation)
	want.GetMappings()[1].SetDescription(redactedDutyDescription)
	require.True(t, proto.Equal(want, got), "got %v", got)
	require.Empty(t, got.GetMappings()[0].GetDescription(), "an empty description stays empty")
	require.NoError(t, duty.Validate(got))
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
		list = append(list, duty.Inclusive(id).Proto())
	}
	in := v2.DutyMappingsAnnotation_builder{Mappings: list}.Build()
	require.NoError(t, in.ValidateAll())

	out := s.transformAnnotations([]*anypb.Any{mustAny(t, in)}, newAssetRefSet())
	require.Len(t, out, 1)
	got := mustUnpack(t, out[0], &v2.DutyMappingsAnnotation{})
	require.NoError(t, got.ValidateAll(), "sanitized IDs satisfy the wire rules")

	seen := map[string]struct{}{}
	for i, m := range got.GetMappings() {
		require.Equal(t, v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_INCLUSIVE, m.GetPrecision())
		if duty.ValidID(ids[i]) {
			require.Equal(t, ids[i], m.GetDutyId(), "catalog-form duty IDs pass through")
		} else {
			require.NotEqual(t, ids[i], m.GetDutyId(), "other duty IDs are replaced")
			require.Equal(t, s.sanitizeDutyID(ids[i]), m.GetDutyId(), "replacement is deterministic")
		}
		seen[m.GetDutyId()] = struct{}{}
	}
	require.Len(t, seen, len(ids), "sanitized IDs stay distinct")
}
