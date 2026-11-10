package c1zsanitize

import (
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
		CatalogId:      "c1.duties",
		CatalogVersion: "1",
		Mappings: []*v2.DutyMapping{
			v2.DutyMapping_builder{
				DutyId:    "payment.release",
				Precision: v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_EXACT,
			}.Build(),
			v2.DutyMapping_builder{
				DutyId:      "software_change.approve",
				Precision:   v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_CONDITIONAL,
				Description: "only when branch protection on acme/payments-service allows self-approval",
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
