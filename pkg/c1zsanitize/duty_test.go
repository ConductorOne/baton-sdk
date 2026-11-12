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
		CatalogVersion: "1.0.0",
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

func TestDutyMappingsAnnotationHandlerUnknownIDs(t *testing.T) {
	s := newTestSanitizer(bytes32("duty-unknown"))
	ids := []string{
		string(duty.PaymentRelease),
		"acme_ledger.post",
		"Acme-Ledger.Post",
		"acme.ledger.post.extra",
		string(duty.SoftwareChangeMerge),
		strings.Repeat("x", 64),
	}
	list := make([]*v2.DutyMapping, 0, len(ids))
	for _, id := range ids {
		list = append(list, duty.Inclusive(id).Proto())
	}
	in := v2.DutyMappingsAnnotation_builder{Mappings: list}.Build()
	require.NoError(t, duty.Validate(in))

	out := s.transformAnnotations([]*anypb.Any{mustAny(t, in)}, newAssetRefSet())
	require.Len(t, out, 1)
	got := mustUnpack(t, out[0], &v2.DutyMappingsAnnotation{})
	require.NoError(t, duty.Validate(got), "sanitized IDs satisfy the wire rules and stay distinct")

	seen := map[string]struct{}{}
	for i, m := range got.GetMappings() {
		require.Equal(t, v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_INCLUSIVE, m.GetPrecision())
		if duty.IsKnown(ids[i]) {
			require.Equal(t, ids[i], m.GetDutyId(), "known duty IDs pass through")
		} else {
			require.NotEqual(t, ids[i], m.GetDutyId(), "unknown duty IDs are rewritten")
			require.False(t, duty.IsKnown(m.GetDutyId()))
			require.Equal(t, s.sanitizeDutyID(ids[i]), m.GetDutyId(), "rewriting is deterministic")
		}
		seen[m.GetDutyId()] = struct{}{}
	}
	require.Len(t, seen, len(ids))
}
