package duty

import (
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

func annotation(mappings ...Mapping) *v2.DutyMappingsAnnotation {
	list := make([]*v2.DutyMapping, 0, len(mappings))
	for _, m := range mappings {
		list = append(list, m.Proto())
	}
	return v2.DutyMappingsAnnotation_builder{
		CatalogId:      CatalogID,
		CatalogVersion: CatalogVersion,
		Mappings:       list,
	}.Build()
}

func TestIDsAreValidAndUnique(t *testing.T) {
	ids := IDs()
	mappings := make([]Mapping, 0, len(ids))
	for _, id := range ids {
		mappings = append(mappings, Exact(id))
	}
	require.NoError(t, Validate(annotation(mappings...)))
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		ann     *v2.DutyMappingsAnnotation
		wantErr string
	}{
		{name: "exact", ann: annotation(Exact(PaymentRelease))},
		{name: "inclusive with description", ann: annotation(Inclusive(PaymentPrepare).WithDescription("admin role"), Inclusive(PaymentAuthorize))},
		{name: "conditional with condition", ann: annotation(Conditional(SoftwareChangeApprove, "only when branch protection allows self-approval"))},
		{name: "no mappings", ann: annotation(), wantErr: "Mappings"},
		{name: "missing catalog id", ann: v2.DutyMappingsAnnotation_builder{Mappings: []*v2.DutyMapping{Exact(PaymentRelease).Proto()}}.Build(), wantErr: "CatalogId"},
		{name: "no dot", ann: annotation(Exact("payment")), wantErr: "DutyId"},
		{name: "two dots", ann: annotation(Exact("payment.release.now")), wantErr: "DutyId"},
		{name: "uppercase", ann: annotation(Exact("Payment.release")), wantErr: "DutyId"},
		{name: "leading digit", ann: annotation(Exact("payment.1release")), wantErr: "DutyId"},
		{name: "too long", ann: annotation(Exact("a." + strings.Repeat("b", 63))), wantErr: "DutyId"},
		{name: "unspecified precision", ann: annotation(Mapping{dutyID: PaymentRelease}), wantErr: "Precision"},
		{name: "unknown precision", ann: annotation(Mapping{dutyID: PaymentRelease, precision: 99}), wantErr: "Precision"},
		{name: "description too long", ann: annotation(Exact(PaymentRelease).WithDescription(strings.Repeat("x", 1025))), wantErr: "Description"},
		{name: "duplicate duty", ann: annotation(Exact(PaymentRelease), Inclusive(PaymentRelease)), wantErr: "mapped more than once"},
		{name: "conditional without condition", ann: annotation(Conditional(SoftwareChangeApprove, "")), wantErr: "has no description"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.ann)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestMappingProto(t *testing.T) {
	m := Conditional(SoftwareChangeApprove, "branch protection").Proto()
	require.Equal(t, SoftwareChangeApprove, m.GetDutyId())
	require.Equal(t, v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_CONDITIONAL, m.GetPrecision())
	require.Equal(t, "branch protection", m.GetDescription())

	m = Exact(PaymentRelease).WithDescription("Payment Release role").Proto()
	require.Equal(t, v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_EXACT, m.GetPrecision())
	require.Equal(t, "Payment Release role", m.GetDescription())
}
