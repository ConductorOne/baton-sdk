package duty

import (
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func annotation(mappings ...Mapping) *v2.DutyMappingsAnnotation {
	list := make([]*v2.DutyMapping, 0, len(mappings))
	for _, m := range mappings {
		list = append(list, m.Proto())
	}
	return v2.DutyMappingsAnnotation_builder{Mappings: list}.Build()
}

func TestValidID(t *testing.T) {
	type name string
	for _, id := range []string{"payment.release", "vendor_payment_instructions.change", "a.b", "a1.b2_c", "a." + strings.Repeat("b", 62)} {
		require.True(t, ValidID(id), "%q", id)
	}
	require.True(t, ValidID(name("payment.release")), "accepts typed strings")
	for _, id := range []string{
		"",
		"payment",
		"vendor.payment_instructions.modify",
		"payment-run.release",
		"Payment.Release",
		"1payment.release",
		"payment._release",
		".release",
		"payment.",
		"payment release",
		"a." + strings.Repeat("b", 63),
	} {
		require.False(t, ValidID(id), "%q", id)
	}
}

func TestValidate(t *testing.T) {
	many := make([]Mapping, 0, 17)
	for i := range 17 {
		many = append(many, Inclusive("duty.d"+strings.Repeat("x", i)))
	}
	tests := []struct {
		name    string
		ann     *v2.DutyMappingsAnnotation
		wantErr string
	}{
		{name: "exact", ann: annotation(Exact("payment.release"))},
		{name: "inclusive with description", ann: annotation(Inclusive("payment.prepare").WithDescription("admin role"), Inclusive("payment.authorize"))},
		{name: "16 mappings", ann: annotation(many[:16]...)},
		{name: "max length", ann: annotation(Exact("a." + strings.Repeat("b", 62)))},
		{name: "undefined precision", ann: annotation(Mapping{dutyID: "payment.release", precision: 99})},
		{name: "no mappings", ann: annotation(), wantErr: "Mappings"},
		{name: "17 mappings", ann: annotation(many...), wantErr: "Mappings"},
		{name: "empty id", ann: annotation(Exact("")), wantErr: "DutyId"},
		{name: "space", ann: annotation(Exact("payment release")), wantErr: "DutyId"},
		{name: "slash", ann: annotation(Exact("payment/release")), wantErr: "DutyId"},
		{name: "too long", ann: annotation(Exact(strings.Repeat("a", 65))), wantErr: "DutyId"},
		{name: "unspecified precision", ann: annotation(Mapping{dutyID: "payment.release"}), wantErr: "Precision"},
		{name: "description too long", ann: annotation(Exact("payment.release").WithDescription(strings.Repeat("x", 1025))), wantErr: "Description"},
		{
			name:    "duplicate duty",
			ann:     annotation(Exact("payment.release"), Inclusive("payment.prepare"), Inclusive("payment.release")),
			wantErr: `mapping 2: duty "payment.release" is already mapped at index 0`,
		},
		{name: "no dot", ann: annotation(Inclusive("payment.prepare"), Exact("payment")), wantErr: `mapping 1: duty "payment" does not follow`},
		{name: "three segments", ann: annotation(Exact("vendor.payment_instructions.modify")), wantErr: `mapping 0: duty "vendor.payment_instructions.modify" does not follow`},
		{name: "hyphen", ann: annotation(Exact("payment-run.release")), wantErr: "does not follow"},
		{name: "uppercase", ann: annotation(Exact("Payment.Release")), wantErr: "does not follow"},
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

func TestValidateReportsEveryMapping(t *testing.T) {
	err := Validate(annotation(Exact("Payment-Run"), Exact("payment.release"), Inclusive("payment.release")))
	require.Error(t, err)
	require.Equal(t, strings.Join([]string{
		`mapping 0: duty "Payment-Run" does not follow the <object>.<action> naming convention`,
		`mapping 2: duty "payment.release" is already mapped at index 1`,
	}, "\n"), err.Error())
}

func TestUndefinedPrecisionRoundTrips(t *testing.T) {
	in := annotation(Mapping{dutyID: "payment.release", precision: 99})
	b, err := proto.Marshal(in)
	require.NoError(t, err)
	out := &v2.DutyMappingsAnnotation{}
	require.NoError(t, proto.Unmarshal(b, out))
	require.Equal(t, v2.DutyMappingPrecision(99), out.GetMappings()[0].GetPrecision())
	require.NoError(t, Validate(out))
}

func TestMappingProto(t *testing.T) {
	type name string

	m := Exact(name("payment.release")).Proto()
	require.Equal(t, "payment.release", m.GetDutyId())
	require.Equal(t, v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_EXACT, m.GetPrecision())
	require.Empty(t, m.GetDescription())

	base := Inclusive("payment.authorize")
	described := base.WithDescription("Finance Administrator role")
	m = described.Proto()
	require.Equal(t, "payment.authorize", m.GetDutyId())
	require.Equal(t, v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_INCLUSIVE, m.GetPrecision())
	require.Equal(t, "Finance Administrator role", m.GetDescription())
	require.Empty(t, base.Proto().GetDescription(), "WithDescription returns a copy")
	require.Equal(t, "replaced", described.WithDescription("replaced").Proto().GetDescription())
}
