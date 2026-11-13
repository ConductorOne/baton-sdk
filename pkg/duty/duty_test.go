package duty

import (
	"slices"
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

func TestIDs(t *testing.T) {
	ids := IDs()
	require.Len(t, ids, 25)
	require.True(t, slices.IsSorted(ids), "IDs is sorted")
	require.Len(t, slices.Compact(slices.Clone(ids)), len(ids), "IDs has no duplicates")

	mappings := make([]Mapping, 0, len(ids))
	for _, id := range ids {
		require.True(t, IsKnown(id), "%s is known", id)
		require.True(t, IsKnown(string(id)), "%s is known as a string", id)
		mappings = append(mappings, Exact(id))
	}
	require.NoError(t, Lint(annotation(mappings...)))

	ids[0] = "mutated"
	require.NotEqual(t, ID("mutated"), IDs()[0], "IDs returns a copy")
	require.False(t, IsKnown("payment.relase"))
}

// pinnedIDs is every duty ID this package has shipped. Duty IDs are
// permanent: consumers store them and compare them across SDK versions, so
// removing or renaming one would silently change what stored mappings mean.
// Add new IDs here; never delete or edit an entry. A retired duty keeps its
// constant, marked Deprecated, and stays in IDs.
var pinnedIDs = []string{
	"access.certify",
	"access_request.approve",
	"access_request.submit",
	"audit_log.configure",
	"change_control.bypass",
	"change_control.modify",
	"deployment_control.modify",
	"entitlement.assign",
	"invoice.approve",
	"invoice.record",
	"journal_entry.approve",
	"journal_entry.prepare",
	"payment.authorize",
	"payment.prepare",
	"payment.release",
	"purchase_order.approve",
	"purchase_order.create",
	"release.deploy",
	"software_change.approve",
	"software_change.author",
	"software_change.merge",
	"vendor.activate",
	"vendor.create_draft",
	"vendor_payment_instructions.change",
	"vendor_payment_instructions.verify",
}

func TestIDsArePermanent(t *testing.T) {
	require.True(t, slices.IsSorted(pinnedIDs), "keep pinnedIDs sorted")
	ids := IDs()
	for _, id := range pinnedIDs {
		require.Contains(t, ids, ID(id), "duty ID %q was removed or renamed; duty IDs are permanent", id)
	}
	for _, id := range ids {
		require.Contains(t, pinnedIDs, string(id), "duty ID %q is new; add it to pinnedIDs so it cannot be removed later", id)
	}
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
		{name: "conditional without description", ann: annotation(Conditional(SoftwareChangeApprove, ""))},
		{name: "unknown id", ann: annotation(Exact("payment.refund"))},
		{name: "three segments", ann: annotation(Exact("vendor.payment_instructions.modify"))},
		{name: "hyphenated", ann: annotation(Exact("payment-run.release"))},
		{name: "no dot", ann: annotation(Exact("payment"))},
		{name: "uppercase", ann: annotation(Exact("Payment.Release"))},
		{name: "max length", ann: annotation(Exact(strings.Repeat("a", 64)))},
		{name: "unknown precision", ann: annotation(Mapping{dutyID: string(PaymentRelease), precision: 99})},
		{name: "no mappings", ann: annotation(), wantErr: "Mappings"},
		{name: "empty id", ann: annotation(Exact("")), wantErr: "DutyId"},
		{name: "space", ann: annotation(Exact("payment release")), wantErr: "DutyId"},
		{name: "slash", ann: annotation(Exact("payment/release")), wantErr: "DutyId"},
		{name: "too long", ann: annotation(Exact(strings.Repeat("a", 65))), wantErr: "DutyId"},
		{name: "unspecified precision", ann: annotation(Mapping{dutyID: string(PaymentRelease)}), wantErr: "Precision"},
		{name: "description too long", ann: annotation(Exact(PaymentRelease).WithDescription(strings.Repeat("x", 1025))), wantErr: "Description"},
		{name: "duplicate duty", ann: annotation(Exact(PaymentRelease), Inclusive(PaymentRelease)), wantErr: "mapped more than once"},
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

func TestUnknownPrecisionRoundTrips(t *testing.T) {
	in := annotation(Mapping{dutyID: string(PaymentRelease), precision: 99})
	b, err := proto.Marshal(in)
	require.NoError(t, err)
	out := &v2.DutyMappingsAnnotation{}
	require.NoError(t, proto.Unmarshal(b, out))
	require.Equal(t, v2.DutyMappingPrecision(99), out.GetMappings()[0].GetPrecision())
	require.NoError(t, Validate(out))
}

func TestLint(t *testing.T) {
	require.NoError(t, Lint(annotation(Exact(PaymentRelease), Inclusive(SoftwareChangeMerge))))

	typo := annotation(Exact(PaymentRelease), Exact("payment.relase"))
	require.NoError(t, Validate(typo), "Validate accepts unknown IDs")
	err := Lint(typo)
	require.ErrorContains(t, err, `"payment.relase" is not a known duty ID`)
	require.NoError(t, Lint(typo, AllowUnknown("payment.relase")))

	err = Lint(annotation(Exact("Payment-Run"), Exact("zeta.one"), Exact("alpha.one")))
	require.Error(t, err)
	require.Equal(t, strings.Join([]string{
		`duty "Payment-Run" is not a known duty ID; check for a typo or use AllowUnknown`,
		`duty "Payment-Run" does not follow the <object>.<action> naming convention`,
		`duty "zeta.one" is not a known duty ID; check for a typo or use AllowUnknown`,
		`duty "alpha.one" is not a known duty ID; check for a typo or use AllowUnknown`,
	}, "\n"), err.Error(), "errors follow mapping order")
	require.NoError(t, Lint(annotation(Exact("Payment-Run")), AllowUnknown("Payment-Run")), "allowed IDs skip the naming check")

	require.ErrorContains(t, Lint(annotation(Exact(PaymentRelease), Exact(PaymentRelease)), AllowUnknown(string(PaymentRelease))), "mapped more than once", "Lint runs Validate")
}

func TestMappingProto(t *testing.T) {
	m := Conditional(SoftwareChangeApprove, "branch protection").Proto()
	require.Equal(t, string(SoftwareChangeApprove), m.GetDutyId())
	require.Equal(t, v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_CONDITIONAL, m.GetPrecision())
	require.Equal(t, "branch protection", m.GetDescription())

	m = Exact(PaymentRelease).WithDescription("Payment Release role").Proto()
	require.Equal(t, v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_EXACT, m.GetPrecision())
	require.Equal(t, "Payment Release role", m.GetDescription())

	require.Equal(t, ID("custom.duty"), Inclusive("custom.duty").ID())
}
