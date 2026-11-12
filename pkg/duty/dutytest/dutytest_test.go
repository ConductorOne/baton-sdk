package dutytest

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/duty"
	"github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
)

// recorder captures failures instead of failing the enclosing test.
type recorder struct {
	testing.TB
	errors []string
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(format string, args ...any) {
	r.errors = append(r.errors, format)
}

func newEntitlement(t *testing.T, opts ...entitlement.EntitlementOption) *v2.Entitlement {
	rt := resource.NewResourceType("Role", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE})
	r, err := resource.NewResource("payments", rt, "payments")
	require.NoError(t, err)
	return entitlement.NewPermissionEntitlement(r, "release", opts...)
}

func TestAssertEntitlement(t *testing.T) {
	AssertEntitlement(t, newEntitlement(t))
	AssertEntitlement(t, newEntitlement(t, entitlement.WithDuties(duty.Exact(duty.PaymentRelease))))

	typo := newEntitlement(t, entitlement.WithDuties(duty.Exact("payment.relase")))
	r := &recorder{TB: t}
	AssertEntitlement(r, typo)
	require.Len(t, r.errors, 1)
	AssertEntitlement(t, typo, duty.AllowUnknown("payment.relase"))

	twice := newEntitlement(t, entitlement.WithDuties(duty.Exact(duty.PaymentRelease)))
	extra, err := anypb.New(v2.DutyMappingsAnnotation_builder{
		Mappings: []*v2.DutyMapping{duty.Exact(duty.PaymentPrepare).Proto()},
	}.Build())
	require.NoError(t, err)
	twice.SetAnnotations(append(twice.GetAnnotations(), extra))
	r = &recorder{TB: t}
	AssertEntitlement(r, twice)
	require.Len(t, r.errors, 1)
}
