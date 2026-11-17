package dutytest

import (
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/duty"
	"github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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
	AssertEntitlement(t, newEntitlement(t, entitlement.WithDuties(duty.Exact("payment.release"), duty.Inclusive("payment.authorize"))))

	invalid := newEntitlement(t, entitlement.WithDuties(duty.Exact("Payment-Release")))
	r := &recorder{TB: t}
	AssertEntitlement(r, invalid)
	require.Len(t, r.errors, 1, "an ID outside the naming convention fails")

	twice := newEntitlement(t,
		entitlement.WithDuties(duty.Exact("payment.release")),
		entitlement.WithDuties(duty.Exact("payment.prepare")),
	)
	r = &recorder{TB: t}
	AssertEntitlement(r, twice)
	require.Len(t, r.errors, 1, "a second annotation fails")

	undecodable := newEntitlement(t)
	undecodable.SetAnnotations([]*anypb.Any{{TypeUrl: "type.googleapis.com/c1.connector.v2.DutyMappingsAnnotation", Value: []byte{0xff, 0xff}}})
	r = &recorder{TB: t}
	AssertEntitlement(r, undecodable)
	require.Len(t, r.errors, 1)
	require.Contains(t, r.errors[0], "does not unmarshal")
}

func TestCases(t *testing.T) {
	cases := Cases()
	names := map[string]struct{}{}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			if _, dup := names[c.Name]; dup {
				t.Fatalf("case name %q is repeated", c.Name)
			}
			names[c.Name] = struct{}{}
			err := duty.Validate(c.Annotation)
			if c.Valid && err != nil {
				t.Errorf("duty.Validate: %v, want nil", err)
			}
			if !c.Valid && err == nil {
				t.Error("duty.Validate: nil, want an error")
			}
			if strings.HasSuffix(c.Name, "passes the wire rule") {
				if err := c.Annotation.ValidateAll(); err != nil {
					t.Errorf("ValidateAll: %v, want nil", err)
				}
			}
			b, err := proto.Marshal(c.Annotation)
			if err != nil {
				t.Fatalf("proto.Marshal: %v", err)
			}
			got := &v2.DutyMappingsAnnotation{}
			if err := proto.Unmarshal(b, got); err != nil {
				t.Fatalf("proto.Unmarshal: %v", err)
			}
			if !proto.Equal(c.Annotation, got) {
				t.Errorf("round trip changed the annotation: got %v, want %v", got, c.Annotation)
			}
		})
	}

	fresh := Cases()
	fresh[0].Annotation.GetMappings()[0].SetDutyId("mutated")
	if Cases()[0].Annotation.GetMappings()[0].GetDutyId() == "mutated" {
		t.Error("Cases returns shared messages")
	}
}
