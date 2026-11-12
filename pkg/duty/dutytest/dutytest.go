// Package dutytest checks duty annotations in connector tests, without a
// ConductorOne instance.
package dutytest

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/duty"
)

// AssertEntitlement fails t if ent's DutyMappingsAnnotation does not pass
// duty.Lint with opts, does not unmarshal, or appears more than once. An
// entitlement without a duty annotation passes.
func AssertEntitlement(t testing.TB, ent *v2.Entitlement, opts ...duty.LintOption) {
	t.Helper()
	count := 0
	for _, a := range ent.GetAnnotations() {
		if !a.MessageIs(&v2.DutyMappingsAnnotation{}) {
			continue
		}
		count++
		ann := &v2.DutyMappingsAnnotation{}
		if err := a.UnmarshalTo(ann); err != nil {
			t.Errorf("entitlement %q: duty annotation does not unmarshal: %v", ent.GetId(), err)
			continue
		}
		if err := duty.Lint(ann, opts...); err != nil {
			t.Errorf("entitlement %q: %v", ent.GetId(), err)
		}
	}
	if count > 1 {
		t.Errorf("entitlement %q has %d duty annotations, want at most 1", ent.GetId(), count)
	}
}
