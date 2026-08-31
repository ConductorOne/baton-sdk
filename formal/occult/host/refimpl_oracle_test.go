// The demand-graph reference implementation (refimpl/) tried out
// against two oracles on the phantom-union scenario:
//
//   - CONTENT oracle (Go): the sealed artifact must equal upstream at
//     the head epoch.
//   - TRACE oracle (engine): every attempt's canonical trace is checked
//     against all five deliverable-7 policies.
//
// The matrix this asserts:
//
//   demand-graph, no crash   -> content true,  all policies ok
//   demand-graph, crash+resume -> content true, all policies ok (both attempts)
//   legacy, no crash         -> content FALSE (phantom row) but all
//     policies ok — the ordering policies are provably blind to the
//     composition bug; that class is owned by the algebra
//     (phantom_test.go derives it deductively)
//   legacy, crash+resume     -> content FALSE and attempt 2 violates
//     exactly clear-before-upsert (resume-without-regrounding is an
//     ordering bug, and the oracle catches it)
package host_test

import (
	"reflect"
	"testing"

	"github.com/conductorone/baton-sdk/formal/occult/host/refimpl"
)

// The sync_phantom.occult scenario: id1 deleted between e0 and e1,
// id2's value changes between e1 and e2, the source cache is attested
// at e0, the previous sync completed at e1, and this sync targets e2.
func phantomConfig(mode refimpl.Mode, crash bool) refimpl.Config {
	up := refimpl.NewUpstream(
		map[string]string{"id1": "vx", "id2": "v1"}, // e0
		map[string]string{"id2": "v1"},              // e1
		map[string]string{"id2": "v2"},              // e2
	)
	return refimpl.Config{
		Mode:             mode,
		Upstream:         up,
		Head:             2,
		Cache:            refimpl.Cache{Base: 0, Rows: up.Rows(0)},
		LastSyncEpoch:    1,
		CrashAfterReplay: crash,
	}
}

var truthAtHead = map[string]string{"id2": "v2"}
var phantomArtifact = map[string]string{"id1": "vx", "id2": "v2"}

// checkAttempts runs every attempt trace through all five policies and
// asserts the expected verdict: expectViolation maps attempt index
// (0-based) to the one policy that must fire; every other cell must be
// "ok".
func checkAttempts(t *testing.T, attempts [][]refimpl.Event, expectViolation map[int]string) {
	t.Helper()
	for i, trace := range attempts {
		term := refimpl.RenderOccult("M", trace)
		for _, policy := range policies {
			verdict := policyVerdictTerm(t, policy, term)
			if expectViolation[i] == policy {
				if verdict != "violation: "+policyLabel(policy) {
					t.Errorf("attempt %d: expected %s to fire, got %q", i+1, policy, verdict)
				}
			} else if verdict != "ok" {
				t.Errorf("attempt %d: expected %s ok, got %q", i+1, policy, verdict)
			}
		}
	}
}

func TestRefImplDemandGraph(t *testing.T) {
	res := refimpl.Run(phantomConfig(refimpl.ModeDemandGraph, false))
	if !reflect.DeepEqual(res.Sealed, truthAtHead) {
		t.Errorf("sealed content %v, want upstream truth %v", res.Sealed, truthAtHead)
	}
	if len(res.Attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(res.Attempts))
	}
	checkAttempts(t, res.Attempts, nil)
}

func TestRefImplDemandGraphCrashResume(t *testing.T) {
	res := refimpl.Run(phantomConfig(refimpl.ModeDemandGraph, true))
	if !reflect.DeepEqual(res.Sealed, truthAtHead) {
		t.Errorf("sealed content %v, want upstream truth %v", res.Sealed, truthAtHead)
	}
	if len(res.Attempts) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(res.Attempts))
	}
	// Attempt 1 is a crash-cut prefix; attempt 2 re-executes the node
	// under a fresh generation (consult, clear, replay again). Both
	// must satisfy every policy.
	checkAttempts(t, res.Attempts, nil)
}

func TestRefImplLegacyPhantom(t *testing.T) {
	res := refimpl.Run(phantomConfig(refimpl.ModeLegacy, false))
	if !reflect.DeepEqual(res.Sealed, phantomArtifact) {
		t.Errorf("sealed content %v, want the phantom artifact %v", res.Sealed, phantomArtifact)
	}
	if reflect.DeepEqual(res.Sealed, truthAtHead) {
		t.Errorf("legacy mode unexpectedly produced the true artifact")
	}
	// The ordering policies are BLIND to this one: the event order is
	// identical to the honest run. The finding lives in the content
	// oracle here and in the algebra deductively (phantom_test.go).
	checkAttempts(t, res.Attempts, nil)
}

func TestRefImplLegacyCrashResume(t *testing.T) {
	res := refimpl.Run(phantomConfig(refimpl.ModeLegacy, true))
	if !reflect.DeepEqual(res.Sealed, phantomArtifact) {
		t.Errorf("sealed content %v, want the phantom artifact %v", res.Sealed, phantomArtifact)
	}
	if len(res.Attempts) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(res.Attempts))
	}
	// Attempt 2 resumed onto the dead attempt's partition without
	// re-grounding: upserts with no clear this attempt. The trace
	// oracle must catch exactly clear-before-upsert.
	checkAttempts(t, res.Attempts, map[int]string{1: "clear_before_upsert"})
}
