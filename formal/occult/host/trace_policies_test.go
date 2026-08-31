// Deliverable 7: the trace-policy oracle set
// (../src/sync_trace_policies.occult) checked as a full verdict matrix
// — every policy against every fixture. The green fixture must satisfy
// all five policies; each red fixture must violate EXACTLY its own
// policy and satisfy the other four, so a policy that silently accepts
// everything (or rejects everything) cannot pass.
package host_test

import (
	"context"
	"fmt"
	"testing"

	occult "github.com/conductorone/occult"
	"github.com/conductorone/occult/state"
)

// policyVerdictTerm evaluates one policy applied to a trace TERM
// (any expression over the module's constructors, module handle "M")
// and returns the engine's verdict string.
func policyVerdictTerm(t *testing.T, policy, term string) string {
	t.Helper()
	interp, pm, err := occult.NewCLIInterpreter("egraph", false, "", "")
	if err != nil {
		t.Fatalf("NewCLIInterpreter: %v", err)
	}
	if _, err := interp.LoadStdlib("sync_trace_policies", readSrc(t, "sync_trace_policies.occult"), pm); err != nil {
		t.Fatalf("LoadStdlib sync_trace_policies: %v", err)
	}
	source := fmt.Sprintf(`M = require("sync_trace_policies"); M.%s(%s)`, policy, term)
	res, err := interp.Eval(context.Background(), "policy-check", source, pm)
	if err != nil {
		t.Fatalf("Eval %q: %v", source, err)
	}
	if res == nil || res.Loc == nil {
		t.Fatalf("Eval %q: no result location", source)
	}
	st, ok := interp.State.Resolve(*res.Loc)
	if !ok || st.Literal == nil || st.Literal.Kind != state.LitString {
		t.Fatalf("Eval %q: verdict did not reduce to a string", source)
	}
	return st.Literal.Str
}

// policyVerdict evaluates one policy applied to one named fixture trace.
func policyVerdict(t *testing.T, policy, trace string) string {
	t.Helper()
	return policyVerdictTerm(t, policy, "M."+trace)
}

var policies = []string{
	"consult_before_replay",
	"clear_before_upsert",
	"once_per_scope",
	"checkpoint_before_progress",
	"seal_obligations",
}

// fixtureViolates maps each fixture to the single policy it violates
// ("" = none: the green trace).
var fixtureViolates = map[string]string{
	"trace_green":          "",
	"trace_red_cbr":        "consult_before_replay",
	"trace_red_cbu":        "clear_before_upsert",
	"trace_red_ops":        "once_per_scope",
	"trace_red_cbp":        "checkpoint_before_progress",
	"trace_red_seal":       "seal_obligations",
	"trace_green_resume":   "",
	"trace_red_ops_resume": "once_per_scope",
	"trace_green_delete":   "",
	"trace_red_cbu_del":    "clear_before_upsert",
	"trace_red_cbp_del":    "checkpoint_before_progress",
	"trace_red_seal_del":   "seal_obligations",
}

func TestTracePolicyMatrix(t *testing.T) {
	for fixture, violated := range fixtureViolates {
		for _, policy := range policies {
			t.Run(fixture+"/"+policy, func(t *testing.T) {
				verdict := policyVerdict(t, policy, fixture)
				if policy == violated {
					if verdict != "violation: "+policyLabel(policy) {
						t.Errorf("expected %s to violate %s, got verdict %q", fixture, policy, verdict)
					}
				} else {
					if verdict != "ok" {
						t.Errorf("expected %s to satisfy %s, got verdict %q", fixture, policy, verdict)
					}
				}
			})
		}
	}
}

func policyLabel(policy string) string {
	switch policy {
	case "consult_before_replay":
		return "consult-before-replay"
	case "clear_before_upsert":
		return "clear-before-upsert"
	case "once_per_scope":
		return "once-per-scope"
	case "checkpoint_before_progress":
		return "checkpoint-before-progress"
	case "seal_obligations":
		return "seal-obligations"
	}
	return policy
}
