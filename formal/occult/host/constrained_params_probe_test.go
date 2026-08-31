// Semantics pin: constrained axiom universals under axiomatic loading.
//
// Context: the engine author's feedback on our modules — "your
// parameters in the examples are largely unconstrained ... Something
// like p(x: Number) instead limits the matching of the callable" (lint
// kind unconstrained-axiom-universal). The hazard is real: an
// unconstrained universal matches e-classes of any shape, including
// cross-model classes merged by module bridges (the engine's
// COMMON_MISTAKES.md §7 names the arithmetic-versus-Peano regression
// this lint was added for).
//
// This probe pins why we have NOT adopted the fix for our free-
// constructor algebras (sync_trace_policies, sync_laws, sync_phantom):
// under LoadStdlib/axiomatic loading, a userspace-sort constraint on an
// axiom universal gates dispatch on classifier membership that free
// constructor terms cannot currently be given. Every documented
// membership form was tried — set extension (Scope = {s1, s2,}), set
// extension plus distinctness (Scope ∈ std.types.Unique), declaration-
// with-membership (∃ s1 ∈ Scope;), and an explicit rewrite premise
// (s ∈ Scope ⇒ rule) — and in all four the member-only green fixture
// stops reducing: membership stays undecided at dispatch, the rule is
// ineligible, the verdict is a stuck term. Built-in classifiers DO
// work (the Number probe below), so the gap is specifically userspace
// free-term membership evidence. This is the same root cause that
// blocked parameterized MPST projection (sync_protocol.occult's
// closed-term rewrite); the engine's own stdlib shows the unresolved
// state — model/stdlib/vector_clock.occult constrains its universals
// to a VectorClock sort that nothing anywhere gives members, which is
// why sync_laws.occult copies the vc_merge ACI axioms instead of
// requiring the module.
//
// The pins below assert TODAY'S behavior. When the engine lands
// membership evidence for free-term classifiers (see the "constrained
// universals" ask in docs/tasks/occult-engine-changes-brief.md),
// TestConstrainedParamsUserspaceMembershipNotConsumed fails — that is
// the signal to constrain the real modules' scope/flag universals and
// delete the pin.
package host_test

import (
	"context"
	"fmt"
	"testing"

	occult "github.com/conductorone/occult"
	"github.com/conductorone/occult/state"
)

// probeConstrainedSrc gates the mini-policy's scope and flag universals
// on userspace sorts, with membership stated in the declaration form
// (∃ member ∈ Sort;). Trace tails (t, r) stay unconstrained — traces
// are inductively generated, not enumerable.
const probeConstrainedSrc = `# lint:disable mixed-source-semantics
# lint:disable unguarded-recursion
# lint:disable unconstrained-axiom-universal
syntax("standard");

∃ ev_a;
∃ ev_b;
∃ s_rogue;
∃ tnil;
∃ tcons;

∃ Scope;
∃ s1 ∈ Scope;
∃ s2 ∈ Scope;
∃ Flag;
∃ tt ∈ Flag;
∃ ff ∈ Flag;

s : Scope;
f : Flag;

∃ probe;
∃ probe_go;
probe(t) = probe_go(t, ff);
probe_go(tnil, f) = "ok";
probe_go(tcons(ev_a(s), r), f) = probe_go(r, tt);
probe_go(tcons(ev_b(s), r), tt) = probe_go(r, tt);
probe_go(tcons(ev_b(s), r), ff) = "violation: b-before-a";

∃ probe_green;
probe_green = tcons(ev_a(s1), tcons(ev_b(s2), tnil));
∃ probe_rogue;
probe_rogue = tcons(ev_a(s_rogue), tnil);
`

// probePremiseSrc states the gate as an explicit rewrite premise
// (s ∈ Scope ⇒ rule) — the docs' other prescribed form. Note the
// implication arrow must be ⇒ (U+21D2); ASCII => is rejected at axiom
// load with "rewrite '=>' requires rewrite LHS".
const probePremiseSrc = `# lint:disable mixed-source-semantics
# lint:disable unguarded-recursion
# lint:disable unconstrained-axiom-universal
syntax("standard");

∃ ev_a;
∃ ev_b;
∃ tnil;
∃ tcons;
∃ tt;
∃ ff;

∃ Scope;
∃ s1 ∈ Scope;
∃ s2 ∈ Scope;

∃ probe;
∃ probe_go;
probe(t) = probe_go(t, ff);
probe_go(tnil, f) = "ok";
s ∈ Scope ⇒ probe_go(tcons(ev_a(s), r), f) = probe_go(r, tt);
s ∈ Scope ⇒ probe_go(tcons(ev_b(s), r), tt) = probe_go(r, tt);
s ∈ Scope ⇒ probe_go(tcons(ev_b(s), r), ff) = "violation: b-before-a";

∃ probe_green;
probe_green = tcons(ev_a(s1), tcons(ev_b(s2), tnil));
`

// probeBuiltinSrc constrains the scope universal to the BUILT-IN Number
// classifier with numeric scope payloads: the control proving the
// dispatch gate itself works when membership is decidable.
const probeBuiltinSrc = `# lint:disable mixed-source-semantics
# lint:disable unguarded-recursion
# lint:disable unconstrained-axiom-universal
syntax("standard");

∃ ev_a;
∃ ev_b;
∃ tnil;
∃ tcons;
∃ tt;
∃ ff;

s : Number;

∃ probe;
∃ probe_go;
probe(t) = probe_go(t, ff);
probe_go(tnil, f) = "ok";
probe_go(tcons(ev_a(s), r), f) = probe_go(r, tt);
probe_go(tcons(ev_b(s), r), tt) = probe_go(r, tt);
probe_go(tcons(ev_b(s), r), ff) = "violation: b-before-a";

∃ probe_green;
probe_green = tcons(ev_a(1), tcons(ev_b(2), tnil));
`

// probeEval loads module src as name and evaluates M.probe(term),
// returning the verdict string and whether it reduced to one.
func probeEval(t *testing.T, name, src, term string) (string, bool) {
	t.Helper()
	interp, pm, err := occult.NewCLIInterpreter("egraph", false, "", "")
	if err != nil {
		t.Fatalf("NewCLIInterpreter: %v", err)
	}
	if _, err := interp.LoadStdlib(name, src, pm); err != nil {
		t.Fatalf("LoadStdlib %s: %v", name, err)
	}
	source := fmt.Sprintf(`M = require(%q); M.probe(%s)`, name, term)
	res, err := interp.Eval(context.Background(), "probe", source, pm)
	if err != nil {
		t.Fatalf("Eval %q: %v", source, err)
	}
	if res == nil || res.Loc == nil {
		return "", false
	}
	st, ok := interp.State.Resolve(*res.Loc)
	if !ok || st.Literal == nil || st.Literal.Kind != state.LitString {
		return "", false
	}
	return st.Literal.Str, true
}

// TestConstrainedParamsBuiltinClassifierWorks is the positive control:
// with a built-in classifier (Number) the constrained rule fires and
// the verdict reduces. The gate mechanism itself is sound.
func TestConstrainedParamsBuiltinClassifierWorks(t *testing.T) {
	if v, ok := probeEval(t, "probe_builtin", probeBuiltinSrc, "M.probe_green"); !ok || v != "ok" {
		t.Fatalf("green fixture under built-in Number gating: got (%q, reduced=%v), want (\"ok\", true)", v, ok)
	}
}

// TestConstrainedParamsGateRefusesNonMembers pins that the gate is
// enforced at dispatch, not advisory lint metadata: a trace carrying a
// declared-but-non-member scope does not reduce to a verdict.
func TestConstrainedParamsGateRefusesNonMembers(t *testing.T) {
	if v, ok := probeEval(t, "probe_constrained", probeConstrainedSrc, "M.probe_rogue"); ok {
		t.Fatalf("non-member scope reduced to %q: constraints are not gating dispatch", v)
	}
}

// TestConstrainedParamsUserspaceMembershipNotConsumed pins the gap: a
// MEMBER-only trace under userspace-sort gating does not reduce either
// — declared membership (∃ s1 ∈ Scope;) and explicit ∈-premises are
// both invisible to axiom-side dispatch, so the gate treats members
// like non-members and the rule never fires.
//
// WHEN THIS TEST FAILS (a verdict reduces): the engine has learned to
// consume userspace membership evidence. Constrain the scope/flag
// universals in sync_trace_policies.occult and sync_phantom.occult per
// the plan in the engine-changes brief, then delete this pin.
func TestConstrainedParamsUserspaceMembershipNotConsumed(t *testing.T) {
	if v, ok := probeEval(t, "probe_constrained", probeConstrainedSrc, "M.probe_green"); ok {
		t.Fatalf("member-only trace reduced to %q under declaration-form membership: "+
			"the engine now consumes userspace membership — constrain the real modules and delete this pin", v)
	}
	if v, ok := probeEval(t, "probe_premise", probePremiseSrc, "M.probe_green"); ok {
		t.Fatalf("member-only trace reduced to %q under ∈-premise gating: "+
			"the engine now consumes userspace membership — constrain the real modules and delete this pin", v)
	}
}
