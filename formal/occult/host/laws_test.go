// Deliverable 9 (docs/tasks/sync-formal-model-brief.md): the equational
// laws of formal/occult/LAWS.md checked by equality saturation against
// the defining equations in ../src/sync_laws.occult. Each check builds a
// fresh interpreter (no cross-check e-graph pollution), loads the axiom
// module into UniversalScope, loads the two query terms as separate
// modules, bridges require() access, saturates, and asks the solver for
// equivalence. Negative controls assert NON-equivalence so a saturation
// bug that merges everything cannot silently green the law set.
//
// This module is a local verification harness: it depends on the sibling
// engine checkout via a replace directive and is not part of the
// baton-sdk public module.
package host_test

import (
	"os"
	"path/filepath"
	"testing"

	occult "github.com/conductorone/occult"
	"github.com/conductorone/occult/ir"
)

func readSrc(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "src", name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

// findQueryNode mirrors the engine's axiom-test helper: the query is the
// last statement of a semicolon-separated module.
func findQueryNode(interp *occult.Interpreter, rootID ir.NodeID) ir.NodeID {
	node, ok := interp.Graph.Node(rootID)
	if !ok {
		return rootID
	}
	if node.Kind == ir.KindList {
		children := interp.Graph.SourcesOfType(rootID, ir.EdgeAst)
		if len(children) > 0 {
			return children[len(children)-1]
		}
	}
	return rootID
}

var lawModules = []localModule{
	{"sync_laws", "sync_laws.occult"},
	{"sync_fixtures", "sync_fixtures.occult"},
}

// areEquivalent runs the shared pipeline with the law modules loaded.
func areEquivalent(t *testing.T, lhs, rhs string) bool {
	t.Helper()
	return equivalent(t, nil, lawModules, lhs, rhs)
}

// prelude prefixes every query with the module imports.
const prelude = `exists L; L = require("sync_laws"); exists F; F = require("sync_fixtures"); `

type lawCase struct {
	name string
	lhs  string
	rhs  string
}

// Positive laws: MUST be equivalent. Names follow formal/occult/LAWS.md.
var lawCases = []lawCase{
	// L1 — REPLACES absorbs (and the axiom-firing sanity checks for the
	// absorption clauses).
	{"L1_fresh_absorbs", `L.app(L.fresh(F.e1), F.x0)`, `L.rows(F.e1)`},
	{"L1_fresh_absorbs_suffix1", `L.foldops(L.opscons(L.fresh(F.e1), L.opsnil), F.x0)`, `L.rows(F.e1)`},
	{"L1_fresh_absorbs_suffix2", `L.foldops(L.opscons(F.op0, L.opscons(L.fresh(F.e1), L.opsnil)), F.x0)`, `L.rows(F.e1)`},
	// L2 — overlay grounds and composes.
	{"L2_overlay_grounds", `L.app(L.ovl(F.e1, F.e2), L.rows(F.e1))`, `L.rows(F.e2)`},
	{"L2_overlay_transitive", `L.app(L.ovl(F.e2, F.e3), L.app(L.ovl(F.e1, F.e2), L.rows(F.e1)))`, `L.app(L.ovl(F.e1, F.e3), L.rows(F.e1))`},
	// L3 — self-grounding overlay absorbs.
	{"L3_selfgrounding_absorbs", `L.app(L.ovlsg(F.e1), F.x0)`, `L.rows(F.e1)`},
	// L4 — copy-skip is identity on the current value.
	{"L4_skip_identity", `L.app(L.skip(F.e1), L.rows(F.e1))`, `L.rows(F.e1)`},
	// L5 — replay-copy idempotence (apply-level).
	{"L5_repl_idempotent", `L.app(L.repl(F.e1), L.app(L.repl(F.e1), F.x0))`, `L.app(L.repl(F.e1), F.x0)`},
	{"L5_fresh_idempotent", `L.app(L.fresh(F.e1), L.app(L.fresh(F.e1), F.x0))`, `L.app(L.fresh(F.e1), F.x0)`},
	// L6 — distinct-id commutation (observational, both observers) and
	// the same-id page-order clause.
	{"L6_commute_obs1", `L.get(L.id1, L.del(L.id1, L.put(L.id2, F.v0, F.m0)))`, `L.get(L.id1, L.put(L.id2, F.v0, L.del(L.id1, F.m0)))`},
	{"L6_commute_obs2", `L.get(L.id2, L.del(L.id1, L.put(L.id2, F.v0, F.m0)))`, `L.get(L.id2, L.put(L.id2, F.v0, L.del(L.id1, F.m0)))`},
	{"L6_tombstone_after_upsert", `L.get(L.id1, L.del(L.id1, L.put(L.id1, F.v0, F.m0)))`, `L.absent`},
	// L8 — dead-membership is absorbing under merge.
	{"L8_dead_absorbs_left", `L.hasdead(L.merge(L.dead(F.g0), F.y0))`, `L.tt`},
	{"L8_dead_absorbs_right", `L.hasdead(L.merge(F.y0, L.dead(F.g0)))`, `L.tt`},
	{"L8_dead_absorbs_deep", `L.hasdead(L.merge(L.merge(F.x0, L.dead(F.g0)), F.y0))`, `L.tt`},
}

// Negative controls: MUST NOT be equivalent. A checker that cannot
// refuse these proves nothing about the positives.
var controlCases = []lawCase{
	{"N1_overlay_not_absorbing", `L.app(L.ovl(F.e1, F.e2), F.x0)`, `L.rows(F.e2)`},
	{"N2_skip_not_absorbing", `L.app(L.skip(F.e1), F.x0)`, `L.rows(F.e1)`},
	{"N3_live_not_dead", `L.hasdead(L.live(F.g0))`, `L.tt`},
	{"N4_distinct_epochs", `L.rows(F.e1)`, `L.rows(F.e2)`},
	{"N5_put_not_invisible", `L.get(L.id1, L.put(L.id1, F.v0, F.m0))`, `L.get(L.id1, F.m0)`},
}

func TestLaws(t *testing.T) {
	for _, c := range lawCases {
		t.Run(c.name, func(t *testing.T) {
			if !areEquivalent(t, prelude+c.lhs, prelude+c.rhs) {
				t.Errorf("law %s: expected %q equivalent to %q", c.name, c.lhs, c.rhs)
			}
		})
	}
}

func TestNegativeControls(t *testing.T) {
	for _, c := range controlCases {
		t.Run(c.name, func(t *testing.T) {
			if areEquivalent(t, prelude+c.lhs, prelude+c.rhs) {
				t.Errorf("control %s: %q must NOT be equivalent to %q — the checker cannot refuse a wrong law", c.name, c.lhs, c.rhs)
			}
		})
	}
}
