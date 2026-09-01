// Shared equivalence pipeline: the engine's axiom-test sequence (load
// modules universal-scoped, load lhs/rhs query modules global-scoped,
// load axioms/equalities, bridge require and property access, saturate,
// ask the solver). Each call builds a fresh interpreter so no e-graph
// state leaks between checks.
package host_test

import (
	"context"
	"testing"

	occult "github.com/conductorone/occult"
	"github.com/conductorone/occult/ir"
	"github.com/conductorone/occult/modelset"
	"github.com/conductorone/occult/parse"
	"github.com/conductorone/occult/solve"
)

// localModule is a .occult source from ../src loaded into UniversalScope
// under the given require() name.
type localModule struct {
	name string
	file string
}

func equivalent(t *testing.T, requires []string, locals []localModule, lhs, rhs string) bool {
	t.Helper()
	reg, err := modelset.Registry()
	if err != nil {
		t.Fatalf("modelset.Registry: %v", err)
	}
	interp := occult.NewInterpreter(reg, solve.NewEGraphSolver())
	pm := parse.DefaultModel()
	ctx := context.Background()

	for _, name := range requires {
		if err := interp.RequireModule(name); err != nil {
			t.Fatalf("RequireModule %s: %v", name, err)
		}
	}
	for _, lm := range locals {
		if _, err := interp.LoadStdlib(lm.name, readSrc(t, lm.file), pm); err != nil {
			t.Fatalf("LoadStdlib %s: %v", lm.name, err)
		}
	}

	lhsRoot, err := interp.LoadModule("lhs", lhs, parse.DefaultModel())
	if err != nil {
		t.Fatalf("LoadModule lhs %q: %v", lhs, err)
	}
	rhsRoot, err := interp.LoadModule("rhs", rhs, parse.DefaultModel())
	if err != nil {
		t.Fatalf("LoadModule rhs %q: %v", rhs, err)
	}

	for _, root := range []ir.NodeID{lhsRoot, rhsRoot} {
		if err := interp.AssignScopes(root, interp.GlobalScope); err != nil {
			t.Fatalf("AssignScopes: %v", err)
		}
		if err := interp.LoadQuantifications(root); err != nil {
			t.Fatalf("LoadQuantifications: %v", err)
		}
		if err := interp.ApplyQuantifications(root); err != nil {
			t.Fatalf("ApplyQuantifications: %v", err)
		}
	}

	moduleRoots := interp.Graph.SourcesOfType(interp.Modules, ir.EdgeAst)
	for _, root := range moduleRoots {
		if err := interp.LoadAxioms(ctx, root); err != nil {
			t.Fatalf("LoadAxioms root %d: %v", root, err)
		}
	}
	allRoots := append(append([]ir.NodeID{}, moduleRoots...), lhsRoot, rhsRoot)
	for _, root := range allRoots {
		if err := interp.LoadEqualities(ctx, root); err != nil {
			t.Fatalf("LoadEqualities root %d: %v", root, err)
		}
	}
	for _, root := range allRoots {
		if err := interp.BridgeRequireCalls(ctx, root); err != nil {
			t.Fatalf("BridgeRequireCalls root %d: %v", root, err)
		}
	}
	for _, root := range allRoots {
		if err := interp.BridgeBareRequiredModuleExports(ctx, root); err != nil {
			t.Fatalf("BridgeBareRequiredModuleExports root %d: %v", root, err)
		}
	}

	lhsQuery := findQueryNode(interp, lhsRoot)
	rhsQuery := findQueryNode(interp, rhsRoot)
	// A swallowed error here is worse than a flake: the positives would
	// fail loudly, but the negative controls assert REFUSAL, and an
	// e-graph with no loaded expressions refuses every equivalence —
	// they would all pass vacuously. (This module is outside root
	// `make lint`, so errcheck does not cover it.)
	if err := interp.Solver.LoadExpressions(ctx, interp.Graph, []solve.Expr{
		{RootNodeID: lhsQuery},
		{RootNodeID: rhsQuery},
	}); err != nil {
		t.Fatalf("LoadExpressions: %v", err)
	}
	for _, root := range allRoots {
		if err := interp.BridgeModulePropertyAccess(ctx, root); err != nil {
			t.Fatalf("BridgeModulePropertyAccess root %d: %v", root, err)
		}
	}
	for _, root := range allRoots {
		if err := interp.BridgeMethodPropertyAccess(ctx, root); err != nil {
			t.Fatalf("BridgeMethodPropertyAccess root %d: %v", root, err)
		}
	}

	if err := interp.Saturate(ctx); err != nil {
		t.Fatalf("Saturate: %v", err)
	}
	eq, err := interp.Solver.AreEquivalent(lhsQuery, rhsQuery)
	if err != nil {
		t.Fatalf("AreEquivalent: %v", err)
	}
	if eq {
		return true
	}
	// The engine's axiom-test fallback: typed universals (d : Protocol)
	// gate rewrites, and the egraph backend discharges them as a
	// conditional equivalence with the membership premises as the
	// condition. Conditional counts as equivalent — for negative
	// controls too, which makes the controls STRICTER.
	if conditional, ok := interp.Solver.(solve.ConditionalEquivalenceAware); ok {
		condEq, condition, err := conditional.AreEquivalentWithConstraint(lhsQuery, rhsQuery)
		if err != nil {
			t.Fatalf("AreEquivalentWithConstraint: %v", err)
		}
		if condEq {
			if condition != nil {
				t.Logf("conditional equivalence under: %s", condition)
			}
			return true
		}
	}
	return false
}
