// The phantom union derived deductively (../src/sync_phantom.occult):
// the engine PROVES the known-broken composition manufactures a
// phantom row (a row deleted upstream between e0 and e1 survives in
// the sealed artifact), proves every ingredient response is
// individually truthful, and proves the premise-validated composition
// (the demand-graph runtime's grounding rule) yields exactly the
// upstream truth from the same ingredients. The negative controls are
// the finding itself: the broken artifact is provably NOT the epoch it
// claims to be.
package host_test

import "testing"

var phantomModules = []localModule{
	{"sync_phantom", "sync_phantom.occult"},
}

const phantomPrelude = `exists S; S = require("sync_phantom"); `

func phantomEquivalent(t *testing.T, lhs, rhs string) bool {
	t.Helper()
	return equivalent(t, nil, phantomModules, phantomPrelude+lhs, phantomPrelude+rhs)
}

var phantomCases = []lawCase{
	// THE BUG, derived: the broken composition retains the deleted row.
	{"phantom_row_survives", `S.get(S.id1, S.result_broken)`, `S.found(S.vx)`},
	// Ground truth at the claimed epoch: the row is absent.
	{"truth_row_absent", `S.get(S.id1, S.rows_e2)`, `S.absent`},
	// Every ingredient is individually truthful — diff12 on ITS base
	// (e1) produces exactly rows_e2's observations.
	{"diff12_truthful_id1", `S.get(S.id1, S.diff12(S.rows_e1))`, `S.get(S.id1, S.rows_e2)`},
	{"diff12_truthful_id2", `S.get(S.id2, S.diff12(S.rows_e1))`, `S.get(S.id2, S.rows_e2)`},
	// The insidious part: on the OTHER observer the broken artifact
	// looks perfect — only the phantom key betrays it.
	{"broken_id2_looks_fine", `S.get(S.id2, S.result_broken)`, `S.found(S.v2)`},
	// The good composition (diff grounded at the replay's attested
	// base) matches upstream truth on BOTH observers from the same
	// stale-cache ingredients.
	{"good_matches_truth_id1", `S.get(S.id1, S.result_good)`, `S.get(S.id1, S.rows_e2)`},
	{"good_matches_truth_id2", `S.get(S.id2, S.result_good)`, `S.get(S.id2, S.rows_e2)`},
}

var phantomControls = []lawCase{
	// The finding: the broken artifact provably differs from the epoch
	// it claims to be (observed at the phantom key).
	{"broken_is_not_e2", `S.get(S.id1, S.result_broken)`, `S.get(S.id1, S.rows_e2)`},
	// And the phantom is not somehow absent.
	{"phantom_not_absent", `S.get(S.id1, S.result_broken)`, `S.absent`},
	// Broken and good compositions are observably different artifacts.
	{"broken_differs_from_good", `S.get(S.id1, S.result_broken)`, `S.get(S.id1, S.result_good)`},
}

func TestPhantomUnionDerived(t *testing.T) {
	for _, c := range phantomCases {
		t.Run(c.name, func(t *testing.T) {
			if !phantomEquivalent(t, c.lhs, c.rhs) {
				t.Errorf("phantom case %s: expected %q equivalent to %q", c.name, c.lhs, c.rhs)
			}
		})
	}
}

func TestPhantomUnionControls(t *testing.T) {
	for _, c := range phantomControls {
		t.Run(c.name, func(t *testing.T) {
			if phantomEquivalent(t, c.lhs, c.rhs) {
				t.Errorf("phantom control %s: %q must NOT be equivalent to %q", c.name, c.lhs, c.rhs)
			}
		})
	}
}
