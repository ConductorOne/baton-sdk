// L9 — stamp-compression admissibility (formal/occult/LAWS.md; graph
// model find G9-CAL-1). These are arithmetic facts about the floor-2
// bucketing, checked GROUND-EXHAUSTIVELY over generations 0..7 — the
// same small-scope envelope the P models use (all known behavior fits
// it; the bound is part of the claim). Each instance is evaluated by
// the engine (native arithmetic + classify), not by Go: Go only
// enumerates the finite domain and reads the engine's verdict string.
//
// The negative control is the FALSE-LIVE claim "floor2(s) >= floor2(cur)
// implies s >= cur" — the parity ambiguity that G9-CAL-1 found
// dynamically (P PASS-BUDGET livelock). The engine must exhibit at
// least one violating pair, and exactly the predicted ones: s = cur-1
// with cur odd.
package host_test

import (
	"context"
	"fmt"
	"testing"

	occult "github.com/conductorone/occult"
	"github.com/conductorone/occult/state"
)

const genBound = 8 // generations 0..7: covers first-admission (odd) and bucket-aligned (even) mints

// evalVerdict evaluates one engine expression that must reduce to the
// string "ok" or "violated". CLI-parity interpreter: the arithmetic and
// boolean stdlibs must be loaded for %, <, ==, classify to reduce.
func evalVerdict(t *testing.T, source string) string {
	t.Helper()
	interp, pm, err := occult.NewCLIInterpreter("egraph", false, "", "")
	if err != nil {
		t.Fatalf("NewCLIInterpreter: %v", err)
	}
	res, err := interp.Eval(context.Background(), "compression-check", source, pm)
	if err != nil {
		t.Fatalf("Eval %q: %v", source, err)
	}
	if res == nil || res.Loc == nil {
		t.Fatalf("Eval %q: no result location", source)
	}
	st, ok := interp.State.Resolve(*res.Loc)
	if !ok || st.Literal == nil || st.Literal.Kind != state.LitString {
		t.Fatalf("Eval %q: result is not a string literal", source)
	}
	return st.Literal.Str
}

// implSource renders: premise(s, cur) -> conclusion(s, cur), engine-side,
// as nested classify with string verdicts.
func implSource(premise, conclusion string, s, cur int) string {
	p := fmt.Sprintf(premise, s, cur)
	c := fmt.Sprintf(conclusion, s, cur)
	return fmt.Sprintf(
		`classify (%s) { {true,} : classify (%s) { {true,} : "ok", Bool : "violated" }, Bool : "ok" }`,
		p, c)
}

const floor2s = `(%[1]d - %[1]d %% 2)`
const floor2c = `(%[2]d - %[2]d %% 2)`

func TestL9aDefiniteStaleSound(t *testing.T) {
	// floor2(s) < floor2(cur)  ->  s < cur
	for s := 0; s < genBound; s++ {
		for cur := 0; cur < genBound; cur++ {
			src := implSource(floor2s+` < `+floor2c, `%[1]d < %[2]d`, s, cur)
			if v := evalVerdict(t, src); v != "ok" {
				t.Errorf("L9a violated at s=%d cur=%d", s, cur)
			}
		}
	}
}

func TestL9bBucketAlignedLivenessProvable(t *testing.T) {
	// cur even AND s <= cur AND floor2(s) >= floor2(cur)  ->  s = cur
	for s := 0; s < genBound; s++ {
		for cur := 0; cur < genBound; cur += 2 {
			full := fmt.Sprintf(
				`classify (%[1]d <= %[2]d) { {true,} : classify ((%[1]d - %[1]d %% 2) >= (%[2]d - %[2]d %% 2)) { {true,} : classify (%[1]d == %[2]d) { {true,} : "ok", Bool : "violated" }, Bool : "ok" }, Bool : "ok" }`,
				s, cur)
			if v := evalVerdict(t, full); v != "ok" {
				t.Errorf("L9b violated at s=%d cur=%d (cur even)", s, cur)
			}
		}
	}
}

func TestL9NegativeControlFalseLive(t *testing.T) {
	// WRONG law: floor2(s) >= floor2(cur) -> s >= cur. Must be violated,
	// and exactly at the G9-CAL-1 ambiguity: s = cur-1 with cur odd.
	violations := map[[2]int]bool{}
	for s := 0; s < genBound; s++ {
		for cur := 0; cur < genBound; cur++ {
			src := implSource(floor2s+` >= `+floor2c, `%[1]d >= %[2]d`, s, cur)
			if v := evalVerdict(t, src); v == "violated" {
				violations[[2]int{s, cur}] = true
			}
		}
	}
	if len(violations) == 0 {
		t.Fatalf("false-live control: no violating pair found — the ground check cannot refute a wrong law")
	}
	for pair := range violations {
		s, cur := pair[0], pair[1]
		if !(cur%2 == 1 && s == cur-1) {
			t.Errorf("false-live control: unexpected violation shape at s=%d cur=%d (predicted only s=cur-1, cur odd)", s, cur)
		}
	}
	for cur := 1; cur < genBound; cur += 2 {
		if !violations[[2]int{cur - 1, cur}] {
			t.Errorf("false-live control: predicted ambiguity pair s=%d cur=%d not exhibited", cur-1, cur)
		}
	}
}
