package pebble

import (
	"go/ast"
	"go/token"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLifecycleMuTakersAreTransitionsOnly pins which methods acquire
// lifecycleMu.
//
// The lock order is lifecycleMu, then writeMu: EndSync holds the
// lifecycle mutex across a finalize whose steps take the write barrier.
// So any method that acquires lifecycleMu must be unreachable from inside
// a write body, or the two orders coexist and the pair deadlocks. That is
// how CurrentSyncStep became a deadlock — a method that reads like a
// getter, called from a write that wanted its own progress, on a lock
// nobody thought about at the call site.
//
// The five below are the sync-lifecycle transitions. They earn the lock
// because their bodies are read-check-write sequences over the sync-run
// record and the binding. EVERY one of them must call
// assertNotTakingLifecycleFromWrite BEFORE taking the lock: the barrier
// re-entrancy check inside the writing transitions fires at the first
// inner write — after lifecycleMu is already held — and a contended
// lifecycleMu parks the caller before that, holding the barrier a
// concurrent EndSync's finalize is waiting on. Only a guard placed
// before the lock turns that hang into a panic, so this test checks the
// position, not just the presence.
//
// A sixth taker is not forbidden, but it does have to be a decision:
// add it here, with the guard in the same shape.
func TestLifecycleMuTakersAreTransitionsOnly(t *testing.T) {
	want := map[string]bool{
		"startNewSync":   true,
		"ResumeSync":     true,
		"SetCurrentSync": true,
		"CheckpointSync": true,
		"EndSync":        true,
	}

	got := map[string]bool{}
	guardPos := map[string]token.Pos{}
	lockPos := map[string]token.Pos{}
	fset, files := parseProductionDir(t, ".")
	for _, f := range files {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "lifecycleMu":
					got[fn.Name.Name] = true
					if _, seen := lockPos[fn.Name.Name]; !seen {
						lockPos[fn.Name.Name] = sel.Pos()
					}
				case "assertNotTakingLifecycleFromWrite":
					if _, seen := guardPos[fn.Name.Name]; !seen {
						guardPos[fn.Name.Name] = sel.Pos()
					}
				}
				return true
			})
		}
	}

	require.Equal(t, want, got,
		"the set of lifecycleMu takers changed. Lock order is lifecycleMu then writeMu, so a new taker "+
			"reachable from inside a write body reintroduces the deadlock TestCurrentSyncStepDoesNotDeadlockWithEndSync "+
			"covers. If the new method is a lifecycle transition, add it here; if it is a read, read the binding "+
			"instead (see CurrentSyncStep) rather than locking.")
	for taker := range want {
		gp, guarded := guardPos[taker]
		require.True(t, guarded,
			"%s takes lifecycleMu without calling assertNotTakingLifecycleFromWrite. Called from a write body it "+
				"parks on the lock holding the barrier — the deadlock — with nothing to turn the hang into a panic.",
			taker)
		require.Less(t, gp, lockPos[taker],
			"%s calls assertNotTakingLifecycleFromWrite at %s, AFTER taking lifecycleMu at %s. Behind the lock the "+
				"guard is too late: a contended lifecycleMu parks the caller first, and the park is the deadlock.",
			taker, fset.Position(gp), fset.Position(lockPos[taker]))
	}
}

// TestWriteBarrierLockedThroughOwnerPairOnly pins where writeMu may be
// locked. Ownership is recorded in lockWriteBarrier/unlockWriteBarrier,
// so a method that locks the mutex directly is invisible to the
// re-entrancy check — it hangs with no output in exactly the situation
// where nobody is looking for a lock bug, because the call reads like
// ordinary work. Close and CheckpointTo take the mutex bare on purpose:
// both run after a drain that already panicked if the caller was inside
// a write, and recording them as owners would make the teardown look
// like a write body.
func TestWriteBarrierLockedThroughOwnerPairOnly(t *testing.T) {
	locksBarrier := map[string]bool{}
	_, files := parseProductionDir(t, ".")
	for _, f := range files {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "writeMu" {
					locksBarrier[fn.Name.Name] = true
				}
				return true
			})
		}
	}

	require.Equal(t, map[string]bool{
		"lockWriteBarrier":   true,
		"unlockWriteBarrier": true,
		"Close":              true,
		"CheckpointTo":       true,
	}, locksBarrier,
		"someone locks writeMu outside the lockWriteBarrier/unlockWriteBarrier pair. Ownership is recorded "+
			"there, so a direct lock is invisible to the re-entrancy check: go through the pair, or explain "+
			"why the new use cannot be inside a write body, the way Close and CheckpointTo do.")
}

// TestAdmissionUsedOnlyThroughItsMethods keeps the close gate's
// invariants inside the admission type, where admission_test.go tests
// them directly.
//
// Those invariants — entering is atomic against Close's flip, the
// drains panic instead of waiting on their own caller, the teardown
// runs exactly once — hold for users of the five entry methods and for
// nobody else. Engine code that reaches past them (a bare
// admit.writers.Add, a read of admit.closing, its own drain) is taking
// on the interleaving bugs the type exists to contain, and it would do
// so silently: same package, so the compiler has no opinion. The two
// drains are additionally pinned to their single callers, because each
// encodes a lifecycle decision (shutting the gate; quiescing writes for
// a checkpoint cut) that a second call site should have to argue for
// here.
func TestAdmissionUsedOnlyThroughItsMethods(t *testing.T) {
	allowedAnywhere := map[string]bool{
		"enterWrite": true, "exitWrite": true,
		"enterRead": true, "exitRead": true,
		"isClosing": true,
	}
	singleCaller := map[string]string{
		"closeAndDrain": "Close",
		"drainWrites":   "CheckpointTo",
	}

	callers := map[string]map[string]bool{}
	fset, files := parseProductionDir(t, ".")
	for path, f := range files {
		if filepath.Base(path) == "admission.go" {
			continue
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				inner, ok := sel.X.(*ast.SelectorExpr)
				if !ok || inner.Sel.Name != "admit" {
					return true
				}
				name := sel.Sel.Name
				if _, pinned := singleCaller[name]; pinned {
					if callers[name] == nil {
						callers[name] = map[string]bool{}
					}
					callers[name][fn.Name.Name] = true
					return true
				}
				require.True(t, allowedAnywhere[name],
					"%s: %s reaches into the admission gate's internals (admit.%s). The gate's invariants are "+
						"only tested for its methods — go through enterWrite/exitWrite, enterRead/exitRead or "+
						"isClosing, or move the new mechanism into admission.go with a test.",
					fset.Position(sel.Pos()), fn.Name.Name, name)
				return true
			})
		}
	}

	for method, caller := range singleCaller {
		require.Equal(t, map[string]bool{caller: true}, callers[method],
			"admit.%s is pinned to %s. A second caller is a second place the engine decides to drain "+
				"in-flight work, which is a lifecycle decision: make it here, in this enumeration, on purpose.",
			method, caller)
	}
}
