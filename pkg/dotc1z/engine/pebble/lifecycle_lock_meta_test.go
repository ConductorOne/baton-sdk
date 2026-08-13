package pebble

import (
	"go/ast"
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
// record and the binding, and none of them can be called from a write
// body — but not all for the same reason. startNewSync, CheckpointSync
// and EndSync take the barrier themselves, so lockWriteBarrier's
// re-entrancy check fires first. ResumeSync and SetCurrentSync never
// touch writeMu (a record read and a rebind, whose locks are
// currentSyncMu and sealMu), so nothing about them would have hung at
// the barrier: they call assertNotTakingLifecycleFromWrite instead, and
// that call is what makes the claim above true for them.
//
// A sixth taker is not forbidden, but it does have to be a decision: add
// it here, and say which of those two protects it.
func TestLifecycleMuTakersAreTransitionsOnly(t *testing.T) {
	want := map[string]bool{
		"startNewSync":   true,
		"ResumeSync":     true,
		"SetCurrentSync": true,
		"CheckpointSync": true,
		"EndSync":        true,
	}

	// The takers that reach lifecycleMu without ever taking the write
	// barrier, and so need the explicit guard to be unreachable from a
	// write body.
	wantGuarded := map[string]bool{
		"ResumeSync":     true,
		"SetCurrentSync": true,
	}

	got := map[string]bool{}
	guarded := map[string]bool{}
	_, files := parseProductionDir(t, ".")
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
				case "assertNotTakingLifecycleFromWrite":
					guarded[fn.Name.Name] = true
				}
				return true
			})
		}
	}

	require.Equal(t, wantGuarded, guarded,
		"the set of lifecycleMu takers that need the explicit write-body guard changed. A taker that does not "+
			"take the write barrier somewhere in its body gets no re-entrancy check, so it has to call "+
			"assertNotTakingLifecycleFromWrite: add it here, or drop the call and say which write in the body "+
			"the barrier check now covers.")
	require.Equal(t, want, got,
		"the set of lifecycleMu takers changed. Lock order is lifecycleMu then writeMu, so a new taker "+
			"reachable from inside a write body reintroduces the deadlock TestCurrentSyncStepDoesNotDeadlockWithEndSync "+
			"covers. If the new method is a lifecycle transition, add it here; if it is a read, read the binding "+
			"instead (see CurrentSyncStep) rather than locking.")
}

// TestWriteBarrierWaitersCheckOwnership pins the coverage the ownership
// check depends on.
//
// The check reports a goroutine waiting on itself, and it can only do
// that on paths it sees. Three enumerations keep it seeing all of them:
// lockWriteBarrier records the barrier's owner, enterWriteWG records
// membership in writeWG, and Close and CheckpointTo drain that group one
// step before the barrier, so they have to ask explicitly. A method that
// locks writeMu directly, a bare writeWG.Add (compactions and flushes
// join the group without the barrier, and one of those is invisible to
// the check), or a second waiter that skips the assertion is back to
// hanging with no output — and it would hang in exactly the situation
// where nobody is looking for a lock bug, because the call reads like
// ordinary work.
func TestWriteBarrierWaitersCheckOwnership(t *testing.T) {
	const assertion = "assertNotWaitingOnOwnWrite"
	waiters := []string{"Close", "CheckpointTo"}

	locksBarrier := map[string]bool{}
	waitsForWrites := map[string]bool{}
	joinsWriteWG := map[string]bool{}
	checksOwnership := map[string]bool{}
	_, files := parseProductionDir(t, ".")
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
				case "writeMu":
					locksBarrier[fn.Name.Name] = true
				case assertion:
					checksOwnership[fn.Name.Name] = true
				case "Wait":
					if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "writeWG" {
						waitsForWrites[fn.Name.Name] = true
					}
				case "Add", "Done":
					if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "writeWG" {
						joinsWriteWG[fn.Name.Name] = true
					}
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
			"there, so a direct lock is invisible to the re-entrancy check: go through the pair, or call "+
			assertion+" first and say why.")
	require.Equal(t, map[string]bool{"enterWriteWG": true, "exitWriteWG": true}, joinsWriteWG,
		"someone joins or leaves writeWG outside the enterWriteWG/exitWriteWG pair. Membership is recorded "+
			"there, and it is membership — not barrier ownership — that hangs a Close: a bare Add gets counted in "+
			"the group the waiters drain while staying invisible to "+assertion+". Go through the pair.")
	require.Equal(t, map[string]bool{"Close": true, "CheckpointTo": true}, waitsForWrites,
		"a new writeWG waiter appeared. Waiting for in-flight writes from inside a write body waits forever, "+
			"so a waiter has to call "+assertion+" before the wait.")
	for _, waiter := range waiters {
		require.True(t, checksOwnership[waiter], "%s waits for in-flight writes without calling %s first", waiter, assertion)
	}
}
