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
// record and the binding, and none of them is callable from a write body
// (each takes the barrier itself, so the re-entrancy check would fire
// first). A sixth taker is not forbidden, but it does have to be a
// decision: add it here, and say why it cannot be reached from a write.
func TestLifecycleMuTakersAreTransitionsOnly(t *testing.T) {
	want := map[string]bool{
		"startNewSync":   true,
		"ResumeSync":     true,
		"SetCurrentSync": true,
		"CheckpointSync": true,
		"EndSync":        true,
	}

	got := map[string]bool{}
	_, files := parseProductionDir(t, ".")
	for _, f := range files {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "lifecycleMu" {
					got[fn.Name.Name] = true
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
}

// TestWriteBarrierWaitersCheckOwnership pins the coverage the ownership
// check depends on.
//
// The check reports a goroutine waiting on itself, and it can only do
// that on paths it sees: lockWriteBarrier records the owner, while Close
// and CheckpointTo drain writeWG one step earlier and so have to ask
// explicitly. A fourth method that locks writeMu directly, or a second
// writeWG waiter that skips the assertion, is back to hanging with no
// output — and it would hang in exactly the situation where nobody is
// looking for a lock bug, because the call reads like ordinary work.
func TestWriteBarrierWaitersCheckOwnership(t *testing.T) {
	const assertion = "assertNotWaitingOnOwnWrite"
	waiters := []string{"Close", "CheckpointTo"}

	locksBarrier := map[string]bool{}
	waitsForWrites := map[string]bool{}
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
				}
				return true
			})
		}
	}

	require.Equal(t, map[string]bool{"lockWriteBarrier": true, "Close": true, "CheckpointTo": true}, locksBarrier,
		"someone locks writeMu outside lockWriteBarrier. Ownership is recorded there, so a direct lock is "+
			"invisible to the re-entrancy check: go through lockWriteBarrier, or call "+assertion+" first and say why.")
	require.Equal(t, map[string]bool{"Close": true, "CheckpointTo": true}, waitsForWrites,
		"a new writeWG waiter appeared. Waiting for in-flight writes from inside a write body waits forever, "+
			"so a waiter has to call "+assertion+" before the wait.")
	for _, waiter := range waiters {
		require.True(t, checksOwnership[waiter], "%s waits for in-flight writes without calling %s first", waiter, assertion)
	}
}
