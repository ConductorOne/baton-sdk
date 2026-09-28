package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A synchronization primitive is a dependency on a concurrency model: it
// exists because two named goroutines touch one piece of state. This registry
// makes that model reviewable the way commitPointRegistry makes commit sites
// reviewable. A primitive missing from it fails the build until its owner
// writes the interleaving down; an entry with no field behind it is stale and
// fails too. "remove:" entries record primitives the review found guarding
// nothing shared, so the cleanup that deletes them also deletes the entry.
//
// Covered: sync.Mutex, sync.RWMutex, sync.Cond, sync.Once, sync.Map and every
// atomic.* type, as struct fields (named types and structs declared inside
// function bodies) and as function-local variables. sync.WaitGroup is not
// covered: it sequences goroutine lifetimes and guards no state.
var syncPrimitiveRegistry = map[string]string{
	"runState.mu":                                "action stack and facts: workers transition and finish actions and set facts while the main loop reads current()",
	"runStats.mu":                                "live stats: workers merge connector and session stats while the main loop reads summaries",
	"parallelActionQueue.mu":                     "work queue: N workers dequeue, transition and complete concurrently",
	"syncer.syncParallel.resultsMu":              "function-local: workers append warnings and errors that syncParallel reads after wg.Wait",
	"parallelActionQueue.cond":                   "wakes workers blocked in next() when a transition admits children or the batch drains; guarded by parallelActionQueue.mu",
	"syncer.parallelTransitionMu":                "transitioner pointer set by syncParallel on the main goroutine and read by every worker's nextPageOrFinishAction",
	"syncer.rlWallMu":                            "rate-limit wall watermark advanced by wait observers on worker goroutines",
	"syncer.listResourceActionsCompletedThisRun": "workers finish list-resource actions; the main loop reads the count for the warning ratio",
	"syncMap.m":                                  "type-scoped marker cache shared by workers resolving the same resource type",
	"queueAudit.mu":                              "test recorder, nil in production (syncTestHooks.queueAudit); workers record concurrently under test",

	"ingestFilterStats.entitlementsDropped":          "workers' afterCommit callbacks add drops; the seal reads totals",
	"ingestFilterStats.grantsDropped":                "workers' afterCommit callbacks add drops; the seal reads totals",
	"ingestFilterStats.grantResourcesDropped":        "workers' afterCommit callbacks add drops; the seal reads totals",
	"ingestFilterStats.expansionTypesDropped":        "workers' afterCommit callbacks add drops; the seal reads totals",
	"ingestFilterStats.expansionsDropped":            "workers' afterCommit callbacks add drops; the seal reads totals",
	"ingestFilterStats.invalidResourceTypesObserved": "workers' afterCommit callbacks add drops; the seal reads totals",
	"ingestFilterStats.invalidResourcesObserved":     "workers' afterCommit callbacks add drops; the seal reads totals",
	"ingestFilterStats.invalidEntitlementsObserved":  "workers' afterCommit callbacks add drops; the seal reads totals",
	"ingestFilterStats.replayBlocked":                "set by workers' afterCommit callbacks; read at seal and by the ledger's terminal facts",
	"ingestFilterStats.reasonFlags":                  "OR'd by workers' afterCommit callbacks; read at seal",
	"ingestFilterStats.known":                        "set by workers' afterCommit callbacks and at restore; read at seal",

	// Found by the CXE-1358 review to guard nothing two goroutines share.
	// docs/verification/syncer-on-ledger/implementation.md, CO-038.
	"ledgerAttempts.mu":       "remove: one owner, the goroutine whose context carries it; ratelimit.ObserveWait runs on the sleeping goroutine",
	"ledgerRuntime.prepareMu": "remove: one-time preparation belongs to the coordinator before parallelSync, not to a race among first pages",
	"ledgerRuntime.mu":        "remove: facts duplicate runState.facts; workers and active are per-worker slots; closing is written and read on the main goroutine",
	"ledgerRuntime.commitMu":  "remove: every publish runs under parallelActionQueue.mu or on one goroutine, and in-memory fact values have no reader",
	"ledgerRunAccounting.mu":  "remove: records the same observations as runStats under a second lock; becomes an attempt-scoped view inside runStats",
}

func TestSyncPrimitivesRegistered(t *testing.T) {
	found := productionPrimitives(t)
	require.NotEmpty(t, found, "no primitives found, so this test is not reading the package")

	var missing, stale []string
	for key, pos := range found {
		if _, ok := syncPrimitiveRegistry[key]; !ok {
			missing = append(missing, fmt.Sprintf("%s (%s)", key, pos))
		}
	}
	for key := range syncPrimitiveRegistry {
		if _, ok := found[key]; !ok {
			stale = append(stale, key)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	require.Emptyf(t, missing,
		"synchronization primitives without a registry entry; add each to syncPrimitiveRegistry naming the two goroutines that interleave on it, or \"remove: <reason>\":\n%s",
		strings.Join(missing, "\n"))
	require.Emptyf(t, stale,
		"syncPrimitiveRegistry entries with no field behind them; remove the stale entries:\n%s",
		strings.Join(stale, "\n"))
}

// productionPrimitives maps "Type.field" (or "func.field" for a struct
// declared inside a function) to its source position, for every field whose
// type is a covered primitive, across the package's non-test files.
func productionPrimitives(t *testing.T) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	fset := token.NewFileSet()
	found := map[string]string{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoErrorf(t, err, "parsing %s", name)
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					collectPrimitiveFields(fset, typeSpec.Name.Name, typeSpec.Type, found)
				}
			case *ast.FuncDecl:
				owner := declKey(d)
				ast.Inspect(d.Body, func(n ast.Node) bool {
					switch node := n.(type) {
					case *ast.StructType:
						collectPrimitiveFields(fset, owner, node, found)
					case *ast.ValueSpec:
						if node.Type != nil && isPrimitiveType(node.Type) {
							for _, name := range node.Names {
								found[owner+"."+name.Name] = fset.Position(name.Pos()).String()
							}
						}
					}
					return true
				})
			}
		}
	}
	return found
}

func collectPrimitiveFields(fset *token.FileSet, owner string, typ ast.Expr, found map[string]string) {
	st, ok := typ.(*ast.StructType)
	if !ok {
		return
	}
	for _, field := range st.Fields.List {
		if !isPrimitiveType(field.Type) {
			continue
		}
		names := field.Names
		if len(names) == 0 {
			found[owner+"."+baseTypeName(field.Type)] = fset.Position(field.Pos()).String()
			continue
		}
		for _, name := range names {
			found[owner+"."+name.Name] = fset.Position(field.Pos()).String()
		}
	}
}

var primitiveSelectors = map[string]map[string]bool{
	"sync":        {"Mutex": true, "RWMutex": true, "Cond": true, "Once": true, "Map": true},
	"native_sync": {"Mutex": true, "RWMutex": true, "Cond": true, "Once": true, "Map": true},
	"atomic":      {"Bool": true, "Int32": true, "Int64": true, "Uint32": true, "Uint64": true, "Uintptr": true, "Pointer": true, "Value": true},
}

func isPrimitiveType(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return isPrimitiveType(e.X)
	case *ast.IndexExpr:
		return isPrimitiveType(e.X)
	case *ast.SelectorExpr:
		pkg, ok := e.X.(*ast.Ident)
		if !ok {
			return false
		}
		return primitiveSelectors[pkg.Name][e.Sel.Name]
	}
	return false
}
