package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A synchronization primitive is a dependency on a concurrency model: it
// exists because two named goroutines touch one piece of state. This registry
// makes that model reviewable the way commitPointRegistry makes commit sites
// reviewable. A primitive missing from it fails the build until its owner
// writes the interleaving down; an entry with no declaration behind it is
// stale and fails too. "remove:" entries record primitives the review found
// guarding nothing shared, so the cleanup that deletes them deletes the entry.
//
// What the test checks: that every declaration is registered and every entry
// is live. It does not check that an entry's sentence is true; that is the
// reviewer's read of the ownership table.
//
// Declarations covered, in production files, through whatever local name the
// file imports "sync" and "sync/atomic" under: struct fields (package-level
// and function-local named types, nested structs, anonymous struct
// variables); package-level and
// function-local `var`s with a primitive type or a primitive value; and `:=`
// assignments whose value is `new(T)`, `&T{}`, or `T{}` for a primitive T.
// Types: Mutex, RWMutex, Cond, Once, Map, and every atomic.* type.
// sync.WaitGroup is not covered: it sequences goroutine lifetimes and guards
// no state. TestSyncPrimitiveWalkerCoverage plants each shape and requires
// the walker to report it.
var syncPrimitiveRegistry = map[string]string{
	"runState.mu":                                "action stack and facts: workers transition and finish actions and set facts while the main loop reads current()",
	"runStats.mu":                                "live stats: workers merge connector and session stats while the main loop reads summaries",
	"parallelActionQueue.mu":                     "work queue: N workers dequeue, transition and complete concurrently",
	"parallelActionQueue.cond":                   "wakes workers blocked in next() when a transition admits children or the batch drains; guarded by parallelActionQueue.mu",
	"syncer.syncParallel.resultsMu":              "function-local: workers append warnings and errors that syncParallel reads after wg.Wait",
	"syncer.parallelTransitionMu":                "transitioner pointer set by syncParallel on the main goroutine and read by every worker's nextPageOrFinishAction",
	"syncer.rlWallMu":                            "rate-limit wall watermark advanced by wait observers on worker goroutines",
	"syncer.listResourceActionsCompletedThisRun": "workers finish list-resource actions; the main loop reads the count for the warning ratio",
	"syncMap.m":                                  "type-scoped marker cache shared by workers resolving the same resource type",
	"childScheduleSet.mu":                        "token path: workers record child scheduling while the invariant pass reads it",
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
	"ledgerRuntime.mu":        "remove: facts duplicate runState.facts; workers and active are per-index slots exclusive to their holder; closing is main-goroutine-only",
	"ledgerRuntime.commitMu":  "remove: every publish runs under parallelActionQueue.mu or on one goroutine, and in-memory fact values have no reader",
	"ledgerRunAccounting.mu":  "remove: records the same observations as runStats under a second lock; becomes an attempt-scoped view inside runStats",
}

func TestSyncPrimitivesRegistered(t *testing.T) {
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
		for key, pos := range primitiveDeclarations(fset, file) {
			found[key] = pos
		}
	}
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
		"syncPrimitiveRegistry entries with no declaration behind them; remove the stale entries:\n%s",
		strings.Join(stale, "\n"))
}

// Plants every covered declaration shape, under aliased imports, and requires
// the walker to report each one. A shape the walker cannot see is a shape the
// registry cannot fence.
func TestSyncPrimitiveWalkerCoverage(t *testing.T) {
	const src = `package p

import (
	stdsync "sync"
	at "sync/atomic"
)

var pkgMu stdsync.Mutex
var pkgOnce = stdsync.Once{}
var pkgPtr = new(at.Int64)

type holder struct {
	mu   stdsync.RWMutex
	cond *stdsync.Cond
	ptr  at.Pointer[holder]
	inner struct {
		nested stdsync.Mutex
	}
}

func f() {
	var localMu stdsync.Mutex
	viaNew := new(stdsync.Mutex)
	viaAddr := &stdsync.Once{}
	viaLit := at.Bool{}
	var anon struct{ m stdsync.Map }
	type localType struct{ tm stdsync.Mutex }
	var wg stdsync.WaitGroup
	_, _, _, _, _, _, _ = localMu, viaNew, viaAddr, viaLit, anon, localType{}, wg
}

func (h *holder) m() {
	counter := at.Uint64{}
	_ = counter
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "planted.go", src, parser.SkipObjectResolution)
	require.NoError(t, err)
	found := primitiveDeclarations(fset, file)
	keys := make([]string, 0, len(found))
	for key := range found {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	require.Equal(t, []string{
		"f.anon.m",
		"f.localMu",
		"f.localType.tm",
		"f.viaAddr",
		"f.viaLit",
		"f.viaNew",
		"holder.cond",
		"holder.inner.nested",
		"holder.m.counter",
		"holder.mu",
		"holder.ptr",
		"package.pkgMu",
		"package.pkgOnce",
		"package.pkgPtr",
	}, keys)
}

// primitiveDeclarations maps a stable key to the source position of every
// covered primitive declaration in one file. Keys: "Type.field" for struct
// fields (nested fields chain names), "package.name" for package-level vars,
// "func.name" (or "Type.method.name") for locals.
func primitiveDeclarations(fset *token.FileSet, file *ast.File) map[string]string {
	aliases := primitiveImportAliases(file)
	found := map[string]string{}
	isPrim := func(expr ast.Expr) bool { return isPrimitiveType(aliases, expr) }
	record := func(key string, pos token.Pos) { found[key] = fset.Position(pos).String() }

	var walkStruct func(owner string, st *ast.StructType)
	walkStruct = func(owner string, st *ast.StructType) {
		for _, field := range st.Fields.List {
			names := field.Names
			if len(names) == 0 {
				if isPrim(field.Type) {
					record(owner+"."+baseTypeName(field.Type), field.Pos())
				}
				continue
			}
			for _, name := range names {
				key := owner + "." + name.Name
				if isPrim(field.Type) {
					record(key, name.Pos())
				}
				if inner, ok := field.Type.(*ast.StructType); ok {
					walkStruct(key, inner)
				}
			}
		}
	}
	valueIsPrimitive := func(expr ast.Expr) bool {
		switch v := expr.(type) {
		case *ast.CallExpr:
			if fn, ok := v.Fun.(*ast.Ident); ok && fn.Name == "new" && len(v.Args) == 1 {
				return isPrim(v.Args[0])
			}
		case *ast.UnaryExpr:
			if lit, ok := v.X.(*ast.CompositeLit); ok && v.Op == token.AND {
				return isPrim(lit.Type)
			}
		case *ast.CompositeLit:
			return isPrim(v.Type)
		}
		return false
	}
	walkValueSpec := func(owner string, spec *ast.ValueSpec) {
		for i, name := range spec.Names {
			if spec.Type != nil && isPrim(spec.Type) {
				record(owner+"."+name.Name, name.Pos())
				continue
			}
			if spec.Type != nil {
				if st, ok := spec.Type.(*ast.StructType); ok {
					walkStruct(owner+"."+name.Name, st)
					continue
				}
			}
			if i < len(spec.Values) && valueIsPrimitive(spec.Values[i]) {
				record(owner+"."+name.Name, name.Pos())
			}
		}
	}

	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					if st, ok := s.Type.(*ast.StructType); ok {
						walkStruct(s.Name.Name, st)
					}
				case *ast.ValueSpec:
					walkValueSpec("package", s)
				}
			}
		case *ast.FuncDecl:
			if d.Body == nil {
				continue
			}
			owner := declKey(d)
			ast.Inspect(d.Body, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.DeclStmt:
					if gen, ok := node.Decl.(*ast.GenDecl); ok {
						for _, spec := range gen.Specs {
							switch s := spec.(type) {
							case *ast.ValueSpec:
								walkValueSpec(owner, s)
							case *ast.TypeSpec:
								if st, ok := s.Type.(*ast.StructType); ok {
									walkStruct(owner+"."+s.Name.Name, st)
								}
							}
						}
					}
					return false
				case *ast.AssignStmt:
					if node.Tok != token.DEFINE {
						return true
					}
					for i, lhs := range node.Lhs {
						id, ok := lhs.(*ast.Ident)
						if !ok || i >= len(node.Rhs) {
							continue
						}
						if valueIsPrimitive(node.Rhs[i]) {
							record(owner+"."+id.Name, id.Pos())
						}
					}
				}
				return true
			})
		}
	}
	return found
}

// primitiveImportAliases maps each local name the file uses for "sync" and
// "sync/atomic" to the package it names.
func primitiveImportAliases(file *ast.File) map[string]string {
	aliases := map[string]string{}
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue
		}
		var def string
		switch path {
		case "sync":
			def = "sync"
		case "sync/atomic":
			def = "atomic"
		default:
			continue
		}
		local := def
		if imp.Name != nil && imp.Name.Name != "_" && imp.Name.Name != "." {
			local = imp.Name.Name
		}
		aliases[local] = def
	}
	return aliases
}

var primitiveTypeNames = map[string]map[string]bool{
	"sync":   {"Mutex": true, "RWMutex": true, "Cond": true, "Once": true, "Map": true},
	"atomic": {"Bool": true, "Int32": true, "Int64": true, "Uint32": true, "Uint64": true, "Uintptr": true, "Pointer": true, "Value": true},
}

func isPrimitiveType(aliases map[string]string, expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return isPrimitiveType(aliases, e.X)
	case *ast.ParenExpr:
		return isPrimitiveType(aliases, e.X)
	case *ast.IndexExpr:
		return isPrimitiveType(aliases, e.X)
	case *ast.SelectorExpr:
		pkg, ok := e.X.(*ast.Ident)
		if !ok {
			return false
		}
		return primitiveTypeNames[aliases[pkg.Name]][e.Sel.Name]
	}
	return false
}
