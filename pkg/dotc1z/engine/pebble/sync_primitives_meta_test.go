package pebble

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Same fence as pkg/sync's TestSyncPrimitivesRegistered, same walker: a
// synchronization primitive is a dependency on a concurrency model, so each
// one names the interleaving it serves or is marked for removal. Entries
// reading "predates the registry" were present before it existed and were not
// audited when it was added; a change to one of them is the moment to replace
// that text with the interleaving. The test checks registration and liveness,
// not the truth of an entry's sentence.
var enginePrimitiveRegistry = map[string]string{ //nolint:gosec // Field names, not credential values; "retainTokens" trips G101.
	"Engine.lifecycleMu":                            "predates the registry; see the field comment",
	"Engine.writeMu":                                "predates the registry; see the field comment",
	"Engine.binding":                                "predates the registry; see the field comment",
	"Engine.computedStatsMu":                        "predates the registry",
	"Engine.deferredGrantStatsMu":                   "predates the registry",
	"Engine.grantDigestBuildPending":                "predates the registry",
	"Engine.grantDigestAbiStale":                    "predates the registry",
	"Engine.entIDLookupGen":                         "predates the registry",
	"Engine.entIDLookupMu":                          "predates the registry",
	"Engine.expandedWriteCalls":                     "predates the registry",
	"Engine.expandedWriteRows":                      "predates the registry",
	"Engine.synthesizedWriteCalls":                  "predates the registry",
	"Engine.synthesizedWriteRows":                   "predates the registry",
	"Ledger.retainTokens":                           "predates the registry",
	"Ledger.mismatches":                             "predates the registry",
	"Ledger.inFlight":                               "predates the registry; see the field comment",
	"pausableCompactionScheduler.paused":            "predates the registry",
	"pausableCompactionScheduler.registered":        "predates the registry",
	"pausableCompactionScheduler.mu.Mutex":          "predates the registry; embedded in the mu struct, see the field comment",
	"pausableCompactionScheduler.mu.isGrantingCond": "predates the registry; see the field comment",
	"BulkSyncImport.mu":                             "predates the registry",
	"BulkSyncImport.entitlementsSkippedMissingRefs": "predates the registry",
	"BulkSyncImport.grantsSkippedMissingRefs":       "predates the registry",
	"spillSorter.chunkMu":                           "predates the registry",
	"grantRebuildTee.mu":                            "predates the registry",
	"synthGrantLayerSession.segMu":                  "predates the registry",
	"Engine.UnsafePutUniqueGrantRecords.errMu":      "predates the registry; function-local, joins parallel encoders",
	"Engine.UnsafePutUniqueGrantRecords.failed":     "predates the registry; function-local, joins parallel encoders",
	"Open.poisonLogMu":                              "predates the registry; function-local",
	"testSeams.ledgerResiduePurges":                 "test hook counter; production never reads it",

	"testSeams.sealCost": "test observation: finalize stores the last seal's timings; only tests read it (LastSealCost)",
}

func TestEnginePrimitivesRegistered(t *testing.T) {
	fset, files := parseProductionDir(t, ".")
	found := map[string]string{}
	for _, file := range files {
		for key, pos := range enginePrimitiveDeclarations(fset, file) {
			found[key] = pos
		}
	}
	require.NotEmpty(t, found, "no primitives found, so this test is not reading the package")

	var missing, stale []string
	for key, pos := range found {
		if _, ok := enginePrimitiveRegistry[key]; !ok {
			missing = append(missing, fmt.Sprintf("%s (%s)", key, pos))
		}
	}
	for key := range enginePrimitiveRegistry {
		if _, ok := found[key]; !ok {
			stale = append(stale, key)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	require.Emptyf(t, missing,
		"synchronization primitives without a registry entry; add each to enginePrimitiveRegistry naming the two goroutines that interleave on it, or \"remove: <reason>\":\n%s",
		strings.Join(missing, "\n"))
	require.Emptyf(t, stale,
		"enginePrimitiveRegistry entries with no declaration behind them; remove the stale entries:\n%s",
		strings.Join(stale, "\n"))
}

func TestEnginePrimitiveWalkerCoverage(t *testing.T) {
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
	found := enginePrimitiveDeclarations(fset, file)
	keys := make([]string, 0, len(found))
	for key := range found {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	require.Equal(t, []string{
		"f.anon.m", "f.localMu", "f.localType.tm", "f.viaAddr", "f.viaLit", "f.viaNew",
		"holder.cond", "holder.inner.nested", "holder.m.counter", "holder.mu", "holder.ptr",
		"package.pkgMu", "package.pkgOnce", "package.pkgPtr",
	}, keys)
}

func enginePrimitiveDeclarations(fset *token.FileSet, file *ast.File) map[string]string {
	aliases := enginePrimitiveImportAliases(file)
	found := map[string]string{}
	isPrim := func(expr ast.Expr) bool { return isEnginePrimitiveType(aliases, expr) }
	record := func(key string, pos token.Pos) { found[key] = fset.Position(pos).String() }

	var walkStruct func(owner string, st *ast.StructType)
	walkStruct = func(owner string, st *ast.StructType) {
		for _, field := range st.Fields.List {
			if len(field.Names) == 0 {
				if isPrim(field.Type) {
					record(owner+"."+engineTypeName(field.Type), field.Pos())
				}
				continue
			}
			for _, name := range field.Names {
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
			owner := d.Name.Name
			if d.Recv != nil && len(d.Recv.List) == 1 {
				owner = engineTypeName(d.Recv.List[0].Type) + "." + owner
			}
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

func enginePrimitiveImportAliases(file *ast.File) map[string]string {
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

var enginePrimitiveTypeNames = map[string]map[string]bool{
	"sync":   {"Mutex": true, "RWMutex": true, "Cond": true, "Once": true, "Map": true},
	"atomic": {"Bool": true, "Int32": true, "Int64": true, "Uint32": true, "Uint64": true, "Uintptr": true, "Pointer": true, "Value": true},
}

func isEnginePrimitiveType(aliases map[string]string, expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return isEnginePrimitiveType(aliases, e.X)
	case *ast.ParenExpr:
		return isEnginePrimitiveType(aliases, e.X)
	case *ast.IndexExpr:
		return isEnginePrimitiveType(aliases, e.X)
	case *ast.SelectorExpr:
		pkg, ok := e.X.(*ast.Ident)
		if !ok {
			return false
		}
		return enginePrimitiveTypeNames[aliases[pkg.Name]][e.Sel.Name]
	}
	return false
}

func engineTypeName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return engineTypeName(e.X)
	case *ast.IndexExpr:
		return engineTypeName(e.X)
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	}
	return fmt.Sprintf("%T", expr)
}
