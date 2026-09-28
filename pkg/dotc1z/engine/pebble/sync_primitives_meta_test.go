package pebble

import (
	"fmt"
	"go/ast"
	"go/token"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Same fence as pkg/sync's TestSyncPrimitivesRegistered: a synchronization
// primitive is a dependency on a concurrency model, so each one names the
// interleaving it serves or is marked for removal. Entries reading "predates
// the registry" were present before it existed and were not audited when it
// was added; a change to one of them is the moment to replace that text with
// the interleaving.
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

	// CXE-1358 review: no production reader; LastSealCost is called only from tests.
	// docs/verification/syncer-on-ledger/implementation.md, CO-038.
	"Engine.sealCost": "remove: test-only observation point in a production struct; move behind testSeams",
}

func TestEnginePrimitivesRegistered(t *testing.T) {
	fset, files := parseProductionDir(t, ".")
	found := map[string]string{}
	for _, file := range files {
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					if typeSpec, ok := spec.(*ast.TypeSpec); ok {
						collectEnginePrimitiveFields(fset, typeSpec.Name.Name, typeSpec.Type, found)
					}
				}
			case *ast.FuncDecl:
				owner := d.Name.Name
				if d.Recv != nil && len(d.Recv.List) == 1 {
					owner = engineTypeName(d.Recv.List[0].Type) + "." + owner
				}
				if d.Body == nil {
					continue
				}
				ast.Inspect(d.Body, func(n ast.Node) bool {
					switch node := n.(type) {
					case *ast.StructType:
						collectEnginePrimitiveFields(fset, owner, node, found)
					case *ast.ValueSpec:
						if node.Type != nil && isEnginePrimitiveType(node.Type) {
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
		"enginePrimitiveRegistry entries with no field behind them; remove the stale entries:\n%s",
		strings.Join(stale, "\n"))
}

func collectEnginePrimitiveFields(fset *token.FileSet, owner string, typ ast.Expr, found map[string]string) {
	st, ok := typ.(*ast.StructType)
	if !ok {
		return
	}
	for _, field := range st.Fields.List {
		if !isEnginePrimitiveType(field.Type) {
			continue
		}
		if len(field.Names) == 0 {
			found[owner+"."+engineTypeName(field.Type)] = fset.Position(field.Pos()).String()
			continue
		}
		for _, name := range field.Names {
			found[owner+"."+name.Name] = fset.Position(field.Pos()).String()
		}
	}
}

var enginePrimitiveSelectors = map[string]map[string]bool{
	"sync":   {"Mutex": true, "RWMutex": true, "Cond": true, "Once": true, "Map": true},
	"atomic": {"Bool": true, "Int32": true, "Int64": true, "Uint32": true, "Uint64": true, "Uintptr": true, "Pointer": true, "Value": true},
}

func isEnginePrimitiveType(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return isEnginePrimitiveType(e.X)
	case *ast.IndexExpr:
		return isEnginePrimitiveType(e.X)
	case *ast.SelectorExpr:
		pkg, ok := e.X.(*ast.Ident)
		if !ok {
			return false
		}
		return enginePrimitiveSelectors[pkg.Name][e.Sel.Name]
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
