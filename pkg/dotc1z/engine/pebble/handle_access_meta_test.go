package pebble

// Meta-tests keyed on the handle FIELD ACCESS itself, not on method
// naming. TestScanReadsArePinned holds the named read families
// (Paginate/Iterate/ForEach) to the pinRead pattern; these two tests
// close the hole it leaves: a read added under any other name used to
// escape enforcement entirely, and that is exactly how the unpinned
// point-read surface (GetGrantRecord and friends) accumulated.
//
// The contract they pin:
//
//   - e.db may be touched only from admitted contexts. Syntactically
//     that means: inside a function literal passed to withWrite /
//     withWriteAllowSealed (the write path's admission), or inside one
//     of the enumerated functions below — each of which is admitted by
//     construction (gate/lifecycle machinery, Open-time code before
//     any reader exists, helpers documented as running only under an
//     admitted write, or the merge surface's documented exclusion).
//     Everything else must pin (pinRead) and use the handle pinRead
//     returns, or take an admitted handle as a parameter.
//
//   - every pinRead's release must be deferred in the same function,
//     so no early return can leak an admission and wedge Close.

import (
	"fmt"
	"go/ast"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// admittedDBAccessors enumerates every function permitted to touch e.db
// outside a withWrite/withWriteAllowSealed literal, and why. Additions
// here need the same justification as an entry in a lock-ordering
// inventory: say which admission covers the access.
var admittedDBAccessors = map[string]string{
	// Gate and lifecycle machinery: these ARE the admission.
	"pinRead":      "returns the handle under a read admission it just took",
	"Close":        "teardown closure runs inside closeAndDrain, after both drains",
	"CheckpointTo": "flush/checkpoint runs under the write barrier with writes drained",

	// Direct gate participants: they call admit.enterWrite themselves
	// (pebble compaction/flush are concurrency-safe with foreground
	// writes, so they skip writeMu — see their doc comments).
	"CompactAllRanges": "holds enterWrite for the duration",
	"Flush":            "holds enterWrite for the duration",

	// Open-time, before the engine is shared: no reader or Close can
	// exist yet.
	"Open":                               "constructor, pre-share",
	"verifyOrStampKeyspaceVersion":       "Open-time, pre-share",
	"stampKeyspaceVersion":               "Open-time, pre-share",
	"isKeyspaceEmpty":                    "Open-time, pre-share",
	"isDataKeyspaceEmpty":                "Open-time, pre-share",
	"readIDIndexFormat":                  "Open-time, pre-share",
	"writeIDIndexFormat":                 "Open-time migration, pre-share",
	"readAppliedIndexVersion":            "Open-time migration, pre-share",
	"writeAppliedIndexVersion":           "Open-time migration, pre-share",
	"migrateIDIndexFormatToStructuredV1": "Open-time migration, pre-share",
	"emitStructuredEntitlementMigration": "Open-time migration, pre-share",
	"emitStructuredGrantMigration":       "Open-time migration, pre-share",
	"replaceRangeWithSST":                "Open-time migration, pre-share",

	// Helpers called only from admitted writes (their callers hold
	// write admission; several are named *Locked for exactly this).
	"hasSyncRun":                             "called only from startNewSync, an admitted write",
	"endSyncFinalize":                        "called only from EndSync, an admitted write",
	"deleteGrantByIdentityLocked":            "caller holds withWrite",
	"ingestSynthLayerSegment":                "called only from the synth-layer flush inside withWrite",
	"buildDeferredGrantIndexesLocked":        "caller holds withWriteAllowSealed",
	"markGrantDigestBuildPending":            "called only under the digest build's write admission",
	"clearGrantDigestBuildPending":           "called only under write admission (build, cleanup, drop)",
	"newGrantDigestFold":                     "constructed only under the digest build's write admission",
	"closePartition":                         "grantDigestFold internals; build holds write admission",
	"buildGrantDigestsFromSpill":             "callers hold withWriteAllowSealed",
	"buildGrantDigestsStandaloneLocked":      "caller holds withWriteAllowSealed",
	"writeMissingEntitlementDigestRoots":     "called only from the digest build, admitted",
	"dropAllGrantDigestStateLocked":          "callers hold withWriteAllowSealed (or Open, pre-share)",
	"findMissingGrantDigestPartitionsLocked": "repair holds write admission",
	"repairOneGrantDigestPartitionLocked":    "repair holds write admission",
	"recomputeGrantDigestGlobalRootLocked":   "repair holds write admission",
	"grantDigestRootPresent":                 "called only from the locked repair scan",
	"foldPartitionNodes":                     "called only under digest build/repair write admission",
}

// admittedDBAccessorFiles are files whose every function is admitted
// wholesale, with the same justification discipline.
var admittedDBAccessorFiles = map[string]string{
	"merge_surface.go":  "documented admission-gate exclusion; see file header",
	"merge_accessor.go": "same compactor ordering fence as merge_surface.go",
}

// insideWithWriteLiteral reports whether the node path (outermost
// first) passes through a function literal that is an argument to
// withWrite or withWriteAllowSealed.
func insideWithWriteLiteral(path []ast.Node) bool {
	for i, n := range path {
		if _, ok := n.(*ast.FuncLit); !ok || i == 0 {
			continue
		}
		call, ok := path[i-1].(*ast.CallExpr)
		if !ok {
			continue
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		if sel.Sel.Name == "withWrite" || sel.Sel.Name == "withWriteAllowSealed" {
			return true
		}
	}
	return false
}

// walkWithPath drives visit with the ancestry (outermost first) of
// every node.
func walkWithPath(root ast.Node, visit func(path []ast.Node, n ast.Node)) {
	var path []ast.Node
	ast.Inspect(root, func(n ast.Node) bool {
		if n == nil {
			path = path[:len(path)-1]
			return true
		}
		visit(path, n)
		path = append(path, n)
		return true
	})
}

func enclosingFuncDecl(path []ast.Node) *ast.FuncDecl {
	for i := len(path) - 1; i >= 0; i-- {
		if fd, ok := path[i].(*ast.FuncDecl); ok {
			return fd
		}
	}
	return nil
}

func TestBareHandleAccessIsGateCovered(t *testing.T) {
	fset, files := parseProductionDir(t, ".")

	var violations []string
	for name, f := range files {
		base := name[strings.LastIndexByte(name, '/')+1:]
		if _, ok := admittedDBAccessorFiles[base]; ok {
			continue
		}
		walkWithPath(f, func(path []ast.Node, n ast.Node) {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "db" {
				return
			}
			// Engine handle accesses only: `e.db` off the conventional
			// receiver, or a chain ending in an `e` field (f.e.db,
			// b.e.db). Other types' db fields (the compaction
			// scheduler's pebble.DBForCompaction) are not this gate's
			// concern.
			switch x := sel.X.(type) {
			case *ast.Ident:
				if x.Name != "e" {
					return
				}
			case *ast.SelectorExpr:
				if x.Sel.Name != "e" {
					return
				}
			default:
				return
			}
			if insideWithWriteLiteral(append(append([]ast.Node{}, path...), n)) {
				return
			}
			fd := enclosingFuncDecl(path)
			if fd == nil {
				return
			}
			if _, ok := admittedDBAccessors[fd.Name.Name]; ok {
				return
			}
			violations = append(violations,
				fmt.Sprintf("%s: %s", fset.Position(sel.Pos()), fd.Name.Name))
		})
	}
	sort.Strings(violations)
	require.Empty(t, violations,
		"bare e.db access outside every admitted context. The handle may only be touched under gate admission: "+
			"pin the read (pinRead) and use the handle it returns, plumb an admitted handle parameter, do the write "+
			"inside withWrite/withWriteAllowSealed, or — if the function really is admitted by construction — add it "+
			"to admittedDBAccessors with a justification.\n%s", strings.Join(violations, "\n"))
}

func TestPinnedReadsDeferTheirRelease(t *testing.T) {
	fset, files := parseProductionDir(t, ".")

	checked := 0
	for _, f := range files {
		walkWithPath(f, func(path []ast.Node, n ast.Node) {
			assign, ok := n.(*ast.AssignStmt)
			if !ok || len(assign.Rhs) != 1 || len(assign.Lhs) != 3 {
				return
			}
			call, ok := assign.Rhs[0].(*ast.CallExpr)
			if !ok {
				return
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "pinRead" {
				return
			}
			checked++
			relIdent, ok := assign.Lhs[1].(*ast.Ident)
			require.True(t, ok && relIdent.Name != "_",
				"%s: pinRead's release discarded — the admission can never be returned and Close will hang",
				fset.Position(assign.Pos()))

			// The innermost enclosing function body owns the defer.
			var body *ast.BlockStmt
			for i := len(path) - 1; i >= 0; i-- {
				switch fn := path[i].(type) {
				case *ast.FuncLit:
					body = fn.Body
				case *ast.FuncDecl:
					body = fn.Body
				}
				if body != nil {
					break
				}
			}
			require.NotNil(t, body, "%s: pinRead outside any function?", fset.Position(assign.Pos()))

			deferred := false
			ast.Inspect(body, func(m ast.Node) bool {
				d, ok := m.(*ast.DeferStmt)
				if !ok {
					return true
				}
				if id, ok := d.Call.Fun.(*ast.Ident); ok && id.Name == relIdent.Name {
					deferred = true
				}
				return true
			})
			require.True(t, deferred,
				"%s: pinRead's release (%q) is not deferred in the same function. An early return between the pin "+
					"and an explicit release leaks the admission, and Close waits on it forever",
				fset.Position(assign.Pos()), relIdent.Name)
		})
	}
	require.Positive(t, checked, "no pinRead call sites found: this check has drifted off the surface it holds")
}
