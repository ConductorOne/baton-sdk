package pebble

// Meta-tests for the write choke point (internal/rawdb). This is the
// INVERTED form of the old declare-your-raw-writes registry: instead
// of every raw-writing file carrying a declaration, raw-write
// primitives may not appear OUTSIDE internal/rawdb at all — the
// compiler enforces it for real code (the raw *pebble.DB and the
// generic batch/set/delete primitives are unexported there), and this
// test closes the textual gaps the compiler can't see:
//
//  1. No production file in this package may reference the raw pebble
//     write surface (a reintroduced *pebble.Batch field, a direct
//     pebble.Open, a resurrected generic method on rawdb.DB).
//  2. rawdb itself must keep its generic primitives unexported — an
//     exported generic Set/Delete/NewBatch would reduce the choke
//     point to an import-path convention.
//  3. UnsafeForTesting() — the single raw escape hatch — may appear
//     only in _test.go files, anywhere in the engine's client tree
//     (this package, pkg/dotc1z, pkg/synccompactor).

import (
	"go/ast"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// rawWriteSignals are textual markers of raw pebble write usage that
// must not appear in this package's production code outside
// internal/rawdb. Kept call-shaped (trailing "(") or type-shaped so
// prose comments don't false-positive; a genuinely innocent new match
// belongs behind a rawdb family op anyway.
var rawWriteSignals = []string{
	"*pebble.Batch",
	"pebble.Open(",
	".db.Set(",
	".db.Delete(",
	".db.DeleteRange(",
	".db.NewBatch(",
	".db.Apply(",
	".db.Ingest(",
	".db.IngestAndExcise(",
	".db.Excise(",
	".db.LogData(",
}

func TestRawWriteSignalsOnlyInsideRawDB(t *testing.T) {
	// Recursive: subpackages (codec, microtests) are inside the choke
	// point's blast radius too — a *pebble.Batch surfacing in a
	// subpackage API is a raw-write conduit exactly like one here.
	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if filepath.Clean(path) == filepath.Clean("internal/rawdb") {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		src, readErr := os.ReadFile(path) //nolint:gosec // meta-test walking the repo's own source tree
		if readErr != nil {
			return readErr
		}
		for _, signal := range rawWriteSignals {
			require.NotContainsf(t, string(src), signal,
				"%s contains raw pebble write signal %q.\n"+
					"Raw writes live ONLY inside internal/rawdb — add or extend a purpose-named family operation there "+
					"(and state its obligations) instead of reintroducing a raw path.", path, signal)
		}
		return nil
	})
	require.NoError(t, err)
}

// TestRawDBExportsNoRawPebbleSurface parses internal/rawdb's AST and
// rejects any EXPORTED function, method, struct field, or type alias
// whose signature exposes a raw pebble write conduit (*pebble.DB,
// *pebble.Batch, or a callback carrying either). Name-based checks
// were bypassable (a future `WithRawDB(func(*pebble.DB))` or
// `RawBatch() *pebble.Batch` would have passed a fixed-name list); a
// type-based check makes the enforcement track what actually matters —
// whether raw write capability can escape the package. The single
// sanctioned exception is UnsafeForTesting (the testing.Testing()-
// gated escape hatch).
func TestRawDBExportsNoRawPebbleSurface(t *testing.T) {
	fset, files := parseProductionDir(t, "internal/rawdb")

	// rawConduit reports whether a type expression mentions pebble.DB
	// or pebble.Batch (at any pointer/func/slice nesting depth).
	var rawConduit func(expr ast.Expr) bool
	rawConduit = func(expr ast.Expr) bool {
		switch e := expr.(type) {
		case *ast.StarExpr:
			return rawConduit(e.X)
		case *ast.SelectorExpr:
			ident, ok := e.X.(*ast.Ident)
			return ok && ident.Name == "pebble" && (e.Sel.Name == "DB" || e.Sel.Name == "Batch")
		case *ast.FuncType:
			if e.Params != nil {
				for _, f := range e.Params.List {
					if rawConduit(f.Type) {
						return true
					}
				}
			}
			if e.Results != nil {
				for _, f := range e.Results.List {
					if rawConduit(f.Type) {
						return true
					}
				}
			}
			return false
		case *ast.ArrayType:
			return rawConduit(e.Elt)
		case *ast.MapType:
			return rawConduit(e.Key) || rawConduit(e.Value)
		case *ast.ChanType:
			return rawConduit(e.Value)
		}
		return false
	}

	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			switch decl := n.(type) {
			case *ast.FuncDecl:
				if !decl.Name.IsExported() || decl.Name.Name == "UnsafeForTesting" {
					return true
				}
				if rawConduit(&ast.FuncType{Params: decl.Type.Params, Results: decl.Type.Results}) {
					t.Errorf("%s: exported %s exposes a raw pebble write conduit in its signature.\n"+
						"The choke point's enforcement rests on raw capability never escaping internal/rawdb; "+
						"add a purpose-named family operation instead.", fset.Position(decl.Pos()), decl.Name.Name)
				}
			case *ast.TypeSpec:
				if !decl.Name.IsExported() {
					return true
				}
				if st, ok := decl.Type.(*ast.StructType); ok {
					for _, field := range st.Fields.List {
						exported := len(field.Names) == 0 // embedded
						for _, n := range field.Names {
							if n.IsExported() {
								exported = true
							}
						}
						if exported && rawConduit(field.Type) {
							t.Errorf("%s: exported type %s carries a raw pebble conduit in an exported field.",
								fset.Position(field.Pos()), decl.Name.Name)
						}
					}
				}
				if rawConduit(decl.Type) {
					t.Errorf("%s: exported type %s aliases a raw pebble conduit.",
						fset.Position(decl.Pos()), decl.Name.Name)
				}
			}
			return true
		})
	}
}

// walkClientTreeProductionGoFiles walks the engine's client tree
// (this package + pkg/dotc1z + pkg/synccompactor) and yields every
// production (non-_test) .go file's path and contents.
func walkClientTreeProductionGoFiles(t *testing.T, visit func(root, path string, src []byte)) {
	t.Helper()
	roots := []string{
		".",                      // this package (incl. internal/rawdb via walk)
		"../..",                  // pkg/dotc1z (store layer)
		"../../../synccompactor", // the DB() exemption client
	}
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				// Don't wander into the engine tree twice from "../..".
				if root == "../.." && d.Name() == "engine" {
					return filepath.SkipDir
				}
				return nil
			}
			name := d.Name()
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				return nil
			}
			src, readErr := os.ReadFile(path) //nolint:gosec // meta-test walking the repo's own source tree; no untrusted paths
			if readErr != nil {
				return readErr
			}
			visit(root, path, src)
			return nil
		})
		require.NoError(t, err)
	}
}

// TestUnsafeForTestingOnlyInTests walks the engine's client tree and
// forbids the raw escape hatch outside _test.go files. The runtime
// testing.Testing() panic already guards execution; this pins the
// SOURCE so a production call site can't even ship dormant.
func TestUnsafeForTestingOnlyInTests(t *testing.T) {
	walkClientTreeProductionGoFiles(t, func(root, path string, src []byte) {
		// The definition (rawdb.go) and the Engine merge surface's
		// delegation (merge_surface.go) are exempt — by EXACT path
		// under this package's root only, so a spoofed nested tree
		// elsewhere cannot smuggle a production call site past the
		// check (review finding, round 1 of PR 2).
		if root == "." && (filepath.Clean(path) == filepath.Clean("internal/rawdb/rawdb.go") ||
			filepath.Clean(path) == "merge_surface.go") {
			return
		}
		// The signal is USE-shaped — a selector (`x.UnsafeForTesting`),
		// whitespace-tolerant so gofmt's multi-line chains
		// (`eng.\n\tUnsafeForTesting()`) still match (review
		// finding, delta round). Covers direct calls, method values,
		// and method expressions ((*T).UnsafeForTesting); a bare
		// method DECLARATION doesn't match — a declaration grants
		// nothing by itself, and any static production use goes
		// through a selector. Known residual: reflection
		// (MethodByName("Unsafe"+"ForTesting")) evades any source
		// scan; the testing.Testing() runtime panic is the backstop
		// that makes such a call inert in production.
		require.Falsef(t, unsafeForTestingUse.Match(src),
			"%s uses UnsafeForTesting in production code. The escape hatch exists solely for test "+
				"fixtures constructing states the production API cannot express; production writes go "+
				"through a rawdb family operation.", path)
	})
}

// Selector-use signals for the source fences below: a dot immediately
// followed by the name (`x.UnsafeForTesting`), or a dot at the end of
// a line with the name on the gofmt continuation line
// (`eng.\n\tUnsafeForTesting()`). Deliberately NOT `\.\s*`: that
// would also match prose in doc comments ("... delta round).
// UnsafeForTesting stays ..."), and a same-line space between dot and
// selector isn't a form gofmt'd code can take.
var unsafeForTestingUse = regexp.MustCompile(`\.(\n\s*)?UnsafeForTesting\b`)

// TestSeamsArmedOnlyInTests forbids ARMING the Engine's test-seam
// field (Engine.test / testSeams, test_seams.go) outside _test.go
// files. The seams stay compiled into production builds — build-tag
// stripping was considered and rejected (a library can't control its
// consumers' tags, and a default-off tag would make plain
// `go test ./...` silently skip the crash-contract tests) — so this
// fence is what keeps them permanently disarmed there: production
// code may nil-check a hook, never install one.
//
// AST-based, not textual: it flags (a) any assignment whose LHS goes
// through a `.test` selector (`e.test.hook = f`, `e.test = seams`)
// and (b) any address-capture of it (`p := &e.test`, `&e.test.hook`),
// which closes the aliasing route a substring scan misses (capture
// the pointer once, assign through it later, no `.test` in sight).
// Scope is exactly this package's production files: the fields are
// unexported, so the compiler already guarantees no other package can
// reach them.
func TestSeamsArmedOnlyInTests(t *testing.T) {
	fset, files := parseProductionDir(t, ".")

	// throughTestField reports whether expr is, or dereferences into, a
	// selector chain containing the field name "test" (Engine has no
	// other field or method of that name, and this scan covers only
	// this package).
	var throughTestField func(expr ast.Expr) bool
	throughTestField = func(expr ast.Expr) bool {
		switch e := expr.(type) {
		case *ast.SelectorExpr:
			return e.Sel.Name == "test" || throughTestField(e.X)
		case *ast.IndexExpr:
			return throughTestField(e.X)
		case *ast.ParenExpr:
			return throughTestField(e.X)
		case *ast.StarExpr:
			return throughTestField(e.X)
		}
		return false
	}

	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.AssignStmt:
				for _, lhs := range node.Lhs {
					if throughTestField(lhs) {
						t.Errorf("%s: assignment through Engine.test in production code. The seams exist "+
							"solely for tests to inject failures and crash cuts; production paths must never "+
							"arm them.", fset.Position(node.Pos()))
					}
				}
			case *ast.UnaryExpr:
				if node.Op == token.AND && throughTestField(node.X) {
					t.Errorf("%s: address-capture of Engine.test in production code — an alias would let a "+
						"later assignment arm a seam without naming .test. Pass the hook's VALUE if production "+
						"code ever needs one.", fset.Position(node.Pos()))
				}
			}
			return true
		})
	}
}

// mergeSurfaceWriteUse are the selector signals for the Engine merge
// surface's raw write ops (merge_surface.go): NewFoldBatch (generic
// Set/Delete/Commit over record keyspaces, obligations handled by the
// fold contract instead of derivation) plus the bulk range/ingest ops
// promoted from the old DB() handle. Whitespace-tolerant like the
// UnsafeForTesting signal. dbQualifiedMergeWriteUse matches the same
// ops reached through a `.db` field selector — the engine's OWN
// rawdb-handle calls (digest drops, session clears, deferred-index
// ingest, all under the write barrier), which are legitimate and are
// blanked out of a file before the fence scan so the fence still
// catches an Engine-method use like `e.NewFoldBatch()` in engine
// production code (review finding, 2.5 round: a root-"." blanket
// exemption silently weakened this fence vs its pre-2.5 form).
var (
	mergeSurfaceWriteUse     = regexp.MustCompile(`\.(\n\s*)?(NewFoldBatch|DropKeyRange|IngestSSTs|ReplaceRangeWithSSTs)\b`)
	dbQualifiedMergeWriteUse = regexp.MustCompile(`\.db\.(NewFoldBatch|DropKeyRange|IngestSSTs|ReplaceRangeWithSSTs)\b`)
)

// TestMergeSurfaceWritesOnlyInCompactor fences the raw write conduits
// the Engine merge surface carries. Their only sanctioned production
// clients are the fold compactor (pkg/synccompactor/pebble), rawdb
// itself (definitions + internal use), merge_surface.go (the
// delegating definitions), and the engine package's own `.db`
// rawdb-handle calls. Any other use — an Engine-method call in engine
// production code, the store layer, other packages — is a caller
// routing around the typed Stage* obligations (review finding, delta
// round: the exemption surface must not quietly become a second
// engine write path).
func TestMergeSurfaceWritesOnlyInCompactor(t *testing.T) {
	sep := string(filepath.Separator)
	walkClientTreeProductionGoFiles(t, func(root, path string, src []byte) {
		if root == "." && (filepath.Clean(path) == "merge_surface.go" ||
			strings.HasPrefix(filepath.Clean(path), filepath.Clean("internal/rawdb")+sep)) {
			return // the ops' definitions
		}
		if root == "../../../synccompactor" && strings.Contains(filepath.ToSlash(filepath.Clean(path)), "/pebble/") {
			return // the sanctioned fold-compactor client
		}
		// Blank the engine's legitimate `.db.<op>` rawdb calls, then
		// scan what remains.
		scrubbed := dbQualifiedMergeWriteUse.ReplaceAll(src, nil)
		require.Falsef(t, mergeSurfaceWriteUse.Match(scrubbed),
			"%s uses an Engine merge-surface write op outside the fold compactor. These bypass typed "+
				"record staging by contract; record writes anywhere else go through the engine's typed APIs.", path)
	})
}
