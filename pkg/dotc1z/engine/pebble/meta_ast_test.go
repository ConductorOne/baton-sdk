package pebble

// Shared AST loader for the source-fence meta-tests. Three of them
// (the os-IO registry, the test-seam fence, and rawdb's no-raw-conduit
// export check) inspect parsed Go source rather than raw bytes; this
// cache parses each package directory once per test run and hands the
// same ASTs to every consumer, so adding the next AST-based fence
// costs a function, not another ParseDir.
//
// Production files only (_test.go excluded): every current consumer
// fences what SHIPS. A future test-file-scoped check should add a
// second cache rather than widen this one.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type parsedDir struct {
	fset *token.FileSet
	// files maps the parser's file path to its AST, production files
	// only, across all packages found in the directory.
	files map[string]*ast.File
	err   error
}

var productionASTs = struct {
	sync.Mutex
	byDir map[string]*parsedDir
}{byDir: map[string]*parsedDir{}}

// parseProductionDir returns the cached parse of dir's production
// (non-_test) .go files. Not recursive — callers name each package
// directory they fence.
func parseProductionDir(t *testing.T, dir string) (*token.FileSet, map[string]*ast.File) {
	t.Helper()
	productionASTs.Lock()
	defer productionASTs.Unlock()
	pd, ok := productionASTs.byDir[dir]
	if !ok {
		pd = &parsedDir{fset: token.NewFileSet(), files: map[string]*ast.File{}}
		pkgs, err := parser.ParseDir(pd.fset, dir, func(fi os.FileInfo) bool {
			return !strings.HasSuffix(fi.Name(), "_test.go")
		}, 0)
		pd.err = err
		for _, pkg := range pkgs {
			for path, f := range pkg.Files {
				pd.files[path] = f
			}
		}
		productionASTs.byDir[dir] = pd
	}
	require.NoError(t, pd.err)
	return pd.fset, pd.files
}
