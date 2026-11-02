package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Production nextPageOrFinishAction and transitionActionState calls must sit in a
// parallelSync handler, the syncer method a one-return handler calls, that method's
// same-file callee, nextPageOrFinishAction, or the two commit closures. Tests and variable calls are unseen.
func TestNextPageCallersAreLedgerHandlers(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	methods := map[string]*ast.FuncDecl{}
	files := map[string]string{}
	var parallel *ast.FuncDecl
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoErrorf(t, err, "parsing %s", name)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Recv == nil || receiverTypeName(fn) != "syncer" {
				continue
			}
			methods[fn.Name.Name] = fn
			files[fn.Name.Name] = name
			if fn.Name.Name == "parallelSync" {
				parallel = fn
			}
		}
	}
	require.NotNil(t, parallel, "parallelSync was not parsed")

	handlers := handlerMethods(parallel)
	require.Contains(t, handlers, "SyncResources")
	require.Contains(t, handlers, "initializeAction")
	require.Contains(t, handlers, "materializeLedgerStaticEntitlements")

	allowed := map[string]bool{}
	for name := range handlers {
		allowed[name] = true
		wrapper, ok := returnedMethod(methods[name])
		if !ok {
			continue
		}
		require.Contains(t, methods, wrapper)
		allowed[wrapper] = true
		for _, callee := range syncerCallees(methods[wrapper]) {
			if files[callee] == files[wrapper] {
				allowed[callee] = true
			}
		}
	}
	require.Contains(t, allowed, "syncLedgerResources")
	require.Contains(t, allowed, "collectLedgerResources")

	var violations []string
	var acceptedNamed []string
	commitClosures := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoErrorf(t, err, "parsing %s", name)
		var stack []ast.Node
		ast.Inspect(file, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			stack = append(stack, n)
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || (sel.Sel.Name != "nextPageOrFinishAction" && sel.Sel.Name != "transitionActionState") {
				return true
			}
			decl, lit := enclosingFunc(stack)
			pos := fset.Position(call.Pos())
			if lit != nil {
				if allowedCommitClosure(stack, lit, decl) {
					commitClosures++
					return true
				}
				violations = append(violations, pos.String()+" "+sel.Sel.Name)
				return true
			}
			if decl != nil && (decl.Name.Name == "nextPageOrFinishAction" || allowed[decl.Name.Name]) {
				acceptedNamed = append(acceptedNamed, decl.Name.Name)
				return true
			}
			where := "<no function>"
			if decl != nil {
				where = decl.Name.Name
			}
			violations = append(violations, pos.String()+" "+where+" calls "+sel.Sel.Name)
			return true
		})
	}
	require.Empty(t, violations)
	require.GreaterOrEqual(t, commitClosures, 2)
	require.Contains(t, acceptedNamed, "initializeAction")
	require.Contains(t, acceptedNamed, "syncLedgerResources")
	require.Contains(t, acceptedNamed, "collectLedgerResources")
	require.Contains(t, acceptedNamed, "nextPageOrFinishAction")
}

func handlerMethods(parallel *ast.FuncDecl) map[string]bool {
	names := map[string]bool{}
	ast.Inspect(parallel.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		var handler ast.Expr
		switch sel.Sel.Name {
		case "invokeActionPage":
			if len(call.Args) >= 3 {
				handler = call.Args[2]
			}
		case "syncParallel":
			if len(call.Args) >= 4 {
				handler = call.Args[3]
			}
		default:
			return true
		}
		ast.Inspect(handler, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			recv, ok := sel.X.(*ast.Ident)
			if ok && recv.Name == "s" {
				names[sel.Sel.Name] = true
			}
			return true
		})
		return true
	})
	return names
}

func returnedMethod(fn *ast.FuncDecl) (string, bool) {
	if fn == nil || fn.Body == nil || len(fn.Body.List) != 1 {
		return "", false
	}
	ret, ok := fn.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		return "", false
	}
	call, ok := ret.Results[0].(*ast.CallExpr)
	if !ok {
		return "", false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	recv, ok := sel.X.(*ast.Ident)
	if !ok || recv.Name != receiverIdent(fn) {
		return "", false
	}
	return sel.Sel.Name, true
}

func syncerCallees(fn *ast.FuncDecl) []string {
	var names []string
	if fn == nil || fn.Body == nil {
		return names
	}
	recv := receiverIdent(fn)
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		id, ok := sel.X.(*ast.Ident)
		if ok && id.Name == recv {
			names = append(names, sel.Sel.Name)
		}
		return true
	})
	return names
}

func enclosingFunc(stack []ast.Node) (*ast.FuncDecl, *ast.FuncLit) {
	var lit *ast.FuncLit
	for i := len(stack) - 1; i >= 0; i-- {
		switch n := stack[i].(type) {
		case *ast.FuncLit:
			if lit == nil {
				lit = n
			}
		case *ast.FuncDecl:
			return n, lit
		}
	}
	return nil, lit
}

func allowedCommitClosure(stack []ast.Node, lit *ast.FuncLit, decl *ast.FuncDecl) bool {
	if decl == nil {
		return false
	}
	switch decl.Name.Name {
	case "invokeActionPage":
		return litIsLastArg(stack, lit, "runPageWithCommit")
	case "syncParallel":
		return ancestorCall(stack, lit, "setParallelActionTransitioner")
	default:
		return false
	}
}

func litIsLastArg(stack []ast.Node, lit *ast.FuncLit, name string) bool {
	for i := len(stack) - 1; i >= 0; i-- {
		call, ok := stack[i].(*ast.CallExpr)
		if !ok || selectorName(call.Fun) != name || len(call.Args) == 0 {
			continue
		}
		return call.Args[len(call.Args)-1] == lit
	}
	return false
}

func ancestorCall(stack []ast.Node, lit *ast.FuncLit, name string) bool {
	seenLit := false
	for i := len(stack) - 1; i >= 0; i-- {
		if stack[i] == lit {
			seenLit = true
			continue
		}
		if !seenLit {
			continue
		}
		call, ok := stack[i].(*ast.CallExpr)
		if ok && selectorName(call.Fun) == name {
			return true
		}
	}
	return false
}

func selectorName(expr ast.Expr) string {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	return sel.Sel.Name
}

func receiverTypeName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return ""
	}
	return typeName(fn.Recv.List[0].Type)
}

func receiverIdent(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 || len(fn.Recv.List[0].Names) != 1 {
		return ""
	}
	return fn.Recv.List[0].Names[0].Name
}

func typeName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return typeName(e.X)
	case *ast.Ident:
		return e.Name
	default:
		return ""
	}
}
