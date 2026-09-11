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

// Splitting the old single mutex into runState's and runStats' left an
// ordering rule that only prose enforced: marshalToken takes run before
// stats, and a second holder of both must take them the same way.
// token.go's header comment and docs/REVIEW_CHECKLIST.md both say so.
//
// A concurrency test cannot pin this. Go's race detector has no lock-order
// analysis, so an inverted second caller deadlocks only when it happens to
// interleave, and the symptom is a test that hangs to its timeout rather
// than one that fails. This walks the package AST instead, so the check is
// deterministic and fires on the commit that adds the second caller.
//
// Two ways a function ends up holding both: taking each mutex directly, or
// taking one and then calling something that takes the other. The second is
// followed one call deep, and only from a function that holds a lock across
// the call — every acquisition on these two holders unlocks by defer, so a
// direct acquisition is held for the rest of the body. A function that holds
// nothing and calls two single-lock functions in sequence, which is what
// unmarshalToken does with loadRunState and loadRunStats, is not holding
// both and does not count.
//
// Where that one hop reaches, exactly: a plain package function, or a
// locking method on runState or runStats. It does not reach a method on any
// other type, so a function holding stats.mu that gets to run.mu through a
// *syncer method is not reported. Closing that needs a transitive closure
// over the call graph rather than one hop. Left undone deliberately: the two
// forms someone writes by hand are covered, the missed shape also has to be
// written stats-first and has to run on a goroutine other than the
// checkpoint's to deadlock at all, and the failure is a hang rather than a
// bad artifact.
//
// The unresolved-acquisition guard below spans every type in the package
// with a mu field, not just these two — childScheduleSet,
// parallelActionQueue and queueAudit included. An acquisition on one of
// those that pkgAST.resolve cannot type fails this test even though no
// run/stats order is at stake there. That is deliberate: a resolver that
// quietly skips what it cannot read stops being a check.

const (
	runHolder   = "runState"
	statsHolder = "runStats"
	// twoLockFunc is the one function allowed to hold both.
	twoLockFunc = "marshalToken"
)

// pkgAST is the package's non-test files. The rule is about production
// ordering, and test helpers routinely build a holder and lock it alone.
type pkgAST struct {
	fset *token.FileSet
	// structFields maps a struct type name to its field names and the base
	// name of each field's type, which is how s.run resolves to runState.
	structFields map[string]map[string]string
	// lockingMethods maps a holder type to the methods that acquire its mu.
	lockingMethods map[string]map[string]bool
	// funcs holds every declaration by the name a caller writes: bare for
	// plain functions, "Type.Method" for methods.
	funcs map[string]*ast.FuncDecl
	decls []*ast.FuncDecl
}

func loadPkgAST(t *testing.T) *pkgAST {
	t.Helper()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	p := &pkgAST{
		fset:           token.NewFileSet(),
		structFields:   map[string]map[string]string{},
		lockingMethods: map[string]map[string]bool{},
		funcs:          map[string]*ast.FuncDecl{},
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(p.fset, name, nil, parser.SkipObjectResolution)
		require.NoErrorf(t, err, "parsing %s", name)

		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					structType, ok := typeSpec.Type.(*ast.StructType)
					if !ok {
						continue
					}
					fields := map[string]string{}
					for _, field := range structType.Fields.List {
						for _, fieldName := range field.Names {
							fields[fieldName.Name] = baseTypeName(field.Type)
						}
					}
					p.structFields[typeSpec.Name.Name] = fields
				}
			case *ast.FuncDecl:
				p.decls = append(p.decls, d)
				p.funcs[declKey(d)] = d
			}
		}
	}
	require.NotEmpty(t, p.decls, "no declarations parsed, so this test is not reading the package")
	require.Contains(t, p.structFields, runHolder)
	require.Contains(t, p.structFields, statsHolder)

	// A holder's own locking methods, which is what a nested acquisition
	// goes through.
	for _, decl := range p.decls {
		recvName, recvType := receiver(decl)
		if recvType != runHolder && recvType != statsHolder {
			continue
		}
		if acquiresOn(decl, recvName) {
			if p.lockingMethods[recvType] == nil {
				p.lockingMethods[recvType] = map[string]bool{}
			}
			p.lockingMethods[recvType][decl.Name.Name] = true
		}
	}
	require.NotEmpty(t, p.lockingMethods[runHolder], "found no locking methods on "+runHolder)
	require.NotEmpty(t, p.lockingMethods[statsHolder], "found no locking methods on "+statsHolder)

	return p
}

func declKey(decl *ast.FuncDecl) string {
	if _, recvType := receiver(decl); recvType != "" {
		return recvType + "." + decl.Name.Name
	}
	return decl.Name.Name
}

// receiver returns the receiver's identifier and type name, both empty for a
// plain function.
func receiver(decl *ast.FuncDecl) (string, string) {
	if decl.Recv == nil || len(decl.Recv.List) != 1 {
		return "", ""
	}
	field := decl.Recv.List[0]
	var name string
	if len(field.Names) == 1 {
		name = field.Names[0].Name
	}
	return name, baseTypeName(field.Type)
}

// baseTypeName strips pointers, slices and maps down to the declared name.
func baseTypeName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return baseTypeName(e.X)
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		if pkgIdent, ok := e.X.(*ast.Ident); ok {
			return pkgIdent.Name + "." + e.Sel.Name
		}
		return e.Sel.Name
	case *ast.ArrayType:
		return baseTypeName(e.Elt)
	case *ast.MapType:
		return baseTypeName(e.Value)
	default:
		return ""
	}
}

// muAcquisition reports the expression a `<expr>.mu.Lock()` or `.RLock()`
// call acquires on, or nil for any other call.
func muAcquisition(call *ast.CallExpr) ast.Expr {
	lockSel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || (lockSel.Sel.Name != "Lock" && lockSel.Sel.Name != "RLock") {
		return nil
	}
	muSel, ok := lockSel.X.(*ast.SelectorExpr)
	if !ok || muSel.Sel.Name != "mu" {
		return nil
	}
	return muSel.X
}

// acquiresOn reports whether decl locks `<name>.mu` directly.
func acquiresOn(decl *ast.FuncDecl, name string) bool {
	found := false
	ast.Inspect(decl, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if target := muAcquisition(call); target != nil {
			if ident, ok := target.(*ast.Ident); ok && ident.Name == name {
				found = true
			}
		}
		return true
	})
	return found
}

// scope maps the identifiers a function can name a holder by — its receiver
// and parameters — to their type names.
func (p *pkgAST) scope(decl *ast.FuncDecl) map[string]string {
	out := map[string]string{}
	if name, typeName := receiver(decl); name != "" {
		out[name] = typeName
	}
	if decl.Type.Params != nil {
		for _, field := range decl.Type.Params.List {
			for _, name := range field.Names {
				out[name.Name] = baseTypeName(field.Type)
			}
		}
	}
	return out
}

// resolve names the type of expr: an identifier in scope, or a field reached
// through one, which is how s.run and s.stats resolve.
func (p *pkgAST) resolve(expr ast.Expr, scope map[string]string) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return scope[e.Name]
	case *ast.ParenExpr:
		return p.resolve(e.X, scope)
	case *ast.StarExpr:
		return p.resolve(e.X, scope)
	case *ast.SelectorExpr:
		outer := p.resolve(e.X, scope)
		if outer == "" {
			return ""
		}
		return p.structFields[outer][e.Sel.Name]
	default:
		return ""
	}
}

// heldDirectly returns the holders decl locks itself, with the position of
// each first acquisition, plus any acquisition whose target this test could
// not resolve to a type.
func (p *pkgAST) heldDirectly(decl *ast.FuncDecl) (map[string]token.Pos, []ast.Expr) {
	scope := p.scope(decl)
	held := map[string]token.Pos{}
	var unresolved []ast.Expr

	ast.Inspect(decl, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		target := muAcquisition(call)
		if target == nil {
			return true
		}
		switch typeName := p.resolve(target, scope); typeName {
		case runHolder, statsHolder:
			if _, seen := held[typeName]; !seen {
				held[typeName] = call.Pos()
			}
		case "":
			unresolved = append(unresolved, target)
		}
		return true
	})
	return held, unresolved
}

// heldThroughCalls returns the holders decl reaches one call deep: a locking
// method on a holder, or a package function that acquires directly.
func (p *pkgAST) heldThroughCalls(decl *ast.FuncDecl) map[string]token.Pos {
	scope := p.scope(decl)
	reached := map[string]token.Pos{}

	ast.Inspect(decl, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.SelectorExpr:
			holder := p.resolve(fun.X, scope)
			if p.lockingMethods[holder][fun.Sel.Name] {
				if _, seen := reached[holder]; !seen {
					reached[holder] = call.Pos()
				}
			}
		case *ast.Ident:
			callee, ok := p.funcs[fun.Name]
			if !ok || callee == decl {
				return true
			}
			calleeHeld, _ := p.heldDirectly(callee)
			for holder := range calleeHeld {
				if _, seen := reached[holder]; !seen {
					reached[holder] = call.Pos()
				}
			}
		}
		return true
	})
	return reached
}

// TestRunStatsLockOrder pins marshalToken as the only holder of both mutexes
// and the order it takes them in.
func TestRunStatsLockOrder(t *testing.T) {
	p := loadPkgAST(t)

	var bothHolders, unresolvedSites []string
	for _, decl := range p.decls {
		direct, unresolved := p.heldDirectly(decl)
		for _, expr := range unresolved {
			unresolvedSites = append(unresolvedSites, fmt.Sprintf("%s: %s", p.fset.Position(expr.Pos()), exprText(expr)))
		}
		if len(direct) == 0 {
			continue
		}

		combined := map[string]token.Pos{}
		for holder, pos := range direct {
			combined[holder] = pos
		}
		// Only a function that already holds a lock can nest a second one.
		for holder, pos := range p.heldThroughCalls(decl) {
			if _, seen := combined[holder]; !seen {
				combined[holder] = pos
			}
		}
		if len(combined) < 2 {
			continue
		}

		key := declKey(decl)
		bothHolders = append(bothHolders, key)
		if key != twoLockFunc {
			continue
		}
		require.Lessf(t, combined[runHolder], combined[statsHolder],
			"%s takes %s's mutex before %s's; token.go's header comment and docs/REVIEW_CHECKLIST.md say run comes first, and a second holder taking them the other way deadlocks",
			key, statsHolder, runHolder)
	}
	sort.Strings(bothHolders)

	require.Emptyf(t, unresolvedSites,
		"pkgAST.resolve could not name the type these mutex acquisitions lock, so this test cannot tell whether they are %s and %s. "+
			"If they are some other holder — parallelActionQueue, queueAudit, childScheduleSet — no lock order is at stake and teaching "+
			"resolve to name the type is all that is needed:\n  %s",
		runHolder, statsHolder, strings.Join(unresolvedSites, "\n  "))
	require.Equalf(t, []string{twoLockFunc}, bothHolders,
		"%s is meant to be the only function holding both %s's and %s's mutex. A new one must take run before stats "+
			"(token.go's header comment, docs/REVIEW_CHECKLIST.md) — say so in its doc comment and add it here.",
		twoLockFunc, runHolder, statsHolder)
}

func exprText(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return exprText(e.X) + "." + e.Sel.Name
	default:
		return fmt.Sprintf("%T", expr)
	}
}
