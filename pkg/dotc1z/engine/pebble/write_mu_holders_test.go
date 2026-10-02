package pebble

// TestWriteMuHolders checks statically that writeMu is the engine's one
// write lock (see the writeMu field doc):
//
//  1. Every DB mutation the engine makes (a call to a mutating rawdb
//     method on e.db) is made while holding writeMu.
//  2. Nothing that holds writeMu calls a method that takes it. writeMu is
//     not reentrant; such a call is a self-deadlock.
//  3. Every *Locked method is called only while holding writeMu.
//  4. The set of methods taking writeMu directly is exactly
//     writeMuDirectAcquirers, so a new bare writeMu.Lock() has to be
//     named here.
//
// "Holding writeMu" is decided syntactically over this package's
// production files. A call site holds the lock when it is inside a
// function literal passed to withWrite/withWriteAllowSealed, inside a
// *Locked method, after the Lock() in a direct acquirer, or inside a
// method every caller of which holds the lock. Open-time methods (reached
// only from Open, before the engine is published) need no lock. A function
// literal inherits its enclosing context unless it escapes: the function
// of a go statement, a struct field, a return value, or an assignment to
// a field. The engine is recognised as e, l.e and u.l.e; the ledger as l,
// u.l and e.ledger (receiverKind). Methods on Engine, Ledger and pageUnit
// share one name space, so a name may not be declared on two of them. A
// mutation through any other alias of the DB is not followed.

import (
	"fmt"
	"go/ast"
	"go/token"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var writeMuDirectAcquirers = map[string]bool{
	"BoundSyncUnstarted":         true,
	"withWrite":                  true,
	"withWriteAllowSealed":       true,
	"withWriteMu":                true,
	"Close":                      true,
	"CheckpointTo":               true,
	"CompactAllRanges":           true,
	"Flush":                      true,
	"transition":                 true,
	"setSealed":                  true,
	"clearCurrentSync":           true,
	"AbortSynthesizedGrantLayer": true,
}

// writeMuMutatingRawdbMethods are the rawdb.DB methods that mutate the DB
// or its durable state. Reads, metrics, probes, and in-memory flag setters
// are not listed.
var writeMuMutatingRawdbMethods = map[string]bool{
	"NewRecordBatch": true, "NewSessionBatch": true, "NewDigestBatch": true, "NewFoldBatch": true,
	"SessionSet": true, "SessionDelete": true, "SessionClearRange": true,
	"MetaSet": true, "MetaDelete": true,
	"SourceCacheSet": true, "SourceCacheDelete": true, "SourceCacheSetMulti": true,
	"DigestSet": true, "DropKeyRange": true,
	"IngestSSTs": true, "ReplaceRangeWithSSTs": true, "ExciseRange": true,
	"ArmDeferredGrantIndex": true, "ClearDeferredGrantIndexMarker": true, "RestoreDeferredIdxPending": true,
	"FlushMemtables": true, "Checkpoint": true, "Compact": true, "WALSyncPoint": true,
	"Close": true,
}

// merge_surface.go is the compactor's explicit exemption surface: it
// mutates outside writeMu by design, fenced by call order (see its file
// comment).
var writeMuExemptFiles = map[string]bool{"merge_surface.go": true}

type writeMuSite struct {
	pos       token.Pos
	enclosing string // Engine method containing the site
	callee    string // e.<callee>(...) for method calls; "" for mutation sites
	rawdb     string // e.db.<rawdb>(...) for mutation sites
	holder    bool   // lexically under writeMu
}

func TestWriteMuHolders(t *testing.T) {
	fset, files := parseProductionDir(t, ".")

	methods := map[string]*ast.FuncDecl{}
	fileOf := map[string]string{}
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			if fd.Recv == nil || (len(fd.Recv.List) == 1 && checkedReceivers[receiverTypeName(fd.Recv.List[0].Type)]) {
				_, dup := methods[fd.Name.Name]
				require.False(t, dup, "%s declared on two checked types; the checker keys by name", fd.Name.Name)
				methods[fd.Name.Name] = fd
				fileOf[fd.Name.Name] = baseName(fset.Position(fd.Pos()).Filename)
			}
		}
	}

	// 4. Direct acquirers and the position of their Lock().
	lockPos := map[string]token.Pos{}
	for name, fd := range methods {
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok && isEngineField(call.Fun, "writeMu", "Lock") {
				if _, seen := lockPos[name]; !seen {
					lockPos[name] = call.Pos()
				}
			}
			return true
		})
	}
	direct := map[string]bool{}
	for name := range lockPos {
		direct[name] = true
	}
	require.Equal(t, sortedKeys(writeMuDirectAcquirers), sortedKeys(direct),
		"the set of methods taking writeMu directly changed; update writeMuDirectAcquirers and the writeMu field doc")

	// Collect every e.X(...) call and every e.db.<mutating>(...) call with
	// its lexical holder flag.
	var sites []writeMuSite
	callers := map[string]map[string]bool{} // callee -> set of enclosing methods
	for name, fd := range methods {
		walkWithContext(fd.Body, strings.HasSuffix(name, "Locked"), lockPos[name], func(n ast.Node, holder bool) {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && writeMuMutatingRawdbMethods[sel.Sel.Name] && isEngineField(sel.X, "db") {
				sites = append(sites, writeMuSite{pos: call.Pos(), enclosing: name, rawdb: sel.Sel.Name, holder: holder})
			}
			callee, _, ok := memberCall(call)
			if !ok {
				// A plain package function (newSourceCacheDeleteBatch(e, ...)).
				if id, isIdent := call.Fun.(*ast.Ident); isIdent {
					if fd, known := methods[id.Name]; known && fd.Recv == nil {
						callee, ok = id.Name, true
					}
				}
			}
			if ok {
				sites = append(sites, writeMuSite{pos: call.Pos(), enclosing: name, callee: callee, holder: holder})
				if callers[callee] == nil {
					callers[callee] = map[string]bool{}
				}
				callers[callee][name] = true
			}
		})
	}

	// Transitive acquirers: methods that call an acquirer.
	acquirers := map[string]bool{}
	for name := range direct {
		acquirers[name] = true
	}
	for changed := true; changed; {
		changed = false
		for _, s := range sites {
			if s.callee != "" && acquirers[s.callee] && !acquirers[s.enclosing] {
				acquirers[s.enclosing] = true
				changed = true
			}
		}
	}

	// openOnly: reached only from Open. heldOnly: every call site holds
	// the lock. Both are greatest fixpoints — start optimistic, falsify.
	openOnly := map[string]bool{}
	heldOnly := map[string]bool{}
	for name := range methods {
		openOnly[name] = len(callers[name]) > 0 || name == "Open"
		heldOnly[name] = len(callers[name]) > 0 || strings.HasSuffix(name, "Locked")
	}
	holds := func(s writeMuSite) bool {
		return s.holder || heldOnly[s.enclosing]
	}
	for changed := true; changed; {
		changed = false
		for _, s := range sites {
			if s.callee == "" {
				continue
			}
			if openOnly[s.callee] && s.callee != "Open" && !openOnly[s.enclosing] {
				openOnly[s.callee] = false
				changed = true
			}
			if heldOnly[s.callee] && !strings.HasSuffix(s.callee, "Locked") && !holds(s) {
				heldOnly[s.callee] = false
				changed = true
			}
		}
	}

	var violations []string
	report := func(pos token.Pos, format string, args ...any) {
		violations = append(violations, fset.Position(pos).String()+": "+fmt.Sprintf(format, args...))
	}
	for _, s := range sites {
		if writeMuExemptFiles[fileOf[s.enclosing]] {
			continue
		}
		underLock := holds(s)
		switch {
		case s.rawdb != "":
			if !underLock && !openOnly[s.enclosing] {
				report(s.pos, "%s: e.db.%s outside writeMu", s.enclosing, s.rawdb)
			}
		case acquirers[s.callee] && underLock:
			report(s.pos, "%s: calls e.%s, which takes writeMu, while holding writeMu", s.enclosing, s.callee)
		case strings.HasSuffix(s.callee, "Locked") && !underLock && !openOnly[s.enclosing]:
			report(s.pos, "%s: calls e.%s outside writeMu", s.enclosing, s.callee)
		}
	}
	sort.Strings(violations)
	require.Empty(t, violations, "writeMu discipline violations:\n"+strings.Join(violations, "\n"))
}

// walkWithContext visits every node under root with the holder flag. The
// flag is holder, or true for nodes after lock (the position of a direct
// e.writeMu.Lock() call; NoPos when there is none). Function literals that
// escape reset both: their bodies run outside the caller's critical
// section even when they appear after the Lock(). Literals invoked
// synchronously (arguments, local closures) inherit.
func walkWithContext(root ast.Node, holder bool, lock token.Pos, visit func(ast.Node, bool)) {
	var walk func(n ast.Node, holder bool, lock token.Pos)
	escaped := func(lit *ast.FuncLit) { walk(lit.Body, false, token.NoPos) }
	walk = func(n ast.Node, holder bool, lock token.Pos) {
		if n == nil {
			return
		}
		if lock.IsValid() && n.Pos() > lock {
			holder = true
		}
		visit(n, holder)
		switch x := n.(type) {
		case *ast.GoStmt:
			if lit, ok := x.Call.Fun.(*ast.FuncLit); ok {
				escaped(lit)
				for _, a := range x.Call.Args {
					walk(a, holder, lock)
				}
				return
			}
		case *ast.AssignStmt:
			toField := false
			for _, l := range x.Lhs {
				if _, ok := l.(*ast.SelectorExpr); ok {
					toField = true
				}
				walk(l, holder, lock)
			}
			for _, r := range x.Rhs {
				if lit, ok := r.(*ast.FuncLit); ok && toField {
					escaped(lit)
				} else {
					walk(r, holder, lock)
				}
			}
			return
		case *ast.ReturnStmt:
			for _, r := range x.Results {
				if lit, ok := r.(*ast.FuncLit); ok {
					escaped(lit)
				} else {
					walk(r, holder, lock)
				}
			}
			return
		case *ast.KeyValueExpr:
			walk(x.Key, holder, lock)
			if lit, ok := x.Value.(*ast.FuncLit); ok {
				escaped(lit)
			} else {
				walk(x.Value, holder, lock)
			}
			return
		case *ast.CallExpr:
			if callee, kind, ok := memberCall(x); ok && kind == recvEngine && (callee == "withWrite" || callee == "withWriteAllowSealed") {
				walk(x.Fun, holder, lock)
				for _, a := range x.Args {
					if lit, ok := a.(*ast.FuncLit); ok {
						walk(lit.Body, true, lock)
					} else {
						walk(a, holder, lock)
					}
				}
				return
			}
		}
		ast.Inspect(n, func(c ast.Node) bool {
			if c == n {
				return true
			}
			if c != nil {
				walk(c, holder, lock)
			}
			return false
		})
	}
	walk(root, holder, lock)
}

var checkedReceivers = map[string]bool{"Engine": true, "Ledger": true, "pageUnit": true}

type recvKind int

const (
	recvNone recvKind = iota
	recvEngine
	recvLedger
)

// receiverKind resolves an expression to the engine or the ledger: e, l.e,
// u.l.e are the engine; l, u.l, e.ledger are the ledger.
func receiverKind(expr ast.Expr) recvKind {
	switch x := expr.(type) {
	case *ast.Ident:
		switch x.Name {
		case "e":
			return recvEngine
		case "l":
			return recvLedger
		}
	case *ast.SelectorExpr:
		switch x.Sel.Name {
		case "l":
			if _, ok := x.X.(*ast.Ident); ok {
				return recvLedger
			}
		case "e":
			if receiverKind(x.X) == recvLedger {
				return recvEngine
			}
		case "ledger":
			if receiverKind(x.X) == recvEngine {
				return recvLedger
			}
		}
	}
	return recvNone
}

// memberCall reports the method name and receiver kind when call is
// <engine>.X(...) or <ledger>.X(...).
func memberCall(call *ast.CallExpr) (string, recvKind, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", recvNone, false
	}
	if kind := receiverKind(sel.X); kind != recvNone {
		return sel.Sel.Name, kind, true
	}
	return "", recvNone, false
}

// isEngineField reports whether expr is <engine>.parts[0].parts[1]...
func isEngineField(expr ast.Expr, parts ...string) bool {
	for i := len(parts) - 1; i >= 0; i-- {
		sel, ok := expr.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != parts[i] {
			return false
		}
		expr = sel.X
	}
	return receiverKind(expr) == recvEngine
}

func baseName(path string) string {
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[i+1:]
	}
	return path
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
