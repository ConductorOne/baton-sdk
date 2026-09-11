package dotc1z

// Why this is a meta-test rather than more cases in
// pebble_store_dirty_test.go: pebbleStore embeds *pebble.Engine, so every
// method of the ledger and stats capabilities is satisfied by promotion
// whether or not anyone wrote it down. The six overrides exist for one
// reason — to wrap the engine call in markDirty, which is what makes Close
// run save() and get the mutation into the .c1z. A mutating method added
// to either interface therefore keeps compiling, keeps satisfying the
// interface, and silently loses its write at Close. No runtime test sees
// that, because the method it would have to call does not exist yet.
//
// So the check is over the method SET: every method of
// c1zstore.PageLedgerStore and c1zstore.SyncStatsStore must be classified
// here, and every one classified as a write must be declared on
// *pebbleStore with markDirty in its body. Adding a method to either
// interface fails this test until its author classifies it.
//
// C22/C24 in docs/verification/page-ledger/plan.md.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// dirtyKind classifies what a capability method does to the file.
type dirtyKind int

const (
	// dirtyWrite mutates the keyspace, so pebbleStore must declare it and
	// mark the store dirty.
	dirtyWrite dirtyKind = iota
	// dirtyRead touches no key, so promotion from the embedded engine is
	// correct and an override would be noise.
	dirtyRead
	// dirtyDeferred returns a writer whose own commit carries the mark.
	dirtyDeferred
)

// capabilityMethods classifies every method of the two capability
// interfaces. A method missing from this map fails the test by name.
var capabilityMethods = map[string]struct {
	kind dirtyKind
	why  string
}{
	// PageLedgerStore.
	"BeginPage":             {dirtyDeferred, "returns a PageWriter; the staging calls write nothing until Commit, and dirtyPageWriter.Commit carries the mark for the whole batch"},
	"GetLedgerRow":          {dirtyRead, "read"},
	"SetRetainLedgerTokens": {dirtyRead, "sets an in-memory flag; the durable retain fact is written by a later page commit, which marks dirty itself"},
	"LedgerFacts":           {dirtyRead, "read"},
	"LedgerCounters":        {dirtyRead, "read"},
	"LedgerFrontier":        {dirtyRead, "read"},
	"TakeoverToken":         {dirtyWrite, "one batch: frontier, facts, bucket, token cleared"},
	"BoundSyncFinished":     {dirtyRead, "read"},
	"ResetLedger":           {dirtyWrite, "a delete is a write; without the mark the wipe never reaches the c1z"},

	// SyncStatsStore.
	"PutCounterBucket": {dirtyWrite, "blind-writes the bucket"},
	"EndSyncWithStats": {dirtyWrite, "the seal: scrub, purge, stamp, ended_at, stats sidecar"},
}

// pebbleStoreMethods returns, for each method declared on *pebbleStore and
// *dirtyPageWriter in this package, whether its body reaches markDirty.
func pebbleStoreMethods(t *testing.T) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", nil, 0)
	require.NoError(t, err)

	out := map[string]bool{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Recv == nil || len(fn.Recv.List) == 0 {
					continue
				}
				star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
				if !ok {
					continue
				}
				ident, ok := star.X.(*ast.Ident)
				if !ok {
					continue
				}
				var key string
				switch ident.Name {
				case "pebbleStore":
					key = fn.Name.Name
				case "dirtyPageWriter":
					key = "dirtyPageWriter." + fn.Name.Name
				default:
					continue
				}
				marks := false
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if id, ok := n.(*ast.Ident); ok && id.Name == "markDirty" {
						marks = true
					}
					return !marks
				})
				out[key] = marks
			}
		}
	}
	return out
}

func TestPebbleStoreDirtyCoverage(t *testing.T) {
	declared := pebbleStoreMethods(t)

	var unclassified []string
	for _, iface := range []reflect.Type{
		reflect.TypeOf((*c1zstore.PageLedgerStore)(nil)).Elem(),
		reflect.TypeOf((*c1zstore.SyncStatsStore)(nil)).Elem(),
	} {
		for i := 0; i < iface.NumMethod(); i++ {
			name := iface.Method(i).Name
			spec, ok := capabilityMethods[name]
			if !ok {
				unclassified = append(unclassified, iface.String()+"."+name)
				continue
			}
			switch spec.kind {
			case dirtyWrite:
				marks, found := declared[name]
				require.Truef(t, found,
					"%s.%s mutates the file (%s) but *pebbleStore does not declare it, so it is promoted from the embedded "+
						"*pebble.Engine and skips markDirty: the mutation lands in pebble and Close drops it without save()",
					iface.String(), name, spec.why)
				require.Truef(t, marks,
					"*pebbleStore.%s is declared but its body never reaches markDirty, so its write does not reach the c1z", name)
			case dirtyDeferred:
				marks, found := declared["dirtyPageWriter."+"Commit"]
				require.Truef(t, found && marks,
					"%s.%s defers its write (%s), so dirtyPageWriter.Commit must carry the mark", iface.String(), name, spec.why)
			case dirtyRead:
				// Promotion is correct; nothing to assert.
			}
		}
	}

	require.Emptyf(t, unclassified,
		"these capability methods are not classified in capabilityMethods, so this test cannot tell whether they need a "+
			"markDirty wrapper. Classify each as dirtyWrite, dirtyRead or dirtyDeferred with a reason:\n  %s",
		strings.Join(unclassified, "\n  "))
}
