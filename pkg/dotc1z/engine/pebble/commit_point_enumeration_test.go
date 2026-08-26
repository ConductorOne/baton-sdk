package pebble

// Mechanical commit-point enumeration (docs/BUG_CATCHING.md §5.10,
// "enumerate injection points mechanically"). The seam registry in
// obligations_on_failure_test.go enforces that every declared seam has
// a failure test; this meta-test enforces the inverse direction: every
// batch-commit call site in engine and compactor production code is assigned a
// registered batch-family route or carries an explicit exclusion with a reason.
// Route registration is drift prevention, not proof that every individual caller
// executed its route; the seam registry is backed by representative failure tests.
// A new commit loop cannot ship without deciding, in writing, how its error path
// is covered — the drift that left the replay clear loop seamless (CO-009) becomes
// a compile-adjacent failure.
//
// Keys are "file.go:EnclosingFunc" (closures attribute to their named
// top-level function). Values are either seam names — which must be
// keys of seamFailureCases or rawdbHookFailureCases, so they are
// transitively backed by executed failure tests — or a single
// "excluded: <reason>" entry.

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

var commitPointRegistry = map[string][]string{
	// Typed record mutations: every site commits a rawdb.RecordBatch,
	// whose Commit passes the record-commit choke-point hook.
	"entitlements.go:PutEntitlementRecords":             {"SetRecordCommitTestHook"},
	"entitlements.go:DeleteEntitlementRecord":           {"SetRecordCommitTestHook"},
	"entitlements.go:DeleteEntitlementRecordByIdentity": {"SetRecordCommitTestHook"},
	"grants.go:PutGrantRecords":                         {"SetRecordCommitTestHook"},
	"grants.go:PutExpandedGrantRecords":                 {"SetRecordCommitTestHook"},
	"grants.go:PutSynthesizedGrantRecords":              {"SetRecordCommitTestHook"},
	"grants.go:putSynthesizedGrantContributionsBatch":   {"SetRecordCommitTestHook"},
	"grants.go:UnsafePutUniqueGrantRecords":             {"SetRecordCommitTestHook"},
	"grants.go:deleteGrantByIdentityLocked":             {"SetRecordCommitTestHook"},
	"grants.go:deleteGrantsByIdentityChunkLocked":       {"SetRecordCommitTestHook"},
	"resources.go:PutResourceRecords":                   {"SetRecordCommitTestHook"},
	"resources.go:DeleteResourceRecord":                 {"SetRecordCommitTestHook"},
	"resource_types.go:PutResourceTypeRecords":          {"SetRecordCommitTestHook"},
	"resource_types.go:DeleteResourceTypeRecord":        {"SetRecordCommitTestHook"},
	"ingest_repair.go:healOrphanPrincipalIndexEntries":  {"SetRecordCommitTestHook"},
	"source_cache.go:InvalidateSourceCacheReplayState":  {"SetRecordCommitTestHook"},

	// Source-cache replay/tombstone loops: dedicated per-loop seams
	// (each distinct commit loop is its own cut — CO-009's lesson).
	// The canonical tombstone paths (DeleteGrantRecordsBounded,
	// DeleteEntitlementRecords, DeleteResourceRecordsBounded) commit
	// exclusively through sourceCacheDeleteBatch.commit below.
	"source_cache.go:ReplaySourceCacheResources":        {"sourceCacheReplayCommitHook"},
	"source_cache.go:ReplaySourceCacheEntitlements":     {"sourceCacheReplayCommitHook"},
	"source_cache.go:ReplaySourceCacheGrants":           {"sourceCacheReplayCommitHook"},
	"source_cache.go:clearReplayDestinationScopeLocked": {"sourceCacheReplayClearCommitHook"},
	"source_cache.go:commit":                            {"sourceCacheDeleteCommitHook"},

	// Digest and session commit points are explicit follow-up debt rather than
	// mutable production test-hook state. Their write failures are covered by
	// errorfs today, but not by exact per-site deterministic seams.
	"digest.go:buildPartitionDigestAtWidth": {
		"excluded: digest batch commit has no exact pre-commit seam; errorfs covers write failure; exact site injection remains follow-up debt",
	},
	"grant_digest_build.go:closePartition": {
		"excluded: digest batch commit has no exact pre-commit seam; crash cuts are post-commit; exact site injection remains follow-up debt",
	},
	"grant_digest_build.go:finish": {
		"excluded: digest batch commit has no exact pre-commit seam; crash cuts are post-commit; exact site injection remains follow-up debt",
	},
	"grant_digest_build.go:writeMissingEntitlementDigestRoots": {
		"excluded: digest batch commit has no exact pre-commit seam; errorfs covers write failure; exact site injection remains follow-up debt",
	},
	"grant_digest_repair.go:InvalidateGrantDigestPartitions": {
		"excluded: repair digest commit has no exact pre-commit seam; invalidation markers and errorfs cover the mechanism; exact site injection remains follow-up debt",
	},
	"grant_digest_repair.go:repairOneGrantDigestPartitionLocked": {
		"excluded: repair digest commit has no exact pre-commit seam; invalidation markers and errorfs cover the mechanism; exact site injection remains follow-up debt",
	},
	"session_store.go:SessionSetMany": {
		"excluded: session batch carries no cross-family obligations; errorfs covers write failure; exact site injection remains follow-up debt",
	},
	"session_store.go:SessionClear": {
		"excluded: session batch carries no cross-family obligations; errorfs covers write failure; exact site injection remains follow-up debt",
	},
	"synccompactor/pebble/fold_commit.go:commitFoldBatch": {
		"excluded: single compactor fold choke point accepts an explicit failure argument; overlay primary/index/restart and merge tests execute its exact error path without mutable engine hooks",
	},

	"internal/rawdb/families.go:DB.SourceCacheSetMulti": {
		"excluded: single-family paged manifest rewrite (seal counts and the rebind count-clear); a failed " +
			"page commit surfaces before EndSync/bindCurrentSync returns, and a partially rewritten manifest " +
			"is fail-closed in both directions (a count-less entry is a hard preflight error; an unpublished " +
			"rebound store is never a replay source), so retry re-converges; errorfs covers write failure; " +
			"exact site injection remains follow-up debt",
	},

	// RecordBatch.Commit is the typed structural route used above. The shared
	// core delegate is intentionally seamless; generic family callers carry
	// explicit dispositions at their call sites.
	"internal/rawdb/families.go:batch.Commit": {
		"excluded: shared core delegate is the choke point; family call sites above carry the routes",
	},
	"internal/rawdb/families.go:RecordBatch.Commit": {"SetRecordCommitTestHook"},
}

func TestCommitPointsHaveFailureSeams(t *testing.T) {
	found := map[string]bool{}
	roots := []struct {
		dir    string
		prefix string
	}{
		{dir: "."},
		{dir: "internal/rawdb", prefix: "internal/rawdb"},
		{dir: "../../../synccompactor/pebble", prefix: "synccompactor/pebble"},
	}
	for _, root := range roots {
		entries, err := os.ReadDir(root.dir)
		require.NoError(t, err)
		fset := token.NewFileSet()
		for _, ent := range entries {
			name := ent.Name()
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(root.dir, name)
			f, err := parser.ParseFile(fset, path, nil, 0)
			require.NoError(t, err, path)
			for _, decl := range f.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "Commit" {
						return true
					}
					displayPath := filepath.ToSlash(filepath.Join(root.prefix, name))
					enclosing := fd.Name.Name
					if root.prefix == "internal/rawdb" && fd.Recv != nil && len(fd.Recv.List) == 1 {
						enclosing = receiverTypeName(fd.Recv.List[0].Type) + "." + enclosing
					}
					found[fmt.Sprintf("%s:%s", displayPath, enclosing)] = true
					return true
				})
			}
		}
	}
	require.NotEmpty(t, found, "commit-point scan found nothing — the scanner is broken")

	var missing []string
	for site := range found {
		if _, ok := commitPointRegistry[site]; !ok {
			missing = append(missing, site)
		}
	}
	sort.Strings(missing)
	require.Emptyf(t, missing,
		"production commit points without a registered failure seam or exclusion — "+
			"add each to commitPointRegistry with a seam name or \"excluded: <reason>\":\n%s",
		strings.Join(missing, "\n"))

	for site, routes := range commitPointRegistry {
		require.Truef(t, found[site],
			"commitPointRegistry entry %q no longer matches a production commit site — remove the stale entry", site)
		require.NotEmptyf(t, routes, "commit point %q registers zero routes", site)
		for _, route := range routes {
			if strings.HasPrefix(route, "excluded: ") {
				require.Greaterf(t, len(route), len("excluded: ")+10,
					"commit point %q exclusion needs a real reason", site)
				continue
			}
			_, engineSeam := seamFailureCases[route]
			_, rawdbHook := rawdbHookFailureCases[route]
			require.Truef(t, engineSeam || rawdbHook,
				"commit point %q routes to %q, which is neither a seamFailureCases key nor a rawdbHookFailureCases key", site, route)
		}
	}
}

func receiverTypeName(expr ast.Expr) string {
	switch v := expr.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.StarExpr:
		return receiverTypeName(v.X)
	default:
		return "unknown-receiver"
	}
}
