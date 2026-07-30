package pebble

// Mechanical commit-point enumeration (docs/BUG_CATCHING.md §5.10,
// "enumerate injection points mechanically"). The seam registry in
// obligations_on_failure_test.go enforces that every declared seam has
// a failure test; this meta-test enforces the inverse direction: every
// batch-commit call site in production code is either reachable by a
// registered failure seam or carries an explicit exclusion with a
// reason. A new commit loop cannot ship without deciding, in writing,
// how its error path gets executed — the drift that left the replay
// clear loop seamless (CO-009) becomes a compile-adjacent failure.
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
	"entitlements.go:PutEntitlementRecords":            {"recordCommitHook"},
	"entitlements.go:DeleteEntitlementRecord":          {"recordCommitHook"},
	"entitlements.go:DeleteEntitlementRecords":         {"recordCommitHook"},
	"grants.go:PutGrantRecords":                        {"recordCommitHook"},
	"grants.go:PutExpandedGrantRecords":                {"recordCommitHook"},
	"grants.go:PutSynthesizedGrantRecords":             {"recordCommitHook"},
	"grants.go:putSynthesizedGrantContributionsBatch":  {"recordCommitHook"},
	"grants.go:UnsafePutUniqueGrantRecords":            {"recordCommitHook"},
	"grants.go:deleteGrantByIdentityLocked":            {"recordCommitHook"},
	"resources.go:PutResourceRecords":                  {"recordCommitHook"},
	"resources.go:DeleteResourceRecord":                {"recordCommitHook"},
	"resource_types.go:PutResourceTypeRecords":         {"recordCommitHook"},
	"resource_types.go:DeleteResourceTypeRecord":       {"recordCommitHook"},
	"if_newer.go:PutResourceRecordsIfNewer":            {"recordCommitHook"},
	"if_newer.go:PutResourceTypeRecordsIfNewer":        {"recordCommitHook"},
	"if_newer.go:PutEntitlementRecordsIfNewer":         {"recordCommitHook"},
	"if_newer.go:PutGrantRecordsIfNewer":               {"recordCommitHook"},
	"ingest_repair.go:healOrphanPrincipalIndexEntries": {"recordCommitHook"},
	"source_cache.go:DeleteGrantRecordsBounded":        {"recordCommitHook"},

	// Source-cache replay/tombstone loops: dedicated per-loop seams
	// (each distinct commit loop is its own cut — CO-009's lesson).
	"source_cache.go:ReplaySourceCacheResources":        {"sourceCacheReplayCommitHook"},
	"source_cache.go:ReplaySourceCacheEntitlements":     {"sourceCacheReplayCommitHook"},
	"source_cache.go:ReplaySourceCacheGrants":           {"sourceCacheReplayCommitHook"},
	"source_cache.go:clearReplayDestinationScopeLocked": {"sourceCacheReplayClearCommitHook"},
	"source_cache.go:commit":                            {"sourceCacheDeleteCommitHook"},

	// Digest build: the build hook fires at the committed-node crash
	// windows these loops create.
	"digest.go:buildPartitionDigestAtWidth":                    {"digestBuildHook"},
	"grant_digest_build.go:closePartition":                     {"digestBuildHook"},
	"grant_digest_build.go:finish":                             {"digestBuildHook"},
	"grant_digest_build.go:writeMissingEntitlementDigestRoots": {"digestBuildHook"},

	// Digest repair: KNOWN GAP, carried visibly. The repair pass has no
	// commit-failure seam; digest reads verify present-means-exact
	// against invalidation markers that record batches stage
	// atomically, and the errorfs sweep covers write-failure physics.
	// A dedicated seam is the outstanding instrument.
	"grant_digest_repair.go:InvalidateGrantDigestPartitions": {
		"excluded: repair-pass digest commit has no failure seam; invalidation markers + errorfs sweep cover the mechanism; dedicated seam is a known gap",
	},
	"grant_digest_repair.go:repairOneGrantDigestPartitionLocked": {
		"excluded: repair-pass digest commit has no failure seam; invalidation markers + errorfs sweep cover the mechanism; dedicated seam is a known gap",
	},

	// Session family: stages no cross-family obligations (no indexes,
	// markers, digests, or fast-path proofs); a failed commit
	// propagates its error and the errorfs sweep covers the physics.
	"session_store.go:SessionSetMany": {
		"excluded: session family carries no cross-family obligations; commit failure propagates; errorfs sweep covers write-failure physics",
	},
	"session_store.go:SessionClear": {
		"excluded: session family carries no cross-family obligations; commit failure propagates; errorfs sweep covers write-failure physics",
	},

	// The shared core delegate is itself the choke point the record
	// hook wraps; routes are registered at the family call sites above.
	"internal/rawdb/families.go:Commit": {
		"excluded: shared core delegate is the choke point; family call sites above carry the routes",
	},
}

func TestCommitPointsHaveFailureSeams(t *testing.T) {
	found := map[string]bool{}
	for _, dir := range []string{".", "internal/rawdb"} {
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		fset := token.NewFileSet()
		for _, ent := range entries {
			name := ent.Name()
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(dir, name)
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
					found[fmt.Sprintf("%s:%s", path, fd.Name.Name)] = true
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
