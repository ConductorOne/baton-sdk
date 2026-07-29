package pebble

// Table-driven harness for the recurring bug class of this stack:
// POST-MUTATION OBLIGATIONS SKIPPED ON THE ERROR PATH. Every mutation
// path with a durable side effect ordered BEFORE its main commit (the
// deferred-index marker arm) or a verdict ordered AFTER other durable
// work (the ended_at stamp) owes a consistent state when the commit
// fails mid-flight. One table row per mutation path: force the failure
// at its seam and assert every declared obligation held anyway. A new
// mutation path gets a table row, not a bespoke test.
//
// The seam registry below (seamFailureCases) is the FailureCases
// enforcement: every error-returning failure seam on testSeams must
// name the test(s) that exercise it, and the meta-test fails when a
// new seam ships without one — or when a registry entry goes stale.

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// seamFailureCases maps every ERROR-RETURNING failure seam on
// testSeams (test_seams.go) to the test functions that exercise its
// failure path. Void hooks (crash-cut points like endSyncPreFlushHook)
// and non-func knobs are out of scope: they inject observation points,
// not failures. Enforced by TestFailureSeamsAreExercised.
var seamFailureCases = map[string][]string{
	"digestBuildHook":  {"TestGrantDigestBuildCrashMidMerge", "TestGrantDigestBuildCrashPostFinish"},
	"recordCommitHook": {"TestFailedMutationPathsFireObligations"},
	"endSyncStampHook": {"TestFailedMutationPathsFireObligations"},
}

// rawdbHookFailureCases covers the failure seams that live ON THE
// CHOKE POINT rather than the engine (the deferred-marker hooks moved
// onto rawdb with the crash state they gate — installed via
// rawdb.DB.SetDeferredMarkerTestHooks). Same enforcement: the setter
// must exist in rawdb's source and every named test must exist here.
var rawdbHookFailureCases = map[string][]string{
	"SetDeferredMarkerTestHooks": {
		"TestDeferredMarkerArmFailureRollsBackCAS",
		"TestDeferredMarkerClearFailureKeepsAgreement",
	},
}

// TestFailureSeamsAreExercised is the completeness meta-test for the
// seam registry: it parses test_seams.go, collects every func-typed
// field whose signature can return an error (a failure-injection
// seam), and requires (a) a registry entry naming at least one test,
// (b) that every named test function exists in this package's test
// sources, and (c) no stale registry entries for removed seams.
func TestFailureSeamsAreExercised(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "test_seams.go", nil, 0)
	require.NoError(t, err)

	failureSeams := map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || ts.Name.Name != "testSeams" {
			return true
		}
		st, ok := ts.Type.(*ast.StructType)
		require.True(t, ok)
		for _, field := range st.Fields.List {
			ft, ok := field.Type.(*ast.FuncType)
			if !ok || ft.Results == nil {
				continue
			}
			returnsError := false
			for _, r := range ft.Results.List {
				if ident, ok := r.Type.(*ast.Ident); ok && ident.Name == "error" {
					returnsError = true
				}
			}
			if !returnsError {
				continue
			}
			for _, name := range field.Names {
				failureSeams[name.Name] = true
			}
		}
		return false
	})
	require.NotEmpty(t, failureSeams, "test_seams.go must declare failure seams")

	// Collect the package's test function names once.
	testFuncs := map[string]bool{}
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	for _, ent := range entries {
		if !strings.HasSuffix(ent.Name(), "_test.go") {
			continue
		}
		tf, err := parser.ParseFile(fset, ent.Name(), nil, 0)
		if err != nil {
			continue // external-package test variants parse separately; names still collected below where valid
		}
		for _, d := range tf.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Recv == nil && strings.HasPrefix(fd.Name.Name, "Test") {
				testFuncs[fd.Name.Name] = true
			}
		}
	}

	for seam := range failureSeams {
		cases, ok := seamFailureCases[seam]
		require.Truef(t, ok,
			"failure seam %q has no registered failure-path test; add a harness row (or bespoke test) and register it in seamFailureCases", seam)
		require.NotEmptyf(t, cases, "failure seam %q registers zero cases", seam)
		for _, name := range cases {
			require.Truef(t, testFuncs[name],
				"seamFailureCases[%q] names test %q, which does not exist in this package", seam, name)
		}
	}
	for seam := range seamFailureCases {
		require.Truef(t, failureSeams[seam],
			"seamFailureCases has an entry for %q, which is not an error-returning seam on testSeams — remove the stale entry", seam)
	}

	// The choke point's own hooks: the setter must still exist (a
	// renamed/removed setter means a stale registry entry), and its
	// cases must exist as tests.
	rawdbSrc, err := os.ReadFile("internal/rawdb/rawdb.go")
	require.NoError(t, err)
	for setter, cases := range rawdbHookFailureCases {
		require.Containsf(t, string(rawdbSrc), "func (d *DB) "+setter+"(",
			"rawdbHookFailureCases names setter %q, which no longer exists on rawdb.DB — update the registry", setter)
		require.NotEmptyf(t, cases, "rawdb hook setter %q registers zero cases", setter)
		for _, name := range cases {
			require.Truef(t, testFuncs[name],
				"rawdbHookFailureCases[%q] names test %q, which does not exist in this package", setter, name)
		}
	}
}

// obligationsCase drives one mutation path to a mid-flight failure.
// run must return the injected error (asserted non-nil); obligations
// assert the engine state the failure demands; converge (optional)
// retries the operation cleanly and asserts full recovery.
type obligationsCase struct {
	name        string
	run         func(t *testing.T, ctx context.Context, a *Adapter, syncID string) error
	obligations func(t *testing.T, ctx context.Context, e *Engine, syncID string)
	converge    func(t *testing.T, ctx context.Context, a *Adapter, syncID string)
}

func TestFailedMutationPathsFireObligations(t *testing.T) {
	injected := errors.New("injected mid-mutation failure")

	cases := []obligationsCase{
		{
			// DEFERRED-regime commit failure AFTER the marker armed:
			// StageGrantPutDeferred durably arms the rebuild marker
			// before its batch can commit (crash contract), so a commit
			// failure leaves armed-marker/no-rows. That state must be
			// internally consistent (flag AND key agree — armed) and
			// harmless: the retry converges and the sealed index is
			// complete. Deterministic pin for what the errorfs soak only
			// covers stochastically.
			name: "deferred-arm-commit-failure",
			run: func(t *testing.T, ctx context.Context, a *Adapter, _ string) error {
				e := a.PebbleEngine()
				e.test.recordCommitHook = func() error { return injected }
				t.Cleanup(func() { e.test.recordCommitHook = nil })
				return a.Grants().StoreExpandedGrants(ctx, mkV2Grant("", "ent-x", "user", "alice"))
			},
			obligations: func(t *testing.T, ctx context.Context, e *Engine, _ string) {
				require.True(t, e.db.DeferredIdxPending(),
					"the marker armed before the commit; the failed commit must not roll it back (a crash here rebuilds at resumed EndSync)")
				_, closer, err := e.db.Get(rawdb.DeferredIdxPendingKey())
				require.NoError(t, err, "the durable marker key must agree with the armed flag")
				closer.Close()
				_, err = e.GetGrantRecord(ctx, canonicalTestGrantID("ent-x", "user", "alice"))
				require.ErrorIs(t, err, pebble.ErrNotFound, "no row of the failed batch may have committed")
			},
			converge: func(t *testing.T, ctx context.Context, a *Adapter, syncID string) {
				e := a.PebbleEngine()
				e.test.recordCommitHook = nil
				// Retry the same write, then seal: the armed marker's
				// rebuild must leave by_principal complete.
				require.NoError(t, a.Grants().StoreExpandedGrants(ctx, mkV2Grant("", "ent-x", "user", "alice")))
				require.NoError(t, a.EndSync(ctx))
				var ents []string
				require.NoError(t, e.IterateGrantsByPrincipal(ctx, "user", "alice", func(r *v3.GrantRecord) bool {
					ents = append(ents, r.GetEntitlement().GetEntitlementId())
					return true
				}))
				require.Equal(t, []string{canonicalTestEntID("ent-x")}, ents,
					"the sealed by_principal index must serve the retried row")
			},
		},
		{
			// The ended_at stamp's commit failing must leak nowhere: the
			// stored record stays unstamped and the sync stays
			// discoverable as unfinished, so a retried EndSync converges.
			name: "endsync-stamp-commit-failure",
			run: func(t *testing.T, ctx context.Context, a *Adapter, _ string) error {
				e := a.PebbleEngine()
				require.NoError(t, a.PutGrants(ctx, mkV2Grant("", "ent-y", "user", "bob")))
				e.test.endSyncStampHook = func() error { return injected }
				t.Cleanup(func() { e.test.endSyncStampHook = nil })
				return a.EndSync(ctx)
			},
			obligations: func(t *testing.T, ctx context.Context, e *Engine, syncID string) {
				rec, err := e.GetSyncRunRecord(ctx, syncID)
				require.NoError(t, err)
				require.Nil(t, rec.GetEndedAt(),
					"a failed stamp commit must not surface ended_at")
				unfinished, err := e.LatestUnfinishedSyncRecord(ctx, nil)
				require.NoError(t, err)
				require.NotNil(t, unfinished,
					"the sync must stay discoverable as unfinished after a failed stamp commit")
				require.Equal(t, syncID, unfinished.GetSyncId())
			},
			converge: func(t *testing.T, ctx context.Context, a *Adapter, syncID string) {
				a.PebbleEngine().test.endSyncStampHook = nil
				require.NoError(t, a.EndSync(ctx), "a retried EndSync must converge")
				rec, err := a.PebbleEngine().GetSyncRunRecord(ctx, syncID)
				require.NoError(t, err)
				require.NotNil(t, rec.GetEndedAt())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			a := newAdapter(t)
			syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)

			err = tc.run(t, ctx, a, syncID)
			require.Error(t, err, "the case must fail mid-mutation; a clean run proves nothing about error-path obligations")
			require.ErrorIs(t, err, injected)
			tc.obligations(t, ctx, a.PebbleEngine(), syncID)
			if tc.converge != nil {
				tc.converge(t, ctx, a, syncID)
			}
		})
	}
}
