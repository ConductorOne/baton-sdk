package pebble

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/types/known/anypb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

func rollbackTestGrant(ent, principal string, immutable bool, sourceEnts ...string) *v3.GrantRecord {
	r := testGrantRecord(ent, principal)
	if immutable {
		r.SetAnnotations([]*anypb.Any{expandedGrantImmutableAnnotationAny})
	}
	if len(sourceEnts) > 0 {
		sources := make(map[string]*v3.GrantSourceRecord, len(sourceEnts))
		for _, s := range sourceEnts {
			id := "app:github:" + s
			sources[id] = v3.GrantSourceRecord_builder{EntitlementId: id, IsDirect: s == ent}.Build()
		}
		r.SetSources(sources)
	}
	return r
}

func withNeedsExpansion(r *v3.GrantRecord) *v3.GrantRecord {
	r.SetExpansion(v3.GrantExpandableRecord_builder{EntitlementIds: []string{"app:github:ent-B"}}.Build())
	r.SetNeedsExpansion(true)
	return r
}

// rollbackClassificationGrants holds one grant per classification:
// derived and suspect are deleted (suspect lacks GrantImmutable), selfAndForeign
// and selfOnly have Sources cleared, plain is untouched.
func rollbackClassificationGrants() []*v3.GrantRecord {
	return []*v3.GrantRecord{
		rollbackTestGrant("ent-A", "derived", true, "ent-B"),
		rollbackTestGrant("ent-A", "suspect", false, "ent-B"),
		withNeedsExpansion(rollbackTestGrant("ent-B", "selfAndForeign", false, "ent-B", "ent-A")),
		rollbackTestGrant("ent-B", "selfOnly", false, "ent-B"),
		withNeedsExpansion(rollbackTestGrant("ent-B", "plain", false)),
	}
}

func finishedSyncWithGrants(t *testing.T, e *Engine, grants ...*v3.GrantRecord) string {
	t.Helper()
	ctx := t.Context()
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.PutGrantRecords(ctx, grants...))
	require.NoError(t, e.EndSync(ctx))
	return syncID
}

func grantKeyspace(t *testing.T, e *Engine) map[string]string {
	t.Helper()
	prefix := encodeGrantPrefix()
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upperBoundOf(prefix)})
	require.NoError(t, err)
	out := map[string]string{}
	for iter.First(); iter.Valid(); iter.Next() {
		out[string(iter.Key())] = string(iter.Value())
	}
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())
	return out
}

func grantRecordsByExternalID(t *testing.T, e *Engine) map[string]*v3.GrantRecord {
	t.Helper()
	out := map[string]*v3.GrantRecord{}
	require.NoError(t, e.IterateGrants(t.Context(), func(r *v3.GrantRecord) bool {
		out[r.GetExternalId()] = r
		return true
	}))
	return out
}

func indexKeys(t *testing.T, e *Engine, prefix []byte) map[string]bool {
	t.Helper()
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upperBoundOf(prefix)})
	require.NoError(t, err)
	out := map[string]bool{}
	for iter.First(); iter.Valid(); iter.Next() {
		out[string(iter.Key())] = true
	}
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())
	return out
}

// requireGrantIndexAgreement checks that by_principal holds exactly one entry
// per grant row and by_needs_expansion exactly one per row with
// needs_expansion set, with no orphans in either.
func requireGrantIndexAgreement(t *testing.T, e *Engine) {
	t.Helper()
	wantPrincipal := map[string]bool{}
	wantNeedsExpansion := map[string]bool{}
	require.NoError(t, e.IterateGrants(t.Context(), func(r *v3.GrantRecord) bool {
		id, err := grantIdentityFromRecord(r)
		require.NoError(t, err)
		wantPrincipal[string(encodeGrantByPrincipalIdentityIndexKey(id))] = true
		if r.GetNeedsExpansion() {
			wantNeedsExpansion[string(encodeGrantByNeedsExpansionIdentityIndexKey(id))] = true
		}
		return true
	}))
	require.Equal(t, wantPrincipal, indexKeys(t, e, []byte{versionV3, typeIndex, idxGrantByPrincipal}), "by_principal")
	require.Equal(t, wantNeedsExpansion, indexKeys(t, e, encodeGrantByNeedsExpansionPrefix()), "by_needs_expansion")
}

func TestRollbackExpansionClassifiesBySources(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	syncID := finishedSyncWithGrants(t, e, rollbackClassificationGrants()...)
	before := grantKeyspace(t, e)

	dry, err := e.RollbackExpansion(ctx, syncID, true, false)
	require.NoError(t, err)
	require.Equal(t, &RollbackExpansionResult{GrantsDeleted: 2, SourcesCleared: 2, SuspectConnectorSourced: 1}, dry)
	require.Equal(t, before, grantKeyspace(t, e), "a dry run writes nothing")

	got, err := e.RollbackExpansion(ctx, syncID, false, false)
	require.NoError(t, err)
	require.Equal(t, dry, got, "dry-run counts equal write counts")
	require.NoError(t, e.EndSync(ctx))

	records := grantRecordsByExternalID(t, e)
	require.ElementsMatch(t,
		[]string{"app:github:ent-B:user:selfAndForeign", "app:github:ent-B:user:selfOnly", "app:github:ent-B:user:plain"},
		slicesKeys(records))
	for id, r := range records {
		require.Emptyf(t, r.GetSources(), "%s keeps no Sources", id)
	}
	require.True(t, records["app:github:ent-B:user:selfAndForeign"].GetNeedsExpansion())
	require.NotNil(t, records["app:github:ent-B:user:selfAndForeign"].GetExpansion())
	plainKey := encodeGrantIdentityKey(mustGrantIdentity(t, records["app:github:ent-B:user:plain"]))
	require.Equal(t, before[string(plainKey)], grantKeyspace(t, e)[string(plainKey)], "a grant without Sources is not rewritten")
	requireGrantIndexAgreement(t, e)
}

func TestRollbackExpansionPreservesSuspectGrants(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	syncID := finishedSyncWithGrants(t, e, rollbackClassificationGrants()...)

	got, err := e.RollbackExpansion(ctx, syncID, false, true)
	require.NoError(t, err)
	require.Equal(t, &RollbackExpansionResult{GrantsDeleted: 1, SourcesCleared: 2, SuspectConnectorSourced: 1, SuspectPreserved: 1}, got)
	require.NoError(t, e.EndSync(ctx))

	suspect := grantRecordsByExternalID(t, e)["app:github:ent-A:user:suspect"]
	require.NotNil(t, suspect)
	require.Contains(t, suspect.GetSources(), "app:github:ent-B", "a preserved suspect grant keeps its Sources")
	requireGrantIndexAgreement(t, e)
}

func TestRollbackExpansionSuspectDeletePoisonsItsScope(t *testing.T) {
	scoped := func(r *v3.GrantRecord, scope string) *v3.GrantRecord {
		r.SetSourceScopeKey(scope)
		return r
	}
	grants := func() []*v3.GrantRecord {
		return []*v3.GrantRecord{
			rollbackTestGrant("ent-A", "derived", true, "ent-B"),
			scoped(rollbackTestGrant("ent-A", "suspect", false, "ent-B"), "scope-suspect"),
			scoped(rollbackTestGrant("ent-B", "selfOnly", false, "ent-B"), "scope-cleared"),
		}
	}
	for _, preserve := range []bool{false, true} {
		t.Run(fmt.Sprintf("preserve-%t", preserve), func(t *testing.T) {
			ctx := t.Context()
			e, _ := newTestEngine(t)
			syncID := finishedSyncWithGrants(t, e, grants()...)
			records := grantRecordsByExternalID(t, e)
			require.Equal(t, "scope-suspect", records["app:github:ent-A:user:suspect"].GetSourceScopeKey())
			require.Equal(t, "scope-cleared", records["app:github:ent-B:user:selfOnly"].GetSourceScopeKey())

			_, err := e.RollbackExpansion(ctx, syncID, false, preserve)
			require.NoError(t, err)
			require.NoError(t, e.EndSync(ctx))

			poisoned := func(scope string) bool {
				p, err := e.SourceCachePoisoned(ctx, string(sourcecache.RowKindGrants), scope)
				require.NoError(t, err)
				return p
			}
			require.Equal(t, !preserve, poisoned("scope-suspect"), "deleting a scoped suspect grant poisons its scope")
			require.False(t, poisoned("scope-cleared"), "clearing Sources keeps the row in its scope")
			require.Equal(t, "scope-cleared", grantRecordsByExternalID(t, e)["app:github:ent-B:user:selfOnly"].GetSourceScopeKey())
		})
	}
}

func TestRollbackExpansionNothingToRollBack(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	syncID := finishedSyncWithGrants(t, e, withNeedsExpansion(rollbackTestGrant("ent-B", "plain", false)), rollbackTestGrant("ent-A", "other", false))
	before := grantKeyspace(t, e)

	for _, dryRun := range []bool{true, false} {
		_, err := e.RollbackExpansion(ctx, syncID, dryRun, false)
		require.ErrorIs(t, err, ErrNothingToRollBack)
	}
	require.Equal(t, before, grantKeyspace(t, e))
}

func TestRollbackExpansionNoGrants(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	syncID := finishedSyncWithGrants(t, e)

	for _, dryRun := range []bool{true, false} {
		got, err := e.RollbackExpansion(ctx, syncID, dryRun, false)
		require.NoError(t, err)
		require.Equal(t, &RollbackExpansionResult{}, got)
	}
}

func TestRollbackExpansionUnknownSync(t *testing.T) {
	e, _ := newTestEngine(t)
	finishedSyncWithGrants(t, e, rollbackClassificationGrants()...)
	_, err := e.RollbackExpansion(t.Context(), "2wM4dJ8i0yCZZWwGTS1yQ2jWbNC", true, false)
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestRollbackExpansionLedgerPhases(t *testing.T) {
	cases := []struct {
		state ledgerState
		// beginPass starts an expansion pass on the finished sync, leaving a
		// pending declaration in Collecting.
		beginPass bool
		refused   bool
	}{
		{state: stateUnstarted, refused: true},
		{state: stateCollecting, refused: true},
		{state: stateExpanding},
		{state: stateSealing},
		{state: stateSealed},
		{state: stateSealed, beginPass: true},
	}
	for _, tc := range cases {
		for _, dryRun := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/begin-pass-%t/dry-run-%t", tc.state, tc.beginPass, dryRun), func(t *testing.T) {
				ctx := t.Context()
				e, _ := newTestEngine(t)
				syncID, _ := buildLedgerState(t, e, tc.state)
				require.NoError(t, e.PutGrantRecords(ctx, rollbackClassificationGrants()...))
				if tc.beginPass {
					require.NoError(t, e.Ledger().BeginPass(ctx, []c1zstore.LedgerWork{expansionSeed()}, nil))
				}
				_, phaseBefore, err := e.Ledger().PendingWork(ctx, 0, 1)
				require.NoError(t, err)
				before := grantKeyspace(t, e)

				core, logs := observer.New(zap.WarnLevel)
				got, err := e.RollbackExpansion(ctxzap.ToContext(ctx, zap.New(core)), syncID, dryRun, false)
				_, phaseAfter, phaseErr := e.Ledger().PendingWork(ctx, 0, 1)
				require.NoError(t, phaseErr)
				if tc.refused {
					require.ErrorIs(t, err, ErrRollbackIncompleteCollection)
					require.Equal(t, phaseBefore, phaseAfter)
					require.Equal(t, before, grantKeyspace(t, e))
					return
				}
				require.NoError(t, err)
				wantWarnings := 0
				if phaseBefore != c1zstore.LedgerQueueAbsent {
					wantWarnings = 1
				}
				require.Equal(t, wantWarnings, logs.FilterMessageSnippet("pending pass").Len(), "a pending pass is reported")
				require.Equal(t, &RollbackExpansionResult{GrantsDeleted: 2, SourcesCleared: 2, SuspectConnectorSourced: 1}, got)
				if dryRun {
					require.Equal(t, phaseBefore, phaseAfter, "a dry run leaves the pending pass")
					require.Equal(t, before, grantKeyspace(t, e))
					return
				}
				require.Equal(t, c1zstore.LedgerQueueAbsent, phaseAfter, "a write drops the pending pass")
				require.NoError(t, e.EndSync(ctx))
				requireGrantIndexAgreement(t, e)
			})
		}
	}
}

func TestRollbackExpansionRefusesUnfinishedLegacyCheckpoint(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.PutGrantRecords(ctx, rollbackClassificationGrants()...))
	require.NoError(t, e.CheckpointSync(ctx, "legacy-token"))

	for _, dryRun := range []bool{true, false} {
		_, err := e.RollbackExpansion(ctx, syncID, dryRun, false)
		require.ErrorIs(t, err, ErrRollbackIncompleteCollection)
	}
}

func multiRoundRollbackGrants(n int) ([]*v3.GrantRecord, RollbackExpansionResult) {
	grants := make([]*v3.GrantRecord, 0, n)
	want := RollbackExpansionResult{}
	for i := range n {
		principal := fmt.Sprintf("u%06d", i)
		switch i % 3 {
		case 0:
			grants = append(grants, rollbackTestGrant("ent-A", principal, true, "ent-B"))
			want.GrantsDeleted++
		case 1:
			grants = append(grants, withNeedsExpansion(rollbackTestGrant("ent-B", principal, false, "ent-B", "ent-A")))
			want.SourcesCleared++
		default:
			grants = append(grants, rollbackTestGrant("ent-C", principal, false))
		}
	}
	return grants, want
}

func TestRollbackExpansionMultiRound(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	const n = 2*rollbackExpansionRoundRows + 7
	grants, want := multiRoundRollbackGrants(n)
	syncID := finishedSyncWithGrants(t, e, grants...)

	dry, err := e.RollbackExpansion(ctx, syncID, true, false)
	require.NoError(t, err)
	require.Equal(t, &want, dry)

	commits := 0
	e.db.SetRecordCommitTestHook(func() error {
		commits++
		return nil
	})
	got, err := e.RollbackExpansion(ctx, syncID, false, false)
	e.db.SetRecordCommitTestHook(nil)
	require.NoError(t, err)
	require.Equal(t, &want, got)
	require.GreaterOrEqual(t, commits, 2, "the writes span at least two rounds")
	require.NoError(t, e.EndSync(ctx))

	records := grantRecordsByExternalID(t, e)
	require.Len(t, records, n-want.GrantsDeleted)
	for id, r := range records {
		require.Emptyf(t, r.GetSources(), "%s keeps no Sources", id)
	}
	requireGrantIndexAgreement(t, e)
}

func TestRollbackExpansionCommitFailureLandsNothing(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	syncID := finishedSyncWithGrants(t, e, rollbackClassificationGrants()...)
	before := grantKeyspace(t, e)

	injected := errors.New("injected commit failure")
	e.db.SetRecordCommitTestHook(func() error { return injected })
	_, err := e.RollbackExpansion(ctx, syncID, false, false)
	e.db.SetRecordCommitTestHook(nil)
	require.ErrorIs(t, err, injected)
	require.Equal(t, before, grantKeyspace(t, e))

	got, err := e.RollbackExpansion(ctx, syncID, false, false)
	require.NoError(t, err, "a retry after a failed commit succeeds")
	require.Equal(t, 2, got.GrantsDeleted)
	require.NoError(t, e.EndSync(ctx))
	requireGrantIndexAgreement(t, e)
}

func TestRollbackExpansionRetryAfterLaterRoundFailureConverges(t *testing.T) {
	ctx := t.Context()
	grants, want := multiRoundRollbackGrants(2*rollbackExpansionRoundRows + 7)

	clean, _ := newTestEngine(t)
	cleanSyncID := finishedSyncWithGrants(t, clean, grants...)
	_, err := clean.RollbackExpansion(ctx, cleanSyncID, false, false)
	require.NoError(t, err)
	require.NoError(t, clean.EndSync(ctx))

	e, _ := newTestEngine(t)
	syncID := finishedSyncWithGrants(t, e, grants...)
	injected := errors.New("injected commit failure")
	commits := 0
	e.db.SetRecordCommitTestHook(func() error {
		commits++
		if commits == 2 {
			return injected
		}
		return nil
	})
	_, err = e.RollbackExpansion(ctx, syncID, false, false)
	e.db.SetRecordCommitTestHook(nil)
	require.ErrorIs(t, err, injected)

	got, err := e.RollbackExpansion(ctx, syncID, false, false)
	require.NoError(t, err)
	require.Less(t, got.GrantsDeleted+got.SourcesCleared, want.GrantsDeleted+want.SourcesCleared,
		"the retry counts only rows the committed first round left")
	require.NoError(t, e.EndSync(ctx))
	require.Equal(t, grantKeyspace(t, clean), grantKeyspace(t, e))
	requireGrantIndexAgreement(t, e)
}

func TestRollbackExpansionMalformedRowAbortsRound(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	syncID := finishedSyncWithGrants(t, e, rollbackClassificationGrants()...)
	records := grantRecordsByExternalID(t, e)
	plainKey := encodeGrantIdentityKey(mustGrantIdentity(t, records["app:github:ent-B:user:plain"]))
	require.NoError(t, e.db.UnsafeForTesting().Set(plainKey, []byte("junk"), pebble.NoSync))
	before := grantKeyspace(t, e)

	for _, dryRun := range []bool{true, false} {
		_, err := e.RollbackExpansion(ctx, syncID, dryRun, false)
		require.ErrorContains(t, err, fmt.Sprintf("grant %x", plainKey))
	}
	require.Equal(t, before, grantKeyspace(t, e))
}

func TestRollbackExpansionCanceledWritesNothing(t *testing.T) {
	e, _ := newTestEngine(t)
	syncID := finishedSyncWithGrants(t, e, rollbackClassificationGrants()...)
	before := grantKeyspace(t, e)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := e.RollbackExpansion(ctx, syncID, false, false)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, before, grantKeyspace(t, e))
}

// The classification scan is the only read of each grant row; deletes and
// clears are staged from the scanned value.
func TestRollbackExpansionIssuesNoPointReads(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "rollback_expansion.go", nil, 0)
	require.NoError(t, err)
	var gets []string
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Get" {
			gets = append(gets, fset.Position(call.Pos()).String())
		}
		return true
	})
	require.Empty(t, gets)
}

func mustGrantIdentity(t *testing.T, r *v3.GrantRecord) grantIdentity {
	t.Helper()
	id, err := grantIdentityFromRecord(r)
	require.NoError(t, err)
	return id
}

func slicesKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range maps.Keys(m) {
		out = append(out, k)
	}
	return out
}
