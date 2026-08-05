package dotc1z

// Stateful reference-model test for the source-cache store lifecycle
// (docs/BUG_CATCHING.md §4 rung 2, the stateful form).
//
// The model is a small in-memory map with explicit durable semantics:
// rows per (kind, scope), manifests per (kind, scope), and the rule
// that every successful public mutation must survive Close + reopen.
// Randomized operation sequences interleave typed puts, manifest
// publication, replay (occupied, zero-row, and missing-manifest),
// canonical and scoped tombstones, and Close/reopen cuts, comparing
// the store against the model after every operation.
//
// This machine-explores the interleavings the directed verification
// tests enumerate by hand — specifically the convention-coupled state
// classes (dirty-vs-Close handoff, zero-result-but-mutated operations,
// scope bleed) that reading kept finding one instance at a time. A
// failure prints the seed; rerun with
// -run '^TestModelRandomizedSourceCacheLifecycle/seed=N$' to reproduce.

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/testtier"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepebble "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

var sourceCacheModelKinds = []sourcecache.RowKind{
	sourcecache.RowKindResources,
	sourcecache.RowKindEntitlements,
	sourcecache.RowKindGrants,
}

// sourceCacheModelScopes deliberately includes a prefix-neighbor pair
// (scope-a / scope-ab). scope-c is tracked but never populated or
// manifested: any row or manifest appearing there is scope bleed.
var sourceCacheModelScopes = []string{"scope-a", "scope-ab", "scope-b"}

const sourceCacheModelBleedScope = "scope-c"

// modelRow carries every identity a later operation may address the
// row by: the comparison key, the canonical tombstone id, and the
// bare scoped-delete id (resource id or grant principal id).
type modelRow struct {
	key         string
	canonicalID string
	bareID      string
}

type sourceCacheModelState struct {
	// rows: kind → scope → comparison key → row.
	rows map[sourcecache.RowKind]map[string]map[string]modelRow
	// manifests: kind → scope → validator ("" = absent).
	manifests map[sourcecache.RowKind]map[string]string
}

func newSourceCacheModelState() *sourceCacheModelState {
	m := &sourceCacheModelState{
		rows:      map[sourcecache.RowKind]map[string]map[string]modelRow{},
		manifests: map[sourcecache.RowKind]map[string]string{},
	}
	for _, kind := range sourceCacheModelKinds {
		m.rows[kind] = map[string]map[string]modelRow{}
		m.manifests[kind] = map[string]string{}
	}
	return m
}

func (m *sourceCacheModelState) scopeRows(kind sourcecache.RowKind, scope string) map[string]modelRow {
	if m.rows[kind][scope] == nil {
		m.rows[kind][scope] = map[string]modelRow{}
	}
	return m.rows[kind][scope]
}

type sourceCacheModelHarness struct {
	t      *testing.T
	r      *rand.Rand
	path   string
	store  c1zstore.Store
	cache  SourceCacheStore
	engine *enginepebble.Engine
	model  *sourceCacheModelState

	src      sourceCacheVerificationStore
	srcModel *sourceCacheModelState

	serial int
	// sawOccupiedCell is the non-vacuity premise: at least one compared
	// cell must have held rows during the run, or every row assertion
	// was empty-vs-empty and the seed proved nothing.
	sawOccupiedCell bool
}

func (h *sourceCacheModelHarness) bind(store c1zstore.Store) {
	h.t.Helper()
	h.store = store
	cache, ok := store.(SourceCacheStore)
	require.True(h.t, ok, "Pebble store must expose SourceCacheStore")
	engine, ok := enginepebble.AsEngine(store)
	require.True(h.t, ok, "Pebble store must expose its engine")
	h.cache = cache
	h.engine = engine
}

// buildRows constructs n fresh rows of kind with a globally unique
// prefix and writes them into the given store under scope. Returned
// modelRows carry every identity later operations need.
func (h *sourceCacheModelHarness) buildRows(
	store c1zstore.Store,
	kind sourcecache.RowKind,
	scope string,
	n int,
) []modelRow {
	h.t.Helper()
	h.serial++
	prefix := fmt.Sprintf("m%04d", h.serial)
	ctx := sourcecache.WithScope(h.t.Context(), scope)
	out := make([]modelRow, 0, n)
	switch kind {
	case sourcecache.RowKindResources:
		rows := make([]*v2.Resource, 0, n)
		for i := range n {
			res := v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "user",
					Resource:     fmt.Sprintf("%s-%d", prefix, i),
				}.Build(),
			}.Build()
			rows = append(rows, res)
			resBID, err := bid.MakeResourceBid(res)
			require.NoError(h.t, err)
			out = append(out, modelRow{
				key:         "user|" + res.GetId().GetResource(),
				canonicalID: resBID,
				bareID:      res.GetId().GetResource(),
			})
		}
		require.NoError(h.t, store.PutResources(ctx, rows...))
	case sourcecache.RowKindEntitlements:
		rows := make([]*v2.Entitlement, 0, n)
		for i := range n {
			ent := v2.Entitlement_builder{
				Id: fmt.Sprintf("%s-%d", prefix, i),
				Resource: v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: "group",
						Resource:     fmt.Sprintf("%s-group-%d", prefix, i),
					}.Build(),
				}.Build(),
			}.Build()
			rows = append(rows, ent)
			out = append(out, modelRow{key: ent.GetId(), canonicalID: ent.GetId()})
		}
		require.NoError(h.t, store.PutEntitlements(ctx, rows...))
	case sourcecache.RowKindGrants:
		rows := make([]*v2.Grant, 0, n)
		for i := range n {
			principal := fmt.Sprintf("%s-principal-%d", prefix, i)
			grant := mkV2Grant("", fmt.Sprintf("%s-%d", prefix, i), "user", principal)
			rows = append(rows, grant)
			out = append(out, modelRow{
				key:         grant.GetId(),
				canonicalID: grant.GetId(),
				bareID:      principal,
			})
		}
		require.NoError(h.t, store.PutGrants(ctx, rows...))
	default:
		h.t.Fatalf("unsupported row kind %q", kind)
	}
	return out
}

// storeRowKeys reads the destination store's rows of kind stamped with
// scope, keyed the same way the model keys them.
func (h *sourceCacheModelHarness) storeRowKeys(kind sourcecache.RowKind, scope string) map[string]struct{} {
	h.t.Helper()
	keys := map[string]struct{}{}
	switch kind {
	case sourcecache.RowKindResources:
		require.NoError(h.t, h.engine.IterateResources(h.t.Context(), func(rec *v3.ResourceRecord) bool {
			if rec.GetSourceScopeKey() == scope {
				keys[rec.GetResourceTypeId()+"|"+rec.GetResourceId()] = struct{}{}
			}
			return true
		}))
	case sourcecache.RowKindEntitlements:
		require.NoError(h.t, h.engine.IterateEntitlements(h.t.Context(), func(rec *v3.EntitlementRecord) bool {
			if rec.GetSourceScopeKey() == scope {
				keys[rec.GetExternalId()] = struct{}{}
			}
			return true
		}))
	case sourcecache.RowKindGrants:
		require.NoError(h.t, h.engine.IterateGrants(h.t.Context(), func(rec *v3.GrantRecord) bool {
			if rec.GetSourceScopeKey() == scope {
				keys[rec.GetExternalId()] = struct{}{}
			}
			return true
		}))
	default:
		h.t.Fatalf("unsupported row kind %q", kind)
	}
	return keys
}

func validateSourceCacheModelCell(
	wantRows map[string]modelRow,
	gotRows map[string]struct{},
	wantValidator string,
	foundManifest bool,
	gotValidator string,
) error {
	if len(gotRows) != len(wantRows) {
		return fmt.Errorf("row count: got %d, want %d", len(gotRows), len(wantRows))
	}
	for key := range wantRows {
		if _, ok := gotRows[key]; !ok {
			return fmt.Errorf("missing row %q", key)
		}
	}
	wantManifest := wantValidator != ""
	if foundManifest != wantManifest {
		return fmt.Errorf("manifest presence: got %t, want %t", foundManifest, wantManifest)
	}
	if foundManifest && gotValidator != wantValidator {
		return fmt.Errorf("manifest validator: got %q, want %q", gotValidator, wantValidator)
	}
	return nil
}

// compare checks every (kind, scope) cell — including the never-touched
// bleed scope — against the model: row sets and manifest state.
func (h *sourceCacheModelHarness) compare(when string) {
	h.t.Helper()
	scopes := append(append([]string{}, sourceCacheModelScopes...), sourceCacheModelBleedScope)
	for _, kind := range sourceCacheModelKinds {
		for _, scope := range scopes {
			want := h.model.scopeRows(kind, scope)
			got := h.storeRowKeys(kind, scope)
			if len(want) > 0 {
				h.sawOccupiedCell = true
			}
			require.Len(h.t, got, len(want), "%s: %s/%s row count", when, kind, scope)
			for key := range want {
				_, ok := got[key]
				require.True(h.t, ok, "%s: %s/%s missing row %q", when, kind, scope, key)
			}
			entry, found, err := h.cache.LookupSourceCacheEntry(h.t.Context(), kind, scope)
			require.NoError(h.t, err, "%s: %s/%s lookup", when, kind, scope)
			wantValidator := h.model.manifests[kind][scope]
			require.NoError(
				h.t,
				validateSourceCacheModelCell(want, got, wantValidator, found, entry.CacheValidator),
				"%s: %s/%s model mismatch",
				when,
				kind,
				scope,
			)
		}
	}
}

func (h *sourceCacheModelHarness) closeReopen() {
	h.t.Helper()
	ctx := h.t.Context()
	require.NoError(h.t, h.store.Close(ctx), "Close must be clean")
	store, err := NewStore(ctx, h.path, WithEngine(c1zstore.EnginePebble))
	require.NoError(h.t, err, "reopen")
	_, startedNew, err := store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(h.t, err, "resume after reopen")
	require.False(h.t, startedNew, "reopen must resume the unfinished sync, not start a new one")
	h.bind(store)
}

func runSourceCacheModelSeed(t *testing.T, seed int64) {
	ctx := t.Context()
	h := &sourceCacheModelHarness{
		t:        t,
		r:        rand.New(rand.NewSource(seed)), //nolint:gosec // deterministic, reproducible op sequences
		path:     t.TempDir() + "/model-dest.c1z",
		model:    newSourceCacheModelState(),
		srcModel: newSourceCacheModelState(),
	}

	// Source artifact: occupied scopes, a prefix-neighbor scope, and a
	// zero-row scope whose manifest makes it a valid empty replacement.
	h.src = newSourceCacheVerificationStore(t)
	for _, kind := range sourceCacheModelKinds {
		for _, fixture := range []struct {
			scope string
			rows  int
		}{
			{scope: "scope-a", rows: 3},
			{scope: "scope-ab", rows: 2},
			{scope: "scope-b", rows: 0},
		} {
			scope, n := fixture.scope, fixture.rows
			if n > 0 {
				for _, row := range h.buildRows(h.src.store, kind, scope, n) {
					h.srcModel.scopeRows(kind, scope)[row.key] = row
				}
			}
			validator := fmt.Sprintf("v-%s-%s", kind, scope)
			require.NoError(t, h.src.cache.PutSourceCacheEntry(ctx, kind, scope, validator))
			h.srcModel.manifests[kind][scope] = validator
		}
	}
	require.NoError(t, h.src.store.EndSync(ctx))
	srcDigestBefore := sourceCacheVerificationEngineDigest(t, h.src.engine)

	store, err := NewStore(ctx, h.path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	h.bind(store)
	t.Cleanup(func() { require.NoError(t, h.store.Close(ctx), "final Close must be clean") })

	pickKind := func() sourcecache.RowKind {
		return sourceCacheModelKinds[h.r.Intn(len(sourceCacheModelKinds))]
	}
	pickScope := func() string {
		return sourceCacheModelScopes[h.r.Intn(len(sourceCacheModelScopes))]
	}
	// pickRows samples up to limit distinct existing rows from a scope.
	pickRows := func(kind sourcecache.RowKind, scope string, limit int) []modelRow {
		rows := h.model.scopeRows(kind, scope)
		keys := make([]string, 0, len(rows))
		for key := range rows {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		h.r.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		out := make([]modelRow, 0, min(limit, len(keys)))
		for _, key := range keys {
			if len(out) >= limit {
				break
			}
			out = append(out, rows[key])
		}
		return out
	}

	const ops = 24
	for op := range ops {
		kind, scope := pickKind(), pickScope()
		when := fmt.Sprintf("op %d", op)
		switch v := h.r.Intn(100); {
		case v < 25: // typed puts
			for _, row := range h.buildRows(h.store, kind, scope, 1+h.r.Intn(3)) {
				h.model.scopeRows(kind, scope)[row.key] = row
			}
		case v < 35: // manifest publication
			h.serial++
			validator := fmt.Sprintf("dv-%04d", h.serial)
			require.NoError(t, h.cache.PutSourceCacheEntry(ctx, kind, scope, validator), when)
			h.model.manifests[kind][scope] = validator
		case v < 50: // replay: replacement from the source artifact
			res, err := h.cache.ReplaySourceCache(ctx, h.src.store, kind, scope)
			require.NoError(t, err, "%s: replay %s/%s", when, kind, scope)
			srcRows := h.srcModel.scopeRows(kind, scope)
			require.Equal(t, int64(len(srcRows)), res.Rows, "%s: replay row count", when)
			replaced := map[string]modelRow{}
			for key, row := range srcRows {
				replaced[key] = row
			}
			h.model.rows[kind][scope] = replaced
			// Replay copies rows only; publishing the destination
			// manifest is the orchestrator's separate PutSourceCacheEntry
			// (see TestVerificationReplayResultIsForwardCacheable), so
			// the model's destination manifest is unchanged.
		case v < 57: // replay without a source manifest: rejected, no mutation
			_, err := h.cache.ReplaySourceCache(ctx, h.src.store, kind, sourceCacheModelBleedScope)
			require.Error(t, err, "%s: replay without source manifest must fail", when)
		case v < 69: // canonical tombstones (valid ids, present and absent)
			ids := make([]string, 0, 3)
			doomed := pickRows(kind, scope, 2)
			for _, row := range doomed {
				ids = append(ids, row.canonicalID)
			}
			if kind == sourcecache.RowKindResources {
				ghost := v2.Resource_builder{
					Id: v2.ResourceId_builder{ResourceType: "user", Resource: "ghost"}.Build(),
				}.Build()
				ghostBID, err := bid.MakeResourceBid(ghost)
				require.NoError(t, err)
				ids = append(ids, ghostBID)
			} else {
				ids = append(ids, "never-there")
			}
			require.NoError(t, h.cache.DeleteSourceCacheRows(ctx, kind, ids), when)
			for _, row := range doomed {
				delete(h.model.scopeRows(kind, scope), row.key)
			}
		case v < 79: // scoped bare-id deletes (resources by id, grants by principal)
			if kind == sourcecache.RowKindEntitlements {
				kind = sourcecache.RowKindGrants
			}
			doomed := pickRows(kind, scope, 2)
			ids := []string{"absent-bare-id"}
			for _, row := range doomed {
				ids = append(ids, row.bareID)
			}
			deleted, err := h.cache.DeleteSourceCacheRowsInScope(ctx, kind, scope, ids)
			require.NoError(t, err, when)
			require.Equal(t, int64(len(doomed)), deleted, "%s: scoped delete count", when)
			for _, row := range doomed {
				delete(h.model.scopeRows(kind, scope), row.key)
			}
		case v < 86: // grant tombstones by external id in scope
			doomed := pickRows(sourcecache.RowKindGrants, scope, 2)
			ids := []string{"absent-grant-id"}
			for _, row := range doomed {
				ids = append(ids, row.canonicalID)
			}
			deleted, err := h.cache.DeleteSourceCacheGrantsByIDInScope(ctx, scope, ids)
			require.NoError(t, err, when)
			require.Equal(t, int64(len(doomed)), deleted, "%s: grant id delete count", when)
			for _, row := range doomed {
				delete(h.model.scopeRows(sourcecache.RowKindGrants, scope), row.key)
			}
		default: // durability cut
			h.closeReopen()
		}
		h.compare(when)
	}

	h.closeReopen()
	h.compare("final reopen")

	require.True(t, h.sawOccupiedCell, "non-vacuity: no compared cell ever held rows")
	require.Equal(t, srcDigestBefore, sourceCacheVerificationEngineDigest(t, h.src.engine),
		"replay source artifact must never be mutated")
}

func TestSourceCacheModelOracleMutationAdequacy(t *testing.T) {
	wantRows := map[string]modelRow{"expected": {key: "expected"}}
	validRows := map[string]struct{}{"expected": {}}
	require.NoError(t, validateSourceCacheModelCell(wantRows, validRows, "validator", true, "validator"))

	tests := []struct {
		name          string
		gotRows       map[string]struct{}
		wantValidator string
		foundManifest bool
		gotValidator  string
	}{
		{name: "missing row", gotRows: map[string]struct{}{}, wantValidator: "validator", foundManifest: true, gotValidator: "validator"},
		{name: "unexpected row", gotRows: map[string]struct{}{"expected": {}, "extra": {}}, wantValidator: "validator", foundManifest: true, gotValidator: "validator"},
		{name: "missing manifest", gotRows: validRows, wantValidator: "validator", foundManifest: false},
		{name: "wrong validator", gotRows: validRows, wantValidator: "validator", foundManifest: true, gotValidator: "wrong"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, validateSourceCacheModelCell(
				wantRows,
				tc.gotRows,
				tc.wantValidator,
				tc.foundManifest,
				tc.gotValidator,
			))
		})
	}
}

func TestModelRandomizedSourceCacheLifecycle(t *testing.T) {
	testtier.RequireNightly(t)
	seeds := 10
	if testing.Short() {
		seeds = 3
	}
	for seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runSourceCacheModelSeed(t, int64(seed))
		})
	}
}
