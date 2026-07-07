package expand

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// #1 Randomized differential harness
// ----------------------------------------------------------------------------

// TestTopologicalMergeDifferentialRandom generates pseudo-random acyclic
// entitlement graphs and asserts every new topological algorithm reproduces the
// current source-batched expander's grant set exactly. Each seed varies the
// node count, edge density, shallow flags, resource-type filters, principal
// pool, multi-grant principals, and pre-existing (indirect) source provenance.
//
// This is the property-based companion to the hand-written cases in
// topological_merge_test.go: the explicit cases pin known-interesting shapes,
// while this fuzz harness sweeps the combinatorial long tail (overlapping
// fan-in/fan-out, mixed filters across reconverging paths) that point cases
// miss. The current expander is a valid oracle here because every generated
// graph is acyclic — edges only ever point from a lower entitlement index to a
// higher one — so the current expander's cycle nondeterminism never applies.
//
// This variant runs entirely against the in-memory MockExpanderStore, so it is
// orders of magnitude faster than the real-store sweep and can afford a wide
// seed range. TestTopologicalMergeDifferentialRandomStore drives the same
// generated cases through real SQLite/Pebble c1z files for the storage- and
// reader-path coverage the mock can't provide.
func TestTopologicalMergeDifferentialRandom(t *testing.T) {
	start, count := fuzzSeedRange(60, 300)
	for seed := start; seed < start+count; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			tc := randomExpansionCase(seed)
			compareCurrentAndAllTopological(t, func(ctx context.Context) (*MockExpanderStore, *EntitlementGraph) {
				return mockStoreFromCase(t, ctx, tc)
			})
		})
	}
}

// TestTopologicalMergeDifferentialRandomStore is the real-store counterpart to
// TestTopologicalMergeDifferentialRandom. For each generated case it runs the
// current source-batched expander on SQLite as ground truth, then asserts every
// new algorithm reproduces it through a real c1z — exercising the V2<->storage
// translation, the StoreExpandedGrants upsert/index-cleanup path, and the real
// grant readers (Pebble's principal-sorted true-streaming path and SQLite's
// id-ordered buffer+sort fallback), none of which the mock store covers.
//
// It is far slower than the mock sweep (a fresh c1z per algorithm per seed), so
// it runs a much smaller seed range. The mock sweep remains the wide net; this
// one is the depth check on the production storage path.
func TestTopologicalMergeDifferentialRandomStore(t *testing.T) {
	// ~1.3s/seed (a fresh c1z per algorithm per seed), so keep this modest: the
	// mock sweep is the wide net, this is the storage-path depth check.
	// Store-backed seeds cost ~10s each (a pebble + sqlite store per
	// seed), so short mode (Windows CI) runs a token few; the wide
	// sweep is long-CI and ad-hoc (BATON_EXPAND_FUZZ_SEEDS) territory.
	start, count := fuzzSeedRange(3, 25)
	for seed := start; seed < start+count; seed++ {
		seed := seed
		tc := randomExpansionCase(seed)
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// Ground truth: the current source-batched expander on SQLite.
			ground := runExpansion(t, ctx, tc, dotc1z.EngineSQLite, 0, func(e *Expander) error { return e.Run(ctx) })

			candidates := []struct {
				name     string
				engine   dotc1z.Engine
				pageSize uint32
				run      func(*Expander) error
			}{
				{"streaming/sqlite", dotc1z.EngineSQLite, 0, func(e *Expander) error { return e.RunTopologicalMergeStreaming(ctx) }},
				{"projection/sqlite", dotc1z.EngineSQLite, 0, func(e *Expander) error { return e.RunTopologicalMergeProjection(ctx) }},
				{"streaming/pebble", dotc1z.EnginePebble, 0, func(e *Expander) error { return e.RunTopologicalMergeStreaming(ctx) }},
				{"projection/pebble", dotc1z.EnginePebble, 0, func(e *Expander) error { return e.RunTopologicalMergeProjection(ctx) }},
				// Small Pebble pages force principal groups to span reader pages,
				// exercising streamingPrincipalGroupStream paging + pushback.
				{"streaming/pebble/small-page", dotc1z.EnginePebble, 2, func(e *Expander) error { return e.RunTopologicalMergeStreaming(ctx) }},
			}
			for _, c := range candidates {
				got := runExpansion(t, ctx, tc, c.engine, c.pageSize, c.run)
				assertStoreSnapshotsEqual(t, ground, got, c.name)
			}
		})
	}
}

// randomExpansionCase deterministically generates an acyclic sqliteParityCase
// for a seed. Both differential fuzzers build from this single generator so the
// mock and real-store sweeps explore the same graph space. Edges only ever run
// from a lower entitlement index to a higher one, keeping every graph acyclic
// (so the current expander is a valid, deterministic oracle).
func randomExpansionCase(seed int64) sqliteParityCase {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test fixture generation.

	numEnts := 3 + rng.Intn(6) // 3..8 entitlements.
	entIDs := make([]string, numEnts)
	for i := range entIDs {
		entIDs[i] = fmt.Sprintf("ent:%02d", i)
	}

	resourceTypes := []string{"user", "group", "service"}

	// A fixed principal pool. Each principal key carries a single stable payload
	// across the whole store so this harness exercises identity and provenance,
	// not payload tie-breaks (those are covered by
	// TestTopologicalMergePrincipalPayloadAgreement).
	numPrincipals := 3 + rng.Intn(8)
	type principal struct {
		rt, id string
	}
	principals := make([]principal, numPrincipals)
	for i := range principals {
		principals[i] = principal{
			rt: resourceTypes[rng.Intn(len(resourceTypes))],
			id: fmt.Sprintf("p%02d", i),
		}
	}

	// Base grants: random principals on random entitlements, with a minority
	// carrying a pre-existing indirect source so shallow/direct edge filtering
	// has something to chew on.
	numGrants := numEnts + rng.Intn(numEnts*3)
	grants := make([]sqliteGrantSpec, 0, numGrants)
	// One grant per (entitlement, principal): Pebble keys grants by
	// structural identity, so duplicate pairs with distinct external ids
	// deliberately fold there while SQLite keeps both rows — cross-engine
	// byte-parity cannot hold for such fixtures. The fold semantics are
	// pinned by the migration/bulk-import duplicate-merge tests; this
	// fuzzer explores expansion semantics.
	seenPair := make(map[string]struct{}, numGrants)
	for i := 0; i < numGrants; i++ {
		entID := entIDs[rng.Intn(numEnts)]
		p := principals[rng.Intn(numPrincipals)]
		pairKey := entID + "\x00" + p.rt + "\x00" + p.id
		if _, dup := seenPair[pairKey]; dup {
			continue
		}
		seenPair[pairKey] = struct{}{}
		gs := sqliteGrantSpec{
			id:            fmt.Sprintf("grant:%03d:%s:%s:%s", i, entID, p.rt, p.id),
			entitlementID: entID,
			principalRT:   p.rt,
			principalID:   p.id,
		}
		if rng.Intn(4) == 0 {
			gs.sources = map[string]bool{"ent:external": false}
		}
		grants = append(grants, gs)
	}

	edges := make([]sqliteEdgeSpec, 0)
	for i := 0; i < numEnts; i++ {
		for j := i + 1; j < numEnts; j++ {
			if rng.Intn(3) != 0 {
				continue
			}
			edge := sqliteEdgeSpec{src: entIDs[i], dst: entIDs[j], shallow: rng.Intn(2) == 0}
			switch rng.Intn(3) {
			case 1:
				edge.rtids = []string{resourceTypes[rng.Intn(len(resourceTypes))]}
			case 2:
				edge.rtids = []string{"user", "service"}
			}
			edges = append(edges, edge)
		}
	}

	return sqliteParityCase{
		name:           fmt.Sprintf("seed=%d", seed),
		entitlementIDs: entIDs,
		grants:         grants,
		edges:          edges,
	}
}

// mockStoreFromCase materializes a sqliteParityCase into an in-memory mock store
// and entitlement graph, mirroring seedSQLiteBaseData / buildGraphFromCase so
// the mock and real-store fuzzers operate on identical inputs.
func mockStoreFromCase(t *testing.T, ctx context.Context, tc sqliteParityCase) (*MockExpanderStore, *EntitlementGraph) {
	t.Helper()
	store := NewMockExpanderStore()
	group := makeResource("group", "org")

	entByID := make(map[string]*v2.Entitlement, len(tc.entitlementIDs))
	for _, id := range tc.entitlementIDs {
		ent := makeEntitlement(id, group)
		store.AddEntitlement(ent)
		entByID[id] = ent
	}
	for _, gs := range tc.grants {
		grant := makeGrant(gs.id, entByID[gs.entitlementID], makeResource(gs.principalRT, gs.principalID))
		if len(gs.sources) > 0 {
			srcMap := make(map[string]*v2.GrantSources_GrantSource, len(gs.sources))
			for sourceID, isDirect := range gs.sources {
				srcMap[sourceID] = &v2.GrantSources_GrantSource{IsDirect: isDirect}
			}
			grant.SetSources(v2.GrantSources_builder{Sources: srcMap}.Build())
		}
		store.AddGrant(grant)
	}

	graph := NewEntitlementGraph(ctx)
	for _, id := range tc.entitlementIDs {
		graph.AddEntitlementID(id)
	}
	for _, edge := range tc.edges {
		require.NoError(t, graph.AddEdge(ctx, edge.src, edge.dst, edge.shallow, edge.rtids))
	}
	// Mirror production / buildGraphFromCase: no-op for the acyclic cases here.
	require.NoError(t, graph.FixCycles(ctx))
	return store, graph
}

// TestTopologicalMergeEmbeddedNulPrincipal exercises the projection key codec
// with principal/grant ids that contain the tuple separator (0x00) and escape
// (0x01) bytes. The projection key is tuple-encoded precisely so these bytes
// don't collide with the key separator; a raw-separator key would mis-split the
// principal during decode and drop or misattribute contributions.
//
// All ids carry embedded NUL/escape bytes, and the destination has in-degree 2
// so the planner selects topological_projection — routing every contributing
// principal through encodeProjectionKV/decodeProjectionPrincipal. The new
// algorithms must still match the current expander exactly.
func TestTopologicalMergeEmbeddedNulPrincipal(t *testing.T) {
	compareCurrentAndAllTopological(t, func(ctx context.Context) (*MockExpanderStore, *EntitlementGraph) {
		store := NewMockExpanderStore()
		group := makeResource("group", "or\x00g")

		s1 := makeEntitlement("ent:s1\x00x", group)
		s2 := makeEntitlement("ent:s2\x01y", group)
		dest := makeEntitlement("ent:dest", group)
		store.AddEntitlement(s1)
		store.AddEntitlement(s2)
		store.AddEntitlement(dest)

		// Principals whose ids contain a separator (0x00) and an escape (0x01)
		// byte, plus one whose resource type carries them too.
		alice := makeResource("user", "ali\x00ce")
		bob := makeResource("user", "bo\x01b")
		odd := makeResource("ro\x00le", "r\x01\x00t")

		store.AddGrant(makeGrant("grant:alice:s1\x00", s1, alice))
		store.AddGrant(makeGrant("grant:bob:s1", s1, bob))
		store.AddGrant(makeGrant("grant:alice:s2", s2, alice))
		store.AddGrant(makeGrant("grant:odd:s2\x01", s2, odd))

		graph := NewEntitlementGraph(ctx)
		graph.AddEntitlementID(s1.GetId())
		graph.AddEntitlementID(s2.GetId())
		graph.AddEntitlementID(dest.GetId())
		require.NoError(t, graph.AddEdge(ctx, s1.GetId(), dest.GetId(), false, nil))
		require.NoError(t, graph.AddEdge(ctx, s2.GetId(), dest.GetId(), false, nil))
		return store, graph
	})
}

// TestTopologicalMergeChunkedDirtyFlush forces the per-destination dirty buffer
// to flush many times within a single destination (by shrinking the flush chunk
// to 2) and asserts every algorithm still matches the current expander. With the
// default 10k chunk no fixture crosses a flush boundary, so this is the only
// coverage of the multi-flush path: buffer truncation/reuse, repeated sink
// calls, and (for projection) projection-row appends interleaved mid-destination.
func TestTopologicalMergeChunkedDirtyFlush(t *testing.T) {
	prev := expansionDirtyFlushChunk
	expansionDirtyFlushChunk = 2
	t.Cleanup(func() { expansionDirtyFlushChunk = prev })

	// A handful of seeds, each a multi-principal graph, so destinations emit
	// more dirty grants than the (now tiny) flush chunk.
	for seed := int64(0); seed < 25; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			tc := randomExpansionCase(seed)
			compareCurrentAndAllTopological(t, func(ctx context.Context) (*MockExpanderStore, *EntitlementGraph) {
				return mockStoreFromCase(t, ctx, tc)
			})
		})
	}
}

// ----------------------------------------------------------------------------
// #2 Deterministic principal payload tie-break
// ----------------------------------------------------------------------------

// TestTopologicalMergePrincipalPayloadAgreement covers the principal-payload
// tie-break flagged in docs/tasks/grant-expansion-streaming-merge.md §4.1.
//
// When the same principal KEY (resource_type, resource) appears on multiple
// parents with DIFFERENT full payloads, a synthesized child grant can only
// carry one of them. The snapshot-based parity tests never catch a divergence
// here because they compare only the principal's resource_type and resource id,
// never the rest of the payload (display name, description, ...).
//
// The load-bearing production guarantee is that the three new algorithms agree
// with each other and are individually deterministic, so the fleet can switch
// algorithms (or fall back) without rewriting principal payloads. This test
// pins that guarantee on the full marshaled principal, and additionally records
// the chosen payload so a future change to the tie-break is visible in review.
func TestTopologicalMergePrincipalPayloadAgreement(t *testing.T) {
	// Two parents (S1, S2) both feed D, giving D in-degree 2 so the planner
	// selects topological_projection and all stream paths are exercised. alice
	// appears on both parents with conflicting display names.
	setup := func(ctx context.Context) (*MockExpanderStore, *EntitlementGraph) {
		store := NewMockExpanderStore()
		group := makeResource("group", "org")

		s1 := makeEntitlement("ent:s1", group)
		s2 := makeEntitlement("ent:s2", group)
		dest := makeEntitlement("ent:dest", group)
		store.AddEntitlement(s1)
		store.AddEntitlement(s2)
		store.AddEntitlement(dest)

		store.AddGrant(makeGrant("grant:alice:s1", s1, makeResourceWithDisplayName("user", "alice", "alice-from-s1")))
		store.AddGrant(makeGrant("grant:alice:s2", s2, makeResourceWithDisplayName("user", "alice", "alice-from-s2")))

		graph := NewEntitlementGraph(ctx)
		graph.AddEntitlementID(s1.GetId())
		graph.AddEntitlementID(s2.GetId())
		graph.AddEntitlementID(dest.GetId())
		require.NoError(t, graph.AddEdge(ctx, s1.GetId(), dest.GetId(), false, []string{"user"}))
		require.NoError(t, graph.AddEdge(ctx, s2.GetId(), dest.GetId(), false, []string{"user"}))
		return store, graph
	}

	run := func(t *testing.T, runAlgo func(context.Context, *Expander) error) string {
		t.Helper()
		ctx := context.Background()
		store, graph := setup(ctx)
		require.NoError(t, runAlgo(ctx, NewExpander(store, graph)))
		name, ok := synthesizedPrincipalDisplayName(store, "ent:dest", "user", "alice")
		require.True(t, ok, "expected a synthesized grant for alice on ent:dest")
		return name
	}

	algos := []struct {
		name string
		run  func(context.Context, *Expander) error
	}{
		{"streaming", func(ctx context.Context, e *Expander) error { return e.RunTopologicalMergeStreaming(ctx) }},
		{"projection", func(ctx context.Context, e *Expander) error { return e.RunTopologicalMergeProjection(ctx) }},
	}

	var reference string
	haveReference := false
	for _, algo := range algos {
		first := run(t, algo.run)
		second := run(t, algo.run)
		require.Equalf(t, first, second, "%s: principal payload not deterministic across runs", algo.name)
		if !haveReference {
			reference = first
			haveReference = true
			continue
		}
		require.Equalf(t, reference, first,
			"%s: synthesized principal payload disagrees with the first algorithm (%q vs %q)",
			algo.name, reference, first)
	}

	// Pin the agreed tie-break so a future change to it surfaces in review.
	// All three algorithms order parents by source node then entitlement id, so
	// the lower parent (S1) wins.
	require.Equal(t, "alice-from-s1", reference, "unexpected principal payload tie-break winner")
}

// ----------------------------------------------------------------------------
// #3 Untouched base grants are never rewritten
// ----------------------------------------------------------------------------

// TestTopologicalMergeUntouchedBaseGrantNotRewritten pins §4 rule 3 of
// docs/tasks/grant-expansion-streaming-merge.md: a base grant for a principal
// that receives NO contribution must be left completely alone — not re-emitted
// through the write sink. Re-emitting it would churn Pebble, reset DiscoveredAt
// preservation guarantees, and diverge from the current expander; not
// re-emitting it is the central reduction the topological rewrite promises.
//
// The snapshot parity tests can't catch a spurious rewrite (they compare the
// final grant set, and a rewrite that preserves content looks identical), so
// this test records every grant handed to StoreExpandedGrants and asserts the
// untouched principal never appears, while a genuinely-contributed principal
// does. It runs on real SQLite and Pebble stores so the true write path is
// exercised.
func TestTopologicalMergeUntouchedBaseGrantNotRewritten(t *testing.T) {
	algos := []struct {
		name string
		run  func(context.Context, *Expander) error
	}{
		{"streaming", func(ctx context.Context, e *Expander) error { return e.RunTopologicalMergeStreaming(ctx) }},
		{"projection", func(ctx context.Context, e *Expander) error { return e.RunTopologicalMergeProjection(ctx) }},
	}

	// S grants alice; D already has a base grant for bob, who is NOT on S and so
	// gets no contribution. After expansion D must hold a synthesized alice
	// grant, and bob's base grant must never have been rewritten.
	tc := sqliteParityCase{
		name:           "untouched_base",
		entitlementIDs: []string{"ent:source", "ent:dest"},
		grants: []sqliteGrantSpec{
			{id: "grant:alice:source", entitlementID: "ent:source", principalRT: "user", principalID: "alice"},
			{id: "grant:bob:dest", entitlementID: "ent:dest", principalRT: "user", principalID: "bob"},
		},
		edges: []sqliteEdgeSpec{{src: "ent:source", dst: "ent:dest", rtids: []string{"user"}}},
	}

	for _, engine := range []dotc1z.Engine{dotc1z.EngineSQLite, dotc1z.EnginePebble} {
		for _, algo := range algos {
			label := string(engine) + "/" + algo.name
			t.Run(label, func(t *testing.T) {
				ctx := context.Background()
				path := filepath.Join(t.TempDir(), "untouched.c1z")

				store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
				require.NoError(t, err)
				syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				seedSQLiteBaseData(t, ctx, store, tc)

				graph := buildGraphFromCase(t, ctx, tc, engine)
				rec := &recordingExpanderStore{inner: benchmarkExpanderStore{store: store}}
				require.NoError(t, algo.run(ctx, NewExpander(rec, graph)))

				// bob's base grant got no contribution; it must never have been
				// written to the expansion sink.
				require.NotContains(t, rec.storedIDs(), "grant:bob:dest",
					"%s: untouched base grant was rewritten through the expansion sink", label)
				// alice did get a contribution; the synthesized grant proves
				// expansion actually ran (so the assertion above is meaningful).
				// Synthesized ids are the raw concat of the connector's own
				// entitlement id (colons and all) with the principal ref.
				require.Contains(t, rec.storedIDs(), "ent:dest:user:alice",
					"%s: expected alice to be synthesized on ent:dest", label)

				require.NoError(t, store.EndSync(ctx))
				require.NoError(t, store.Close(ctx))

				ro, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
				require.NoError(t, err)
				defer func() { require.NoError(t, ro.Close(ctx)) }()
				require.NoError(t, ro.SetCurrentSync(ctx, syncID))
				snap := readBackGrantSnapshot(t, ctx, ro)

				bob, ok := snap["ent:dest\x00user\x00bob"]
				require.Truef(t, ok, "%s: bob's base grant disappeared", label)
				require.Emptyf(t, bob.sourceDirect, "%s: untouched base grant gained sources", label)
				require.NotContainsf(t, bob.annotationTypes, immutableAnnotationAny.GetTypeUrl(),
					"%s: untouched base grant gained the immutable expansion annotation", label)
			})
		}
	}
}

// ----------------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------------

// fuzzSeedRange returns the [start, start+count) seed range for a differential
// fuzzer. By default count is shortCount in -short mode and longCount otherwise,
// starting at 0. Both are overridable for long ad-hoc fuzzing runs:
//
//	BATON_EXPAND_FUZZ_SEEDS        number of seeds to sweep (count)
//	BATON_EXPAND_FUZZ_SEED_OFFSET  first seed (start), to resume/shard a sweep
func fuzzSeedRange(shortCount, longCount int) (int64, int64) {
	count := int64(longCount)
	if testing.Short() {
		count = int64(shortCount)
	}
	if v := os.Getenv("BATON_EXPAND_FUZZ_SEEDS"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			count = n
		}
	}
	var start int64
	if v := os.Getenv("BATON_EXPAND_FUZZ_SEED_OFFSET"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			start = n
		}
	}
	return start, count
}

func makeResourceWithDisplayName(resourceType, resource, displayName string) *v2.Resource {
	r := makeResource(resourceType, resource)
	r.SetDisplayName(displayName)
	return r
}

// synthesizedPrincipalDisplayName returns the display name of the grant for the
// given principal on the given entitlement in a mock store. It reads the last
// write for that (entitlement, principal) so it reflects the expander's output.
func synthesizedPrincipalDisplayName(store *MockExpanderStore, entitlementID, rt, id string) (string, bool) {
	var found *v2.Grant
	for _, grant := range store.grants[entitlementID] {
		pid := grant.GetPrincipal().GetId()
		if pid.GetResourceType() == rt && pid.GetResource() == id {
			found = grant
		}
	}
	if found == nil {
		return "", false
	}
	return found.GetPrincipal().GetDisplayName(), true
}

// recordingExpanderStore wraps an ExpanderStore and records every grant handed
// to StoreExpandedGrants, so a test can assert which destinations were (and were
// not) rewritten. It forwards the principal-sort capability so the streaming and
// projection algorithms still take their true Pebble paths.
type recordingExpanderStore struct {
	inner  ExpanderStore
	stored []*v2.Grant
}

func (s *recordingExpanderStore) GetEntitlement(
	ctx context.Context,
	req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest,
) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	return s.inner.GetEntitlement(ctx, req)
}

func (s *recordingExpanderStore) ListGrantsForEntitlement(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	return s.inner.ListGrantsForEntitlement(ctx, req)
}

func (s *recordingExpanderStore) StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	s.stored = append(s.stored, grants...)
	return s.inner.StoreExpandedGrants(ctx, grants...)
}

func (s *recordingExpanderStore) GrantsForEntitlementPrincipalSorted() bool {
	return s.inner.GrantsForEntitlementPrincipalSorted()
}

func (s *recordingExpanderStore) storedIDs() []string {
	ids := make([]string, 0, len(s.stored))
	for _, grant := range s.stored {
		ids = append(ids, grant.GetId())
	}
	sort.Strings(ids)
	return ids
}
