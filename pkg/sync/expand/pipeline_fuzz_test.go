package expand

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// Whole-pipeline differential fuzz: c1z against c1z
// ----------------------------------------------------------------------------

// TestFullPipelineDifferentialFuzz is the end-to-end companion to the
// expansion-only differential fuzzers: for each generated case it builds a
// COMPLETE c1z artifact on each engine through the full production pipeline —
// seed data → expansion via the production evaluator selection (Run, which
// routes Pebble through the topological projection + layer sessions and
// SQLite through the source-batched expander) → EndSync (deferred by_principal
// rebuild, stats sidecar, compaction pause) → save → reopen read-only — and
// then compares the two ARTIFACTS through every reader surface: global grant
// listings, per-entitlement listings, per-principal listings (Pebble's
// deferred index vs SQLite's query), entitlement/resource listings, point
// gets by bare id against the saved file, and the stats sidecar. Both files
// are then thrown away.
//
// The expansion-only fuzzers pin algorithm parity in-process; this one pins
// everything wrapped around the algorithms: translate round-trips, the layer
// ingest path, the deferred index build, the id-index format stamp, the
// envelope save/reopen, and the bare-id lookup edge on a real reopened file.
func TestFullPipelineDifferentialFuzz(t *testing.T) {
	// Two full c1z lifecycles per seed (~0.7s); the expansion-only fuzzers
	// are the wide net, this is the whole-pipeline depth check. Seed count
	// comes from fuzzSeedRange (-short/BATON_EXPAND_FUZZ_SEEDS/_SEED_OFFSET);
	// BATON_EXPAND_FUZZ_DURATION (e.g. "30m") switches to a time-bound soak
	// that keeps walking seeds until the deadline, logging progress
	// periodically (visible with `go test -v`).
	start, count := fuzzSeedRange(6, 20)
	var deadline time.Time
	if v := os.Getenv("BATON_EXPAND_FUZZ_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		require.NoErrorf(t, err, "BATON_EXPAND_FUZZ_DURATION=%q", v)
		deadline = time.Now().Add(d)
		count = 1 << 40 // deadline-bound, not seed-bound
	}
	began := time.Now()
	lastLog := began
	var done int64
	for seed := start; seed < start+count; seed++ {
		if !deadline.IsZero() && time.Now().After(deadline) {
			break
		}
		seed := seed
		tc := pipelineFuzzCase(seed)
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			sqliteArt := buildPipelineArtifact(t, ctx, tc, c1zstore.EngineSQLite)
			pebbleArt := buildPipelineArtifact(t, ctx, tc, c1zstore.EnginePebble)
			comparePipelineArtifacts(t, tc, sqliteArt, pebbleArt)
		})
		done++
		if time.Since(lastLog) >= 30*time.Second {
			fmt.Fprintf(os.Stderr, "pipeline fuzz: %d seeds done (last seed=%d), %s elapsed\n",
				done, seed, time.Since(began).Round(time.Second))
			lastLog = time.Now()
		}
	}
	fmt.Fprintf(os.Stderr, "pipeline fuzz: finished — %d seeds in %s\n", done, time.Since(began).Round(time.Second))
}

// pipelineFuzzCase derives a whole-pipeline case from the shared generator,
// rewriting half the entitlement ids on even seeds into the SDK-shaped
// "group:org:<name>" form. The shared generator's "ent:NN" ids exercise
// Pebble's opaque identity encoding; the prefixed ids exercise the
// byte-prefix-stripped encoding — the artifact comparison must be blind to
// which one a given entitlement landed on.
func pipelineFuzzCase(seed int64) sqliteParityCase {
	tc := randomExpansionCase(seed)
	if seed%2 != 0 {
		return tc
	}
	rewrite := map[string]string{}
	for i, id := range tc.entitlementIDs {
		if i%2 == 0 {
			rewrite[id] = "group:org:" + strings.ReplaceAll(id, ":", "-")
		}
	}
	mapped := func(id string) string {
		if to, ok := rewrite[id]; ok {
			return to
		}
		return id
	}
	for i := range tc.entitlementIDs {
		tc.entitlementIDs[i] = mapped(tc.entitlementIDs[i])
	}
	for i := range tc.edges {
		tc.edges[i].src = mapped(tc.edges[i].src)
		tc.edges[i].dst = mapped(tc.edges[i].dst)
	}
	for i := range tc.grants {
		tc.grants[i].entitlementID = mapped(tc.grants[i].entitlementID)
	}
	return tc
}

// pipelineArtifact is everything read back out of one saved-and-reopened c1z.
type pipelineArtifact struct {
	engine c1zstore.Engine
	// grants is the global listing snapshot, keyed by (ent, principal).
	grants map[string]storeGrantSnapshot
	// byEntitlement / byPrincipal map an entitlement id / principal key to
	// the sorted grant keys its reader listing returned.
	byEntitlement map[string][]string
	byPrincipal   map[string][]string
	// entitlements / resources are the sorted id listings.
	entitlements []string
	resources    []string
	// pointGetGrantIDs are the grant ids GetGrant resolved on this artifact.
	pointGetGrantIDs []string
	// stats are the sidecar/synthetic counts for the sync.
	stats map[string]int64
}

// buildPipelineArtifact runs the full pipeline for one engine and returns the
// artifact snapshot. The c1z lives in the test's temp dir and is discarded
// when the test ends.
func buildPipelineArtifact(t *testing.T, ctx context.Context, tc sqliteParityCase, engine c1zstore.Engine) *pipelineArtifact {
	t.Helper()
	path := filepath.Join(t.TempDir(), fmt.Sprintf("pipeline-%s.c1z", engine))

	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	seedSQLiteBaseData(t, ctx, store, tc)

	// Production path selection: Run consults
	// GrantsForEntitlementPrincipalSorted, so Pebble takes the topological
	// projection (with layer sessions) and SQLite takes the source-batched
	// expander — exactly what a real sync does on each engine.
	graph := buildGraphFromCase(t, ctx, tc, engine)
	require.NoError(t, NewExpander(benchmarkExpanderStore{store: store}, graph).Run(ctx))

	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	ro, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, ro.Close(ctx)) }()
	require.NoError(t, ro.SetCurrentSync(ctx, syncID))

	art := &pipelineArtifact{
		engine:        engine,
		grants:        readBackGrantSnapshot(t, ctx, ro),
		byEntitlement: map[string][]string{},
		byPrincipal:   map[string][]string{},
	}

	// Entitlement listing, ids verbatim.
	pageToken := ""
	for {
		resp, err := ro.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
			PageSize: 100, PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		for _, e := range resp.GetList() {
			art.entitlements = append(art.entitlements, e.GetId())
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	sort.Strings(art.entitlements)

	// Resource listing (per resource type, since ListResources filters).
	rtSeen := map[string]struct{}{"group": {}}
	for _, g := range tc.grants {
		rtSeen[g.principalRT] = struct{}{}
	}
	for rt := range rtSeen {
		pageToken = ""
		for {
			resp, err := ro.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
				ResourceTypeId: rt, PageSize: 100, PageToken: pageToken,
			}.Build())
			require.NoError(t, err)
			for _, r := range resp.GetList() {
				art.resources = append(art.resources, r.GetId().GetResourceType()+"/"+r.GetId().GetResource())
			}
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
	}
	sort.Strings(art.resources)

	// Per-entitlement listings through the reader API, with the full stub
	// (resource refs) the way production callers address entitlements.
	group := makeResource("group", "org")
	for _, entID := range tc.entitlementIDs {
		stub := makeEntitlement(entID, group)
		keys := []string{}
		pageToken = ""
		for {
			resp, err := ro.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
				Entitlement: stub, PageSize: 7, PageToken: pageToken,
			}.Build())
			require.NoError(t, err)
			for _, g := range resp.GetList() {
				keys = append(keys, pipelineGrantKey(g))
			}
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
		sort.Strings(keys)
		art.byEntitlement[entID] = keys
	}

	// Per-principal listings: on Pebble this reads the by_principal index the
	// deferred EndSync build produced; on SQLite it is a query. Divergence
	// here means the deferred rebuild missed or invented rows.
	principals := map[string]*v2.ResourceId{}
	for _, g := range tc.grants {
		principals[g.principalRT+"/"+g.principalID] = v2.ResourceId_builder{
			ResourceType: g.principalRT, Resource: g.principalID,
		}.Build()
	}
	for key, pid := range principals {
		keys := []string{}
		pageToken = ""
		for {
			resp, err := ro.ListGrantsForPrincipal(ctx, reader_v2.GrantsReaderServiceListGrantsForPrincipalRequest_builder{
				PrincipalId: pid, PageSize: 7, PageToken: pageToken,
			}.Build())
			require.NoError(t, err)
			for _, g := range resp.GetList() {
				keys = append(keys, pipelineGrantKey(g))
			}
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
		sort.Strings(keys)
		art.byPrincipal[key] = keys
	}

	// Point gets against the saved artifact. Entitlements resolve by bare id
	// on both engines. Grants resolve by bare id on both engines too:
	// SDK-shaped ids through Pebble's combinatorial identity probe, and
	// connector-custom ids through its stored-external-id scan of last
	// resort (SQLite reader parity; the generator never reuses an id across
	// grants, so the scan's exactly-one rule always resolves).
	for _, entID := range tc.entitlementIDs {
		got, err := ro.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
			EntitlementId: entID,
		}.Build())
		require.NoErrorf(t, err, "%s: GetEntitlement(%q) on the saved artifact", engine, entID)
		require.Equal(t, entID, got.GetEntitlement().GetId(), "%s: GetEntitlement id verbatim", engine)
	}
	for _, snap := range art.grants {
		got, err := ro.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{GrantId: snap.id}.Build())
		require.NoErrorf(t, err, "%s: GetGrant(%q) on the saved artifact", engine, snap.id)
		require.Equalf(t, snap.id, got.GetGrant().GetId(), "%s: GetGrant id verbatim", engine)
		art.pointGetGrantIDs = append(art.pointGetGrantIDs, snap.id)
	}
	sort.Strings(art.pointGetGrantIDs)

	stats, err := ro.SyncMeta().Stats(ctx, connectorstore.SyncTypeAny, "")
	require.NoError(t, err)
	art.stats = stats
	return art
}

func pipelineGrantKey(g *v2.Grant) string {
	return g.GetEntitlement().GetId() + "\x00" +
		g.GetPrincipal().GetId().GetResourceType() + "\x00" +
		g.GetPrincipal().GetId().GetResource() + "\x00" + g.GetId()
}

// comparePipelineArtifacts asserts the two saved artifacts are equivalent
// through every reader surface. Grant ids are compared byte-for-byte:
// external ids are an external-consumer contract, so the two engines must
// emit identical strings for both stored and synthesized grants.
func comparePipelineArtifacts(t *testing.T, tc sqliteParityCase, ground, got *pipelineArtifact) {
	t.Helper()

	require.Equal(t, ground.entitlements, got.entitlements, "entitlement listings diverge")
	require.Equal(t, ground.resources, got.resources, "resource listings diverge")

	// Global grant sets, including byte-identical public ids.
	require.Equalf(t, len(ground.grants), len(got.grants), "grant count diverges (%s=%d, %s=%d)",
		ground.engine, len(ground.grants), got.engine, len(got.grants))
	for key, want := range ground.grants {
		g, ok := got.grants[key]
		require.Truef(t, ok, "grant %q present on %s, missing on %s", key, ground.engine, got.engine)
		require.Equalf(t, want.id, g.id, "grant %q: public id diverges across engines", key)
		require.Equalf(t, want.sourceDirect, g.sourceDirect, "grant %q: sources/directness diverge", key)
		require.Equalf(t, want.annotationTypes, g.annotationTypes, "grant %q: annotations diverge", key)
	}

	for _, entID := range tc.entitlementIDs {
		require.Equalf(t, ground.byEntitlement[entID], got.byEntitlement[entID],
			"per-entitlement listing diverges for %q", entID)
	}
	for key := range ground.byPrincipal {
		require.Equalf(t, ground.byPrincipal[key], got.byPrincipal[key],
			"per-principal listing diverges for %q (pebble side reads the deferred by_principal index)", key)
	}

	require.Equal(t, ground.pointGetGrantIDs, got.pointGetGrantIDs, "bare-id point-get coverage diverges")

	// Cross-engine stats: compare the keys both engines emit. SQLite's map
	// has no "resources" total (resources are keyed per resource type);
	// Pebble additionally emits a total — a map-shape difference, not data.
	statKeys := []string{"grants", "entitlements", "resource_types"}
	rtSeen := map[string]struct{}{"group": {}}
	for _, g := range tc.grants {
		rtSeen[g.principalRT] = struct{}{}
	}
	for rt := range rtSeen {
		statKeys = append(statKeys, rt)
	}
	for _, k := range statKeys {
		require.Equalf(t, ground.stats[k], got.stats[k], "stats[%s] diverges (%s=%d, %s=%d)",
			k, ground.engine, ground.stats[k], got.engine, got.stats[k])
	}
	require.Equal(t, int64(len(ground.grants)), ground.stats["grants"], "%s: stats disagree with the grant listing", ground.engine)
	require.Equal(t, int64(len(got.grants)), got.stats["grants"], "%s: stats disagree with the grant listing", got.engine)
}
