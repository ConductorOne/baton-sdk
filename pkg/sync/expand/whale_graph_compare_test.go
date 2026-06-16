package expand

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

// canonicalGraph reduces an EntitlementGraph to engine-independent,
// node-id-independent sets so two graphs built from different storage
// backends can be compared by entitlement identity rather than by the
// insertion-order node IDs each backend happens to assign.
type canonicalGraph struct {
	entitlements map[string]struct{}
	edges        map[string]struct{} // "src\x00dst|shallow|rtids"
}

func canonicalize(g *EntitlementGraph) canonicalGraph {
	out := canonicalGraph{
		entitlements: make(map[string]struct{}, len(g.Nodes)),
		edges:        make(map[string]struct{}, len(g.Edges)),
	}
	nodeEnt := make(map[int]string, len(g.Nodes))
	for id, n := range g.Nodes {
		// Pre-cycle-fix load: exactly one entitlement per node.
		ents := append([]string(nil), n.EntitlementIDs...)
		sort.Strings(ents)
		for _, e := range ents {
			out.entitlements[e] = struct{}{}
		}
		if len(ents) > 0 {
			nodeEnt[id] = strings.Join(ents, ",")
		}
	}
	for _, e := range g.Edges {
		rt := append([]string(nil), e.ResourceTypeIDs...)
		sort.Strings(rt)
		key := fmt.Sprintf("%s\x00%s|shallow=%t|rt=%s",
			nodeEnt[e.SourceID], nodeEnt[e.DestinationID], e.IsShallow, strings.Join(rt, ","))
		out.edges[key] = struct{}{}
	}
	return out
}

func loadSQLGraph(ctx context.Context, t *testing.T, path string) *EntitlementGraph {
	t.Helper()
	c1f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c1f.Close(ctx) })

	latest, err := c1f.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, latest, "no full sync in SQL c1z")
	t.Logf("SQL latest full sync: %s", latest.ID)
	require.NoError(t, c1f.SetSyncID(ctx, latest.ID))

	g, err := loadEntitlementGraphFromStore(ctx, c1f)
	require.NoError(t, err)
	return g
}

func loadPebbleGraph(ctx context.Context, t *testing.T, path string) *EntitlementGraph {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	latest, err := store.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, latest, "no full sync in pebble c1z")
	t.Logf("pebble latest full sync: %s", latest.ID)
	_, _, err = store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, latest.ID)
	require.NoError(t, err)

	g, err := loadEntitlementGraphFromStore(ctx, store)
	require.NoError(t, err)
	return g
}

func sampleSet(s map[string]struct{}, n int) []string {
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > n {
		keys = keys[:n]
	}
	for i, k := range keys {
		keys[i] = strings.ReplaceAll(k, "\x00", " -> ")
	}
	return keys
}

// TestCompareWhaleGraphs loads the expansion graph from both the SQL and
// pebble copies of the same sync and diffs them by entitlement identity.
// Set BATON_SQL_PATH and BATON_PEBBLE_PATH to run.
func TestCompareWhaleGraphs(t *testing.T) {
	sqlPath := os.Getenv("BATON_SQL_PATH")
	pebblePath := os.Getenv("BATON_PEBBLE_PATH")
	if sqlPath == "" || pebblePath == "" {
		t.Skip("set BATON_SQL_PATH and BATON_PEBBLE_PATH")
	}
	ctx := context.Background()

	sqlGraph := loadSQLGraph(ctx, t, sqlPath)
	pebbleGraph := loadPebbleGraph(ctx, t, pebblePath)

	t.Logf("SQL graph:    %d nodes, %d edges", len(sqlGraph.Nodes), len(sqlGraph.Edges))
	t.Logf("pebble graph: %d nodes, %d edges", len(pebbleGraph.Nodes), len(pebbleGraph.Edges))

	sqlC := canonicalize(sqlGraph)
	pebbleC := canonicalize(pebbleGraph)

	entOnlySQL := diffSets(sqlC.entitlements, pebbleC.entitlements)
	entOnlyPebble := diffSets(pebbleC.entitlements, sqlC.entitlements)
	edgeOnlySQL := diffSets(sqlC.edges, pebbleC.edges)
	edgeOnlyPebble := diffSets(pebbleC.edges, sqlC.edges)

	t.Logf("entitlements: SQL=%d pebble=%d  only-in-SQL=%d only-in-pebble=%d",
		len(sqlC.entitlements), len(pebbleC.entitlements), len(entOnlySQL), len(entOnlyPebble))
	t.Logf("edges:        SQL=%d pebble=%d  only-in-SQL=%d only-in-pebble=%d",
		len(sqlC.edges), len(pebbleC.edges), len(edgeOnlySQL), len(edgeOnlyPebble))

	if len(entOnlySQL) > 0 {
		t.Logf("sample entitlements only in SQL:\n  %s", strings.Join(sampleSet(entOnlySQL, 20), "\n  "))
	}
	if len(entOnlyPebble) > 0 {
		t.Logf("sample entitlements only in pebble:\n  %s", strings.Join(sampleSet(entOnlyPebble, 20), "\n  "))
	}
	if len(edgeOnlySQL) > 0 {
		t.Logf("sample edges only in SQL:\n  %s", strings.Join(sampleSet(edgeOnlySQL, 20), "\n  "))
	}
	if len(edgeOnlyPebble) > 0 {
		t.Logf("sample edges only in pebble:\n  %s", strings.Join(sampleSet(edgeOnlyPebble, 20), "\n  "))
	}

	require.Empty(t, entOnlySQL, "entitlements present only in SQL graph")
	require.Empty(t, entOnlyPebble, "entitlements present only in pebble graph")
	require.Empty(t, edgeOnlySQL, "edges present only in SQL graph")
	require.Empty(t, edgeOnlyPebble, "edges present only in pebble graph")
}

func diffSets(a, b map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{})
	for k := range a {
		if _, ok := b[k]; !ok {
			out[k] = struct{}{}
		}
	}
	return out
}
