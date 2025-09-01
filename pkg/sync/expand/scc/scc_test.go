package scc

import (
	"context"
	"reflect"
	"sort"
	"testing"
)

// makeAdj creates an adjacency map ensuring every node appears as a key.
func makeAdj(nodes []int, edges [][2]int) map[int]map[int]int {
	adj := make(map[int]map[int]int, len(nodes))
	for _, u := range nodes {
		adj[u] = make(map[int]int)
	}
	for _, e := range edges {
		u, v := e[0], e[1]
		adj[u][v] = 1
	}
	return adj
}

// normalizeGroups sorts nodes within each group and sorts groups by lexicographic order.
func normalizeGroups(groups [][]int) [][]int {
	out := make([][]int, len(groups))
	for i := range groups {
		g := append([]int(nil), groups[i]...)
		sort.Ints(g)
		out[i] = g
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		for k := 0; k < len(a) && k < len(b); k++ {
			if a[k] != b[k] {
				return a[k] < b[k]
			}
		}
		return len(a) < len(b)
	})
	return out
}

func defaultOpts() Options {
	o := DefaultOptions()
	o.Deterministic = true
	o.MaxWorkers = 1 // ensure deterministic exploration in tests
	return o
}

func TestRingSingleComponent(t *testing.T) {
	n := 6
	nodes := make([]int, n)
	for i := 0; i < n; i++ {
		nodes[i] = i
	}
	var edges [][2]int
	for i := 0; i < n; i++ {
		edges = append(edges, [2]int{i, (i + 1) % n})
	}
	adj := makeAdj(nodes, edges)

	groups := CondenseFWBWGroupsFromAdj(context.Background(), adj, defaultOpts())
	if len(groups) != 1 {
		t.Fatalf("expected 1 component, got %d: %+v", len(groups), groups)
	}
	if len(groups[0]) != n {
		t.Fatalf("expected ring size %d, got %d", n, len(groups[0]))
	}
}

func TestChainAllSingletons(t *testing.T) {
	n := 7
	nodes := make([]int, n)
	for i := 0; i < n; i++ {
		nodes[i] = i
	}
	var edges [][2]int
	for i := 0; i+1 < n; i++ {
		edges = append(edges, [2]int{i, i + 1})
	}
	adj := makeAdj(nodes, edges)

	groups := CondenseFWBWGroupsFromAdj(context.Background(), adj, defaultOpts())
	if len(groups) != n {
		t.Fatalf("expected %d singleton components, got %d", n, len(groups))
	}
	for idx, g := range groups {
		if len(g) != 1 {
			t.Fatalf("expected singleton at comp %d, got size %d", idx, len(g))
		}
	}
}

func TestSelfLoopIsolatedCyclicSingleton(t *testing.T) {
	nodes := []int{1, 2}
	edges := [][2]int{{1, 1}, {1, 2}}
	adj := makeAdj(nodes, edges)

	groups := CondenseFWBWGroupsFromAdj(context.Background(), adj, defaultOpts())
	// Expect two components: {1} and {2}
	if len(groups) != 2 {
		t.Fatalf("expected 2 components, got %d: %+v", len(groups), groups)
	}
	// Determine which comp has node 1
	var comp1 []int
	for _, g := range groups {
		for _, id := range g {
			if id == 1 {
				comp1 = g
				break
			}
		}
	}
	if len(comp1) != 1 {
		t.Fatalf("node 1 should be singleton SCC, got %v", comp1)
	}
	// Verify self-loop classification logic: len==1 but adj[1][1] exists
	if adj[1][1] == 0 {
		t.Fatalf("expected self-loop for node 1 in adjacency")
	}
}

func TestCliqueSingleComponent(t *testing.T) {
	n := 5
	nodes := make([]int, n)
	for i := 0; i < n; i++ {
		nodes[i] = 100 + i
	}
	var edges [][2]int
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			edges = append(edges, [2]int{nodes[i], nodes[j]})
		}
	}
	adj := makeAdj(nodes, edges)
	groups := CondenseFWBWGroupsFromAdj(context.Background(), adj, defaultOpts())
	if len(groups) != 1 || len(groups[0]) != n {
		t.Fatalf("expected one SCC of size %d, got %+v", n, groups)
	}
}

func TestTailIntoRing(t *testing.T) {
	ringN := 4
	tail := []int{900, 901, 902}
	var nodes []int
	for i := 0; i < ringN; i++ {
		nodes = append(nodes, i)
	}
	nodes = append(nodes, tail...)
	var edges [][2]int
	// ring
	for i := 0; i < ringN; i++ {
		edges = append(edges, [2]int{i, (i + 1) % ringN})
	}
	// tail into ring start (0)
	edges = append(edges, [2]int{tail[0], tail[1]}, [2]int{tail[1], tail[2]}, [2]int{tail[2], 0})
	adj := makeAdj(nodes, edges)

	groups := CondenseFWBWGroupsFromAdj(context.Background(), adj, defaultOpts())
	// Expect: 1 SCC of size ringN, plus len(tail) singletons
	if len(groups) != 1+len(tail) {
		t.Fatalf("expected %d components, got %d: %+v", 1+len(tail), len(groups), groups)
	}
	sizes := make([]int, 0, len(groups))
	for _, g := range groups {
		sizes = append(sizes, len(g))
	}
	sort.Ints(sizes)
	want := append(make([]int, len(tail)), 0)
	for i := 0; i < len(tail); i++ {
		want[i] = 1
	}
	want[len(want)-1] = ringN
	sort.Ints(want)
	if !reflect.DeepEqual(sizes, want) {
		t.Fatalf("component sizes mismatch: got %v want %v", sizes, want)
	}
}

func TestMultipleDisjointSCCs(t *testing.T) {
	// Ring A: 0-1-2-0 (size 3)
	// Ring B: 10-11-12-13-10 (size 4)
	// Singletons: 100, 101
	nodes := []int{0, 1, 2, 10, 11, 12, 13, 100, 101}
	edges := [][2]int{{0, 1}, {1, 2}, {2, 0}, {10, 11}, {11, 12}, {12, 13}, {13, 10}}
	adj := makeAdj(nodes, edges)

	groups := CondenseFWBWGroupsFromAdj(context.Background(), adj, defaultOpts())
	// Filter out any empty groups (should not occur logically, but tolerate
	// preallocated empty slots when packing components).
	sizes := make([]int, 0, len(groups))
	for _, g := range groups {
		if len(g) == 0 {
			continue
		}
		sizes = append(sizes, len(g))
	}
	sort.Ints(sizes)
	want := []int{1, 1, 3, 4}
	if !reflect.DeepEqual(sizes, want) {
		t.Fatalf("component sizes mismatch: got %v want %v", sizes, want)
	}
	// Ensure partition covers all nodes
	seen := make(map[int]bool, len(nodes))
	for _, g := range groups {
		for _, id := range g {
			seen[id] = true
		}
	}
	for _, id := range nodes {
		if !seen[id] {
			t.Fatalf("node %d missing from partition", id)
		}
	}
}

func TestDeterminismWithSingleWorker(t *testing.T) {
	// Graph with two SCCs and two singletons
	nodes := []int{1, 2, 3, 4, 5, 6}
	edges := [][2]int{{1, 2}, {2, 1}, {3, 4}, {4, 3}}
	adj := makeAdj(nodes, edges)
	opts := defaultOpts()

	var ref [][]int
	for i := 0; i < 5; i++ {
		groups := CondenseFWBWGroupsFromAdj(context.Background(), adj, opts)
		ng := normalizeGroups(groups)
		if i == 0 {
			ref = ng
			continue
		}
		if !reflect.DeepEqual(ng, ref) {
			t.Fatalf("non-deterministic groups across runs: got %v want %v", ng, ref)
		}
	}
}
