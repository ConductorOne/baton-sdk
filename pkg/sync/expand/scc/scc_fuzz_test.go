package scc

import (
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func clamp(x, lo, hi int) int {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}

func equalGroups(a, b [][]int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !reflect.DeepEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

// assertPartition ensures every key in adj appears in exactly one group; no duplicates.
func assertPartition(t *testing.T, adj map[int]map[int]int, groups [][]int) {
	t.Helper()
	seen := make(map[int]int, len(adj))
	for gid, g := range groups {
		for _, id := range g {
			if _, ok := seen[id]; ok {
				t.Fatalf("node %d appears in multiple groups", id)
			}
			seen[id] = gid
		}
	}
	for u := range adj {
		if _, ok := seen[u]; !ok {
			t.Fatalf("node %d missing from partition", u)
		}
	}
}

// assertDAGCondensation builds component meta-graph and checks it is acyclic.
func assertDAGCondensation(t *testing.T, adj map[int]map[int]int, groups [][]int) {
	t.Helper()
	idToComp := make(map[int]int, len(adj))
	for cid, g := range groups {
		for _, id := range g {
			idToComp[id] = cid
		}
	}
	compAdj := make(map[int]map[int]struct{}, len(groups))
	for u := range groups {
		compAdj[u] = make(map[int]struct{})
	}
	for u, nbrs := range adj {
		cu := idToComp[u]
		for v := range nbrs {
			cv := idToComp[v]
			if cu == cv {
				continue
			}
			compAdj[cu][cv] = struct{}{}
		}
	}
	indeg := make([]int, len(groups))
	for u := range compAdj {
		for v := range compAdj[u] {
			indeg[v]++
		}
	}
	q := make([]int, 0, len(groups))
	for u := 0; u < len(groups); u++ {
		if indeg[u] == 0 {
			q = append(q, u)
		}
	}
	visited := 0
	for len(q) > 0 {
		u := q[0]
		q = q[1:]
		visited++
		for v := range compAdj[u] {
			indeg[v]--
			if indeg[v] == 0 {
				q = append(q, v)
			}
		}
	}
	if visited != len(groups) {
		t.Fatalf("component condensation has a cycle: visited=%d total=%d", visited, len(groups))
	}
}

// generateAdjacency creates a bounded graph according to mode; returns map[int]map[int]int with all nodes as keys.
func generateAdjacency(numNodes, edgeBudget, mode int, r *rand.Rand, selfLoopFrac, bidirFrac int) map[int]map[int]int {
	if numNodes <= 0 {
		numNodes = 1
	}
	adj := make(map[int]map[int]int, numNodes)
	for i := 0; i < numNodes; i++ {
		adj[i] = make(map[int]int)
	}

	addEdge := func(u, v int) {
		if u < 0 || u >= numNodes || v < 0 || v >= numNodes {
			return
		}
		if adj[u] == nil {
			adj[u] = make(map[int]int)
		}
		adj[u][v] = 1
	}

	edgesAdded := 0
	budget := edgeBudget
	maxBudget := numNodes * numNodes
	if budget > maxBudget {
		budget = maxBudget
	}

	switch mode % 8 {
	case 0: // random directed
		for edgesAdded < budget {
			u := r.Intn(numNodes)
			v := r.Intn(numNodes)
			addEdge(u, v)
			edgesAdded++
			if r.Intn(256) < bidirFrac {
				addEdge(v, u)
			}
			if r.Intn(256) < selfLoopFrac {
				addEdge(u, u)
			}
		}
	case 1: // many disjoint 2-cycles + isolates
		for i := 0; i+1 < numNodes && edgesAdded+2 <= budget; i += 2 {
			addEdge(i, i+1)
			addEdge(i+1, i)
			edgesAdded += 2
		}
	case 2: // lollipop: clique K_m + tail T
		m := int(math.Sqrt(float64(numNodes)))
		if m < 2 {
			m = 2
		}
		if m > numNodes {
			m = numNodes
		}
		T := numNodes - m
		for i := 0; i < m; i++ {
			for j := 0; j < m; j++ {
				if i == j || edgesAdded >= budget {
					continue
				}
				addEdge(i, j)
				edgesAdded++
			}
		}
		if T > 0 {
			addEdge(m-1, m)
			edgesAdded++
			for i := m; i+1 < numNodes && edgesAdded < budget; i++ {
				addEdge(i, i+1)
				edgesAdded++
			}
		}
	case 3: // bipartite; optionally bidirectional
		a := numNodes / 2
		if a == 0 {
			a = 1
		}
		for i := 0; i < a; i++ {
			for j := a; j < numNodes && edgesAdded < budget; j++ {
				addEdge(i, j)
				edgesAdded++
				if r.Intn(256) < bidirFrac && edgesAdded < budget {
					addEdge(j, i)
					edgesAdded++
				}
			}
		}
	case 4: // multi-ring stitched by tails
		start := 0
		for start < numNodes && edgesAdded < budget {
			size := 3 + r.Intn(5)
			if start+size > numNodes {
				size = numNodes - start
			}
			if size >= 2 {
				for i := 0; i < size; i++ {
					u := start + i
					v := start + ((i + 1) % size)
					addEdge(u, v)
					edgesAdded++
					if edgesAdded >= budget {
						break
					}
				}
			}
			// one-way tail to next block
			next := start + size
			if next < numNodes && edgesAdded < budget {
				addEdge(start+size-1, next)
				edgesAdded++
			}
			start += size
		}
	case 5: // star hub asymmetry
		hub := r.Intn(numNodes)
		for i := 0; i < numNodes && edgesAdded < budget; i++ {
			if i == hub {
				continue
			}
			addEdge(hub, i)
			edgesAdded++
			if r.Intn(256) < bidirFrac && edgesAdded < budget {
				addEdge(i, hub)
				edgesAdded++
			}
		}
	case 6: // skewed external IDs (still using 0..N-1 as keys here; CSR handles mapping)
		for edgesAdded < budget {
			u := r.Intn(numNodes)
			v := (r.Intn(numNodes) * 13) % numNodes
			addEdge(u, v)
			edgesAdded++
		}
	case 7: // layered DAG with sparse backedges
		layers := 1 + r.Intn(8)
		per := (numNodes + layers - 1) / layers
		// forward edges between layers
		for L := 0; L+1 < layers && edgesAdded < budget; L++ {
			aStart := L * per
			aEnd := (L + 1) * per
			if aEnd > numNodes {
				aEnd = numNodes
			}
			bStart := (L + 1) * per
			bEnd := (L + 2) * per
			if bEnd > numNodes {
				bEnd = numNodes
			}
			for u := aStart; u < aEnd && edgesAdded < budget; u++ {
				for v := bStart; v < bEnd && edgesAdded < budget; v++ {
					if r.Intn(3) == 0 { // sparsify
						addEdge(u, v)
						edgesAdded++
					}
				}
			}
		}
		// sparse backedges inside a layer
		for L := 0; L < layers && edgesAdded < budget; L++ {
			s := L * per
			e := (L + 1) * per
			if e > numNodes {
				e = numNodes
			}
			for u := s; u < e; u++ {
				bound := e - s
				if bound < 1 {
					bound = 1
				}
				if r.Intn(10) == 0 && edgesAdded < budget {
					v := s + r.Intn(bound)
					addEdge(u, v)
					edgesAdded++
				}
			}
		}
	}

	// occasional self-loops
	if selfLoopFrac > 0 {
		for i := 0; i < numNodes; i++ {
			if r.Intn(256) < selfLoopFrac {
				addEdge(i, i)
			}
		}
	}
	return adj
}

// Fuzzers

// Cancellation fuzzer: short deadline; only assert return (no structural checks).
func FuzzCondenseFWBW_Cancellation(f *testing.F) {
	f.Add(512, uint64(4), 2048, uint8(2), uint8(8), uint8(0))
	f.Fuzz(func(t *testing.T, numNodes int, seed uint64, edgeBudget int, mode uint8, selfLoopFrac uint8, bidirFrac uint8) {
		numNodes = clamp(numNodes, 1, 1000)
		if edgeBudget > 100000 {
			edgeBudget = 100000
		}
		maxEdges := numNodes * numNodes
		if edgeBudget > maxEdges {
			edgeBudget = maxEdges
		}
		r := rand.New(rand.NewSource(int64(seed))) //nolint:gosec // math/rand is acceptable for fuzzing/tests
		adj := generateAdjacency(numNodes, edgeBudget, int(mode%8), r, int(selfLoopFrac), int(bidirFrac))
		opts := DefaultOptions()
		opts.MaxWorkers = 1
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		_ = CondenseFWBW(ctx, adjSource{adj: adj}, opts)
	})
}

// --- Byte-based fuzzer ---

// decodeVarint decodes a little varint (LEB128-style) from data starting at *i.
// Returns value and whether decoding succeeded.
func decodeVarint(data []byte, i *int) (uint64, bool) {
	var x uint64
	var s uint
	for {
		if *i >= len(data) || s >= 64 {
			return 0, false
		}
		b := data[*i]
		*i++
		if b < 0x80 {
			x |= uint64(b) << s
			return x, true
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
}

// generateAdjFromBytes builds an adjacency from a byte stream with caps.
func generateAdjFromBytes(data []byte, maxN, maxM int) map[int]map[int]int {
	if len(data) == 0 {
		return map[int]map[int]int{0: {}}
	}
	i := 0
	n64, ok := decodeVarint(data, &i)
	if !ok {
		n64 = 1
	}
	m64, ok := decodeVarint(data, &i)
	if !ok {
		m64 = 0
	}
	// Helper utilities.
	var n int
	switch {
	case n64 < 1:
		n = 1
	case n64 > uint64(maxN): //nolint:gosec // maxN is a small, non-negative test bound
		n = maxN
	case n64 > uint64(^uint(0)>>1):
		n = maxN
	default:
		n = int(n64)
	}
	var m int
	switch {
	case m64 > uint64(maxM): //nolint:gosec // maxM is a non-negative test bound
		m = maxM
	case m64 > uint64(^uint(0)>>1):
		m = maxM
	default:
		m = int(m64)
	}
	adj := make(map[int]map[int]int, n)
	for v := 0; v < n; v++ {
		adj[v] = make(map[int]int)
	}
	// Edge pairs
	for e := 0; e < m && i < len(data); e++ {
		// If not enough bytes left, break
		if i+8 > len(data) {
			break
		}
		u := int(binary.LittleEndian.Uint32(data[i:])) % n
		i += 4
		v := int(binary.LittleEndian.Uint32(data[i:])) % n
		i += 4
		adj[u][v] = 1
	}
	// Optional flags for reverse edges and self-loops
	if i < len(data) {
		flags := data[i]
		// bit0: add reverse for all edges
		if flags&0x1 != 0 {
			for u, nbrs := range adj {
				for v := range nbrs {
					adj[v][u] = 1
				}
			}
		}
		// bit1: sprinkle self-loops for some nodes based on subsequent bytes
		if flags&0x2 != 0 {
			i++
			for v := 0; v < n && i < len(data); v++ {
				if data[i]%3 == 0 {
					adj[v][v] = 1
				}
				i++
			}
		}
	}
	return adj
}

func FuzzCondenseFWBW_FromBytes(f *testing.F) {
	// Seed with simple patterns
	f.Add([]byte{5, 10, 0, 0, 0, 0, 1, 0, 0, 0}) // n=5,m=10, one edge 0->1
	f.Add([]byte{10, 20})                        // small n,m with empty pairs

	f.Fuzz(func(t *testing.T, data []byte) {
		// Caps for CI-friendly fuzz
		const maxN = 1000
		const maxM = 100000
		adj := generateAdjFromBytes(data, maxN, maxM)
		opts := DefaultOptions()
		opts.Deterministic = true
		opts.MaxWorkers = 1
		groups := CondenseFWBW(context.Background(), adjSource{adj: adj}, opts)
		assertPartition(t, adj, groups)
		assertDAGCondensation(t, adj, groups)
		// idempotence in deterministic mode
		groups2 := CondenseFWBW(context.Background(), adjSource{adj: adj}, opts)
		if !equalGroups(normalizeGroups(groups), normalizeGroups(groups2)) {
			t.Fatalf("non-deterministic result with Deterministic=true")
		}
	})
}
