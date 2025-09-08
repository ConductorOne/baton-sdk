// Package scc provides an iterative FW–BW SCC condensation for directed graphs,
// adapted for Baton’s entitlement graph. It builds an immutable CSR + transpose,
// runs reachability-based SCC with a stack-based driver (no recursion, BFS may
// run in parallel), and returns components as groups of your original node IDs.
package scc

// Iterative FW–BW SCC condensation for directed graphs.
//
// High-level algorithm: Build CSR + transpose; maintain a LIFO stack of
// subproblems (bitset masks). For each mask:
//   1) Trim sources/sinks repeatedly; each peeled vertex is a singleton SCC.
//   2) Pick a pivot (lowest-index active vertex), run forward/backward BFS
//      restricted to the mask to get F and B.
//   3) The SCC is C = F ∩ B. Assign its component id and clear those bits
//      from the mask.
//   4) Partition the remaining mask into F\C, B\C, and U = mask \ (F ∪ B),
//      and push the non-empty masks onto the stack in a deterministic order.
//
// Parallelism is contained inside BFS (bounded by Options.MaxWorkers) and no
// recursive goroutines are spawned by the driver. Determinism is achieved via
// deterministic CSR construction (sorted ids and neighbors) and by using the
// lowest-index active pivot with a fixed child-push order.

import (
	"context"
	"runtime"
	"sort"
	"sync"
	"time"
)

// Options controls SCC execution.
//
// MaxWorkers bounds BFS concurrency per level. Deterministic toggles stable
// CSR index assignment and neighbor ordering.
type Options struct {
	MaxWorkers    int
	Deterministic bool
}

// DefaultOptions returns a sensible baseline.
func DefaultOptions() Options {
	return Options{
		MaxWorkers:    runtime.GOMAXPROCS(0),
		Deterministic: false,
	}
}

// CSR is a compact adjacency for G and its transpose Gᵗ.
// Indices are 0..N-1; IdxToNodeID maps back to the original node IDs.
//
// Invariants (validated by validateCSR):
//   - len(Row)  == N+1; Row[0] == 0; Row is non-decreasing; Row[N] == len(Col)
//   - len(TRow) == N+1; TRow[0] == 0; TRow is non-decreasing; TRow[N] == len(TCol)
//   - 0 <= Col[p]  < N for all p; 0 <= TCol[p] < N for all p
//   - len(IdxToNodeID) == N; NodeIDToIdx[IdxToNodeID[i]] == i for all i
//   - For each v, (TRow[v+1]-TRow[v]) equals the number of occurrences of v in Col
//     (transpose degree matches inbound counts)
type CSR struct {
	N           int
	Row         []int // len N+1
	Col         []int // len = m, m = Row[N]
	TRow        []int // len N+1
	TCol        []int // len = m, m = TRow[N]
	IdxToNodeID []int // len N
}

// Source is a minimal read-only graph provider used to build CSR without
// materializing an intermediate adjacency map. It must enumerate all nodes
// (including isolated nodes) and for each node provide its unique outgoing
// destinations.
type Source interface {
	ForEachNode(fn func(id int) bool)
	ForEachEdgeFrom(src int, fn func(dst int) bool)
}

// Metrics captures a few summary counters for a condense run.
type Metrics struct {
	Nodes          int
	Edges          int
	Components     int
	Peeled         int
	MasksProcessed int
	MasksPushed    int
	BFScalls       int
	Duration       time.Duration
	MaxWorkers     int
	Deterministic  bool
}

// CondenseFWBW runs SCC directly from a streaming Source. Preferred entry point.
func CondenseFWBW(ctx context.Context, src Source, opts Options) ([][]int, *Metrics) {
	if opts.MaxWorkers <= 0 {
		opts.MaxWorkers = runtime.GOMAXPROCS(0)
	}
	start := time.Now()
	csr := buildCSRFromSource(src, opts)
	metrics := Metrics{
		Nodes:         csr.N,
		Edges:         len(csr.Col),
		MaxWorkers:    opts.MaxWorkers,
		Deterministic: opts.Deterministic,
	}
	comp := make([]int, csr.N)
	for i := range comp {
		comp[i] = -1
	}
	nextID := sccFWBWIterative(ctx, csr, comp, opts, &metrics)

	groups := make([][]int, nextID)
	for idx := range csr.N {
		cid := comp[idx]
		if cid < 0 {
			cid = nextID
			nextID++
			comp[idx] = cid
			if cid >= len(groups) {
				tmp := make([][]int, cid+1)
				copy(tmp, groups)
				groups = tmp
			}
		}
		groups[cid] = append(groups[cid], csr.IdxToNodeID[idx])
	}
	metrics.Components = nextID
	metrics.Duration = time.Since(start)
	return groups, &metrics
}

// buildCSRFromSource constructs CSR/transpose from a Source without
// materializing an intermediate adjacency map. If opts.Deterministic, node IDs
// are sorted and per-row neighbors are written in ascending order.
func buildCSRFromSource(src Source, opts Options) *CSR {
	// 1) Collect nodes
	nodes := make([]int, 0, 1024)
	src.ForEachNode(func(id int) bool {
		nodes = append(nodes, id)
		return true
	})
	if opts.Deterministic {
		sort.Ints(nodes)
	}
	id2idx := make(map[int]int, len(nodes))
	for i, id := range nodes {
		id2idx[id] = i
	}
	n := len(nodes)

	// 2) Count out-degrees and total edges
	outDeg := make([]int, n)
	m := 0
	for i := range n {
		srcID := nodes[i]
		src.ForEachEdgeFrom(srcID, func(dst int) bool {
			j, ok := id2idx[dst]
			if !ok {
				return true
			}
			_ = j // only used to validate membership
			outDeg[i]++
			m++
			return true
		})
	}

	// 3) Allocate Row/Col
	row := make([]int, n+1)
	for i := range n {
		row[i+1] = row[i] + outDeg[i]
	}
	col := make([]int, m)
	cur := make([]int, n)
	copy(cur, row)

	// 4) Fill rows
	if opts.Deterministic {
		for i := range n {
			srcID := nodes[i]
			neighbors := make([]int, 0, outDeg[i])
			src.ForEachEdgeFrom(srcID, func(dst int) bool {
				if j, ok := id2idx[dst]; ok {
					neighbors = append(neighbors, j)
				}
				return true
			})
			sort.Ints(neighbors)
			off := cur[i]
			copy(col[off:off+len(neighbors)], neighbors)
			cur[i] += len(neighbors)
		}
	} else {
		for i := range n {
			srcID := nodes[i]
			src.ForEachEdgeFrom(srcID, func(dst int) bool {
				if j, ok := id2idx[dst]; ok {
					pos := cur[i]
					col[pos] = j
					cur[i] = pos + 1
				}
				return true
			})
		}
	}

	// 5) Transpose
	inDeg := make([]int, n)
	for _, v := range col {
		inDeg[v]++
	}
	trow := make([]int, n+1)
	for i := range n {
		trow[i+1] = trow[i] + inDeg[i]
	}
	tcol := make([]int, m)
	tcur := make([]int, n)
	copy(tcur, trow)
	for u := range n {
		start, end := row[u], row[u+1]
		for p := start; p < end; p++ {
			v := col[p]
			pos := tcur[v]
			tcol[pos] = u
			tcur[v] = pos + 1
		}
	}

	csr := &CSR{
		N:           n,
		Row:         row,
		Col:         col,
		TRow:        trow,
		TCol:        tcol,
		IdxToNodeID: nodes,
	}
	validateCSR(csr)
	return csr
}

// validateCSR performs internal consistency checks on CSR and panics
// with a descriptive message when a violation is found. This is intended to
// catch programmer errors at build time and in tests; it runs unconditionally.
func validateCSR(csr *CSR) {
	if csr == nil {
		panic("scc: CSR is nil")
	}
	n := csr.N
	if n < 0 {
		panic("scc: CSR.N is negative")
	}
	if len(csr.Row) != n+1 {
		panic("scc: len(Row) != N+1")
	}
	if len(csr.TRow) != n+1 {
		panic("scc: len(TRow) != N+1")
	}
	if len(csr.IdxToNodeID) != n {
		panic("scc: len(IdxToNodeID) != N")
	}
	// Row invariants and degree sums
	if csr.Row[0] != 0 {
		panic("scc: Row[0] != 0")
	}
	for i := range len(csr.Row) - 1 {
		if csr.Row[i] > csr.Row[i+1] {
			panic("scc: Row is not non-decreasing")
		}
	}
	m := csr.Row[n]
	if m != len(csr.Col) {
		panic("scc: Row[N] != len(Col)")
	}
	// TRow invariants
	if csr.TRow[0] != 0 {
		panic("scc: TRow[0] != 0")
	}
	for i := range len(csr.TRow) - 1 {
		if csr.TRow[i] > csr.TRow[i+1] {
			panic("scc: TRow is not non-decreasing")
		}
	}
	mt := csr.TRow[n]
	if mt != len(csr.TCol) {
		panic("scc: TRow[N] != len(TCol)")
	}
	// Col bounds
	for p := range len(csr.Col) {
		v := csr.Col[p]
		if v < 0 || v >= n {
			panic("scc: Col index out of range")
		}
	}
	for p := range len(csr.TCol) {
		v := csr.TCol[p]
		if v < 0 || v >= n {
			panic("scc: TCol index out of range")
		}
	}
	// NodeID mapping bijection check removed: CSR does not store NodeIDToIdx.
	// Transpose degree equals inbound counts
	inDeg := make([]int, n)
	for _, v := range csr.Col {
		inDeg[v]++
	}
	for v := range n {
		expected := inDeg[v]
		span := csr.TRow[v+1] - csr.TRow[v]
		if span != expected {
			panic("scc: transpose degree mismatch")
		}
	}
}

// bitset moved to bitset.go

// sccFWBWIterative implements the driver loop described at the top.
func sccFWBWIterative(ctx context.Context, csr *CSR, comp []int, opts Options, metrics *Metrics) int {
	nextID := 0

	// Initialize root mask with all vertices.
	root := newBitset(csr.N)
	for i := range csr.N {
		root.set(i)
	}

	type item struct{ mask *bitset }
	stack := make([]item, 0, 64)
	stack = append(stack, item{mask: root})

	for len(stack) > 0 {
		select {
		case <-ctx.Done():
			return nextID
		default:
		}

		it := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		active := it.mask
		if metrics != nil {
			metrics.MasksProcessed++
		}

		// Trim loop: peel sources/sinks; each peeled vertex becomes its own SCC.
		for {
			if n := trimSingletons(csr, active, comp, &nextID); n == 0 {
				break
			} else if metrics != nil {
				metrics.Peeled += n
			}
			if active.isEmpty() {
				break
			}
		}
		if active.isEmpty() {
			continue
		}

		// Pivot and BFS (restricted to active mask).
		pivot := firstActive(active)
		f := bfsMultiSource(ctx, csr, []int{pivot}, active, false, opts.MaxWorkers)
		b := bfsMultiSource(ctx, csr, []int{pivot}, active, true, opts.MaxWorkers)
		if metrics != nil {
			metrics.BFScalls += 2
		}

		// Component and partition masks.
		c := f.clone().and(b)
		assignComponent(c, comp, &nextID, active)

		fNotC := f.clone().andNot(c)
		bNotC := b.clone().andNot(c)
		fOrB := f.clone().or(b)
		u := active.clone().andNot(fOrB)

		// assignComponent cleared C from 'active'; child masks are disjoint subsets
		// of the original mask. Push children in a fixed order for determinism.
		pushes := 0
		if !u.isEmpty() {
			stack = append(stack, item{mask: u})
			pushes++
		}
		if !bNotC.isEmpty() {
			stack = append(stack, item{mask: bNotC})
			pushes++
		}
		if !fNotC.isEmpty() {
			stack = append(stack, item{mask: fNotC})
			pushes++
		}
		if metrics != nil {
			metrics.MasksPushed += pushes
		}
	}

	return nextID
}

// bfsMultiSource runs a parallel BFS from sources over csr.
// If useTranspose is true, traverses csr.T* arrays. Traversal respects
// 'active' mask; returns visited including sources.
// Cancellation: checks ctx between levels.
func bfsMultiSource(ctx context.Context, csr *CSR, sources []int, active *bitset, useTranspose bool, maxWorkers int) *bitset {
	if maxWorkers <= 0 {
		maxWorkers = runtime.GOMAXPROCS(0)
	}
	visited := newBitset(csr.N)
	frontier := frontierSeed(sources, active, visited)
	if len(frontier) == 0 {
		return visited
	}

	getRow := func(u int) (int, int) {
		if useTranspose {
			return csr.TRow[u], csr.TRow[u+1]
		}
		return csr.Row[u], csr.Row[u+1]
	}
	getCol := func(p int) int {
		if useTranspose {
			return csr.TCol[p]
		}
		return csr.Col[p]
	}

	for len(frontier) > 0 {
		select {
		case <-ctx.Done():
			return visited
		default:
		}

		workers := min(maxWorkers, len(frontier))
		var wg sync.WaitGroup
		wg.Add(workers)

		chunkSize := (len(frontier) + workers - 1) / workers
		nextBuckets := make([][]int, workers)

		for w := 0; w < workers; w++ {
			start := w * chunkSize
			end := start + chunkSize
			if start >= len(frontier) {
				wg.Done()
				continue
			}
			end = min(end, len(frontier))

			w := w // capture
			go func(start, end int) {
				defer wg.Done()
				local := make([]int, 0, 256)
				for i := start; i < end; i++ {
					u := frontier[i]
					rs, re := getRow(u)
					for p := rs; p < re; p++ {
						v := getCol(p)
						if !active.test(v) {
							continue
						}
						if !visited.testAndSetAtomic(v) {
							local = append(local, v)
						}
					}
				}
				nextBuckets[w] = local
			}(start, end)
		}
		wg.Wait()

		total := 0
		for _, b := range nextBuckets {
			total += len(b)
		}
		next := make([]int, total)
		off := 0
		for _, b := range nextBuckets {
			copy(next[off:], b)
			off += len(b)
		}
		frontier = next
	}

	return visited
}

func frontierSeed(sources []int, active, visited *bitset) []int {
	out := make([]int, 0, len(sources))
	for _, s := range sources {
		if s < 0 {
			continue
		}
		if !active.test(s) {
			continue
		}
		if visited.testAndSetAtomic(s) {
			continue
		}
		out = append(out, s)
	}
	return out
}

func firstActive(active *bitset) int {
	pivot := -1
	active.forEachSet(func(i int) {
		if pivot == -1 {
			pivot = i
		}
	})
	return pivot
}

// assignComponent writes comp ids for cMask, and clears those vertices from 'active'.
func assignComponent(cMask *bitset, comp []int, nextID *int, active *bitset) {
	if cMask.isEmpty() {
		return
	}
	cid := *nextID
	*nextID++
	cMask.forEachSet(func(i int) {
		comp[i] = cid
		active.clearAtomic(i)
	})
}

// Degree-array pool for trim to reduce allocations for small graphs.
var (
	intSlicePool sync.Pool // *([]int)
)

func getIntSlice(n int) []int {
	p, _ := intSlicePool.Get().(*[]int)
	if p == nil || cap(*p) < n {
		return make([]int, n)
	}
	s := (*p)[:n]
	for i := range s {
		s[i] = 0
	}
	return s
}

func putIntSlice(s []int) {
	// avoid keeping very large slices
	if cap(s) > 1<<14 {
		return
	}
	intSlicePool.Put(&s)
}

// trimSingletons peels vertices with restricted in/out degree within 'active'.
// Each peeled vertex becomes its own SCC id. Returns count peeled.
func trimSingletons(csr *CSR, active *bitset, comp []int, nextID *int) int {
	n := csr.N
	inDeg := getIntSlice(n)
	outDeg := getIntSlice(n)
	defer func() { putIntSlice(inDeg); putIntSlice(outDeg) }()

	// Out-degree within active.
	for u := range n {
		if !active.test(u) {
			continue
		}
		rs, re := csr.Row[u], csr.Row[u+1]
		d := 0
		for p := rs; p < re; p++ {
			v := csr.Col[p]
			if active.test(v) {
				d++
			}
		}
		outDeg[u] = d
	}
	// In-degree within active (via transpose).
	for v := range n {
		if !active.test(v) {
			continue
		}
		rs, re := csr.TRow[v], csr.TRow[v+1]
		d := 0
		for p := rs; p < re; p++ {
			u := csr.TCol[p]
			if active.test(u) {
				d++
			}
		}
		inDeg[v] = d
	}

	// Initialize queue of zeros.
	queue := getIntSlice(0)
	defer putIntSlice(queue)
	for i := range n {
		if !active.test(i) {
			continue
		}
		if inDeg[i] == 0 || outDeg[i] == 0 {
			queue = append(queue, i)
		}
	}

	peeled := 0
	for len(queue) > 0 {
		u := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if !active.test(u) {
			continue
		}
		if inDeg[u] > 0 && outDeg[u] > 0 {
			continue
		}

		// assign and remove u
		comp[u] = *nextID
		*nextID++
		active.clearAtomic(u)
		peeled++

		// Decrement out-neighbors' inDeg
		rs, re := csr.Row[u], csr.Row[u+1]
		for p := rs; p < re; p++ {
			v := csr.Col[p]
			if !active.test(v) {
				continue
			}
			if inDeg[v] > 0 {
				inDeg[v]--
				if inDeg[v] == 0 {
					queue = append(queue, v)
				}
			}
		}
		// Decrement in-neighbors' outDeg
		rs, re = csr.TRow[u], csr.TRow[u+1]
		for p := rs; p < re; p++ {
			w := csr.TCol[p]
			if !active.test(w) {
				continue
			}
			if outDeg[w] > 0 {
				outDeg[w]--
				if outDeg[w] == 0 {
					queue = append(queue, w)
				}
			}
		}
	}
	return peeled
}
