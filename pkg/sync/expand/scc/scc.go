// Package scc provides a parallel FW–BW SCC condensation for directed graphs,
// adapted for Baton’s entitlement graph. It builds an immutable CSR + transpose,
// runs reachability-based SCC (with parallel BFS and parallel recursion), and
// returns components as groups of your original node IDs.
package scc

import (
	"context"
	"math/bits"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
)

// Lightweight pools to reduce transient allocations in hot paths.
var (
	intSlicePool    sync.Pool // *([]int)
	bucketSlicePool sync.Pool // *([]int)
)

func getIntSlice(n int) []int {
	p, _ := intSlicePool.Get().(*[]int)
	if p == nil || cap(*p) < n {
		return make([]int, n)
	}
	s := (*p)[:n]
	return s
}

func putIntSlice(s []int) {
	intSlicePool.Put(&s)
}

func getBucketSlice() []int {
	p, _ := bucketSlicePool.Get().(*[]int)
	if p == nil {
		return make([]int, 0, 256)
	}
	return (*p)[:0]
}

func putBucketSlice(s []int) {
	bucketSlicePool.Put(&s)
}

// Options controls parallel SCC execution.
type Options struct {
	// MaxWorkers bounds concurrency for BFS/recursion. Defaults to GOMAXPROCS.
	MaxWorkers int
	// EnableTrim peels vertices with min(in,out)==0 before FW–BW (usually beneficial).
	EnableTrim bool
	// SmallCutoff size below which we switch to a simpler (sequential) routine.
	// You can later swap this to Tarjan for tiny subgraphs.
	SmallCutoff int
	// Deterministic maps node IDs to CSR indices in sorted order (recommended).
	Deterministic bool
}

// DefaultOptions returns a sensible baseline.
func DefaultOptions() Options {
	return Options{
		MaxWorkers:    runtime.GOMAXPROCS(0),
		EnableTrim:    false,
		SmallCutoff:   8192,
		Deterministic: false,
	}
}

// CSR is a compact adjacency for G and its transpose Gᵗ.
// Indices are 0..N-1; IdxToNodeID maps back to the original node IDs.
type CSR struct {
	N           int
	Row         []int // len N+1
	Col         []int // len = m
	TRow        []int // len N+1
	TCol        []int // len = m
	IdxToNodeID []int
	NodeIDToIdx map[int]int
}

// BuildCSRFromAdj constructs CSR/transpose from adjacency map[int]map[int]int.
// If opts.Deterministic, node IDs are sorted before assigning indices.
// Isolated nodes must appear as keys in adj (with empty inner map).
func BuildCSRFromAdj(adj map[int]map[int]int, opts Options) *CSR {
	nodes := make([]int, 0, len(adj)*2)
	seen := make(map[int]struct{}, len(adj)*2)
	for u, nbrs := range adj {
		if _, ok := seen[u]; !ok {
			seen[u] = struct{}{}
			nodes = append(nodes, u)
		}
		for v := range nbrs {
			if _, ok := seen[v]; !ok {
				seen[v] = struct{}{}
				nodes = append(nodes, v)
			}
		}
	}
	if opts.Deterministic {
		sort.Ints(nodes)
	}
	id2idx := make(map[int]int, len(nodes))
	for i, id := range nodes {
		id2idx[id] = i
	}
	n := len(nodes)

	// Count edges per row.
	outDeg := make([]int, n)
	m := 0
	for uID, nbrs := range adj {
		u := id2idx[uID]
		deg := len(nbrs)
		outDeg[u] += deg
		m += deg
	}

	row := make([]int, n+1)
	for i := 0; i < n; i++ {
		row[i+1] = row[i] + outDeg[i]
	}
	col := make([]int, m)

	// Fill CSR.
	cur := make([]int, n)
	copy(cur, row)
	for uID, nbrs := range adj {
		u := id2idx[uID]
		if opts.Deterministic {
			vs := make([]int, 0, len(nbrs))
			for vID := range nbrs {
				vs = append(vs, vID)
			}
			sort.Ints(vs)
			for _, vID := range vs {
				v := id2idx[vID]
				pos := cur[u]
				col[pos] = v
				cur[u]++
			}
		} else {
			for vID := range nbrs {
				v := id2idx[vID]
				pos := cur[u]
				col[pos] = v
				cur[u]++
			}
		}
	}

	// Transpose.
	inDeg := make([]int, n)
	for _, v := range col {
		inDeg[v]++
	}
	trow := make([]int, n+1)
	for i := 0; i < n; i++ {
		trow[i+1] = trow[i] + inDeg[i]
	}
	tcol := make([]int, m)
	tcur := make([]int, n)
	copy(tcur, trow)
	for u := 0; u < n; u++ {
		start, end := row[u], row[u+1]
		for p := start; p < end; p++ {
			v := col[p]
			pos := tcur[v]
			tcol[pos] = u
			tcur[v]++
		}
	}

	return &CSR{
		N:           n,
		Row:         row,
		Col:         col,
		TRow:        trow,
		TCol:        tcol,
		IdxToNodeID: nodes,
		NodeIDToIdx: id2idx,
	}
}

// bitset is a packed, atomically updatable bitset.
//
// Concurrency notes for callers:
//   - Only testAndSetAtomic and clearAtomic are safe for concurrent use on the
//     same bitset value. They perform CAS loops on 64-bit words.
//   - All other methods (test, set, clone, and, or, andNot, isEmpty, count,
//     forEachSet) are NOT safe to call concurrently with writers and must not
//     race with any mutation of the same bitset.
//   - Concurrent calls to test are fine only if no goroutine may write to that
//     bitset at the same time (including via atomic methods), otherwise it is a
//     data race by the Go memory model and race detector.
//   - Slice storage for []uint64 is 64-bit aligned, satisfying atomic.*Uint64
//     alignment requirements across architectures.
//
// Package usage:
//   - In BFS, multiple workers set bits in the per-search "visited" bitset via
//     testAndSetAtomic only; there are no concurrent non-atomic reads/writes.
//   - The shared "active" mask is only modified outside an in-flight BFS; BFS
//     reads it (test) while no goroutine mutates it, and recursive calls operate
//     on disjoint cloned masks.
type bitset struct{ w []uint64 }

func newBitset(n int) *bitset {
	if n <= 0 {
		return &bitset{}
	}
	return &bitset{w: make([]uint64, (n+63)>>6)}
}

// test reads a bit without synchronization. Do not call concurrently with any
// writer to the same bitset.
func (b *bitset) test(i int) bool {
	if i < 0 {
		return false
	}
	w := i >> 6
	return (b.w[w] & (1 << (uint(i) & 63))) != 0
}

// set writes a bit without synchronization. Not safe to race with other
// accesses (reads or writes) to the same bitset.
func (b *bitset) set(i int) {
	if i < 0 {
		return
	}
	w := i >> 6
	b.w[w] |= 1 << (uint(i) & 63)
}

// testAndSetAtomic atomically sets bit i and returns true if it was already
// set. Safe for concurrent use by multiple goroutines.
func (b *bitset) testAndSetAtomic(i int) bool {
	if i < 0 {
		return false
	}
	w := i >> 6
	mask := uint64(1) << (uint(i) & 63)
	addr := &b.w[w]
	for {
		old := atomic.LoadUint64(addr)
		if old&mask != 0 {
			return true
		}
		if atomic.CompareAndSwapUint64(addr, old, old|mask) {
			return false
		}
	}
}

// clearAtomic atomically clears bit i. Safe for concurrent use by multiple
// goroutines.
func (b *bitset) clearAtomic(i int) {
	if i < 0 {
		return
	}
	w := i >> 6
	mask := ^(uint64(1) << (uint(i) & 63))
	addr := &b.w[w]
	for {
		old := atomic.LoadUint64(addr)
		if atomic.CompareAndSwapUint64(addr, old, old&mask) {
			return
		}
	}
}

func (b *bitset) clone() *bitset {
	cp := &bitset{w: make([]uint64, len(b.w))}
	copy(cp.w, b.w)
	return cp
}

func (b *bitset) and(x *bitset) *bitset {
	for i := range b.w {
		b.w[i] &= x.w[i]
	}
	return b
}

func (b *bitset) or(x *bitset) *bitset {
	for i := range b.w {
		b.w[i] |= x.w[i]
	}
	return b
}

func (b *bitset) andNot(x *bitset) *bitset {
	for i := range b.w {
		b.w[i] &^= x.w[i]
	}
	return b
}

func (b *bitset) isEmpty() bool {
	for _, w := range b.w {
		if w != 0 {
			return false
		}
	}
	return true
}

func (b *bitset) count() int {
	total := 0
	for _, w := range b.w {
		total += bits.OnesCount64(w)
	}
	return total
}

func (b *bitset) forEachSet(fn func(i int)) {
	for wi, w := range b.w {
		for w != 0 {
			tz := bits.TrailingZeros64(w)
			i := (wi << 6) + tz
			fn(i)
			w &^= 1 << uint(tz) //nolint:gosec //bits.TrailingZeros64 never returns negative.
		}
	}
}

// CondenseFWBWGroupsFromAdj runs parallel FW–BW SCC on adj and returns only
// the component groups as slices of original node IDs. This avoids building the
// idToComp map when callers don't need it.
func CondenseFWBWGroupsFromAdj(ctx context.Context, adj map[int]map[int]int, opts Options) [][]int {
	if opts.MaxWorkers <= 0 {
		opts.MaxWorkers = runtime.GOMAXPROCS(0)
	}
	csr := BuildCSRFromAdj(adj, opts)
	// Small graphs: use low-overhead Tarjan SCC
	if csr.N <= opts.SmallCutoff {
		comp := tarjanSCC(csr)
		maxID := -1
		for _, c := range comp {
			if c > maxID {
				maxID = c
			}
		}
		groups := make([][]int, maxID+1)
		for idx := 0; idx < csr.N; idx++ {
			cid := comp[idx]
			groups[cid] = append(groups[cid], csr.IdxToNodeID[idx])
		}
		return groups
	}
	comp := make([]int, csr.N)
	for i := range comp {
		comp[i] = -1
	}
	active := newBitset(csr.N)
	for i := 0; i < csr.N; i++ {
		active.set(i)
	}
	nextID := 0
	sccFWBW(ctx, csr, active, comp, &nextID, opts)

	groups := make([][]int, nextID)
	for idx := 0; idx < csr.N; idx++ {
		cid := comp[idx]
		if cid < 0 {
			// Defensive: assign singleton if anything slipped through
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
	return groups
}

func sccFWBW(ctx context.Context, csr *CSR, active *bitset, comp []int, nextID *int, opts Options) {
	// Optional trimming loop.
	if opts.EnableTrim {
		for {
			if n := trimSingletons(csr, active, comp, nextID); n == 0 {
				break
			}
			if active.isEmpty() {
				return
			}
		}
	}

	// Base case: small subgraph — do simple repeated FW–BW (Tarjan hook point).
	if active.count() <= opts.SmallCutoff {
		for !active.isEmpty() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			pivot := firstActive(active)
			f := bfsMultiSource(ctx, csr, []int{pivot}, active, false, opts.MaxWorkers)
			b := bfsMultiSource(ctx, csr, []int{pivot}, active, true, opts.MaxWorkers)
			c := f.clone().and(b)
			assignComponent(c, comp, nextID, active)

			// Partitions
			fNotC := f.clone().andNot(c)
			bNotC := b.clone().andNot(c)
			fOrB := f.clone().or(b)
			u := active.clone().andNot(fOrB)

			if !fNotC.isEmpty() {
				sccFWBW(ctx, csr, fNotC, comp, nextID, opts)
				active.andNot(fNotC)
			}
			if !bNotC.isEmpty() {
				sccFWBW(ctx, csr, bNotC, comp, nextID, opts)
				active.andNot(bNotC)
			}
			if !u.isEmpty() {
				sccFWBW(ctx, csr, u, comp, nextID, opts)
				active.andNot(u)
			}
		}
		return
	}

	// General case: one pivot; add pivot batching later for extra speed.
	pivot := firstActive(active)
	f := bfsMultiSource(ctx, csr, []int{pivot}, active, false, opts.MaxWorkers)
	b := bfsMultiSource(ctx, csr, []int{pivot}, active, true, opts.MaxWorkers)
	c := f.clone().and(b)
	assignComponent(c, comp, nextID, active)

	// F\C, B\C, and U = active \ (F ∪ B)
	fNotC := f.clone().andNot(c)
	bNotC := b.clone().andNot(c)
	fOrB := f.clone().or(b)
	u := active.clone().andNot(fOrB)

	type sub struct{ mask *bitset }
	var subs []sub
	if !fNotC.isEmpty() {
		subs = append(subs, sub{fNotC})
	}
	if !bNotC.isEmpty() {
		subs = append(subs, sub{bNotC})
	}
	if !u.isEmpty() {
		subs = append(subs, sub{u})
	}
	if len(subs) == 0 {
		return
	}

	if len(subs) == 1 || opts.MaxWorkers <= 1 {
		for _, s := range subs {
			sccFWBW(ctx, csr, s.mask, comp, nextID, opts)
			active.andNot(s.mask)
		}
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(subs))
	for _, s := range subs {
		mask := s.mask
		go func() {
			defer wg.Done()
			sccFWBW(ctx, csr, mask, comp, nextID, opts)
		}()
	}
	wg.Wait()
	for _, s := range subs {
		active.andNot(s.mask)
	}
}

// bfsMultiSource runs a parallel BFS from sources over csr.
// If useTranspose is true, traverses csr.T* arrays.
// Traversal respects 'active' mask; returns visited including sources.
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

		// Gate parallelism: if frontier small, do sequential step to avoid overhead
		if len(frontier) <= 64 || maxWorkers == 1 {
			next := make([]int, 0, len(frontier))
			for _, u := range frontier {
				rs, re := getRow(u)
				for p := rs; p < re; p++ {
					v := getCol(p)
					if !active.test(v) {
						continue
					}
					// Non-atomic is safe because we are single-threaded on this step
					w := v >> 6
					mask := uint64(1) << (uint(v) & 63) //nolint:gosec //active.test returns false for negative values.
					if (visited.w[w] & mask) == 0 {
						visited.w[w] |= mask
						next = append(next, v)
					}
				}
			}
			frontier = next
			continue
		}

		workers := min(maxWorkers, len(frontier))
		var wg sync.WaitGroup
		wg.Add(workers)

		chunkSize := (len(frontier) + workers - 1) / workers
		nextBuckets := make([][]int, workers)

		for w := range workers {
			start := w * chunkSize
			end := start + chunkSize
			if start >= len(frontier) {
				wg.Done()
				continue
			}
			if end > len(frontier) {
				end = len(frontier)
			}

			w := w // capture
			go func(start, end int) {
				defer wg.Done()
				local := getBucketSlice()
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

		// Flatten next frontier and return bucket buffers to pool.
		total := 0
		for _, b := range nextBuckets {
			total += len(b)
		}
		next := make([]int, total)
		off := 0
		for _, b := range nextBuckets {
			copy(next[off:], b)
			off += len(b)
			putBucketSlice(b)
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
	var pivot = -1
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

// trimSingletons peels vertices with restricted in/out degree within 'active'.
// Each peeled vertex becomes its own SCC id. Returns count peeled.
func trimSingletons(csr *CSR, active *bitset, comp []int, nextID *int) int {
	n := csr.N
	inDeg := getIntSlice(n)
	outDeg := getIntSlice(n)
	defer func() {
		putIntSlice(inDeg)
		putIntSlice(outDeg)
	}()

	// Initialize degrees within active and queue zeros
	queue := getIntSlice(0)
	defer putIntSlice(queue)
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

// Add a simple sequential Tarjan SCC for small graphs.
func tarjanSCC(csr *CSR) []int {
	n := csr.N
	index := 0
	indices := make([]int, n)
	lowlink := make([]int, n)
	onstack := make([]bool, n)
	stack := make([]int, 0, n)
	comp := make([]int, n)
	for i := range n {
		indices[i] = -1
		comp[i] = -1
	}
	compID := 0
	var strongConnect func(v int)
	strongConnect = func(v int) {
		indices[v] = index
		lowlink[v] = index
		index++
		stack = append(stack, v)
		onstack[v] = true
		for p := csr.Row[v]; p < csr.Row[v+1]; p++ {
			w := csr.Col[p]
			if indices[w] == -1 {
				strongConnect(w)
				if lowlink[w] < lowlink[v] {
					lowlink[v] = lowlink[w]
				}
			} else if onstack[w] {
				if indices[w] < lowlink[v] {
					lowlink[v] = indices[w]
				}
			}
		}
		if lowlink[v] == indices[v] {
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onstack[w] = false
				comp[w] = compID
				if w == v {
					break
				}
			}
			compID++
		}
	}
	for v := range n {
		if indices[v] == -1 {
			strongConnect(v)
		}
	}
	return comp
}
