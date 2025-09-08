package scc

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// withTimeout runs f and fails the test if it doesn't complete within d.
func withTimeout(t *testing.T, d time.Duration, f func(t *testing.T)) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f(t)
	}()
	select {
	case <-done:
		return
	case <-time.After(d):
		t.Fatalf("function did not complete within %v (possible hang)", d)
	}
}

// adversarialGraphs returns a set of graphs that exercise different recursion paths.
func adversarialGraphs() []map[int]map[int]int {
	var graphs []map[int]map[int]int

	// Small ring (single SCC)
	{
		n := 16
		nodes := make([]int, n)
		for i := 0; i < n; i++ {
			nodes[i] = i
		}
		var edges [][2]int
		for i := 0; i < n; i++ {
			edges = append(edges, [2]int{i, (i + 1) % n})
		}
		graphs = append(graphs, makeAdj(nodes, edges))
	}

	// Chain (all acyclic singletons)
	{
		n := 64
		nodes := make([]int, n)
		for i := 0; i < n; i++ {
			nodes[i] = i
		}
		var edges [][2]int
		for i := 0; i+1 < n; i++ {
			edges = append(edges, [2]int{i, i + 1})
		}
		graphs = append(graphs, makeAdj(nodes, edges))
	}

	// Two disjoint rings plus some isolated nodes
	{
		nodes := []int{0, 1, 2, 10, 11, 12, 13, 100, 101}
		edges := [][2]int{{0, 1}, {1, 2}, {2, 0}, {10, 11}, {11, 12}, {12, 13}, {13, 10}}
		graphs = append(graphs, makeAdj(nodes, edges))
	}

	// Dense bidirectional bipartite (one big SCC)
	{
		a, b := 8, 8
		var nodes []int
		for i := 0; i < a+b; i++ {
			nodes = append(nodes, i)
		}
		var edges [][2]int
		for i := 0; i < a; i++ {
			for j := a; j < a+b; j++ {
				edges = append(edges, [2]int{i, j})
				edges = append(edges, [2]int{j, i})
			}
		}
		graphs = append(graphs, makeAdj(nodes, edges))
	}

	// Tail into ring (classic FW–BW partitions)
	{
		ringN := 10
		tailN := 12
		var nodes []int
		for i := 0; i < ringN; i++ {
			nodes = append(nodes, i)
		}
		for i := 0; i < tailN; i++ {
			nodes = append(nodes, 1000+i)
		}
		var edges [][2]int
		// ring
		for i := 0; i < ringN; i++ {
			edges = append(edges, [2]int{i, (i + 1) % ringN})
		}
		// tail into ring start (0)
		for i := 0; i+1 < tailN; i++ {
			edges = append(edges, [2]int{1000 + i, 1000 + i + 1})
		}
		edges = append(edges, [2]int{1000 + tailN - 1, 0})
		graphs = append(graphs, makeAdj(nodes, edges))
	}

	return graphs
}

// TestNoHang_GeneralCase forces FW–BW general-case recursion and ensures each adversarial graph completes quickly.
func TestNoHang_GeneralCase(t *testing.T) {
	graphs := adversarialGraphs()
	opts := DefaultOptions()
	opts.MaxWorkers = 4
	for gi, adj := range graphs {
		gi, adj := gi, adj
		t.Run(
			funcName("general", gi, false),
			func(t *testing.T) {
				withTimeout(t, 2*time.Second, func(t *testing.T) {
					_, _ = CondenseFWBW(context.Background(), adjSource{adj: adj}, opts)
				})
			},
		)
	}
}

// TestNoHang_BaseCase removed; single unified path now.

// funcName formats a helpful subtest name.
func funcName(kind string, idx int, _ bool) string {
	return kind + "_#" + strconv.Itoa(idx)
}

// ---- Generators for adversarial graphs ----

func genChain(n int) map[int]map[int]int {
	nodes := make([]int, n)
	for i := 0; i < n; i++ {
		nodes[i] = i
	}
	var edges [][2]int
	for i := 0; i+1 < n; i++ {
		edges = append(edges, [2]int{i, i + 1})
	}
	return makeAdj(nodes, edges)
}

func genLollipop(m, t int) map[int]map[int]int {
	var nodes []int
	for i := 0; i < m+t; i++ {
		nodes = append(nodes, i)
	}
	var edges [][2]int
	// clique K_m
	for i := 0; i < m; i++ {
		for j := 0; j < m; j++ {
			if i == j {
				continue
			}
			edges = append(edges, [2]int{i, j})
		}
	}
	// tail
	edges = append(edges, [2]int{m - 1, m})
	for i := m; i+1 < m+t; i++ {
		edges = append(edges, [2]int{i, i + 1})
	}
	return makeAdj(nodes, edges)
}

func genBipartite(a, b int, both bool) map[int]map[int]int {
	var nodes []int
	for i := 0; i < a+b; i++ {
		nodes = append(nodes, i)
	}
	var edges [][2]int
	for i := 0; i < a; i++ {
		for j := a; j < a+b; j++ {
			edges = append(edges, [2]int{i, j})
			if both {
				edges = append(edges, [2]int{j, i})
			}
		}
	}
	return makeAdj(nodes, edges)
}

func genDisjointTwoCycles(k int) map[int]map[int]int {
	// nodes: 0..2k-1, edges: (2i <-> 2i+1)
	var nodes []int
	var edges [][2]int
	for i := 0; i < k; i++ {
		u := 2 * i
		v := u + 1
		nodes = append(nodes, u, v)
		edges = append(edges, [2]int{u, v}, [2]int{v, u})
	}
	return makeAdj(nodes, edges)
}

func genIsolates(n int) map[int]map[int]int {
	var nodes []int
	for i := 0; i < n; i++ {
		nodes = append(nodes, i)
	}
	return makeAdj(nodes, nil)
}

func genSelfLoops(n int) map[int]map[int]int {
	var nodes []int
	var edges [][2]int
	for i := 0; i < n; i++ {
		nodes = append(nodes, i)
		edges = append(edges, [2]int{i, i})
	}
	return makeAdj(nodes, edges)
}

// ---- Adversarial specific tests ----

func TestNoHang_DeepChain(t *testing.T) {
	adj := genChain(4000)
	for _, cfg := range []struct {
		name string
		opt  Options
	}{
		{"recursion_w1", func() Options {
			o := DefaultOptions()
			o.MaxWorkers = 1
			return o
		}()},
		{"recursion_w2", func() Options {
			o := DefaultOptions()
			o.MaxWorkers = 2
			return o
		}()},
	} {
		cfg := cfg
		t.Run(cfg.name, func(t *testing.T) {
			withTimeout(t, 2*time.Second, func(t *testing.T) {
				_, _ = CondenseFWBW(context.Background(), adjSource{adj: adj}, cfg.opt)
			})
		})
	}
}

func TestNoHang_Lollipop(t *testing.T) {
	adj := genLollipop(64, 512)
	for _, cfg := range []struct {
		name string
		opt  Options
	}{
		{"recursion_w4", func() Options {
			o := DefaultOptions()
			o.MaxWorkers = 4
			return o
		}()},
		{"recursion_w2", func() Options {
			o := DefaultOptions()
			o.MaxWorkers = 2
			return o
		}()},
	} {
		cfg := cfg
		t.Run(cfg.name, func(t *testing.T) {
			withTimeout(t, 2*time.Second, func(t *testing.T) {
				_, _ = CondenseFWBW(context.Background(), adjSource{adj: adj}, cfg.opt)
			})
		})
	}
}

func TestNoHang_Isolates_And_SelfLoops(t *testing.T) {
	iso := genIsolates(3000)
	self := genSelfLoops(3000)
	for _, tc := range []struct {
		name string
		adj  map[int]map[int]int
	}{
		{"isolates", iso},
		{"selfloops", self},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withTimeout(t, 3*time.Second, func(t *testing.T) {
				o := DefaultOptions()
				o.MaxWorkers = 4
				_, _ = CondenseFWBW(context.Background(), adjSource{adj: tc.adj}, o)
			})
		})
	}
}

func TestNoHang_BipartiteDense(t *testing.T) {
	adj := genBipartite(128, 128, true)
	for _, mw := range []int{1, 4} {
		mw := mw
		t.Run("mw_"+strconv.Itoa(mw), func(t *testing.T) {
			withTimeout(t, 2*time.Second, func(t *testing.T) {
				o := DefaultOptions()
				o.MaxWorkers = mw
				_, _ = CondenseFWBW(context.Background(), adjSource{adj: adj}, o)
			})
		})
	}
}

func TestNoHang_ManyTwoCycles(t *testing.T) {
	adj := genDisjointTwoCycles(2000)
	withTimeout(t, 2*time.Second, func(t *testing.T) {
		o := DefaultOptions()
		o.MaxWorkers = 4
		_, _ = CondenseFWBW(context.Background(), adjSource{adj: adj}, o)
	})
}

func TestCancel_HeavyGraphs(t *testing.T) {
	// Use contexts with very short deadlines; ensure prompt return.
	lollipop := genLollipop(128, 2048)
	bip := genBipartite(256, 256, true)
	for _, tc := range []struct {
		name string
		adj  map[int]map[int]int
	}{
		{"lollipop", lollipop},
		{"bipartite", bip},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
			defer cancel()
			start := time.Now()
			o := DefaultOptions()
			o.MaxWorkers = 8
			_, _ = CondenseFWBW(ctx, adjSource{adj: tc.adj}, o)
			if time.Since(start) > 200*time.Millisecond {
				t.Fatalf("cancellation not honored promptly")
			}
		})
	}
}
