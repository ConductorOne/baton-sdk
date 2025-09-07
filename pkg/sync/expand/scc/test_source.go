package scc

// adjSource adapts a map[int]map[int]int adjacency to the Source interface for tests.
type adjSource struct {
	adj map[int]map[int]int
}

func (a adjSource) ForEachNode(fn func(id int) bool) {
	for id := range a.adj {
		if !fn(id) {
			return
		}
	}
}

func (a adjSource) ForEachEdgeFrom(src int, fn func(dst int) bool) {
	if row, ok := a.adj[src]; ok {
		for dst := range row {
			if !fn(dst) {
				return
			}
		}
	}
}
