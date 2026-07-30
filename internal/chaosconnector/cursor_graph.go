package chaosconnector

import (
	"fmt"
	"math/rand"
)

// CursorPage is the transport-independent control shape of one paginated
// connector response.
type CursorPage struct {
	Spawn []string
	Next  string
}

// CursorGraph is shared by chaos schedules and the checkpoint/cut harnesses.
// Tokens contains every payload-bearing page in deterministic creation order.
type CursorGraph struct {
	Pages          map[string]CursorPage
	Tokens         []string
	PoisonedFirst  map[string]bool
	PoisonedSecond map[string]bool
}

// Validate checks that every reference resolves and every declared payload
// token is reachable. Re-mentions and cycles are legal adversarial shapes.
func (g *CursorGraph) Validate() error {
	if g == nil {
		return fmt.Errorf("chaosconnector: nil cursor graph")
	}
	declared := make(map[string]struct{}, len(g.Tokens))
	for _, token := range g.Tokens {
		if token == "" {
			return fmt.Errorf("chaosconnector: empty payload token")
		}
		if _, duplicate := declared[token]; duplicate {
			return fmt.Errorf("chaosconnector: duplicate payload token %q", token)
		}
		declared[token] = struct{}{}
		if _, ok := g.Pages[token]; !ok {
			return fmt.Errorf("chaosconnector: payload token %q has no page", token)
		}
	}
	if _, ok := g.Pages[""]; !ok {
		return fmt.Errorf("chaosconnector: cursor graph has no root page")
	}

	seen := make(map[string]struct{}, len(g.Tokens))
	queue := referencedTokens(g.Pages[""])
	for len(queue) > 0 {
		token := queue[0]
		queue = queue[1:]
		if _, visited := seen[token]; visited {
			continue
		}
		if _, ok := declared[token]; !ok {
			return fmt.Errorf("chaosconnector: cursor graph references undeclared token %q", token)
		}
		seen[token] = struct{}{}
		queue = append(queue, referencedTokens(g.Pages[token])...)
	}
	if len(seen) != len(declared) {
		for _, token := range g.Tokens {
			if _, ok := seen[token]; !ok {
				return fmt.Errorf("chaosconnector: payload token %q is unreachable", token)
			}
		}
	}
	return nil
}

func referencedTokens(page CursorPage) []string {
	out := append([]string(nil), page.Spawn...)
	if page.Next != "" {
		out = append(out, page.Next)
	}
	return out
}

// GenerateCursorGraph builds one reachable acyclic fan-out topology using the
// supplied replay RNG. The two poison sets let callers model independent
// collection phases such as entitlements and grants.
func GenerateCursorGraph(rng *rand.Rand, minTokens, maxTokens, poisonPercent int) *CursorGraph {
	if minTokens < 1 || maxTokens < minTokens {
		panic("chaosconnector: invalid cursor graph token bounds")
	}
	if poisonPercent < 0 || poisonPercent > 100 {
		panic("chaosconnector: invalid cursor graph poison percentage")
	}
	total := minTokens
	if maxTokens > minTokens {
		total += rng.Intn(maxTokens - minTokens + 1)
	}
	graph := &CursorGraph{
		Pages:          make(map[string]CursorPage, total+1),
		Tokens:         make([]string, total),
		PoisonedFirst:  make(map[string]bool),
		PoisonedSecond: make(map[string]bool),
	}
	for i := range graph.Tokens {
		graph.Tokens[i] = fmt.Sprintf("n%02d", i)
		graph.Pages[graph.Tokens[i]] = CursorPage{}
	}

	unattached := append([]string(nil), graph.Tokens...)
	take := func() string {
		token := unattached[0]
		unattached = unattached[1:]
		return token
	}
	root := CursorPage{}
	frontier := make([]string, 0, total)
	for range 1 + rng.Intn(4) {
		if len(unattached) == 0 {
			break
		}
		token := take()
		root.Spawn = append(root.Spawn, token)
		frontier = append(frontier, token)
	}
	if rng.Intn(3) == 0 && len(unattached) > 0 {
		root.Next = take()
		frontier = append(frontier, root.Next)
	}
	graph.Pages[""] = root

	for len(unattached) > 0 {
		attachPoint := graph.Tokens[0]
		if len(frontier) > 0 {
			index := rng.Intn(len(frontier))
			attachPoint = frontier[index]
			frontier = append(frontier[:index], frontier[index+1:]...)
		}
		page := graph.Pages[attachPoint]
		attached := false
		if rng.Intn(2) == 0 && len(unattached) > 0 {
			page.Next = take()
			frontier = append(frontier, page.Next)
			attached = true
		}
		for i, count := 0, rng.Intn(4); i < count && len(unattached) > 0; i++ {
			token := take()
			page.Spawn = append(page.Spawn, token)
			frontier = append(frontier, token)
			attached = true
		}
		if !attached && len(frontier) == 0 && len(unattached) > 0 && page.Next == "" {
			page.Next = take()
			frontier = append(frontier, page.Next)
		}
		graph.Pages[attachPoint] = page
	}

	for _, token := range append([]string{""}, graph.Tokens...) {
		if rng.Intn(100) < poisonPercent {
			graph.PoisonedFirst[token] = true
		}
		if rng.Intn(100) < poisonPercent {
			graph.PoisonedSecond[token] = true
		}
	}
	if err := graph.Validate(); err != nil {
		panic(err)
	}
	return graph
}
