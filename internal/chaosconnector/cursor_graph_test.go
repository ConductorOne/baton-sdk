package chaosconnector

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateCursorGraphIsDeterministicAndReachable(t *testing.T) {
	first := GenerateCursorGraph(rand.New(rand.NewSource(42)), 8, 35, 15)  //nolint:gosec // replayable test topology
	second := GenerateCursorGraph(rand.New(rand.NewSource(42)), 8, 35, 15) //nolint:gosec // replayable test topology
	require.Equal(t, first, second)

	seen := make(map[string]struct{}, len(first.Tokens))
	queue := append([]string(nil), first.Pages[""].Spawn...)
	if next := first.Pages[""].Next; next != "" {
		queue = append(queue, next)
	}
	for len(queue) > 0 {
		token := queue[0]
		queue = queue[1:]
		_, duplicate := seen[token]
		require.False(t, duplicate, "generated acyclic graph attached token %q more than once", token)
		seen[token] = struct{}{}
		page, ok := first.Pages[token]
		require.True(t, ok, "generated graph references unknown token %q", token)
		queue = append(queue, page.Spawn...)
		if page.Next != "" {
			queue = append(queue, page.Next)
		}
	}
	require.Len(t, seen, len(first.Tokens))
}
