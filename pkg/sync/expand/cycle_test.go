package expand

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCycleDetectionHelper_BasicScenarios(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name  string
		expr  string
		start string
		want  []int
		has   bool
	}{
		{
			name:  "no-cycle simple chain",
			expr:  "1>2>3",
			start: "1",
			want:  nil,
			has:   false,
		},
		{
			name:  "pseudo-cycle diamond (no back-edge on path)",
			expr:  "1>2>3 1>3",
			start: "1",
			want:  nil,
			has:   false,
		},
		{
			name:  "self-cycle",
			expr:  "1>1",
			start: "1",
			want:  []int{1},
			has:   true,
		},
		{
			name:  "simple 3-cycle",
			expr:  "1>2>3>1",
			start: "1",
			want:  []int{1, 2, 3},
			has:   true,
		},
		{
			name:  "back-edge to subpath node",
			expr:  "1>2>3>2 3>4",
			start: "1",
			want:  []int{2, 3},
			has:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := parseExpression(t, ctx, tc.expr)
			startNodeID := g.EntitlementsToNodes[tc.start]
			cycle, ok := g.cycleDetectionHelper(startNodeID)

			if !tc.has {
				require.False(t, ok)
				require.Nil(t, cycle)
				return
			}

			require.True(t, ok)
			require.NotNil(t, cycle)
			require.Truef(t, elementsMatch(tc.want, cycle), "expected %v, got %v", tc.want, cycle)
		})
	}
}

func TestCycleDetectionHelper_MultipleCyclesDifferentStarts(t *testing.T) {
	ctx := context.Background()
	g := parseExpression(t, ctx, "1>2>1 3>4>3")

	// Start at 1 -> should find cycle {1,2}
	{
		startNodeID := g.EntitlementsToNodes["1"]
		cycle, ok := g.cycleDetectionHelper(startNodeID)
		require.True(t, ok)
		require.NotNil(t, cycle)
		require.True(t, elementsMatch([]int{1, 2}, cycle))
	}

	// Start at 3 -> should find cycle {3,4}
	{
		startNodeID := g.EntitlementsToNodes["3"]
		cycle, ok := g.cycleDetectionHelper(startNodeID)
		require.True(t, ok)
		require.NotNil(t, cycle)
		require.True(t, elementsMatch([]int{3, 4}, cycle))
	}
}

func TestCycleDetectionHelper_LargeRing(t *testing.T) {
	ctx := context.Background()

	// Build a modest-size ring to validate we return the entire cycle.
	const n = 10
	expr := ""
	for i := 1; i <= n; i++ {
		next := i + 1
		if next > n {
			next = 1
		}
		if i > 1 {
			expr += " "
		}
		expr += (func(a, b int) string { return fmt.Sprintf("%d>%d", a, b) })(i, next)
	}

	g := parseExpression(t, ctx, expr)
	startNodeID := g.EntitlementsToNodes["1"]
	cycle, ok := g.cycleDetectionHelper(startNodeID)
	require.True(t, ok)
	require.NotNil(t, cycle)
	require.Len(t, cycle, n)
}
