package expand

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/require"
)

const separatorRelation = '>'
const separatorNext = ' '
const separatorList = ','
const dummyID = "nil"

func elementsMatch(listA []int, listB []int) bool {
	if len(listA) != len(listB) {
		return false
	}
	setA := mapset.NewSet(listA...)
	setB := mapset.NewSet(listB...)

	differenceA := setA.Difference(setB)
	if differenceA.Cardinality() > 0 {
		return false
	}

	differenceB := setB.Difference(setA)

	return differenceB.Cardinality() <= 0
}

func parseExpression(
	t *testing.T,
	ctx context.Context,
	input string,
) *EntitlementGraph {
	graph := NewEntitlementGraph(context.Background())
	expressions := strings.Split(input, string(separatorNext))
	entitlementIDSet := mapset.NewSet[string]()
	for _, expression := range expressions {
		entitlementIDs := strings.Split(expression, string(separatorRelation))
		previousEntitlementID := dummyID
		for _, entitlementID := range entitlementIDs {
			// Create the entitlement if it hasn't already been created.
			if !entitlementIDSet.Contains(entitlementID) {
				graph.AddEntitlement(&v2.Entitlement{Id: entitlementID})
			}
			// Add an edge if from left side to right side.
			if previousEntitlementID != dummyID {
				err := graph.AddEdge(ctx, previousEntitlementID, entitlementID, false, nil)
				require.NoError(t, err)
			}
			previousEntitlementID = entitlementID
		}
	}

	err := graph.Validate()
	require.NoError(t, err)

	return graph
}

// expectEntitlements is a private test helper that asserts that the output of
// `GetDescendantEntitlements()` contains exactly the entitlements in the DSL
// string `expectedEntitlements`.
func expectEntitlements(t *testing.T, entitlementsMap map[string]*Edge, expectedEntitlements string) {
	entitlementIDs := make([]string, 0)
	for entitlementID := range entitlementsMap {
		entitlementIDs = append(entitlementIDs, entitlementID)
	}
	expectedEntitlementIDs := make([]string, 0)
	for _, entitlementID := range strings.Split(expectedEntitlements, string(separatorList)) {
		if entitlementID != "" {
			expectedEntitlementIDs = append(expectedEntitlementIDs, entitlementID)
		}
	}

	require.ElementsMatch(t, expectedEntitlementIDs, entitlementIDs)
}

func createNodeIDList(input string) [][]int {
	nodeIDs := make([][]int, 0)
	expressions := strings.Split(input, string(separatorNext))
	for _, expression := range expressions {
		ids := strings.Split(expression, string(separatorList))
		nextRow := make([]int, 0)
		for _, id := range ids {
			if id != "" {
				nodeID, err := strconv.Atoi(id)
				if err == nil {
					nextRow = append(nextRow, nodeID)
				}
			}
		}
		if len(nextRow) > 0 {
			nodeIDs = append(nodeIDs, nextRow)
		}
	}
	return nodeIDs
}

func TestGetDescendants(t *testing.T) {
	testCases := []struct {
		expression  string
		rootID      string
		expectedIDs string
		message     string
	}{
		{"1", "1", "", "no descendants"},
		{"1>2 1>3", "1", "2,3", "N descendants"},
		{"1>2>3", "1", "2", "grandchildren don't count"},
		{"1>1", "1", "1", "unfixed self-cycle"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.message, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			graph := parseExpression(t, ctx, testCase.expression)

			descendantEntitlements := graph.GetDescendantEntitlements(testCase.rootID)
			expectEntitlements(t, descendantEntitlements, testCase.expectedIDs)
		})
	}
}

func TestRemoveNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	graph := parseExpression(t, ctx, "1")

	node := graph.GetNode("1")
	require.NotNil(t, node)

	graph.removeNode(1)

	node = graph.GetNode("1")
	require.Nil(t, node)
}

func TestGetFirstCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCases := []struct {
		expression        string
		expectedCycleSize int
		message           string
	}{
		{"1>2>3>4 4>2", 3, "example"},
		{"1>2>3>4 1>5>6>7", 0, "no cycle"},
		{"1>2>3 1>3", 0, "pseudo cycle"},
		{"1>1", 1, "self cycle"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.message, func(t *testing.T) {
			graph := parseExpression(t, ctx, testCase.expression)
			cycle := graph.GetFirstCycle(ctx)
			if testCase.expectedCycleSize == 0 {
				require.Nil(t, cycle)
			} else {
				require.NotNil(t, cycle)
				require.Len(t, cycle, testCase.expectedCycleSize)
			}
		})
	}
}

func TestHandleCycle(t *testing.T) {
	testCases := []struct {
		expression     string
		expectedCycles string
		message        string
	}{
		{"1>2>3>4>2", "2,3,4", "example"},
		{"1>1 2", "1", "simplest"},
		{"1>2>1", "1,2", "simple"},
		{"1>2>1 3>4>3", "1,2 3,4", "two cycles"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.message, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			graph := parseExpression(t, ctx, testCase.expression)

			cycle := graph.GetFirstCycle(ctx)
			expectedCycles := createNodeIDList(testCase.expectedCycles)
			require.NotNil(t, cycle)
			found := false
			for _, expectedCycle := range expectedCycles {
				if elementsMatch(expectedCycle, cycle) {
					found = true
					break
				}
			}
			require.True(t, found)

			err := graph.FixCycles(ctx)
			require.NoError(t, err, graph.Str())
			err = graph.Validate()
			require.NoError(t, err)
			cycle = graph.GetFirstCycle(ctx)
			require.Nil(t, cycle)
		})
	}
}

func TestHandleComplexCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	graph := parseExpression(t, ctx, "1>2>3>2>1")

	require.Equal(t, 3, len(graph.Nodes))
	require.Equal(t, 4, len(graph.Edges))
	require.Equal(t, 3, len(graph.GetEntitlements()))

	err := graph.FixCycles(ctx)
	require.NoError(t, err, graph.Str())
	err = graph.Validate()
	require.NoError(t, err)

	require.Equal(t, 1, len(graph.Nodes))
	require.Equal(t, 0, len(graph.Edges))
	require.Equal(t, 3, len(graph.GetEntitlements()))

	cycle := graph.GetFirstCycle(ctx)
	require.Nil(t, cycle)
}

// TestHandleCliqueCycle reduces a N=3 clique to a single node.
func TestHandleCliqueCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test can be flaky.
	N := 1
	for range N {
		graph := parseExpression(t, ctx, "1>2>3>2>1>3>1")

		require.Equal(t, 3, len(graph.Nodes))
		require.Equal(t, 6, len(graph.Edges))
		require.Equal(t, 3, len(graph.GetEntitlements()))

		err := graph.FixCycles(ctx)
		require.NoError(t, err, graph.Str())
		err = graph.Validate()
		require.NoError(t, err)

		require.Equal(t, 1, len(graph.Nodes))
		require.Equal(t, 0, len(graph.Edges))
		require.Equal(t, 3, len(graph.GetEntitlements()))

		cycle := graph.GetFirstCycle(ctx)
		require.Nil(t, cycle)
	}
}

func TestMarkEdgeExpanded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	graph := parseExpression(t, ctx, "1>2>3")
	require.False(t, graph.IsExpanded())

	// Edge doesn't exist, don't panic.
	graph.MarkEdgeExpanded("1", "3")
	require.False(t, graph.IsEntitlementExpanded("1"))
	require.False(t, graph.IsEntitlementExpanded("2"))
	require.False(t, graph.IsExpanded())

	graph.MarkEdgeExpanded("1", "2")
	require.True(t, graph.IsEntitlementExpanded("1"))
	require.False(t, graph.IsEntitlementExpanded("2"))
	require.False(t, graph.IsExpanded())

	graph.MarkEdgeExpanded("2", "3")
	require.True(t, graph.IsEntitlementExpanded("1"))
	require.True(t, graph.IsEntitlementExpanded("2"))
	require.True(t, graph.IsExpanded())
}

func TestDeepNoCycles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	depth := 40

	expressionStr := ""
	for i := range depth {
		expressionStr += fmt.Sprintf("%d>%d", i+1, i+2)
	}
	graph := parseExpression(t, ctx, expressionStr)

	require.Equal(t, depth+1, len(graph.Nodes))
	require.Equal(t, depth, len(graph.Edges))
	require.Equal(t, depth+1, len(graph.GetEntitlements()))

	err := graph.FixCycles(ctx)
	require.NoError(t, err, graph.Str())
	err = graph.Validate()
	require.NoError(t, err)

	require.Equal(t, depth+1, len(graph.Nodes))
	require.Equal(t, depth, len(graph.Edges))
	require.Equal(t, depth+1, len(graph.GetEntitlements()))

	cycle := graph.GetFirstCycle(ctx)
	require.Nil(t, cycle)
}

func TestDeepCycles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	depth := 40

	expressionStr := ""
	for i := range depth {
		expressionStr += fmt.Sprintf("%d>%d", i+1, i+2)
	}
	expressionStr += fmt.Sprintf("%d>%d", depth, 1)
	graph := parseExpression(t, ctx, expressionStr)

	require.Equal(t, depth+1, len(graph.Nodes))
	require.Equal(t, depth+1, len(graph.Edges))
	require.Equal(t, depth+1, len(graph.GetEntitlements()))

	err := graph.FixCycles(ctx)
	require.NoError(t, err, graph.Str())
	err = graph.Validate()
	require.NoError(t, err)

	require.Equal(t, 1, len(graph.Nodes))
	require.Equal(t, 0, len(graph.Edges))
	require.Equal(t, depth+1, len(graph.GetEntitlements()))

	cycle := graph.GetFirstCycle(ctx)
	require.Nil(t, cycle)
}
