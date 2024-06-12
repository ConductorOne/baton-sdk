package expand

import (
	"context"
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

	require.EqualValues(t, expectedEntitlementIDs, entitlementIDs)
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

func TestGetCycles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	graph := parseExpression(t, ctx, "1>2>3>4 4>2")
	cycles, isCycle := graph.GetCycles()
	require.True(t, isCycle)
	require.Equal(t, [][]int{{2, 3, 4}}, cycles)
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

			cycles, isCycle := graph.GetCycles()
			expectedCycles := createNodeIDList(testCase.expectedCycles)
			require.True(t, isCycle)
			require.ElementsMatch(t, expectedCycles, cycles)

			err := graph.FixCycles()
			require.NoError(t, err, graph.Str())
			err = graph.Validate()
			require.NoError(t, err)
			cycles, isCycle = graph.GetCycles()
			require.False(t, isCycle)
			require.Empty(t, cycles)
		})
	}
}

// TestHandleComplexCycle reduces a N=3 clique to a single node.
func TestHandleComplexCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	graph := parseExpression(t, ctx, "1>2>3>2>1>3>1")

	require.Equal(t, 3, len(graph.Nodes))
	require.Equal(t, 6, len(graph.Edges))
	require.Equal(t, 3, len(graph.GetEntitlements()))

	err := graph.FixCycles()
	require.NoError(t, err, graph.Str())
	err = graph.Validate()
	require.NoError(t, err)

	require.Equal(t, 1, len(graph.Nodes))
	require.Equal(t, 0, len(graph.Edges))
	require.Equal(t, 3, len(graph.GetEntitlements()))

	cycles, isCycle := graph.GetCycles()
	require.False(t, isCycle)
	require.Empty(t, cycles)
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
