package sync

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

func createExpectedEntitlementsMap(input string) map[string]*grantInfo {
	expectedEntitlements := make(map[string]*grantInfo)
	expectedIDs := strings.Split(input, string(separatorList))
	for _, id := range expectedIDs {
		if id != "" {
			expectedEntitlements[id] = &grantInfo{
				Expanded:        false,
				Shallow:         false,
				ResourceTypeIDs: nil,
			}
		}
	}
	return expectedEntitlements
}

func createNodeIDList(input string) [][]int {
	nodeIds := make([][]int, 0)
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
			nodeIds = append(nodeIds, nextRow)
		}

	}
	return nodeIds
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
			expectedEntitlements := createExpectedEntitlementsMap(testCase.expectedIDs)
			require.EqualValues(t, expectedEntitlements, descendantEntitlements, graph.Str())
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
		{"1>2>3>4 4>2", "2,3,4", "example"},
		{"1>1 2", "1", "simplest"},
		{"1>2 2>1", "1,2", "simple"},
		{"1>2 2>1 3>4 4>3", "1,2 3,4", "two cycles"},
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
