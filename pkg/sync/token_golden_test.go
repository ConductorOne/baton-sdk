package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The sync token is read and written by different SDK versions against the
// same artifact, so its bytes are a compatibility surface. This file holds
// the fixtures' expected values; testdata/tokens holds the bytes. Neither
// moves when the codec is reorganized — the adapter in
// token_golden_adapter_test.go is what points at the codec's entry points.

const goldenTokenDir = "testdata/tokens"

// goldenGraph is the shape a fixture's inline entitlement graph decodes to.
type goldenGraph struct {
	nodes      int
	edges      int
	nextNodeID int
	nextEdgeID int
	depth      int
	loaded     bool
}

type goldenTokenCase struct {
	// file is the input fixture, relative to goldenTokenDir.
	file string
	// expected is the fixture the input must re-encode to. Empty means the
	// input re-encodes to its own bytes.
	expected string
	// inlineGraph selects the writer option that serializes the entitlement
	// graph into the token.
	inlineGraph bool
	// needsExpansion is what NeedsExpansion(input) returns.
	needsExpansion bool
	// graph is what GraphFromToken(input) returns; nil expects nil.
	graph *goldenGraph
	// compaction is what CompactionStatsFromToken(input) returns; nil
	// expects nil.
	compaction *CompactionTokenStats
}

func goldenTokenCases() []goldenTokenCase {
	return []goldenTokenCase{
		{file: "empty.json"},
		{file: "v1_init.json"},
		{file: "v1_actions_multi.json"},
		{file: "v2_type_scoped.json"},
		{file: "v1_fact_needs_expansion.json", needsExpansion: true},
		{file: "v1_fact_has_external_resource_grants.json"},
		{file: "v1_fact_should_fetch_related_resources.json"},
		{file: "v1_fact_should_skip_entitlements_and_grants.json"},
		{file: "v1_fact_should_skip_grants.json"},
		{file: "v1_facts_all.json", needsExpansion: true},
		{file: "v1_run_stats.json"},
		{
			file: "v1_compaction.json",
			compaction: &CompactionTokenStats{
				Mode:           "fold",
				StatsSyncID:    "sync-base",
				BaseSyncID:     "sync-base",
				PartialSyncIDs: []string{"sync-p1", "sync-p2"},
				PartialCount:   2,
				RecordCounts: map[string]*CompactionRecordCounts{
					"resources":    {Output: 100, Added: 30, Replaced: 20, Carried: 50},
					"grants":       {Output: 900, Added: 400, Replaced: 100, Carried: 400},
					"entitlements": {Output: 40},
				},
			},
		},
		{
			file:           "v1_inline_graph.json",
			inlineGraph:    true,
			needsExpansion: true,
			graph:          &goldenGraph{nodes: 4, edges: 3, nextNodeID: 4, nextEdgeID: 3, depth: 2, loaded: true},
		},
		{
			file:           "v1_inline_graph.json",
			expected:       "v1_inline_graph.dropped.json",
			needsExpansion: true,
			graph:          &goldenGraph{nodes: 4, edges: 3, nextNodeID: 4, nextEdgeID: 3, depth: 2, loaded: true},
		},
		{file: "v1_inline_graph.dropped.json", needsExpansion: true},
		{file: "v0_current_action.json", expected: "v0_current_action.expected.json", needsExpansion: true},
		{file: "v0_no_current_action.json", expected: "v0_no_current_action.expected.json"},
		{file: "v0_empty_object.json", expected: "v0_empty_object.expected.json"},
	}
}

func (tc goldenTokenCase) name() string {
	if tc.expected == "" {
		return tc.file
	}
	return tc.file + "_to_" + tc.expected
}

func readGoldenToken(t *testing.T, file string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(goldenTokenDir, file))
	require.NoError(t, err)
	return strings.TrimRight(string(body), "\n")
}

func TestGoldenTokenRoundTrip(t *testing.T) {
	for _, tc := range goldenTokenCases() {
		t.Run(tc.name(), func(t *testing.T) {
			input := readGoldenToken(t, tc.file)
			want := input
			if tc.expected != "" {
				want = readGoldenToken(t, tc.expected)
			}

			// Encoding the same decoded token twice must produce the same
			// bytes, or the byte comparison below would pass or fail on Go's
			// randomized map iteration order rather than on the codec.
			first, second, err := goldenEncodeTwice(input, tc.inlineGraph)
			require.NoError(t, err)
			require.Equal(t, first, second, "two encodings of one decoded token differ")
			require.Equal(t, want, first)

			if tc.expected != "" {
				// V0 upgrades one way, so the upgraded bytes are what every
				// checkpoint after the resume carries. If they did not
				// re-encode to themselves the first checkpoint after a
				// resume would differ from the second.
				again, err := goldenDecodeEncode(want, tc.inlineGraph)
				require.NoError(t, err)
				require.Equal(t, want, again)
			}
		})
	}
}

func TestGoldenTokenHelpers(t *testing.T) {
	for _, tc := range goldenTokenCases() {
		t.Run(tc.name(), func(t *testing.T) {
			input := readGoldenToken(t, tc.file)

			needs, err := NeedsExpansion(input)
			require.NoError(t, err)
			require.Equal(t, tc.needsExpansion, needs)

			graph, err := GraphFromToken(input)
			require.NoError(t, err)
			if tc.graph == nil {
				require.Nil(t, graph)
			} else {
				require.NotNil(t, graph)
				require.Len(t, graph.Nodes, tc.graph.nodes)
				require.Len(t, graph.Edges, tc.graph.edges)
				require.Equal(t, tc.graph.nextNodeID, graph.NextNodeID)
				require.Equal(t, tc.graph.nextEdgeID, graph.NextEdgeID)
				require.Equal(t, tc.graph.depth, graph.Depth)
				require.Equal(t, tc.graph.loaded, graph.Loaded)
			}

			compaction, err := CompactionStatsFromToken(input)
			require.NoError(t, err)
			require.Equal(t, tc.compaction, compaction)
		})
	}
}

// TestGoldenTokenFixturesAllUsed fails when a fixture is checked in without
// a case reading it. An orphaned fixture looks like coverage and is not.
func TestGoldenTokenFixturesAllUsed(t *testing.T) {
	entries, err := os.ReadDir(goldenTokenDir)
	require.NoError(t, err)

	used := make(map[string]bool)
	for _, tc := range goldenTokenCases() {
		used[tc.file] = true
		if tc.expected != "" {
			used[tc.expected] = true
		}
	}

	var onDisk []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		onDisk = append(onDisk, entry.Name())
		require.True(t, used[entry.Name()], "fixture %s is not read by any case", entry.Name())
	}
	require.Len(t, used, len(onDisk))
}
