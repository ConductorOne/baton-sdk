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
		// An operation string this SDK does not know decodes to UnknownOp,
		// which is 0, and `operation` is omitempty on a uint8 kind — so
		// encoding/json drops the key before ActionOp.MarshalJSON can write
		// "unknown". The action survives, still paginating, with no op.
		{file: "v1_unknown_op.json", expected: "v1_unknown_op.expected.json"},
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
			expected:       "v1_inline_graph.dropped.json",
			needsExpansion: true,
			graph:          &goldenGraph{nodes: 4, edges: 3, nextNodeID: 4, nextEdgeID: 3, depth: 2, loaded: true},
		},
		{file: "v1_inline_graph.dropped.json", needsExpansion: true},
		{file: "v0_current_action.json", expected: "v0_current_action.expected.json", needsExpansion: true},
		{file: "v0_no_current_action.json", expected: "v0_no_current_action.expected.json"},
		{file: "v0_empty_object.json", expected: "v0_empty_object.expected.json"},
		// An unrecognized version falls back to the V0 parser, which shares
		// the fact and completed_actions_count keys with V1 but not
		// actions_map / action_order. The facts and the count survive; the
		// action stack and current_action_id are dropped. The graph is still
		// decoded (entitlement_graph is a shared key), which is why this
		// case expects one even though the re-encoded bytes carry none.
		{
			file:           "v3_future_version.json",
			expected:       "v3_future_version.expected.json",
			needsExpansion: true,
			graph:          &goldenGraph{nodes: 2, edges: 1, nextNodeID: 2, nextEdgeID: 1, depth: 3, loaded: true},
		},
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
	// Trim \r as well as \n. .gitattributes pins these files to LF so the
	// working tree matches the committed bytes, but a checkout that predates
	// it, an archive download, or an editor that rewrites endings would
	// otherwise leave a \r that no writer emits and every case compares
	// against.
	return strings.TrimRight(string(body), "\r\n")
}

func TestGoldenTokenRoundTrip(t *testing.T) {
	for _, tc := range goldenTokenCases() {
		t.Run(tc.name(), func(t *testing.T) {
			input := readGoldenToken(t, tc.file)
			want := input
			if tc.expected != "" {
				want = readGoldenToken(t, tc.expected)
			}

			// One decode, two encodes: Marshal must be idempotent, so the
			// byte comparison below reads the codec and not a side effect the
			// first encode left on the state. Map key order is not the risk —
			// encoding/json sorts map keys. Nor does this prove Marshal
			// leaves the live state alone: blanking the expansion page token
			// in place would still give two equal encodings.
			// TestSyncerTokenOmitsEntitlementGraph asserts that.
			first, second, err := goldenEncodeTwice(input)
			require.NoError(t, err)
			require.Equal(t, first, second, "two encodings of one decoded token differ")
			require.Equal(t, want, first)

			if tc.expected != "" {
				// V0 upgrades one way, so the upgraded bytes are what every
				// checkpoint after the resume carries. If they did not
				// re-encode to themselves the first checkpoint after a
				// resume would differ from the second.
				again, err := goldenDecodeEncode(want)
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
