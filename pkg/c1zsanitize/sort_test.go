package c1zsanitize

import (
	"testing"

	"github.com/stretchr/testify/require"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

// TestSortSyncsParentFirst exercises the stable topological refinement that
// orders source syncs parent-before-child. It is a refinement, not a re-sort:
// an already-valid order is preserved exactly, and only a child that precedes
// its in-set parent is moved.
func TestSortSyncsParentFirst(t *testing.T) {
	mk := func(id, parent string) *reader_v2.SyncRun {
		return reader_v2.SyncRun_builder{Id: id, ParentSyncId: parent}.Build()
	}
	ids := func(runs []*reader_v2.SyncRun) []string {
		out := make([]string, len(runs))
		for i, r := range runs {
			out[i] = r.GetId()
		}
		return out
	}

	tests := []struct {
		name string
		in   []*reader_v2.SyncRun
		want []string
	}{
		{
			name: "empty",
			in:   nil,
			want: []string{},
		},
		{
			name: "single",
			in:   []*reader_v2.SyncRun{mk("a", "")},
			want: []string{"a"},
		},
		{
			name: "already correct order preserved",
			in:   []*reader_v2.SyncRun{mk("p", ""), mk("c", "p")},
			want: []string{"p", "c"},
		},
		{
			name: "inversion is fixed",
			in:   []*reader_v2.SyncRun{mk("c", "p"), mk("p", "")},
			want: []string{"p", "c"},
		},
		{
			name: "external parent is a root, order preserved",
			in:   []*reader_v2.SyncRun{mk("a", "outside"), mk("b", "outside")},
			want: []string{"a", "b"},
		},
		{
			name: "three-level chain inverted",
			in:   []*reader_v2.SyncRun{mk("c", "b"), mk("b", "a"), mk("a", "")},
			want: []string{"a", "b", "c"},
		},
		{
			name: "stable among independent roots",
			in:   []*reader_v2.SyncRun{mk("r1", ""), mk("r2", ""), mk("c1", "r1")},
			want: []string{"r1", "r2", "c1"},
		},
		{
			name: "two-node cycle is flushed in input order, not dropped or looping",
			in:   []*reader_v2.SyncRun{mk("a", "b"), mk("b", "a")},
			want: []string{"a", "b"},
		},
		{
			name: "self-parent cycle flushed",
			in:   []*reader_v2.SyncRun{mk("p", ""), mk("x", "x")},
			want: []string{"p", "x"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sortSyncsParentFirst(tc.in)
			require.Equal(t, tc.want, ids(got))
			// Every input sync appears exactly once in the output.
			require.Len(t, got, len(tc.in))
		})
	}
}
