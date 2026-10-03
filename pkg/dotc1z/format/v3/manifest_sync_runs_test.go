package v3

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v3pb "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// A written sync_runs projection reads back whole while it fits
// maxManifestSyncRunsBytes. One run past the budget, the header read drops it
// instead of failing, so a file that retained many syncs still opens.
func TestManifestSyncRunsProjectionBudget(t *testing.T) {
	run := func(i int) *c1zv3.SyncRunSummary {
		return c1zv3.SyncRunSummary_builder{
			SyncId: fmt.Sprintf("sync-%08d", i),
			Type:   v3pb.SyncType_SYNC_TYPE_FULL,
		}.Build()
	}
	fitting := maxManifestSyncRunsBytes / proto.Size(run(0))

	for _, tc := range []struct {
		runs     int
		wantRuns int
	}{
		{runs: 2, wantRuns: 2},
		{runs: fitting, wantRuns: fitting},
		{runs: fitting + 1, wantRuns: 0},
	} {
		t.Run(fmt.Sprint(tc.runs), func(t *testing.T) {
			runs := make([]*c1zv3.SyncRunSummary, tc.runs)
			for i := range runs {
				runs[i] = run(i)
			}
			var buf bytes.Buffer
			require.NoError(t, WriteEnvelope(&buf, c1zv3.C1ZManifestV3_builder{Engine: "pebble", SyncRuns: runs}.Build(), t.TempDir()))

			got, err := ReadManifestHeader(&buf)
			require.NoError(t, err)
			require.Equal(t, "pebble", got.GetEngine())
			require.Len(t, got.GetSyncRuns(), tc.wantRuns)
			if tc.wantRuns > 0 {
				require.Equal(t, runs[tc.wantRuns-1].GetSyncId(), got.GetSyncRuns()[tc.wantRuns-1].GetSyncId())
			}
		})
	}
}
