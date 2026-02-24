package dotc1z

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/stretchr/testify/require"
)

const (
	roundtripGrantCount = 500_000
	roundtripMaxClose   = 2 * time.Minute
	roundtripMaxOpen    = 2 * time.Minute
)

// TestRegression_C1ZRoundTripPerformance creates a large c1z file, closes it
// (triggering WAL checkpoint + zstd compression), then reopens it (triggering
// zstd decompression + migration). Both close and reopen must complete within
// their respective time limits.
//
// A separate correctness check verifies data survives the round-trip intact.
func TestRegression_C1ZRoundTripPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping regression test in short mode")
	}

	ctx := t.Context()

	tmpDir, err := os.MkdirTemp("", "regression-roundtrip-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	c1zPath := filepath.Join(tmpDir, "roundtrip.c1z")

	c1f, err := NewC1ZFile(ctx, c1zPath, WithTmpDir(tmpDir))
	require.NoError(t, err)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))

	g1 := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		DisplayName: "Group 1",
	}.Build()
	require.NoError(t, c1f.PutResources(ctx, g1))

	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()
	require.NoError(t, c1f.PutEntitlements(ctx, ent1))

	const batchSize = 1000
	buf := make([]*v2.Grant, 0, batchSize)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		require.NoError(t, c1f.PutGrants(ctx, buf...))
		buf = buf[:0]
	}

	for i := range roundtripGrantCount {
		buf = append(buf, v2.Grant_builder{
			Id:          fmt.Sprintf("grant-%d", i),
			Entitlement: ent1,
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("u%d", i)}.Build(),
			}.Build(),
		}.Build())
		if len(buf) >= batchSize {
			flush()
		}
	}
	flush()

	require.NoError(t, c1f.EndSync(ctx))

	// --- measure close (WAL checkpoint + zstd compress) ---
	closeStart := time.Now()
	require.NoError(t, c1f.Close(ctx))
	closeElapsed := time.Since(closeStart)
	t.Logf("Close (%d grants): %v", roundtripGrantCount, closeElapsed)
	require.LessOrEqual(t, closeElapsed, roundtripMaxClose,
		"close took %v, limit is %v", closeElapsed, roundtripMaxClose)

	// Verify the compressed file exists and has reasonable size.
	info, err := os.Stat(c1zPath)
	require.NoError(t, err)
	t.Logf("Compressed c1z size: %d bytes (%.1f MB)", info.Size(), float64(info.Size())/(1<<20))
	require.Greater(t, info.Size(), int64(0))

	// --- measure open (zstd decompress + migrations + pragmas) ---
	openStart := time.Now()
	c1f2, err := NewC1ZFile(ctx, c1zPath, WithTmpDir(tmpDir), WithReadOnly(true))
	openElapsed := time.Since(openStart)
	require.NoError(t, err)
	defer c1f2.Close(ctx)

	t.Logf("Open (%d grants): %v", roundtripGrantCount, openElapsed)
	require.LessOrEqual(t, openElapsed, roundtripMaxOpen,
		"open took %v, limit is %v", openElapsed, roundtripMaxOpen)

	// Correctness: data survived the round-trip.
	require.NoError(t, c1f2.ViewSync(ctx, syncID))

	total := 0
	pageToken := ""
	for {
		resp, err := c1f2.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		total += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, roundtripGrantCount, total, "grant count mismatch after round-trip")
}
