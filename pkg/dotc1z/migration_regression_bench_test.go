package dotc1z

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/stretchr/testify/require"
)

func BenchmarkRegression_Migration(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping regression benchmark in short mode")
	}

	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "bench-migration-*")
	require.NoError(b, err)
	b.Cleanup(func() { os.RemoveAll(tmpDir) })

	c1zPath := filepath.Join(tmpDir, "migration.c1z")
	seedLargeC1ZForBench(ctx, b, c1zPath, tmpDir, migrationGrantCount)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c1f, err := NewC1ZFile(ctx, c1zPath, WithTmpDir(tmpDir))
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if err := c1f.Close(ctx); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}

// seedLargeC1ZForBench is the benchmark-compatible version of seedLargeC1Z.
func seedLargeC1ZForBench(ctx context.Context, b *testing.B, c1zPath, tmpDir string, numGrants int) {
	b.Helper()

	c1f, err := NewC1ZFile(ctx, c1zPath, WithTmpDir(tmpDir))
	require.NoError(b, err)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(b, err)
	_ = syncID

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	roleRT := v2.ResourceType_builder{Id: "role", DisplayName: "Role"}.Build()
	require.NoError(b, c1f.PutResourceTypes(ctx, groupRT, userRT, roleRT))

	const numGroups = 50
	const numEntitlements = 100

	groupResources := make([]*v2.Resource, numGroups)
	for i := range numGroups {
		groupResources[i] = v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "group", Resource: fmt.Sprintf("group-%d", i)}.Build(),
			DisplayName: fmt.Sprintf("Group %d", i),
		}.Build()
	}
	require.NoError(b, c1f.PutResources(ctx, groupResources...))

	ents := make([]*v2.Entitlement, numEntitlements)
	for i := range numEntitlements {
		ents[i] = v2.Entitlement_builder{
			Id:       fmt.Sprintf("ent-%d", i),
			Resource: groupResources[i%numGroups],
		}.Build()
	}
	require.NoError(b, c1f.PutEntitlements(ctx, ents...))

	expandable := v2.GrantExpandable_builder{
		EntitlementIds:  []string{"ent-1"},
		Shallow:         false,
		ResourceTypeIds: []string{"user"},
	}.Build()

	const batchSize = 1000
	buf := make([]*v2.Grant, 0, batchSize)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		require.NoError(b, c1f.PutGrants(ctx, buf...))
		buf = buf[:0]
	}

	for i := range numGrants {
		ent := ents[i%numEntitlements]
		gb := v2.Grant_builder{
			Id:          fmt.Sprintf("grant-%d", i),
			Entitlement: ent,
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("u%d", i)}.Build(),
			}.Build(),
		}
		if i%10 == 0 {
			gb.Annotations = annotations.New(expandable)
		}
		buf = append(buf, gb.Build())
		if len(buf) >= batchSize {
			flush()
		}
	}
	flush()

	const userResourceBatch = 5000
	userBuf := make([]*v2.Resource, 0, userResourceBatch)
	for i := range numGrants {
		userBuf = append(userBuf, v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("u%d", i)}.Build(),
			DisplayName: fmt.Sprintf("User %d", i),
		}.Build())
		if len(userBuf) >= userResourceBatch {
			require.NoError(b, c1f.PutResources(ctx, userBuf...))
			userBuf = userBuf[:0]
		}
	}
	if len(userBuf) > 0 {
		require.NoError(b, c1f.PutResources(ctx, userBuf...))
	}

	require.NoError(b, c1f.EndSync(ctx))
	require.NoError(b, c1f.Close(ctx))
}
