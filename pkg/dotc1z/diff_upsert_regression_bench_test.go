package dotc1z

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/stretchr/testify/require"
)

func BenchmarkRegression_DiffUpsert(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping regression benchmark in short mode")
	}

	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "bench-diff-upsert-*")
	require.NoError(b, err)
	b.Cleanup(func() { os.RemoveAll(tmpDir) })

	c1zPath := filepath.Join(tmpDir, "diff.c1z")

	c1f, err := NewC1ZFile(ctx, c1zPath, WithTmpDir(tmpDir))
	require.NoError(b, err)
	b.Cleanup(func() { _ = c1f.Close(ctx) })

	_, err = c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(b, err)

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	require.NoError(b, c1f.PutResourceTypes(ctx, groupRT, userRT))

	g1 := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		DisplayName: "Group 1",
	}.Build()
	require.NoError(b, c1f.PutResources(ctx, g1))

	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()
	require.NoError(b, c1f.PutEntitlements(ctx, ent1))

	makeGrants := func(prefix string) []*v2.Grant {
		out := make([]*v2.Grant, diffUpsertGrantCount)
		for i := range out {
			out[i] = v2.Grant_builder{
				Id:          fmt.Sprintf("grant-%d", i),
				Entitlement: ent1,
				Principal: v2.Resource_builder{
					Id:          v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("u%d", i)}.Build(),
					DisplayName: fmt.Sprintf("%s %d", prefix, i),
				}.Build(),
			}.Build()
		}
		return out
	}

	// Seed baseline.
	baseline := makeGrants("baseline")
	const batchSize = 1000
	for i := 0; i < len(baseline); i += batchSize {
		end := min(i+batchSize, len(baseline))
		require.NoError(b, c1f.PutGrants(ctx, baseline[i:end]...))
	}

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		updated := makeGrants(fmt.Sprintf("iter%d", iter))
		for i := 0; i < len(updated); i += batchSize {
			end := min(i+batchSize, len(updated))
			if err := c1f.PutGrantsIfNewer(ctx, updated[i:end]...); err != nil {
				b.Fatal(err)
			}
		}
	}
}
