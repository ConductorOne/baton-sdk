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

func BenchmarkRegression_C1ZClose(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping regression benchmark in short mode")
	}

	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpDir, err := os.MkdirTemp("", "bench-close-*")
		require.NoError(b, err)

		c1zPath := filepath.Join(tmpDir, "close.c1z")
		c1f := seedRoundtripForBench(ctx, b, c1zPath, tmpDir)

		b.StartTimer()
		err = c1f.Close(ctx)
		b.StopTimer()

		require.NoError(b, err)
		os.RemoveAll(tmpDir)
	}
}

func BenchmarkRegression_C1ZOpen(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping regression benchmark in short mode")
	}

	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "bench-open-*")
	require.NoError(b, err)
	b.Cleanup(func() { os.RemoveAll(tmpDir) })

	c1zPath := filepath.Join(tmpDir, "open.c1z")
	c1f := seedRoundtripForBench(ctx, b, c1zPath, tmpDir)
	require.NoError(b, c1f.Close(ctx))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c1f2, err := NewC1ZFile(ctx, c1zPath, WithTmpDir(tmpDir), WithReadOnly(true))
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if err := c1f2.Close(ctx); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}

func seedRoundtripForBench(ctx context.Context, b *testing.B, c1zPath, tmpDir string) *C1File {
	b.Helper()

	c1f, err := NewC1ZFile(ctx, c1zPath, WithTmpDir(tmpDir))
	require.NoError(b, err)

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

	const batchSize = 1000
	buf := make([]*v2.Grant, 0, batchSize)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		require.NoError(b, c1f.PutGrants(ctx, buf...))
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

	require.NoError(b, c1f.EndSync(ctx))
	return c1f
}
