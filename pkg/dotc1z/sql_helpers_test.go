package dotc1z

import (
	"context"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

func generateResources(count int, resourceType *v2.ResourceType) []*v2.Resource {
	if count < 1 {
		return nil
	}

	response := make([]*v2.Resource, count)

	for i := range count {
		id := ksuid.New().String()

		newResource, err := resource.NewResource(id, resourceType, id)
		if err != nil {
			panic(err)
		}

		response[i] = newResource
	}

	return response
}

func TestPutResources(t *testing.T) {
	ctx := context.Background()

	tempDir := filepath.Join(t.TempDir(), "test.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)

	syncId, err := c1zFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	resourceType := &v2.ResourceType{
		Id:          "test_resource_type",
		DisplayName: "Test Resource Type",
	}
	err = c1zFile.PutResourceTypes(ctx, resourceType)
	require.NoError(t, err)

	err = c1zFile.PutResources(
		ctx,
		generateResources(10_000, resourceType)...,
	)
	require.NoError(t, err)

	err = c1zFile.EndSync(ctx)
	require.NoError(t, err)

	stats, err := c1zFile.Stats(ctx, connectorstore.SyncTypeFull, syncId)
	require.NoError(t, err)

	require.Equal(t, int64(10_000), stats[resourceType.Id])

	err = c1zFile.Close()
	require.NoError(t, err)
}

func BenchmarkPutResources(b *testing.B) {
	cases := []struct {
		name           string
		resourcesCount int
	}{
		{"500", 500},
		{"1k", 1_000},
		{"10k", 10_000},
		{"50k", 50_000},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				ctx := context.Background()

				tempDir := filepath.Join(b.TempDir(), "test.c1z")

				c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
				require.NoError(b, err)

				_, err = c1zFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(b, err)

				resourceType := &v2.ResourceType{
					Id:          "test_resource_type",
					DisplayName: "Test Resource Type",
				}
				err = c1zFile.PutResourceTypes(ctx, resourceType)
				require.NoError(b, err)

				generatedData := generateResources(c.resourcesCount, resourceType)

				b.StartTimer()

				err = c1zFile.PutResources(
					ctx,
					generatedData...,
				)
				require.NoError(b, err)

				b.StopTimer()

				err = c1zFile.EndSync(ctx)
				require.NoError(b, err)

				err = c1zFile.Close()
				require.NoError(b, err)
			}
		})
	}
}
