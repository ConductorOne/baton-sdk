package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func TestToPebbleCommandConvertsSQLiteC1Z(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.c1z")
	outPath := filepath.Join(dir, "out.c1z")

	src, err := dotc1z.NewStore(ctx, srcPath, dotc1z.WithTmpDir(dir), dotc1z.WithEngine(c1zstore.EngineSQLite))
	require.NoError(t, err)

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()))
	user := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
	}.Build()
	require.NoError(t, src.PutResources(ctx, user))
	require.NoError(t, src.EndSync(ctx))
	require.NoError(t, src.Close(ctx))

	root := &cobra.Command{Use: "baton"}
	root.PersistentFlags().StringP("file", "f", "sync.c1z", "The path to the c1z file to work with.")
	root.AddCommand(toPebbleCmd())
	root.SetArgs([]string{
		"to-pebble",
		"--file", srcPath,
		"--out", outPath,
		"--sync-id", syncID,
	})
	require.NoError(t, root.Execute())

	dst, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close(ctx)) }()

	resp, err := dst.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	require.Equal(t, "u1", resp.GetList()[0].GetId().GetResource())
}
