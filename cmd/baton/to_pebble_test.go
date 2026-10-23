package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// slimGrantsFixture is a committed v1 (SQLite) c1z with one finished full
// sync holding three grants; see pkg/dotc1z/sqlite/testdata.
const slimGrantsFixture = "../../pkg/dotc1z/sqlite/testdata/slim_grants.c1z"

func TestToPebbleCommandConvertsSQLiteC1Z(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.c1z")
	outPath := filepath.Join(dir, "out.c1z")

	fixture, err := os.ReadFile(slimGrantsFixture)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(srcPath, fixture, 0600)) // #nosec G703 -- srcPath is under t.TempDir().

	root := &cobra.Command{Use: "baton"}
	root.PersistentFlags().StringP("file", "f", "sync.c1z", "The path to the c1z file to work with.")
	root.AddCommand(toPebbleCmd())
	root.SetArgs([]string{
		"to-pebble",
		"--file", srcPath,
		"--out", outPath,
	})
	require.NoError(t, root.Execute())

	dst, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close(ctx)) }()

	latest, err := dst.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, latest)
	require.NoError(t, dst.SetCurrentSync(ctx, latest.ID))

	resp, err := dst.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 3)
}
