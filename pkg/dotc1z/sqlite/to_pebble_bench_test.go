package sqlite_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	sqlitereader "github.com/conductorone/baton-sdk/pkg/dotc1z/sqlite"
	"github.com/stretchr/testify/require"
)

func BenchmarkToPebbleReference(b *testing.B) {
	ctx := context.Background()
	sourcePath := filepath.Join("testdata", "edge_keys.c1z")
	source, err := dotc1z.NewC1ZFile(ctx, sourcePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(b.TempDir()))
	require.NoError(b, err)
	defer func() { require.NoError(b, source.Close(ctx)) }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputPath := filepath.Join(b.TempDir(), fmt.Sprintf("reference-%d.c1z", i))
		_, err := source.ToPebble(ctx, outputPath, "")
		require.NoError(b, err)
		require.NoError(b, os.Remove(outputPath))
	}
}

func BenchmarkToPebbleExtracted(b *testing.B) {
	ctx := context.Background()
	source, err := sqlitereader.Open(
		ctx,
		filepath.Join("testdata", "edge_keys.c1z"),
		sqlitereader.WithTmpDir(b.TempDir()),
	)
	require.NoError(b, err)
	defer func() { require.NoError(b, source.Close(ctx)) }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputPath := filepath.Join(b.TempDir(), fmt.Sprintf("extracted-%d.c1z", i))
		_, err := source.ToPebble(ctx, outputPath, "")
		require.NoError(b, err)
		require.NoError(b, os.Remove(outputPath))
	}
}
