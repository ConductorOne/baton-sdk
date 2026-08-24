package dotc1z

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestToPebbleNoConvertibleSyncEmptySyncID pins that ToPebble with syncID ""
// errors — and leaves no partial output file — when sync runs exist but none
// has a convertible type (resolveConvertSyncID's allowlist: full,
// resources_only, partial). This retargets the deleted
// TestToPebbleDiffOnlyEmptySyncID, which used the removed diff sync types as
// its non-convertible seed. StartNewSync rejects unknown types, so the row is
// rewritten with raw SQL — the same shape as a file written by a different
// SDK version whose sync types this SDK does not know.
func TestToPebbleNoConvertibleSyncEmptySyncID(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	src, err := NewC1ZFile(ctx, filepath.Join(dir, "unconvertible.c1z"), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	_, err = src.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	_, err = src.db.ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET sync_type = 'frobnitz'", syncRuns.Name()))
	require.NoError(t, err)

	outPath := filepath.Join(dir, "out.c1z")
	_, err = src.ToPebble(ctx, outPath, "")
	require.ErrorContains(t, err, "no convertible sync found")
	_, statErr := os.Stat(outPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}
