package dotc1z

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// Pin the resolver's new contract: an empty c1z surfaces sql.ErrNoRows
// rather than silently running an unscoped grants query across every
// sync_id in the file.

func TestListWithAnnotationsPage_ErrNoRowsWhenNoSyncs(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "empty.c1z")

	c1f, err := NewC1ZFile(ctx, path)
	require.NoError(t, err)
	defer func() { _ = c1f.Close(ctx) }()

	// No StartNewSync, no view, no cached sync run.
	_, _, err = c1f.Grants().ListWithAnnotationsPage(ctx, "")
	require.Error(t, err)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.True(t, errors.Is(err, sql.ErrNoRows))
}

func TestListWithAnnotationsPage_SucceedsAfterSyncCommitted(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t)
	defer cleanup()

	require.NoError(t, c1f.EndSync(ctx))

	rows, _, err := c1f.Grants().ListWithAnnotationsPage(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, rows)
}

func TestResolveSyncIDForPayloadQuery_AllBranches(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t)
	defer cleanup()

	id, err := c1f.resolveSyncIDForPayloadQuery(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, id)

	require.NoError(t, c1f.EndSync(ctx))
	id2, err := c1f.resolveSyncIDForPayloadQuery(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, id2)

	path := filepath.Join(t.TempDir(), "empty.c1z")
	emptyC1f, err := NewC1ZFile(ctx, path)
	require.NoError(t, err)
	defer func() { _ = emptyC1f.Close(ctx) }()

	_, err = emptyC1f.resolveSyncIDForPayloadQuery(ctx)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

var _ = connectorstore.SyncTypeFull
