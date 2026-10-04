package dotc1z

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// A v1 file can hold any int64 grant rowids. Rowids 0 and MaxInt64 overflow
// the lane sampler's span; conversion must still succeed.
func TestSecurity_ToPebbleWithExtremeGrantRowids(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	v1Path := filepath.Join(dir, "v1.c1z")
	src, err := NewC1ZFile(ctx, v1Path, WithTmpDir(dir), WithEngine(c1zstore.EngineSQLite))
	require.NoError(t, err)
	_, err = src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice"), mkV2Grant("g2", "ent", "user", "bob")))
	for _, stmt := range []string{
		`UPDATE v1_grants SET id = 9223372036854775807 WHERE id = (SELECT MAX(id) FROM v1_grants)`,
		`UPDATE v1_grants SET id = 0 WHERE id = (SELECT MIN(id) FROM v1_grants)`,
	} {
		_, err = src.rawDb.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}
	require.NoError(t, src.Close(ctx))

	ro, err := NewC1ZFile(ctx, v1Path, WithTmpDir(dir), WithReadOnly(true))
	require.NoError(t, err)
	defer func() { _ = ro.Close(ctx) }()
	_, err = ro.ToPebble(ctx, filepath.Join(dir, "out.c1z"), "")
	require.NoError(t, err)
}
