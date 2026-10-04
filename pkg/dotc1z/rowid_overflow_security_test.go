package dotc1z

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestSecurity_RowidSpanOverflowSkipsSampling: the grants table's id is a
// plain SQLite integer primary key, so a hostile v1 c1z can store any signed
// 64-bit rowid. sampleGrantBoundaries computed span := maxID - minID + 1 with
// unchecked arithmetic; ids {0, MaxInt64} wrap the span negative and
// rng.Int63n(span) panicked the converting process — a poison-pill against
// the shared compaction worker that converts every v1 input. Sampling is a
// pure lane-balance optimization, so a degenerate span now skips sampling.
func TestSecurity_RowidSpanOverflowSkipsSampling(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	// A v1 store with a sync and one grant row, then hostile rowids planted
	// directly in the grants table (the conversion reads MIN(id)/MAX(id)).
	v1Path := filepath.Join(dir, "hostile.c1z")
	src, err := NewC1ZFile(ctx, v1Path, WithTmpDir(dir), WithEngine(c1zstore.EngineSQLite))
	require.NoError(t, err)
	_, err = src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// One real grant row through the writer API so the table exists, then
	// re-plant its rowids with the hostile extremes.
	require.NoError(t, src.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")))
	db := src.rawDb
	_, err = db.Exec(`UPDATE v1_grants SET id = 0`)
	require.NoError(t, err)
	// Duplicate the one real row with a different external id: every NOT
	// NULL column is satisfied by construction. The grants table's unique
	// index is (external_id, sync_id), so a fresh external_id is enough.
	_, err = db.Exec(`INSERT INTO v1_grants (resource_type_id, resource_id, entitlement_id, ` +
		`principal_resource_type_id, principal_resource_id, external_id, expansion, needs_expansion, data, sync_id, discovered_at) ` +
		`SELECT resource_type_id, resource_id, entitlement_id, principal_resource_type_id, ` +
		`principal_resource_id, 'hostile-b', expansion, needs_expansion, data, sync_id, discovered_at ` +
		`FROM v1_grants LIMIT 1`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE v1_grants SET id = 9223372036854775807 WHERE external_id = 'hostile-b'`)
	require.NoError(t, err)
	require.NoError(t, src.Close(ctx))

	// Reopen the saved v1 file and convert: the lane sampler reads
	// MIN(id)/MAX(id) straight from the file, hits the wrapped span, and
	// (pre-fix) panicked in rng.Int63n.
	ro, err := NewC1ZFile(ctx, v1Path, WithTmpDir(dir), WithReadOnly(true))
	require.NoError(t, err)
	defer func() { _ = ro.Close(ctx) }()

	outPath := filepath.Join(dir, "out.c1z")
	convertErr := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = errPanicked{r}
			}
		}()
		_, err = ro.ToPebble(ctx, outPath, "")
		return err
	}()
	require.NoError(t, convertErr, "conversion of hostile rowids must not fail, let alone panic")
}

type errPanicked struct{ v any }

func (e errPanicked) Error() string { return "panicked" }
