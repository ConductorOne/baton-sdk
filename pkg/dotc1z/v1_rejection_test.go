package dotc1z

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestNewStoreRejectsV1Fixtures opens a copy of every committed v1 (SQLite)
// fixture through NewStore and checks the three properties the removal
// promises: the open fails with ErrV1Unsupported, the file is untouched, and
// nothing is written under the tmp dir. Read-only, writable, and
// explicit-Pebble opens are all covered.
func TestNewStoreRejectsV1Fixtures(t *testing.T) {
	ctx := context.Background()
	fixtures, err := filepath.Glob(filepath.Join("sqlite", "testdata", "*.c1z"))
	require.NoError(t, err)
	require.NotEmpty(t, fixtures, "no v1 fixtures found under pkg/dotc1z/sqlite/testdata")

	optionSets := map[string][]C1ZOption{
		"read-only": {WithReadOnly(true)},
		"writable":  nil,
		"pebble":    {WithEngine(c1zstore.EnginePebble)},
	}

	for _, fixture := range fixtures {
		original, err := os.ReadFile(fixture)
		require.NoError(t, err)
		require.NotEmpty(t, original, fixture)

		for name, opts := range optionSets {
			t.Run(filepath.Base(fixture)+"/"+name, func(t *testing.T) {
				dir := t.TempDir()
				path := filepath.Join(dir, "copy.c1z")
				require.NoError(t, os.WriteFile(path, original, 0o600)) // #nosec G703 -- path is under t.TempDir().
				tmpDir := filepath.Join(dir, "tmp")
				require.NoError(t, os.Mkdir(tmpDir, 0o700))

				store, err := NewStore(ctx, path, append(append([]C1ZOption{}, opts...), WithTmpDir(tmpDir))...)
				require.ErrorIs(t, err, ErrV1Unsupported)
				require.Nil(t, store)

				after, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, original, after, "v1 file must be byte-identical after a rejected open")

				entries, err := os.ReadDir(tmpDir)
				require.NoError(t, err)
				require.Empty(t, entries, "a rejected open must not write under the tmp dir")
			})
		}
	}
}
