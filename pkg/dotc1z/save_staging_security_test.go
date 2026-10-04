package dotc1z

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// The hosted runner saves into a 0600 os.CreateTemp placeholder in a shared
// directory. Another account there could plant the old fixed staging name,
// <out>.tmp, to fail every save, and the save used to republish the artifact
// 0644. The save must ignore the planted entry and keep the placeholder's mode.
func TestSecurity_SaveIntoPrivatePlaceholder(t *testing.T) {
	ctx := context.Background()
	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			dir := t.TempDir()
			placeholder, err := os.CreateTemp(dir, "baton-sdk-sync-upload")
			require.NoError(t, err)
			require.NoError(t, placeholder.Close())
			c1zPath := placeholder.Name()
			planted := filepath.Join(c1zPath+".tmp", "blocker")
			require.NoError(t, os.MkdirAll(planted, 0o755))

			store, err := NewStore(ctx, c1zPath, WithTmpDir(t.TempDir()), WithEngine(engine))
			require.NoError(t, err)
			_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			require.NoError(t, store.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")))
			require.NoError(t, store.EndSync(ctx))
			require.NoError(t, store.Close(ctx))

			require.DirExists(t, planted)
			fi, err := os.Stat(c1zPath)
			require.NoError(t, err)
			require.NotZero(t, fi.Size())
			if runtime.GOOS != "windows" {
				require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
			}
		})
	}
}
