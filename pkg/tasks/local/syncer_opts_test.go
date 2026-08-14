package local

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
)

// The local task path carries configuration to the sync engine through the
// same shape the service-mode path does -- option constructor, task field,
// SyncOpt -- so it gets the same treatment: run the assembled options through a
// real engine and read the field they were supposed to set.
//
// The fields are unexported and a SyncOpt is an opaque closure that cannot be
// identified or applied from outside pkg/sync, so the assertion reads them
// reflectively. Same trade-off, and same twin, as
// pkg/tasks/c1api/full_sync_opts_test.go: the alternative is asserting nothing.
func localSyncerEngine(t *testing.T, opts ...Option) reflect.Value {
	t.Helper()

	syncer := &localSyncer{dbPath: filepath.Join(t.TempDir(), "sync.c1z")}
	for _, opt := range opts {
		opt(syncer)
	}

	created, err := sdkSync.NewSyncer(t.Context(), nil, syncer.syncOpts(nil)...)
	require.NoError(t, err)

	value := reflect.ValueOf(created)
	require.Equal(t, reflect.Ptr, value.Kind(), "NewSyncer must return a pointer to inspect")

	field := value.Elem().FieldByName("externalPrincipalIndexEnabled")
	require.True(t, field.IsValid(),
		"pkg/sync syncer has no field \"externalPrincipalIndexEnabled\": this test reads it by "+
			"name to prove local-mode configuration reaches the sync engine, so a rename needs "+
			"updating here too")
	return field
}

func TestLocalSyncerThreadsExternalPrincipalIndex(t *testing.T) {
	require.False(t, localSyncerEngine(t).Bool(),
		"the linear scan is the default and must survive a task configured with no options")
	require.False(t, localSyncerEngine(t, WithExternalPrincipalIndex(false)).Bool())
	require.True(t, localSyncerEngine(t, WithExternalPrincipalIndex(true)).Bool(),
		"a local task opted into the indexed matcher must reach the engine with it enabled")
}
