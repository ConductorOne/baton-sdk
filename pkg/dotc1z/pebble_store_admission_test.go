package dotc1z

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/stretchr/testify/require"
)

const admissionTestTimeout = 5 * time.Second

func newAdmissionTestStore(t *testing.T, path string) *pebbleStore {
	t.Helper()
	store, err := pebbleDriver{}.OpenStore(context.Background(), path, StoreOptions{})
	require.NoError(t, err)
	return store.(*pebbleStore)
}

func waitForAdmissionState(t *testing.T, store *pebbleStore, want pebbleStoreAdmissionState) {
	t.Helper()
	deadline := time.Now().Add(admissionTestTimeout)
	for time.Now().Before(deadline) {
		store.closeMu.Lock()
		got := store.admission
		store.closeMu.Unlock()
		if got == want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("timed out waiting for admission state %v", want)
}

func receiveError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(admissionTestTimeout):
		t.Fatal("timed out waiting for operation")
		return nil
	}
}

func TestPebbleStoreMutationAdmissionPersistsBeforeClose(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "admission.c1z")
	store := newAdmissionTestStore(t, path)
	_, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	admitted := make(chan struct{})
	release := make(chan struct{})
	store.mutationAdmissionHook = func() {
		close(admitted)
		<-release
	}
	mutationErr := make(chan error, 1)
	go func() {
		mutationErr <- store.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build())
	}()
	select {
	case <-admitted:
	case <-time.After(admissionTestTimeout):
		t.Fatal("mutation was not admitted")
	}

	closeErr := make(chan error, 2)
	go func() { closeErr <- store.Close(ctx) }()
	waitForAdmissionState(t, store, pebbleStoreClosing)
	go func() { closeErr <- store.Close(ctx) }()
	require.ErrorIs(t, store.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "late"}.Build()), pebble.ErrEngineClosing)

	select {
	case err := <-closeErr:
		t.Fatalf("Close returned before admitted mutation completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	require.NoError(t, receiveError(t, mutationErr))
	require.NoError(t, receiveError(t, closeErr))
	require.NoError(t, receiveError(t, closeErr))

	reopened, err := NewStore(ctx, path, WithReadOnly(true))
	require.NoError(t, err)
	defer reopened.Close(ctx)
	engine, ok := pebble.AsEngine(reopened)
	require.True(t, ok)
	rec, err := engine.GetResourceTypeRecord(ctx, "group")
	require.NoError(t, err)
	require.Equal(t, "Group", rec.GetDisplayName())
}

func TestPebbleStoreSessionAdmissionPersistsBeforeClose(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "session-admission.c1z")
	store := newAdmissionTestStore(t, path)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	admitted := make(chan struct{})
	release := make(chan struct{})
	store.mutationAdmissionHook = func() {
		close(admitted)
		<-release
	}
	mutationErr := make(chan error, 1)
	go func() {
		mutationErr <- store.SessionStore().Set(ctx, "racing", []byte("persisted"), sessions.WithSyncID(syncID))
	}()
	select {
	case <-admitted:
	case <-time.After(admissionTestTimeout):
		t.Fatal("session mutation was not admitted")
	}
	closeErr := make(chan error, 1)
	go func() { closeErr <- store.Close(ctx) }()
	waitForAdmissionState(t, store, pebbleStoreClosing)
	close(release)
	require.NoError(t, receiveError(t, mutationErr))
	require.NoError(t, receiveError(t, closeErr))

	reopened, err := NewStore(ctx, path, WithReadOnly(true))
	require.NoError(t, err)
	defer reopened.Close(ctx)
	value, found, err := reopened.SessionStore().Get(ctx, "racing", sessions.WithSyncID(syncID))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("persisted"), value)
}

func TestPebbleStoreSaveFailureReopensAdmissionForRetry(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	parent := filepath.Join(root, "missing")
	path := filepath.Join(parent, "retry.c1z")
	store := newAdmissionTestStore(t, path)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.Error(t, store.Close(ctx))

	require.NoError(t, store.SessionStore().Set(ctx, "after-failure", []byte("kept"), sessions.WithSyncID(syncID)))
	require.NoError(t, os.MkdirAll(parent, 0o755))
	require.NoError(t, store.Close(ctx))

	reopened, err := NewStore(ctx, path, WithReadOnly(true))
	require.NoError(t, err)
	defer reopened.Close(ctx)
	value, found, err := reopened.SessionStore().Get(ctx, "after-failure", sessions.WithSyncID(syncID))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("kept"), value)
}

func TestPebbleStorePartialMutationErrorRemainsDirty(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "partial-error.c1z")
	initial := newAdmissionTestStore(t, path)
	syncID, err := initial.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, initial.EndSync(ctx))
	require.NoError(t, initial.Close(ctx))

	store := newAdmissionTestStore(t, path)

	injected := errors.New("after commit")
	err = pebble.WithEngineMutation(ctx, store, func(ctx context.Context, engine *pebble.Engine) error {
		if err := engine.SetCurrentSync(ctx, syncID); err != nil {
			return err
		}
		if err := engine.PutResourceTypeRecord(ctx, v3.ResourceTypeRecord_builder{
			ExternalId:  "partial",
			DisplayName: "Partial",
		}.Build()); err != nil {
			return err
		}
		return injected
	})
	require.ErrorIs(t, err, injected)
	require.NoError(t, store.Close(ctx))

	reopened, err := NewStore(ctx, path, WithReadOnly(true))
	require.NoError(t, err)
	defer reopened.Close(ctx)
	engine, ok := pebble.AsEngine(reopened)
	require.True(t, ok)
	_, err = engine.GetResourceTypeRecord(ctx, "partial")
	require.NoError(t, err)
}

func TestPebbleStoreCloseEngineOnlyAdmission(t *testing.T) {
	ctx := context.Background()
	clean := newAdmissionTestStore(t, filepath.Join(t.TempDir(), "clean.c1z"))
	require.NoError(t, clean.CloseEngineOnly())
	require.ErrorIs(t, clean.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "closed"}.Build()), pebble.ErrEngineClosing)

	path := filepath.Join(t.TempDir(), "dirty.c1z")
	dirty := newAdmissionTestStore(t, path)
	_, err := dirty.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.Error(t, dirty.CloseEngineOnly())
	require.NoError(t, dirty.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "retry"}.Build()))
	require.NoError(t, dirty.Close(ctx))
}

func TestPebbleStoreWrapperFamiliesRejectAfterClosing(t *testing.T) {
	ctx := context.Background()
	store := newAdmissionTestStore(t, filepath.Join(t.TempDir(), "families.c1z"))
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	admitted := make(chan struct{})
	release := make(chan struct{})
	store.mutationAdmissionHook = func() {
		close(admitted)
		<-release
	}
	mutationErr := make(chan error, 1)
	go func() {
		mutationErr <- store.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "blocking"}.Build())
	}()
	select {
	case <-admitted:
	case <-time.After(admissionTestTimeout):
		t.Fatal("mutation was not admitted")
	}

	closeErr := make(chan error, 1)
	go func() { closeErr <- store.Close(ctx) }()
	waitForAdmissionState(t, store, pebbleStoreClosing)

	require.ErrorIs(t, store.SyncMeta().RecalculateStats(ctx, syncID), pebble.ErrEngineClosing, "sync metadata")
	_, err = store.FileOps().GenerateSyncDiff(ctx, syncID, syncID)
	require.ErrorIs(t, err, pebble.ErrEngineClosing, "file operations")
	require.ErrorIs(t, store.Grants().StoreExpandedGrants(ctx), pebble.ErrEngineClosing, "grant storage")
	require.ErrorIs(t, store.SessionStore().Set(ctx, "late", []byte("value"), sessions.WithSyncID(syncID)), pebble.ErrEngineClosing, "session storage")
	require.ErrorIs(t, store.EnsureGrantIndexes(ctx), pebble.ErrEngineClosing, "grant-index repair")
	layerStore := store.Grants().(pebbleStoreGrantLayerStorer)
	_, err = layerStore.BeginExpandedGrantLayer(ctx)
	require.ErrorIs(t, err, pebble.ErrEngineClosing, "expanded-grant session")

	close(release)
	require.NoError(t, receiveError(t, mutationErr))
	require.NoError(t, receiveError(t, closeErr))
}

func TestPebbleStoreAbandonedSessionsDoNotBlockClose(t *testing.T) {
	t.Run("bulk import", func(t *testing.T) {
		ctx := context.Background()
		store := newAdmissionTestStore(t, filepath.Join(t.TempDir(), "bulk.c1z"))
		syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		var bulk *pebble.BulkSyncImport
		require.NoError(t, pebble.WithEngineMutation(ctx, store, func(ctx context.Context, engine *pebble.Engine) error {
			var err error
			bulk, err = engine.StartBulkSyncImport(ctx, syncID, t.TempDir())
			return err
		}))
		closeErr := make(chan error, 1)
		go func() { closeErr <- store.Close(ctx) }()
		require.NoError(t, receiveError(t, closeErr))
		bulk.Abort()
	})

	t.Run("expanded grant layer", func(t *testing.T) {
		t.Setenv("BATON_PEBBLE_SYNTH_LAYER_SEGMENT_ROWS", "1")
		ctx := context.Background()
		store := newAdmissionTestStore(t, filepath.Join(t.TempDir(), "expanded.c1z"))
		_, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		started, err := store.Grants().(interface {
			BeginExpandedGrantLayer(context.Context) (bool, error)
		}).BeginExpandedGrantLayer(ctx)
		require.NoError(t, err)
		require.True(t, started)
		layerStore := store.Grants().(pebbleStoreGrantLayerStorer)
		dest := v2.Entitlement_builder{
			Id: "member",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
			}.Build(),
		}.Build()
		principal := v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "u1"}.Build()
		require.NoError(t, layerStore.AddExpandedGrantLayerContributions(
			ctx,
			dest,
			[]*v3.PrincipalRef{principal},
			[]batonGrant.Sources{{{EntitlementID: "source", IsDirect: true}}},
		))
		closeErr := make(chan error, 1)
		go func() { closeErr <- store.Close(ctx) }()
		require.NoError(t, receiveError(t, closeErr))
	})
}

func TestPebbleStoreMutationWrapperInventory(t *testing.T) {
	fset := token.NewFileSet()
	files := []string{"pebble_store.go", "pebble_store_session.go", "ingest_invariant_store.go"}
	parsed := make(map[string]*ast.File, len(files))
	for _, name := range files {
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		parsed[name] = file
	}

	required := map[string]bool{
		"GenerateSyncDiff": false, "MarkSyncSupportsDiff": false, "MarkIngestInvariantsVerified": false,
		"ClearIngestInvariantVerification": false, "RecalculateStats": false, "NormalizeForFixtureSave": false,
		"StartNewSync": false, "StartNewSyncWithID": false, "ResumeSync": false, "StartOrResumeSync": false,
		"SetCurrentSync": false, "CheckpointSync": false, "EndSync": false, "PutAsset": false,
		"SetSupportsDiff": false, "SetSyncLink": false, "PutGrants": false, "UnsafePutUniqueGrants": false,
		"PutResourceTypes": false, "PutResources": false, "PutEntitlements": false, "DeleteGrant": false,
		"DeleteGrantByRefs": false, "DeleteResourceRecord": false, "DeleteEntitlementByRefs": false,
		"StoreExpandedGrants": false, "StoreNewExpandedGrants": false, "StoreNewExpandedGrantContributions": false,
		"BeginExpandedGrantLayer": false, "AddExpandedGrantLayerContributions": false,
		"FinishExpandedGrantLayer": false, "AbortExpandedGrantLayer": false,
		"Set": false, "SetMany": false, "Delete": false, "Clear": false, "EnsureGrantIndexes": false,
	}
	for _, file := range parsed {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if _, tracked := required[fn.Name.Name]; !tracked {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if ok && sel.Sel.Name == "withMutation" {
					required[fn.Name.Name] = true
				}
				return true
			})
		}
	}
	for name, guarded := range required {
		require.Truef(t, guarded, "mutating wrapper %s must call withMutation", name)
	}

	require.NoError(t, filepath.WalkDir("..", func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.Ident:
				require.NotEqualf(t, "MarkStoreDirty", n.Name, "%s uses forbidden post-write dirty marking", path)
			case *ast.SelectorExpr:
				require.NotEqualf(t, "markDirty", n.Sel.Name, "%s uses forbidden post-write dirty marking", path)
			}
			return true
		})
		return nil
	}))
}

var _ c1zstore.Store = (*pebbleStore)(nil)
