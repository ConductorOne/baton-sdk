package dotc1z

import (
	"bytes"
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

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

func TestPebbleStoreCloseReportsTerminalErrorOnlyOnce(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the directory permissions this test relies on")
	}
	ctx := context.Background()
	root := t.TempDir()
	// The store unpacks into a child of workDir, so making workDir unwritable
	// after the open leaves Close unable to unlink that child. That is the
	// cheapest way to reach the closed state carrying an error: the save itself
	// still succeeds, only the temp-dir cleanup fails.
	workDir := filepath.Join(root, "work")
	require.NoError(t, os.MkdirAll(workDir, 0o755))
	opened, err := pebbleDriver{}.OpenStore(ctx, filepath.Join(root, "terminal.c1z"), StoreOptions{TmpDir: workDir})
	require.NoError(t, err)
	store := opened.(*pebbleStore)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, os.Chmod(workDir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(workDir, 0o755) })

	require.Error(t, store.Close(ctx))

	// The store is closed. Later calls have no attempt of their own to fail, so
	// they report nil per io.Closer convention — a deferred Close paired with an
	// explicit one must not resurface an error the caller already handled.
	require.NoError(t, store.Close(ctx))
	require.NoError(t, store.CloseEngineOnly())
}

func TestPebbleStoreCloseDoesNotInheritAnotherTeardownsFailure(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "handoff.c1z")
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

	// CloseEngineOnly owns the teardown and parks draining the admitted write.
	// Close arrives second and parks waiting on that attempt.
	engineOnlyErr := make(chan error, 1)
	go func() { engineOnlyErr <- store.CloseEngineOnly() }()
	waitForAdmissionState(t, store, pebbleStoreClosing)
	closeErr := make(chan error, 1)
	go func() { closeErr <- store.Close(ctx) }()
	select {
	case err := <-closeErr:
		t.Fatalf("Close returned before the owning teardown finished: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	require.NoError(t, receiveError(t, mutationErr))
	// The store is dirty, so CloseEngineOnly refuses to discard it.
	require.Error(t, receiveError(t, engineOnlyErr))
	// That refusal says nothing about whether Close should save, so Close runs
	// its own attempt. Adopting the other caller's error instead left the sync
	// stranded in the temp dir with no envelope on disk.
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

func TestPebbleStoreFoldAccountingIgnoresFailedFolds(t *testing.T) {
	ctx := context.Background()
	store := newAdmissionTestStore(t, filepath.Join(t.TempDir(), "fold-accounting.c1z"))
	store.closeMu.Lock()
	baseline := store.foldDeadBytes
	store.closeMu.Unlock()

	injected := errors.New("fold failed")
	err := pebble.WithEngineFoldMutation(ctx, store, func(ctx context.Context, _ *pebble.Engine) (int64, error) {
		return 4096, injected
	})
	require.ErrorIs(t, err, injected)

	store.closeMu.Lock()
	require.False(t, store.dirty, "a failed fold marked the store dirty, so Close would spend a full envelope save on an output the compactor is about to discard")
	require.Equal(t, baseline, store.foldDeadBytes, "a failed fold recorded bytes it never shadowed, pulling the rebuild cutover forward")
	store.closeMu.Unlock()

	require.NoError(t, pebble.WithEngineFoldMutation(ctx, store, func(ctx context.Context, _ *pebble.Engine) (int64, error) {
		return 4096, nil
	}))
	store.closeMu.Lock()
	require.True(t, store.dirty, "a fold that landed must mark the store dirty so Close writes the envelope")
	require.Equal(t, baseline+4096, store.foldDeadBytes)
	store.closeMu.Unlock()

	require.NoError(t, store.Close(ctx))
}

// TestPebbleStoreCloseWarnsWhileBlockedOnDrain pins the observability of the
// uncancellable drain. compactPebble and compactPebbleFold each hold a single
// admission for an entire merge, so a wedged compaction or a frozen Lambda
// leaves Close blocked with no ctx.Err() to report. Without a log line the only
// way to tell a hung Close from a slow one is a stack dump.
func TestPebbleStoreCloseWarnsWhileBlockedOnDrain(t *testing.T) {
	logs := &lockedBuffer{}
	encCfg := zap.NewProductionEncoderConfig()
	ctx := ctxzap.ToContext(context.Background(), zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(encCfg), zapcore.AddSync(logs), zap.WarnLevel,
	)))

	store := newAdmissionTestStore(t, filepath.Join(t.TempDir(), "drain-warn.c1z"))
	store.drainWarnInterval = time.Millisecond
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

	deadline := time.Now().Add(admissionTestTimeout)
	var logged string
	for time.Now().Before(deadline) {
		if logged = logs.String(); strings.Contains(logged, "still waiting to tear down") {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.Contains(t, logged, "still waiting to tear down", "a blocked teardown must say so")
	require.Contains(t, logged, `"active_writes":1`, "the warning must name the write it is waiting on")
	require.Contains(t, logged, "in-flight mutations")

	close(release)
	require.NoError(t, receiveError(t, mutationErr))
	require.NoError(t, receiveError(t, closeErr))

	// The watchdog must stop with the teardown rather than outliving it.
	before := logs.Len()
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, before, logs.Len(), "the watchdog kept logging after Close returned")
}

// lockedBuffer is a log sink the watchdog goroutine writes while the test
// goroutine reads.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
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

	tracked := make(map[string]bool, len(guardedMutationWrappers))
	for _, name := range guardedMutationWrappers {
		tracked[name] = false
	}
	// Every declaration bearing a guarded name must call withMutation — not
	// just one of them. Keying on the name alone would let a same-named,
	// unguarded method on another receiver hide behind the guarded one.
	for fileName, file := range parsed {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if _, isTracked := tracked[fn.Name.Name]; !isTracked {
				continue
			}
			tracked[fn.Name.Name] = true
			guarded := false
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if ok && sel.Sel.Name == "withMutation" {
					guarded = true
				}
				return true
			})
			require.Truef(t, guarded, "mutating wrapper %s (%s) must call withMutation", fn.Name.Name, fileName)
		}
	}
	for name, seen := range tracked {
		require.Truef(t, seen, "guarded wrapper %s not found in parsed files", name)
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
