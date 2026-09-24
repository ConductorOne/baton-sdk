package dotc1z_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	sdk "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/synccompactor"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const crashSyncID = "000000000000000000000000001"

var errPublicLedgerCut = errors.New("public ledger durable cut")

type crashCollectionConnector struct {
	types.ConnectorClient
	skipStatic bool
	cleanupErr error
}

func crashResource() *v2.Resource {
	return v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "type", Resource: "one"}.Build(), DisplayName: "resource", Icon: &v2.AssetRef{Id: "icon"}}.Build()
}
func crashEntitlement() *v2.Entitlement {
	return v2.Entitlement_builder{Id: "type:one:member", Slug: "member", Resource: crashResource(), DisplayName: "member"}.Build()
}
func (crashCollectionConnector) Validate(context.Context, *v2.ConnectorServiceValidateRequest, ...grpc.CallOption) (*v2.ConnectorServiceValidateResponse, error) {
	return &v2.ConnectorServiceValidateResponse{}, nil
}
func (crashCollectionConnector) GetMetadata(context.Context, *v2.ConnectorServiceGetMetadataRequest, ...grpc.CallOption) (*v2.ConnectorServiceGetMetadataResponse, error) {
	return &v2.ConnectorServiceGetMetadataResponse{}, nil
}
func (c crashCollectionConnector) Cleanup(context.Context, *v2.ConnectorServiceCleanupRequest, ...grpc.CallOption) (*v2.ConnectorServiceCleanupResponse, error) {
	if c.cleanupErr != nil {
		return nil, c.cleanupErr
	}
	return &v2.ConnectorServiceCleanupResponse{}, nil
}

func (crashCollectionConnector) ListResourceTypes(
	_ context.Context, req *v2.ResourceTypesServiceListResourceTypesRequest, _ ...grpc.CallOption,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	if req.GetPageToken() != "" {
		return &v2.ResourceTypesServiceListResourceTypesResponse{}, nil
	}
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{List: []*v2.ResourceType{{Id: "type"}}, NextPageToken: "private-type-cursor"}.Build(), nil
}
func (crashCollectionConnector) ListResources(_ context.Context, r *v2.ResourcesServiceListResourcesRequest, _ ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	if r.GetParentResourceId() != nil {
		return &v2.ResourcesServiceListResourcesResponse{}, nil
	}
	return v2.ResourcesServiceListResourcesResponse_builder{List: []*v2.Resource{crashResource()}}.Build(), nil
}
func (crashCollectionConnector) GetResource(context.Context, *v2.ResourceGetterServiceGetResourceRequest, ...grpc.CallOption) (*v2.ResourceGetterServiceGetResourceResponse, error) {
	return v2.ResourceGetterServiceGetResourceResponse_builder{Resource: crashResource()}.Build(), nil
}
func (c crashCollectionConnector) ListStaticEntitlements(
	context.Context, *v2.EntitlementsServiceListStaticEntitlementsRequest, ...grpc.CallOption,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	if c.skipStatic {
		return &v2.EntitlementsServiceListStaticEntitlementsResponse{}, nil
	}
	return v2.EntitlementsServiceListStaticEntitlementsResponse_builder{List: []*v2.Entitlement{{Slug: "static"}}}.Build(), nil
}
func (crashCollectionConnector) ListEntitlements(context.Context, *v2.EntitlementsServiceListEntitlementsRequest, ...grpc.CallOption) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	return v2.EntitlementsServiceListEntitlementsResponse_builder{List: []*v2.Entitlement{crashEntitlement()}}.Build(), nil
}
func (crashCollectionConnector) ListGrants(context.Context, *v2.GrantsServiceListGrantsRequest, ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	return v2.GrantsServiceListGrantsResponse_builder{List: []*v2.Grant{v2.Grant_builder{Id: "grant", Entitlement: crashEntitlement(), Principal: crashResource()}.Build()}}.Build(), nil
}

type crashAssetStream struct {
	grpc.ClientStream
	phase int
}

func (s *crashAssetStream) Recv() (*v2.AssetServiceGetAssetResponse, error) {
	s.phase++
	switch s.phase {
	case 1:
		return v2.AssetServiceGetAssetResponse_builder{Metadata: v2.AssetServiceGetAssetResponse_Metadata_builder{ContentType: "image/example"}.Build()}.Build(), nil
	case 2:
		return v2.AssetServiceGetAssetResponse_builder{Data: v2.AssetServiceGetAssetResponse_Data_builder{Data: []byte("asset bytes")}.Build()}.Build(), nil
	default:
		return nil, io.EOF
	}
}

func (crashCollectionConnector) GetAsset(context.Context, *v2.AssetServiceGetAssetRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[v2.AssetServiceGetAssetResponse], error) {
	return &crashAssetStream{}, nil
}

type crashPageWriter struct {
	c1zstore.PageWriter
	commit func(context.Context, c1zstore.LedgerActionIdentity, *c1zstore.LedgerRow, func() error) error
}

func (w crashPageWriter) Commit(ctx context.Context, id c1zstore.LedgerActionIdentity, row *c1zstore.LedgerRow) error {
	return w.commit(ctx, id, row, func() error { return w.PageWriter.Commit(ctx, id, row) })
}

type durableCrashImage struct {
	fs *vfs.MemFS
	id c1zstore.LedgerActionIdentity
}

type durableSyncResult struct {
	data  map[string]string
	stats *reader.SyncStats
}

func runDurableSync(t *testing.T, fs vfs.FS, dir, mode string, workers int, cutOp string, after, prefix bool) (durableSyncResult, *durableCrashImage) {
	t.Helper()
	var result durableSyncResult
	var image *durableCrashImage
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		e, err := engine.Open(ctx, dir, engine.WithVFS(fs))
		require.NoError(t, err)
		defer func() { require.NoError(t, e.Close()) }()
		var once sync.Once
		store := dotc1z.PebbleStoreForTesting(e, func(w c1zstore.PageWriter) c1zstore.PageWriter {
			return crashPageWriter{PageWriter: w, commit: func(ctx context.Context, id c1zstore.LedgerActionIdentity, row *c1zstore.LedgerRow, commit func() error) error {
				match := id.Op == cutOp && (id.ResourceTypeID != "" || id.Op == "list-resource-types" || id.Op == "sync-terminal-v1")
				capture := func() {
					once.Do(func() {
						if mem, ok := fs.(*vfs.MemFS); ok {
							image = &durableCrashImage{fs: mem.CrashClone(vfs.CrashCloneCfg{}), id: id}
							return
						}
						root, err := os.OpenRoot(filepath.Dir(dir))
						require.NoError(t, err)
						require.NoError(t, root.WriteFile("cut", []byte(id.Op), 0600))
						require.NoError(t, root.Close())
						os.Exit(78)
					})
				}
				if match && !after {
					capture()
					return errPublicLedgerCut
				}
				if mode == "archive-failure" && id.Op == "sync-terminal-v1" {
					if err := w.SetFactValue(c1zstore.LedgerFactReportOptions, "invalid-json"); err != nil {
						return err
					}
					if err := w.SetFactValue(c1zstore.LedgerFactReportOptionsPrefix+row.Attempt, "invalid-json"); err != nil {
						return err
					}
				}
				if err := commit(); err != nil {
					return err
				}
				if prefix {
					if err := e.Flush(ctx); err != nil {
						return err
					}
				}
				if match && after {
					capture()
					return errPublicLedgerCut
				}
				return nil
			}}
		})
		store.(c1zstore.WriteHookStore).SetWriteHook(func(_ context.Context, event c1zstore.WriteHookEvent) error {
			if event.Bypass == "" {
				return fmt.Errorf("unregistered page write: %s", event.Method)
			}
			return nil
		})
		existing, err := e.GetSyncRunRecord(ctx, crashSyncID)
		if existing == nil {
			require.ErrorIs(t, err, pebble.ErrNotFound)
			created, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			run, err := e.GetSyncRunRecord(ctx, created)
			require.NoError(t, err)
			run.SetSyncId(crashSyncID)
			require.NoError(t, e.PutSyncRunRecord(ctx, run))
			require.NoError(t, e.SetCurrentSync(ctx, crashSyncID))
			if mode == "assets" {
				require.NoError(t, store.PutResourceTypes(ctx, &v2.ResourceType{Id: "type"}))
				require.NoError(t, store.PutResources(ctx, crashResource()))
				state := `{"version":1,"actions_map":{"a":{"id":"a","operation":"fetch-assets","resource_type_id":"type","resource_id":"one"}}` +
					`,"action_order":["a"],"ingest_quality":{}}`
				require.NoError(t, store.CheckpointSync(ctx, state))
				require.NoError(t, e.Flush(ctx))
			}
		} else {
			require.NoError(t, err)
		}
		opts := []sdk.SyncOpt{sdk.WithConnectorStore(store), sdk.WithSyncID(crashSyncID), sdk.WithWorkerCount(workers), sdk.WithDontExpandGrants()}
		if mode == "targeted" {
			opts = append(opts, sdk.WithTargetedSyncResources([]*v2.Resource{crashResource()}))
		}
		runner, err := sdk.NewSyncer(ctx, crashCollectionConnector{}, opts...)
		require.NoError(t, err)
		err = runner.Sync(ctx)
		if cutOp != "" {
			require.Error(t, err)
			require.NotNil(t, image)
			return
		}
		require.NoError(t, err)
		result.data = make(map[string]string)
		it, err := e.NewIter(nil)
		require.NoError(t, err)
		for it.First(); it.Valid(); it.Next() {
			key := it.Key()
			if mode != "archive-failure" && len(key) > 1 {
				require.NotEqual(t, byte(12), key[1], "default seal left live ledger state")
			}
			if len(key) > 1 && (key[1] >= 1 && key[1] <= 5 || key[1] == 7 || key[1] == 8 || key[1] == 10) {
				result.data[string(key)] = string(it.Value())
			}
		}
		require.NoError(t, it.Error())
		require.NoError(t, it.Close())
		result.stats, err = store.SyncMeta().StatsV2(ctx, connectorstore.SyncTypeFull, crashSyncID)
		require.NoError(t, err)
		result.stats.SetStepDurationsMs(nil)
		for _, v := range result.stats.GetConnectorCallStats() {
			v.SetTotalMs(0)
			v.SetMaxMs(0)
		}
		for _, v := range result.stats.GetSessionStoreStats() {
			v.SetTotalMs(0)
			v.SetMaxMs(0)
		}
		require.NoError(t, e.SetCurrentSync(ctx, crashSyncID))
		ledger := store.(c1zstore.PageLedgerStore)
		finished, err := ledger.BoundSyncFinished(ctx)
		require.NoError(t, err)
		require.True(t, finished)
		facts, err := ledger.LedgerFacts(ctx)
		require.NoError(t, err)
		report, err := ledger.GetArchivedLedgerReport(ctx)
		require.NoError(t, err)
		if mode == "archive-failure" {
			require.NotEmpty(t, facts)
			require.NotContains(t, facts, c1zstore.LedgerFactDiscardOnSeal)
			require.Empty(t, report)
			row, found, err := ledger.GetLedgerRow(ctx, c1zstore.LedgerActionIdentity{Op: "list-resource-types"})
			require.NoError(t, err)
			require.True(t, found)
			require.True(t, row.Scrubbed)
		} else {
			require.Empty(t, facts)
			require.NotEmpty(t, report)
		}
	})
	return result, image
}

func TestPublicLedgerDurableCrashImages(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("MemFS engine staging requires Unix path separators; real-filesystem process cases run on Windows")
	}
	for _, mode := range []string{"full", "targeted", "assets"} {
		for _, workers := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s/workers-%d", mode, workers), func(t *testing.T) {
				baseline, _ := runDurableSync(t, vfs.NewCrashableMem(), "public-sync", mode, workers, "", false, false)
				require.NotEmpty(t, baseline.data)
				require.EqualValues(t, 1, baseline.stats.GetResourceTypes())
				require.EqualValues(t, 1, baseline.stats.GetResources())
				if mode == "full" {
					require.EqualValues(t, 2, baseline.stats.GetEntitlements())
					require.EqualValues(t, 1, baseline.stats.GetGrants())
				}
				if mode == "targeted" {
					require.EqualValues(t, 1, baseline.stats.GetEntitlements())
					require.EqualValues(t, 1, baseline.stats.GetGrants())
				}
				if mode == "assets" {
					require.EqualValues(t, 1, baseline.stats.GetAssets())
				} else {
					require.EqualValues(t, 1, baseline.stats.GetResources())
				}
				ops := []string{"list-resource-types", "list-resources", "list-static-entitlements", "materialize-static-entitlements", "list-entitlements", "list-grants"}
				if mode == "targeted" {
					ops = []string{"targeted-resource-sync"}
				}
				if mode == "assets" {
					ops = []string{"fetch-assets"}
				}
				ops = append(ops, "sync-terminal-v1")
				for _, op := range ops {
					for _, after := range []bool{false, true} {
						for _, prefix := range []bool{false, true} {
							t.Run(fmt.Sprintf("%s/after-%t/prefix-%t", op, after, prefix), func(t *testing.T) {
								_, image := runDurableSync(t, vfs.NewCrashableMem(), "public-sync", mode, workers, op, after, prefix)
								probe, err := engine.Open(t.Context(), "public-sync", engine.WithVFS(image.fs), engine.WithReadOnly(true))
								require.NoError(t, err)
								_, found, err := probe.Ledger().GetRow(t.Context(), image.id)
								require.NoError(t, err)
								if !after {
									require.False(t, found)
								}
								if after && prefix {
									require.True(t, found)
								}
								t.Logf("durable target row present: %t", found)
								require.NoError(t, probe.Close())
								actual, _ := runDurableSync(t, image.fs, "public-sync", mode, 5-workers, "", false, false)
								require.Equal(t, baseline.data, actual.data)
								require.True(t, proto.Equal(baseline.stats, actual.stats), "stats differ: baseline=%s recovered=%s", baseline.stats, actual.stats)
							})
						}
					}
				}
			})
		}
	}
}

func TestPublicLedgerTargetAssetProcessCrashes(t *testing.T) {
	if mode := os.Getenv("BATON_SPECIAL_CRASH_MODE"); mode != "" {
		workers, err := strconv.Atoi(os.Getenv("BATON_SPECIAL_CRASH_WORKERS"))
		require.NoError(t, err)
		op := "targeted-resource-sync"
		if mode == "assets" {
			op = "fetch-assets"
		}
		if mode == "static" {
			op = "materialize-static-entitlements"
		}
		runDurableSync(t, vfs.Default, os.Getenv("BATON_SPECIAL_CRASH_DIR"), mode, workers, op, os.Getenv("BATON_SPECIAL_CRASH_AFTER") == "true", os.Getenv("BATON_SPECIAL_CRASH_PREFIX") == "true")
		t.Fatal("process cut not reached")
	}
	for _, mode := range []string{"targeted", "assets", "static"} {
		for _, workers := range []int{1, 4} {
			baseline, _ := runDurableSync(t, vfs.Default, filepath.Join(t.TempDir(), "db"), mode, workers, "", false, false)
			for _, after := range []bool{false, true} {
				for _, prefix := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/workers-%d/after-%t/prefix-%t", mode, workers, after, prefix), func(t *testing.T) {
						dir := filepath.Join(t.TempDir(), "db")
						exe, err := os.Executable()
						require.NoError(t, err)
						cmd := exec.CommandContext(t.Context(), exe, "-test.run=^TestPublicLedgerTargetAssetProcessCrashes$")
						cmd.Env = append(os.Environ(), "BATON_SPECIAL_CRASH_MODE="+mode, "BATON_SPECIAL_CRASH_WORKERS="+strconv.Itoa(workers),
							"BATON_SPECIAL_CRASH_DIR="+dir, "BATON_SPECIAL_CRASH_AFTER="+strconv.FormatBool(after), "BATON_SPECIAL_CRASH_PREFIX="+strconv.FormatBool(prefix))
						output, err := cmd.CombinedOutput()
						var exit *exec.ExitError
						require.ErrorAs(t, err, &exit, string(output))
						require.Equal(t, 78, exit.ExitCode(), string(output))
						require.NotContains(t, string(output), "WARNING: DATA RACE")
						marker, err := os.ReadFile(filepath.Join(filepath.Dir(dir), "cut"))
						require.NoError(t, err)
						require.NotEmpty(t, marker)
						actual, _ := runDurableSync(t, vfs.Default, dir, mode, 5-workers, "", false, false)
						require.Equal(t, baseline.data, actual.data)
						require.True(t, proto.Equal(baseline.stats, actual.stats), "stats differ: baseline=%s recovered=%s", baseline.stats, actual.stats)
					})
				}
			}
		}
	}
}

func TestPublicLedgerArchiveFailureKeepsDataAndStats(t *testing.T) {
	baseline, _ := runDurableSync(t, vfs.Default, filepath.Join(t.TempDir(), "db"), "full", 1, "", false, false)
	retained, _ := runDurableSync(t, vfs.Default, filepath.Join(t.TempDir(), "db"), "archive-failure", 1, "", false, false)
	require.Equal(t, baseline.data, retained.data)
	require.True(t, proto.Equal(baseline.stats, retained.stats))
}

var errPublicLedgerCleanup = errors.New("public ledger cleanup failed after seal")

func TestPublicLedgerCleanupErrorAfterSealKeepsFinishedArtifact(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "cleanup-error.c1z")
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	store.(c1zstore.WriteHookStore).SetWriteHook(func(_ context.Context, event c1zstore.WriteHookEvent) error {
		if event.Bypass == "" {
			return fmt.Errorf("unregistered page write: %s", event.Method)
		}
		return nil
	})
	runner, err := sdk.NewSyncer(ctx, crashCollectionConnector{cleanupErr: errPublicLedgerCleanup}, sdk.WithConnectorStore(store), sdk.WithDontExpandGrants())
	require.NoError(t, err)
	require.NoError(t, runner.Sync(ctx))
	run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.NotNil(t, run)
	token, err := store.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Empty(t, token)
	facts, err := store.(c1zstore.PageLedgerStore).LedgerFacts(ctx)
	require.NoError(t, err)
	require.Empty(t, facts)
	report, err := store.(c1zstore.PageLedgerStore).GetArchivedLedgerReport(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, report)
	response, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{})
	require.NoError(t, err)
	require.Len(t, response.GetList(), 1)
	require.NoError(t, store.Close(ctx))

	reopened, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close(ctx)) }()
	stats, err := reopened.SyncMeta().StatsV2(ctx, connectorstore.SyncTypeFull, run.ID)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.GetResourceTypes())
	require.EqualValues(t, 1, stats.GetResources())
	token, err = reopened.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Empty(t, token)
	finished, err := reopened.(c1zstore.PageLedgerStore).BoundSyncFinished(ctx)
	require.NoError(t, err)
	require.True(t, finished)
}

type publicLedgerCompactorInput struct {
	name           string
	partial        bool
	debug          bool
	retainTokens   bool
	archiveFailure bool
}

func (i publicLedgerCompactorInput) label() string {
	shape := "full"
	if i.partial {
		shape = "partial"
	}
	return i.name + "/" + shape
}

func createPublicLedgerCompactorInput(t *testing.T, ctx context.Context, root string, input publicLedgerCompactorInput) *synccompactor.CompactableSync {
	t.Helper()
	path := filepath.Join(root, fmt.Sprintf("%s-partial-%t.c1z", input.name, input.partial))
	baseStore, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	store := baseStore
	if input.archiveFailure {
		store = dotc1z.WrapPebbleStoreForTesting(baseStore, func(w c1zstore.PageWriter) c1zstore.PageWriter {
			return crashPageWriter{PageWriter: w, commit: func(ctx context.Context, id c1zstore.LedgerActionIdentity, row *c1zstore.LedgerRow, commit func() error) error {
				if id.Op == "sync-terminal-v1" {
					if err := w.SetFactValue(c1zstore.LedgerFactReportOptions, "invalid-json"); err != nil {
						return err
					}
					if err := w.SetFactValue(c1zstore.LedgerFactReportOptionsPrefix+row.Attempt, "invalid-json"); err != nil {
						return err
					}
				}
				return commit()
			}}
		})
	}
	store.(c1zstore.WriteHookStore).SetWriteHook(func(_ context.Context, event c1zstore.WriteHookEvent) error {
		if event.Bypass == "" {
			return fmt.Errorf("unregistered page write: %s", event.Method)
		}
		return nil
	})
	opts := []sdk.SyncOpt{sdk.WithConnectorStore(store), sdk.WithDontExpandGrants(), sdk.WithLedgerDebug(input.debug), sdk.WithRetainLedgerTokens(input.retainTokens)}
	if input.partial {
		opts = append(opts, sdk.WithTargetedSyncResources([]*v2.Resource{crashResource()}))
	}
	runner, err := sdk.NewSyncer(ctx, crashCollectionConnector{skipStatic: true}, opts...)
	require.NoError(t, err)
	require.NoError(t, runner.Sync(ctx))
	run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.NotNil(t, run)
	ledger := store.(c1zstore.PageLedgerStore)
	facts, err := ledger.LedgerFacts(ctx)
	require.NoError(t, err)
	report, err := ledger.GetArchivedLedgerReport(ctx)
	rowID := c1zstore.LedgerActionIdentity{Op: "list-resource-types"}
	if input.partial {
		rowID = c1zstore.LedgerActionIdentity{Op: "targeted-resource-sync", ResourceTypeID: "type", ResourceID: "one"}
	}
	row, found, rowErr := ledger.GetLedgerRow(ctx, rowID)
	require.NoError(t, rowErr)
	switch input.name {
	case "disposed":
		require.Empty(t, facts, input.label())
		require.NoError(t, err)
		require.NotEmpty(t, report, input.label())
		require.False(t, found, input.label())
	case "debug-scrubbed":
		require.NotContains(t, facts, c1zstore.LedgerFactDiscardOnSeal, input.label())
		require.NotContains(t, facts, c1zstore.LedgerFactRetainTokens, input.label())
		require.NoError(t, err)
		require.NotEmpty(t, report, input.label())
		require.True(t, found, input.label())
		require.True(t, row.Scrubbed, input.label())
		require.Empty(t, row.NextPageToken, input.label())
	case "debug-retain-tokens":
		require.Contains(t, facts, c1zstore.LedgerFactRetainTokens, input.label())
		require.NoError(t, err)
		require.NotEmpty(t, report, input.label())
		require.True(t, found, input.label())
		require.False(t, row.Scrubbed, input.label())
		if !input.partial {
			require.Equal(t, "private-type-cursor", row.NextPageToken, input.label())
		}
	case "archive-generation-failure":
		require.NotEmpty(t, facts, input.label())
		require.NotContains(t, facts, c1zstore.LedgerFactDiscardOnSeal, input.label())
		require.NoError(t, err)
		require.Empty(t, report, input.label())
		require.True(t, found, input.label())
		require.True(t, row.Scrubbed, input.label())
	}
	require.NoError(t, store.Close(ctx))
	return &synccompactor.CompactableSync{FilePath: path, SyncID: run.ID}
}

func TestPublicLedgerDisposedFilesCompact(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	var inputs []*synccompactor.CompactableSync
	for _, input := range []publicLedgerCompactorInput{
		{name: "disposed"},
		{name: "disposed", partial: true},
		{name: "debug-scrubbed", debug: true},
		{name: "debug-scrubbed", partial: true, debug: true},
		{name: "debug-retain-tokens", debug: true, retainTokens: true},
		{name: "debug-retain-tokens", partial: true, debug: true, retainTokens: true},
		{name: "archive-generation-failure", archiveFailure: true},
		{name: "archive-generation-failure", partial: true, archiveFailure: true},
	} {
		inputs = append(inputs, createPublicLedgerCompactorInput(t, ctx, root, input))
	}
	var expected map[string]string
	for _, mode := range []synccompactor.PebbleCompactorMode{synccompactor.PebbleCompactorModeOverlay, synccompactor.PebbleCompactorModeFold} {
		t.Run(string(mode), func(t *testing.T) {
			compactor, cleanup, err := synccompactor.NewCompactor(ctx, t.TempDir(), inputs, synccompactor.WithEngine(c1zstore.EnginePebble),
				synccompactor.WithTmpDir(t.TempDir()), synccompactor.WithPebbleCompactorMode(mode), synccompactor.WithSkipGrantExpansion())
			require.NoError(t, err)
			defer func() { require.NoError(t, cleanup()) }()
			out, err := compactor.Compact(ctx)
			require.NoError(t, err)
			require.NotNil(t, out)
			store, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithReadOnly(true))
			require.NoError(t, err)
			defer func() { require.NoError(t, store.Close(ctx)) }()
			stats, err := store.SyncMeta().StatsV2(ctx, connectorstore.SyncTypeFull, out.SyncID)
			require.NoError(t, err)
			require.EqualValues(t, 1, stats.GetResources())
			require.EqualValues(t, 1, stats.GetEntitlements())
			require.EqualValues(t, 1, stats.GetGrants())
			e, ok := engine.AsEngine(store)
			require.True(t, ok)
			data := make(map[string]string)
			it, err := e.NewIter(nil)
			require.NoError(t, err)
			for it.First(); it.Valid(); it.Next() {
				key := it.Key()
				if len(key) > 1 && (key[1] >= 1 && key[1] <= 5 || key[1] == 7 || key[1] == 8 || key[1] == 10) {
					data[string(key)] = string(it.Value())
				}
			}
			require.NoError(t, it.Error())
			require.NoError(t, it.Close())
			if expected == nil {
				expected = data
			} else {
				require.Equal(t, expected, data)
			}
		})
	}
}
