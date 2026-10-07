package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	stdsync "sync"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var errSQLiteConvertCut = errors.New("sqlite-convert: injected stop")

// Stops by cancelling the caller's context rather than returning an error:
// only the external-stop exit (checkpointOnStop) writes a token on the SQLite
// path; a connector error is retried, then fails the run without one.
type sqliteConvertCutConnector struct {
	*ledgerFamilyConnector
	mu       stdsync.Mutex
	served   []string
	cutAfter int
	cancel   context.CancelCauseFunc
}

func grantPageKey(r *v2.GrantsServiceListGrantsRequest) string {
	return r.GetResource().GetId().GetResource() + "/" + r.GetPageToken()
}

func (c *sqliteConvertCutConnector) ListGrants(ctx context.Context, r *v2.GrantsServiceListGrantsRequest, opts ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.served) >= c.cutAfter {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	resp, err := c.ledgerFamilyConnector.ListGrants(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	c.served = append(c.served, grantPageKey(r))
	if len(c.served) == c.cutAfter {
		c.cancel(errSQLiteConvertCut)
	}
	return resp, nil
}

type sqliteConvertResumeConnector struct {
	*ledgerFamilyConnector
	mu      stdsync.Mutex
	refused map[string]bool
	allowed map[string]bool
	grants  []string
}

func (c *sqliteConvertResumeConnector) ListGrants(
	ctx context.Context, r *v2.GrantsServiceListGrantsRequest, opts ...grpc.CallOption,
) (*v2.GrantsServiceListGrantsResponse, error) {
	key := grantPageKey(r)
	c.mu.Lock()
	c.grants = append(c.grants, key)
	refused := c.refused[key] && !c.allowed[key]
	c.mu.Unlock()
	if refused {
		return nil, fmt.Errorf("repeated grant page the sqlite token recorded as complete: %s", key)
	}
	return c.ledgerFamilyConnector.ListGrants(ctx, r, opts...)
}

func (*sqliteConvertResumeConnector) ListResourceTypes(
	context.Context, *v2.ResourceTypesServiceListResourceTypesRequest, ...grpc.CallOption,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return nil, fmt.Errorf("repeated sqlite resource types")
}
func (*sqliteConvertResumeConnector) ListResources(context.Context, *v2.ResourcesServiceListResourcesRequest, ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	return nil, fmt.Errorf("repeated sqlite resources")
}
func (*sqliteConvertResumeConnector) ListEntitlements(context.Context, *v2.EntitlementsServiceListEntitlementsRequest, ...grpc.CallOption) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	return nil, fmt.Errorf("repeated sqlite entitlements")
}
func (*sqliteConvertResumeConnector) ListStaticEntitlements(
	context.Context, *v2.EntitlementsServiceListStaticEntitlementsRequest, ...grpc.CallOption,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	return nil, fmt.Errorf("repeated sqlite static entitlements")
}

type storeLogicalSnapshot struct {
	resourceTypes []*v2.ResourceType
	resources     []*v2.Resource
	entitlements  []*v2.Entitlement
	grants        []*v2.Grant
}

func readStoreLogicalSnapshot(t *testing.T, store c1zstore.Store) storeLogicalSnapshot {
	t.Helper()
	ctx := t.Context()
	var out storeLogicalSnapshot
	for token := ""; ; {
		resp, err := store.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageToken: token}.Build())
		require.NoError(t, err)
		out.resourceTypes = append(out.resourceTypes, resp.GetList()...)
		if token = resp.GetNextPageToken(); token == "" {
			break
		}
	}
	for token := ""; ; {
		resp, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageToken: token}.Build())
		require.NoError(t, err)
		out.resources = append(out.resources, resp.GetList()...)
		if token = resp.GetNextPageToken(); token == "" {
			break
		}
	}
	for token := ""; ; {
		resp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{PageToken: token}.Build())
		require.NoError(t, err)
		out.entitlements = append(out.entitlements, resp.GetList()...)
		if token = resp.GetNextPageToken(); token == "" {
			break
		}
	}
	for token := ""; ; {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: token}.Build())
		require.NoError(t, err)
		out.grants = append(out.grants, resp.GetList()...)
		if token = resp.GetNextPageToken(); token == "" {
			break
		}
	}
	sort.Slice(out.resourceTypes, func(i, j int) bool { return out.resourceTypes[i].GetId() < out.resourceTypes[j].GetId() })
	sort.Slice(out.resources, func(i, j int) bool {
		return out.resources[i].GetId().GetResourceType()+"/"+out.resources[i].GetId().GetResource() <
			out.resources[j].GetId().GetResourceType()+"/"+out.resources[j].GetId().GetResource()
	})
	sort.Slice(out.entitlements, func(i, j int) bool { return out.entitlements[i].GetId() < out.entitlements[j].GetId() })
	sort.Slice(out.grants, func(i, j int) bool { return out.grants[i].GetId() < out.grants[j].GetId() })
	return out
}

func requireLogicalSnapshotEqual(t *testing.T, want, got storeLogicalSnapshot) {
	t.Helper()
	requireProtoListEqual(t, "resource types", want.resourceTypes, got.resourceTypes)
	requireProtoListEqual(t, "resources", want.resources, got.resources)
	requireProtoListEqual(t, "entitlements", want.entitlements, got.entitlements)
	requireProtoListEqual(t, "grants", want.grants, got.grants)
}

func requireProtoListEqual[T proto.Message](t *testing.T, kind string, want, got []T) {
	t.Helper()
	require.Len(t, got, len(want), kind)
	for i := range want {
		require.True(t, proto.Equal(want[i], got[i]), "%s[%d]: want %v got %v", kind, i, want[i], got[i])
	}
}

func readC1ZFormat(t *testing.T, path string) dotc1z.C1ZFormat {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	format, err := dotc1z.ReadHeaderFormat(f)
	require.NoError(t, err)
	return format
}

type sqliteConvertReference struct {
	data  storeLogicalSnapshot
	stats *v3.SyncStatsRecord
}

func uninterruptedPebbleReference(t *testing.T) sqliteConvertReference {
	t.Helper()
	ctx := t.Context()
	dir := t.TempDir()
	path := filepath.Join(dir, "reference.c1z")
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	created, err := NewSyncer(ctx, newLedgerFamilyConnector(t), WithConnectorStore(store), WithWorkerCount(1), WithDontExpandGrants())
	require.NoError(t, err)
	require.NoError(t, created.Sync(ctx))
	ref := sqliteConvertReference{data: readStoreLogicalSnapshot(t, store)}
	eng, ok := engine.AsEngine(store)
	require.True(t, ok)
	ref.stats, err = engine.ReadSyncStatsRecord(ctx, eng, eng.CurrentSyncID())
	require.NoError(t, err)
	require.NoError(t, store.Close(ctx))
	return ref
}

func requireRecordCountsEqual(t *testing.T, want, got *v3.SyncStatsRecord) {
	t.Helper()
	require.Equal(t, want.GetResourceTypes(), got.GetResourceTypes())
	require.Equal(t, want.GetResources(), got.GetResources())
	require.Equal(t, want.GetEntitlements(), got.GetEntitlements())
	require.Equal(t, want.GetGrants(), got.GetGrants())
}

func TestSQLiteCheckpointConvertsToPebbleLedger(t *testing.T) {
	reference := uninterruptedPebbleReference(t)
	require.EqualValues(t, 2, reference.stats.GetResourceTypes())
	require.EqualValues(t, 4, reference.stats.GetResources())
	require.EqualValues(t, 4, reference.stats.GetGrants())

	for _, cutAfter := range []int{1, 2, 3} {
		for _, workers := range []int{1, 4} {
			for _, attach := range []string{"path", "store"} {
				t.Run(fmt.Sprintf("grants-served-%d/resume-workers-%d/%s", cutAfter, workers, attach), func(t *testing.T) {
					dir := t.TempDir()
					path := filepath.Join(dir, "sync.c1z")
					attempt := runSQLiteAttemptStoppedInGrants(t, path, dir, cutAfter)
					sqliteSyncID := attempt.syncID
					require.Equal(t, dotc1z.C1ZFormatV1, readC1ZFormat(t, path))

					resumeConnector := &sqliteConvertResumeConnector{
						ledgerFamilyConnector: newLedgerFamilyConnector(t), refused: attempt.servedPages, allowed: attempt.pendingPages,
					}
					switch attach {
					case "path":
						resumed, err := NewSyncer(t.Context(), resumeConnector,
							WithC1ZPath(path), WithTmpDir(dir), WithStorageEngine(c1zstore.EnginePebble),
							WithWorkerCount(workers), WithDontExpandGrants())
						require.NoError(t, err)
						rs := resumed.(*syncer)
						rs.testHooks.checkpointHook = func(string) { t.Error("ledger wrote a checkpoint token") }
						require.NoError(t, rs.Sync(t.Context()))
						require.True(t, rs.ledgered, "a converted file must attach as a ledger")
						require.Equal(t, sqliteSyncID, rs.syncID, "the resume must continue the SQLite sync, not start another")
						require.NoError(t, rs.Close(t.Context()))
					case "store":
						f := openLedgerFixtureAt(t, path, false)
						require.Equal(t, dotc1z.C1ZFormatV3, readC1ZFormat(t, path))
						bindLegacyCombinedSync(t, f)
						require.Equal(t, sqliteSyncID, f.engine.CurrentSyncID())
						state, err := f.ledger.State(t.Context())
						require.NoError(t, err)
						require.Equal(t, c1zstore.LedgerState{Phase: c1zstore.LedgerQueueAbsent, Finished: false, Token: true}, state)
						converted, err := f.store.CurrentSyncStep(t.Context())
						require.NoError(t, err)
						require.Equal(t, attempt.token, converted, "ToPebble must carry the SQLite token verbatim")

						resumed, err := NewSyncer(t.Context(), resumeConnector, WithConnectorStore(f.store), WithWorkerCount(workers), WithDontExpandGrants())
						require.NoError(t, err)
						rs := resumed.(*syncer)
						rs.testHooks.checkpointHook = func(string) { t.Error("ledger wrote a checkpoint token") }
						require.NoError(t, rs.Sync(t.Context()))
						require.Equal(t, sqliteSyncID, rs.syncID, "the resume must continue the SQLite sync, not start another")
						require.NoError(t, f.store.Close(t.Context()))
					}
					require.Equal(t, dotc1z.C1ZFormatV3, readC1ZFormat(t, path))
					for page := range attempt.pendingPages {
						require.Contains(t, resumeConnector.grants, page, "the resume skipped a grant page the token left pending")
					}

					f := openLedgerFixtureAt(t, path, false)
					bindLegacyCombinedSync(t, f)
					require.Equal(t, sqliteSyncID, f.engine.CurrentSyncID())
					finished, err := f.engine.BoundSyncFinished(t.Context())
					require.NoError(t, err)
					require.True(t, finished)
					token, err := f.store.CurrentSyncStep(t.Context())
					require.NoError(t, err)
					require.Empty(t, token)
					facts, err := f.ledger.LedgerFacts(t.Context())
					require.NoError(t, err)
					require.Empty(t, facts)
					report, err := f.ledger.GetArchivedLedgerReport(t.Context())
					require.NoError(t, err)
					require.NotEmpty(t, report)
					requireLogicalSnapshotEqual(t, reference.data, readStoreLogicalSnapshot(t, f.store))
					stats, err := engine.ReadSyncStatsRecord(t.Context(), f.engine, sqliteSyncID)
					require.NoError(t, err)
					requireRecordCountsEqual(t, reference.stats, stats)
					require.NoError(t, f.store.Close(t.Context()))
				})
			}
		}
	}
}

type sqliteAttempt struct {
	syncID       string
	token        string
	servedPages  map[string]bool
	pendingPages map[string]bool
}

func runSQLiteAttemptStoppedInGrants(t *testing.T, path, dir string, cutAfter int) sqliteAttempt {
	t.Helper()
	runCtx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	connector := &sqliteConvertCutConnector{ledgerFamilyConnector: newLedgerFamilyConnector(t), cutAfter: cutAfter, cancel: cancel}
	created, err := NewSyncer(runCtx, connector,
		WithC1ZPath(path), WithTmpDir(dir), WithStorageEngine(c1zstore.EngineSQLite),
		WithWorkerCount(1), WithDontExpandGrants())
	require.NoError(t, err)
	s := created.(*syncer)
	var lastToken string
	s.testHooks.checkpointHook = func(token string) { lastToken = token }
	err = s.Sync(runCtx)
	require.Error(t, err)
	require.False(t, s.ledgered)
	require.ErrorIs(t, context.Cause(runCtx), errSQLiteConvertCut)
	require.Len(t, connector.served, cutAfter)
	require.NotEmpty(t, lastToken, "the stop exit must have checkpointed on SQLite")
	syncID := s.syncID
	require.NotEmpty(t, syncID)
	require.NoError(t, s.Close(context.Background()))

	store, err := dotc1z.NewStore(context.Background(), path, dotc1z.WithEngine(c1zstore.EngineSQLite), dotc1z.WithTmpDir(dir), dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	require.NoError(t, store.SetCurrentSync(context.Background(), syncID))
	saved, err := store.CurrentSyncStep(context.Background())
	require.NoError(t, err)
	require.Equal(t, lastToken, saved, "the saved v1 file must hold the last checkpoint")
	require.NoError(t, store.Close(context.Background()))

	pending, _, _, err := decodeLedgerCheckpoint(saved)
	require.NoError(t, err)
	pendingPages := map[string]bool{}
	for _, a := range pending.actions {
		switch a.identity.Op {
		case SyncGrantsOp.String():
			if a.identity.ResourceID != "" {
				pendingPages[a.identity.ResourceID+"/"+a.identity.PageToken] = true
			}
		case SyncResourceTypesOp.String(), SyncResourcesOp.String(), SyncEntitlementsOp.String(), SyncStaticEntitlementsOp.String():
			t.Fatalf("cut landed before the grants phase: %+v", a.identity)
		}
	}
	require.NotEmpty(t, pendingPages, "the token must carry remaining grant work")
	served := map[string]bool{}
	for _, page := range connector.served {
		served[page] = true
	}
	return sqliteAttempt{syncID: syncID, token: lastToken, servedPages: served, pendingPages: pendingPages}
}

// The call sequence is C1's sync-baton activity on an upload: read-only open
// with the connector's engine, CloneSync, open the clone with the same engine
// (a v1 clone converts under Pebble), SetCurrentSync, NeedsExpansion with the
// PendingExpansion fallback, an expansion-only syncer, StatsV2.
func TestSQLiteUploadHostExpansion(t *testing.T) {
	for _, hostEngine := range []c1zstore.Engine{c1zstore.EnginePebble, c1zstore.EngineSQLite} {
		t.Run("host-"+string(hostEngine), func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			uploaded := filepath.Join(dir, "upload.c1z")
			source, want, expanded := ledgerUnexpandedSource(t)
			connector, err := NewSyncer(ctx, source,
				WithC1ZPath(uploaded), WithTmpDir(dir), WithStorageEngine(c1zstore.EngineSQLite), WithDontExpandGrants())
			require.NoError(t, err)
			require.NoError(t, connector.Sync(ctx))
			require.False(t, connector.(*syncer).ledgered)
			require.NoError(t, connector.Close(ctx))
			require.Equal(t, dotc1z.C1ZFormatV1, readC1ZFormat(t, uploaded))

			hostDir := t.TempDir()
			upload, err := dotc1z.NewStore(ctx, uploaded, dotc1z.WithTmpDir(hostDir), dotc1z.WithReadOnly(true), dotc1z.WithEngine(hostEngine))
			require.NoError(t, err)
			require.Equal(t, string(c1zstore.EngineSQLite), upload.Metadata().Engine, "a read-only open must not convert the upload")
			require.Equal(t, dotc1z.C1ZFormatV1, readC1ZFormat(t, uploaded))
			latest, err := upload.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{SyncType: string(connectorstore.SyncTypeAny)}.Build())
			require.NoError(t, err)
			syncID := latest.GetSync().GetId()
			require.NotEmpty(t, syncID)
			uploadGrants, err := upload.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
			require.NoError(t, err)
			require.ElementsMatch(t, want, grantIDs(uploadGrants.GetList()))

			cloned := filepath.Join(hostDir, "cloned.c1z")
			require.NoError(t, upload.FileOps().CloneSync(ctx, cloned, syncID, c1zstore.WithCloneTmpDir(hostDir)))
			require.Equal(t, dotc1z.C1ZFormatV1, readC1ZFormat(t, cloned))
			clone, err := dotc1z.NewStore(ctx, cloned, dotc1z.WithTmpDir(hostDir), dotc1z.WithSkipCleanup(false), dotc1z.WithSkipVacuum(false), dotc1z.WithEngine(hostEngine))
			require.NoError(t, err)
			require.Equal(t, string(hostEngine), clone.Metadata().Engine)
			require.NoError(t, clone.SetCurrentSync(ctx, syncID))

			needsExpansion, err := NeedsExpansion(latest.GetSync().GetSyncToken())
			require.NoError(t, err)
			if !needsExpansion {
				for _, err := range clone.Grants().PendingExpansion(ctx) {
					require.NoError(t, err)
					needsExpansion = true
					break
				}
			}
			require.True(t, needsExpansion, "the host must see the unexpanded upload as needing expansion")

			host, err := NewSyncer(ctx, ledgerExpansionConnector{mockConnector: newMockConnector()},
				WithConnectorStore(clone), WithSyncID(syncID), WithTmpDir(hostDir), WithRunDuration(time.Hour),
				WithExternalResourceC1ZPath(""), WithExternalResourceEntitlementIdFilter(""), WithSyncResourceTypes(nil),
				WithOnlyExpandGrants())
			require.NoError(t, err)
			hs := host.(*syncer)
			require.Equal(t, hostEngine == c1zstore.EnginePebble, hs.ledgered)
			if hs.ledgered {
				hs.testHooks.checkpointHook = func(string) { t.Error("ledger wrote a checkpoint token") }
			}
			require.NoError(t, host.Sync(ctx))
			require.Equal(t, syncID, hs.syncID)

			stats, err := clone.SyncMeta().StatsV2(ctx, connectorstore.SyncTypeAny, syncID)
			require.NoError(t, err)
			require.NotNil(t, stats)
			finalGrants, err := clone.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
			require.NoError(t, err)
			require.ElementsMatch(t, expanded, grantIDs(finalGrants.GetList()))
			require.NoError(t, clone.Close(ctx))
			require.NoError(t, upload.Close(ctx))

			reopened, err := dotc1z.NewStore(ctx, cloned, dotc1z.WithTmpDir(hostDir), dotc1z.WithReadOnly(true), dotc1z.WithEngine(hostEngine))
			require.NoError(t, err)
			finished, err := reopened.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
			require.NoError(t, err)
			require.NotNil(t, finished)
			require.Equal(t, syncID, finished.ID)
			finalGrants, err = reopened.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
			require.NoError(t, err)
			require.ElementsMatch(t, expanded, grantIDs(finalGrants.GetList()))
			require.NoError(t, reopened.Close(ctx))
		})
	}
}
