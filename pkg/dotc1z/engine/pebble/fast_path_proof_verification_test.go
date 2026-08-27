package pebble

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

func fastPathProofArmed(e *Engine, kind sourcecache.RowKind) bool {
	e.currentSyncMu.RLock()
	defer e.currentSyncMu.RUnlock()
	switch kind {
	case sourcecache.RowKindResources:
		return e.freshResourcesEmpty
	case sourcecache.RowKindEntitlements:
		return e.freshEntitlementsEmpty
	case sourcecache.RowKindGrants:
		return e.freshGrantsEmpty
	default:
		panic(fmt.Sprintf("unsupported row kind %q", kind))
	}
}

func armFastPathProof(e *Engine, kind sourcecache.RowKind) {
	e.currentSyncMu.Lock()
	defer e.currentSyncMu.Unlock()
	switch kind {
	case sourcecache.RowKindResources:
		e.freshResourcesEmpty = true
	case sourcecache.RowKindEntitlements:
		e.freshEntitlementsEmpty = true
	case sourcecache.RowKindGrants:
		e.freshGrantsEmpty = true
	default:
		panic(fmt.Sprintf("unsupported row kind %q", kind))
	}
}

func putFastPathProofRow(
	t *testing.T,
	e *Engine,
	kind sourcecache.RowKind,
	scope string,
	prefix string,
	row int,
) {
	t.Helper()
	switch kind {
	case sourcecache.RowKindResources:
		require.NoError(t, e.PutResourceRecords(t.Context(), v3.ResourceRecord_builder{
			ResourceTypeId: "user",
			ResourceId:     fmt.Sprintf("%s-%d", prefix, row),
			SourceScopeKey: scope,
		}.Build()))
	case sourcecache.RowKindEntitlements:
		require.NoError(t, e.PutEntitlementRecords(t.Context(), v3.EntitlementRecord_builder{
			ExternalId: fmt.Sprintf("%s-entitlement-%d", prefix, row),
			Resource: v3.ResourceRef_builder{
				ResourceTypeId: "group",
				ResourceId:     fmt.Sprintf("%s-group-%d", prefix, row),
			}.Build(),
			SourceScopeKey: scope,
		}.Build()))
	case sourcecache.RowKindGrants:
		require.NoError(t, e.PutGrantRecords(t.Context(), v3.GrantRecord_builder{
			ExternalId: fmt.Sprintf("%s-grant-%d", prefix, row),
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: "group",
				ResourceId:     fmt.Sprintf("%s-group-%d", prefix, row),
				EntitlementId:  fmt.Sprintf("%s-entitlement-%d", prefix, row),
			}.Build(),
			Principal: v3.PrincipalRef_builder{
				ResourceTypeId: "user",
				ResourceId:     fmt.Sprintf("%s-user-%d", prefix, row),
			}.Build(),
			SourceScopeKey: scope,
		}.Build()))
	default:
		t.Fatalf("unsupported row kind %q", kind)
	}
}

func TestVerificationBindCurrentSyncDisarmsAllFastPathProofs(t *testing.T) {
	ctx := t.Context()
	a := newAdapter(t)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		require.True(t, fastPathProofArmed(e, kind), "%s proof premise was not armed", kind)
	}

	require.NoError(t, e.bindCurrentSync(syncID))
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		require.False(t, fastPathProofArmed(e, kind), "%s proof survived conservative rebind", kind)
	}
}

func TestVerificationBulkImportFinishDisarmsAllFastPathProofs(t *testing.T) {
	ctx := t.Context()
	a := newAdapter(t)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()
	bulk, err := e.StartBulkSyncImport(ctx, syncID, t.TempDir())
	require.NoError(t, err)
	defer bulk.Abort()

	require.NoError(t, bulk.AddResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build()))
	require.NoError(t, bulk.AddResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()))
	require.NoError(t, bulk.AddEntitlements(ctx, mkV2Entitlement("group:g1:member", "group", "g1")))
	shard, err := bulk.NewGrantShard()
	require.NoError(t, err)
	shard.Close()
	require.NoError(t, bulk.Finish(ctx))

	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		require.False(t, fastPathProofArmed(e, kind), "%s proof survived bulk ingest", kind)
	}
}

func TestVerificationReplayClearCommittedPrefixDisarmsFastPath(t *testing.T) {
	injected := errors.New("verification destination-clear commit failure")
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			prev := newAdapter(t)
			_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			putFastPathProofRow(t, prev.PebbleEngine(), kind, "scope-a", "source", 0)
			sealReplaySource(ctx, t, prev.PebbleEngine(), kind, "scope-a")

			current := newAdapter(t)
			_, err = current.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			for row := range 3 {
				putFastPathProofRow(t, current.PebbleEngine(), kind, "scope-a", "destination", row)
			}
			armFastPathProof(current.PebbleEngine(), kind)
			require.True(t, fastPathProofArmed(current.PebbleEngine(), kind), "fault premise was not armed")
			current.PebbleEngine().test.sourceCacheReplayBatchRows = 2
			commitCalls := 0
			current.PebbleEngine().test.sourceCacheReplayClearCommitHook = func(
				gotKind string,
				_ int,
				_ bool,
			) error {
				require.Equal(t, string(kind), gotKind)
				commitCalls++
				if commitCalls == 2 {
					return injected
				}
				return nil
			}

			var replayErr error
			switch kind {
			case sourcecache.RowKindResources:
				_, replayErr = current.PebbleEngine().ReplaySourceCacheResources(
					ctx,
					prev.PebbleEngine(),
					"scope-a",
				)
			case sourcecache.RowKindEntitlements:
				_, replayErr = current.PebbleEngine().ReplaySourceCacheEntitlements(
					ctx,
					prev.PebbleEngine(),
					"scope-a",
				)
			case sourcecache.RowKindGrants:
				_, replayErr = current.PebbleEngine().ReplaySourceCacheGrants(
					ctx,
					prev.PebbleEngine(),
					"scope-a",
				)
			}
			require.ErrorIs(t, replayErr, injected)
			require.Equal(t, 2, commitCalls, "fault did not occur after one committed clear batch")
			armedAfterFailure := fastPathProofArmed(current.PebbleEngine(), kind)
			current.PebbleEngine().test.sourceCacheReplayClearCommitHook = nil

			putFastPathProofRow(t, current.PebbleEngine(), kind, "scope-b", "destination", 2)
			require.NoError(t, auditSourceScopeBiconditional(current.PebbleEngine()),
				"colliding write used stale empty-keyspace proof after committed clear")
			require.False(t, armedAfterFailure, "committed clear did not consume %s fast-path proof", kind)
		})
	}
}

func TestVerificationReplayWriteBarrierDrainsBeforeClose(t *testing.T) {
	ctx := t.Context()
	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	putFastPathProofRow(t, prev.PebbleEngine(), sourcecache.RowKindResources, "scope-a", "source", 0)
	sealReplaySource(ctx, t, prev.PebbleEngine(), sourcecache.RowKindResources, "scope-a")

	current := newAdapter(t)
	_, err = current.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	enteredCommit := make(chan struct{})
	releaseCommit := make(chan struct{})
	current.PebbleEngine().test.sourceCacheReplayCommitHook = func(kind string, rows int, final bool) error {
		if kind != string(sourcecache.RowKindResources) || rows != 1 || !final {
			return fmt.Errorf("unexpected replay commit kind=%q rows=%d final=%t", kind, rows, final)
		}
		close(enteredCommit)
		<-releaseCommit
		return nil
	}

	replayDone := make(chan error, 1)
	go func() {
		_, replayErr := current.PebbleEngine().ReplaySourceCacheResources(
			context.Background(),
			prev.PebbleEngine(),
			"scope-a",
		)
		replayDone <- replayErr
	}()
	select {
	case <-enteredCommit:
	case <-time.After(5 * time.Second):
		t.Fatal("replay did not reach the commit barrier")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- current.PebbleEngine().Close()
	}()
	select {
	case closeErr := <-closeDone:
		close(releaseCommit)
		t.Fatalf("Close returned while replay still owned the write barrier: %v", closeErr)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseCommit)
	require.NoError(t, <-replayDone)
	require.NoError(t, <-closeDone)
}
