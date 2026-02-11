package dotc1z

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func TestC1ZConcurrentClose(t *testing.T) {
	ctx := t.Context()

	testFilePath := filepath.Join(c1zTests.workingDir, "test-concurrent-close.c1z")

	f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)

	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	err = f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: testResourceType}.Build())
	require.NoError(t, err)

	// Add a bunch of resources, entitlements, and grants to fill up the WAL file.
	userCount := 100
	resourceCount := 1500
	entitlementsPerResource := 10
	grantsPerEntitlement := 10

	users := []*v2.Resource{}
	for i := range userCount {
		user := v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: testResourceType,
				Resource:     fmt.Sprintf("user-%07d", i),
			}.Build(),
		}.Build()
		users = append(users, user)
	}
	err = f.PutResources(ctx, users...)
	require.NoError(t, err)

	resources := []*v2.Resource{}
	for i := range resourceCount {
		resource := v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: testResourceType,
				Resource:     fmt.Sprintf("resource-%07d", i),
			}.Build(),
		}.Build()
		resources = append(resources, resource)
		entitlements := []*v2.Entitlement{}
		for j := range entitlementsPerResource {
			entitlement := v2.Entitlement_builder{
				Id:       fmt.Sprintf("entitlement-r%07d-%07d", i, j),
				Resource: resource,
			}.Build()
			entitlements = append(entitlements, entitlement)
			grants := []*v2.Grant{}
			for k := range grantsPerEntitlement {
				grants = append(grants, v2.Grant_builder{
					Id:          fmt.Sprintf("grant-r%07d-%07d-%07d", i, j, k),
					Principal:   users[k%userCount],
					Entitlement: entitlement,
				}.Build())
			}
			err = f.PutGrants(ctx, grants...)
			require.NoError(t, err)
		}
		err = f.PutEntitlements(ctx, entitlements...)
		require.NoError(t, err)
	}

	err = f.PutResources(ctx, resources...)
	require.NoError(t, err)

	err = f.EndSync(ctx)
	require.NoError(t, err)

	expectedGrantStats := map[string]int64{
		testResourceType: int64(resourceCount * entitlementsPerResource * grantsPerEntitlement),
	}

	stats, err := f.GrantStats(ctx, connectorstore.SyncTypeAny, syncID)
	require.NoError(t, err)
	for k, v := range expectedGrantStats {
		require.Equal(t, v, stats[k])
	}

	start := time.Now()
	// Close concurrently with a PutGrants operation.
	wg := sync.WaitGroup{}
	wg.Add(2)
	closeFunc := func() {
		defer wg.Done()
		err = f.Close(ctx)
		require.NoError(t, err)
		elapsed := time.Since(start)
		t.Logf("close took %s", elapsed)
	}

	syncID, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	putGrantsFunc := func() {
		// Close will finish at some point, causing DB operations to fail.
		defer wg.Done()
		// Put grants in a loop until we get a DbNotOpen error.
		i := 0
		for {
			err = f.PutGrants(ctx, v2.Grant_builder{
				Id: fmt.Sprintf("grant-%d", i),
				Principal: v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: testResourceType,
						Resource:     fmt.Sprintf("user-%07d", i%userCount),
					}.Build(),
				}.Build(),
				Entitlement: v2.Entitlement_builder{
					Id: fmt.Sprintf("entitlement-r%07d-%07d", i%resourceCount, i%entitlementsPerResource),
					Resource: v2.Resource_builder{
						Id: v2.ResourceId_builder{
							ResourceType: testResourceType,
							Resource:     fmt.Sprintf("resource-%07d", i%resourceCount),
						}.Build(),
					}.Build(),
				}.Build(),
			}.Build())
			if err != nil {
				require.ErrorIs(t, err, ErrDbNotOpen)
				break
			}
			i++
		}
		t.Logf("grant insert count: %d", i)
		err = f.EndSync(ctx)
		require.ErrorIs(t, err, ErrDbNotOpen)
	}
	wg.Go(putGrantsFunc)
	wg.Go(closeFunc)

	// Wait for both goroutines to finish.
	wg.Wait()

	// Validate that the WAL file is nonexistent or empty.
	walPath := f.dbFilePath + "-wal"
	walInfo, err := os.Stat(walPath)
	if err != nil {
		require.ErrorIs(t, err, os.ErrNotExist, "WAL file should not exist")
	} else {
		require.Equal(t, int64(0), walInfo.Size(), "WAL file should be empty")
	}
}
