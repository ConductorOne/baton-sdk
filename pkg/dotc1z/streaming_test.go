package dotc1z

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestStreamingReader exercises the iter.Seq2-based streaming
// reader methods (RFC §B3) on the SQLite backend.
func TestStreamingReader(t *testing.T) {
	ctx := t.Context()
	c1z, err := NewC1ZFile(ctx, filepath.Join(t.TempDir(), "stream.c1z"))
	require.NoError(t, err)
	defer func() { _ = c1z.Close(ctx) }()

	syncID, err := c1z.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, c1z.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user"}.Build(),
		v2.ResourceType_builder{Id: "group"}.Build(),
		v2.ResourceType_builder{Id: "app"}.Build(),
	))
	mkRes := func(rt, id string) *v2.Resource {
		return v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build(),
		}.Build()
	}
	appRes := mkRes("app", "gh")
	require.NoError(t, c1z.PutResources(ctx, appRes))

	const userCount = 25
	users := make([]*v2.Resource, userCount)
	for i := 0; i < userCount; i++ {
		users[i] = mkRes("user", "u"+strconv.Itoa(i))
	}
	require.NoError(t, c1z.PutResources(ctx, users...))

	entA := v2.Entitlement_builder{Id: "ent-A", Resource: appRes, Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION, Slug: "A"}.Build()
	require.NoError(t, c1z.PutEntitlements(ctx, entA))

	grants := make([]*v2.Grant, userCount)
	for i := 0; i < userCount; i++ {
		grants[i] = v2.Grant_builder{
			Id:          "g-" + strconv.Itoa(i),
			Entitlement: entA,
			Principal:   mkRes("user", "u"+strconv.Itoa(i)),
		}.Build()
	}
	require.NoError(t, c1z.PutGrants(ctx, grants...))
	require.NoError(t, c1z.EndSync(ctx))

	t.Run("StreamGrants yields all", func(t *testing.T) {
		seen := 0
		for g, err := range c1z.StreamGrants(ctx, syncID, connectorstore.StreamGrantsOptions{}) {
			require.NoError(t, err)
			require.NotNil(t, g)
			seen++
		}
		require.Equal(t, userCount, seen)
	})

	t.Run("StreamGrants entitlement filter", func(t *testing.T) {
		seen := 0
		for g, err := range c1z.StreamGrants(ctx, syncID, connectorstore.StreamGrantsOptions{EntitlementID: "ent-A"}) {
			require.NoError(t, err)
			require.Equal(t, "ent-A", g.GetEntitlement().GetId())
			seen++
		}
		require.Equal(t, userCount, seen)
	})

	t.Run("StreamResources RT filter", func(t *testing.T) {
		seen := 0
		for r, err := range c1z.StreamResources(ctx, syncID, connectorstore.StreamResourcesOptions{ResourceTypeID: "user"}) {
			require.NoError(t, err)
			require.Equal(t, "user", r.GetId().GetResourceType())
			seen++
		}
		require.Equal(t, userCount, seen)
	})

	t.Run("StreamEntitlements", func(t *testing.T) {
		seen := 0
		for e, err := range c1z.StreamEntitlements(ctx, syncID) {
			require.NoError(t, err)
			require.Equal(t, "ent-A", e.GetId())
			seen++
		}
		require.Equal(t, 1, seen)
	})

	t.Run("early stop via return false honored", func(t *testing.T) {
		count := 0
		for g, err := range c1z.StreamGrants(ctx, syncID, connectorstore.StreamGrantsOptions{}) {
			require.NoError(t, err)
			_ = g
			count++
			if count == 3 {
				break
			}
		}
		require.Equal(t, 3, count)
	})

	t.Run("ctx cancel surfaces as error", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		var gotErr error
		for _, err := range c1z.StreamGrants(cctx, syncID, connectorstore.StreamGrantsOptions{}) {
			if err != nil {
				gotErr = err
				break
			}
		}
		require.Error(t, gotErr)
		require.True(t, errors.Is(gotErr, context.Canceled), "want context.Canceled, got %v", gotErr)
	})

	// Compile-time check: C1File satisfies StreamingReader.
	var _ connectorstore.StreamingReader = c1z
}
