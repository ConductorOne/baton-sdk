package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type invalidBindingConnector struct {
	*mockConnector
	calls int
}

func (c *invalidBindingConnector) ListResourceTypes(
	ctx context.Context, r *v2.ResourceTypesServiceListResourceTypesRequest, opts ...grpc.CallOption,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	c.calls++
	return c.mockConnector.ListResourceTypes(ctx, r, opts...)
}
func TestSyncInvalidBindingDoesNotCollect(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "different"}[existing], func(t *testing.T) {
			ctx := t.Context()
			store, err := dotc1z.NewStore(ctx, filepath.Join(t.TempDir(), "invalid.c1z"), dotc1z.WithEngine(c1zstore.EnginePebble))
			require.NoError(t, err)
			defer func() { require.NoError(t, store.Close(ctx)) }()
			if existing {
				_, err = store.StartNewSync(ctx, "full", "")
				require.NoError(t, err)
			}
			raw, ok := engine.AsEngine(store)
			require.True(t, ok)
			snapshot := func() map[string]string {
				it, err := raw.NewIter(nil)
				require.NoError(t, err)
				defer func() { require.NoError(t, it.Close()) }()
				rows := make(map[string]string)
				for it.First(); it.Valid(); it.Next() {
					rows[string(it.Key())] = string(it.Value())
				}
				require.NoError(t, it.Error())
				return rows
			}
			before := snapshot()
			c := &invalidBindingConnector{mockConnector: newMockConnector()}
			c.rtDB = []*v2.ResourceType{{Id: "must-not-be-collected"}}
			s, err := NewSyncer(ctx, c, WithConnectorStore(store), WithSyncID("000000000000000000000000001"), WithDontExpandGrants())
			require.NoError(t, err)
			require.ErrorContains(t, s.Sync(ctx), "not found")
			require.Zero(t, c.calls)
			require.Equal(t, before, snapshot())
		})
	}
}
