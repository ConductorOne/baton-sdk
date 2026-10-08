package c1zsanitize

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

var fixedAnchor = time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)

func anyTB(tb testing.TB, message proto.Message) *anypb.Any {
	tb.Helper()
	value, err := anypb.New(message)
	require.NoError(tb, err)
	return value
}

func buildSyncFixture(tb testing.TB, ctx context.Context, path string, grants int) {
	tb.Helper()
	store, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(tb, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(tb, err)
	require.NoError(tb, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
		v2.ResourceType_builder{Id: "role", DisplayName: "Role", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE}}.Build(),
	))
	role := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "role", Resource: "admin"}.Build(), DisplayName: "Admin",
	}.Build()
	resources := []*v2.Resource{role}
	for i := 0; i < grants; i++ {
		assetID := fmt.Sprintf("icon-%d", i)
		resources = append(resources, v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("user-%d", i)}.Build(),
			DisplayName: fmt.Sprintf("User %d", i),
			Annotations: []*anypb.Any{anyTB(tb, v2.UserTrait_builder{
				Login: fmt.Sprintf("user-%d@example.com", i),
				Icon:  v2.AssetRef_builder{Id: assetID}.Build(),
			}.Build())},
		}.Build())
		require.NoError(tb, store.PutAsset(ctx, v2.AssetRef_builder{Id: assetID}.Build(), "image/png", []byte{1, 2, 3}))
	}
	require.NoError(tb, store.PutResources(ctx, resources...))
	entitlement := v2.Entitlement_builder{
		Id: "admin", Resource: role, DisplayName: "Admin", Slug: "admin",
	}.Build()
	require.NoError(tb, store.PutEntitlements(ctx, entitlement))
	rows := make([]*v2.Grant, 0, grants)
	for i := 0; i < grants; i++ {
		grant := v2.Grant_builder{
			Id: fmt.Sprintf("grant-%d", i), Entitlement: entitlement, Principal: resources[i+1],
		}.Build()
		if i == 0 {
			grant.SetAnnotations(annotations.New(v2.GrantExpandable_builder{
				EntitlementIds: []string{"admin"},
			}.Build()))
		}
		rows = append(rows, grant)
	}
	require.NoError(tb, store.PutGrants(ctx, rows...))
	require.NoError(tb, store.EndSync(ctx))
	require.NoError(tb, store.Close(ctx))
}

func sanitizeToFile(t *testing.T, ctx context.Context, srcPath, dstPath string, secret []byte, opts Options) {
	t.Helper()
	src := mustOpen(t, ctx, srcPath, true)
	defer src.Close(ctx)
	dst, err := dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	opts.Secret = secret
	opts.TimestampAnchor = fixedAnchor
	require.NoError(t, Sanitize(ctx, src, dst, opts))
	require.NoError(t, dst.Close(ctx))
}

func BenchmarkSanitizePageLedger(b *testing.B) {
	const grants = 2000
	ctx := context.Background()
	dir := b.TempDir()
	srcPath := filepath.Join(dir, "source.c1z")
	buildSyncFixture(b, ctx, srcPath, grants)
	secret := bytes32("bench-sanitize")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dstPath := filepath.Join(dir, fmt.Sprintf("destination-%d.c1z", i))
		src, err := dotc1z.NewC1ZFile(ctx, srcPath, dotc1z.WithReadOnly(true))
		if err != nil {
			b.Fatal(err)
		}
		dst, err := dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
		if err != nil {
			b.Fatal(err)
		}
		if err := Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor}); err != nil {
			b.Fatal(err)
		}
		_ = dst.Close(ctx)
		_ = src.Close(ctx)
	}
}
