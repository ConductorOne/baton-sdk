package c1zsanitize

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// Determinism golden test: sanitizing the same source twice with the
// same secret + anchor produces record-identical output (compared via reader).
func TestSanitizeDeterministicAcrossRuns(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	dstA := filepath.Join(tmp, "dstA.c1z")
	dstB := filepath.Join(tmp, "dstB.c1z")
	secret := bytes32("determinism")

	buildFixture(t, ctx, srcPath)

	run := func(out string) *collected {
		src := mustOpen(t, ctx, srcPath, true)
		defer src.Close(ctx)
		dst := mustOpen(t, ctx, out, false)
		require.NoError(t, Sanitize(ctx, src, dst, Options{Secret: secret}))
		require.NoError(t, dst.Close(ctx))
		ro := mustOpen(t, ctx, out, true)
		t.Cleanup(func() { _ = ro.Close(ctx) })
		return collectRecords(t, ctx, ro)
	}

	a := run(dstA)
	b := run(dstB)

	require.Equal(t, a.idOccurrences, b.idOccurrences, "id occurrence maps must be identical across runs")
	require.Len(t, b.resourceTypes, len(a.resourceTypes))
	require.Len(t, b.resources, len(a.resources))
	require.Len(t, b.entitlements, len(a.entitlements))
	require.Len(t, b.grants, len(a.grants))
}

// Forward-reference regression: a resource-type row carries a ChildResourceType
// referencing a type declared LATER in the listing. With the buffering
// pre-pass the token resolves against the full known set and is preserved
// verbatim (matching the referenced type's own id); the row-by-row bug would
// have HMAC'd it.
func TestSanitizeChildResourceTypeForwardReference(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	dstPath := filepath.Join(tmp, "dst.c1z")
	secret := bytes32("child-fwd-ref")

	src := mustOpen(t, ctx, srcPath, false)
	_, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	childAnno, err := anypb.New(v2.ChildResourceType_builder{ResourceTypeId: "project"}.Build())
	require.NoError(t, err)

	// "account" references "project" and is declared BEFORE it.
	require.NoError(t, src.PutResourceTypes(ctx,
		v2.ResourceType_builder{
			Id:          "account",
			DisplayName: "Account",
			Annotations: []*anypb.Any{childAnno},
		}.Build(),
		v2.ResourceType_builder{Id: "project", DisplayName: "Project"}.Build(),
	))
	require.NoError(t, src.EndSync(ctx))
	require.NoError(t, src.Close(ctx))

	srcRO := mustOpen(t, ctx, srcPath, true)
	defer srcRO.Close(ctx)
	dst := mustOpen(t, ctx, dstPath, false)
	require.NoError(t, Sanitize(ctx, srcRO, dst, Options{Secret: secret}))
	require.NoError(t, dst.Close(ctx))

	dstRO := mustOpen(t, ctx, dstPath, true)
	defer dstRO.Close(ctx)
	rec := collectRecords(t, ctx, dstRO)

	var account *v2.ResourceType
	for _, rt := range rec.resourceTypes {
		if rt.GetId() == "account" {
			account = rt
		}
	}
	require.NotNil(t, account, "account resource type should be present")

	var child *v2.ChildResourceType
	for _, a := range account.GetAnnotations() {
		if a.MessageIs(&v2.ChildResourceType{}) {
			child = &v2.ChildResourceType{}
			require.NoError(t, a.UnmarshalTo(child))
		}
	}
	require.NotNil(t, child, "ChildResourceType annotation must survive")
	require.Equal(t, "project", child.GetResourceTypeId(),
		"forward-referenced declared type must be preserved verbatim, not HMAC'd (B1)")
}

// Cache-key regression: two principals with empty resource ids but different
// display names must not conflate through the principal cache.
func TestCachedPrincipalEmptyIDNoConflation(t *testing.T) {
	s := newTestSanitizer(bytes32("b2-empty-id"))
	cache := newGrantSubCache(false)

	p1 := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user"}.Build(), // empty Resource
		DisplayName: "Alice",
	}.Build()
	p2 := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user"}.Build(), // empty Resource, same type
		DisplayName: "Bob",
	}.Build()

	out1 := s.cachedPrincipal(p1, newAssetRefSet(), cache)
	out2 := s.cachedPrincipal(p2, newAssetRefSet(), cache)

	require.Equal(t, s.id("Alice"), out1.GetDisplayName())
	require.Equal(t, s.id("Bob"), out2.GetDisplayName())
	require.NotEqual(t, out1.GetDisplayName(), out2.GetDisplayName(),
		"distinct empty-resource principals must not conflate (B2)")
	require.Empty(t, cache.principals, "principals with empty resource id must not be cached (B2)")
}
