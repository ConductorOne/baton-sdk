package dotc1z

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// pickExpandable returns the GrantExpandable annotation on a grant, or nil.
func pickExpandable(t *testing.T, g *v2.Grant) *v2.GrantExpandable {
	t.Helper()
	exp := &v2.GrantExpandable{}
	annos := annotations.Annotations(g.GetAnnotations())
	ok, err := annos.Pick(exp)
	require.NoError(t, err)
	if !ok {
		return nil
	}
	return exp
}

func collectStream(t *testing.T, seq func(func(*v2.Grant, error) bool)) []*v2.Grant {
	t.Helper()
	var out []*v2.Grant
	for g, err := range seq {
		require.NoError(t, err)
		out = append(out, g)
	}
	return out
}

// TestStreamGrantsIncludeExpansionRoundTrip is the rollback/replay round-trip
// guard: a grant written with a GrantExpandable annotation is stripped into the
// expansion side column on write; streaming it back without IncludeExpansion
// drops the topology (the documented default), and streaming with
// IncludeExpansion restores the exact GrantExpandable so a replay/copy
// re-emits the same expansion edges.
func TestStreamGrantsIncludeExpansionRoundTrip(t *testing.T) {
	ctx := context.Background()

	tmpFile, err := os.CreateTemp("", "test-stream-expansion-*.c1z")
	require.NoError(t, err)
	tmpPath := tmpFile.Name()
	require.NoError(t, tmpFile.Close())
	defer os.Remove(tmpPath)

	wantEntitlementIDs := []string{"ent:a", "ent:b"}

	func() {
		c1f, err := newC1ZFile(ctx, tmpPath)
		require.NoError(t, err)
		_, err = c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)

		groupRT := v2.ResourceType_builder{Id: "group"}.Build()
		userRT := v2.ResourceType_builder{Id: "user"}.Build()
		require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))

		g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
		u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
		require.NoError(t, c1f.PutResources(ctx, g1, u1))
		ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()
		require.NoError(t, c1f.PutEntitlements(ctx, ent1))

		expandableGrant := v2.Grant_builder{
			Id:          "grant-expandable",
			Entitlement: ent1,
			Principal:   u1,
			Annotations: annotations.New(v2.GrantExpandable_builder{
				EntitlementIds:  wantEntitlementIDs,
				Shallow:         true,
				ResourceTypeIds: []string{"user"},
			}.Build()),
		}.Build()
		plainGrant := v2.Grant_builder{Id: "grant-plain", Entitlement: ent1, Principal: u1}.Build()
		require.NoError(t, c1f.PutGrants(ctx, expandableGrant, plainGrant))
		require.NoError(t, c1f.EndSync(ctx))
		require.NoError(t, c1f.Close(ctx))
	}()

	c1f, err := newC1ZFile(ctx, tmpPath)
	require.NoError(t, err)
	defer c1f.Close(ctx)

	byID := func(grants []*v2.Grant, id string) *v2.Grant {
		for _, g := range grants {
			if g.GetId() == id {
				return g
			}
		}
		return nil
	}

	// Default stream: expansion topology is absent (data blob was stripped).
	defaultGrants := collectStream(t, c1f.StreamGrants(ctx, "", connectorstore.StreamGrantsOptions{}))
	require.Len(t, defaultGrants, 2)
	require.Nil(t, pickExpandable(t, byID(defaultGrants, "grant-expandable")),
		"default stream must not carry the stripped GrantExpandable annotation")

	// IncludeExpansion: the GrantExpandable annotation is restored exactly.
	expandedGrants := collectStream(t, c1f.StreamGrants(ctx, "", connectorstore.StreamGrantsOptions{IncludeExpansion: true}))
	require.Len(t, expandedGrants, 2)

	restored := pickExpandable(t, byID(expandedGrants, "grant-expandable"))
	require.NotNil(t, restored, "IncludeExpansion must re-attach the GrantExpandable annotation")
	require.Equal(t, wantEntitlementIDs, restored.GetEntitlementIds())
	require.True(t, restored.GetShallow())
	require.Equal(t, []string{"user"}, restored.GetResourceTypeIds())

	// A non-expandable grant gains nothing under IncludeExpansion.
	require.Nil(t, pickExpandable(t, byID(expandedGrants, "grant-plain")),
		"a grant with no expansion must stay unchanged under IncludeExpansion")
}
