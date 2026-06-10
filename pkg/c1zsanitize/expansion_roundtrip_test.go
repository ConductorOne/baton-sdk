package c1zsanitize

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// pickGrantExpandable returns the GrantExpandable annotation on a grant, or nil.
func pickGrantExpandable(t *testing.T, g *v2.Grant) *v2.GrantExpandable {
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

// listGrantsWithExpansion reads every grant of r's resolved sync through the
// expansion-aware paginated path, so the GrantExpandable annotation that the
// SQLite writer strips into the side column is re-attached.
func listGrantsWithExpansion(t *testing.T, ctx context.Context, r *dotc1z.C1File) []*v2.Grant {
	t.Helper()
	var out []*v2.Grant
	pageToken := ""
	for {
		req := v2.GrantsServiceListGrantsRequest_builder{
			PageSize:  1000,
			PageToken: pageToken,
		}.Build()
		resp, err := r.ListGrantsWithExpansion(ctx, req)
		require.NoError(t, err)
		out = append(out, resp.GetList()...)
		if resp.GetNextPageToken() == "" {
			return out
		}
		pageToken = resp.GetNextPageToken()
	}
}

// TestSanitizeGrantExpansionRoundTrip is the real SQLite source->sanitize->dest
// round-trip fidelity check that the in-memory annotation test (perf_test.go's
// TestGraphAnnotationHandlers) could not catch.
//
// The fixture grant is written through the REAL SQLite writer (PutGrants), so —
// exactly as in production files — its GrantExpandable annotation is stripped
// out of the data blob and into the side `expansion` column. The sanitizer must
// therefore read that column back (ListGrantsWithExpansion) for the annotation
// to reach handleGrantExpandable; otherwise the topology is silently dropped.
//
// This test FAILS without the ListGrants expansion-reattach wiring (copyGrants
// would read only the data blob, the annotation would never surface, and the
// sanitized dst grant's expansion column would be NULL) and PASSES with it.
func TestSanitizeGrantExpansionRoundTrip(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	dstPath := filepath.Join(tmp, "dst.c1z")

	secret := bytes32("expansion-roundtrip")

	const (
		entA       = "ent-admin"
		entB       = "ent-reader"
		knownRT    = "user"  // declared resource type -> token preserved verbatim
		unknownRT  = "group" // not declared -> token HMAC'd
		expGrantID = "grant-expandable"
		plainID    = "grant-plain"
	)

	// Build the source through the real SQLite writer so GrantExpandable lands
	// in the expansion side column, as production c1z files store it.
	func() {
		f, err := dotc1z.NewC1ZFile(ctx, srcPath)
		require.NoError(t, err)
		_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)

		require.NoError(t, f.PutResourceTypes(ctx,
			v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
			v2.ResourceType_builder{Id: "role", DisplayName: "Role", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE}}.Build(),
		))

		role := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "role", Resource: "admin"}.Build(), DisplayName: "Admin"}.Build()
		user := v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
			DisplayName: "User 1",
			Annotations: []*anypb.Any{anyTB(t, v2.UserTrait_builder{Login: "u1@acme.com"}.Build())},
		}.Build()
		require.NoError(t, f.PutResources(ctx, role, user))

		ent := v2.Entitlement_builder{Id: entA, Resource: role, DisplayName: "admin", Slug: "admin"}.Build()
		require.NoError(t, f.PutEntitlements(ctx, ent))

		expandableGrant := v2.Grant_builder{
			Id:          expGrantID,
			Entitlement: ent,
			Principal:   user,
			Annotations: annotations.New(v2.GrantExpandable_builder{
				EntitlementIds:  []string{entA, entB},
				Shallow:         true,
				ResourceTypeIds: []string{knownRT, unknownRT},
			}.Build()),
		}.Build()
		plainGrant := v2.Grant_builder{Id: plainID, Entitlement: ent, Principal: user}.Build()
		require.NoError(t, f.PutGrants(ctx, expandableGrant, plainGrant))
		require.NoError(t, f.EndSync(ctx))
		require.NoError(t, f.Close(ctx))
	}()

	// Sanitize src -> dst.
	src := mustOpen(t, ctx, srcPath, true)
	dst := mustOpen(t, ctx, dstPath, false)
	require.NoError(t, Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor}))
	require.NoError(t, dst.Close(ctx))
	require.NoError(t, src.Close(ctx))

	// Reference sanitizer with the same secret + the declared known types, used
	// to compute the expected sanitized cross-reference tokens. s.id/transformID
	// depend only on the secret, so this reproduces the real run's output.
	ref := newTestSanitizer(secret)
	ref.knownResourceTypes[knownRT] = struct{}{}
	ref.knownResourceTypes["role"] = struct{}{}

	// Read the sanitized grants back through the expansion-aware path.
	dstRO := mustOpen(t, ctx, dstPath, true)
	defer dstRO.Close(ctx)
	grants := listGrantsWithExpansion(t, ctx, dstRO)
	require.Len(t, grants, 2)

	var sanitizedExpandable, sanitizedPlain *v2.GrantExpandable
	var expandableCount int
	for _, g := range grants {
		if ge := pickGrantExpandable(t, g); ge != nil {
			expandableCount++
			sanitizedExpandable = ge
		} else {
			sanitizedPlain = ge // stays nil
		}
	}

	// Exactly one grant carries a (sanitized) GrantExpandable — proving the
	// annotation survived the real-SQLite strip -> sanitize -> re-read round trip.
	require.Equal(t, 1, expandableCount,
		"the expandable grant must carry a GrantExpandable after sanitize (this fails without the ListGrants expansion-reattach wiring)")
	require.NotNil(t, sanitizedExpandable)
	require.Nil(t, sanitizedPlain, "the non-expandable grant must stay NULL (no GrantExpandable)")

	// Entitlement ids: rewritten through transformID (cross-references).
	require.Equal(t,
		[]string{ref.transformID(entA), ref.transformID(entB)},
		sanitizedExpandable.GetEntitlementIds())
	require.NotContains(t, sanitizedExpandable.GetEntitlementIds(), entA)
	require.NotContains(t, sanitizedExpandable.GetEntitlementIds(), entB)

	// Resource-type tokens: known type preserved verbatim; unknown HMAC'd.
	require.Equal(t,
		[]string{knownRT, ref.sanitizeResourceTypeToken(unknownRT)},
		sanitizedExpandable.GetResourceTypeIds())
	require.NotEqual(t, unknownRT, sanitizedExpandable.GetResourceTypeIds()[1],
		"undeclared resource-type token must be sanitized")

	// Structural shallow flag preserved.
	require.True(t, sanitizedExpandable.GetShallow(), "shallow flag preserved")
}
