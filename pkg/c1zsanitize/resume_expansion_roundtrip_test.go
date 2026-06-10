package c1zsanitize

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// buildExpandedGrantFixture writes a single-sync c1z whose grants each carry a
// GrantExpandable annotation — i.e. an expanded grant-expansion topology. The
// grants are written through the real SQLite writer, so (as in production
// files) PutGrants strips each GrantExpandable out of the data blob and into the
// side `expansion` column. nGrants grants share one entitlement on a role and
// each bind a distinct user principal.
//
// expEntIDs/expRTIDs are the GrantExpandable cross-reference fields used so the
// test can assert how the sanitizer rewrites them: a declared resource type
// ("user") is preserved verbatim under the known-type rule, while an undeclared
// token ("group") is HMAC'd.
func buildExpandedGrantFixture(t *testing.T, ctx context.Context, path string, nGrants int) {
	t.Helper()
	f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)

	_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, f.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
		v2.ResourceType_builder{Id: "role", DisplayName: "Role", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE}}.Build(),
	))

	role := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "role", Resource: "admin"}.Build(), DisplayName: "Admin"}.Build()
	resources := []*v2.Resource{role}
	for i := 0; i < nGrants; i++ {
		resources = append(resources, v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("u%d", i)}.Build(),
			DisplayName: fmt.Sprintf("User %d", i),
			Annotations: []*anypb.Any{anyTB(t, v2.UserTrait_builder{Login: fmt.Sprintf("u%d@acme.com", i)}.Build())},
		}.Build())
	}
	require.NoError(t, f.PutResources(ctx, resources...))

	ent := v2.Entitlement_builder{Id: "ent-admin", Resource: role, DisplayName: "admin", Slug: "admin"}.Build()
	require.NoError(t, f.PutEntitlements(ctx, ent))

	var grants []*v2.Grant
	for i := 0; i < nGrants; i++ {
		principal := resources[i+1] // resources[0] is the role
		grants = append(grants, v2.Grant_builder{
			Id:          fmt.Sprintf("grant-%d", i),
			Entitlement: ent,
			Principal:   principal,
			Annotations: annotations.New(v2.GrantExpandable_builder{
				EntitlementIds:  []string{"ent-admin", fmt.Sprintf("ent-other-%d", i)},
				Shallow:         true,
				ResourceTypeIds: []string{"user", "group"}, // user: declared -> verbatim; group: undeclared -> HMAC'd
			}.Build()),
		}.Build())
	}
	require.NoError(t, f.PutGrants(ctx, grants...))
	require.NoError(t, f.EndSync(ctx))
	require.NoError(t, f.Close(ctx))
}

// TestSanitizeRoundtripPreservesGrantExpansionTopology is the resumable
// sanitize roundtrip fidelity check for grant-expansion topology.
//
// The c1zsanitize package has no literal "rollback"/"replay" verbs — sanitize is
// a one-directional src->dst transform. The roundtrip the spec asks for maps
// onto the package's REAL resumable-checkpoint mechanism (Options.Resumable):
//
//   - SANITIZE -> Sanitize(ctx, src, dst, Options{Resumable: true}).
//   - "ROLLBACK" -> an interruption at the grant phase (interruptingWriter fails
//     the first PutGrants). The partial destination is persisted; the incomplete
//     grant-phase work is effectively rolled back to the last checkpoint (the
//     resources/entitlements checkpoint), with no grants committed.
//   - "REPLAY" -> the resume run: Sanitize(...) again into the persisted partial
//     destination. loadResumeStates reads the checkpoint and replays the
//     read-only resource/entitlement walk plus the full grant phase from the
//     checkpointed page cursor to completion.
//
// The fixture's grants each carry a GrantExpandable (stripped into the SQLite
// side column on write). The assertion is that after the full
// sanitize -> interrupt(rollback) -> resume(replay) roundtrip, every sanitized
// destination grant still carries its GrantExpandable, sanitized correctly:
// entitlement ids rewritten via transformID, the declared resource-type token
// preserved verbatim and the undeclared one HMAC'd, and shallow preserved. This
// exercises the PR #938 ListGrants expansion-reattach wiring specifically on the
// resume/replay read path.
func TestSanitizeRoundtripPreservesGrantExpansionTopology(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	cleanDst := filepath.Join(tmp, "clean.c1z")
	resumeDst := filepath.Join(tmp, "resume.c1z")
	secret := bytes32("expansion-resume-roundtrip")

	const nGrants = 12
	buildExpandedGrantFixture(t, ctx, srcPath, nGrants)

	// Reference: a single uninterrupted resumable run, for record-identity.
	sanitizeToFile(t, ctx, srcPath, cleanDst, secret, Options{Resumable: true})

	// SANITIZE + "ROLLBACK": interrupt the resumable run at the grant phase
	// (first PutGrants fails). Resources + entitlements were written and
	// checkpointed; no grants are committed. Persist the partial destination.
	func() {
		src := mustOpen(t, ctx, srcPath, true)
		defer src.Close(ctx)
		w := &interruptingWriter{C1File: mustOpen(t, ctx, resumeDst, false), failOnPutGrantsCall: 1}
		err := Sanitize(ctx, src, w, Options{Secret: secret, TimestampAnchor: fixedAnchor, Resumable: true})
		require.Error(t, err, "run must be interrupted at the grant phase (rollback point)")
		require.NoError(t, w.Close(ctx))
	}()

	// Sanity: the partial destination has no committed grants yet — the grant
	// phase truly rolled back to the checkpoint.
	func() {
		ro := mustOpen(t, ctx, resumeDst, true)
		defer ro.Close(ctx)
		rec := collectRecords(t, ctx, ro)
		require.Zero(t, len(rec.grants), "interrupted run must have committed no grants (rolled back to checkpoint)")
	}()

	// "REPLAY": resume into the persisted partial destination, replaying the
	// grant phase to completion.
	func() {
		src := mustOpen(t, ctx, srcPath, true)
		defer src.Close(ctx)
		dst := mustOpen(t, ctx, resumeDst, false)
		require.NoError(t, Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor, Resumable: true}))
		require.NoError(t, dst.Close(ctx))
	}()

	// Reference sanitizer (same secret + declared known types) to compute the
	// expected sanitized cross-reference tokens. s.id/transformID depend only on
	// the secret, so this reproduces the real run's output.
	ref := newTestSanitizer(secret)
	ref.knownResourceTypes["user"] = struct{}{}
	ref.knownResourceTypes["role"] = struct{}{}

	resumeRO := mustOpen(t, ctx, resumeDst, true)
	defer resumeRO.Close(ctx)

	// Record-identity with the uninterrupted run (counts + id occurrences).
	cleanRO := mustOpen(t, ctx, cleanDst, true)
	defer cleanRO.Close(ctx)
	clean := collectRecords(t, ctx, cleanRO)
	resumed := collectRecords(t, ctx, resumeRO)
	require.Equal(t, nGrants, len(clean.grants))
	require.Equal(t, len(clean.grants), len(resumed.grants), "resumed grant count must match the uninterrupted run")
	require.Equal(t, clean.idOccurrences, resumed.idOccurrences, "resumed output must be record-identical to the uninterrupted run")

	// The topology assertion: every resumed destination grant carries a
	// sanitized GrantExpandable. Read through the expansion-aware path (the dst
	// SQLite writer re-stripped it into the side column).
	grants := listGrantsWithExpansion(t, ctx, resumeRO)
	require.Len(t, grants, nGrants)

	expandableCount := 0
	for _, g := range grants {
		ge := pickGrantExpandable(t, g)
		if ge == nil {
			continue
		}
		expandableCount++

		// Entitlement ids rewritten via transformID (cross-references).
		require.Len(t, ge.GetEntitlementIds(), 2)
		require.Equal(t, ref.transformID("ent-admin"), ge.GetEntitlementIds()[0])
		require.NotContains(t, ge.GetEntitlementIds(), "ent-admin",
			"entitlement id must be sanitized, not passed through")

		// Resource-type tokens: declared "user" verbatim, undeclared "group" HMAC'd.
		require.Equal(t,
			[]string{"user", ref.sanitizeResourceTypeToken("group")},
			ge.GetResourceTypeIds())
		require.NotEqual(t, "group", ge.GetResourceTypeIds()[1],
			"undeclared resource-type token must be sanitized")

		// Structural shallow flag preserved.
		require.True(t, ge.GetShallow(), "shallow flag must survive the roundtrip")
	}
	require.Equal(t, nGrants, expandableCount,
		"every grant's expansion topology must survive sanitize -> rollback -> replay intact")
}
