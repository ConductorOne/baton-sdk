package c1zsanitize

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/conductorone/baton-sdk/pkg/sync"
)

// buildNestedExpandableC1Z writes a pre-expansion c1z modeling nested groups:
// g2 is a member of g1 (an expandable grant via group:g2:member) and g3 is a
// member of g2 (expandable via group:g3:member); alice is a direct member of g3
// and bob a direct member of g2. The sync is started but NOT finished and NOT
// expanded — expandViaRealSyncer below finishes and expands it.
//
// Unlike cmd/baton's buildPreExpansionC1Z, this fixture sets NO hand-built
// GrantSources: every Sources map in the resulting file is produced by the real
// expander, so "GrantSources topology" here means precisely the expansion
// topology under test. The nested shape forces multi-hop derivation (alice and
// bob reach g1 through g2), giving several derived grants rather than one.
func buildNestedExpandableC1Z(t *testing.T, ctx context.Context, path string) string {
	t.Helper()
	f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, f.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}.Build(),
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
	))

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	g3 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g3"}.Build(), DisplayName: "G3"}.Build()
	alice := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(), DisplayName: "Alice"}.Build()
	bob := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(), DisplayName: "Bob"}.Build()
	require.NoError(t, f.PutResources(ctx, g1, g2, g3, alice, bob))

	g1Member := v2.Entitlement_builder{Id: "group:g1:member", DisplayName: "g1 member", Resource: g1, Slug: "member"}.Build()
	g2Member := v2.Entitlement_builder{Id: "group:g2:member", DisplayName: "g2 member", Resource: g2, Slug: "member"}.Build()
	g3Member := v2.Entitlement_builder{Id: "group:g3:member", DisplayName: "g3 member", Resource: g3, Slug: "member"}.Build()
	require.NoError(t, f.PutEntitlements(ctx, g1Member, g2Member, g3Member))

	// g2 member of g1, expandable into g1:member from g2:member.
	nestG2InG1 := v2.Grant_builder{
		Id:          "group:g1:member:group:g2",
		Entitlement: g1Member,
		Principal:   g2,
		Annotations: []*anypb.Any{anyTB(t, v2.GrantExpandable_builder{EntitlementIds: []string{"group:g2:member"}}.Build())},
	}.Build()
	// g3 member of g2, expandable into g2:member from g3:member.
	nestG3InG2 := v2.Grant_builder{
		Id:          "group:g2:member:group:g3",
		Entitlement: g2Member,
		Principal:   g3,
		Annotations: []*anypb.Any{anyTB(t, v2.GrantExpandable_builder{EntitlementIds: []string{"group:g3:member"}}.Build())},
	}.Build()
	aliceG3 := v2.Grant_builder{Id: "group:g3:member:user:alice", Entitlement: g3Member, Principal: alice}.Build()
	bobG2 := v2.Grant_builder{Id: "group:g2:member:user:bob", Entitlement: g2Member, Principal: bob}.Build()
	require.NoError(t, f.PutGrants(ctx, nestG2InG1, nestG3InG2, aliceG3, bobG2))

	require.NoError(t, f.Close(ctx))
	return syncID
}

// expandViaRealSyncer runs the production grant expander over an existing c1z
// by path, exactly as cmd/baton's rollback_expansion_test.expandViaSyncer does:
// an empty connector with WithOnlyExpandGrants, so only the expansion phase runs
// over the data already in the file. It finishes the sync with a non-empty
// completed token, sets the supports_diff marker, and writes the derived grants.
// WithConnectorStore is deliberately NOT used (it double-closes the store).
func expandViaRealSyncer(t *testing.T, ctx context.Context, path, syncID string) {
	t.Helper()
	conn, err := sdk.NewEmptyConnector()
	require.NoError(t, err)
	syncer, err := sync.NewSyncer(ctx, conn,
		sync.WithC1ZPath(path),
		sync.WithSyncID(syncID),
		sync.WithOnlyExpandGrants(),
		sync.WithTmpDir(t.TempDir()),
	)
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))
	require.NoError(t, syncer.Close(ctx))
}

// singleSyncID returns the id of the one sync in path, failing if there is not
// exactly one. Sanitize emits one destination sync per source sync, with a NEW
// (sanitized) id, so the rollback/replay must target this id, not the source's.
func singleSyncID(t *testing.T, ctx context.Context, path string) string {
	t.Helper()
	ro := mustOpen(t, ctx, path, true)
	defer ro.Close(ctx)
	syncs, err := listAllSyncs(ctx, ro)
	require.NoError(t, err)
	require.Len(t, syncs, 1)
	return syncs[0].GetId()
}

// loadGrantsBySync reads every grant of syncID keyed by grant id, with the
// GrantSources the writer persisted. ListGrants returns Sources on the grant
// (cmd/baton's loadGrant relies on the same), which is the topology this test
// compares across the roundtrip.
func loadGrantsBySync(t *testing.T, ctx context.Context, path, syncID string) map[string]*v2.Grant {
	t.Helper()
	ro := mustOpen(t, ctx, path, true)
	defer ro.Close(ctx)
	require.NoError(t, ro.SetCurrentSync(ctx, syncID))
	out := map[string]*v2.Grant{}
	pageToken := ""
	for {
		resp, err := ro.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 1000, PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			out[g.GetId()] = g
		}
		if resp.GetNextPageToken() == "" {
			return out
		}
		pageToken = resp.GetNextPageToken()
	}
}

// rollbackDeleteCount returns how many grants a dry-run rollback would delete on
// syncID — the count of purely expander-derived rows still present.
func rollbackDeleteCount(t *testing.T, ctx context.Context, path, syncID string) int {
	t.Helper()
	ro := mustOpen(t, ctx, path, true)
	defer ro.Close(ctx)
	res, err := ro.RollbackExpansion(ctx, syncID, true)
	require.NoError(t, err)
	return res.GrantsDeleted
}

// TestSanitizeRealRollbackReplayRoundtrip exercises the REAL rollback+replay
// mechanism — dotc1z.RollbackExpansion plus the production grant expander —
// through a sanitized c1z, rather than the resume-checkpoint proxy that
// TestSanitizeRoundtripPreservesGrantExpansionTopology uses. The two tests guard
// different concerns and both must pass.
//
// Real APIs used (signatures confirmed against the PR #938 tree):
//   - (*dotc1z.C1File).RollbackExpansion(ctx, syncID string, dryRun bool, ...RollbackOption) (*RollbackResult, error)
//   - dotc1z.WithSyncTokenRewrite(func(token string) (string, error)) RollbackOption
//   - sync.PrepareExpansionReplayToken(stateStr string) (string, error) — re-marks
//     a finished sync's token for expansion so the replay re-derives instead of no-oping.
//   - sync.WithOnlyExpandGrants() — runs only the expansion phase over existing data.
//
// Flow:
//
//	a. PRE-EXPANSION FIXTURE — buildNestedExpandableC1Z: nested groups with
//	   GrantExpandable annotations and no hand-set Sources.
//	b. REAL SYNCER EXPANSION — expandViaRealSyncer: derives grants, sets
//	   supports_diff, finishes the sync.
//	c. SANITIZE — Sanitize the expanded c1z to a new file. Sanitize carries the
//	   supports_diff marker and ends the destination sync, so the output is a
//	   finished, expansion-marked sync RollbackExpansion accepts; the GrantSources
//	   keys and GrantExpandable ids are relabeled consistently under the secret.
//	d. ROLLBACK — RollbackExpansion(..., WithSyncTokenRewrite(PrepareExpansionReplayToken))
//	   on the sanitized output: deletes the derived grants, clears self-sources,
//	   and rewrites the token so the sync is replayable.
//	e. REPLAY — expandViaRealSyncer again on the rolled-back output.
//	f. ASSERT — the replayed grants match the pre-rollback (post-sanitize)
//	   expanded state: identical id set, identical per-grant GrantSources
//	   (proto.Equal), and the same number of grants a fresh rollback would
//	   delete. Because sanitize is a consistent relabeling, expanding the
//	   rolled-back sanitized base reproduces exactly the sanitized expansion.
func TestSanitizeRealRollbackReplayRoundtrip(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	sanPath := filepath.Join(tmp, "sanitized.c1z")
	secret := bytes32("real-rollback-replay-roundtrip")

	// a + b: pre-expansion fixture, then real expansion.
	origSyncID := buildNestedExpandableC1Z(t, ctx, srcPath)
	expandViaRealSyncer(t, ctx, srcPath, origSyncID)

	// c: sanitize the expanded file. sanitizeToFile pins the fixed anchor.
	sanitizeToFile(t, ctx, srcPath, sanPath, secret, Options{})
	sanSyncID := singleSyncID(t, ctx, sanPath)

	// Pre-rollback expanded state, in sanitized id-space.
	preRollback := loadGrantsBySync(t, ctx, sanPath, sanSyncID)
	derivedDeletable := rollbackDeleteCount(t, ctx, sanPath, sanSyncID)
	require.Positive(t, derivedDeletable,
		"the real expansion through a sanitized file must leave derived grants to roll back")

	// d: rollback in place on the sanitized output, rewriting the token so the
	// sync can be replayed.
	func() {
		store := mustOpen(t, ctx, sanPath, false)
		defer store.Close(ctx)
		res, err := store.RollbackExpansion(ctx, sanSyncID, false,
			dotc1z.WithSyncTokenRewrite(sync.PrepareExpansionReplayToken))
		require.NoError(t, err)
		require.Equal(t, derivedDeletable, res.GrantsDeleted,
			"rollback must delete exactly the derived grants the dry run counted")
	}()

	postRollback := loadGrantsBySync(t, ctx, sanPath, sanSyncID)
	require.Less(t, len(postRollback), len(preRollback),
		"rollback must remove the derived grants from the sanitized output")
	require.Zero(t, rollbackDeleteCount(t, ctx, sanPath, sanSyncID),
		"a rolled-back sync has no derived grants left to delete")

	// e: replay — re-expand the rolled-back sanitized output.
	expandViaRealSyncer(t, ctx, sanPath, sanSyncID)

	// f: the replayed expansion reproduces the pre-rollback sanitized topology.
	replayed := loadGrantsBySync(t, ctx, sanPath, sanSyncID)
	require.Equal(t, len(preRollback), len(replayed),
		"replay must restore the full grant count")
	require.Equal(t, derivedDeletable, rollbackDeleteCount(t, ctx, sanPath, sanSyncID),
		"replay must re-derive exactly the grants the rollback removed")

	for id, pre := range preRollback {
		got, ok := replayed[id]
		require.Truef(t, ok, "grant %q must be re-derived by replay", id)
		require.Truef(t, proto.Equal(pre.GetSources(), got.GetSources()),
			"grant %q GrantSources topology must survive the rollback->replay roundtrip", id)
	}
}
