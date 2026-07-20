package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Throwaway cost measurement for runIngestionInvariants against a real
// (whale-scale, post-expansion) pebble c1z. Times each invariant's
// underlying store work exactly as the syncer performs it. Gated on
// BATON_WHALE_C1Z pointing at the artifact.
//
// TEMPORARY VERIFICATION FILE — safe to delete; nothing references it.

import (
	"os"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/logging"

	"github.com/stretchr/testify/require"
)

func TestWhaleInvariantCost(t *testing.T) {
	path := os.Getenv("BATON_WHALE_C1Z")
	if path == "" {
		t.Skip("set BATON_WHALE_C1Z to the artifact path")
	}
	ctx, err := logging.Init(t.Context(), logging.WithLogLevel("error"))
	require.NoError(t, err)

	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithReadOnly(true),
		dotc1z.WithTmpDir(t.TempDir()),
	)
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	run, err := store.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, run)
	require.NoError(t, store.SetCurrentSync(ctx, run.ID))

	facts, ok := store.(dotc1z.IngestInvariantStore)
	require.True(t, ok)

	// --- I3: grant→resource referential check, exactly as
	// checkGrantResourceReferences performs it (skip-scan + point Get per
	// distinct entitlement resource; the insert-fact probe only on
	// dangling refs).
	start := time.Now()
	distinct, dangling, annotated := 0, 0, 0
	require.NoError(t, facts.ForEachDistinctGrantEntitlementResource(ctx, func(rt, rid string) error {
		distinct++
		exists, err := facts.HasResourceRecord(ctx, rt, rid)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
		dangling++
		carry, err := facts.GrantsForEntResourceCarryInsertFact(ctx, rt, rid)
		if err != nil {
			return err
		}
		if carry {
			annotated++
		}
		return nil
	}))
	i3 := time.Since(start)
	t.Logf("I3 grant→resource: %v (distinct ent-resources=%d dangling=%d insert-annotated=%d)", i3, distinct, dangling, annotated)

	// --- I5: stored-keyspace exclusion-group validation, exactly as
	// checkStoredExclusionGroups performs it (full entitlement listing
	// + tracker.record per row).
	start = time.Now()
	tracker := &exclusionGroupTracker{}
	ents := 0
	pageToken := ""
	for {
		resp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, ent := range resp.GetList() {
			ents++
			require.NoError(t, tracker.record(ent))
		}
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	i5 := time.Since(start)
	t.Logf("I5 exclusion groups: %v (entitlements=%d)", i5, ents)

	// --- I7: entitlement→resource referential check, exactly as
	// checkEntitlementResourceReferences performs it (skip-scan + point
	// Get per distinct entitlement resource).
	start = time.Now()
	i7Distinct, i7Dangling := 0, 0
	require.NoError(t, facts.ForEachDistinctEntitlementResource(ctx, func(rt, rid string) error {
		i7Distinct++
		exists, err := facts.HasResourceRecord(ctx, rt, rid)
		if err != nil {
			return err
		}
		if !exists {
			i7Dangling++
		}
		return nil
	}))
	i7 := time.Since(start)
	t.Logf("I7 entitlement→resource: %v (distinct resources=%d dangling=%d)", i7, i7Distinct, i7Dangling)

	// --- I8: grant→entitlement referential check, exactly as
	// checkGrantEntitlementReferences performs it (skip-scan at
	// entitlement-identity granularity + point probe per distinct
	// entitlement; the engine only surfaces dangling ones).
	typeKnown := map[string]bool{}
	typeExists := func(rt string) bool {
		if v, ok := typeKnown[rt]; ok {
			return v
		}
		_, err := store.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
			ResourceTypeId: rt,
		}.Build())
		typeKnown[rt] = err == nil
		return typeKnown[rt]
	}

	start = time.Now()
	i8Dangling, i8UnsyncedType := 0, 0
	require.NoError(t, facts.ForEachDanglingGrantEntitlement(ctx, func(entID, rt, rid string) error {
		i8Dangling++
		if !typeExists(rt) {
			i8UnsyncedType++
		}
		return nil
	}))
	i8 := time.Since(start)
	t.Logf("I8 grant→entitlement: %v (dangling=%d of which unsynced-type=%d)", i8, i8Dangling, i8UnsyncedType)

	// --- I9: grant→principal dangling scan over by_principal. On a
	// sealed artifact the deferred build already ran, so
	// EnsureGrantIndexes no-ops and the scan is one seek + probe per
	// distinct principal.
	start = time.Now()
	i9Dangling, i9MatchOnly, i9UnsyncedType := 0, 0, 0
	require.NoError(t, facts.EnsureGrantIndexes(ctx))
	require.NoError(t, facts.ForEachDanglingGrantPrincipal(ctx, func(rt, rid string, matchOnly bool, _ int64) error {
		i9Dangling++
		if matchOnly {
			i9MatchOnly++
		}
		if !typeExists(rt) {
			i9UnsyncedType++
		}
		return nil
	}))
	i9 := time.Since(start)
	t.Logf("I9 grant→principal: %v (dangling principals=%d match-carrier-only=%d unsynced-type=%d)", i9, i9Dangling, i9MatchOnly, i9UnsyncedType)

	// --- Contrast: a full O(grants) pass over the grant keyspace — what
	// I3 would cost if it were per-grant instead of per-distinct-resource.
	// Uses the raw listing read path (value decode per row, like any
	// full-table invariant would pay).
	start = time.Now()
	grants := 0
	pageToken = ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken, PageSize: 1000}.Build())
		require.NoError(t, err)
		grants += len(resp.GetList())
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	scan := time.Since(start)
	t.Logf("CONTRAST full grant scan (O(grants)): %v (grants=%d) — vs I3's %v", scan, grants, i3)

	// --- I4 (STRICT-ONLY in production; measured for reference): full
	// resource listing + annotation walk, as checkChildScheduling would.
	start = time.Now()
	resources, withCRT := 0, 0
	pageToken = ""
	for {
		resp, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, r := range resp.GetList() {
			resources++
			for _, a := range r.GetAnnotations() {
				if a.MessageIs((*v2.ChildResourceType)(nil)) {
					withCRT++
					break
				}
			}
		}
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	i4 := time.Since(start)
	t.Logf("I4 (strict-only) child scheduling scan: %v (resources=%d with-child-annotations=%d)", i4, resources, withCRT)
}
