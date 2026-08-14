package sync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
)

// TestStripExternalResourceMatchAnnotations is a fast, direct unit test of
// stripExternalResourceMatchAnnotations, independent of any full sync.
func TestStripExternalResourceMatchAnnotations(t *testing.T) {
	other := annotations.New(&v2.GrantExpandable{Shallow: true})

	t.Run("nil is a no-op", func(t *testing.T) {
		require.Empty(t, stripExternalResourceMatchAnnotations(nil))
	})

	t.Run("removes all three match types, keeps everything else", func(t *testing.T) {
		mixed := annotations.New(
			&v2.ExternalResourceMatchAll{ResourceType: v2.ResourceType_TRAIT_USER},
			&v2.ExternalResourceMatch{ResourceType: v2.ResourceType_TRAIT_USER, Key: "k", Value: "v"},
			&v2.ExternalResourceMatchID{Id: "some_id"},
			&v2.GrantExpandable{Shallow: true},
		)
		got := stripExternalResourceMatchAnnotations(mixed)
		gotAnnos := annotations.Annotations(got)
		require.False(t, gotAnnos.Contains(&v2.ExternalResourceMatchAll{}))
		require.False(t, gotAnnos.Contains(&v2.ExternalResourceMatch{}))
		require.False(t, gotAnnos.Contains(&v2.ExternalResourceMatchID{}))
		require.True(t, gotAnnos.Contains(&v2.GrantExpandable{}))
		require.Len(t, got, 1)
	})

	t.Run("annotations with no match type pass through unchanged", func(t *testing.T) {
		got := stripExternalResourceMatchAnnotations(other)
		require.Equal(t, []*anypb.Any(other), got)
	})
}

// failAfterNPutGrants lets the first n PutGrants calls through, then fails
// every call after that -- simulating an interruption partway through
// processGrantsWithExternalPrincipals's scan/expand loop, after some batches
// have already durably flushed but before the scan (and the delete loop)
// complete. successfulBatches records what actually got through, so callers
// can assert the cut landed where they intended it to (see
// requireExternalMatchBatchFlushedBeforeCut) instead of assuming it from the
// call count alone.
type failAfterNPutGrants struct {
	c1zstore.Store
	n                 int
	calls             int
	successfulBatches [][]*v2.Grant
}

var errMidScanCut = errors.New("test: cut after N PutGrants calls")

func (f *failAfterNPutGrants) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	f.calls++
	if f.calls > f.n {
		return errMidScanCut
	}
	batch := make([]*v2.Grant, len(grants))
	copy(batch, grants)
	f.successfulBatches = append(f.successfulBatches, batch)
	return f.Store.PutGrants(ctx, grants...)
}

// DeleteGrantByRefs forwards like countingGrantPutStore's (see its comment):
// embedding the c1zstore.Store interface doesn't promote this optional
// method, so without an explicit passthrough the syncer's delete loop
// always falls back to the id-based path even on engines (Pebble) that
// support the refs-based one.
func (f *failAfterNPutGrants) DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error {
	deleter, ok := f.Store.(grantByRefsDeleter)
	if !ok {
		return f.DeleteGrant(ctx, grant.GetId())
	}
	return deleter.DeleteGrantByRefs(ctx, grant)
}

// requireExternalMatchBatchFlushedBeforeCut fails the test if none of the
// successful PutGrants calls before the cut actually contained a resolved
// external-match replacement (principal in externalUserIDs). Without this,
// a future change to page sizing or call ordering could shift the cut to
// land before any external-match batch flushes, silently making the
// resume-amplification property these tests exist to check unreachable --
// they'd still pass, having stopped testing anything.
func requireExternalMatchBatchFlushedBeforeCut(t *testing.T, cutStore *failAfterNPutGrants, externalUserIDs map[string]bool) {
	t.Helper()
	for _, batch := range cutStore.successfulBatches {
		for _, g := range batch {
			if externalUserIDs[g.GetPrincipal().GetId().GetResource()] {
				return
			}
		}
	}
	t.Fatalf("no external-match replacement grant was flushed before the cut (%d successful batches); "+
		"the cut point no longer exercises resume-after-partial-progress", len(cutStore.successfulBatches))
}

// externalPrincipalResumeScenario reports which principal ids are the
// legitimate resolved-external-user targets, for the final correctness
// check against anything else (leftover placeholders, stray grants).
type externalPrincipalResumeScenario struct {
	externalUserIDs map[string]bool
}

// runExternalPrincipalResumeNoAmplificationTest is the shared harness behind
// the three shape-specific tests below. It runs the same scenario twice --
// once uninterrupted (the baseline), once cut mid-scan and then resumed with
// a fresh syncer against the same store -- and requires the resumed run to
// write exactly as much as the baseline. Before the fix in
// newGrantForExternalPrincipal, a replacement grant durably flushed by an
// interrupted attempt still carried its placeholder's ExternalResourceMatch*
// annotation, so a resumed attempt's fresh scan (empty newGrantIDs, no
// memory of the prior attempt) re-matched and rewrote it as if unresolved --
// real, engine-order-dependent amplification with no data-correctness
// symptom to catch it (grant ids are deterministic, so it never produced a
// duplicate). This asserts the stronger property directly: resume must not
// write more than a clean run would, regardless of engine.
func runExternalPrincipalResumeNoAmplificationTest(
	t *testing.T,
	engine c1zstore.Engine,
	userCount int,
	buildScenario func(t *testing.T, ctx context.Context, externalMc *mockConnector, internalMc *mockConnector, upns []string) externalPrincipalResumeScenario,
) {
	ctx := context.Background()

	countTotalWrites := func(counting *countingGrantPutStore) int {
		total := 0
		for _, batch := range counting.putBatches {
			total += len(batch)
		}
		return total
	}

	// --- Baseline: one clean, uninterrupted run. ---
	baselineDir, err := os.MkdirTemp("", "baseline-resume-noamp-test")
	require.NoError(t, err)
	defer os.RemoveAll(baselineDir)

	internalMcBaseline := newMockConnector()
	internalMcBaseline.rtDB = append(internalMcBaseline.rtDB, userResourceType, groupResourceType)
	externalMcBaseline := newMockConnector()
	externalMcBaseline.rtDB = append(externalMcBaseline.rtDB, userResourceType, groupResourceType)
	upns := make([]string, userCount)
	for i := range userCount {
		upns[i] = fmt.Sprintf("upn_%d@example.com", i)
	}
	_ = buildScenario(t, ctx, externalMcBaseline, internalMcBaseline, upns)

	externalC1zpath := filepath.Join(baselineDir, "external.c1z")
	externalSyncer, err := NewSyncer(ctx, externalMcBaseline, WithC1ZPath(externalC1zpath), WithTmpDir(baselineDir))
	require.NoError(t, err)
	require.NoError(t, externalSyncer.Sync(ctx))
	require.NoError(t, externalSyncer.Close(ctx))

	baselineC1zpath := filepath.Join(baselineDir, "internal.c1z")
	rawBaselineStore, err := dotc1z.NewStore(ctx, baselineC1zpath, dotc1z.WithEngine(engine), dotc1z.WithTmpDir(baselineDir))
	require.NoError(t, err)
	countingBaseline := &countingGrantPutStore{Store: rawBaselineStore}
	baselineSyncer, err := NewSyncer(ctx, internalMcBaseline, WithConnectorStore(countingBaseline), WithTmpDir(baselineDir), WithExternalResourceC1ZPath(externalC1zpath))
	require.NoError(t, err)
	require.NoError(t, baselineSyncer.Sync(ctx))
	require.NoError(t, baselineSyncer.Close(ctx))
	baselineTotal := countTotalWrites(countingBaseline)
	t.Logf("[%s] baseline (uninterrupted) total grant-writes: %d", engine, baselineTotal)

	// --- Interrupted + resumed run of the identical scenario. ---
	resumeDir, err := os.MkdirTemp("", "resume-noamp-test")
	require.NoError(t, err)
	defer os.RemoveAll(resumeDir)

	internalMcResume := newMockConnector()
	internalMcResume.rtDB = append(internalMcResume.rtDB, userResourceType, groupResourceType)
	externalMcResume := newMockConnector()
	externalMcResume.rtDB = append(externalMcResume.rtDB, userResourceType, groupResourceType)
	resumeScenario := buildScenario(t, ctx, externalMcResume, internalMcResume, upns)

	externalC1zpathResume := filepath.Join(resumeDir, "external.c1z")
	externalSyncerResume, err := NewSyncer(ctx, externalMcResume, WithC1ZPath(externalC1zpathResume), WithTmpDir(resumeDir))
	require.NoError(t, err)
	require.NoError(t, externalSyncerResume.Sync(ctx))
	require.NoError(t, externalSyncerResume.Close(ctx))

	internalC1zpath := filepath.Join(resumeDir, "internal.c1z")
	rawStore1, err := dotc1z.NewStore(ctx, internalC1zpath, dotc1z.WithEngine(engine), dotc1z.WithTmpDir(resumeDir))
	require.NoError(t, err)
	cutStore := &failAfterNPutGrants{Store: rawStore1, n: 2}
	syncer1, err := NewSyncer(ctx, internalMcResume, WithConnectorStore(cutStore), WithTmpDir(resumeDir), WithExternalResourceC1ZPath(externalC1zpathResume))
	require.NoError(t, err)
	// Pinned well above this test's actual runtime: the baseline/resume
	// write-count comparison below only holds if resume replays from
	// scratch, which only holds if no non-forced checkpoint lands before
	// the cut. minCheckpointInterval (10s) is normally short enough that a
	// slow CI runner could land one, making resume skip already-checkpointed
	// work and fail this test's assertion for a reason that has nothing to
	// do with the property it checks.
	syncer1Concrete, ok := syncer1.(*syncer)
	require.True(t, ok)
	syncer1Concrete.checkpointInterval = time.Hour
	require.ErrorIs(t, syncer1.Sync(ctx), errMidScanCut)
	requireExternalMatchBatchFlushedBeforeCut(t, cutStore, resumeScenario.externalUserIDs)

	ctxCancel, cancel := context.WithCancel(ctx)
	cancel()
	require.NoError(t, syncer1.Close(ctxCancel))

	rawStore2, err := dotc1z.NewStore(ctx, internalC1zpath, dotc1z.WithEngine(engine), dotc1z.WithTmpDir(resumeDir))
	require.NoError(t, err)
	countingResume := &countingGrantPutStore{Store: rawStore2}
	syncer2, err := NewSyncer(ctx, internalMcResume, WithConnectorStore(countingResume), WithTmpDir(resumeDir), WithExternalResourceC1ZPath(externalC1zpathResume))
	require.NoError(t, err)
	require.NoError(t, syncer2.Sync(ctx))
	require.NoError(t, syncer2.Close(ctx))
	resumeTotal := countTotalWrites(countingResume)
	t.Logf("[%s] resumed (cut mid-scan, then resumed) total grant-writes: %d", engine, resumeTotal)

	require.Equal(t, baselineTotal, resumeTotal,
		"resuming after a mid-scan interruption must not write more than an uninterrupted run -- "+
			"a mismatch means a leftover already-resolved grant retained its match annotation and got redundantly re-matched")

	store, err := dotc1z.NewStore(ctx, internalC1zpath, dotc1z.WithEngine(engine), dotc1z.WithTmpDir(resumeDir))
	require.NoError(t, err)
	allGrants, err := store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	require.NoError(t, store.Close(ctx))

	seen := make(map[string]bool, userCount)
	dupes := 0
	nonExternal := 0
	for _, grant := range allGrants.GetList() {
		principalID := grant.GetPrincipal().GetId().GetResource()
		if !resumeScenario.externalUserIDs[principalID] {
			nonExternal++
		}
		if seen[principalID] {
			dupes++
		}
		seen[principalID] = true
	}
	require.Len(t, allGrants.GetList(), userCount, "should have exactly one grant per external user after resume, no more no less")
	require.Zero(t, dupes, "no duplicate grants for the same principal after resume")
	require.Zero(t, nonExternal, "no leftover placeholder or stray grants after resume")
}

func TestExternalResourceMatchAllResumeAfterMidScanCut(t *testing.T) {
	skipChaosInShort(t)
	const userCount = 5*externalGrantFlushBatchSize + 200
	build := func(t *testing.T, ctx context.Context, externalMc, internalMc *mockConnector, upns []string) externalPrincipalResumeScenario {
		externalUserIDs := make(map[string]bool, userCount)
		for i := range upns {
			u, err := externalMc.AddUserProfile(ctx, fmt.Sprintf("ext_user_%d", i), map[string]any{})
			require.NoError(t, err)
			externalUserIDs[u.GetId().GetResource()] = true
		}
		internalGroup, _, err := internalMc.AddGroup(ctx, "internal_group")
		require.NoError(t, err)
		internalMc.grantDB[internalGroup.GetId().GetResource()] = []*v2.Grant{
			gt.NewGrant(
				internalGroup, "member",
				v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: "placeholder"}.Build(),
				gt.WithAnnotation(v2.ExternalResourceMatchAll_builder{ResourceType: v2.ResourceType_TRAIT_USER}.Build()),
			),
		}
		return externalPrincipalResumeScenario{externalUserIDs: externalUserIDs}
	}
	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			runExternalPrincipalResumeNoAmplificationTest(t, engine, userCount, build)
		})
	}
}

func TestExternalResourceMatchProfileResumeAfterMidScanCut(t *testing.T) {
	skipChaosInShort(t)
	// One distinct grant per user, each independently matched by profile
	// key/val -- the actual baton-sharepoint shape (helper.go emits
	// ExternalResourceMatch keyed on userPrincipalName), not a fan-out from
	// a single ExternalResourceMatchAll grant. Smaller than the MatchAll
	// count above: each user here is its own PutGrants-worthy grant (not a
	// single fan-out source), so reaching multiple flush batches with a
	// cut-then-resume needs far fewer of them.
	const userCount = 2*externalGrantFlushBatchSize + 50
	build := func(t *testing.T, ctx context.Context, externalMc, internalMc *mockConnector, upns []string) externalPrincipalResumeScenario {
		externalUserIDs := make(map[string]bool, userCount)
		for i, upn := range upns {
			u, err := externalMc.AddUserProfile(ctx, fmt.Sprintf("ext_user_%d", i), map[string]any{"userPrincipalName": upn})
			require.NoError(t, err)
			externalUserIDs[u.GetId().GetResource()] = true
		}
		internalGroup, _, err := internalMc.AddGroup(ctx, "internal_group")
		require.NoError(t, err)
		grants := make([]*v2.Grant, 0, len(upns))
		for i, upn := range upns {
			grants = append(grants, gt.NewGrant(
				internalGroup, "member",
				v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: fmt.Sprintf("placeholder_%d", i)}.Build(),
				gt.WithAnnotation(v2.ExternalResourceMatch_builder{
					ResourceType: v2.ResourceType_TRAIT_USER, Key: "userPrincipalName", Value: upn,
				}.Build()),
			))
		}
		internalMc.grantDB[internalGroup.GetId().GetResource()] = grants
		return externalPrincipalResumeScenario{externalUserIDs: externalUserIDs}
	}
	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			runExternalPrincipalResumeNoAmplificationTest(t, engine, userCount, build)
		})
	}
}

func TestExternalResourceMatchIDResumeAfterMidScanCut(t *testing.T) {
	skipChaosInShort(t)
	// Sharepoint also emits ExternalResourceMatchID directly (helper.go).
	// One distinct grant per user, matched by external id rather than a
	// profile key/val lookup.
	const userCount = 2*externalGrantFlushBatchSize + 50
	build := func(t *testing.T, ctx context.Context, externalMc, internalMc *mockConnector, upns []string) externalPrincipalResumeScenario {
		externalUserIDs := make(map[string]bool, userCount)
		externalIDs := make([]string, len(upns))
		for i := range upns {
			u, err := externalMc.AddUserProfile(ctx, fmt.Sprintf("ext_user_%d", i), map[string]any{})
			require.NoError(t, err)
			externalUserIDs[u.GetId().GetResource()] = true
			externalIDs[i] = u.GetId().GetResource()
		}
		internalGroup, _, err := internalMc.AddGroup(ctx, "internal_group")
		require.NoError(t, err)
		grants := make([]*v2.Grant, 0, len(upns))
		for i, extID := range externalIDs {
			grants = append(grants, gt.NewGrant(
				internalGroup, "member",
				v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: fmt.Sprintf("placeholder_%d", i)}.Build(),
				gt.WithAnnotation(v2.ExternalResourceMatchID_builder{Id: extID}.Build()),
			))
		}
		internalMc.grantDB[internalGroup.GetId().GetResource()] = grants
		return externalPrincipalResumeScenario{externalUserIDs: externalUserIDs}
	}
	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			runExternalPrincipalResumeNoAmplificationTest(t, engine, userCount, build)
		})
	}
}
