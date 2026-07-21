package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// TestPushChildResourceActionsDedupesPerSync pins the per-sync child
// scheduling dedupe: a parent discovered by several ingestion paths in
// one sync must schedule each (childType, parent) pair exactly once —
// a duplicate would double the child-listing connector work. The
// dedupe rides the I4 evidence set (childScheduleSet.recordIfNew), so
// the evidence stays complete for every pair regardless of which
// discovery scheduled.
func TestPushChildResourceActionsDedupesPerSync(t *testing.T) {
	ctx := context.Background()
	s := &syncer{state: newState()}

	countActions := func() int {
		st, ok := s.state.(*state)
		require.True(t, ok)
		return len(st.actions)
	}

	s.pushChildResourceActions(ctx, []string{"project", "repo"}, "org", "org0")
	require.Equal(t, 2, countActions())

	// Second discovery of the same parent: no new actions.
	s.pushChildResourceActions(ctx, []string{"project", "repo"}, "org", "org0")
	require.Equal(t, 2, countActions())

	// A different parent still schedules.
	s.pushChildResourceActions(ctx, []string{"project"}, "org", "org1")
	require.Equal(t, 3, countActions())

	// The I4 evidence is intact for every recorded pair.
	require.True(t, s.childSchedule.has("project", "org", "org0"))
	require.True(t, s.childSchedule.has("repo", "org", "org0"))
	require.True(t, s.childSchedule.has("project", "org", "org1"))
}

// TestIngestInvariantAnnotationCoverage is the completeness meta-test for
// the ingestion-invariant registry: every connector annotation known to
// imply a syncer side effect must map to the mechanism that guarantees it
// regardless of ingestion path (stream today, or whatever comes next).
// Adding a new side-effect-implying annotation without extending
// sideEffectAnnotationCoverage — and the invariant behind it — fails
// here instead of in review round eleven.
func TestIngestInvariantAnnotationCoverage(t *testing.T) {
	// TypeScopedEntitlements and TypeScopedGrants join this list (mapped
	// to I7/I8) when the type-scoped listing protos land.
	sideEffectAnnotations := []proto.Message{
		&v2.GrantExpandable{},
		&v2.ExternalResourceMatch{},
		&v2.ExternalResourceMatchAll{},
		&v2.ExternalResourceMatchID{},
		&v2.InsertResourceGrants{},
		&v2.ChildResourceType{},
		&v2.EntitlementExclusionGroup{},
	}
	for _, msg := range sideEffectAnnotations {
		name := string(msg.ProtoReflect().Descriptor().FullName())
		require.Contains(t, sideEffectAnnotationCoverage, name,
			"side-effect-implying annotation %s has no registered ingestion invariant", name)
	}
	require.Len(t, sideEffectAnnotationCoverage, len(sideEffectAnnotations),
		"coverage map and the enumerated annotation set drifted; update both together")
}

// ingestInvariantExclusions documents the invariant ids that the
// annotation registry references but the verdict table deliberately
// does NOT carry: their mechanisms are still stream-coupled (correct
// while the response loop is the only ingestion path) and their
// store-derived replacements land with source-cache replay. Removing an
// entry here without adding the table row fails the verdict-table
// meta-test.
var ingestInvariantExclusions = map[string]string{
	"I1": "expansion arming rides the response loop (SetNeedsExpansion) + needs_expansion persistence; store-derived probe arrives with replay",
	"I2": "external-match arming rides the response loop (SetHasExternalResourcesGrants); store-derived existence-bit repair arrives with replay",
	"I6": "source-cache scope consistency has no subject until replay state exists; arrives with replay",
}

// TestIngestInvariantVerdictTable is the completeness meta-test for the
// verdict table: the table (not check-internal if-branches) is the
// policy matrix, so every hole must be visible here.
func TestIngestInvariantVerdictTable(t *testing.T) {
	byID := map[string]*ingestInvariant{}
	order := make([]string, 0, len(ingestInvariants))
	for i := range ingestInvariants {
		inv := &ingestInvariants[i]
		require.NotContainsf(t, byID, inv.id, "duplicate verdict-table row %s", inv.id)
		require.NotNilf(t, inv.check, "verdict-table row %s has no check", inv.id)
		byID[inv.id] = inv
		order = append(order, inv.id)
	}

	// Every invariant the annotation registry names is either a table
	// row or a documented exclusion — and never both.
	for anno, mechanism := range sideEffectAnnotationCoverage {
		id, _, ok := strings.Cut(mechanism, ":")
		require.Truef(t, ok, "registry entry for %s does not lead with an invariant id: %q", anno, mechanism)
		_, inTable := byID[id]
		_, excluded := ingestInvariantExclusions[id]
		require.Truef(t, inTable || excluded,
			"registry entry for %s names %s, which is neither a verdict-table row nor a documented exclusion", anno, id)
		require.Falsef(t, inTable && excluded,
			"%s is both a verdict-table row and a documented exclusion; remove one", id)
	}

	// The replay ladder does not exist yet: no row may pre-enable it.
	for _, inv := range ingestInvariants {
		require.Falsef(t, inv.ridesReplayLadder,
			"row %s sets ridesReplayLadder, but the ErrReplayIntegrity ladder arrives with replay", inv.id)
	}

	// A fail-fast-only check trivially promotes under fail-fast; the
	// fields must agree so the matrix reads honestly.
	for _, inv := range ingestInvariants {
		if inv.failFastOnly {
			require.Truef(t, inv.failFastPromotes, "row %s is failFastOnly but not failFastPromotes", inv.id)
		}
	}

	// The referential family order is load-bearing for the future drop
	// cascade (I7 drops orphan entitlements whose grants I8 then
	// catches; I9 runs last): pin it while the checks are read-only so
	// the drop flip cannot reorder verdicts.
	idx := map[string]int{}
	for i, id := range order {
		idx[id] = i
	}
	require.Less(t, idx["I7"], idx["I3"], "I7 must run before I3")
	require.Less(t, idx["I3"], idx["I8"], "I3 must run before I8")
	require.Less(t, idx["I8"], idx["I9"], "I8 must run before I9")
	require.Equal(t, "I5", order[0], "I5 (cheapest, hard-fail class) runs first")

	// Halt stages: unique, and every declared stage is enumerated by
	// ingestInvariantHaltStages (the halt sweep iterates that list, so
	// enumeration is coverage).
	stages := ingestInvariantHaltStages()
	seen := map[string]bool{}
	for _, st := range stages {
		require.NotEmpty(t, st)
		require.Falsef(t, seen[st], "duplicate halt stage %q", st)
		seen[st] = true
	}
	for _, inv := range ingestInvariants {
		if inv.haltStage != "" {
			require.Truef(t, seen[inv.haltStage], "row %s halt stage %q missing from ingestInvariantHaltStages", inv.id, inv.haltStage)
		}
	}
	require.True(t, seen[haltStageI9IndexesEnsured])
	require.Equal(t, haltStageInvariantsComplete, stages[len(stages)-1],
		"the whole-pass seam fires last")
}

// newInvariantTestStore opens a fresh SQLite-backed C1File with an open
// full sync, for driving RunIngestInvariants directly.
func newInvariantTestStore(ctx context.Context, t *testing.T) (*dotc1z.C1File, string) {
	t.Helper()
	tempDir := t.TempDir()
	store, err := dotc1z.NewC1ZFile(ctx, filepath.Join(tempDir, "invariants.c1z"), dotc1z.WithTmpDir(tempDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	return store, syncID
}

// TestIngestInvariantI5StoredValidation drives I5 through
// RunIngestInvariants over a real store: conflicts must fail the pass
// in every mode, clean keyspaces must pass, and — the reason the
// streaming validator was retired — the verdict must not depend on how
// rows arrived or which sync type wrote them.
func TestIngestInvariantI5StoredValidation(t *testing.T) {
	ctx := context.Background()

	newEnt := func(rt *v2.ResourceType, resourceID, slug string, opts ...et.EntitlementOption) *v2.Entitlement {
		r, err := rs.NewResource(resourceID, rt, resourceID)
		require.NoError(t, err)
		return et.NewPermissionEntitlement(r, slug, opts...)
	}

	t.Run("clean keyspace passes", func(t *testing.T) {
		store, syncID := newInvariantTestStore(ctx, t)
		require.NoError(t, store.PutEntitlements(ctx,
			newEnt(userResourceType, "user1", "viewer", et.WithExclusionGroup("user-role")),
			newEnt(groupResourceType, "group1", "admin", et.WithExclusionGroup("group-role")),
		))
		require.NoError(t, RunIngestInvariants(ctx, store, IngestInvariantsPolicy{
			ActiveSyncID: syncID,
			SyncType:     connectorstore.SyncTypeFull,
		}))
	})

	t.Run("cross-resource-type group conflict fails", func(t *testing.T) {
		store, syncID := newInvariantTestStore(ctx, t)
		require.NoError(t, store.PutEntitlements(ctx,
			newEnt(userResourceType, "user1", "viewer", et.WithExclusionGroup("role")),
			newEnt(groupResourceType, "group1", "admin", et.WithExclusionGroup("role")),
		))
		err := RunIngestInvariants(ctx, store, IngestInvariantsPolicy{
			ActiveSyncID: syncID,
			SyncType:     connectorstore.SyncTypeFull,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "ingest invariant I5 violated")
		require.Contains(t, err.Error(), `"role"`)
	})

	t.Run("duplicate defaults fail", func(t *testing.T) {
		store, syncID := newInvariantTestStore(ctx, t)
		require.NoError(t, store.PutEntitlements(ctx,
			newEnt(userResourceType, "user1", "viewer", et.WithExclusionGroupDefault("role", 1)),
			newEnt(userResourceType, "user2", "editor", et.WithExclusionGroupDefault("role", 2)),
		))
		err := RunIngestInvariants(ctx, store, IngestInvariantsPolicy{
			ActiveSyncID: syncID,
			SyncType:     connectorstore.SyncTypeFull,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "is_default")
	})

	t.Run("compaction expand pass warns instead of failing", func(t *testing.T) {
		// A keep-newer merge of two individually-valid syncs can union
		// entitlement generations into conflicts no input contained
		// (e.g. a moved default keeps both is_default rows). The expand
		// pass must not reject the merge's own artifact; fail-fast
		// still promotes.
		store, syncID := newInvariantTestStore(ctx, t)
		require.NoError(t, store.PutEntitlements(ctx,
			newEnt(userResourceType, "user1", "viewer", et.WithExclusionGroupDefault("role", 1)),
			newEnt(userResourceType, "user2", "editor", et.WithExclusionGroupDefault("role", 2)),
		))
		policy := IngestInvariantsPolicy{
			ActiveSyncID:     syncID,
			SyncType:         connectorstore.SyncTypeFull,
			OnlyExpandGrants: true,
		}
		require.NoError(t, RunIngestInvariants(ctx, store, policy),
			"merge-manufactured conflicts must aggregate into a warning on the expand pass, not fail the seal")
		policy.FailFast = true
		err := RunIngestInvariants(ctx, store, policy)
		require.Error(t, err, "fail-fast promotes the expand pass's tolerated warning")
		require.Contains(t, err.Error(), "ingest invariant I5 violated")
	})

	t.Run("I5 runs on partial syncs too", func(t *testing.T) {
		store, err := dotc1z.NewC1ZFile(ctx, filepath.Join(t.TempDir(), "partial.c1z"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close(ctx) })
		syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
		require.NoError(t, err)
		require.NoError(t, store.PutEntitlements(ctx,
			newEnt(userResourceType, "user1", "viewer", et.WithExclusionGroup("role")),
			newEnt(groupResourceType, "group1", "admin", et.WithExclusionGroup("role")),
		))
		err = RunIngestInvariants(ctx, store, IngestInvariantsPolicy{
			ActiveSyncID: syncID,
			SyncType:     connectorstore.SyncTypePartial,
		})
		require.Error(t, err, "a partial store holding both conflicting rows is valid evidence")
		require.Contains(t, err.Error(), "ingest invariant I5 violated")
	})
}

// TestIngestInvariantI4ChildScheduling drives I4 through
// RunIngestInvariants: a stored resource carrying ChildResourceType
// whose (childType, parent) pair was never recorded on the schedule set
// must fail under fail-fast, and must be skipped entirely in default
// mode and on processes that did not run the resources phase.
func TestIngestInvariantI4ChildScheduling(t *testing.T) {
	ctx := context.Background()
	store, syncID := newInvariantTestStore(ctx, t)

	parent, err := rs.NewResource("org0", groupResourceType, "org0",
		rs.WithAnnotation(v2.ChildResourceType_builder{ResourceTypeId: "project"}.Build()))
	require.NoError(t, err)
	require.NoError(t, store.PutResources(ctx, parent))

	policy := func(schedule *childScheduleSet, failFast, phaseRan bool) IngestInvariantsPolicy {
		return IngestInvariantsPolicy{
			ActiveSyncID:      syncID,
			SyncType:          connectorstore.SyncTypeFull,
			FailFast:          failFast,
			childSchedule:     schedule,
			resourcesPhaseRan: phaseRan,
		}
	}

	// Unscheduled pair under fail-fast: violation.
	err = RunIngestInvariants(ctx, store, policy(&childScheduleSet{}, true, true))
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I4 violated")
	require.Contains(t, err.Error(), "project under group/org0")

	// Recording the pair satisfies the invariant.
	scheduled := &childScheduleSet{}
	require.True(t, scheduled.recordIfNew("project", "group", "org0"))
	require.NoError(t, RunIngestInvariants(ctx, store, policy(scheduled, true, true)))

	// Default mode skips I4 entirely (failFastOnly row).
	require.NoError(t, RunIngestInvariants(ctx, store, policy(&childScheduleSet{}, false, true)))

	// A process that resumed past the resources phase cannot verify the
	// predicate; the check declines instead of false-positives.
	require.NoError(t, RunIngestInvariants(ctx, store, policy(&childScheduleSet{}, true, false)))
}

// TestSyncFailsOnStoredExclusionGroupConflict pins the syncer seam:
// the invariant pass runs after collection and a violating store is
// never sealed as complete. The conflict is only observable POST-
// collection (two entitlements on different resource types share a
// group id, emitted on different pages), which is exactly the shape
// the retired streaming validator handled and the stored validation
// must keep catching.
func TestSyncFailsOnStoredExclusionGroupConflict(t *testing.T) {
	// A second entitlement-bearing resource type (the shared
	// userResourceType fixture skips entitlements at the type level, so
	// it can never carry the conflicting row).
	projectResourceType := v2.ResourceType_builder{
		Id:          "project",
		DisplayName: "Project",
	}.Build()

	runWithSyncModes(t, func(t *testing.T, extraOpts []SyncOpt) {
		ctx := t.Context()
		tempDir := t.TempDir()

		mc := newMockConnector()
		mc.rtDB = append(mc.rtDB, projectResourceType, groupResourceType)

		project, err := rs.NewResource("project1", projectResourceType, "project1")
		require.NoError(t, err)
		mc.AddResource(ctx, project)
		group, _, err := mc.AddGroup(ctx, "group1")
		require.NoError(t, err)

		// Same exclusion group id on two resource types: an I5 conflict.
		mc.entDB["project1"] = append(mc.entDB["project1"],
			et.NewPermissionEntitlement(project, "viewer", et.WithExclusionGroup("shared-role")))
		mc.entDB["group1"] = append(mc.entDB["group1"],
			et.NewPermissionEntitlement(group, "admin", et.WithExclusionGroup("shared-role")))

		opts := append([]SyncOpt{
			WithC1ZPath(filepath.Join(tempDir, "i5-conflict.c1z")),
			WithTmpDir(tempDir),
		}, extraOpts...)
		syncer, err := NewSyncer(ctx, mc, opts...)
		require.NoError(t, err)

		err = syncer.Sync(ctx)
		require.Error(t, err, "a sync with an exclusion-group conflict must not seal")
		require.Contains(t, err.Error(), "ingest invariant I5 violated")
		require.Contains(t, err.Error(), `"shared-role"`)
		require.NoError(t, syncer.Close(ctx))
	})
}

// TestIngestInvariantHaltHook pins the halt-hook plumbing end to end
// over a real store: a nil-returning hook observes only declared
// stages, and an erroring hook fails the pass at exactly its stage.
// (The per-stage crash/resume sweep rides the pebble-backed suite; this
// is the engine-agnostic wiring test.)
func TestIngestInvariantHaltHook(t *testing.T) {
	ctx := context.Background()
	store, syncID := newInvariantTestStore(ctx, t)

	declared := map[string]bool{}
	for _, st := range ingestInvariantHaltStages() {
		declared[st] = true
	}

	var observed []string
	base := IngestInvariantsPolicy{
		ActiveSyncID: syncID,
		SyncType:     connectorstore.SyncTypeFull,
	}
	pol := base
	pol.halt = func(stage string) error {
		require.Truef(t, declared[stage], "pass fired undeclared halt stage %q", stage)
		observed = append(observed, stage)
		return nil
	}
	require.NoError(t, RunIngestInvariants(ctx, store, pol))
	require.Contains(t, observed, "invariants-I5-complete",
		"I5 runs on every engine, so its stage must fire even without the pebble inspection surface")

	// An erroring hook stops the pass with the hook's error.
	pol = base
	pol.halt = func(stage string) error {
		if stage == "invariants-I5-complete" {
			return fmt.Errorf("halt at %s", stage)
		}
		return nil
	}
	err := RunIngestInvariants(ctx, store, pol)
	require.Error(t, err)
	require.Contains(t, err.Error(), "halt at invariants-I5-complete")
}
