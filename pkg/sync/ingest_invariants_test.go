package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// TestPushChildResourceActionsDedupesPerSync pins the per-sync child
// scheduling dedupe: a parent discovered by several ingestion paths in
// one sync (replay copy + delta overlay re-emission) must schedule each
// (childType, parent) pair exactly once — the duplicate would double the
// child-listing connector work on every warm overlay. The dedupe rides
// the I4 evidence set (childScheduleSet.recordIfNew), so the evidence
// stays complete for every pair regardless of which discovery scheduled.
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

	// Second discovery of the same parent (e.g. the overlay re-emission
	// after a replay copy already scheduled it): no new actions.
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
// regardless of ingestion path (stream, replay, or whatever comes next).
// Adding a new side-effect-implying annotation without extending
// sideEffectAnnotationCoverage — and the invariant behind it — fails
// here instead of in review round eleven.
func TestIngestInvariantAnnotationCoverage(t *testing.T) {
	sideEffectAnnotations := []proto.Message{
		&v2.GrantExpandable{},
		&v2.ExternalResourceMatch{},
		&v2.ExternalResourceMatchAll{},
		&v2.ExternalResourceMatchID{},
		&v2.InsertResourceGrants{},
		&v2.ChildResourceType{},
		&v2.EntitlementExclusionGroup{},
		&v2.TypeScopedEntitlements{},
		&v2.TypeScopedGrants{},
	}
	for _, msg := range sideEffectAnnotations {
		name := string(msg.ProtoReflect().Descriptor().FullName())
		require.Contains(t, sideEffectAnnotationCoverage, name,
			"side-effect-implying annotation %s has no registered ingestion invariant; "+
				"see docs/tasks/source-cache-ingestion-invariants.md", name)
	}
	require.Len(t, sideEffectAnnotationCoverage, len(sideEffectAnnotations),
		"coverage map and the enumerated annotation set drifted; update both together")
}
