package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Halt-stage sweep for the ingestion-invariant pass: every declared
// stage (ingestInvariantHaltStages) fires over a store with the full
// inspection surface, an error at ANY stage stops the pass at exactly
// that boundary, and a clean re-run over the same state converges —
// the pass is read-only apart from the orphan heal and the deferred
// index build, both idempotent, so resume-after-halt must always
// succeed. Iterating the declared list IS the coverage contract: a new
// table row's stage is swept here by construction.

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

func TestIngestInvariantHaltStageSweep(t *testing.T) {
	ctx := context.Background()
	store, pass := newPebbleInvariantStore(ctx, t)

	// A small healthy keyspace so every invariant runs its body (no
	// verdicts fire; the sweep is about the seams).
	repo, err := rs.NewResource("Repo r1", invariantRepoRT, "r1")
	require.NoError(t, err)
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	alice, err := rs.NewResource("alice", userRT, "alice")
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(repo, "admin", et.WithExclusionGroup("role"))
	require.NoError(t, store.PutResourceTypes(ctx, invariantRepoRT, userRT))
	require.NoError(t, store.PutResources(ctx, repo, alice))
	require.NoError(t, store.PutEntitlements(ctx, ent))
	require.NoError(t, store.PutGrants(ctx, grant.NewGrant(repo, "admin", alice.GetId())))

	declared := ingestInvariantHaltStages()
	require.NotEmpty(t, declared)

	// The pass-level stages are everything except the syncer-seam stage,
	// which fires in Sync() after the pass returns (pinned by
	// TestSyncFailsOnStoredExclusionGroupConflict's seam and the
	// end-of-list assertion in the verdict-table meta-test).
	passStages := declared[:len(declared)-1]
	require.Equal(t, haltStageInvariantsComplete, declared[len(declared)-1])

	// Clean run: every pass-level stage fires, in declared order.
	var fired []string
	pass.p.halt = func(stage string) error {
		fired = append(fired, stage)
		return nil
	}
	require.NoError(t, RunIngestInvariants(ctx, store, *pass.p))
	require.Equal(t, passStages, fired,
		"every declared pass-level stage must fire, in order, over a store with the full inspection surface")

	// Error sweep: failing at stage k stops the pass at exactly that
	// boundary (no later stage fires), and a clean re-run over the same
	// state converges.
	for _, haltAt := range passStages {
		var seen []string
		pol := *pass.p
		pol.halt = func(stage string) error {
			seen = append(seen, stage)
			if stage == haltAt {
				return fmt.Errorf("injected halt at %s", stage)
			}
			return nil
		}
		err := RunIngestInvariants(ctx, store, pol)
		require.Errorf(t, err, "halt at %s must fail the pass", haltAt)
		require.Contains(t, err.Error(), "injected halt at "+haltAt)
		require.Equal(t, haltAt, seen[len(seen)-1],
			"no stage may fire after the failing one")

		// Resume: the same pass over the same state, clean hook.
		pol.halt = nil
		require.NoErrorf(t, RunIngestInvariants(ctx, store, pol), "re-run after halt at %s must converge", haltAt)
	}
}
