package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Pins the dangling-type probe's error handling: a resource-type read
// failure during the I7/I8/I9 sweep must FAIL the check, never
// masquerade as "type never synced" — that verdict picks the DROP arm,
// so an IO error or canceled context would seal a sanitized artifact
// where the replay policy demands a loud failure.

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/logging"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// typeProbeFaultStore wraps a real store and fails every GetResourceType
// with a chosen non-NotFound error, leaving the rest of the invariant
// surface (HasResourceRecord, the sweep iterators, the delete paths)
// intact — the exact seam the danglingTypeProbe reads.
type typeProbeFaultStore struct {
	c1zstore.Store
	dotc1z.IngestInvariantStore
	err error
}

func (s typeProbeFaultStore) GetResourceType(
	context.Context, *reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest,
) (*reader_v2.ResourceTypesReaderServiceGetResourceTypeResponse, error) {
	return nil, s.err
}

func TestDanglingTypeProbeErrorFailsInsteadOfDropping(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(repo, "admin")

	// A dangling entitlement (no resource row, no type row): with a
	// healthy probe this is a disabled-type gap and gets DROPPED. With
	// the probe FAILING, the sweep must fail — before the fix, the
	// probe's error was collapsed into "type never synced" and the row
	// was silently dropped.
	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, cur.PutEntitlements(ctx, ent))

	inv, ok := cur.(dotc1z.IngestInvariantStore)
	require.True(t, ok)
	probeFault := errors.New("injected: resource-type read failed")
	s := &syncer{
		store:    typeProbeFaultStore{Store: cur, IngestInvariantStore: inv, err: probeFault},
		syncType: connectorstore.SyncTypeFull,
	}

	err = s.checkEntitlementResourceReferences(ctx)
	require.Error(t, err, "a probe read failure must fail the sweep, not pick a drop verdict")
	require.ErrorIs(t, err, probeFault)

	_, err = cur.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: ent.GetId(),
	}.Build())
	require.NoError(t, err, "no row may be dropped on a probe failure")

	// Same seam for I8: a grant whose entitlement has no row.
	err = s.checkGrantEntitlementReferences(ctx)
	require.NoError(t, err, "no dangling grants planted; I8 must pass without probing")

	require.NoError(t, cur.Close(ctx))
}

// TestExclusionGroupVerdictRidesWarmColdLadder pins I5's membership in
// the warm/cold ErrReplayIntegrity ladder: replayed stale entitlement
// rows can manufacture warm-only exclusion-group conflicts (a stale
// group member beside a fresh one), so a warm verdict must carry the
// sentinel (runners discard + retry cold) while a cold verdict is
// attributed plainly to the connector — the same classification as
// I3/I6/enabled I7–I9.
func TestExclusionGroupVerdictRidesWarmColdLadder(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	group := v2.EntitlementExclusionGroup_builder{ExclusionGroupId: "eg-1", IsDefault: true}.Build()
	res := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	// Two defaults in one group: an I5 violation however the rows arrived.
	ent1 := v2.Entitlement_builder{Id: "group:g1:member", Resource: res, Annotations: annotations.New(group)}.Build()
	ent2 := v2.Entitlement_builder{Id: "group:g1:owner", Resource: res, Annotations: annotations.New(group)}.Build()

	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, cur.PutEntitlements(ctx, ent1, ent2))

	s := &syncer{store: cur, syncType: connectorstore.SyncTypeFull}

	// Cold: plain connector attribution, no sentinel.
	err = s.validateStoredExclusionGroups(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I5")
	require.Contains(t, err.Error(), "multiple default entitlements")
	require.NotErrorIs(t, err, ErrReplayIntegrity, "a cold verdict is terminal, not a replay fault")

	// Warm: the same evidence may be replay-carried staleness.
	s.sourceCache.prev = struct{ dotc1z.SourceCacheStore }{}
	err = s.validateStoredExclusionGroups(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrReplayIntegrity, "a warm verdict must ride the discard-and-retry-cold ladder")

	require.NoError(t, cur.Close(ctx))
}

// TestDanglingAttributionBuckets pins the drop-arm attribution wording:
// the drop arm only ever sees config gaps and compaction-manufactured
// gaps (enabled-type danglings in a normal sync FAIL before any drop),
// so no drop warning may blame connector magic-id construction.
func TestDanglingAttributionBuckets(t *testing.T) {
	require.Contains(t, danglingAttribution(3, 0), "config gap")
	require.Contains(t, danglingAttribution(0, 2), "compaction")
	require.Contains(t, danglingAttribution(1, 1), "mixed")
	for _, s := range []string{danglingAttribution(3, 0), danglingAttribution(0, 2), danglingAttribution(1, 1)} {
		require.NotContains(t, s, "magic-id",
			"drop-arm attribution must not blame connector id construction — those cases fail the sync instead")
	}
}
