package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Mixed-population pin for I9's exemption: exemptions must be judged per
// GRANT, not per principal. One dangling principal carrying BOTH an
// unprocessed external-match carrier and a plain grant must keep exactly
// the carrier and drop exactly the plain grant in default mode, and must
// hard-fail (dropping nothing) in fail-fast mode. This is the fixture
// shape that homogeneous exemption tests structurally cannot check — the
// escape route the per-entitlement-resource I8 exemption granularity bug
// took.

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/logging"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

func TestIngestInvariantI9MixedPrincipalPopulation(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	repo, err := rs.NewResource("Repo r1", equivRepoRT, "r1")
	require.NoError(t, err)
	adminEnt := et.NewAssignmentEntitlement(repo, "admin")
	viewerEnt := et.NewAssignmentEntitlement(repo, "viewer")
	ghost := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "ghost"}.Build(),
	}.Build()

	// The SAME dangling principal carries one match carrier and one
	// plain grant.
	carrier := grant.NewGrant(repo, "admin", ghost.GetId())
	annos := annotations.Annotations(carrier.GetAnnotations())
	annos.Update(v2.ExternalResourceMatch_builder{Key: "email", Value: "ghost@example.com"}.Build())
	carrier.SetAnnotations(annos)
	plain := grant.NewGrant(repo, "viewer", ghost.GetId())

	cur := newRepairTestStore(ctx, t, filepath.Join(tmpDir, "cur.c1z"), tmpDir)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, cur.PutResourceTypes(ctx, equivRepoRT))
	require.NoError(t, cur.PutResources(ctx, repo))
	require.NoError(t, cur.PutEntitlements(ctx, adminEnt, viewerEnt))
	require.NoError(t, cur.PutGrants(ctx, carrier, plain))

	s := &syncer{store: cur, syncType: connectorstore.SyncTypeFull}

	requireGrant := func(id string, wantPresent bool, msg string) {
		t.Helper()
		_, err := cur.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
			GrantId: id,
		}.Build())
		if wantPresent {
			require.NoError(t, err, msg)
		} else {
			require.Error(t, err, msg)
		}
	}

	// Fail-fast: a mixed principal is NOT the exempt shape — it must be
	// named as a violation, and nothing may be dropped.
	s.failFastInvariants = true
	err = s.checkGrantPrincipalReferences(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingest invariant I9")
	requireGrant(carrier.GetId(), true, "fail-fast must not drop the carrier")
	requireGrant(plain.GetId(), true, "fail-fast must not drop the plain grant")

	// Default mode: per-grant judgment — the carrier survives, the plain
	// grant is dropped.
	s.failFastInvariants = false
	require.NoError(t, s.checkGrantPrincipalReferences(ctx))
	requireGrant(carrier.GetId(), true, "the match carrier must be kept (config-gap evidence)")
	requireGrant(plain.GetId(), false, "the plain grant under the dangling principal must be dropped")

	// After the drop, the principal's surviving population is
	// carriers-only — the exempt shape — so BOTH modes now pass without
	// further mutation (idempotence of the mixed case).
	require.NoError(t, s.checkGrantPrincipalReferences(ctx))
	s.failFastInvariants = true
	require.NoError(t, s.checkGrantPrincipalReferences(ctx))
	requireGrant(carrier.GetId(), true, "the carrier must survive repeated passes")

	require.NoError(t, cur.Close(ctx))
}
