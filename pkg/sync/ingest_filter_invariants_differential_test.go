package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Differential tests tying the fresh-ingest filter (ingest_filter.go,
// landed separately on main) to the post-collection ingestion
// invariants (ingest_invariants.go). The two features were built on
// separate branches and each carries its own exemption rules; these
// tests pin the CONTRACT between them rather than either in isolation:
//
//  1. After ingest filtering, the invariant seam finds ZERO
//     never-synced-type danglings from fresh connector pages. The
//     filter is the noise/cost reduction; the seam is the correctness
//     guarantee — if the filter and the invariants disagree about an
//     exemption, this is the test that goes red.
//
//  2. The filter's external-resource PREDICTION (keep a reference
//     because the external phase will copy its type) can miss at the
//     resource level. The seam is the safety net that reports the
//     miss — as a synced-type dangling, since the type row does get
//     copied. Warn-only: the row is kept, the sync succeeds.
//
// Both run the real pipeline end to end (NewSyncer over Pebble), not
// the filter/pass functions in isolation, so ordering (filter →
// collection → external phase → invariant seam) is part of what's
// pinned.

import (
	"path/filepath"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// requireNoUnsyncedTypeDanglingsAtSeam asserts no I7/I8/I9 aggregated
// warning attributed danglings to never-synced types — the consistency
// property the ingest filter is supposed to establish for fresh pages.
func requireNoUnsyncedTypeDanglingsAtSeam(t *testing.T, entries []capturedEntry) {
	t.Helper()
	for i := range entries {
		e := &entries[i]
		if e.level != zapcore.WarnLevel || !contains(e.message, "ingest invariant I") {
			continue
		}
		for _, f := range e.fields {
			if f.Key == "refs_into_unsynced_types" {
				require.Zero(t, f.Integer,
					"invariant seam found never-synced-type danglings after ingest filtering: %q — "+
						"the filter and the invariants disagree about an exemption", e.message)
			}
		}
	}
}

// TestIngestFilterInvariantSeamConsistency pins the plan's consistency
// property: a full sync whose fresh pages reference never-scheduled
// resource types ends with (a) the filter's aggregate warning, (b) NO
// unsynced-type danglings at the invariant seam, and (c) the one
// surviving dangling — an unprocessed external-match carrier the filter
// deliberately keeps — classified by the seam's carrier warning, not
// counted as a violation.
func TestIngestFilterInvariantSeamConsistency(t *testing.T) {
	ctx := t.Context()
	tempDir := t.TempDir()
	c1zPath := filepath.Join(tempDir, "differential.c1z")

	policyRT := v2.ResourceType_builder{Id: "iam_policy", DisplayName: "IAM Policy"}.Build()
	mc := newMockConnector()
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType, policyRT)
	group, goodEntitlement, err := mc.AddGroup(ctx, "g1")
	require.NoError(t, err)
	user, err := mc.AddUser(ctx, "u1")
	require.NoError(t, err)
	disabled := ingestFilterResource("iam_policy", "p1")
	disabledEntitlement := ingestFilterEntitlement(disabled, "iam_policy:p1:assigned")
	mc.entDB[group.GetId().GetResource()] = append(mc.entDB[group.GetId().GetResource()], disabledEntitlement)
	mc.grantDB[group.GetId().GetResource()] = []*v2.Grant{
		ingestFilterGrant("good", goodEntitlement, user),
		// Filtered: entitlement resource type never scheduled.
		ingestFilterGrant("disabled-entitlement", disabledEntitlement, user),
		// Filtered: principal resource type never scheduled, no exemption.
		ingestFilterGrant("disabled-principal", goodEntitlement, disabled),
		// Kept by the filter (external-match carriers own their
		// placeholders); no external c1z is configured, so it reaches the
		// seam unprocessed and dangling. The seam must classify it as a
		// kept carrier — evidence of a config gap — not a violation.
		ingestFilterGrant(
			"match-carrier", goodEntitlement, disabled,
			annotations.New(v2.ExternalResourceMatchID_builder{Id: "external-p1"}.Build())...,
		),
	}

	core, entries := newCaptureCore()
	lctx := ctxzap.ToContext(ctx, zap.New(core))

	syncer, err := NewSyncer(lctx, mc,
		WithC1ZPath(c1zPath),
		WithTmpDir(tempDir),
		WithStorageEngine(c1zstore.EnginePebble),
		WithSyncResourceTypes([]string{"group", "user"}),
	)
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(lctx))
	require.NoError(t, syncer.Close(lctx))

	got := entries()

	// (a) The differential setup actually exercised the filter — without
	// this the consistency assertion below would pass vacuously.
	filterWarn := findEntry(got, zapcore.WarnLevel, "fresh ingest filtered references")
	require.NotNil(t, filterWarn, "the filter's aggregate warning must fire for the dropped references")
	require.GreaterOrEqual(t, fieldInt(t, filterWarn, "grants_dropped"), int64(2))
	require.GreaterOrEqual(t, fieldInt(t, filterWarn, "entitlements_dropped"), int64(1))

	// (b) The seam found zero never-synced-type danglings from fresh
	// pages, and no main referential warning fired at all: everything the
	// filter dropped is gone, and the surviving carrier is exempt.
	requireNoUnsyncedTypeDanglingsAtSeam(t, got)
	require.Nil(t, findEntry(got, zapcore.WarnLevel, "ingest invariant I7: entitlements reference resources"),
		"filtered entitlements must not reach the seam as I7 danglings")
	require.Nil(t, findEntry(got, zapcore.WarnLevel, "ingest invariant I8: grants reference entitlements"),
		"filtered grants must not reach the seam as I8 danglings")
	require.Nil(t, findEntry(got, zapcore.WarnLevel, "ingest invariant I9: grants reference principals"),
		"filtered grants must not reach the seam as I9 danglings; the carrier is exempt")

	// (c) The kept carrier is visible as exactly that.
	carrierWarn := findEntry(got, zapcore.WarnLevel, "kept unprocessed external-match carrier")
	require.NotNil(t, carrierWarn, "the surviving match carrier must be classified as a kept carrier")
	require.EqualValues(t, 1, fieldInt(t, carrierWarn, "match_carriers_kept"))
}

// TestIngestFilterExternalPredictionMissCaughtAtSeam pins the filter's
// one lenient arm: it keeps references into a type the external phase
// WILL copy (type present in the external c1z with a user/group trait)
// — a type-level prediction that can miss at the resource level. The
// external phase copies the type and its resources, but the specific
// principal never existed there, so the reference reaches the seam
// dangling. The seam must report it — attributed as a SYNCED-type
// dangling, because the type row was copied — while the sync still
// succeeds and the row is kept (warn mode).
func TestIngestFilterExternalPredictionMissCaughtAtSeam(t *testing.T) {
	ctx := t.Context()
	tempDir := t.TempDir()

	// External c1z: the "user" type (USER trait) and one real user.
	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)
	_, err := externalMc.AddUser(ctx, "ext-user-1")
	require.NoError(t, err)
	externalC1zPath := filepath.Join(tempDir, "external.c1z")
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, externalSyncer.Sync(ctx))
	require.NoError(t, externalSyncer.Close(ctx))

	// Internal connector: lists only "group"; one grant references a
	// "user" principal that exists NOWHERE — not internally (the type is
	// never listed) and not in the external c1z either.
	internalMc := newMockConnector()
	internalMc.rtDB = append(internalMc.rtDB, groupResourceType)
	internalGroup, _, err := internalMc.AddGroup(ctx, "g1")
	require.NoError(t, err)
	missingPrincipal := v2.ResourceId_builder{ResourceType: "user", Resource: "missing-user"}.Build()
	internalMc.grantDB[internalGroup.GetId().GetResource()] = []*v2.Grant{
		gt.NewGrant(internalGroup, "member", missingPrincipal),
	}

	core, entries := newCaptureCore()
	lctx := ctxzap.ToContext(ctx, zap.New(core))

	internalC1zPath := filepath.Join(tempDir, "internal.c1z")
	internalSyncer, err := NewSyncer(lctx, internalMc,
		WithC1ZPath(internalC1zPath),
		WithTmpDir(tempDir),
		WithStorageEngine(c1zstore.EnginePebble),
		WithExternalResourceC1ZPath(externalC1zPath),
	)
	require.NoError(t, err)
	require.NoError(t, internalSyncer.Sync(lctx), "a prediction miss is warn-only; it must not fail the sync")
	require.NoError(t, internalSyncer.Close(lctx))

	got := entries()

	// The filter kept the reference (prediction), so nothing was dropped.
	require.Nil(t, findEntry(got, zapcore.WarnLevel, "fresh ingest filtered references"),
		"the will-be-copied prediction must keep the reference at ingest time")

	// The prediction missed at the resource level and the seam caught it:
	// one dangling principal, attributed as synced-type (the external
	// phase did copy the "user" type row).
	seamWarn := findEntry(got, zapcore.WarnLevel, "ingest invariant I9: grants reference principals")
	require.NotNil(t, seamWarn, "the seam is the safety net for filter prediction misses and must warn")
	require.EqualValues(t, 1, fieldInt(t, seamWarn, "dangling_principals"))
	require.EqualValues(t, 1, fieldInt(t, seamWarn, "refs_under_synced_types"),
		"the copied type row makes this the synced-type flavor")
	require.EqualValues(t, 0, fieldInt(t, seamWarn, "refs_into_unsynced_types"))

	// Warn mode keeps the row: the grant survives into the sealed store,
	// and the external phase really did copy the type + real users.
	store, err := dotc1z.NewStore(ctx, internalC1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()

	rtResp, err := store.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	rtIDs := make([]string, 0, len(rtResp.GetList()))
	for _, rt := range rtResp.GetList() {
		rtIDs = append(rtIDs, rt.GetId())
	}
	require.Contains(t, rtIDs, "user", "the external phase must have copied the predicted type")

	grantsResp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, grantsResp.GetList(), 1, "warn mode keeps the dangling row (drops arrive with a later PR)")
	require.Equal(t, "missing-user", grantsResp.GetList()[0].GetPrincipal().GetId().GetResource())
}
