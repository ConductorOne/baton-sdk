package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// etagObservingMockConnector emits an ETag on every ListGrants
// response and records the ETag the syncer attaches to the resource
// it sends back on the next sync. It remains the fixture for the
// previous-sync plumbing tests below (soft-fail contract, replay
// eligibility gates); the replay behavior itself is verified by the
// source-cache chaos suites.
type etagObservingMockConnector struct {
	*mockConnector
	etagValue     string
	entitlementID string

	mu                  sync.Mutex
	etagsReceivedByCall []string
}

func newEtagObservingMockConnector(etagValue string) *etagObservingMockConnector {
	mc := &etagObservingMockConnector{
		mockConnector: newMockConnector(),
		etagValue:     etagValue,
	}
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)
	return mc
}

func (mc *etagObservingMockConnector) WithData(resource *v2.Resource, ent *v2.Entitlement, grants ...*v2.Grant) {
	mc.AddResource(context.Background(), resource)
	mc.entitlementID = ent.GetId()
	mc.entDB[resource.GetId().GetResource()] = []*v2.Entitlement{ent}
	mc.grantDB[resource.GetId().GetResource()] = grants
}

func (mc *etagObservingMockConnector) ListGrants(
	ctx context.Context,
	in *v2.GrantsServiceListGrantsRequest,
	_ ...grpc.CallOption,
) (*v2.GrantsServiceListGrantsResponse, error) {
	// Record the ETag the syncer attached to the resource (if any)
	// on this ListGrants call. The first call (sync 1) sees no
	// ETag because no previous sync exists; subsequent calls
	// MUST see the ETag persisted by the previous sync.
	var incomingETag string
	if res := in.GetResource(); res != nil {
		annos := annotations.Annotations(res.GetAnnotations())
		et := &v2.ETag{}
		if ok, _ := annos.Pick(et); ok {
			incomingETag = et.GetValue()
		}
	}
	mc.mu.Lock()
	mc.etagsReceivedByCall = append(mc.etagsReceivedByCall, incomingETag)
	mc.mu.Unlock()

	var key string
	if r := in.GetResource(); r != nil {
		key = r.GetId().GetResource()
	}
	return v2.GrantsServiceListGrantsResponse_builder{
		List: mc.grantDB[key],
		Annotations: annotations.New(&v2.ETag{
			Value:         mc.etagValue,
			EntitlementId: mc.entitlementID,
		}),
	}.Build(), nil
}

// The two skipped ETag replay tests that lived here
// (TestPebble_EtagReplay_SendsPreviousEtagOnSecondSync and
// TestPebble_EtagReplay_CarriesPreviousSyncsGrantsForward) were REMOVED in
// Phase 6b, replaced by the source-cache chaos suites per the frozen plan
// (docs/verification/sync-replay-6b/plan.md, closure rules):
//
//   - "the previous sync's validator is re-presented to the connector on
//     the next sync" is subsumed by the lookup consult contract — the gate
//     matrix (chaos_source_cache_gate_test.go) and the generational suite
//     (chaos_source_cache_generational_test.go, etag-style scope) assert
//     the exact validator every consult observes across generations;
//   - "previous-sync rows are carried forward on a validator match" is
//     subsumed by the collection-semantics suite
//     (chaos_source_cache_collection_test.go) and the generational
//     steady-state suite, which compare replayed content against
//     independent cold baselines by full-proto fingerprint.
//
// The generalized source-cache annotations replace the ETag/ETagMatch
// protobufs as the replay contract; the old protobufs remain for wire
// compatibility only.

// TestOptionalPreviousSyncC1ZPath_SoftFails pins the best-effort
// contract of WithOptionalPreviousSyncC1ZPath: a missing or corrupt
// previous-sync c1z (the service-mode spare is a cache the handler
// maintains automatically) must degrade to a sync without ETag replay,
// never fail NewSyncer or the sync. The strict WithPreviousSyncC1ZPath
// keeps surfacing unusable-file failures.
func TestOptionalPreviousSyncC1ZPath_SoftFails(t *testing.T) {
	ctx := t.Context()
	ctx, err := logging.Init(ctx)
	require.NoError(t, err)

	tempDir := t.TempDir()

	group, err := rs.NewGroupResource("g1", groupResourceType, "g1", nil)
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(group, "member", et.WithGrantableTo(groupResourceType, userResourceType))
	ent.SetSlug("member")
	user, err := rs.NewUserResource("u1", userResourceType, "u1", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	grant := gt.NewGrant(group, "member", user)

	corruptPath := filepath.Join(tempDir, "corrupt-prev.c1z")
	require.NoError(t, os.WriteFile(corruptPath, []byte("not a c1z"), 0o600))

	for name, path := range map[string]string{
		"missing": filepath.Join(tempDir, "does-not-exist.c1z"),
		"corrupt": corruptPath,
	} {
		t.Run(name, func(t *testing.T) {
			mc := newEtagObservingMockConnector("etag-v1")
			mc.WithData(group, ent, grant)
			store, err := dotc1z.NewStore(ctx, filepath.Join(t.TempDir(), "out.c1z"),
				dotc1z.WithEngine(c1zstore.EnginePebble),
				dotc1z.WithTmpDir(tempDir),
			)
			require.NoError(t, err)
			syncer, err := NewSyncer(ctx, mc,
				WithConnectorStore(store),
				WithTmpDir(tempDir),
				WithOptionalPreviousSyncC1ZPath(path),
			)
			require.NoError(t, err, "optional previous-sync c1z must not fail NewSyncer")
			require.NoError(t, syncer.Sync(ctx), "sync must proceed without replay")
			require.NoError(t, syncer.Close(ctx))
		})
	}

	// Strict variant: the same corrupt file must fail loudly.
	mc := newEtagObservingMockConnector("etag-v1")
	mc.WithData(group, ent, grant)
	store, err := dotc1z.NewStore(ctx, filepath.Join(t.TempDir(), "strict.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	_, err = NewSyncer(ctx, mc,
		WithConnectorStore(store),
		WithTmpDir(tempDir),
		WithPreviousSyncC1ZPath(corruptPath),
	)
	require.Error(t, err, "explicit previous-sync c1z must surface unusable-file failures")
	require.NoError(t, store.Close(ctx))
}

// buildSyncedPreviousArtifact runs a REAL full sync (group + member
// entitlement + one grant to an existing user) into a fresh Pebble c1z and
// returns its path. Unlike a bare StartNewSync/EndSync artifact, the result
// carries everything the consume gates demand of a replay source: a finished
// FULL run, ingest-quality stats with no drops (G4), and the save-time
// materialization witness in the envelope manifest (G5).
func buildSyncedPreviousArtifact(t *testing.T, extra ...SyncOpt) string {
	t.Helper()
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "previous.c1z")

	group, err := rs.NewGroupResource("g1", groupResourceType, "g1", nil)
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(group, "member", et.WithGrantableTo(groupResourceType, userResourceType))
	ent.SetSlug("member")
	user, err := rs.NewUserResource("u1", userResourceType, "u1", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	grant := gt.NewGrant(group, "member", user)

	mc := newEtagObservingMockConnector("etag-v1")
	mc.WithData(group, ent, grant)
	// The grant's principal must resolve to a synced resource so an
	// unrestricted sync ends with clean ingest quality.
	mc.AddResource(ctx, user)

	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	opts := append([]SyncOpt{WithConnectorStore(store), WithTmpDir(tempDir)}, extra...)
	syncer, err := NewSyncer(ctx, mc, opts...)
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))
	require.NoError(t, syncer.Close(ctx))
	return path
}

// buildBarePreviousArtifact writes an artifact with StartNewSync/EndSync
// only — no syncer, so no ingest-quality stats. Saved by the current SDK it
// still carries the envelope witness, which makes it the isolation case for
// G4's absent-stats fail-closed rule.
func buildBarePreviousArtifact(t *testing.T, engine c1zstore.Engine, syncType connectorstore.SyncType) string {
	t.Helper()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "previous.c1z")
	previous, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
	require.NoError(t, err)
	_, err = previous.StartNewSync(ctx, syncType, "")
	require.NoError(t, err)
	require.NoError(t, previous.EndSync(ctx))
	require.NoError(t, previous.Close(ctx))
	return path
}

// TestPreviousSyncC1ZPathEnforcesReplayEligibility pins the NewSyncer half
// of the source-cache gate ladder (docs/verification/sync-replay-6b/plan.md
// B2, gates G1–G5): which previous-sync artifacts are accepted as warm
// replay sources (previousSyncReader != nil) and which degrade to a cold
// sync. The install-time gates (G6 capability, G7 compat byte-match) run at
// Sync, not NewSyncer, so reader presence here is independent of the
// connector's declared capability.
func TestPreviousSyncC1ZPathEnforcesReplayEligibility(t *testing.T) {
	for _, tc := range []struct {
		name       string
		build      func(t *testing.T) string
		wantReader bool
	}{
		{
			// Every gate passes: Pebble artifact (G2) holding a finished,
			// non-compacted FULL sync (G3) produced by a real syncer run, so
			// ingest-quality stats exist with no drop flags (G4) and the
			// envelope carries this SDK's materialization witness (G5).
			name:       "pebble-full-synced",
			build:      func(t *testing.T) string { return buildSyncedPreviousArtifact(t) },
			wantReader: true,
		},
		{
			// G4 absent-stats conservatism: a finished FULL Pebble sync
			// written without the syncer has no ingest-quality stats.
			// Unknown quality must fail closed — pre-quality artifacts can
			// never seed a warm sync.
			name: "pebble-full-no-quality-stats",
			build: func(t *testing.T) string {
				return buildBarePreviousArtifact(t, c1zstore.EnginePebble, connectorstore.SyncTypeFull)
			},
		},
		{
			// G3: partial syncs never serve as replay sources.
			name: "pebble-partial",
			build: func(t *testing.T) string {
				return buildBarePreviousArtifact(t, c1zstore.EnginePebble, connectorstore.SyncTypePartial)
			},
		},
		{
			// G3: a folded (compacted) artifact is rejected even when its
			// quality stats and witness are intact — fold retains stale
			// source-scope indexes, so compacted must dominate.
			name: "pebble-compacted-full",
			build: func(t *testing.T) string {
				path := buildSyncedPreviousArtifact(t)
				ctx := t.Context()
				store, err := dotc1z.NewStore(ctx, path,
					dotc1z.WithEngine(c1zstore.EnginePebble),
					dotc1z.WithTmpDir(t.TempDir()),
				)
				require.NoError(t, err)
				latest, ok := store.(interface {
					LatestFinishedSyncID(ctx context.Context, syncType connectorstore.SyncType) (string, error)
				})
				require.True(t, ok)
				syncID, err := latest.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
				require.NoError(t, err)
				eng, ok := enginepkg.AsEngine(store)
				require.True(t, ok)
				run, err := eng.GetSyncRunRecord(ctx, syncID)
				require.NoError(t, err)
				run.SetCompacted(true)
				require.NoError(t, eng.PutSyncRunRecord(ctx, run))
				require.True(t, enginepkg.MarkStoreDirty(store))
				require.NoError(t, store.Close(ctx))
				return path
			},
		},
		{
			// G2: SQLite is conversion-only (RFC 0010) — always cold.
			name: "sqlite-full",
			build: func(t *testing.T) string {
				return buildBarePreviousArtifact(t, c1zstore.EngineSQLite, connectorstore.SyncTypeFull)
			},
		},
		{
			// G4 replay-blocked: restricting the sync to the group type
			// leaves the grant's user principal type unscheduled, so the
			// ingest filter drops the grant and flags the run
			// source_cache_replay_blocked. A lossy sync must never seed the
			// next generation, however healthy it looks structurally.
			name: "pebble-quality-blocked",
			build: func(t *testing.T) string {
				return buildSyncedPreviousArtifact(t, WithSyncResourceTypes([]string{"group"}))
			},
		},
		{
			// G5 — CO-017 old-fold shape (plan B8). An older SDK's fold
			// byte-copies the payload intact (source-cache manifest entries,
			// indexes, compat record survive; compacted stays false on the
			// surviving run) but rebuilds the ENVELOPE manifest from its own
			// descriptors, so it cannot carry the materialization witness.
			// Simulate exactly that: extract the payload of a fully eligible
			// artifact, clear the witness, and re-wrap the same payload. The
			// result passes G1–G4 and must be rejected by the fence alone.
			name: "pebble-fence-stripped-old-fold-shape",
			build: func(t *testing.T) string {
				path := buildSyncedPreviousArtifact(t)
				src, err := os.Open(path)
				require.NoError(t, err)
				payloadDir := filepath.Join(t.TempDir(), "payload")
				require.NoError(t, os.MkdirAll(payloadDir, 0o755))
				manifest, _, err := formatv3.ExtractEnvelopePayload(src, payloadDir)
				require.NoError(t, err)
				require.NoError(t, src.Close())
				// Sanity: current-SDK saves stamp the witness; without this
				// the strip below would be a no-op and the case vacuous.
				require.Equal(t, sourcecache.MaterializationPolicyGeneration, manifest.GetSdkMaterializationGeneration())
				manifest.SetSdkMaterializationGeneration("")
				stripped := filepath.Join(t.TempDir(), "old-fold-shape.c1z")
				out, err := os.Create(stripped)
				require.NoError(t, err)
				require.NoError(t, formatv3.WriteEnvelope(out, manifest, payloadDir))
				require.NoError(t, out.Close())
				return stripped
			},
		},
		{
			// G5 — witness MISMATCH (not absence): an artifact whose witness
			// names a different materialization generation (a future SDK's
			// save, or any generation bump) must be rejected the same way.
			// The fence is an exact-match check, never "witness present".
			name: "pebble-fence-foreign-witness",
			build: func(t *testing.T) string {
				path := buildSyncedPreviousArtifact(t)
				src, err := os.Open(path)
				require.NoError(t, err)
				payloadDir := filepath.Join(t.TempDir(), "payload")
				require.NoError(t, os.MkdirAll(payloadDir, 0o755))
				manifest, _, err := formatv3.ExtractEnvelopePayload(src, payloadDir)
				require.NoError(t, err)
				require.NoError(t, src.Close())
				manifest.SetSdkMaterializationGeneration(
					sourcecache.MaterializationPolicyGeneration + "-future")
				rewrapped := filepath.Join(t.TempDir(), "foreign-witness.c1z")
				out, err := os.Create(rewrapped)
				require.NoError(t, err)
				require.NoError(t, formatv3.WriteEnvelope(out, manifest, payloadDir))
				require.NoError(t, out.Close())
				return rewrapped
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			previousPath := tc.build(t)

			current, err := dotc1z.NewStore(ctx, filepath.Join(t.TempDir(), "current.c1z"), dotc1z.WithEngine(c1zstore.EnginePebble))
			require.NoError(t, err)
			connector := newEtagObservingMockConnector("etag-v1")
			got, err := NewSyncer(
				ctx,
				connector,
				WithConnectorStore(current),
				WithTmpDir(t.TempDir()),
				WithPreviousSyncC1ZPath(previousPath),
			)
			require.NoError(t, err)
			concrete := got.(*syncer)
			require.Equal(t, tc.wantReader, concrete.previousSyncReader != nil)
			require.NoError(t, got.Close(ctx))
		})
	}
}
