package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"errors"
	"fmt"
	native_sync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	"github.com/conductorone/baton-sdk/pkg/types"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// fakeSourceCacheStore satisfies dotc1z.SourceCacheStore with configurable
// failures, for the B7 verdict-taxonomy enumeration.
type fakeSourceCacheStore struct {
	replayErr      error
	deleteRowsErr  error
	deleteScopeErr error
	putEntryErr    error
}

func (f *fakeSourceCacheStore) LookupSourceCacheEntry(context.Context, sourcecache.RowKind, string) (sourcecache.Entry, bool, error) {
	return sourcecache.Entry{}, false, nil
}

func (f *fakeSourceCacheStore) PutSourceCacheEntry(context.Context, sourcecache.RowKind, string, string) error {
	return f.putEntryErr
}

func (f *fakeSourceCacheStore) ReplaySourceCache(context.Context, connectorstore.Reader, sourcecache.RowKind, string) (dotc1z.SourceCacheReplayResult, error) {
	return dotc1z.SourceCacheReplayResult{}, f.replayErr
}

func (f *fakeSourceCacheStore) DeleteSourceCacheRows(context.Context, sourcecache.RowKind, string, []string) error {
	return f.deleteRowsErr
}

func (f *fakeSourceCacheStore) DeleteSourceCacheRowsInScope(context.Context, sourcecache.RowKind, string, []string) (int64, error) {
	return 0, f.deleteScopeErr
}

func (f *fakeSourceCacheStore) DeleteSourceCacheGrantsByIDInScope(context.Context, string, []string) (int64, error) {
	return 0, nil
}

func (f *fakeSourceCacheStore) PutSourceCacheCompat(context.Context, sourcecache.CompatKey) error {
	return nil
}

func (f *fakeSourceCacheStore) GetSourceCacheCompat(context.Context) (sourcecache.CompatKey, bool, error) {
	return sourcecache.CompatKey{}, false, nil
}

// stubPreviousReader is a non-nil connectorstore.Reader serving as the
// replay base. Its Reader methods are never reached (the fake store's
// ReplaySourceCache ignores it); LookupSourceCacheEntry serves the
// hit-binding check in beforeUpserts.
type stubPreviousReader struct {
	connectorstore.Reader
	entry    sourcecache.Entry
	found    bool
	entryErr error
}

func (r stubPreviousReader) LookupSourceCacheEntry(context.Context, sourcecache.RowKind, string) (sourcecache.Entry, bool, error) {
	return r.entry, r.found, r.entryErr
}

// bareStubPreviousReader is a previous reader WITHOUT the source-cache
// entry surface, for the binding check's fail-closed cell.
type bareStubPreviousReader struct{ connectorstore.Reader }

// TestSourceCacheReplayVerdictTaxonomy enumerates every orchestration
// replay failure path named in plan B7 and pins its frozen warm/cold
// classification, surfaced via errors.Is/errors.As (oracle OR4, criterion
// R9). Each cell drives the real page-ops pipeline against a fake store.
func TestSourceCacheReplayVerdictTaxonomy(t *testing.T) {
	const scope = "grants:team-1"
	rw := v2.SourceCacheCapability_builder{
		Mode:            v2.SourceCacheCapability_MODE_READ_WRITE,
		CacheGeneration: "gen-1",
	}.Build()

	newSyncerFixture := func(store dotc1z.SourceCacheStore) *syncer {
		s := &syncer{
			sourceCacheCapability: rw,
			sourceCacheStore:      store,
			state:                 newState(),
			syncType:              connectorstore.SyncTypeFull,
			// The replay cells exercise post-gate behavior: this attempt
			// installed and delivered a warm lookup. The not-warm gate has
			// its own dedicated cell below.
			sourceCacheWarm: true,
		}
		if store != nil {
			// The base publishes the validator the hit cells record, so
			// the hit-binding check passes and cells reach their target
			// failure site.
			s.previousSyncReader = stubPreviousReader{
				entry: sourcecache.Entry{CacheValidator: "v-hit"},
				found: true,
			}
		}
		return s
	}

	corruptAnno := func(typeURL string) annotations.Annotations {
		return annotations.Annotations{{TypeUrl: "type.googleapis.com/" + typeURL, Value: []byte{0xff, 0xff}}}
	}

	requireVerdict := func(t *testing.T, err error, want ReplayVerdict, kind sourcecache.RowKind) {
		t.Helper()
		require.Error(t, err)
		// The identity contract 6c's runner depends on: reachable through
		// arbitrary wrapping via errors.Is / errors.As.
		wrapped := fmt.Errorf("sync failed: %w", err)
		require.ErrorIs(t, wrapped, ErrReplayIntegrity)
		var rie *ReplayIntegrityError
		require.ErrorAs(t, wrapped, &rie)
		require.Equal(t, want, rie.Verdict)
		require.Equal(t, kind, rie.RowKind)
		require.NotNil(t, rie.Unwrap(), "the underlying cause must stay reachable")
	}

	ctx := context.Background()
	kind := sourcecache.RowKindGrants

	t.Run("parse-cold-paths", func(t *testing.T) {
		s := newSyncerFixture(&fakeSourceCacheStore{})
		for name, annos := range map[string]annotations.Annotations{
			"unparsable-record": corruptAnno("c1.connector.v2.SourceCacheRecord"),
			"unparsable-replay": corruptAnno("c1.connector.v2.SourceCacheReplay"),
			"invalid-replay-scope-key": annotations.New(
				v2.SourceCacheReplay_builder{ScopeKey: ""}.Build()),
			"two-scopes-on-one-page": annotations.New(
				v2.SourceCacheRecord_builder{ScopeKey: "grants:a", CacheValidator: "v"}.Build(),
				v2.SourceCacheReplay_builder{ScopeKey: "grants:b", CacheValidator: "v"}.Build()),
		} {
			t.Run(name, func(t *testing.T) {
				_, err := s.sourceCachePageOps(ctx, kind, annos, 0)
				requireVerdict(t, err, ReplayVerdictCold, kind)
			})
		}
		t.Run("invalid-record-scope-key", func(t *testing.T) {
			longKey := make([]byte, 300)
			for i := range longKey {
				longKey[i] = 'x'
			}
			_, err := s.sourceCachePageOps(ctx, kind,
				annotations.New(v2.SourceCacheRecord_builder{ScopeKey: string(longKey)}.Build()), 0)
			requireVerdict(t, err, ReplayVerdictCold, kind)
		})
		t.Run("principal-tombstones-on-entitlements", func(t *testing.T) {
			_, err := s.sourceCachePageOps(ctx, sourcecache.RowKindEntitlements,
				annotations.New(v2.SourceCacheRecord_builder{
					ScopeKey:            "ents:a",
					DeletedPrincipalIds: []string{"u1"},
				}.Build()), 0)
			requireVerdict(t, err, ReplayVerdictCold, sourcecache.RowKindEntitlements)
		})
		t.Run("replay-on-store-without-surface", func(t *testing.T) {
			bare := newSyncerFixture(nil)
			_, err := bare.sourceCachePageOps(ctx, kind,
				annotations.New(v2.SourceCacheReplay_builder{ScopeKey: scope}.Build()), 0)
			requireVerdict(t, err, ReplayVerdictCold, kind)
		})
		t.Run("duplicate-same-type-annotations", func(t *testing.T) {
			// CO-6b-003: Pick returns the first match, so duplicates would
			// silently collapse and a second scope could bypass the
			// one-scope-per-page rule. annotations.New dedupes same-type
			// messages, but the wire does not (Append/Merge or a remote
			// connector's raw slice) — build the duplicates with Append.
			s := newSyncerFixture(&fakeSourceCacheStore{})
			dupRecords := annotations.Annotations{}
			dupRecords.Append(
				v2.SourceCacheRecord_builder{ScopeKey: "grants:a", CacheValidator: "v"}.Build(),
				v2.SourceCacheRecord_builder{ScopeKey: "grants:b", CacheValidator: "v"}.Build())
			_, err := s.sourceCachePageOps(ctx, kind, dupRecords, 0)
			requireVerdict(t, err, ReplayVerdictCold, kind)
			dupReplays := annotations.Annotations{}
			dupReplays.Append(
				v2.SourceCacheReplay_builder{ScopeKey: "grants:a"}.Build(),
				v2.SourceCacheReplay_builder{ScopeKey: "grants:b"}.Build())
			_, err = s.sourceCachePageOps(ctx, kind, dupReplays, 0)
			requireVerdict(t, err, ReplayVerdictCold, kind)
		})
	})

	t.Run("sync-shape-gates", func(t *testing.T) {
		// CO-6b-003: source-cache handling is untargeted-FULL-sync only. A
		// partial/targeted sync ignores the annotations wholesale (nil ops,
		// no error) — its pages must not stamp, publish, or replay.
		for name, mutate := range map[string]func(*syncer){
			"partial-sync":  func(s *syncer) { s.syncType = connectorstore.SyncTypePartial },
			"targeted-sync": func(s *syncer) { s.targetedSyncResources = []*v2.Resource{{}} },
		} {
			t.Run(name, func(t *testing.T) {
				s := newSyncerFixture(&fakeSourceCacheStore{})
				mutate(s)
				require.False(t, s.sourceCacheEnabled())
				ops, err := s.sourceCachePageOps(ctx, kind,
					annotations.New(v2.SourceCacheRecord_builder{ScopeKey: scope, CacheValidator: "v"}.Build()), 0)
				require.NoError(t, err)
				require.Nil(t, ops)
			})
		}
	})

	buildReplayOps := func(t *testing.T, s *syncer) *sourceCachePageOps {
		t.Helper()
		ops, err := s.sourceCachePageOps(ctx, kind,
			annotations.New(v2.SourceCacheReplay_builder{ScopeKey: scope, CacheValidator: "v2"}.Build()), 0)
		require.NoError(t, err)
		require.NotNil(t, ops)
		return ops
	}

	t.Run("provenance-cold-paths", func(t *testing.T) {
		t.Run("replay-on-attempt-not-warm", func(t *testing.T) {
			// CO-6b-003: a checkpointed hit-set must not authorize replay
			// on an attempt whose consume gates degraded to cold (compat
			// drift, withdrawn/swapped previous artifact). The hit exists;
			// the warm flag does not; the replay dies cold.
			s := newSyncerFixture(&fakeSourceCacheStore{})
			s.sourceCacheWarm = false
			s.state.RecordSourceCacheHit(kind, scope, "v-hit")
			err := buildReplayOps(t, s).beforeUpserts(ctx)
			requireVerdict(t, err, ReplayVerdictCold, kind)
		})
		t.Run("replay-without-this-sync-hit", func(t *testing.T) {
			s := newSyncerFixture(&fakeSourceCacheStore{})
			err := buildReplayOps(t, s).beforeUpserts(ctx)
			requireVerdict(t, err, ReplayVerdictCold, kind)
		})
		t.Run("replay-with-hit-but-no-previous-artifact", func(t *testing.T) {
			s := newSyncerFixture(&fakeSourceCacheStore{})
			s.previousSyncReader = nil
			s.state.RecordSourceCacheHit(kind, scope, "v-hit")
			err := buildReplayOps(t, s).beforeUpserts(ctx)
			requireVerdict(t, err, ReplayVerdictCold, kind)
		})
		// Hit-binding cells: the eligibility gates cannot distinguish two
		// artifacts from the same connector and config (identical compat
		// keys), so a previous artifact swapped between attempts must be
		// caught by binding the recorded hit's validator to the CURRENT
		// base's manifest entry.
		t.Run("swapped-base-validator-mismatch", func(t *testing.T) {
			s := newSyncerFixture(&fakeSourceCacheStore{})
			s.previousSyncReader = stubPreviousReader{
				entry: sourcecache.Entry{CacheValidator: "v-older-artifact"},
				found: true,
			}
			s.state.RecordSourceCacheHit(kind, scope, "v-hit")
			err := buildReplayOps(t, s).beforeUpserts(ctx)
			requireVerdict(t, err, ReplayVerdictCold, kind)
		})
		t.Run("swapped-base-entry-missing", func(t *testing.T) {
			s := newSyncerFixture(&fakeSourceCacheStore{})
			s.previousSyncReader = stubPreviousReader{found: false}
			s.state.RecordSourceCacheHit(kind, scope, "v-hit")
			err := buildReplayOps(t, s).beforeUpserts(ctx)
			requireVerdict(t, err, ReplayVerdictCold, kind)
		})
		t.Run("base-entry-read-failure", func(t *testing.T) {
			s := newSyncerFixture(&fakeSourceCacheStore{})
			s.previousSyncReader = stubPreviousReader{entryErr: errors.New("manifest read failed")}
			s.state.RecordSourceCacheHit(kind, scope, "v-hit")
			err := buildReplayOps(t, s).beforeUpserts(ctx)
			requireVerdict(t, err, ReplayVerdictCold, kind)
		})
		t.Run("base-without-entry-surface", func(t *testing.T) {
			s := newSyncerFixture(&fakeSourceCacheStore{})
			s.previousSyncReader = bareStubPreviousReader{}
			s.state.RecordSourceCacheHit(kind, scope, "v-hit")
			err := buildReplayOps(t, s).beforeUpserts(ctx)
			requireVerdict(t, err, ReplayVerdictCold, kind)
		})
	})

	t.Run("replay-copy-classification", func(t *testing.T) {
		for name, tc := range map[string]struct {
			err  error
			want ReplayVerdict
		}{
			"source-side-error-is-cold":       {err: errors.New("preflight: poisoned scope"), want: ReplayVerdictCold},
			"destination-commit-is-warm":      {err: fmt.Errorf("commit: %w", dotc1z.ErrSourceCacheReplayDestination), want: ReplayVerdictWarm},
			"cancellation-mid-replay-is-warm": {err: fmt.Errorf("copy: %w", context.Canceled), want: ReplayVerdictWarm},
		} {
			t.Run(name, func(t *testing.T) {
				s := newSyncerFixture(&fakeSourceCacheStore{replayErr: tc.err})
				s.state.RecordSourceCacheHit(kind, scope, "v-hit")
				err := buildReplayOps(t, s).beforeUpserts(ctx)
				requireVerdict(t, err, tc.want, kind)
			})
		}
		t.Run("ambient-cancellation-does-not-promote", func(t *testing.T) {
			// CO-6b-003: only the ERROR CHAIN classifies. In parallel mode
			// a sibling failure cancels the batch context, and a genuine
			// source-integrity error racing that cancellation must stay
			// cold (fail-closed), not read as warm.
			require.Equal(t, ReplayVerdictCold, replayCopyVerdict(errors.New("preflight: poisoned scope")))
			require.Equal(t, ReplayVerdictWarm, replayCopyVerdict(fmt.Errorf("copy: %w", context.Canceled)))
		})
	})

	t.Run("destination-warm-paths", func(t *testing.T) {
		buildRecordOps := func(t *testing.T, s *syncer, record *v2.SourceCacheRecord) *sourceCachePageOps {
			t.Helper()
			ops, err := s.sourceCachePageOps(ctx, kind, annotations.New(record), 0)
			require.NoError(t, err)
			require.NotNil(t, ops)
			return ops
		}
		t.Run("canonical-tombstone-failure", func(t *testing.T) {
			s := newSyncerFixture(&fakeSourceCacheStore{deleteRowsErr: errors.New("write stall")})
			ops := buildRecordOps(t, s, v2.SourceCacheRecord_builder{
				ScopeKey: scope, CacheValidator: "v2", DeletedIds: []string{"g1"},
			}.Build())
			requireVerdict(t, ops.afterUpserts(ctx), ReplayVerdictWarm, kind)
		})
		t.Run("principal-tombstone-failure", func(t *testing.T) {
			s := newSyncerFixture(&fakeSourceCacheStore{deleteScopeErr: errors.New("write stall")})
			ops := buildRecordOps(t, s, v2.SourceCacheRecord_builder{
				ScopeKey: scope, CacheValidator: "v2", DeletedPrincipalIds: []string{"u1"},
			}.Build())
			requireVerdict(t, ops.afterUpserts(ctx), ReplayVerdictWarm, kind)
		})
		t.Run("manifest-publish-failure", func(t *testing.T) {
			s := newSyncerFixture(&fakeSourceCacheStore{putEntryErr: errors.New("write stall")})
			ops := buildRecordOps(t, s, v2.SourceCacheRecord_builder{
				ScopeKey: scope, CacheValidator: "v2",
			}.Build())
			requireVerdict(t, ops.afterUpserts(ctx), ReplayVerdictWarm, kind)
		})
		t.Run("page-row-put-failure-is-warm", func(t *testing.T) {
			// The put itself happens in the collection handlers; they wrap
			// through wrapPageRowPutError (B7: overlay/record upsert write
			// errors are warm). Nil ops and nil errors pass through.
			s := newSyncerFixture(&fakeSourceCacheStore{})
			ops := buildRecordOps(t, s, v2.SourceCacheRecord_builder{
				ScopeKey: scope, CacheValidator: "v2",
			}.Build())
			requireVerdict(t, ops.wrapPageRowPutError(errors.New("write stall")), ReplayVerdictWarm, kind)
			var nilOps *sourceCachePageOps
			plain := errors.New("plain store error")
			require.Equal(t, plain, nilOps.wrapPageRowPutError(plain),
				"a page without honored annotations must not gain a verdict")
			require.NoError(t, ops.wrapPageRowPutError(nil))
		})
		t.Run("malformed-resource-tombstone-is-cold", func(t *testing.T) {
			// CO-6b-003: resource tombstones must be Baton resource BIDs;
			// a malformed id fails deterministically before any write, so
			// retrying it warm can never succeed.
			s := newSyncerFixture(&fakeSourceCacheStore{})
			ops, err := s.sourceCachePageOps(ctx, sourcecache.RowKindResources,
				annotations.New(v2.SourceCacheRecord_builder{
					ScopeKey: "res:all", CacheValidator: "v2", DeletedIds: []string{"not-a-bid"},
				}.Build()), 0)
			require.NoError(t, err)
			requireVerdict(t, ops.afterUpserts(ctx), ReplayVerdictCold, sourcecache.RowKindResources)
		})
	})

	t.Run("compat-materialization-only-mismatch-degrades", func(t *testing.T) {
		// G7 exact-match with ONLY the stored sdk_materialization_generation
		// flipped (a corrupt or stale payload field under a current envelope
		// witness): the warm store must degrade. The chaos gate matrix flips
		// the connector-controlled fields; this cell covers the one field a
		// capability cannot reach.
		s := newSyncerFixture(&fakeSourceCacheStore{})
		stored := s.computeSourceCacheCompatKey()
		stored.SDKMaterializationGeneration += "-stale"
		s.previousSyncReader = &fakeCompatPreviousReader{compat: stored, found: true}
		_, warm := s.sourceCacheWarmStore(ctx)
		require.False(t, warm, "a materialization-generation mismatch alone must degrade to cold")

		// Sanity against vacuity: the same reader with the computed key
		// byte-matched goes warm.
		s.previousSyncReader = &fakeCompatPreviousReader{compat: s.computeSourceCacheCompatKey(), found: true}
		_, warm = s.sourceCacheWarmStore(ctx)
		require.True(t, warm)
	})
}

// fakeCompatPreviousReader satisfies the consume-side gate surfaces
// (sourceCacheEntryReader + sourceCacheCompatReader) with a fixed compat
// record, for G7 field-isolation cells the chaos capability cannot reach.
type fakeCompatPreviousReader struct {
	connectorstore.Reader
	compat sourcecache.CompatKey
	found  bool
}

func (f *fakeCompatPreviousReader) LookupSourceCacheEntry(context.Context, sourcecache.RowKind, string) (sourcecache.Entry, bool, error) {
	return sourcecache.Entry{}, false, nil
}

func (f *fakeCompatPreviousReader) GetSourceCacheCompat(context.Context) (sourcecache.CompatKey, bool, error) {
	return f.compat, f.found, nil
}

// TestChaosSourceCacheReplayWithoutHitFailsCold pins R8's loud-failure
// clause at sync level: a connector that emits SourceCacheReplay while the
// sync is COLD (no previous artifact, so the lookup is NoopLookup and no
// hit can exist) skipped row generation with nothing to fall back to — the
// sync must FAIL with a cold ErrReplayIntegrity, never degrade silently.
func TestChaosSourceCacheReplayWithoutHitFailsCold(t *testing.T) {
	skipChaosInShort(t)

	fx := newSCCollectionFixture(t)
	// One cell per collection handler (resources, entitlements, grants):
	// the loud cold failure must fire from each handler's own
	// sourceCachePageOps seam, and — with the held-lock ride-along — each
	// cell also proves that handler's deferred release() backstop frees
	// the scope lock on the error path (CO-6b-007; removing any one
	// handler's defer fails its cell at the harness's sync-end assertion).
	cells := []struct {
		rowKind  sourcecache.RowKind
		scopeKey string
		mutate   func(d *chaosconnector.Dataset)
	}{
		{
			rowKind:  sourcecache.RowKindResources,
			scopeKey: "resources:user",
			mutate: func(d *chaosconnector.Dataset) {
				d.Resources[scUserTypeID] = chaosconnector.Pages[*v2.Resource]{
					"": {Annotations: scReplayAnno("resources:user", scValidatorV1, false, nil, nil)},
				}
			},
		},
		{
			rowKind:  sourcecache.RowKindEntitlements,
			scopeKey: "entitlements:team-1",
			mutate: func(d *chaosconnector.Dataset) {
				d.Entitlements["team-1"] = chaosconnector.Pages[*v2.Entitlement]{
					"": {Annotations: scReplayAnno("entitlements:team-1", scValidatorV1, false, nil, nil)},
				}
			},
		},
		{
			rowKind:  sourcecache.RowKindGrants,
			scopeKey: scGrantsScopeKey,
			mutate: func(d *chaosconnector.Dataset) {
				d.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
					"": {Annotations: scReplayAnno(scGrantsScopeKey, scValidatorV1, false, nil, nil)},
				}
			},
		},
	}
	for _, cell := range cells {
		t.Run(string(cell.rowKind), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			tmpDir, paths := sourceCachePaths(t, 1)

			d := scCollectionBase(fx)
			// The ROOT page itself carries a replay annotation: a
			// misbehaving connector replaying without ever consulting.
			cell.mutate(d)
			scenario := &chaosconnector.Scenario{
				Name: "source-cache-replay-without-hit-" + string(cell.rowKind), Seed: 1, InitialEpoch: "seed",
				Epochs: map[string]*chaosconnector.Dataset{"seed": d},
			}
			require.NoError(t, scenario.Validate())

			run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			run.SetSourceCacheCapability(sourceCacheCapabilityRW("gen-1", "cfg-1"))
			harness := newChaosHarness(t, ctx, run, paths[0], tmpDir, chaosTransportDirect, WithWorkerCount(1))
			syncErr := harness.Syncer.Sync(ctx)
			require.Error(t, syncErr, "a replay with no this-sync lookup hit must fail the sync")
			require.ErrorIs(t, syncErr, ErrReplayIntegrity)
			var rie *ReplayIntegrityError
			require.ErrorAs(t, syncErr, &rie)
			require.Equal(t, ReplayVerdictCold, rie.Verdict)
			require.Equal(t, cell.rowKind, rie.RowKind)
			require.Equal(t, cell.scopeKey, rie.ScopeKey)
			require.NoError(t, harness.Close(t.Context()))
		})
	}
}

// TestChaosSourceCacheCompatDriftOnResume pins R4's drift-on-resume clause
// (plan B4): the connector's capability changes between an interrupted
// attempt and its resume, so the artifact's cached rows are
// mixed-generation. The resume must proceed green but degrade its own
// lookup to cold, leave the ORIGINAL compat record in place, and mark the
// artifact replay-blocked so it cannot seed the next generation.
func TestChaosSourceCacheCompatDriftOnResume(t *testing.T) {
	skipChaosInShort(t)

	cells := []struct {
		name string
		// resumeCapability arms the resume attempt; nil withdraws the
		// capability entirely (CO-6b-003's stale-produce-state branch).
		resumeCapability *v2.SourceCacheCapability
	}{
		{
			// The recomputed compat key differs from the stored one.
			name:             "drifted-generation",
			resumeCapability: sourceCacheCapabilityRW("gen-2", "cfg-1"),
		},
		{
			// The connector stops declaring the capability across the
			// resume: attempt 1's produce state (compat record, stamps)
			// was recorded under conditions this attempt no longer
			// declares — the same mixed-generation hazard as drift.
			name:             "capability-withdrawn",
			resumeCapability: nil,
		},
	}

	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			tmpDir, paths := sourceCachePaths(t, 2)
			seedPath, driftPath := paths[0], paths[1]

			fixture := newSourceCacheFixture(t)
			scenario := newSourceCacheScenario(t, fixture)

			// Generation A: clean cold seed under gen-1.
			seedRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			seedRun.SetSourceCacheCapability(sourceCacheCapabilityRW("gen-1", "cfg-1"))
			requireSourceCacheEvents(t,
				runSourceCacheSync(t, ctx, seedRun, chaosTransportDirect, seedPath, tmpDir, "", WithWorkerCount(1)),
				[]chaosconnector.SourceCacheLookupEvent{scColdEvent()})

			// Generation B, attempt 1 under gen-1: writes the compat record at
			// sync start, then crashes on the first grants root request.
			interruptedRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
				ID: "cut-grants-root",
				Match: chaosconnector.Matcher{
					Service:   chaosconnector.ExactString("GrantsService"),
					Method:    chaosconnector.ExactString("ListGrants"),
					PageToken: chaosconnector.ExactString(""),
					Attempt:   1,
					Phase:     chaosconnector.PhaseBeforeCall,
				},
				Effects:  []chaosconnector.Effect{{Kind: chaosconnector.EffectCrash}},
				MinFires: 1,
				MaxFires: 1,
			}))
			require.NoError(t, err)
			interruptedRun.SetSourceCacheCapability(sourceCacheCapabilityRW("gen-1", "cfg-1"))
			interruptedHarness := newChaosHarness(t, ctx, interruptedRun, driftPath, tmpDir, chaosTransportDirect,
				WithPreviousSyncC1ZPath(seedPath), WithWorkerCount(1))
			require.ErrorIs(t, interruptedHarness.Syncer.Sync(ctx), chaosconnector.ErrInterruptRequested)
			require.NoError(t, interruptedHarness.Close(t.Context()))
			require.NoError(t, interruptedRun.Runtime().VerifyRequired())

			// Resume under the drifted/withdrawn capability. The sync
			// proceeds but every consult must go cold even though the
			// previous artifact would pass the consume gates for gen-1.
			resumeRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			resumeRun.SetSourceCacheCapability(tc.resumeCapability)
			resumeEvents := runSourceCacheSync(t, ctx, resumeRun, chaosTransportDirect, driftPath, tmpDir, seedPath, WithWorkerCount(1))
			requireAllColdEvents(t, resumeEvents, 1)

			// The ORIGINAL record stays: it described the rows recorded before
			// the drift; overwriting it would vouch for mixed-generation rows.
			snapshot := readSourceCacheSnapshot(t, ctx, driftPath, tmpDir)
			require.NotNil(t, snapshot.Compat)
			require.Equal(t, "gen-1", snapshot.Compat.ConnectorCacheGeneration,
				"drift must not overwrite the original compat record")

			// The artifact is barred from seeding the next generation.
			quality := readLifecycleIngestQuality(t, driftPath)
			require.NotNil(t, quality)
			require.True(t, quality.GetSourceCacheReplayBlocked(),
				"compat drift on resume must mark the artifact replay-blocked")
		})
	}
}

// TestChaosSourceCacheDriftedResumeRejectsRestoredReplay pins the
// CO-6b-003 warm gate: a CHECKPOINTED lookup hit must not re-authorize a
// replay verdict on a resume attempt whose consume gates degraded to cold.
//
// PREMISE (corrected per re-review, CO-6b-005): per-resource grants
// actions dispatch in batches of maxPeekActionsCount (100), and provenance
// recorded during a batch becomes durable only at the checkpoint atop the
// NEXT loop iteration — so the consult site and the crash site must sit in
// DIFFERENT batches or no checkpoint ever contains the hit and the resume
// dies on the ordinary no-hit gate instead. The test therefore uses 102
// team resources: the consult team drains first (batch 1, hit for scope S
// recorded and checkpointed at the top of the batch-2 iteration — the test
// captures that checkpoint and asserts the hit is IN it), a filler team at
// the top of batch 2 crashes on its first serve, and the carrier team
// (batch 2, after the filler) statically holds the SourceCacheReplay for S
// it never got to serve in attempt 1. The resume runs under gen-2 (compat
// drift ⇒ cold), restores the hit-set WITH the hit, re-runs the carrier —
// and must die loudly with a cold ErrReplayIntegrity from the warm gate,
// not silently copy from a base the drifted attempt never re-validated.
// Mutation-verified: with the warm gate removed, the restored hit and the
// still-eligible seed base let the copy proceed and the test fails.
func TestChaosSourceCacheDriftedResumeRejectsRestoredReplay(t *testing.T) {
	skipChaosInShort(t)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	tmpDir, paths := sourceCachePaths(t, 2)
	seedPath, warmPath := paths[0], paths[1]

	const consultScopeKey = "grants:team-hit"
	fixture := newSourceCacheFixture(t)
	// Resources list order is push order; actions drain in REVERSE, so the
	// consult team (listed last) drains first and the carrier (listed
	// first) drains last, with 100 fillers between them forcing the batch
	// split: batch 1 = consult + fillers 100..02, batch 2 = filler 01 +
	// carrier.
	// NOTE: the syncer pushes per-resource grants actions in the STORE's
	// lexicographic resource-id order, so the ids are chosen to sort the
	// carrier (team-1) first — draining last — and the consult site
	// (z-team-hit) last — draining first.
	teams := []*v2.Resource{fixture.Team} // team-1 is the replay carrier.
	for i := 1; i <= 100; i++ {
		filler, err := rs.NewGroupResource(fmt.Sprintf("Filler %03d", i), fixture.TeamType, fmt.Sprintf("u-filler-%03d", i), nil)
		require.NoError(t, err)
		teams = append(teams, filler)
	}
	consultTeam, err := rs.NewGroupResource("Team Hit", fixture.TeamType, "z-team-hit", nil)
	require.NoError(t, err)
	teams = append(teams, consultTeam)

	dataset := newSourceCacheDataset(fixture)
	dataset.Resources[scTeamTypeID] = chaosconnector.Pages[*v2.Resource]{"": {List: teams}}
	// team-hit: the consult site. The spec declares no warm branch, so the
	// hit for S is recorded but the cold root (with its record) is served
	// either way — the REPLAY verdict for S travels to the carrier.
	dataset.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
		"z-team-hit": {ScopeKey: consultScopeKey, Validator: scValidatorV1},
	}
	dataset.Grants["z-team-hit"] = chaosconnector.Pages[*v2.Grant]{
		"": {
			Annotations: annotations.New(v2.SourceCacheRecord_builder{
				ScopeKey:       consultScopeKey,
				CacheValidator: scValidatorV1,
			}.Build()),
		},
	}
	// team-1: the handed-off replay carrier for team-hit's scope S.
	dataset.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
		"": {
			Annotations: annotations.New(v2.SourceCacheReplay_builder{
				ScopeKey:       consultScopeKey,
				CacheValidator: scValidatorV1,
			}.Build()),
		},
	}
	scenario := &chaosconnector.Scenario{
		Name:         "source-cache-drifted-resume-replay",
		Seed:         1,
		InitialEpoch: "initial",
		Epochs:       map[string]*chaosconnector.Dataset{"initial": dataset},
	}
	require.NoError(t, scenario.Validate())

	// Generation A seeds from a variant whose team-1 root is plain: A has
	// no previous artifact, so a replay page in A would (correctly) die on
	// the no-hit check before ever producing the seed.
	seedDataset := newSourceCacheDataset(fixture)
	seedDataset.Resources[scTeamTypeID] = dataset.Resources[scTeamTypeID]
	seedDataset.SourceCacheGrants = dataset.SourceCacheGrants
	seedDataset.Grants["z-team-hit"] = dataset.Grants["z-team-hit"]
	seedDataset.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{"": {List: fixture.Grants}}
	seedScenario := &chaosconnector.Scenario{
		Name:         "source-cache-drifted-resume-replay-seed",
		Seed:         1,
		InitialEpoch: "initial",
		Epochs:       map[string]*chaosconnector.Dataset{"initial": seedDataset},
	}
	require.NoError(t, seedScenario.Validate())
	seedRun, err := chaosconnector.NewRun(seedScenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	seedRun.SetSourceCacheCapability(sourceCacheCapabilityRW("gen-1", "cfg-1"))
	requireAllColdEvents(t,
		runSourceCacheSync(t, ctx, seedRun, chaosTransportDirect, seedPath, tmpDir, "", WithWorkerCount(1)), 1)

	// Generation B attempt 1 under gen-1: z-team-hit consults in batch 1
	// (hit recorded); the checkpoint atop the batch-2 iteration makes it
	// durable; u-filler-001 (top of batch 2) crashes on first serve,
	// before the carrier's replay page is ever processed.
	interruptedRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: "cut-batch-two",
		Match: chaosconnector.Matcher{
			Service:   chaosconnector.ExactString("GrantsService"),
			Method:    chaosconnector.ExactString("ListGrants"),
			Subject:   chaosconnector.ExactString("u-filler-001"),
			PageToken: chaosconnector.ExactString(""),
			Attempt:   1,
			Phase:     chaosconnector.PhaseBeforeCall,
		},
		Effects:  []chaosconnector.Effect{{Kind: chaosconnector.EffectCrash}},
		MinFires: 1,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	interruptedRun.SetSourceCacheCapability(sourceCacheCapabilityRW("gen-1", "cfg-1"))
	interruptedHarness := newChaosHarness(t, ctx, interruptedRun, warmPath, tmpDir, chaosTransportDirect,
		WithPreviousSyncC1ZPath(seedPath), WithWorkerCount(1))
	interruptedConcrete, ok := interruptedHarness.Syncer.(*syncer)
	require.True(t, ok)
	// Persist every loop iteration's checkpoint and capture each token so
	// the premise is provable, not assumed.
	interruptedConcrete.checkpointInterval = 0
	var lastCheckpoint string
	interruptedConcrete.testCheckpointHook = func(token string) { lastCheckpoint = token }
	require.ErrorIs(t, interruptedHarness.Syncer.Sync(ctx), chaosconnector.ErrInterruptRequested)
	require.NoError(t, interruptedHarness.Close(t.Context()))
	require.NoError(t, interruptedRun.Runtime().VerifyRequired())
	// The hit for S was recorded before the cut...
	requireSourceCacheEvents(t, interruptedRun.SourceCacheLookupEvents(),
		[]chaosconnector.SourceCacheLookupEvent{{
			RowKind:           sourcecache.RowKindGrants,
			ScopeKey:          consultScopeKey,
			Hit:               true,
			PreviousValidator: scValidatorV1,
			Matched:           true,
			ServedWarm:        false,
		}})
	// ...and — the premise this test exists for — the DURABLE CHECKPOINT
	// that survives the crash contains it. Without this the resume would
	// reject on the ordinary no-hit gate and the warm gate would be
	// untested vacuously.
	require.NotEmpty(t, lastCheckpoint)
	checkpointed := newState()
	require.NoError(t, checkpointed.Unmarshal(lastCheckpoint))
	v, hasHit := checkpointed.SourceCacheHitValidator(sourcecache.RowKindGrants, consultScopeKey)
	require.True(t, hasHit, "the surviving checkpoint must contain the batch-1 hit; if it does not, the batch split regressed and this test is vacuous")
	require.Equal(t, scValidatorV1, v)
	// The second premise half: the carrier's grants action is still
	// PENDING in that same checkpoint — the resume genuinely re-serves a
	// replay verdict it did not invent.
	carrierPending := false
	for cur := checkpointed.Current(); cur != nil; cur = checkpointed.Current() {
		if cur.Op == SyncGrantsOp && cur.ResourceID == "team-1" {
			carrierPending = true
		}
		checkpointed.FinishAction(ctx, cur)
	}
	require.True(t, carrierPending, "the surviving checkpoint must still hold the carrier's grants action; if not, the batch layout regressed and this test is vacuous")

	// Resume under gen-2: gates degrade to cold (compat drift). The
	// restored hit-set still holds S; the carrier re-runs its root and
	// re-serves the replay verdict — loud cold death from the warm gate,
	// not a silent copy from the un-revalidated base.
	resumeRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	resumeRun.SetSourceCacheCapability(sourceCacheCapabilityRW("gen-2", "cfg-1"))
	resumeHarness := newChaosHarness(t, ctx, resumeRun, warmPath, tmpDir, chaosTransportDirect,
		WithPreviousSyncC1ZPath(seedPath), WithWorkerCount(1))
	err = resumeHarness.Syncer.Sync(ctx)
	require.ErrorIs(t, err, ErrReplayIntegrity,
		"a drifted resume must reject the restored replay verdict loudly")
	var rie *ReplayIntegrityError
	require.ErrorAs(t, err, &rie)
	require.Equal(t, ReplayVerdictCold, rie.Verdict)
	require.Equal(t, sourcecache.RowKindGrants, rie.RowKind)
	require.Equal(t, consultScopeKey, rie.ScopeKey)
	require.NoError(t, resumeHarness.Close(t.Context()))
}

// TestChaosSourceCacheUnsupportedShapesBlockReplaySeed pins the CO-6b-003
// produce-side guard: a source-cache-annotated page whose shape replay
// cannot reproduce — resource rows declaring child resource types, or a
// grants response carrying InsertResourceGrants — must mark the sealed
// artifact replay-blocked so it never seeds a warm generation (the derived
// rows would silently vanish on replay). Generation B against the blocked
// artifact must go all-cold through the G4 quality gate.
func TestChaosSourceCacheUnsupportedShapesBlockReplaySeed(t *testing.T) {
	skipChaosInShort(t)

	cells := []struct {
		name    string
		dataset func(fixture *scFixture) *chaosconnector.Dataset
	}{
		{
			// A record-annotated RESOURCES page whose row declares a child
			// resource type: child discovery is scheduled from a page's own
			// rows, which a replayed page never has.
			name: "child-resource-types",
			dataset: func(fixture *scFixture) *chaosconnector.Dataset {
				teamWithChildren := proto.Clone(fixture.Team).(*v2.Resource)
				annos := annotations.Annotations(teamWithChildren.GetAnnotations())
				annos.Update(v2.ChildResourceType_builder{ResourceTypeId: scUserTypeID}.Build())
				teamWithChildren.SetAnnotations(annos)
				dataset := newSourceCacheDataset(fixture)
				dataset.Resources[scTeamTypeID] = chaosconnector.Pages[*v2.Resource]{
					"": {
						List: []*v2.Resource{teamWithChildren},
						Annotations: annotations.New(v2.SourceCacheRecord_builder{
							ScopeKey:       "resources:team",
							CacheValidator: "rv1",
						}.Build()),
					},
				}
				return dataset
			},
		},
		{
			// A record-annotated GRANTS page carrying InsertResourceGrants:
			// grant-discovered resources are materialized from the page's
			// own rows and response annotation, which replay never re-runs.
			name: "insert-resource-grants",
			dataset: func(fixture *scFixture) *chaosconnector.Dataset {
				dataset := newSourceCacheDataset(fixture)
				grantsAnnos := annotations.New(v2.SourceCacheRecord_builder{
					ScopeKey:       scGrantsScopeKey,
					CacheValidator: scValidatorV1,
				}.Build())
				grantsAnnos.Update(&v2.InsertResourceGrants{})
				dataset.Grants["team-1"] = chaosconnector.Pages[*v2.Grant]{
					"": {List: fixture.Grants, Annotations: grantsAnnos},
				}
				// Both generations serve cold (G4 blocks B); the spec's
				// warm branch is unreachable, so don't declare one.
				dataset.SourceCacheGrants[scGrantsScopeKey] = nil
				dataset.SourceCacheGrants = map[string]*chaosconnector.SourceCacheSpec{
					"team-1": {ScopeKey: scGrantsScopeKey, Validator: scValidatorV1},
				}
				return dataset
			},
		},
	}

	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			tmpDir, paths := sourceCachePaths(t, 2)
			seedPath, nextPath := paths[0], paths[1]

			fixture := newSourceCacheFixture(t)
			scenario := &chaosconnector.Scenario{
				Name:         "source-cache-unsupported-shape-" + tc.name,
				Seed:         1,
				InitialEpoch: "initial",
				Epochs:       map[string]*chaosconnector.Dataset{"initial": tc.dataset(fixture)},
			}
			require.NoError(t, scenario.Validate())
			capability := sourceCacheCapabilityRW("gen-1", "cfg-1")

			// Generation A syncs green but seals replay-blocked.
			seedRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			seedRun.SetSourceCacheCapability(capability)
			requireAllColdEvents(t,
				runSourceCacheSync(t, ctx, seedRun, chaosTransportDirect, seedPath, tmpDir, "", WithWorkerCount(1)), 1)
			quality := readLifecycleIngestQuality(t, seedPath)
			require.NotNil(t, quality)
			require.True(t, quality.GetSourceCacheReplayBlocked(),
				"an unsupported page shape must mark the artifact replay-blocked")

			// Generation B: the blocked artifact must not seed a warm sync.
			nextRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			nextRun.SetSourceCacheCapability(capability)
			requireAllColdEvents(t,
				runSourceCacheSync(t, ctx, nextRun, chaosTransportDirect, nextPath, tmpDir, seedPath, WithWorkerCount(1)), 1)
		})
	}
}

// deliverabilityProbeClient satisfies sourcecache.SetLookup structurally and
// implements the deliverability probe. deliverable=false models a wrapper
// whose transport cannot forward an interface value (the runner's
// subprocess client, CO-6b-001): SetSourceCache exists but delivers into
// the void.
type deliverabilityProbeClient struct {
	types.ConnectorClient
	deliverable bool
	delivered   []sourcecache.Lookup
}

func (c *deliverabilityProbeClient) SetSourceCache(_ context.Context, lookup sourcecache.Lookup) {
	c.delivered = append(c.delivered, lookup)
}

func (c *deliverabilityProbeClient) SourceCacheLookupDeliverable() bool { return c.deliverable }

// structuralSetLookupClient satisfies SetLookup but not the probe — the
// presumed-deliverable default for clients that own their SetSourceCache.
type structuralSetLookupClient struct {
	types.ConnectorClient
	delivered []sourcecache.Lookup
}

func (c *structuralSetLookupClient) SetSourceCache(_ context.Context, lookup sourcecache.Lookup) {
	c.delivered = append(c.delivered, lookup)
}

// TestSourceCacheLookupDeliverabilityProbe pins the install-side
// deliverability contract (CO-6b-003, closing the original false-warm
// BLOCKER): a client that satisfies SetLookup structurally while its probe
// reports undeliverable receives NOTHING — no delivery call, no warm flag —
// while deliverable and probe-less clients receive the (possibly nil)
// lookup unconditionally. Removing the probe type-assert in
// installSourceCacheLookup fails the undeliverable cell.
func TestSourceCacheLookupDeliverabilityProbe(t *testing.T) {
	ctx := context.Background()
	newFixture := func(client types.ConnectorClient) *syncer {
		return &syncer{
			connector:             client,
			state:                 newState(),
			syncType:              connectorstore.SyncTypeFull,
			sourceCacheCapability: sourceCacheCapabilityRW("gen-1", "cfg-1"),
		}
	}

	t.Run("undeliverable-transport-gets-no-delivery-and-stays-cold", func(t *testing.T) {
		client := &deliverabilityProbeClient{deliverable: false}
		s := newFixture(client)
		teardown, err := s.installSourceCacheLookup(ctx)
		require.NoError(t, err)
		require.NotNil(t, teardown)
		require.Empty(t, client.delivered,
			"SetSourceCache must not be called when the transport cannot deliver: the connector would consult NoopLookup while the syncer believed a lookup was live")
		require.False(t, s.sourceCacheWarm)
		teardown()
		require.Empty(t, client.delivered, "teardown must not deliver either")
	})

	t.Run("deliverable-transport-receives-delivery-and-teardown", func(t *testing.T) {
		client := &deliverabilityProbeClient{deliverable: true}
		s := newFixture(client)
		teardown, err := s.installSourceCacheLookup(ctx)
		require.NoError(t, err)
		// No previous artifact in this fixture, so the consume gates are
		// cold and the delivered lookup is nil (the builder substitutes
		// NoopLookup); delivery is still unconditional so a long-lived
		// server never carries a prior sync's lookup.
		require.Len(t, client.delivered, 1)
		require.Nil(t, client.delivered[0])
		require.False(t, s.sourceCacheWarm)
		teardown()
		require.Len(t, client.delivered, 2)
		require.Nil(t, client.delivered[1])
	})

	t.Run("probe-less-client-is-presumed-deliverable", func(t *testing.T) {
		client := &structuralSetLookupClient{}
		s := newFixture(client)
		teardown, err := s.installSourceCacheLookup(ctx)
		require.NoError(t, err)
		require.Len(t, client.delivered, 1)
		teardown()
	})

	t.Run("client-without-setlookup-stays-cold", func(t *testing.T) {
		// A capable connector behind a client that does not satisfy
		// SetLookup at all (coverage-triage cell, CO-6b-007): install
		// degrades to cold with a warn, returns a no-op teardown, and the
		// sync proceeds.
		s := newFixture(struct{ types.ConnectorClient }{})
		teardown, err := s.installSourceCacheLookup(ctx)
		require.NoError(t, err)
		require.NotNil(t, teardown)
		require.False(t, s.sourceCacheWarm)
		teardown()
	})
}

// blockingReplayStore parks every ReplaySourceCache call until release is
// closed, so the test can hold one copy mid-flight and observe whether a
// second is admitted.
type blockingReplayStore struct {
	fakeSourceCacheStore
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (b *blockingReplayStore) ReplaySourceCache(context.Context, connectorstore.Reader, sourcecache.RowKind, string) (dotc1z.SourceCacheReplayResult, error) {
	b.calls.Add(1)
	b.entered <- struct{}{}
	<-b.release
	return dotc1z.SourceCacheReplayResult{}, nil
}

// TestSourceCacheReplayOncePerScopeIsAtomic overlaps two beforeUpserts
// calls for the SAME (rowKind, scopeKey) — the shape the per-scope lock
// exists for (CO-6b-003): two workers serving replay annotations for one
// scope, both past the provenance gates, racing decide-copy-mark. The
// first copy is held mid-flight; if the second call reaches the store
// while the first has not yet marked, the once-per-scope guard is not
// atomic and a REPLACEMENT copy could wipe overlay rows the first already
// admitted. Removing the scope mutex in beforeUpserts fails this test
// inside the 300ms window; with the mutex the second call parks on Lock
// and then takes the already-replayed skip path.
func TestSourceCacheReplayOncePerScopeIsAtomic(t *testing.T) {
	ctx := context.Background()
	const scope = "grants:team-1"
	kind := sourcecache.RowKindGrants

	store := &blockingReplayStore{
		entered: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	releaseOnce := native_sync.OnceFunc(func() { close(store.release) })
	t.Cleanup(releaseOnce)

	s := &syncer{
		sourceCacheCapability: sourceCacheCapabilityRW("gen-1", "cfg-1"),
		sourceCacheStore:      store,
		state:                 newState(),
		syncType:              connectorstore.SyncTypeFull,
		sourceCacheWarm:       true,
		previousSyncReader: stubPreviousReader{
			entry: sourcecache.Entry{CacheValidator: "v-hit"},
			found: true,
		},
	}
	s.state.RecordSourceCacheHit(kind, scope, "v-hit")

	errs := make(chan error, 2)
	for range 2 {
		go func() {
			ops, err := s.sourceCachePageOps(ctx, kind,
				annotations.New(v2.SourceCacheReplay_builder{ScopeKey: scope, CacheValidator: "v-hit"}.Build()), 0)
			if err != nil {
				errs <- err
				return
			}
			err = ops.beforeUpserts(ctx)
			ops.release() // as the handlers' defer does
			errs <- err
		}()
	}

	// One copy is now mid-flight. Give the second call a generous window
	// to (incorrectly) reach the store as well before releasing the first.
	<-store.entered
	select {
	case <-store.entered:
		t.Fatal("second replay copy entered the store while the first was mid-flight: decide-copy-mark is not atomic per scope")
	case <-time.After(300 * time.Millisecond):
	}
	releaseOnce()

	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	require.Equal(t, int32(1), store.calls.Load(), "exactly one replay copy must land per scope per sync")
	require.True(t, s.state.SourceCacheReplayed(kind, scope))
}

// TestSourceCacheRecordPageParksBehindReplayCopy pins the record-page half
// of the scope lock (re-review N1): a RECORD-only page for a scope must not
// begin its row puts while another action's REPLACEMENT copy for the same
// scope is mid-flight — the copy deletes the scope's rows before copying
// the base, so an interleaved record page's fresh rows would be silently
// wiped (or its validator published over an incomplete scope). The record
// page's beforeUpserts must park on the scope lock until the copy holder
// releases at afterUpserts. Reverting beforeUpserts to early-return before
// the lock for record-only pages fails this test inside the 300ms window.
func TestSourceCacheRecordPageParksBehindReplayCopy(t *testing.T) {
	ctx := context.Background()
	const scope = "grants:team-1"
	kind := sourcecache.RowKindGrants

	store := &blockingReplayStore{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	releaseOnce := native_sync.OnceFunc(func() { close(store.release) })
	t.Cleanup(releaseOnce)

	s := &syncer{
		sourceCacheCapability: sourceCacheCapabilityRW("gen-1", "cfg-1"),
		sourceCacheStore:      store,
		state:                 newState(),
		syncType:              connectorstore.SyncTypeFull,
		sourceCacheWarm:       true,
		previousSyncReader: stubPreviousReader{
			entry: sourcecache.Entry{CacheValidator: "v-hit"},
			found: true,
		},
	}
	s.state.RecordSourceCacheHit(kind, scope, "v-hit")

	replayOps, err := s.sourceCachePageOps(ctx, kind,
		annotations.New(v2.SourceCacheReplay_builder{ScopeKey: scope, CacheValidator: "v-hit"}.Build()), 0)
	require.NoError(t, err)
	copyDone := make(chan error, 1)
	go func() {
		err := replayOps.beforeUpserts(ctx)
		replayOps.release()
		copyDone <- err
	}()
	<-store.entered // the replacement copy is now mid-flight, lock held

	recordOps, err := s.sourceCachePageOps(ctx, kind,
		annotations.New(v2.SourceCacheRecord_builder{ScopeKey: scope, CacheValidator: "v-next"}.Build()), 1)
	require.NoError(t, err)
	recordEntered := make(chan error, 1)
	go func() { recordEntered <- recordOps.beforeUpserts(ctx) }()

	select {
	case <-recordEntered:
		t.Fatal("record page proceeded past beforeUpserts while a replacement copy for its scope was mid-flight: its rows could be wiped by the copy")
	case <-time.After(300 * time.Millisecond):
	}

	releaseOnce()
	require.NoError(t, <-copyDone)
	require.NoError(t, <-recordEntered)
	// afterUpserts must release the lock — a leak would deadlock every
	// later page for the scope, including the action's own retry.
	require.NoError(t, recordOps.afterUpserts(ctx))
	require.True(t, s.sourceCacheScopeLock(kind, scope).TryLock(),
		"the scope lock must be free once the record page's afterUpserts returns")
}

// TestParseSourceCacheCapabilityUnparsable pins the boundary contract for a
// corrupt SourceCacheCapability annotation (coverage-triage cell,
// CO-6b-007): unparsable means NOT DECLARED — the sync runs plain cold, it
// does not error. A capability is an opt-in, so failing to read one must
// degrade to the opted-out behavior, never block the sync.
func TestParseSourceCacheCapabilityUnparsable(t *testing.T) {
	ctx := context.Background()

	corrupt, err := anypb.New(sourceCacheCapabilityRW("gen-1", "cfg-1"))
	require.NoError(t, err)
	// A truncated tag byte cannot parse as any proto message.
	corrupt.Value = []byte{0xff}
	require.Nil(t, parseSourceCacheCapability(ctx, annotations.Annotations{corrupt}),
		"an unparsable capability must read as not declared")

	// Control: the same annotation uncorrupted parses.
	parsed := parseSourceCacheCapability(ctx, annotations.New(sourceCacheCapabilityRW("gen-1", "cfg-1")))
	require.NotNil(t, parsed)
	require.Equal(t, v2.SourceCacheCapability_MODE_READ_WRITE, parsed.GetMode())
}

// countingEntryReader counts LookupSourceCacheEntry consultations so input
// -validation cells can assert the store is never reached.
type countingEntryReader struct {
	stubPreviousReader
	calls atomic.Int32
}

func (r *countingEntryReader) LookupSourceCacheEntry(ctx context.Context, kind sourcecache.RowKind, scopeKey string) (sourcecache.Entry, bool, error) {
	r.calls.Add(1)
	return r.stubPreviousReader.LookupSourceCacheEntry(ctx, kind, scopeKey)
}

// TestWarmLookupInputValidationDegradesToMiss pins the warm lookup's
// input-validation contract (pkg/sourcecache; coverage-triage cells,
// CO-6b-007): invalid connector-supplied arguments and internal read
// failures are MISSES, never connector-call errors — a miss means "fetch
// cold", which is always safe, while an error would fail a page the
// connector could have served fresh. None of these paths may record a hit.
func TestWarmLookupInputValidationDegradesToMiss(t *testing.T) {
	ctx := context.Background()

	newLookup := func(reader sourceCacheEntryReader) (*previousSyncSourceCacheLookup, *int) {
		hits := 0
		return &previousSyncSourceCacheLookup{
			prev: reader,
			onHit: func(sourcecache.RowKind, string, string) {
				hits++
			},
		}, &hits
	}

	t.Run("invalid-row-kind-is-a-miss-without-consulting-the-store", func(t *testing.T) {
		reader := &countingEntryReader{}
		lookup, hits := newLookup(reader)
		_, found, err := lookup.LookupPreviousSourceCache(ctx, sourcecache.RowKind("bogus"), "grants:team-1")
		require.NoError(t, err, "an invalid row kind must not fail the connector's call")
		require.False(t, found)
		require.Zero(t, reader.calls.Load(), "invalid input must not reach the store")
		require.Zero(t, *hits)
	})

	t.Run("invalid-scope-key-is-a-miss-without-consulting-the-store", func(t *testing.T) {
		reader := &countingEntryReader{}
		lookup, hits := newLookup(reader)
		_, found, err := lookup.LookupPreviousSourceCache(ctx, sourcecache.RowKindGrants, "")
		require.NoError(t, err, "an invalid scope key must not fail the connector's call")
		require.False(t, found)
		require.Zero(t, reader.calls.Load())
		require.Zero(t, *hits)
	})

	t.Run("store-read-failure-is-a-miss", func(t *testing.T) {
		reader := &countingEntryReader{stubPreviousReader: stubPreviousReader{entryErr: errors.New("pebble: sstable checksum mismatch")}}
		lookup, hits := newLookup(reader)
		_, found, err := lookup.LookupPreviousSourceCache(ctx, sourcecache.RowKindGrants, "grants:team-1")
		require.NoError(t, err, "an internal read failure must degrade to a miss while fresh fetch is still available")
		require.False(t, found)
		require.Equal(t, int32(1), reader.calls.Load())
		require.Zero(t, *hits)
	})

	t.Run("control-valid-hit-is-reported", func(t *testing.T) {
		reader := &countingEntryReader{stubPreviousReader: stubPreviousReader{entry: sourcecache.Entry{CacheValidator: "v-1"}, found: true}}
		lookup, hits := newLookup(reader)
		entry, found, err := lookup.LookupPreviousSourceCache(ctx, sourcecache.RowKindGrants, "grants:team-1")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "v-1", entry.CacheValidator)
		require.Equal(t, 1, *hits)
	})
}

// TestSourceCachePageOpsStoreWithoutSurface pins the no-surface page
// contract (plan B3; coverage-triage cells, CO-6b-007) on a capable
// connector whose CURRENT store has no source-cache surface: a record is
// ignored with a warn — nothing can be stamped, cold-sync behavior — while
// a replay fails loud and cold, because the connector skipped row
// generation and there is nothing to fall back to.
func TestSourceCachePageOpsStoreWithoutSurface(t *testing.T) {
	ctx := context.Background()
	s := &syncer{
		state:                 newState(),
		syncType:              connectorstore.SyncTypeFull,
		sourceCacheCapability: sourceCacheCapabilityRW("gen-1", "cfg-1"),
		// sourceCacheStore deliberately nil: the current store exposed no
		// dotc1z.SourceCacheStore surface at install time.
	}

	ops, err := s.sourceCachePageOps(ctx, sourcecache.RowKindGrants,
		scRecordAnno("grants:team-1", "v-1"), 1)
	require.NoError(t, err, "a record on a surfaceless store is ignored, not an error")
	require.Nil(t, ops)

	_, err = s.sourceCachePageOps(ctx, sourcecache.RowKindGrants,
		scReplayAnno("grants:team-1", "v-1", false, nil, nil), 0)
	require.ErrorIs(t, err, ErrReplayIntegrity)
	var rie *ReplayIntegrityError
	require.ErrorAs(t, err, &rie)
	require.Equal(t, ReplayVerdictCold, rie.Verdict, "no rows were generated and none can be copied: cold")
}

// compatErrPreviousReader has the entry surface AND the compat surface,
// but every compat read fails.
type compatErrPreviousReader struct{ stubPreviousReader }

func (r compatErrPreviousReader) GetSourceCacheCompat(context.Context) (sourcecache.CompatKey, bool, error) {
	return sourcecache.CompatKey{}, false, errors.New("pebble: compat record read failed")
}

// TestSourceCacheWarmStoreDegradeLadder pins the install-time consume-gate
// degradations for a previous store that is PRESENT but structurally or
// operationally unusable (coverage-triage cells, CO-6b-007): each rung
// degrades to cold with a warn — the sync proceeds, never errors.
func TestSourceCacheWarmStoreDegradeLadder(t *testing.T) {
	ctx := context.Background()
	cells := []struct {
		name   string
		reader connectorstore.Reader
	}{
		{name: "previous-store-without-entry-surface", reader: bareStubPreviousReader{}},
		{name: "previous-store-without-compat-surface", reader: stubPreviousReader{}},
		{name: "compat-read-failure", reader: compatErrPreviousReader{}},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			s := &syncer{
				state:                 newState(),
				syncType:              connectorstore.SyncTypeFull,
				sourceCacheCapability: sourceCacheCapabilityRW("gen-1", "cfg-1"),
				previousSyncReader:    cell.reader,
			}
			reader, warm := s.sourceCacheWarmStore(ctx)
			require.False(t, warm)
			require.Nil(t, reader)
		})
	}
}
