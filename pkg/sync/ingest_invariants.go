package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Ingestion invariants: store-derived definitions of the syncer's
// side effects, evaluated at consuming seams. Design:
// docs/tasks/source-cache-ingestion-invariants.md.
//
// Background. The syncer historically attached side effects (flags,
// scheduled actions, derived rows, validation) to the connector-response
// ingest loop. That was correct while response pages were the only way
// rows entered the store; source-cache replay added an engine-side
// ingestion path and turned every stream-coupled side effect into a
// latent bug. Rather than mirroring each side effect once per ingestion
// path (unmarked coupling, rediscovered by review), each side effect is
// defined as a function of the store and evaluated here:
//
//   - I1 expansion arming: PendingExpansion probe at SyncGrantExpansionOp
//     (lives in parallel_syncer.go — the pattern's prototype).
//   - I2 external-match arming: engine existence-bit probe at
//     SyncExternalResourcesOp start; REPAIRS the flag. Pebble maintains
//     the bit for fresh and replayed rows alike; the response-loop arm
//     remains as the fast path and as SQLite's only mechanism.
//   - I3 grant→resource referential integrity: post-collection prefix
//     skip-scan over the grant keyspace. Dangling ref + an
//     InsertResourceGrants-annotated grant = hard violation (the
//     related-resource machinery lost a row); dangling without the
//     annotation = warning (tolerated connector behavior). Pebble-only
//     (the scan rides the pebble key order); full syncs only.
//   - I4 child-scheduling completeness: every stored resource carrying
//     ChildResourceType must have had its (childType, parent) action
//     scheduled. Check-only; skipped when the resources phase did not
//     run in this process (a zero-row child listing leaves no store
//     evidence that its action ran). Full syncs only. STRICT MODE ONLY:
//     the check costs a full post-collection resource scan and its
//     lenient outcome was only ever a warning, so production (lenient)
//     skips it entirely — tests and the equivalence harness enforce it.
//   - I5 exclusion-group validation: the stored entitlement keyspace is
//     validated post-collection (one default per group, one resource
//     type per group, size cap). This REPLACES the former streaming
//     validation entirely — it is engine-agnostic and covers replayed,
//     fresh, and generated (static) entitlements uniformly.
//   - I6 source-cache scope consistency: every scope present in a
//     by_source_scope index has a manifest entry at seal. Orphaned
//     stamps mean a lost manifest write or a stamp leak — either would
//     poison a FUTURE sync's replay while this sync reads clean.
//     Pebble-only (the only engine with source-cache state); the
//     replay-time counterpart (copied-row count vs the scope index)
//     lives in the engine's ReplaySourceCache* and always hard-fails.
//
// Evidence is monotone (existence bits, set-union scheduling records),
// so parallel workers and mixed stream/replay arrival orders yield
// identical verdicts; checks run at phase-quiesced points, so they are
// deterministic and idempotent under crash/resume.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	stdsync "sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// sideEffectAnnotationCoverage is the enumerated coupling between
// connector annotations that imply syncer side effects and the mechanism
// that guarantees them independent of ingestion path. This map IS the
// audit that review rounds used to re-derive from memory; the
// completeness meta-test (ingest_invariants_test.go) fails when a listed
// annotation loses its mapping, and NEW side-effect-implying annotations
// must be added here with an invariant (or a documented exclusion)
// before they ship.
var sideEffectAnnotationCoverage = map[string]string{
	"c1.connector.v2.GrantExpandable":           "I1: needs_expansion index probe at SyncGrantExpansionOp (parallel_syncer.go)",
	"c1.connector.v2.ExternalResourceMatch":     "I2: engine existence-bit repair at SyncExternalResourcesOp",
	"c1.connector.v2.ExternalResourceMatchAll":  "I2: engine existence-bit repair at SyncExternalResourcesOp",
	"c1.connector.v2.ExternalResourceMatchID":   "I2: engine existence-bit repair at SyncExternalResourcesOp",
	"c1.connector.v2.InsertResourceGrants":      "I3: grant→resource referential check post-collection; replay carries the row copy",
	"c1.connector.v2.ChildResourceType":         "I4: scheduled-set completeness check post-collection; replay carries the scheduling",
	"c1.connector.v2.EntitlementExclusionGroup": "I5: stored-keyspace validation post-collection",
}

// childScheduleSet is the monotone record of scheduled child-resource
// actions, keyed (childTypeID, parentTypeID, parentID). Guarded for
// parallel workers; in-memory only (see the I4 resume caveat).
type childScheduleSet struct {
	mu stdsync.Mutex
	m  map[string]struct{}
}

func childScheduleKey(childTypeID, parentTypeID, parentID string) string {
	return childTypeID + "\x00" + parentTypeID + "\x00" + parentID
}

// recordIfNew records the pair and reports whether it was NEW this sync.
// The set doubles as the scheduling dedupe: a parent can be discovered by
// several ingestion paths in one sync (replay copies its stored row AND a
// delta overlay re-emits it changed), and its children only need one
// SyncResourcesOp per pair — the second discovery would double the
// child-listing work. Atomic check-and-set so parallel workers can't both
// see "new".
func (c *childScheduleSet) recordIfNew(childTypeID, parentTypeID, parentID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = make(map[string]struct{})
	}
	key := childScheduleKey(childTypeID, parentTypeID, parentID)
	if _, ok := c.m[key]; ok {
		return false
	}
	c.m[key] = struct{}{}
	return true
}

func (c *childScheduleSet) has(childTypeID, parentTypeID, parentID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.m[childScheduleKey(childTypeID, parentTypeID, parentID)]
	return ok
}

// repairExternalMatchFlag is invariant I2's repair: at the consuming seam
// (SyncExternalResourcesOp start), reconcile the in-memory flag with the
// engine's existence bit. Sound because the gated computation is a pure
// function of the store: running match processing when the store holds
// match-annotated grants is required; the flag is only an optimization
// that lets other syncs skip the scan.
//
// A fact-read error FAILS the sync: proceeding with an unset flag would
// silently skip match processing for replay-armed grants (the exact bug
// class I2 exists to prevent), and an engine-meta read error at this
// point means the store is unhealthy anyway.
func (s *syncer) repairExternalMatchFlag(ctx context.Context) error {
	if s.state.HasExternalResourcesGrants() {
		return nil
	}
	facts, ok := s.store.(dotc1z.IngestFactStore)
	if !ok {
		return nil // SQLite: the response-loop arm is the mechanism.
	}
	has, err := facts.HasExternalMatchGrants(ctx)
	if err != nil {
		return fmt.Errorf("ingest invariant I2: reading external-match fact: %w", err)
	}
	if has {
		ctxzap.Extract(ctx).Debug("ingest invariant I2: arming external-match processing from store-derived fact")
		s.state.SetHasExternalResourcesGrants()
	}
	return nil
}

// ErrReplayIntegrity classifies sync failures where source-cache REPLAY
// state is implicated: a replay copy that lost or dropped rows, or
// store-derived evidence that a replayed artifact is inconsistent. Since
// replay is a pure optimization, every such failure has a safe
// remediation the runner can apply automatically — discard the previous
// sync source and re-run the sync cold (see the runners' cold-retry).
//
// The retry doubles as the attribution experiment: a clean cold run
// means replay was at fault (and the sync self-healed); a cold run that
// STILL violates the invariant is re-attributed to connector data — the
// cold-path failure deliberately does NOT carry this sentinel, so a
// recurring connector bug fails plainly (and cheaply) every sync instead
// of looping through wasted warm-attempt-plus-retry cycles under an SDK
// label. Also deliberately NOT applied to connector-contract violations
// (e.g. a replay requested for a scope that was never looked up): a cold
// retry would mask those without fixing the connector.
var ErrReplayIntegrity = errors.New("source-cache replay integrity failure")

// relatedResourceRepairer is the optional store capability behind I3's
// repair arm (pebble implements it).
type relatedResourceRepairer interface {
	RepairRelatedResource(ctx context.Context, prev connectorstore.Reader, resourceTypeID, resourceID string) (bool, error)
}

// checkGrantResourceReferences is invariant I3: every distinct
// entitlement resource referenced by a grant must exist as a resource
// row. One seek per distinct resource (the grant keyspace leads with the
// entitlement resource), value reads only for dangling refs.
//
// The annotated arm (dangling ref + InsertResourceGrants) is
// REPAIR-flavored in lenient mode: those rows are the syncer's own
// establishment guarantee (no ListResources call ever returns them), and
// the previous sync's row — still open on s.sourceCache.prevReader — is
// a full-fidelity copy source, exactly the copy replay itself performs.
// Strict mode (tests, the harness) skips repair and hard-fails so the
// underlying machinery bug is named rather than papered over. An
// unrepairable annotated ref fails the sync under ErrReplayIntegrity.
func (s *syncer) checkGrantResourceReferences(ctx context.Context) error {
	if s.syncType != connectorstore.SyncTypeFull {
		return nil
	}
	facts, ok := s.store.(dotc1z.IngestFactStore)
	if !ok {
		return nil // engine without the inspection surface
	}
	l := ctxzap.Extract(ctx)
	return facts.ForEachDistinctGrantEntitlementResource(ctx, func(rt, rid string) error {
		exists, err := facts.HasResourceRecord(ctx, rt, rid)
		if err != nil {
			return fmt.Errorf("ingest invariant I3: probing resource %s/%s: %w", rt, rid, err)
		}
		if exists {
			return nil
		}
		annotated, err := facts.GrantsForEntResourceCarryInsertFact(ctx, rt, rid)
		if err != nil {
			return fmt.Errorf("ingest invariant I3: probing grant annotations for %s/%s: %w", rt, rid, err)
		}
		if annotated {
			if s.sourceCache.prev == nil {
				// COLD sync: replay cannot be the culprit — the cold
				// path materializes these rows unconditionally in the
				// same page handling, so this state means the connector
				// destroyed or never supplied the row (e.g. a tombstone
				// naming a grant-inserted resource). Deliberately NOT
				// ErrReplayIntegrity: a cold retry can't fix connector
				// data, and mislabeling would send operators hunting an
				// SDK replay bug. This is also the terminal verdict of
				// the runners' cold retry — a warm failure retried cold
				// that still fails lands here, correctly re-attributed.
				return fmt.Errorf(
					"ingest invariant I3 violated on a COLD sync: grants reference resource %s/%s via InsertResourceGrants "+
						"but no resource row exists — the connector's own data is inconsistent "+
						"(check for tombstones deleting grant-inserted resources)",
					rt, rid)
			}
			if !s.strictIngestionInvariants {
				if repaired := s.repairRelatedResource(ctx, rt, rid); repaired {
					l.Warn("ingest invariant I3: REPAIRED a lost grant-inserted resource by copying the previous sync's row; "+
						"the replay path failed to carry it — investigate before trusting warm syncs",
						zap.String("resource_type_id", rt),
						zap.String("resource_id", rid),
					)
					return nil
				}
			}
			// WARM sync: replay is implicated (possibly alongside a
			// connector bug — the runners' cold retry disambiguates:
			// a clean cold run means replay was at fault and the sync
			// self-healed; a cold failure lands in the branch above.
			return fmt.Errorf(
				"ingest invariant I3 violated: grants reference resource %s/%s via InsertResourceGrants but no resource row exists "+
					"(related-resource insertion was lost and could not be repaired from the previous sync): %w",
				rt, rid, ErrReplayIntegrity)
		}
		// Tolerated today: connectors emitting grants for unlisted
		// resources without InsertResourceGrants. Visible, not fatal.
		l.Warn("ingest invariant I3: grants reference a resource that was never synced",
			zap.String("resource_type_id", rt),
			zap.String("resource_id", rid),
		)
		return nil
	})
}

// repairRelatedResource attempts I3's repair: copy the missing
// grant-inserted resource row from the previous sync's store. Best
// effort — a false return falls through to the hard failure.
func (s *syncer) repairRelatedResource(ctx context.Context, rt, rid string) bool {
	repairer, ok := s.store.(relatedResourceRepairer)
	if !ok || s.sourceCache.prevReader == nil {
		return false
	}
	repaired, err := repairer.RepairRelatedResource(ctx, s.sourceCache.prevReader, rt, rid)
	if err != nil {
		ctxzap.Extract(ctx).Warn("ingest invariant I3: related-resource repair failed",
			zap.String("resource_type_id", rt),
			zap.String("resource_id", rid),
			zap.Error(err),
		)
		return false
	}
	return repaired
}

// checkChildScheduling is invariant I4: every stored resource carrying a
// ChildResourceType annotation (and passing the resource-type filter)
// must have had its child action scheduled. Check-only — scheduling
// cannot be derived after the fact (an executed child action that
// returned zero rows leaves no store evidence).
func (s *syncer) checkChildScheduling(ctx context.Context) error {
	if !s.strictIngestionInvariants {
		// Strict mode only: the check costs a full post-collection
		// resource scan (value decode + annotation walk per row) and in
		// lenient mode a violation was only ever a warning — a bad
		// trade at whale scale. Tests and the harness run strict and
		// enforce it there.
		return nil
	}
	if s.syncType != connectorstore.SyncTypeFull || !s.resourcesPhaseRanHere {
		// Resumed past the resources phase: the scheduled set from the
		// prior process is gone; the predicate is unverifiable.
		return nil
	}
	syncResourceTypeMap := make(map[string]bool, len(s.syncResourceTypes))
	for _, rt := range s.syncResourceTypes {
		syncResourceTypeMap[rt] = true
	}

	var violations []string
	pageToken := ""
	for {
		resp, err := s.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			PageToken:    pageToken,
			ActiveSyncId: s.getActiveSyncID(),
		}.Build())
		if err != nil {
			return fmt.Errorf("ingest invariant I4: listing resources: %w", err)
		}
		for _, r := range resp.GetList() {
			for _, a := range r.GetAnnotations() {
				if !a.MessageIs((*v2.ChildResourceType)(nil)) {
					continue
				}
				crt := &v2.ChildResourceType{}
				if err := a.UnmarshalTo(crt); err != nil {
					return fmt.Errorf("ingest invariant I4: parsing ChildResourceType on %s/%s: %w",
						r.GetId().GetResourceType(), r.GetId().GetResource(), err)
				}
				childType := crt.GetResourceTypeId()
				if childType == "" {
					continue
				}
				if len(s.syncResourceTypes) > 0 && !syncResourceTypeMap[childType] {
					continue
				}
				if !s.childSchedule.has(childType, r.GetId().GetResourceType(), r.GetId().GetResource()) {
					violations = append(violations, fmt.Sprintf("%s under %s/%s",
						childType, r.GetId().GetResourceType(), r.GetId().GetResource()))
				}
			}
		}
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	if len(violations) > 0 {
		// Sorted so the error text is byte-stable regardless of store
		// iteration order (the verdict already is).
		sort.Strings(violations)
		return fmt.Errorf(
			"ingest invariant I4 violated: %d child resource sync(s) were never scheduled: %v",
			len(violations), violations)
	}
	return nil
}

// exclusionGroupTracker is the pure validation core behind invariant I5:
// one default per group, one resource type per group, at most
// maxEntitlementsPerExclusionGroup members. Error text intentionally
// matches the retired streaming validator. Recording the same
// entitlement twice is idempotent for the default check but counts twice
// toward the cap — callers feed each stored entitlement exactly once
// (the store keyspace holds one row per entitlement id).
type exclusionGroupTracker struct {
	groups map[string]*exclusionGroupState
}

type exclusionGroupState struct {
	resourceTypeID string
	defaultEntID   string
	count          uint32
}

func (t *exclusionGroupTracker) record(ent *v2.Entitlement) error {
	eg := &v2.EntitlementExclusionGroup{}
	annos := annotations.Annotations(ent.GetAnnotations())
	ok, err := annos.Pick(eg)
	if err != nil {
		return fmt.Errorf("parsing exclusion group on %q: %w", ent.GetId(), err)
	}
	if !ok || eg.GetExclusionGroupId() == "" {
		return nil
	}
	groupID := eg.GetExclusionGroupId()
	if t.groups == nil {
		t.groups = map[string]*exclusionGroupState{}
	}
	st := t.groups[groupID]
	if st == nil {
		st = &exclusionGroupState{}
		t.groups[groupID] = st
	}
	rt := ent.GetResource().GetId().GetResourceType()
	if st.resourceTypeID == "" {
		st.resourceTypeID = rt
	} else if st.resourceTypeID != rt {
		return fmt.Errorf("exclusion group %q is used on multiple resource types (%q and %q); "+
			"exclusion groups may span resources but must be scoped to a single resource type",
			groupID, st.resourceTypeID, rt)
	}
	if eg.GetIsDefault() {
		if st.defaultEntID != "" && st.defaultEntID != ent.GetId() {
			return fmt.Errorf("exclusion group %q has multiple default entitlements (%q and %q); "+
				"at most one entitlement per exclusion group may set is_default=true",
				groupID, st.defaultEntID, ent.GetId())
		}
		st.defaultEntID = ent.GetId()
	}
	st.count++
	if st.count > maxEntitlementsPerExclusionGroup {
		return fmt.Errorf("exclusion group %q has too many entitlements (%d); "+
			"at most %d entitlements are allowed per exclusion group",
			groupID, st.count, maxEntitlementsPerExclusionGroup)
	}
	return nil
}

// validateStoredExclusionGroups is invariant I5: exclusion-group
// invariants validated over the STORED entitlement keyspace. This is the
// sole exclusion-group validation: it covers fresh, replayed, and
// generated (static) entitlements uniformly, on every engine, regardless
// of how rows arrived.
func (s *syncer) validateStoredExclusionGroups(ctx context.Context) error {
	tracker := &exclusionGroupTracker{}
	pageToken := ""
	for {
		resp, err := s.store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
			PageToken:    pageToken,
			ActiveSyncId: s.getActiveSyncID(),
		}.Build())
		if err != nil {
			return fmt.Errorf("ingest invariant I5: listing entitlements: %w", err)
		}
		for _, ent := range resp.GetList() {
			if err := tracker.record(ent); err != nil {
				return err
			}
		}
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	return nil
}

// checkSourceCacheScopeConsistency is invariant I6: at the sealed sync's
// quiesce point, every scope present in a by_source_scope index must have
// a manifest entry. Post-processing only re-stamps rows with scopes that
// already completed (external-match transformed grants and expansion
// writes inherit the SOURCE scope), so an orphan at this seam is always a
// lost manifest write or a stamp leak — and the damage is deferred: THIS
// sync reads clean, the NEXT sync replays from the damaged state.
func (s *syncer) checkSourceCacheScopeConsistency(ctx context.Context) error {
	sc, ok := s.store.(dotc1z.SourceCacheStore)
	if !ok {
		return nil // engine without source-cache state
	}
	orphans, err := sc.SourceCacheOrphanScopes(ctx)
	if err != nil {
		return fmt.Errorf("ingest invariant I6: scanning scope indexes: %w", err)
	}
	if len(orphans) == 0 {
		return nil
	}
	err = fmt.Errorf(
		"ingest invariant I6 violated: scope index entries exist with no manifest entry (lost manifest write or stamp leak): %v",
		orphans)
	if s.strictIngestionInvariants {
		return err
	}
	ctxzap.Extract(ctx).Warn("ingest invariant I6 violated (lenient mode)", zap.Error(err))
	return nil
}

// runIngestionInvariants evaluates the post-collection (check-flavored)
// invariants: I5, I6, I4, I3, in that order (cheapest and most
// behavior-critical first). Runs after the action loop drains and before
// EndSync, so every ingestion path — stream, replay, resume re-runs,
// expansion, external-match processing — has finished writing.
// Idempotent: a resumed sync that re-reaches this point re-evaluates
// with the same verdict.
func (s *syncer) runIngestionInvariants(ctx context.Context) error {
	if err := s.validateStoredExclusionGroups(ctx); err != nil {
		return err
	}
	if err := s.checkSourceCacheScopeConsistency(ctx); err != nil {
		return err
	}
	if err := s.checkChildScheduling(ctx); err != nil {
		return err
	}
	return s.checkGrantResourceReferences(ctx)
}
