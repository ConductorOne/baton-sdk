package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Ingestion invariants: store-derived definitions of the syncer's
// side effects, evaluated at a post-collection seam.
//
// Background. The syncer historically attached side effects (flags,
// scheduled actions, derived rows, validation) to the connector-response
// ingest loop. That is correct only while response pages are the only
// way rows enter the store; any second ingestion path (source-cache
// replay, bulk import, compaction merges) turns every stream-coupled
// side effect into a latent bug. Rather than mirroring each side effect
// once per ingestion path (unmarked coupling, rediscovered by review),
// each side effect is defined as a function of the store and evaluated
// here, after every writer has finished:
//
//   - I3 grant→resource referential integrity: post-collection prefix
//     skip-scan over the grant keyspace. Dangling ref + an
//     InsertResourceGrants-annotated grant = hard violation (the
//     related-resource machinery lost a row); dangling without the
//     annotation = aggregated warning (tolerated connector behavior).
//     Pebble-only (the scan rides the pebble key order); full syncs only.
//   - I4 child-scheduling completeness: every stored resource carrying
//     ChildResourceType must have had its (childType, parent) action
//     scheduled. Check-only; skipped when the resources phase did not
//     run in this process (a zero-row child listing leaves no store
//     evidence that its action ran). Full syncs only. FAIL-FAST ONLY:
//     the check costs a full post-collection resource scan and its
//     default outcome was only ever a warning, so production (default)
//     skips it entirely — tests and equivalence harnesses enforce it.
//   - I5 exclusion-group validation: the stored entitlement keyspace is
//     validated post-collection (one default per group, one resource
//     type per group, size cap). This REPLACES the former streaming
//     validation entirely — it is engine-agnostic and covers fresh and
//     generated (static) entitlements uniformly, however rows arrived.
//     Runs on every sync type: a conflict needs both rows present, so
//     partial-store evidence is always valid evidence.
//   - I7 entitlement→resource referential integrity: post-collection
//     prefix skip-scan over the entitlement keyspace (one seek per
//     distinct resource). Per-resource entitlement scopes cannot dangle
//     by construction (the scope is only visited when the resource was
//     synced); the exposure is TYPE-scoped listings and magic-id
//     construction bugs in connectors.
//   - I8 grant→entitlement referential integrity: post-collection
//     skip-scan over the grant keyspace at entitlement-identity
//     granularity plus one point probe per distinct entitlement.
//     Exposure: dominantly, connector magic-id bugs — annotations and
//     grants naming entitlement ids the entitlement enumeration never
//     produced (production expansion drops millions of edges per week
//     to this class). EXEMPT: grants under InsertResourceGrants pages
//     (the machinery inserts the resource; no listing returns an
//     entitlement for it — an established shape).
//   - I9 grant→principal referential integrity: dangling-principal scan
//     over the by_principal index (one seek + probe per distinct
//     principal; a pending deferred index build is forced first, which
//     just moves EndSync's own pass earlier). EXEMPT: grants carrying
//     ExternalResourceMatch* annotations — unprocessed match carriers
//     are evidence that no external resource file was configured, not
//     bad data (the match op deletes carriers when it runs).
//
// I7/I8/I9 verdicts are aggregated warnings in default mode, split by
// whether the referent's resource TYPE was synced:
//
//   - Never-synced-type danglings (a CONFIG gap: typically a type added
//     to the connector after the instance was configured and left
//     disabled by default). The platform's ingestion provably discards
//     such rows anyway; a later change may drop them at seal, with this
//     warning's aggregates as the fleet evidence for that flip.
//   - Synced-type danglings (the id was constructed for a row the
//     enumeration never produced — a magic-id BUG, or a connector
//     emitting grants for rows it never listed). Attributed in the same
//     aggregate; never a per-row log line (the expansion path's
//     per-edge warnings produce millions of lines a week in production).
//
// FAIL-FAST mode (tests, equivalence harnesses) hard-fails every
// dangling so engineered violations are named. See the verdict table
// (ingestInvariants) for the per-invariant policy matrix; the table is
// the single dispatcher input, so a policy hole is a missing row, not a
// missing if-branch.
//
// Evidence is monotone (existence probes, set-union scheduling
// records), so parallel workers and any arrival order yield identical
// verdicts; checks run at a phase-quiesced point, so they are
// deterministic and idempotent under crash/resume.
//
// Not yet here (extraction staging from the replay branch): I1/I2
// store-derived fact probes and the warm/cold ErrReplayIntegrity
// verdict ladder arrive with source-cache replay; the never-synced-type
// drop arms (and their aggregated DROPPED reporting) arrive with the
// dangling-reference drop policy.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	stdsync "sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	c1zpb "github.com/conductorone/baton-sdk/pb/c1/c1z/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// ErrIngestInvariantViolated classifies DATA VERDICTS — the store's
// content violates an invariant — as distinct from the pass's own IO
// failures (listing errors, probe errors, cancellation). A verdict is
// deterministic on an immutable dataset: retrying it re-fails forever,
// so runners map it to their non-retryable failure class
// (pkg/tasks/c1api wraps it with ErrTaskNonRetryable), while IO
// failures stay retryable. Test with errors.Is.
var ErrIngestInvariantViolated = errors.New("ingest invariant violated")

// invariantVerdictError carries the sentinel WITHOUT altering the
// verdict's message (operator-facing text and the error-string
// assertions in the suites stay byte-identical).
type invariantVerdictError struct{ err error }

func (e *invariantVerdictError) Error() string { return e.err.Error() }

func (e *invariantVerdictError) Unwrap() []error {
	return []error{e.err, ErrIngestInvariantViolated}
}

// invariantVerdict marks err as a data verdict.
func invariantVerdict(err error) error { return &invariantVerdictError{err: err} }

// sideEffectAnnotationCoverage is the enumerated coupling between
// connector annotations that imply syncer side effects and the mechanism
// that guarantees them independent of ingestion path. This map IS the
// audit that review rounds used to re-derive from memory; the
// completeness meta-test (ingest_invariants_test.go) fails when a listed
// annotation loses its mapping, and NEW side-effect-implying annotations
// must be added here with an invariant (or a documented exclusion)
// before they ship.
//
// I1 and I2 are registered against today's stream-coupled mechanisms:
// they are correct while the response loop is the only ingestion path,
// and are superseded by store-derived probes when source-cache replay
// adds a second one (see ingestInvariantExclusions in the meta-test).
// When the type-scoped listing annotations (TypeScopedEntitlements,
// TypeScopedGrants) land, they register here against I7 and I8
// respectively — type-granularity scopes are exactly the shapes those
// referential checks exist for.
var sideEffectAnnotationCoverage = map[string]string{
	"c1.connector.v2.GrantExpandable":           "I1: response-loop expansion arming (SetNeedsExpansion) + needs_expansion column persistence; store-derived probe arrives with replay",
	"c1.connector.v2.ExternalResourceMatch":     "I2: response-loop match arming (SetHasExternalResourcesGrants); store-derived existence-bit repair arrives with replay",
	"c1.connector.v2.ExternalResourceMatchAll":  "I2: response-loop match arming (SetHasExternalResourcesGrants); store-derived existence-bit repair arrives with replay",
	"c1.connector.v2.ExternalResourceMatchID":   "I2: response-loop match arming (SetHasExternalResourcesGrants); store-derived existence-bit repair arrives with replay",
	"c1.connector.v2.InsertResourceGrants":      "I3: grant→resource referential check post-collection",
	"c1.connector.v2.ChildResourceType":         "I4: scheduled-set completeness check post-collection",
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
// The set doubles as the scheduling dedupe: a parent can be discovered
// more than once in one sync, and its children only need one
// SyncResourcesOp per pair — the second discovery would double the
// child-listing work. Atomic check-and-set so parallel workers can't
// both see "new".
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

// IngestInvariantsPolicy parameterizes one run of the ingestion
// invariant pass. The exported fields are the caller contract (the
// syncer today; a store-producing pipeline like the compactor's expand
// pass can run the same pass without a syncer). The unexported fields
// carry syncer-private evidence (the in-memory I4 schedule set) and the
// test-only halt hook; external callers leave them zero and the
// affected invariants degrade per their gates.
type IngestInvariantsPolicy struct {
	// ActiveSyncID scopes every store read to the sync under judgment.
	ActiveSyncID string
	// SyncType gates the full-keyspace invariants: store-derived
	// referential verdicts are only evaluable over a COMPLETE keyspace,
	// so I3/I4/I7/I8/I9 run on full syncs only. I5 runs on every sync
	// type (a conflict requires both rows present — valid evidence on a
	// partial store).
	SyncType connectorstore.SyncType
	// FailFast promotes every invariant verdict to a hard,
	// plainly-attributed failure: tolerated warns fail, and I4 (skipped
	// entirely in default mode) runs. Tests and equivalence harnesses
	// set it.
	FailFast bool
	// CompactionMerge marks a pass over a PRE-SEALED artifact this
	// process did not collect — the compactor's keep-newer merge
	// (whose key union manufactures shapes no single input contained:
	// dangling references, stranded InsertResourceGrants rows,
	// exclusion-group conflicts) and rollback-expansion's replay
	// (whose inputs include such merged artifacts). Verdicts that
	// would blame the connector are attributed to the merge and hard
	// arms soften to aggregated warnings: the hard arms exist to stop
	// a NEW collection from sealing bad data, not to re-adjudicate an
	// artifact that already sealed. A normal connector sync must never
	// set this.
	CompactionMerge bool

	// I4 evidence: only a process that ran the resources phase can
	// supply the in-memory scheduled set, so only the syncer sets these.
	childSchedule     *childScheduleSet
	resourcesPhaseRan bool
	syncResourceTypes []string

	// halt, when non-nil, fires at named seams of the pass (see
	// ingestInvariantHaltStages); returning an error fails the sync at
	// exactly that boundary. Test-only.
	halt func(stage string) error
}

// ingestInvariantsPass is the per-run state shared by the checks: the
// store handles and the policy.
type ingestInvariantsPass struct {
	store connectorstore.Reader
	// facts is the optional pebble inspection surface; nil degrades
	// every requiresStore invariant (SQLite artifacts are never
	// scanned).
	facts dotc1z.IngestInvariantStore
	p     *IngestInvariantsPolicy
}

func (pass *ingestInvariantsPass) haltAt(stage string) error {
	if pass.p.halt == nil {
		return nil
	}
	return pass.p.halt(stage)
}

// ingestInvariant is one row of the verdict table: the policy matrix
// entry AND the dispatch entry for a single invariant. The dispatcher
// consumes the table in order, so gating lives here once — a
// policy hole is a missing row or a wrong field, both of which the
// completeness meta-test (ingest_invariants_test.go) and the fail-fast
// suite pin — instead of an if-branch inside a check body.
type ingestInvariant struct {
	// id is the invariant's name in docs, log lines, and the
	// sideEffectAnnotationCoverage registry ("I5").
	id string
	// runsOnAllSyncTypes: false gates the check to full syncs (complete
	// keyspace); true runs it on partial syncs too.
	runsOnAllSyncTypes bool
	// requiresStore: the check needs the pebble inspection surface
	// (dotc1z.IngestInvariantStore) and degrades to a no-op without it.
	requiresStore bool
	// failFastOnly: the check ONLY runs under FailFast (I4's
	// full-resource-scan cost is a bad trade for a default-mode
	// warning).
	failFastOnly bool
	// failFastPromotes: under FailFast the check's tolerated
	// warn-verdicts become hard failures. Enforced by the fail-fast
	// test suite; documented here so the matrix is one artifact.
	failFastPromotes bool
	// ridesReplayLadder: the check's FAIL-class verdicts wrap
	// ErrReplayIntegrity on warm syncs for the runners' cold-retry
	// ladder. Always false until source-cache replay lands; the
	// meta-test pins that so the field cannot silently pre-enable.
	ridesReplayLadder bool
	// haltStage, when non-empty, names the test-only halt seam fired
	// AFTER this invariant completes.
	haltStage string
	// check is a method expression on the pass (receiver-first calling
	// convention).
	check func(pass *ingestInvariantsPass, ctx context.Context) error
}

// ingestInvariants is the verdict table, in evaluation order: the
// hard-fail class (I5) runs first — behavior-criticality, not cost,
// orders the table (I5's full entitlement listing is mechanically the
// most expensive read here; it goes first so a seal-blocking verdict
// surfaces before the cheaper warn-class scans spend their seeks).
// The relative order of the
// referential family (I7, I3, I8, I9) is load-bearing for the future
// drop arms (I7 dropping dangling entitlements orphans their grants,
// which I8 then catches; I9 runs last over whatever grants survive) —
// keep it even while the checks are read-only so the drop flip cannot
// reorder verdicts.
var ingestInvariants = []ingestInvariant{
	{
		id:                 "I5",
		runsOnAllSyncTypes: true,
		// Hard failure on connector-produced stores; the compaction
		// expand pass aggregates merge-manufactured conflicts into a
		// warning, which fail-fast promotes back to a failure.
		failFastPromotes: true,
		haltStage:        "invariants-I5-complete",
		check:            (*ingestInvariantsPass).checkStoredExclusionGroups,
	},
	{
		id:               "I4",
		failFastOnly:     true,
		failFastPromotes: true, // the whole check exists only under fail-fast
		check:            (*ingestInvariantsPass).checkChildScheduling,
	},
	{
		id:               "I7",
		requiresStore:    true,
		failFastPromotes: true,
		haltStage:        "invariants-I7-complete",
		check:            (*ingestInvariantsPass).checkEntitlementResourceReferences,
	},
	{
		id:               "I3",
		requiresStore:    true,
		failFastPromotes: true,
		haltStage:        "invariants-I3-complete",
		check:            (*ingestInvariantsPass).checkGrantResourceReferences,
	},
	{
		id:               "I8",
		requiresStore:    true,
		failFastPromotes: true,
		haltStage:        "invariants-I8-complete",
		check:            (*ingestInvariantsPass).checkGrantEntitlementReferences,
	},
	{
		id:               "I9",
		requiresStore:    true,
		failFastPromotes: true,
		check:            (*ingestInvariantsPass).checkGrantPrincipalReferences,
	},
}

// haltStageI9IndexesEnsured fires INSIDE I9, between the forced
// deferred-index build (marker durably cleared) and the dangling scan:
// a crash there must resume without EndSync repeating the O(grants)
// rebuild and with I9 re-scanning cleanly.
const haltStageI9IndexesEnsured = "invariants-I9-indexes-ensured"

// haltStageInvariantsComplete fires at the syncer seam AFTER the whole
// pass, before the checkpoint/EndSync: a resumed sync re-runs the
// entire pass over the same state, so every check must be idempotent.
const haltStageInvariantsComplete = "ingest-invariants-complete"

// ingestInvariantHaltStages enumerates every halt seam the pass can
// fire, in firing order — the halt sweep iterates this list, so a new
// stage is swept by construction, and the meta-test pins
// uniqueness/nonemptiness.
func ingestInvariantHaltStages() []string {
	stages := make([]string, 0, len(ingestInvariants)+2)
	for _, inv := range ingestInvariants {
		if inv.id == "I9" {
			stages = append(stages, haltStageI9IndexesEnsured)
		}
		if inv.haltStage != "" {
			stages = append(stages, inv.haltStage)
		}
	}
	return append(stages, haltStageInvariantsComplete)
}

// RunIngestInvariants evaluates the post-collection ingestion
// invariants over store, per the verdict table (ingestInvariants) and
// policy. Callers run it at a quiesced point — after every ingestion
// path has finished writing and before the sync is sealed — so a
// violating store is never published as complete. Idempotent: a
// resumed run re-evaluates with the same verdicts.
//
// The syncer is one caller (runIngestionInvariants); the pass is a
// store-level function so store-producing pipelines without a syncer
// (the compactor's expand pass) can enforce the same contract.
func RunIngestInvariants(ctx context.Context, store connectorstore.Reader, policy IngestInvariantsPolicy) error {
	if store == nil {
		return fmt.Errorf("ingest invariants: store is required")
	}
	// Fail closed on a zero SyncType: connectorstore's zero value is
	// SyncTypeAny (""), which is not SyncTypeFull — a caller omitting
	// the field would silently skip every full-keyspace invariant and
	// read a green pass that validated almost nothing.
	if policy.SyncType == "" {
		return fmt.Errorf("ingest invariants: policy.SyncType is required (the zero value would silently skip the full-keyspace invariants; pass the sync's actual type)")
	}
	pass := &ingestInvariantsPass{store: store, p: &policy}
	if facts, ok := store.(dotc1z.IngestInvariantStore); ok {
		pass.facts = facts
	}
	var skippedNoStore []string
	for i := range ingestInvariants {
		inv := &ingestInvariants[i]
		if !inv.runsOnAllSyncTypes && policy.SyncType != connectorstore.SyncTypeFull {
			continue
		}
		if inv.requiresStore && pass.facts == nil {
			// Engine without the inspection surface (SQLite): the
			// referential invariants degrade by design, but the
			// downgrade must be visible — a sealed artifact that was
			// never referentially validated should not be
			// indistinguishable from one that passed.
			skippedNoStore = append(skippedNoStore, inv.id)
			continue
		}
		if inv.failFastOnly && !policy.FailFast {
			continue
		}
		if err := inv.check(pass, ctx); err != nil {
			return err
		}
		if inv.haltStage != "" {
			if err := pass.haltAt(inv.haltStage); err != nil {
				return err
			}
		}
	}
	if len(skippedNoStore) > 0 {
		ctxzap.Extract(ctx).Info("ingest invariants: store engine lacks the inspection surface; referential invariants were not evaluated",
			zap.Strings("skipped_invariants", skippedNoStore),
		)
	}
	return nil
}

// runIngestionInvariants is the syncer's call into the pass: policy
// from syncer state, evidence from the in-memory schedule set, halt
// hook from the test seam.
func (s *syncer) runIngestionInvariants(ctx context.Context) error {
	policy := IngestInvariantsPolicy{
		ActiveSyncID:      s.getActiveSyncID(),
		SyncType:          s.syncType,
		FailFast:          s.failFastInvariants,
		CompactionMerge:   s.compactionMergedStore,
		childSchedule:     &s.childSchedule,
		resourcesPhaseRan: s.resourcesPhaseRanHere,
		syncResourceTypes: s.syncResourceTypes,
	}
	if s.testIngestHaltHook != nil {
		policy.halt = s.testIngestHaltHook
	}
	return RunIngestInvariants(ctx, s.store, policy)
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

// exclusionGroupViolation is the typed verdict record returns: it
// carries the offending group id so the warn-only aggregation can
// count LOGICAL conflicts (distinct groups) instead of violating rows —
// an oversized group would otherwise report one "conflict" per excess
// row and crowd every example slot with near-identical strings.
type exclusionGroupViolation struct {
	groupID string
	err     error
}

func (v *exclusionGroupViolation) Error() string { return v.err.Error() }
func (v *exclusionGroupViolation) Unwrap() error { return v.err }

func (t *exclusionGroupTracker) record(ent *v2.Entitlement) error {
	// Every EntitlementExclusionGroup annotation is JUDGED, not just
	// the first (Pick-first would let a second annotation carry an
	// unseen is_default or a cross-type membership — a hostile or
	// malformed connector shape, but one the stored keyspace
	// faithfully holds and the validator must therefore see). The
	// member CAP, however, counts DISTINCT ENTITLEMENTS per group —
	// exactly what its error message claims — so duplicate annotations
	// naming the same group on one entitlement validate their rules
	// without inflating the count (one entitlement with 51 copies of
	// the same annotation is degenerate data, not a 51-member group).
	var counted map[string]bool
	for _, a := range ent.GetAnnotations() {
		if !a.MessageIs((*v2.EntitlementExclusionGroup)(nil)) {
			continue
		}
		eg := &v2.EntitlementExclusionGroup{}
		if err := a.UnmarshalTo(eg); err != nil {
			return fmt.Errorf("parsing exclusion group on %q: %w", ent.GetId(), err)
		}
		groupID := eg.GetExclusionGroupId()
		if groupID == "" {
			continue
		}
		if counted == nil {
			counted = map[string]bool{}
		}
		if err := t.recordMembership(ent, eg, counted[groupID]); err != nil {
			return err
		}
		counted[groupID] = true
	}
	return nil
}

// recordMembership applies the group rules to one (entitlement,
// exclusion-group annotation) membership. alreadyCounted suppresses
// the cap increment for repeat annotations of the same group on the
// same entitlement; the type and default rules always apply.
func (t *exclusionGroupTracker) recordMembership(ent *v2.Entitlement, eg *v2.EntitlementExclusionGroup, alreadyCounted bool) error {
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
		return &exclusionGroupViolation{groupID: groupID, err: fmt.Errorf(
			"exclusion group %q is used on multiple resource types (%q and %q); "+
				"exclusion groups may span resources but must be scoped to a single resource type",
			groupID, st.resourceTypeID, rt)}
	}
	if eg.GetIsDefault() {
		if st.defaultEntID != "" && st.defaultEntID != ent.GetId() {
			return &exclusionGroupViolation{groupID: groupID, err: fmt.Errorf(
				"exclusion group %q has multiple default entitlements (%q and %q); "+
					"at most one entitlement per exclusion group may set is_default=true",
				groupID, st.defaultEntID, ent.GetId())}
		}
		st.defaultEntID = ent.GetId()
	}
	if alreadyCounted {
		return nil
	}
	st.count++
	if st.count > maxEntitlementsPerExclusionGroup {
		return &exclusionGroupViolation{groupID: groupID, err: fmt.Errorf(
			"exclusion group %q has too many entitlements (%d); "+
				"at most %d entitlements are allowed per exclusion group",
			groupID, st.count, maxEntitlementsPerExclusionGroup)}
	}
	return nil
}

// checkStoredExclusionGroups is invariant I5: exclusion-group
// invariants validated over the STORED entitlement keyspace. This is
// the sole exclusion-group validation: it covers fresh and generated
// (static) entitlements uniformly, on every engine, regardless of how
// rows arrived — the page-level streaming validator it replaced could
// not see rows a non-stream ingestion path wrote. A verdict on a
// connector-produced store is a hard failure (a conflicting artifact
// must not seal).
//
// The compaction expand pass (CompactionMerge) is the exception: it
// runs over a keep-newer MERGE of individually-valid syncs, and the
// key union can manufacture conflicts no input contained — a default
// moved from E1 to E2 keeps both rows' is_default, a group grown
// across generations exceeds the member cap, a group id reused on a
// new resource type collides with retained old rows. Failing would
// reject an artifact the merge produced by design (the same class
// I3/I7/I8/I9 exempt), so expand-only runs aggregate into one warning
// instead. Fail-fast still hard-fails everything.
func (pass *ingestInvariantsPass) checkStoredExclusionGroups(ctx context.Context) error {
	// warnOnly: merge-manufactured conflicts are expected shapes on the
	// expand pass, not seal-blockers.
	warnOnly := pass.p.CompactionMerge && !pass.p.FailFast
	tracker := &exclusionGroupTracker{}
	// Conflicts are counted per GROUP, not per violating row: one
	// oversized group re-errors on every excess row, and per-row
	// counting would report it as dozens of "conflicts" while crowding
	// the example slots with near-identical strings. The first error
	// per group is the example.
	conflictGroups := map[string]bool{}
	corruptAnnotations := 0
	var examples []string
	pageToken := ""
	for {
		resp, err := pass.store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
			PageToken:    pageToken,
			ActiveSyncId: pass.p.ActiveSyncID,
		}.Build())
		if err != nil {
			return fmt.Errorf("ingest invariant I5: listing entitlements: %w", err)
		}
		for _, ent := range resp.GetList() {
			if err := tracker.record(ent); err != nil {
				// Every arm of I5 — typed group violations AND corrupt
				// (undecodable) annotations — follows the pass semantics:
				// a NEW collection hard-fails the seal with the
				// non-retryable sentinel (both are deterministic stored
				// bytes; retrying cannot fix either), while a pass over a
				// pre-sealed artifact (compactor merge, rollback replay)
				// observes and attributes. A corrupt annotation is not
				// merge-MANUFACTURED, but on a pre-sealed pass the bytes
				// already sealed once and failing here — after the merge
				// or the rollback mutation — punishes the wrong actor
				// with no operator escape.
				if !warnOnly {
					return invariantVerdict(fmt.Errorf("ingest invariant I5 violated: %w", err))
				}
				var viol *exclusionGroupViolation
				if !errors.As(err, &viol) {
					// Corrupt annotation: counted separately from group
					// conflicts (it is artifact damage, not a group
					// shape), one example per row.
					corruptAnnotations++
					if len(examples) < maxDanglingIDExamples {
						examples = append(examples, err.Error())
					}
					continue
				}
				if conflictGroups[viol.groupID] {
					continue
				}
				conflictGroups[viol.groupID] = true
				if len(examples) < maxDanglingIDExamples {
					examples = append(examples, err.Error())
				}
			}
		}
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	if len(conflictGroups) > 0 || corruptAnnotations > 0 {
		// The attribution differs by what was found: group conflicts are
		// merge-manufactured by design, but corrupt annotations are
		// artifact damage the merge merely carried — saying "manufactured
		// by design" about damage would misdirect the reader.
		msg := "ingest invariant I5: exclusion-group conflicts on the compaction expand pass (keep-newer merges manufacture these by design; not a connector bug)"
		if len(conflictGroups) == 0 {
			msg = "ingest invariant I5: corrupt exclusion-group annotations on a pre-sealed artifact " +
				"(artifact damage carried by the merge/replay input, not merge-manufactured; the rows could not be judged)"
		}
		ctxzap.Extract(ctx).Warn(msg,
			zap.Int("conflict_groups", len(conflictGroups)),
			zap.Int("corrupt_annotations", corruptAnnotations),
			zap.Strings("group_examples", examples),
		)
	}
	return nil
}

// checkChildScheduling is invariant I4: every stored resource carrying a
// ChildResourceType annotation (and passing the resource-type filter)
// must have had its child action scheduled. Check-only — scheduling
// cannot be derived after the fact (an executed child action that
// returned zero rows leaves no store evidence). Fail-fast only (the
// table gates it): the check costs a full post-collection resource scan
// (value decode + annotation walk per row) and in default mode a
// violation was only ever a warning — a bad trade at whale scale.
func (pass *ingestInvariantsPass) checkChildScheduling(ctx context.Context) error {
	if !pass.p.resourcesPhaseRan || pass.p.childSchedule == nil {
		// Resumed past the resources phase (or a caller with no
		// scheduling evidence at all): the scheduled set from the prior
		// process is gone; the predicate is unverifiable.
		return nil
	}
	syncResourceTypeMap := make(map[string]bool, len(pass.p.syncResourceTypes))
	for _, rt := range pass.p.syncResourceTypes {
		syncResourceTypeMap[rt] = true
	}

	var violations []string
	pageToken := ""
	for {
		resp, err := pass.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			PageToken:    pageToken,
			ActiveSyncId: pass.p.ActiveSyncID,
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
				if len(pass.p.syncResourceTypes) > 0 && !syncResourceTypeMap[childType] {
					continue
				}
				if !pass.p.childSchedule.has(childType, r.GetId().GetResourceType(), r.GetId().GetResource()) {
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
		return invariantVerdict(fmt.Errorf(
			"ingest invariant I4 violated: %d child resource sync(s) were never scheduled: %v",
			len(violations), violations))
	}
	return nil
}

// maxDanglingIDExamples caps the id samples carried on the aggregated
// dangling-reference warnings. Production data shows a handful of
// distinct ids explain millions of dangling rows (magic-id construction
// bugs repeat), so a small sample is enough to pivot on.
const maxDanglingIDExamples = 25

// danglingTypeProbe memoizes "does a resource-type row exist" — the
// attribution split for dangling references. A reference into a type
// with NO type row means the type was never synced (typically a type
// added to the connector after the instance was configured and left
// disabled by default — a CONFIG gap); a missing row under a type that
// EXISTS means the id was constructed for a row the enumeration never
// produced (a magic-id BUG).
type danglingTypeProbe struct {
	store connectorstore.Reader
	// syncScope carries the pass's ActiveSyncID as a SyncDetails
	// annotation (GetResourceType has no ActiveSyncId field), scoping
	// the probe like every other read in the pass: without it, the
	// pebble adapter falls back to current-sync resolution, which can
	// fail on an unbound store (external callers, corrupted metadata)
	// while the sibling ListEntitlements/-Resources reads — which DO
	// carry the id — succeed.
	syncScope []*anypb.Any
	known     map[string]bool
}

// newTypeProbe builds the pass's memoizing resource-type probe, scoped
// to the pass's sync.
func (pass *ingestInvariantsPass) newTypeProbe() (*danglingTypeProbe, error) {
	p := &danglingTypeProbe{store: pass.store}
	if pass.p.ActiveSyncID != "" {
		sd, err := anypb.New(c1zpb.SyncDetails_builder{Id: pass.p.ActiveSyncID}.Build())
		if err != nil {
			return nil, fmt.Errorf("type probe: encoding sync scope: %w", err)
		}
		p.syncScope = []*anypb.Any{sd}
	}
	return p, nil
}

func (p *danglingTypeProbe) typeExists(ctx context.Context, resourceTypeID string) (bool, error) {
	if v, ok := p.known[resourceTypeID]; ok {
		return v, nil
	}
	_, err := p.store.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
		ResourceTypeId: resourceTypeID,
		Annotations:    p.syncScope,
	}.Build())
	if err != nil && status.Code(err) != codes.NotFound {
		// A read failure is NOT "type never synced": that verdict picks
		// the config-gap attribution, so an IO error or canceled context
		// here would mislabel the aggregate where policy demands a loud
		// failure. Propagate, and memoize nothing — only definitive
		// answers may be cached (a memoized transient error would poison
		// every later reference to the type).
		return false, fmt.Errorf("resource-type probe for %q: %w", resourceTypeID, err)
	}
	if p.known == nil {
		p.known = map[string]bool{}
	}
	exists := err == nil
	p.known[resourceTypeID] = exists
	return exists, nil
}

// danglingAttribution renders the attribution split for the aggregated
// dangling-reference warnings: references into never-synced types are
// config gaps; references into SYNCED types are connector data bugs —
// unless the pass runs over a compaction merge (CompactionMerge),
// where keep-newer merges manufacture them by design.
func danglingAttribution(unsyncedTypeCount, syncedTypeCount int, compactionMerge bool) string {
	switch {
	case syncedTypeCount == 0:
		return "referenced resource type(s) never synced — likely disabled on the connector (config gap), not a code bug"
	case unsyncedTypeCount == 0 && compactionMerge:
		return "references target SYNCED types under the compaction expand pass — keep-newer merges manufacture these by design; not a connector bug"
	case unsyncedTypeCount == 0:
		return "references target SYNCED types — likely a connector magic-id construction bug, or rows emitted for referents the enumeration never produced"
	case compactionMerge:
		return "mixed: some references target never-synced types (config gap), some target synced types under the compaction expand pass (keep-newer merges manufacture these by design)"
	default:
		return "mixed: some references target never-synced types (config gap), some target synced types (likely connector data bugs)"
	}
}

// checkGrantResourceReferences is invariant I3: every distinct
// entitlement resource referenced by a grant must exist as a resource
// row. One seek per distinct resource (the grant keyspace leads with the
// entitlement resource), value reads only for dangling refs.
//
// The annotated arm (dangling ref + InsertResourceGrants) is a hard
// violation: those rows are the syncer's own establishment guarantee
// (no ListResources call ever returns them; the machinery materializes
// the row in the same page handling), so a missing row means the
// machinery lost it or the connector's own data destroyed it. The
// compaction expand pass (CompactionMerge) is exempt from the hard arm
// in default mode: keep-newer merges can strand an annotated grant
// whose resource row lost the merge, by design — those aggregate into
// the warning instead.
//
// The unannotated arm (connectors emitting grants for unlisted
// resources without InsertResourceGrants) is tolerated: one aggregated
// warning per sync — except under fail-fast, whose contract is that
// every tolerated warn becomes a named failure.
func (pass *ingestInvariantsPass) checkGrantResourceReferences(ctx context.Context) error {
	danglingResources, unsyncedType, syncedType := 0, 0, 0
	var annotatedMergeStranded int
	var resourceExamples []string
	probe, err := pass.newTypeProbe()
	if err != nil {
		return err
	}
	err = pass.facts.ForEachDistinctGrantEntitlementResource(ctx, func(rt, rid string) error {
		exists, err := pass.facts.HasResourceRecord(ctx, rt, rid)
		if err != nil {
			return fmt.Errorf("ingest invariant I3: probing resource %s/%s: %w", rt, rid, err)
		}
		if exists {
			return nil
		}
		annotated, err := pass.facts.GrantsForEntResourceCarryInsertFact(ctx, rt, rid)
		if err != nil {
			return fmt.Errorf("ingest invariant I3: probing grant annotations for %s/%s: %w", rt, rid, err)
		}
		if annotated {
			if pass.p.CompactionMerge && !pass.p.FailFast {
				// A keep-newer merge stranded a machinery-owned grant:
				// the input that won the grant row lost the resource
				// row. Manufactured by the merge, not evidence of a
				// machinery bug — aggregate it.
				annotatedMergeStranded++
				danglingResources++
				syncedType++ // the flavor split below reports it under synced types
				if len(resourceExamples) < maxDanglingIDExamples {
					resourceExamples = append(resourceExamples, rt+"/"+rid)
				}
				return nil
			}
			return invariantVerdict(fmt.Errorf(
				"ingest invariant I3 violated: grants reference resource %s/%s via InsertResourceGrants "+
					"but no resource row exists — the related-resource machinery lost the row or the "+
					"connector's own data is inconsistent (check for anything deleting grant-inserted resources)",
				rt, rid))
		}
		// Tolerated today: connectors emitting grants for unlisted
		// resources without InsertResourceGrants. Visible, not fatal —
		// except under fail-fast, whose contract is that every tolerated
		// warn becomes a named failure (harnesses must catch connector
		// shapes production only logs).
		if pass.p.FailFast {
			return invariantVerdict(fmt.Errorf(
				"ingest invariant I3 violated: grants reference resource %s/%s which was never synced "+
					"(no InsertResourceGrants annotation — tolerated with a warning in production, promoted under fail-fast)",
				rt, rid))
		}
		typeSynced, probeErr := probe.typeExists(ctx, rt)
		if probeErr != nil {
			return fmt.Errorf("ingest invariant I3: %w", probeErr)
		}
		danglingResources++
		if typeSynced {
			syncedType++
		} else {
			unsyncedType++
		}
		if len(resourceExamples) < maxDanglingIDExamples {
			resourceExamples = append(resourceExamples, rt+"/"+rid)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if danglingResources > 0 {
		ctxzap.Extract(ctx).Warn("ingest invariant I3: grants reference resources with no resource row",
			zap.Int("dangling_resources", danglingResources),
			zap.Int("refs_into_unsynced_types", unsyncedType),
			zap.Int("refs_under_synced_types", syncedType),
			zap.Int("insert_resource_grants_merge_stranded", annotatedMergeStranded),
			zap.String("attribution", danglingAttribution(unsyncedType, syncedType, pass.p.CompactionMerge)),
			zap.Strings("resource_examples", resourceExamples),
		)
	}
	return nil
}

// checkEntitlementResourceReferences is invariant I7: every distinct
// resource referenced by an entitlement row must exist as a resource
// row. One seek per distinct resource among entitlements — bounded by
// the entitlement count. Default mode aggregates every dangling into
// one warning with the type-synced attribution split; fail-fast mode
// hard-fails the first one.
func (pass *ingestInvariantsPass) checkEntitlementResourceReferences(ctx context.Context) error {
	probe, err := pass.newTypeProbe()
	if err != nil {
		return err
	}
	danglingResources, unsyncedType, syncedType := 0, 0, 0
	var resourceExamples []string
	err = pass.facts.ForEachDistinctEntitlementResource(ctx, func(rt, rid string) error {
		exists, err := pass.facts.HasResourceRecord(ctx, rt, rid)
		if err != nil {
			return fmt.Errorf("ingest invariant I7: probing resource %s/%s: %w", rt, rid, err)
		}
		if exists {
			return nil
		}
		typeSynced, probeErr := probe.typeExists(ctx, rt)
		if probeErr != nil {
			return fmt.Errorf("ingest invariant I7: %w", probeErr)
		}
		if pass.p.FailFast {
			return invariantVerdict(fmt.Errorf(
				"ingest invariant I7 violated: entitlements reference resource %s/%s but no resource row exists "+
					"(resource type synced: %t — false means a config gap, true a magic-id construction bug)",
				rt, rid, typeSynced))
		}
		danglingResources++
		if typeSynced {
			syncedType++
		} else {
			unsyncedType++
		}
		if len(resourceExamples) < maxDanglingIDExamples {
			resourceExamples = append(resourceExamples, rt+"/"+rid)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if danglingResources > 0 {
		ctxzap.Extract(ctx).Warn("ingest invariant I7: entitlements reference resources with no resource row",
			zap.Int("dangling_resources", danglingResources),
			zap.Int("refs_into_unsynced_types", unsyncedType),
			zap.Int("refs_under_synced_types", syncedType),
			zap.String("attribution", danglingAttribution(unsyncedType, syncedType, pass.p.CompactionMerge)),
			zap.Strings("resource_examples", resourceExamples),
		)
	}
	return nil
}

// checkGrantEntitlementReferences is invariant I8: every distinct
// entitlement referenced by a grant must exist as an entitlement row.
// One seek plus one point probe per distinct entitlement — O(distinct
// entitlements), never O(grants).
//
// InsertResourceGrants grants are EXEMPT in every mode, per GRANT: that
// pattern has always produced grants whose entitlements have no row (the
// machinery inserts the grant's embedded resource; no listing ever
// returns an entitlement for it), and downstream consumers synthesize
// the entitlement from the grant. The exemption follows the ownership
// rule at the grant's own granularity: a connector-owned grant dangling
// under the same missing entitlement as machinery-owned IRG grants is
// still a violation — a resource- or entitlement-level exemption would
// let one IRG grant launder every dangling reference that shares its
// resource.
//
// Default mode aggregates every dangling into one warning with the
// type-synced attribution split; fail-fast mode hard-fails the first
// non-exempt one.
func (pass *ingestInvariantsPass) checkGrantEntitlementReferences(ctx context.Context) error {
	probe, err := pass.newTypeProbe()
	if err != nil {
		return err
	}
	danglingEnts, unsyncedType, syncedType := 0, 0, 0
	var entIDExamples []string
	err = pass.facts.ForEachDanglingGrantEntitlement(ctx, func(entitlementID, rt, rid string) error {
		allCarry, err := pass.facts.GrantsForEntitlementAllCarryInsertFact(ctx, entitlementID, rt, rid)
		if err != nil {
			return fmt.Errorf("ingest invariant I8: probing grant annotations for entitlement %q: %w", entitlementID, err)
		}
		if allCarry {
			return nil // the established InsertResourceGrants shape
		}
		typeSynced, probeErr := probe.typeExists(ctx, rt)
		if probeErr != nil {
			return fmt.Errorf("ingest invariant I8: %w", probeErr)
		}
		if pass.p.FailFast {
			return invariantVerdict(fmt.Errorf(
				"ingest invariant I8 violated: grants without InsertResourceGrants reference entitlement %q (resource %s/%s) but no entitlement row exists "+
					"(resource type synced: %t — false means a config gap, true a magic-id construction bug)",
				entitlementID, rt, rid, typeSynced))
		}
		danglingEnts++
		if typeSynced {
			syncedType++
		} else {
			unsyncedType++
		}
		if len(entIDExamples) < maxDanglingIDExamples {
			entIDExamples = append(entIDExamples, entitlementID)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if danglingEnts > 0 {
		ctxzap.Extract(ctx).Warn("ingest invariant I8: grants reference entitlements with no entitlement row",
			zap.Int("dangling_entitlements", danglingEnts),
			zap.Int("refs_into_unsynced_types", unsyncedType),
			zap.Int("refs_under_synced_types", syncedType),
			zap.String("attribution", danglingAttribution(unsyncedType, syncedType, pass.p.CompactionMerge)),
			zap.Strings("entitlement_id_examples", entIDExamples),
		)
	}
	return nil
}

// checkGrantPrincipalReferences is invariant I9: every distinct
// principal referenced by a grant must exist as a resource row. Scans
// the by_principal index (one seek + probe per distinct principal); a
// pending deferred index build is forced first so every grant write
// path is covered — that build is EndSync's own pass moved earlier, so
// the net cost is ~zero.
//
// Grants carrying ExternalResourceMatch* annotations are EXEMPT in
// every mode: unprocessed match carriers mean no external resource file
// was configured (the match op deletes carriers when it runs) — they
// are evidence of a config gap, not bad data, and hiding them would
// make a misconfigured deployment look clean.
//
// Default mode aggregates every dangling into one warning with the
// type-synced attribution split; fail-fast mode hard-fails the first
// non-exempt one.
func (pass *ingestInvariantsPass) checkGrantPrincipalReferences(ctx context.Context) error {
	if err := pass.facts.EnsureGrantIndexes(ctx); err != nil {
		return fmt.Errorf("ingest invariant I9: ensuring grant indexes: %w", err)
	}
	if err := pass.haltAt(haltStageI9IndexesEnsured); err != nil {
		return err
	}
	probe, err := pass.newTypeProbe()
	if err != nil {
		return err
	}
	danglingPrincipals, unsyncedType, syncedType := 0, 0, 0
	var matchCarriersKept int64
	var principalExamples []string
	err = pass.facts.ForEachDanglingGrantPrincipal(ctx, func(rt, rid string, matchAnnotatedOnly bool, carrierGrants int64) error {
		// Carrier grants are counted per GRANT in both shapes: a fully
		// annotated principal is exempt outright, and a MIXED population
		// still reports its carriers (the plain grants are what make the
		// principal a violation, not the carriers beside them).
		matchCarriersKept += carrierGrants
		if matchAnnotatedOnly {
			return nil
		}
		typeSynced, probeErr := probe.typeExists(ctx, rt)
		if probeErr != nil {
			return fmt.Errorf("ingest invariant I9: %w", probeErr)
		}
		if pass.p.FailFast {
			return invariantVerdict(fmt.Errorf(
				"ingest invariant I9 violated: grants reference principal %s/%s but no resource row exists "+
					"(resource type synced: %t — false means a config gap, true means the connector "+
					"emitted grants for a principal it never synced)",
				rt, rid, typeSynced))
		}
		danglingPrincipals++
		if typeSynced {
			syncedType++
		} else {
			unsyncedType++
		}
		if len(principalExamples) < maxDanglingIDExamples {
			principalExamples = append(principalExamples, rt+"/"+rid)
		}
		return nil
	})
	if err != nil {
		return err
	}
	l := ctxzap.Extract(ctx)
	if danglingPrincipals > 0 {
		l.Warn("ingest invariant I9: grants reference principals with no resource row",
			zap.Int("dangling_principals", danglingPrincipals),
			zap.Int("refs_into_unsynced_types", unsyncedType),
			zap.Int("refs_under_synced_types", syncedType),
			zap.String("attribution", danglingAttribution(unsyncedType, syncedType, pass.p.CompactionMerge)),
			zap.Strings("principal_examples", principalExamples),
		)
	}
	if matchCarriersKept > 0 {
		l.Warn("ingest invariant I9: kept unprocessed external-match carrier grants with dangling principals — was the external resource file configured?",
			zap.Int64("match_carriers_kept", matchCarriersKept),
		)
	}
	return nil
}
