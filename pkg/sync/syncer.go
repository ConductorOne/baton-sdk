package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	native_sync "sync"
	"time"

	"github.com/Masterminds/semver/v3"
	storage_v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/conductorone/baton-sdk/pkg/types/entitlement"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/conductorone/baton-sdk/pkg/uotel"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/metrics"
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/sync/progresslog"
	"github.com/conductorone/baton-sdk/pkg/types"
)

var tracer = otel.Tracer("baton-sdk/sync")

var dontFixCycles, _ = strconv.ParseBool(os.Getenv("BATON_DONT_FIX_CYCLES"))

var ErrSyncNotComplete = fmt.Errorf("sync exited without finishing")
var ErrTooManyWarnings = fmt.Errorf("too many warnings, exiting sync")
var ErrNoSyncIDFound = fmt.Errorf("no syncID found after starting or resuming sync")

var timedSyncOps = []ActionOp{
	SyncResourceTypesOp,
	SyncResourcesOp,
	SyncTargetedResourceOp,
	SyncStaticEntitlementsOp,
	SyncEntitlementsOp,
	SyncGrantsOp,
	SyncExternalResourcesOp,
	SyncAssetsOp,
}

var connectorCallMethods = []string{
	"list-resource-types",
	"get-resource",
	"list-resources",
	"list-entitlements",
	"list-static-entitlements",
	"get-asset",
	"list-grants",
}

// IsSyncPreservable returns true if the error returned by Sync() means that the sync artifact is useful.
// This either means that there was no error, or that the error is recoverable (we can resume the sync and possibly succeed next time).
func IsSyncPreservable(err error) bool {
	if err == nil {
		return true
	}
	// ErrSyncNotComplete means we hit the run duration timeout.
	// ErrTooManyWarnings means we hit too many warnings.
	// Both are recoverable errors.
	if errors.Is(err, ErrSyncNotComplete) || errors.Is(err, ErrTooManyWarnings) {
		return true
	}
	statusErr, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch statusErr.Code() {
	case codes.OK,
		codes.NotFound,
		codes.PermissionDenied,
		codes.ResourceExhausted,
		codes.FailedPrecondition,
		codes.Aborted,
		codes.Unavailable,
		codes.Unauthenticated:
		return true
	default:
		return false
	}
}

type Syncer interface {
	Sync(context.Context) error
	Close(context.Context) error
}

// syncMap is a thin generic wrapper around sync.Map that provides
// compile-time type safety for keys and values.
type syncMap[K comparable, V any] struct {
	m native_sync.Map
}

func (sm *syncMap[K, V]) Load(key K) (V, bool) {
	val, ok := sm.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return val.(V), true
}

func (sm *syncMap[K, V]) Store(key K, val V) {
	sm.m.Store(key, val)
}

// syncer orchestrates a connector sync and stores the results using the provided datasource.Writer.
type syncer struct {
	c1zPath                             string
	externalResourceC1ZPath             string
	externalResourceEntitlementIdFilter string
	previousSyncC1ZPath                 string
	previousSyncC1ZPathOptional         bool
	store                               c1zstore.Store
	externalResourceReader              connectorstore.Reader
	previousSyncReader                  connectorstore.Reader
	connector                           types.ConnectorClient
	state                               State
	runDuration                         time.Duration
	transitionHandler                   func(s Action)
	progressHandler                     func(p *Progress)
	tmpDir                              string
	storageEngine                       c1zstore.Engine
	skipFullSync                        bool
	lastCheckPointTime                  time.Time
	counts                              *progresslog.ProgressLog
	targetedSyncResources               []*v2.Resource
	onlyExpandGrants                    bool
	dontExpandGrants                    bool
	syncID                              string
	skipEGForResourceType               syncMap[string, bool]
	skipEntitlementsForResourceType     syncMap[string, bool]
	scheduledResourceTypes              syncMap[string, bool]
	ingestFilterStats                   ingestFilterStats
	skipEntitlementsAndGrants           bool
	skipGrants                          bool
	resourceTypeTraits                  syncMap[string, []v2.ResourceType_Trait]
	syncType                            connectorstore.SyncType
	injectSyncIDAnnotation              bool
	setSessionStore                     sessions.SetSessionStore
	syncResourceTypes                   []string
	workerCount                         int // If 1, sync is sequential (default). If > 1, sync operations are done in parallel.
	metricsHandler                      metrics.Handler
	syncIdentity                        uotel.SyncIdentity
	recordStats                         bool
}

var _ Syncer = (*syncer)(nil)

// expanderStoreAdapter composes the Reader methods on C1ZStore with
// GrantStore.StoreExpandedGrants so the expander package can depend on
// a single narrow interface without knowing about C1ZStore.
type expanderStoreAdapter struct {
	store c1zstore.Store
}

func (a expanderStoreAdapter) GetEntitlement(ctx context.Context, req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	return a.store.GetEntitlement(ctx, req)
}

// requireEntitlementRefs guards the expansion → store boundary: grant
// listings during expansion must address the entitlement by its structured
// resource refs (from a fetched entitlement record), never by a bare id
// string. Bare-id resolution is a query-planning convenience reserved for
// interactive edges (CLI, explorer) where an ambiguity error is acceptable;
// expansion must not depend on it. Every expansion call site fetches the
// entitlement record first, so this only fires on a regression.
func requireEntitlementRefs(ent *v2.Entitlement) error {
	if ent == nil || ent.GetId() == "" {
		return errors.New("grant expansion: missing entitlement")
	}
	if res := ent.GetResource(); res.GetId().GetResourceType() == "" || res.GetId().GetResource() == "" {
		return fmt.Errorf("grant expansion: entitlement %q has no resource refs; expansion must not resolve bare id strings", ent.GetId())
	}
	return nil
}

func (a expanderStoreAdapter) ListGrantsForEntitlement(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	if err := requireEntitlementRefs(req.GetEntitlement()); err != nil {
		return nil, err
	}
	return a.store.ListGrantsForEntitlement(ctx, req)
}

func (a expanderStoreAdapter) ListGrantPrincipalKeysForEntitlement(
	ctx context.Context,
	entitlement *v2.Entitlement,
	pageToken string,
	pageSize uint32,
) ([]string, string, error) {
	if err := requireEntitlementRefs(entitlement); err != nil {
		return nil, "", err
	}
	// Preserve Pebble's compact prefetch path through this wrapper. Non-Pebble
	// stores fall back to regular grant listing and local key extraction.
	if store, ok := a.store.(interface {
		ListGrantPrincipalKeysForEntitlement(context.Context, *v2.Entitlement, string, uint32) ([]string, string, error)
	}); ok {
		return store.ListGrantPrincipalKeysForEntitlement(ctx, entitlement, pageToken, pageSize)
	}
	resp, err := a.store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: entitlement,
		PageToken:   pageToken,
		PageSize:    pageSize,
	}.Build())
	if err != nil {
		return nil, "", err
	}
	keys := make([]string, 0, len(resp.GetList()))
	for _, g := range resp.GetList() {
		if g.GetPrincipal() == nil {
			continue
		}
		id := g.GetPrincipal().GetId()
		keys = append(keys, id.GetResourceType()+"\x00"+id.GetResource())
	}
	return keys, resp.GetNextPageToken(), nil
}

func (a expanderStoreAdapter) StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	return a.store.Grants().StoreExpandedGrants(ctx, grants...)
}

func (a expanderStoreAdapter) StoreNewExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	if fast, ok := a.store.Grants().(interface {
		StoreNewExpandedGrants(context.Context, ...*v2.Grant) error
	}); ok {
		return fast.StoreNewExpandedGrants(ctx, grants...)
	}
	return a.store.Grants().StoreExpandedGrants(ctx, grants...)
}

func (a expanderStoreAdapter) StoreNewExpandedGrantContributions(ctx context.Context, dest *v2.Entitlement, principals []*storage_v3.PrincipalRef, sources []batonGrant.Sources) error {
	if fast, ok := a.store.Grants().(interface {
		StoreNewExpandedGrantContributions(context.Context, *v2.Entitlement, []*storage_v3.PrincipalRef, []batonGrant.Sources) error
	}); ok {
		return fast.StoreNewExpandedGrantContributions(ctx, dest, principals, sources)
	}
	grants := make([]*v2.Grant, 0, len(principals))
	for i, principalRef := range principals {
		principal := resourceFromPrincipalRef(principalRef)
		grant, err := expand.NewExpandedGrantForStore(dest, principal, sources[i])
		if err != nil {
			return err
		}
		grants = append(grants, grant)
	}
	return a.store.Grants().StoreExpandedGrants(ctx, grants...)
}

// expandedGrantLayerStorer is the layer-scoped synthesized-grant layer session
// surface the store's GrantStore may implement (Pebble). Local interface so
// the adapter can pass sessions through without importing engine internals.
type expandedGrantLayerStorer interface {
	BeginExpandedGrantLayer(ctx context.Context) (bool, error)
	AddExpandedGrantLayerContributions(ctx context.Context, dest *v2.Entitlement, principals []*storage_v3.PrincipalRef, sources []batonGrant.Sources) error
	FinishExpandedGrantLayer(ctx context.Context) error
	AbortExpandedGrantLayer(ctx context.Context) error
}

func (a expanderStoreAdapter) BeginExpandedGrantLayer(ctx context.Context) (bool, error) {
	if fast, ok := a.store.Grants().(expandedGrantLayerStorer); ok {
		return fast.BeginExpandedGrantLayer(ctx)
	}
	return false, nil
}

func (a expanderStoreAdapter) AddExpandedGrantLayerContributions(ctx context.Context, dest *v2.Entitlement, principals []*storage_v3.PrincipalRef, sources []batonGrant.Sources) error {
	fast, ok := a.store.Grants().(expandedGrantLayerStorer)
	if !ok {
		return errors.New("expanded grant layer: store does not support layer sessions")
	}
	return fast.AddExpandedGrantLayerContributions(ctx, dest, principals, sources)
}

func (a expanderStoreAdapter) FinishExpandedGrantLayer(ctx context.Context) error {
	fast, ok := a.store.Grants().(expandedGrantLayerStorer)
	if !ok {
		return errors.New("expanded grant layer: store does not support layer sessions")
	}
	return fast.FinishExpandedGrantLayer(ctx)
}

func (a expanderStoreAdapter) AbortExpandedGrantLayer(ctx context.Context) error {
	if fast, ok := a.store.Grants().(expandedGrantLayerStorer); ok {
		return fast.AbortExpandedGrantLayer(ctx)
	}
	return nil
}

func resourceFromPrincipalRef(ref *storage_v3.PrincipalRef) *v2.Resource {
	if ref == nil {
		return nil
	}
	var parent *v2.ResourceId
	if ref.GetParentResourceId() != "" {
		parent = v2.ResourceId_builder{
			ResourceType: ref.GetParentResourceTypeId(),
			Resource:     ref.GetParentResourceId(),
		}.Build()
	}
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: ref.GetResourceTypeId(),
			Resource:     ref.GetResourceId(),
		}.Build(),
		ParentResourceId: parent,
	}.Build()
}

// GrantsForEntitlementPrincipalSorted forwards the underlying engine's
// principal-sort guarantee (Pebble) so the topological merge can stream grant
// groups instead of buffering and sorting each entitlement. Engines that do not
// implement it (SQLite) report false and get the buffering fallback.
func (a expanderStoreAdapter) GrantsForEntitlementPrincipalSorted() bool {
	store, ok := a.store.(interface {
		GrantsForEntitlementPrincipalSorted() bool
	})
	return ok && store.GrantsForEntitlementPrincipalSorted()
}

const minCheckpointInterval = 10 * time.Second

// Checkpoint marshals the current state and stores it.
func (s *syncer) Checkpoint(ctx context.Context, force bool) error {
	if !force && !s.lastCheckPointTime.IsZero() && time.Since(s.lastCheckPointTime) < minCheckpointInterval {
		return nil
	}
	start := time.Now()
	if s.recordStats {
		defer func() {
			s.state.AddStepDuration("checkpoint", time.Since(start))
		}()
	}
	ctx, span := tracer.Start(ctx, "syncer.Checkpoint")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	s.lastCheckPointTime = time.Now()
	checkpoint, err := s.state.Marshal()
	if err != nil {
		return err
	}
	err = s.store.CheckpointSync(ctx, checkpoint)
	if err != nil {
		return err
	}

	return nil
}

func (s *syncer) timedStep(op ActionOp, f func() error) error {
	if !s.recordStats {
		return f()
	}
	start := time.Now()
	err := f()
	s.state.AddStepDuration(op.String(), time.Since(start))
	return err
}

func (s *syncer) observeConnectorCall(
	ctx context.Context,
	method string,
	start time.Time,
	resourceTypeID string,
	resourceID string,
) {
	if !s.recordStats {
		return
	}

	elapsed := time.Since(start)
	s.state.RecordConnectorCall(method, elapsed)
	if resourceTypeID != "" {
		s.state.RecordConnectorCall(method+":"+resourceTypeID, elapsed)
	}
	if elapsed > time.Minute {
		ctxzap.Extract(ctx).Warn("slow connector call",
			zap.String("method", method),
			zap.String("resource_type_id", resourceTypeID),
			zap.String("resource_id", resourceID),
			zap.Duration("elapsed", elapsed),
		)
	}
}

// syncSummaryFields builds the timing summary for logs and span attrs.
//
// Token state keeps the full maps (including method:resource_type and wait
// labels). Logs/spans are a readable projection:
//   - sync_step_durations_ms is only timedSyncOps (matches sync_steps_total_ms)
//   - waits and other buckets (checkpoint, connector extras) are separate fields
//   - connector_call_stats is flat methods; top-N by-RT is a sibling field
//   - session span attrs are aggregates; per-op session maps stay log-only
func (s *syncer) syncSummaryFields(span trace.Span) []zap.Field {
	stepDurations := s.state.StepDurations()
	callStats := s.state.ConnectorCallStats()
	sessionStats := s.state.SessionStoreStats()

	ops, waits, other := partitionStepDurationsForLog(stepDurations)
	flatCalls, topCallsByRT := partitionConnectorCallStatsForLog(callStats)
	sessionForLog, logSession := filterSessionStatsForLog(sessionStats)

	var stepsTotalMs int64
	for _, op := range timedSyncOps {
		stepsTotalMs += stepDurations[op.String()]
	}

	attrs := []attribute.KeyValue{
		attribute.String("sync.type", string(s.syncType)),
		attribute.Int64("sync.steps.total_ms", stepsTotalMs),
		attribute.Int64("sync.completed_actions", int64(s.state.GetCompletedActionsCount())), //nolint:gosec // action counts fit int64
		attribute.Int("sync.worker_count", s.workerCount),
	}
	for _, op := range timedSyncOps {
		bucket := op.String()
		key := strings.ReplaceAll(bucket, "-", "_")
		attrs = append(attrs, attribute.Int64("sync.step."+key+".duration_ms", stepDurations[bucket]))
	}
	for _, waitBucket := range []string{"checkpoint", "rate_limit_wait", "retry_wait"} {
		key := strings.ReplaceAll(waitBucket, "-", "_")
		attrs = append(attrs, attribute.Int64("sync.step."+key+".duration_ms", stepDurations[waitBucket]))
	}
	for _, method := range connectorCallMethods {
		key := strings.ReplaceAll(method, "-", "_")
		stat := callStats[method]
		attrs = append(attrs,
			attribute.Int64("sync.connector."+key+".count", stat.Count),
			attribute.Int64("sync.connector."+key+".total_ms", stat.TotalMs),
			attribute.Int64("sync.connector."+key+".max_ms", stat.MaxMs),
		)
	}
	if len(sessionStats) > 0 {
		count, errors, timeouts, totalMs, maxMs := aggregateSessionStats(sessionStats)
		attrs = append(attrs,
			attribute.Int64("sync.session.count", count),
			attribute.Int64("sync.session.errors", errors),
			attribute.Int64("sync.session.timeouts", timeouts),
			attribute.Int64("sync.session.total_ms", totalMs),
			attribute.Int64("sync.session.max_ms", maxMs),
		)
	}
	span.SetAttributes(attrs...)

	fields := []zap.Field{
		zap.String("sync_id", s.syncID),
		zap.String("sync_type", string(s.syncType)),
		zap.Any("sync_step_durations_ms", ops),
		zap.Int64("sync_steps_total_ms", stepsTotalMs),
		zap.Any("connector_call_stats", flatCalls),
		zap.Uint64("completed_actions", s.state.GetCompletedActionsCount()),
		zap.Int("worker_count", s.workerCount),
	}
	if len(waits) > 0 {
		fields = append(fields, zap.Any("sync_step_wait_ms", waits))
	}
	if len(other) > 0 {
		// checkpoint, source_cache_*, and other non-op buckets — not part of
		// sync_steps_total_ms. Keep them out of the primary map so a dedicated
		// source-cache log line isn't duplicated under step durations.
		fields = append(fields, zap.Any("sync_step_other_ms", other))
	}
	if len(topCallsByRT) > 0 {
		fields = append(fields, zap.Any("connector_call_stats_by_resource_type", topCallsByRT))
	}
	if logSession {
		fields = append(fields, zap.Any("session_store_stats", sessionForLog))
	}
	return fields
}

// recordSessionOp feeds store-side session observations into the stats
// token under store.-prefixed ops (the c1z view of session traffic).
// Session traffic is driven by connector RPCs the syncer initiated, so the
// state is set before any op arrives; the nil guard covers the Validate
// window between store load and state creation.
func (s *syncer) recordSessionOp(op string, elapsed time.Duration, opErr error) {
	if !s.recordStats {
		return
	}
	st := s.state
	if st == nil {
		return
	}
	st.RecordSessionOp("store."+op, elapsed, opErr, session.IsDeadlineExceeded(opErr))
}

// recordSessionUsage folds a connector-reported SessionStoreUsage response
// annotation into the stats token under connector.-prefixed ops (the
// connector-perceived view, including transport). The difference between the
// connector.* and store.* views isolates session-transport overhead.
func (s *syncer) recordSessionUsage(annos []*anypb.Any) {
	if !s.recordStats || len(annos) == 0 {
		return
	}
	st := s.state
	if st == nil {
		return
	}
	usage := &v2.SessionStoreUsage{}
	respAnnos := annotations.Annotations(annos)
	ok, err := respAnnos.Pick(usage)
	if err != nil || !ok {
		return
	}
	for _, op := range usage.GetOps() {
		if op.GetOp() == "" {
			continue
		}
		st.MergeSessionStat("connector."+op.GetOp(), SessionStoreStat{
			Count:    op.GetCount(),
			Errors:   op.GetErrors(),
			Timeouts: op.GetTimeouts(),
			TotalMs:  op.GetTotalMs(),
			MaxMs:    op.GetMaxMs(),
		})
	}
}

func (s *syncer) returnSyncError(l *zap.Logger, span trace.Span, err error) error {
	if err == nil || !s.recordStats || s.state == nil || errors.Is(err, ErrSyncNotComplete) {
		return err
	}
	l.Info("sync stats so far", s.syncSummaryFields(span)...)
	return err
}

func (s *syncer) handleInitialActionForStep(ctx context.Context, a Action) {
	if s.transitionHandler != nil {
		s.transitionHandler(a)
	}
}

func (s *syncer) handleProgress(ctx context.Context, a *Action, c int) {
	if s.progressHandler != nil {
		//nolint:gosec // No risk of overflow because `c` is a slice length.
		count := uint32(c)
		s.progressHandler(NewProgress(a, count))
	}
}

// maxEntitlementsPerExclusionGroup caps how many entitlements may share a
// single exclusion_group_id. Phase 1 limit.
const maxEntitlementsPerExclusionGroup = 50

// recordEntitlementExclusionGroup enforces the invariants on an exclusion
// group membership: a given exclusion_group_id must stay within one resource
// type, a group may have at most one entitlement marked is_default, and a group
// may contain at most maxEntitlementsPerExclusionGroup entitlements. Empty
// group ids are treated as "no exclusion group" and skipped.
func (s *syncer) recordEntitlementExclusionGroup(eg *v2.EntitlementExclusionGroup, entitlementID, resourceTypeID string) error {
	groupID := eg.GetExclusionGroupId()
	if groupID == "" {
		return nil
	}
	if existing, conflict := s.state.CheckAndSetExclusionGroupResourceType(groupID, resourceTypeID); conflict {
		return fmt.Errorf("exclusion group %q is used on multiple resource types (%q and %q); "+
			"exclusion groups may span resources but must be scoped to a single resource type",
			groupID, existing, resourceTypeID)
	}
	if eg.GetIsDefault() {
		if existing, conflict := s.state.CheckAndSetExclusionGroupDefault(groupID, entitlementID); conflict {
			return fmt.Errorf("exclusion group %q has multiple default entitlements (%q and %q); "+
				"at most one entitlement per exclusion group may set is_default=true",
				groupID, existing, entitlementID)
		}
	}
	if count := s.state.IncrementExclusionGroupCount(groupID); count > maxEntitlementsPerExclusionGroup {
		return fmt.Errorf("exclusion group %q has too many entitlements (%d); "+
			"at most %d entitlements are allowed per exclusion group",
			groupID, count, maxEntitlementsPerExclusionGroup)
	}
	return nil
}

// validateEntitlementExclusionGroups picks the exclusion group annotation off
// each entitlement (if present) and forwards to recordEntitlementExclusionGroup.
// Use this on lists of entitlements that may independently carry exclusion
// group annotations (e.g., the dynamic ListEntitlements path); callers that
// already have the annotation in hand should call recordEntitlementExclusionGroup
// directly to avoid the per-entitlement Pick.
func (s *syncer) validateEntitlementExclusionGroups(ents []*v2.Entitlement) error {
	for _, ent := range ents {
		eg := &v2.EntitlementExclusionGroup{}
		entAnnos := annotations.Annotations(ent.GetAnnotations())
		ok, err := entAnnos.Pick(eg)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := s.recordEntitlementExclusionGroup(eg, ent.GetId(), ent.GetResource().GetId().GetResourceType()); err != nil {
			return err
		}
	}
	return nil
}

// nextPageOrFinishAction updates the action with the next page token, or if there is no next page, finishes the action.
// It also pushes any child actions before updating/finishing the action.
// This is useful for pagination, and for actions that create other actions.
func (s *syncer) nextPageOrFinishAction(ctx context.Context, action *Action, nextPageToken string, childActions ...Action) error {
	if nextPageToken != "" {
		err := s.state.NextPage(ctx, action.ID, nextPageToken)
		if err != nil {
			return err
		}
	}

	for _, a := range childActions {
		s.state.PushAction(ctx, a)
	}

	if nextPageToken == "" {
		s.state.FinishAction(ctx, action)
	}

	return nil
}

func isWarning(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}

	if status.Code(err) == codes.NotFound {
		return true
	}

	return false
}

func (s *syncer) startOrResumeSync(ctx context.Context) (string, bool, error) {
	// Sync resuming logic:
	// If we know our sync ID, set it as the current sync and return (resuming that sync).
	// If targetedSyncResources is not set, find the most recent unfinished sync of our desired sync type & resume it (regardless of partial or full).
	//   If there are no unfinished syncs of our desired sync type, start a new sync.
	// If targetedSyncResources is set, start a new partial sync. Use the most recent completed sync as the parent sync ID (if it exists).

	if s.syncID != "" {
		err := s.store.SetCurrentSync(ctx, s.syncID)
		if err != nil {
			return "", false, err
		}
		return s.syncID, false, nil
	}

	var syncID string
	var newSync bool
	var err error
	if len(s.targetedSyncResources) == 0 {
		syncID, newSync, err = s.store.StartOrResumeSync(ctx, s.syncType, "")
		if err != nil {
			return "", false, err
		}
		return syncID, newSync, nil
	}

	// Get most recent completed full sync if it exists
	latestFullSyncResponse, err := s.store.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{
		SyncType: string(connectorstore.SyncTypeFull),
	}.Build())
	if err != nil {
		return "", false, err
	}
	var latestFullSyncId string
	latestFullSync := latestFullSyncResponse.GetSync()
	if latestFullSync != nil {
		latestFullSyncId = latestFullSync.GetId()
	}
	syncID, err = s.store.StartNewSync(ctx, connectorstore.SyncTypePartial, latestFullSyncId)
	if err != nil {
		return "", false, err
	}
	newSync = true

	return syncID, newSync, nil
}

func (s *syncer) getActiveSyncID() string {
	if s.injectSyncIDAnnotation {
		return s.syncID
	}
	return ""
}

// Sync starts the syncing process. The sync process is driven by the action stack that is part of the state object.
// For each page of data that is required to be fetched from the connector, a new action is pushed on to the stack. Once
// an action is completed, it is popped off of the queue. Before processing each action, we checkpoint the state object
// into the datasource. This allows for graceful resumes if a sync is interrupted.
func (s *syncer) Sync(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.Sync")
	// Propagate connector identity to every descendant span (sync + dotc1z).
	// An explicit WithSyncIdentity option wins; otherwise inherit whatever the
	// caller already set on ctx.
	if !s.syncIdentity.IsZero() {
		ctx = uotel.WithSyncIdentity(ctx, s.syncIdentity)
	}
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if s.skipFullSync {
		return s.SkipSync(ctx)
	}

	l := ctxzap.Extract(ctx)

	runCtx := ctx
	var runCanc context.CancelFunc
	if s.runDuration > 0 {
		runCtx, runCanc = context.WithTimeout(ctx, s.runDuration)
	}
	if runCanc != nil {
		defer runCanc()
	}

	err = s.loadStore(ctx)
	if err != nil {
		return err
	}
	s.recordStats = s.store.Metadata().Engine == string(c1zstore.EnginePebble)

	resp, err := s.connector.Validate(ctx, &v2.ConnectorServiceValidateRequest{})
	if err != nil {
		return err
	}

	if resp.GetSdkVersion() != "" {
		sdkVersion, err := semver.NewVersion(resp.GetSdkVersion())
		if err != nil {
			l.Warn("error parsing sdk version", zap.String("sdk_version", resp.GetSdkVersion()), zap.Error(err))
		} else {
			supportsActiveSyncId, err := semver.NewConstraint(">= 0.4.3")
			if err != nil {
				return fmt.Errorf("error parsing sdk version %s: %w", resp.GetSdkVersion(), err)
			}
			s.injectSyncIDAnnotation = supportsActiveSyncId.Check(sdkVersion)
		}
	}

	syncResourceTypeMap := make(map[string]bool)
	if len(s.syncResourceTypes) > 0 {
		for _, rt := range s.syncResourceTypes {
			syncResourceTypeMap[rt] = true
		}
	}

	// Validate any targeted resource IDs before starting a sync.
	targetedResources := []*v2.Resource{}
	for _, r := range s.targetedSyncResources {
		if len(s.syncResourceTypes) > 0 {
			if _, ok := syncResourceTypeMap[r.GetId().GetResourceType()]; !ok {
				continue
			}
		}

		targetedResources = append(targetedResources, r)
	}

	syncID, newSync, err := s.startOrResumeSync(ctx)
	if err != nil {
		return err
	}
	s.syncID = syncID

	// Set the syncID on the wrapper after we have it
	if syncID == "" {
		err = ErrNoSyncIDFound
		l.Error("no syncID found after starting or resuming sync", zap.Error(err))
		return err
	}

	span.SetAttributes(attribute.String("sync_id", syncID))

	if newSync {
		l.Debug("beginning new sync", zap.String("sync_id", syncID))
	} else {
		l.Debug("resuming previous sync", zap.String("sync_id", syncID))
	}

	currentStep, err := s.store.CurrentSyncStep(ctx)
	if err != nil {
		return err
	}

	state := newState()
	err = state.Unmarshal(currentStep)
	if err != nil {
		return err
	}
	s.state = state
	if !newSync {
		currentAction := s.state.Current()
		currentActionOp := ""
		currentActionPageToken := ""
		currentActionResourceID := ""
		currentActionResourceTypeID := ""
		if currentAction != nil {
			currentActionOp = currentAction.Op.String()
			currentActionPageToken = currentAction.PageToken
			currentActionResourceID = currentAction.ResourceID
			currentActionResourceTypeID = currentAction.ResourceTypeID
		}
		entitlementGraph := s.state.EntitlementGraph(ctx)
		l.Info("resumed previous sync",
			zap.String("sync_id", syncID),
			zap.String("sync_type", string(s.syncType)),
			zap.String("current_action_op", currentActionOp),
			zap.String("current_action_resource_id", currentActionResourceID),
			zap.String("current_action_resource_type_id", currentActionResourceTypeID),
			zap.String("current_action_page_token", currentActionPageToken),
			zap.Bool("needs_expansion", s.state.NeedsExpansion()),
			zap.Bool("has_external_resources_grants", s.state.HasExternalResourcesGrants()),
			zap.Bool("should_fetch_related_resources", s.state.ShouldFetchRelatedResources()),
			zap.Bool("should_skip_entitlements_and_grants", s.state.ShouldSkipEntitlementsAndGrants()),
			zap.Bool("should_skip_grants", s.state.ShouldSkipGrants()),
			zap.Bool("graph_loaded", entitlementGraph.Loaded),
			zap.Bool("graph_has_no_cycles", entitlementGraph.HasNoCycles),
			zap.Int("graph_depth", entitlementGraph.Depth),
			zap.Int("graph_actions", len(entitlementGraph.Actions)),
			zap.Int("graph_edges", len(entitlementGraph.Edges)),
			zap.Int("graph_nodes", len(entitlementGraph.Nodes)),
			zap.Uint64("completed_actions", s.state.GetCompletedActionsCount()),
		)
	}

	if !newSync && s.state.Current() == nil {
		l.Debug("current action is nil, pushing init action for sync", zap.String("sync_id", syncID))
		// Push init action if no current action. This is probably a finished sync that we're running grant expansion on.
		s.state.PushAction(ctx, Action{Op: InitOp})
		err = s.Checkpoint(ctx, true)
		if err != nil {
			return s.returnSyncError(l, span, err)
		}
	}

	warnings, err := s.parallelSync(ctx, runCtx, targetedResources)
	if err != nil {
		return s.returnSyncError(l, span, err)
	}

	s.logIngestFilterSummary(ctx)

	// Force a checkpoint to clear completed actions & entitlement graph in sync_token.
	s.state.ClearEntitlementGraph(ctx)
	s.state.ClearExclusionGroupTracking(ctx)

	err = s.Checkpoint(ctx, true)
	if err != nil {
		return s.returnSyncError(l, span, err)
	}

	err = s.store.Cleanup(runCtx)
	if err != nil {
		// If we hit the context deadline while cleaning up, return ErrSyncNotComplete.
		// Since the sync isn't ended yet, we can resume this sync and make more progress in cleanup.
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrSyncNotComplete
		}
		return s.returnSyncError(l, span, err)
	}

	err = s.store.EndSync(ctx)
	if err != nil {
		return s.returnSyncError(l, span, err)
	}

	if s.recordStats {
		l.Info("Sync complete.", s.syncSummaryFields(span)...)
	} else {
		l.Info("Sync complete.")
	}

	cleanupResp, err := s.connector.Cleanup(ctx, v2.ConnectorServiceCleanupRequest_builder{
		ActiveSyncId: s.getActiveSyncID(),
	}.Build())
	if err != nil {
		l.Error("error clearing connector caches", zap.Error(err))
	}
	// The sync is already sealed, so cleanup-time session usage (the Clear
	// itself plus any final backend traffic) is log-only evidence.
	if s.recordStats {
		usage := &v2.SessionStoreUsage{}
		cleanupAnnos := annotations.Annotations(cleanupResp.GetAnnotations())
		if ok, pickErr := cleanupAnnos.Pick(usage); pickErr == nil && ok {
			l.Info("connector session store cleanup stats", zap.Any("session_store_usage", usage))
		}
	}

	if len(warnings) > 0 {
		l.Warn("sync completed with warnings", zap.Int("warning_count", len(warnings)), zap.Any("warnings", warnings))
	}
	return nil
}

func (s *syncer) SkipSync(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.SkipSync")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)
	l.Info("skipping sync")

	var runCanc context.CancelFunc
	if s.runDuration > 0 {
		_, runCanc = context.WithTimeout(ctx, s.runDuration)
	}
	if runCanc != nil {
		defer runCanc()
	}

	err = s.loadStore(ctx)
	if err != nil {
		return err
	}

	_, err = s.connector.Validate(ctx, &v2.ConnectorServiceValidateRequest{})
	if err != nil {
		return err
	}

	// TODO: Create a new sync type for empty syncs.
	_, err = s.store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		return err
	}

	err = s.store.EndSync(ctx)
	if err != nil {
		return err
	}

	err = s.store.Cleanup(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (s *syncer) listAllResourceTypes(ctx context.Context) iter.Seq2[[]*v2.ResourceType, error] {
	return func(yield func([]*v2.ResourceType, error) bool) {
		pageToken := ""
		for {
			start := time.Now()
			resp, err := s.connector.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
				PageToken:    pageToken,
				ActiveSyncId: s.getActiveSyncID(),
			}.Build())
			s.observeConnectorCall(ctx, "list-resource-types", start, "", "")
			if err != nil {
				_ = yield(nil, err)
				return
			}
			resourceTypes := resp.GetList()
			if len(resourceTypes) > 0 {
				if !yield(resourceTypes, err) {
					return
				}
			}
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				return
			}
		}
	}
}

// SyncResourceTypes calls the ListResourceType() connector endpoint and persists the results in to the datasource.
func (s *syncer) SyncResourceTypes(ctx context.Context, action *Action) error {
	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncResourceTypes")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if action.PageToken == "" {
		ctxzap.Extract(ctx).Info("Syncing resource types...")
		s.handleInitialActionForStep(ctx, *action)
	}

	start := time.Now()
	resp, err := s.connector.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
		PageToken:    action.PageToken,
		ActiveSyncId: s.getActiveSyncID(),
	}.Build())
	s.observeConnectorCall(ctx, "list-resource-types", start, action.ResourceTypeID, action.ResourceID)
	if err != nil {
		return err
	}

	var resourceTypes []*v2.ResourceType
	if len(s.syncResourceTypes) > 0 {
		syncResourceTypeMap := make(map[string]bool)
		for _, rt := range s.syncResourceTypes {
			syncResourceTypeMap[rt] = true
		}
		for _, rt := range resp.GetList() {
			if shouldSync := syncResourceTypeMap[rt.GetId()]; shouldSync {
				resourceTypes = append(resourceTypes, rt)
			}
		}
	} else {
		resourceTypes = resp.GetList()
	}

	err = s.store.PutResourceTypes(ctx, resourceTypes...)
	if err != nil {
		return err
	}

	s.counts.AddResourceTypes(len(resourceTypes))
	s.handleProgress(ctx, action, len(resourceTypes))

	if resp.GetNextPageToken() == "" {
		s.counts.LogResourceTypesProgress(ctx)

		if len(s.syncResourceTypes) > 0 {
			validResourceTypesResp, err := s.store.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
				PageToken:    action.PageToken,
				ActiveSyncId: s.getActiveSyncID(),
			}.Build())
			if err != nil {
				return err
			}
			err = validateSyncResourceTypesFilter(s.syncResourceTypes, validResourceTypesResp.GetList())
			if err != nil {
				return err
			}
		}
	}

	return s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken())
}

func validateSyncResourceTypesFilter(resourceTypesFilter []string, validResourceTypes []*v2.ResourceType) error {
	validResourceTypesMap := make(map[string]bool)
	for _, rt := range validResourceTypes {
		validResourceTypesMap[rt.GetId()] = true
	}
	for _, rt := range resourceTypesFilter {
		if _, ok := validResourceTypesMap[rt]; !ok {
			return fmt.Errorf("invalid resource type '%s' in filter", rt)
		}
	}
	return nil
}

func (s *syncer) hasChildResources(resource *v2.Resource) bool {
	annos := annotations.Annotations(resource.GetAnnotations())

	return annos.Contains((*v2.ChildResourceType)(nil))
}

// getSubResources fetches the sub resource types from a resources' annotations.
// No span here: this is per-resource in-memory annotation iteration with no I/O.
// At sync scale (100k+ resources per trace) the span overhead and trace bloat
// outweighed any debugging value.
func (s *syncer) getSubResources(ctx context.Context, parent *v2.Resource) error {
	syncResourceTypeMap := make(map[string]bool)
	for _, rt := range s.syncResourceTypes {
		syncResourceTypeMap[rt] = true
	}

	for _, a := range parent.GetAnnotations() {
		if a.MessageIs((*v2.ChildResourceType)(nil)) {
			crt := &v2.ChildResourceType{}
			err := a.UnmarshalTo(crt)
			if err != nil {
				return err
			}
			if len(s.syncResourceTypes) > 0 {
				if shouldSync := syncResourceTypeMap[crt.GetResourceTypeId()]; !shouldSync {
					continue
				}
			}
			childAction := Action{
				Op:                   SyncResourcesOp,
				ResourceTypeID:       crt.GetResourceTypeId(),
				ParentResourceID:     parent.GetId().GetResource(),
				ParentResourceTypeID: parent.GetId().GetResourceType(),
			}
			s.state.PushAction(ctx, childAction)
		}
	}

	return nil
}

func (s *syncer) getResourceFromConnector(ctx context.Context, resourceID *v2.ResourceId, parentResourceID *v2.ResourceId) (*v2.Resource, error) {
	ctx, span := tracer.Start(ctx, "syncer.getResource")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	start := time.Now()
	resourceResp, err := s.connector.GetResource(ctx,
		v2.ResourceGetterServiceGetResourceRequest_builder{
			ResourceId:       resourceID,
			ParentResourceId: parentResourceID,
			ActiveSyncId:     s.getActiveSyncID(),
		}.Build(),
	)
	s.observeConnectorCall(ctx, "get-resource", start, resourceID.GetResourceType(), resourceID.GetResource())
	if err == nil {
		return resourceResp.GetResource(), nil
	}
	l := ctxzap.Extract(ctx)
	if status.Code(err) == codes.NotFound {
		l.Warn("skipping resource due to not found", zap.String("resource_id", resourceID.GetResource()), zap.String("resource_type_id", resourceID.GetResourceType()))
		return nil, nil
	}
	if status.Code(err) == codes.Unimplemented {
		l.Warn("skipping resource due to unimplemented connector", zap.String("resource_id", resourceID.GetResource()), zap.String("resource_type_id", resourceID.GetResourceType()))
		return nil, nil
	}
	return nil, err
}

func (s *syncer) SyncTargetedResource(ctx context.Context, action *Action) error {
	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncTargetedResource")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	resourceID := action.ResourceID
	resourceTypeID := action.ResourceTypeID
	if resourceID == "" || resourceTypeID == "" {
		return errors.New("cannot get resource without a resource target")
	}

	parentResourceID := action.ParentResourceID
	parentResourceTypeID := action.ParentResourceTypeID
	var prID *v2.ResourceId
	if parentResourceID != "" && parentResourceTypeID != "" {
		prID = v2.ResourceId_builder{
			ResourceType: parentResourceTypeID,
			Resource:     parentResourceID,
		}.Build()
	}

	resource, err := s.getResourceFromConnector(ctx, v2.ResourceId_builder{
		ResourceType: resourceTypeID,
		Resource:     resourceID,
	}.Build(), prID)
	if err != nil {
		return err
	}

	// If getResource encounters not found or unimplemented, it returns a nil resource and nil error.
	if resource == nil {
		s.state.FinishAction(ctx, action)
		return nil
	}

	// Save our resource in the DB
	if err := s.store.PutResources(ctx, resource); err != nil {
		return err
	}

	s.state.FinishAction(ctx, action)

	// Actions happen in reverse order. We want to sync child resources, then entitlements, then grants

	shouldSkipGrants, err := s.shouldSkipGrants(ctx, resource)
	if err != nil {
		return err
	}
	if !shouldSkipGrants {
		s.state.PushAction(ctx, Action{
			Op:             SyncGrantsOp,
			ResourceTypeID: resourceTypeID,
			ResourceID:     resourceID,
		})
	}

	shouldSkipEnts, err := s.shouldSkipEntitlements(ctx, resource)
	if err != nil {
		return err
	}

	if !shouldSkipEnts {
		s.state.PushAction(ctx, Action{
			Op:             SyncEntitlementsOp,
			ResourceTypeID: resourceTypeID,
			ResourceID:     resourceID,
		})
	}

	err = s.getSubResources(ctx, resource)
	if err != nil {
		return err
	}

	return nil
}

// SyncResources handles fetching all of the resources from the connector given the provided resource types. For each
// resource, we gather any child resource types it may emit, and traverse the resource tree.
func (s *syncer) SyncResources(ctx context.Context, action *Action) error {
	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncResources")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if action.ResourceTypeID == "" {
		if action.PageToken == "" {
			ctxzap.Extract(ctx).Info("Syncing resources...")
			s.handleInitialActionForStep(ctx, *action)
		}

		resp, err := s.store.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
			PageToken:    action.PageToken,
			ActiveSyncId: s.getActiveSyncID(),
		}.Build())
		if err != nil {
			return err
		}

		actions := make([]Action, 0)
		for _, rt := range resp.GetList() {
			newAction := Action{Op: SyncResourcesOp, ResourceTypeID: rt.GetId()}
			// If this request specified a parent resource, only queue up syncing resources for children of the parent resource
			if action.ParentResourceTypeID != "" && action.ParentResourceID != "" {
				newAction.ParentResourceID = action.ParentResourceID
				newAction.ParentResourceTypeID = action.ParentResourceTypeID
			}

			actions = append(actions, newAction)
		}

		return s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken(), actions...)
	}

	return s.syncResources(ctx, action)
}

// syncResources fetches a given resource from the connector, and returns a slice of new child resources to fetch.
// No span here: this is the only call site of SyncResources, which already
// owns a span — the duplicate inflated trace span counts without adding
// information.
func (s *syncer) syncResources(ctx context.Context, action *Action) error {
	req := v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: action.ResourceTypeID,
		PageToken:      action.PageToken,
		ActiveSyncId:   s.getActiveSyncID(),
	}.Build()
	if action.ParentResourceTypeID != "" && action.ParentResourceID != "" {
		req.SetParentResourceId(v2.ResourceId_builder{
			ResourceType: action.ParentResourceTypeID,
			Resource:     action.ParentResourceID,
		}.Build())
	}

	start := time.Now()
	resp, err := s.connector.ListResources(ctx, req)
	s.observeConnectorCall(ctx, "list-resources", start, action.ResourceTypeID, action.ResourceID)
	s.recordSessionUsage(resp.GetAnnotations())
	if err != nil {
		return err
	}

	bulkPutResoruces := []*v2.Resource{}
	for _, r := range resp.GetList() {
		validatedResource := false

		// Check if we've already synced this resource, skip it if we have
		_, err = s.store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
			ResourceId: v2.ResourceId_builder{ResourceType: r.GetId().GetResourceType(), Resource: r.GetId().GetResource()}.Build(),
		}.Build())
		if err == nil {
			err = s.validateResourceTraits(ctx, r)
			if err != nil {
				return err
			}
			validatedResource = true

			// We must *ALSO* check if we have any child resources.
			if !s.hasChildResources(r) {
				// Since we only have the resource type IDs of child resources,
				// we can't tell if we already have synced those child resources.
				// Those children may also have their own child resources,
				// so we are conservative here and just re-sync this resource.
				continue
			}
		}

		if err != nil && status.Code(err) != codes.NotFound {
			return err
		}

		if !validatedResource {
			err = s.validateResourceTraits(ctx, r)
			if err != nil {
				return err
			}
		}

		bulkPutResoruces = append(bulkPutResoruces, r)

		err = s.getSubResources(ctx, r)
		if err != nil {
			return err
		}
	}

	if len(bulkPutResoruces) > 0 {
		err = s.store.PutResources(ctx, bulkPutResoruces...)
		if err != nil {
			return err
		}
	}

	s.handleProgress(ctx, action, len(resp.GetList()))
	s.counts.AddResources(action.ResourceTypeID, len(resp.GetList()))
	if resp.GetNextPageToken() == "" {
		// Last page of resources for this resource type, so log the progress.
		s.counts.LogResourcesProgress(ctx, action.ResourceTypeID)
	}

	return s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken())
}

// No span here: this is called per-resource, but only does I/O on the
// first time a resource type is seen (cached afterward). The wrapped
// C1File.GetResourceType call is itself spanned, so we still see the
// uncached path.
func (s *syncer) validateResourceTraits(ctx context.Context, r *v2.Resource) error {
	resourceTypeTraits, ok := s.resourceTypeTraits.Load(r.GetId().GetResourceType())
	if !ok {
		resourceTypeResponse, err := s.store.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
			ResourceTypeId: r.GetId().GetResourceType(),
		}.Build())
		if err != nil {
			return err
		}
		resourceTypeTraits = resourceTypeResponse.GetResourceType().GetTraits()
		s.resourceTypeTraits.Store(r.GetId().GetResourceType(), resourceTypeTraits)
	}

	for _, t := range resourceTypeTraits {
		var trait proto.Message
		switch t {
		case v2.ResourceType_TRAIT_APP:
			trait = &v2.AppTrait{}
		case v2.ResourceType_TRAIT_GROUP:
			trait = &v2.GroupTrait{}
		case v2.ResourceType_TRAIT_USER:
			trait = &v2.UserTrait{}
		case v2.ResourceType_TRAIT_ROLE:
			trait = &v2.RoleTrait{}
		case v2.ResourceType_TRAIT_SECRET:
			trait = &v2.SecretTrait{}
		default:
		}

		if trait != nil {
			annos := annotations.Annotations(r.GetAnnotations())
			if !annos.Contains(trait) {
				ctxzap.Extract(ctx).Error(
					"resource was missing expected trait",
					zap.String("trait", string(trait.ProtoReflect().Descriptor().Name())),
					zap.String("resource_type_id", r.GetId().GetResourceType()),
					zap.String("resource_id", r.GetId().GetResource()),
				)
				return fmt.Errorf("resource was missing expected trait %s", trait.ProtoReflect().Descriptor().Name())
			}
		}
	}

	return nil
}

// shouldSkipEntitlementsAndGrants determines if we should sync entitlements for a given resource. We cache the
// result of this function for each resource type to avoid constant lookups in the database.
// No span here: the function is called per-resource and is almost always a cached map
// lookup; the uncached path hits C1File.GetResourceType, which is itself spanned.
func (s *syncer) shouldSkipEntitlementsAndGrants(ctx context.Context, r *v2.Resource) (bool, error) {
	if s.state.ShouldSkipEntitlementsAndGrants() {
		return true, nil
	}

	rAnnos := annotations.Annotations(r.GetAnnotations())
	if rAnnos.Contains(&v2.SkipEntitlementsAndGrants{}) {
		return true, nil
	}

	// We've checked this resource type, so we can return what we have cached directly.
	if skip, ok := s.skipEGForResourceType.Load(r.GetId().GetResourceType()); ok {
		return skip, nil
	}

	rt, err := s.store.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
		ResourceTypeId: r.GetId().GetResourceType(),
	}.Build())
	if err != nil {
		return false, err
	}

	rtAnnos := annotations.Annotations(rt.GetResourceType().GetAnnotations())

	skipEntitlements := rtAnnos.Contains(&v2.SkipEntitlementsAndGrants{})
	s.skipEGForResourceType.Store(r.GetId().GetResourceType(), skipEntitlements)

	return skipEntitlements, nil
}

func (s *syncer) shouldSkipGrants(ctx context.Context, r *v2.Resource) (bool, error) {
	annos := annotations.Annotations(r.GetAnnotations())
	if annos.Contains(&v2.SkipGrants{}) {
		return true, nil
	}

	if s.state.ShouldSkipGrants() {
		return true, nil
	}

	return s.shouldSkipEntitlementsAndGrants(ctx, r)
}

// No span here: shouldSkipEntitlements is called per-resource and almost
// always a cached map lookup; uncached path hits C1File.GetResourceType,
// which is itself spanned.
func (s *syncer) shouldSkipEntitlements(ctx context.Context, r *v2.Resource) (bool, error) {
	ok, err := s.shouldSkipEntitlementsAndGrants(ctx, r)
	if err != nil {
		return false, err
	}

	if ok {
		return true, nil
	}

	rAnnos := annotations.Annotations(r.GetAnnotations())
	if rAnnos.Contains(&v2.SkipEntitlements{}) || rAnnos.Contains(&v2.SkipEntitlementsAndGrants{}) {
		return true, nil
	}

	if skip, ok := s.skipEntitlementsForResourceType.Load(r.GetId().GetResourceType()); ok {
		return skip, nil
	}

	rt, err := s.store.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
		ResourceTypeId: r.GetId().GetResourceType(),
	}.Build())
	if err != nil {
		return false, err
	}

	rtAnnos := annotations.Annotations(rt.GetResourceType().GetAnnotations())

	skipEntitlements := rtAnnos.Contains(&v2.SkipEntitlements{}) || rtAnnos.Contains(&v2.SkipEntitlementsAndGrants{})
	s.skipEntitlementsForResourceType.Store(r.GetId().GetResourceType(), skipEntitlements)

	return skipEntitlements, nil
}

// SyncEntitlements fetches the entitlements from the connector. It first lists each resource from the datastore,
// and pushes an action to fetch the entitlements for each resource.
func (s *syncer) SyncEntitlements(ctx context.Context, action *Action) error {
	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncEntitlements")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if action.ResourceTypeID == "" && action.ResourceID == "" {
		pageToken := action.PageToken

		if pageToken == "" {
			ctxzap.Extract(ctx).Info("Syncing entitlements...")
			s.handleInitialActionForStep(ctx, *action)
		}

		resp, err := s.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			PageToken:    pageToken,
			ActiveSyncId: s.getActiveSyncID(),
		}.Build())
		if err != nil {
			return err
		}

		actions := make([]Action, 0)
		for _, r := range resp.GetList() {
			shouldSkipEntitlements, err := s.shouldSkipEntitlements(ctx, r)
			if err != nil {
				return err
			}
			if shouldSkipEntitlements {
				continue
			}
			actions = append(actions, Action{Op: SyncEntitlementsOp, ResourceID: r.GetId().GetResource(), ResourceTypeID: r.GetId().GetResourceType()})
		}

		return s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken(), actions...)
	}

	err = s.syncEntitlementsForResource(ctx, action)
	if err != nil {
		return err
	}

	return nil
}

// syncEntitlementsForResource fetches the entitlements for a specific resource from the connector.
// No span here: only call site is SyncEntitlements, which already owns a span.
func (s *syncer) syncEntitlementsForResource(ctx context.Context, action *Action) error {
	resourceID := v2.ResourceId_builder{
		ResourceType: action.ResourceTypeID,
		Resource:     action.ResourceID,
	}.Build()
	resourceResponse, err := s.store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: resourceID,
	}.Build())
	if err != nil {
		return err
	}

	resource := resourceResponse.GetResource()

	start := time.Now()
	resp, err := s.connector.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource:     resource,
		PageToken:    action.PageToken,
		ActiveSyncId: s.getActiveSyncID(),
	}.Build())
	s.observeConnectorCall(ctx, "list-entitlements", start, action.ResourceTypeID, action.ResourceID)
	s.recordSessionUsage(resp.GetAnnotations())
	if err != nil {
		return err
	}
	// Filter before exclusion-group validation: a dropped entitlement must not
	// mutate exclusion-group state or fail the sync as if it had been ingested.
	entitlements, err := s.filterFreshEntitlements(ctx, resp.GetList())
	if err != nil {
		return fmt.Errorf("sync-entitlements: filtering disabled-type references: %w", err)
	}
	if err := s.validateEntitlementExclusionGroups(entitlements); err != nil {
		return err
	}
	err = s.store.PutEntitlements(ctx, entitlements...)
	if err != nil {
		return err
	}

	s.handleProgress(ctx, action, len(entitlements))
	if resp.GetNextPageToken() == "" {
		s.counts.AddEntitlementsProgress(resourceID.ResourceType, 1)
		s.counts.LogEntitlementsProgress(ctx, resourceID.GetResourceType())
	}

	return s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken())
}

func (s *syncer) SyncStaticEntitlements(ctx context.Context, action *Action) error {
	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncStaticEntitlements")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if action.ResourceTypeID != "" {
		return s.syncStaticEntitlementsForResourceType(ctx, action)
	}

	ctxzap.Extract(ctx).Info("Syncing static entitlements...")
	s.handleInitialActionForStep(ctx, *action)

	actions := make([]Action, 0)
	for rts, err := range s.listAllResourceTypes(ctx) {
		if err != nil {
			return err
		}
		for _, rt := range rts {
			// Queue up actions to sync static entitlements for each resource type
			actions = append(actions, Action{Op: SyncStaticEntitlementsOp, ResourceTypeID: rt.GetId()})
		}
	}

	return s.nextPageOrFinishAction(ctx, action, "", actions...)
}

func (s *syncer) syncStaticEntitlementsForResourceType(ctx context.Context, action *Action) error {
	ctx, span := tracer.Start(ctx, "syncer.syncStaticEntitlementsForResource")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	start := time.Now()
	resp, err := s.connector.ListStaticEntitlements(ctx, v2.EntitlementsServiceListStaticEntitlementsRequest_builder{
		ResourceTypeId: action.ResourceTypeID,
		PageToken:      action.PageToken,
		ActiveSyncId:   s.getActiveSyncID(),
	}.Build())
	s.observeConnectorCall(ctx, "list-static-entitlements", start, action.ResourceTypeID, action.ResourceID)
	s.recordSessionUsage(resp.GetAnnotations())
	if err != nil {
		// Ignore prefixError if we're calling a lambda with an old version of baton-sdk.
		if strings.Contains(err.Error(), `unable to resolve \"type.googleapis.com/c1.connector.v2.EntitlementsServiceListStaticEntitlementsRequest\": \"not found\"","errorType":"prefixError"`) {
			l := ctxzap.Extract(ctx)
			l.Info("ignoring prefixError when calling ListStaticEntitlements", zap.Error(err))
			s.state.FinishAction(ctx, action)
			return nil
		}

		return err
	}

	for _, ent := range resp.GetList() {
		resourcePageToken := ""
		for {
			// Get all resources of resource type and create entitlements for each one.
			resourcesResp, err := s.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
				ResourceTypeId: action.ResourceTypeID,
				PageToken:      resourcePageToken,
				ActiveSyncId:   s.getActiveSyncID(),
			}.Build())
			if err != nil {
				return err
			}

			annos := annotations.Annotations(ent.GetAnnotations())
			exclusionGroup := &v2.EntitlementExclusionGroup{}
			hasExclusionGroup, err := annos.Pick(exclusionGroup)
			if err != nil {
				return err
			}
			baseExclusionGroupID := exclusionGroup.GetExclusionGroupId()

			entitlements := []*v2.Entitlement{}
			for _, resource := range resourcesResp.GetList() {
				displayName := ent.GetDisplayName()
				if displayName == "" {
					displayName = resource.GetDisplayName()
				}
				description := ent.GetDescription()
				if description == "" {
					description = resource.GetDescription()
				}

				if hasExclusionGroup && exclusionGroup.GetScopeToResource() {
					exclusionGroup.SetExclusionGroupId(baseExclusionGroupID + "-" + resource.GetId().GetResource())
					annos.Update(exclusionGroup)
				}

				entID := entitlement.NewEntitlementID(resource, ent.GetSlug())
				if hasExclusionGroup {
					if err := s.recordEntitlementExclusionGroup(exclusionGroup, entID, resource.GetId().GetResourceType()); err != nil {
						return err
					}
				}

				entitlements = append(entitlements, &v2.Entitlement{
					Resource:    resource,
					Id:          entID,
					DisplayName: displayName,
					Description: description,
					GrantableTo: ent.GetGrantableTo(),
					Annotations: annos,
					Slug:        ent.GetSlug(),
					Purpose:     ent.GetPurpose(),
				})
			}
			err = s.store.PutEntitlements(ctx, entitlements...)
			if err != nil {
				return err
			}
			resourcePageToken = resourcesResp.GetNextPageToken()
			if resourcePageToken == "" {
				break
			}
		}
	}

	s.handleProgress(ctx, action, len(resp.GetList()))

	return s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken())
}

// syncAssetsForResource looks up a resource given the input ID. From there it looks to see if there are any traits that
// include references to an asset. For each AssetRef, we then call GetAsset on the connector and stream the asset from the connector.
// Once we have the entire asset, we put it in the database.
func (s *syncer) syncAssetsForResource(ctx context.Context, action *Action) error {
	ctx, span := tracer.Start(ctx, "syncer.syncAssetsForResource")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)
	resourceResponse, err := s.store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: action.ResourceTypeID,
			Resource:     action.ResourceID,
		}.Build(),
	}.Build())
	if err != nil {
		return err
	}

	var assetRefs []*v2.AssetRef

	rAnnos := annotations.Annotations(resourceResponse.GetResource().GetAnnotations())

	// Icons live on the resource; resource.GetIcon falls back to the
	// deprecated trait-level icons for resources emitted by older connectors.
	assetRefs = append(assetRefs, resource.GetIcon(resourceResponse.GetResource()))

	appTrait := &v2.AppTrait{}
	ok, err := rAnnos.Pick(appTrait)
	if err != nil {
		return err
	}
	if ok {
		assetRefs = append(assetRefs, appTrait.GetLogo())
	}

	for _, assetRef := range assetRefs {
		if assetRef == nil {
			continue
		}

		err := func() error {
			l.Debug("fetching asset", zap.String("asset_ref_id", assetRef.GetId()))
			start := time.Now()
			defer s.observeConnectorCall(ctx, "get-asset", start, action.ResourceTypeID, action.ResourceID)

			resp, err := s.connector.GetAsset(ctx, v2.AssetServiceGetAssetRequest_builder{Asset: assetRef}.Build())
			if err != nil {
				return err
			}

			// FIXME(jirwin): if the return from the client is nil, skip this asset
			// Temporary until we can implement assets on the platform side
			if resp == nil {
				return nil
			}

			var metadata *v2.AssetServiceGetAssetResponse_Metadata
			assetBytes := &bytes.Buffer{}

			var recvErr error
			var msg *v2.AssetServiceGetAssetResponse
			for !errors.Is(recvErr, io.EOF) {
				msg, recvErr = resp.Recv()
				if recvErr != nil {
					if errors.Is(recvErr, io.EOF) {
						continue
					}
					l.Error("error fetching asset", zap.Error(recvErr))
					return recvErr
				}

				l.Debug("received asset message")

				switch msg.WhichMsg() {
				case v2.AssetServiceGetAssetResponse_Metadata_case:
					metadata = msg.GetMetadata()
				case v2.AssetServiceGetAssetResponse_Data_case:
					l.Debug("Received data for asset")
					_, err := io.Copy(assetBytes, bytes.NewReader(msg.GetData().GetData()))
					if err != nil {
						_ = resp.CloseSend()
						return err
					}
				case v2.AssetServiceGetAssetResponse_Msg_not_set_case:
					l.Debug("Received unset asset message")
					continue
				}
			}

			if metadata == nil {
				return fmt.Errorf("no metadata received, unable to store asset")
			}

			return s.store.PutAsset(ctx, assetRef, metadata.GetContentType(), assetBytes.Bytes())
		}()
		if err != nil {
			return err
		}
	}

	s.state.FinishAction(ctx, action)
	return nil
}

// SyncAssets iterates each resource in the data store, and adds an action to fetch all of the assets for that resource.
func (s *syncer) SyncAssets(ctx context.Context, action *Action) error {
	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncAssets")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if action.ResourceTypeID == "" && action.ResourceID == "" {
		if action.PageToken == "" {
			ctxzap.Extract(ctx).Info("Syncing assets...")
			s.handleInitialActionForStep(ctx, *action)
		}

		resp, err := s.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			PageToken:    action.PageToken,
			ActiveSyncId: s.getActiveSyncID(),
		}.Build())
		if err != nil {
			return err
		}

		actions := make([]Action, 0)
		for _, r := range resp.GetList() {
			actions = append(actions, Action{Op: SyncAssetsOp, ResourceID: r.GetId().GetResource(), ResourceTypeID: r.GetId().GetResourceType()})
		}

		return s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken(), actions...)
	}

	err = s.syncAssetsForResource(ctx, action)
	if err != nil {
		ctxzap.Extract(ctx).Error("error syncing assets", zap.Error(err))
		return err
	}

	return nil
}

// SyncGrantExpansion handles the grant expansion phase of sync.
// It first loads the entitlement graph from grants, fixes any cycles, then runs expansion.
func (s *syncer) SyncGrantExpansion(ctx context.Context, action *Action) error {
	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncGrantExpansion")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	entitlementGraph := s.state.EntitlementGraph(ctx)

	// Phase 1: Load the entitlement graph from grants (paginated)
	if !entitlementGraph.Loaded {
		err := s.loadEntitlementGraph(ctx, action, entitlementGraph)
		if err != nil {
			return err
		}
		return nil
	}

	// Phase 2: Fix cycles in the graph (only runs once after loading completes)
	if !entitlementGraph.HasNoCycles {
		err := s.fixEntitlementGraphCycles(ctx, entitlementGraph)
		if err != nil {
			return err
		}
	}

	// Phase 3: Run the expansion algorithm
	err = s.expandGrantsForEntitlements(ctx, action)
	if err != nil {
		return err
	}

	return nil
}

// loadEntitlementGraph loads one page of expandable grants and adds relationships to the graph.
// This method handles pagination via the syncer's state machine.
func (s *syncer) loadEntitlementGraph(ctx context.Context, action *Action, graph *expand.EntitlementGraph) error {
	l := ctxzap.Extract(ctx)
	if action.PageToken == "" {
		l.Info("Expanding grants...")
		s.handleInitialActionForStep(ctx, *action)
	}

	// Read expansion metadata directly from SQL columns, avoiding the
	// cost of unmarshalling full grant protos. One page per action step
	// so the action state machine can checkpoint progress.
	page, nextPageToken, err := s.store.Grants().PendingExpansionPage(ctx, action.PageToken)
	if err != nil {
		return err
	}

	for _, def := range page {
		dstEntitlementID := def.TargetEntitlementID

		for _, srcEntitlementID := range def.Annotation.GetEntitlementIds() {
			// Validate that the source entitlement's resource matches the grant's principal.
			srcEntitlement, err := s.store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
				EntitlementId: srcEntitlementID,
			}.Build())
			if err != nil {
				// Only skip not-found entitlements; propagate other errors
				// to avoid silently dropping edges and yielding incorrect expansions.
				if status.Code(err) == codes.NotFound {
					l.Debug("source entitlement not found, skipping edge",
						zap.String("src_entitlement_id", srcEntitlementID),
						zap.String("dst_entitlement_id", dstEntitlementID),
					)
					continue
				}
				l.Error("error fetching source entitlement",
					zap.String("src_entitlement_id", srcEntitlementID),
					zap.String("dst_entitlement_id", dstEntitlementID),
					zap.Error(err),
				)
				return err
			}

			sourceEntitlementResourceID := srcEntitlement.GetEntitlement().GetResource().GetId()
			if sourceEntitlementResourceID == nil {
				return fmt.Errorf("source entitlement resource id was nil")
			}
			if def.PrincipalResourceTypeID != sourceEntitlementResourceID.GetResourceType() ||
				def.PrincipalResourceID != sourceEntitlementResourceID.GetResource() {
				l.Error(
					"source entitlement resource id did not match grant principal id",
					zap.String("grant_principal_resource_type_id", def.PrincipalResourceTypeID),
					zap.String("grant_principal_resource_id", def.PrincipalResourceID),
					zap.String("source_entitlement_resource_id", sourceEntitlementResourceID.String()))

				return fmt.Errorf("source entitlement resource id did not match grant principal id")
			}

			graph.AddEntitlementID(dstEntitlementID)
			graph.AddEntitlementID(srcEntitlementID)
			err = graph.AddEdge(ctx, srcEntitlementID, dstEntitlementID, def.Annotation.GetShallow(), def.Annotation.GetResourceTypeIds())
			if err != nil {
				return fmt.Errorf("error adding edge to graph: %w", err)
			}
		}
	}

	// Handle pagination
	if nextPageToken != "" {
		if err := s.state.NextPage(ctx, action.ID, nextPageToken); err != nil {
			return err
		}
	} else {
		graph.Loaded = true
		l.Info("Finished loading entitlement graph", zap.Int("edges", len(graph.Edges)))
	}
	return nil
}

// fixEntitlementGraphCycles detects and fixes cycles in the entitlement graph.
func (s *syncer) fixEntitlementGraphCycles(ctx context.Context, graph *expand.EntitlementGraph) error {
	l := ctxzap.Extract(ctx)

	comps, sccMetrics := graph.ComputeCyclicComponents(ctx)
	if len(comps) == 0 {
		graph.HasNoCycles = true
		return nil
	}
	l.Warn(
		"cycle detected in entitlement graph",
		zap.Any("cycle", comps[0]),
		zap.Any("scc_metrics", sccMetrics),
	)
	l.Debug("initial graph stats",
		zap.Int("edges", len(graph.Edges)),
		zap.Int("nodes", len(graph.Nodes)),
		zap.Int("actions", len(graph.Actions)),
		zap.Int("depth", graph.Depth),
		zap.Bool("has_no_cycles", graph.HasNoCycles),
	)
	if dontFixCycles {
		return fmt.Errorf("cycles detected in entitlement graph")
	}
	return graph.FixCyclesFromComponents(ctx, comps)
}

// SyncGrants fetches the grants for each resource from the connector. It iterates each resource
// from the datastore, and pushes a new action to sync the grants for each individual resource.
func (s *syncer) SyncGrants(ctx context.Context, action *Action) error {
	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncGrants")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if action.ResourceTypeID == "" && action.ResourceID == "" {
		if action.PageToken == "" {
			ctxzap.Extract(ctx).Info("Syncing grants...")
			s.handleInitialActionForStep(ctx, *action)
		}

		resp, err := s.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			PageToken:    action.PageToken,
			ActiveSyncId: s.getActiveSyncID(),
		}.Build())
		if err != nil {
			return fmt.Errorf("sync-grants: error listing resources: %w", err)
		}

		actions := make([]Action, 0)
		for _, r := range resp.GetList() {
			shouldSkip, err := s.shouldSkipGrants(ctx, r)
			if err != nil {
				return err
			}

			if shouldSkip {
				continue
			}
			actions = append(actions, Action{Op: SyncGrantsOp, ResourceID: r.GetId().GetResource(), ResourceTypeID: r.GetId().GetResourceType()})
		}

		return s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken(), actions...)
	}
	err = s.syncGrantsForResource(ctx, action)
	if err != nil {
		return err
	}

	return nil
}

// syncGrantsForResource fetches the grants for a specific resource from the connector.
// No span here: only call site is SyncGrants, which already owns a span.
func (s *syncer) syncGrantsForResource(ctx context.Context, action *Action) error {
	resourceID := v2.ResourceId_builder{
		ResourceType: action.ResourceTypeID,
		Resource:     action.ResourceID,
	}.Build()
	resourceResponse, err := s.store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: resourceID,
	}.Build())
	if err != nil {
		return fmt.Errorf("sync-grants-for-resource: error getting resource: %w", err)
	}

	resource := resourceResponse.GetResource()

	start := time.Now()
	resp, err := s.connector.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource:     resource,
		PageToken:    action.PageToken,
		ActiveSyncId: s.getActiveSyncID(),
	}.Build())
	s.observeConnectorCall(ctx, "list-grants", start, action.ResourceTypeID, action.ResourceID)
	s.recordSessionUsage(resp.GetAnnotations())
	if err != nil {
		return fmt.Errorf("sync-grants-for-resource: error listing grants: %w", err)
	}

	grants := resp.GetList()

	l := ctxzap.Extract(ctx)
	resourcesToInsertMap := make(map[string]*v2.Resource, 0)
	respAnnos := annotations.Annotations(resp.GetAnnotations())
	insertResourceGrants := respAnnos.Contains(&v2.InsertResourceGrants{})

	// Stamp InsertResourceGrants per-grant so the slim-blob writer's
	// gate sees it. The annotation is response-level, but the writer
	// needs it per-row to avoid stripping the Resource this path
	// subsequently writes to v1_resources.
	//
	// Aliasing the same *anypb.Any across grants is safe — Any is
	// treated as immutable downstream. Avoids the per-grant proto
	// marshal that annotations.Update would do.
	if insertResourceGrants {
		insertResourceGrantsSentinel := &v2.InsertResourceGrants{}
		var insertAny *anypb.Any
		insertAny, err = anypb.New(insertResourceGrantsSentinel)
		if err != nil {
			return fmt.Errorf("error marshaling InsertResourceGrants annotation: %w", err)
		}
		for _, g := range grants {
			annos := annotations.Annotations(g.GetAnnotations())
			if annos.Contains(insertResourceGrantsSentinel) {
				continue
			}
			g.SetAnnotations(append(annos, insertAny))
		}

		// Collect grant-discovered resources before fresh-ingest filtering:
		// a resource of a scheduled type is a legitimate discovery (uplift
		// creates an AppResource for it) even when the discovering grant is
		// dropped for an unscheduled principal type. Resources of unscheduled
		// types are skipped — uplift can't resolve their type, so the row
		// would be dead data.
		for _, g := range grants {
			resource := g.GetEntitlement().GetResource()
			ok, err := s.filterFreshGrantResource(ctx, resource)
			if err != nil {
				return fmt.Errorf("sync-grants-for-resource: filtering grant-discovered resource: %w", err)
			}
			if !ok {
				continue
			}
			bid, err := bid.MakeBid(resource)
			if err != nil {
				return err
			}
			resourcesToInsertMap[bid] = resource
		}
	}

	grants, err = s.filterFreshGrants(ctx, grants)
	if err != nil {
		return fmt.Errorf("sync-grants-for-resource: filtering disabled-type references: %w", err)
	}

	for _, grant := range grants {
		grantAnnos := annotations.Annotations(grant.GetAnnotations())
		if !s.dontExpandGrants && grantAnnos.Contains(&v2.GrantExpandable{}) {
			s.state.SetNeedsExpansion()
		}
		if grantAnnos.ContainsAny(&v2.ExternalResourceMatchAll{}, &v2.ExternalResourceMatch{}, &v2.ExternalResourceMatchID{}) {
			s.state.SetHasExternalResourcesGrants()
		}

		if !s.state.ShouldFetchRelatedResources() {
			continue
		}
		// Some connectors emit grants for other resources. If we're doing a partial sync, check if it exists and queue a fetch if not.
		entitlementResource := grant.GetEntitlement().GetResource()
		_, err := s.store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
			ResourceId: entitlementResource.GetId(),
		}.Build())
		if err != nil {
			if status.Code(err) != codes.NotFound {
				return err
			}

			erId := entitlementResource.GetId()
			prId := entitlementResource.GetParentResourceId()
			resource, err := s.getResourceFromConnector(ctx, erId, prId)
			if err != nil {
				l.Error("error fetching entitlement resource", zap.Error(err))
				return err
			}
			if resource == nil {
				continue
			}
			if err := s.store.PutResources(ctx, resource); err != nil {
				return err
			}
		}
	}

	if len(resourcesToInsertMap) > 0 {
		resourcesToInsert := make([]*v2.Resource, 0)
		for _, resource := range resourcesToInsertMap {
			resourcesToInsert = append(resourcesToInsert, resource)
		}
		err = s.store.PutResources(ctx, resourcesToInsert...)
		if err != nil {
			return fmt.Errorf("sync-grants-for-resource: error putting resources: %w", err)
		}
	}

	err = s.store.PutGrants(ctx, grants...)
	if err != nil {
		return fmt.Errorf("sync-grants-for-resource: error putting grants: %w", err)
	}

	s.handleProgress(ctx, action, len(grants))

	if resp.GetNextPageToken() == "" {
		s.counts.AddGrantsProgress(resourceID.GetResourceType(), 1)
		s.counts.LogGrantsProgress(ctx, resourceID.GetResourceType())
	}

	return s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken())
}

func (s *syncer) SyncExternalResources(ctx context.Context, action *Action) error {
	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncExternalResources")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)
	l.Info("Syncing external resources")

	if s.externalResourceEntitlementIdFilter != "" {
		err := s.SyncExternalResourcesWithGrantToEntitlement(ctx, s.externalResourceEntitlementIdFilter)
		if err != nil {
			return err
		}
	} else {
		err := s.SyncExternalResourcesUsersAndGroups(ctx)
		if err != nil {
			return err
		}
	}
	s.state.FinishAction(ctx, action)
	return nil
}

func (s *syncer) SyncExternalResourcesWithGrantToEntitlement(ctx context.Context, entitlementId string) error {
	ctx, span := tracer.Start(ctx, "syncer.SyncExternalResourcesWithGrantToEntitlement")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)
	l.Info("Syncing external baton resources with grants to entitlement...")

	skipEGForResourceType := make(map[string]bool)

	filterEntitlement, err := s.externalResourceReader.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: entitlementId,
	}.Build())
	if err != nil {
		return err
	}

	resourceTypeIDs := mapset.NewSet[string]()
	resourceIDs := make(map[string]*v2.ResourceId)

	for grants, err := range s.listExternalGrantsForEntitlement(ctx, filterEntitlement.GetEntitlement()) {
		if err != nil {
			return err
		}
		for _, g := range grants {
			resourceTypeIDs.Add(g.GetPrincipal().GetId().GetResourceType())
			resourceIDs[g.GetPrincipal().GetId().GetResource()] = g.GetPrincipal().GetId()
		}
	}

	resourceTypes := make([]*v2.ResourceType, 0)
	for _, resourceTypeId := range resourceTypeIDs.ToSlice() {
		resourceTypeResp, err := s.externalResourceReader.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{ResourceTypeId: resourceTypeId}.Build())
		if err != nil {
			return err
		}
		// Should we error or skip if this is not user or group?
		for _, t := range resourceTypeResp.GetResourceType().GetTraits() {
			if t == v2.ResourceType_TRAIT_USER || t == v2.ResourceType_TRAIT_GROUP {
				resourceTypes = append(resourceTypes, resourceTypeResp.GetResourceType())
				continue
			}
		}

		rtAnnos := annotations.Annotations(resourceTypeResp.GetResourceType().GetAnnotations())
		skipEntitlements := rtAnnos.Contains(&v2.SkipEntitlementsAndGrants{})
		skipEGForResourceType[resourceTypeResp.GetResourceType().GetId()] = skipEntitlements
	}

	err = s.store.PutResourceTypes(ctx, resourceTypes...)
	if err != nil {
		return err
	}

	principals := make([]*v2.Resource, 0)
	for _, resourceId := range resourceIDs {
		resourceResp, err := s.externalResourceReader.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{ResourceId: resourceId}.Build())
		if err != nil {
			if status.Code(err) == codes.NotFound {
				l.Debug(
					"resource was not found in external sync",
					zap.String("resource_id", resourceId.GetResource()),
					zap.String("resource_type_id", resourceId.GetResourceType()),
				)
				continue
			}
			return err
		}
		resourceVal := resourceResp.GetResource()
		resourceAnnos := annotations.Annotations(resourceVal.GetAnnotations())
		batonID := &v2.BatonID{}
		resourceAnnos.Update(batonID)
		resourceVal.SetAnnotations(resourceAnnos)
		principals = append(principals, resourceVal)
	}

	err = s.store.PutResources(ctx, principals...)
	if err != nil {
		return err
	}

	entsCount := 0
	ents := make([]*v2.Entitlement, 0)
	for _, principal := range principals {
		rAnnos := annotations.Annotations(principal.GetAnnotations())
		skipEnts := skipEGForResourceType[principal.GetId().GetResourceType()] || rAnnos.Contains(&v2.SkipEntitlementsAndGrants{})
		if skipEnts {
			continue
		}

		resourceEnts, err := s.listExternalEntitlementsForResource(ctx, principal)
		if err != nil {
			return err
		}
		ents = append(ents, resourceEnts...)
		entsCount += len(resourceEnts)
	}

	err = s.store.PutEntitlements(ctx, ents...)
	if err != nil {
		return err
	}

	grantsForEntsCount := 0
	for _, ent := range ents {
		rAnnos := annotations.Annotations(ent.GetResource().GetAnnotations())
		if rAnnos.Contains(&v2.SkipGrants{}) {
			continue
		}
		for grants, err := range s.listExternalGrantsForEntitlement(ctx, ent) {
			if err != nil {
				return err
			}
			grantsForEntsCount += len(grants)
			err = s.store.PutGrants(ctx, grants...)
			if err != nil {
				return err
			}
		}
	}

	l.Info("Synced external resources for entitlement",
		zap.Int("resource_type_count", len(resourceTypes)),
		zap.Int("resource_count", len(principals)),
		zap.Int("entitlement_count", entsCount),
		zap.Int("grant_count", grantsForEntsCount),
	)

	err = s.processGrantsWithExternalPrincipals(ctx, principals)
	if err != nil {
		return err
	}

	return nil
}

func (s *syncer) SyncExternalResourcesUsersAndGroups(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.SyncExternalResourcesUsersAndGroups")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)
	l.Info("Syncing external resources for users and groups...")

	skipEGForResourceType := make(map[string]bool)

	resourceTypes, err := s.listExternalResourceTypes(ctx)
	if err != nil {
		return err
	}

	userAndGroupResourceTypes := make([]*v2.ResourceType, 0)
	ents := make([]*v2.Entitlement, 0)
	principals := make([]*v2.Resource, 0)
	for _, rt := range resourceTypes {
		for _, t := range rt.GetTraits() {
			if t == v2.ResourceType_TRAIT_USER || t == v2.ResourceType_TRAIT_GROUP {
				userAndGroupResourceTypes = append(userAndGroupResourceTypes, rt)
				continue
			}
		}
	}

	err = s.store.PutResourceTypes(ctx, userAndGroupResourceTypes...)
	if err != nil {
		return err
	}

	for _, rt := range userAndGroupResourceTypes {
		rtAnnos := annotations.Annotations(rt.GetAnnotations())
		skipEntitlements := rtAnnos.Contains(&v2.SkipEntitlementsAndGrants{})
		skipEGForResourceType[rt.GetId()] = skipEntitlements

		resourceListResp, err := s.listExternalResourcesForResourceType(ctx, rt.GetId())
		if err != nil {
			return err
		}

		for _, resourceVal := range resourceListResp {
			resourceAnnos := annotations.Annotations(resourceVal.GetAnnotations())
			batonID := &v2.BatonID{}
			resourceAnnos.Update(batonID)
			resourceVal.SetAnnotations(resourceAnnos)
			principals = append(principals, resourceVal)
		}
	}

	err = s.store.PutResources(ctx, principals...)
	if err != nil {
		return err
	}

	entsCount := 0
	principalsCount := len(principals)
	for _, principal := range principals {
		skipEnts := skipEGForResourceType[principal.GetId().GetResourceType()]
		if skipEnts {
			continue
		}
		rAnnos := annotations.Annotations(principal.GetAnnotations())
		if rAnnos.Contains(&v2.SkipEntitlementsAndGrants{}) {
			continue
		}

		resourceEnts, err := s.listExternalEntitlementsForResource(ctx, principal)
		if err != nil {
			return err
		}
		ents = append(ents, resourceEnts...)
		entsCount += len(resourceEnts)
		err = s.store.PutEntitlements(ctx, resourceEnts...)
		if err != nil {
			return err
		}
	}

	grantsForEntsCount := 0
	for _, ent := range ents {
		rAnnos := annotations.Annotations(ent.GetResource().GetAnnotations())
		if rAnnos.Contains(&v2.SkipGrants{}) {
			continue
		}
		for grants, err := range s.listExternalGrantsForEntitlement(ctx, ent) {
			if err != nil {
				return err
			}
			grantsForEntsCount += len(grants)
			err = s.store.PutGrants(ctx, grants...)
			if err != nil {
				return err
			}
		}
	}

	l.Info("Synced external resources",
		zap.Int("resource_type_count", len(userAndGroupResourceTypes)),
		zap.Int("resource_count", principalsCount),
		zap.Int("entitlement_count", entsCount),
		zap.Int("grant_count", grantsForEntsCount),
	)

	err = s.processGrantsWithExternalPrincipals(ctx, principals)
	if err != nil {
		return err
	}

	return nil
}

func (s *syncer) listExternalResourcesForResourceType(ctx context.Context, resourceTypeId string) ([]*v2.Resource, error) {
	resources := make([]*v2.Resource, 0)
	pageToken := ""
	for {
		resourceResp, err := s.externalResourceReader.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			PageToken:      pageToken,
			ResourceTypeId: resourceTypeId,
		}.Build())
		if err != nil {
			return nil, err
		}
		resources = append(resources, resourceResp.GetList()...)
		pageToken = resourceResp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return resources, nil
}

func (s *syncer) listExternalEntitlementsForResource(ctx context.Context, resource *v2.Resource) ([]*v2.Entitlement, error) {
	ents := make([]*v2.Entitlement, 0)
	entitlementToken := ""

	for {
		entitlementsList, err := s.externalResourceReader.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
			PageToken: entitlementToken,
			Resource:  resource,
		}.Build())
		if err != nil {
			return nil, err
		}
		ents = append(ents, entitlementsList.GetList()...)
		entitlementToken = entitlementsList.GetNextPageToken()
		if entitlementToken == "" {
			break
		}
	}
	for _, ent := range ents {
		annos := annotations.Annotations(ent.GetAnnotations())
		annos.Update(&v2.EntitlementImmutable{})
		ent.SetAnnotations(annos)
	}
	return ents, nil
}

func (s *syncer) listExternalGrantsForEntitlement(ctx context.Context, ent *v2.Entitlement) iter.Seq2[[]*v2.Grant, error] {
	return func(yield func([]*v2.Grant, error) bool) {
		pageToken := ""
		for {
			grantsForEntitlementResp, err := s.externalResourceReader.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
				Entitlement: ent,
				PageToken:   pageToken,
			}.Build())
			if err != nil {
				_ = yield(nil, err)
				return
			}
			grants := grantsForEntitlementResp.GetList()
			if len(grants) > 0 {
				// Add immutable annotation to external resource grants.
				for _, grant := range grants {
					annos := annotations.Annotations(grant.GetAnnotations())
					annos.Update(&v2.GrantImmutable{})
					grant.SetAnnotations(annos)
				}
				if !yield(grants, err) {
					return
				}
			}
			pageToken = grantsForEntitlementResp.GetNextPageToken()
			if pageToken == "" {
				return
			}
		}
	}
}

func (s *syncer) listExternalResourceTypes(ctx context.Context) ([]*v2.ResourceType, error) {
	resourceTypes := make([]*v2.ResourceType, 0)
	rtPageToken := ""
	for {
		resourceTypesResp, err := s.externalResourceReader.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
			PageToken: rtPageToken,
		}.Build())
		if err != nil {
			return nil, err
		}
		resourceTypes = append(resourceTypes, resourceTypesResp.GetList()...)
		rtPageToken = resourceTypesResp.GetNextPageToken()
		if rtPageToken == "" {
			break
		}
	}
	return resourceTypes, nil
}

func (s *syncer) processGrantsWithExternalPrincipals(ctx context.Context, principals []*v2.Resource) error {
	ctx, span := tracer.Start(ctx, "processGrantsWithExternalPrincipals")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if !s.state.HasExternalResourcesGrants() {
		return nil
	}

	l := ctxzap.Extract(ctx)

	groupPrincipals := make([]*v2.Resource, 0)
	userPrincipals := make([]*v2.Resource, 0)
	principalMap := make(map[string]*v2.Resource)

	for _, principal := range principals {
		rAnnos := annotations.Annotations(principal.GetAnnotations())
		batonID := &v2.BatonID{}
		if !rAnnos.Contains(batonID) {
			continue
		}
		if rAnnos.Contains(&v2.UserTrait{}) {
			userPrincipals = append(userPrincipals, principal)
		}
		if rAnnos.Contains(&v2.GroupTrait{}) {
			groupPrincipals = append(groupPrincipals, principal)
		}
		principalID := principal.GetId().GetResource()
		if principalID == "" {
			l.Error("principal resource id was empty")
			continue
		}
		principalMap[principalID] = principal
	}

	grantsToDelete := make([]*v2.Grant, 0)
	expandedGrants := make([]*v2.Grant, 0)

	for ga, err := range s.store.Grants().ListWithAnnotations(ctx) {
		if err != nil {
			return err
		}

		grant := ga.Grant
		annos := annotations.Annotations(grant.GetAnnotations())
		if !annos.ContainsAny(&v2.ExternalResourceMatchAll{}, &v2.ExternalResourceMatch{}, &v2.ExternalResourceMatchID{}) {
			continue
		}

		// Match all
		matchResourceMatchAllAnno, err := GetExternalResourceMatchAllAnnotation(annos)
		if err != nil {
			return err
		}
		if matchResourceMatchAllAnno != nil {
			var processPrincipals []*v2.Resource
			switch matchResourceMatchAllAnno.GetResourceType() {
			case v2.ResourceType_TRAIT_USER:
				processPrincipals = userPrincipals
			case v2.ResourceType_TRAIT_GROUP:
				processPrincipals = groupPrincipals
			default:
				l.Error("unexpected external resource type trait", zap.Any("trait", matchResourceMatchAllAnno.GetResourceType()))
			}
			for _, principal := range processPrincipals {
				newGrant := newGrantForExternalPrincipal(grant, principal)
				expandedGrants = append(expandedGrants, newGrant)
			}
			grantsToDelete = append(grantsToDelete, grant)
			continue
		}

		// Expansion annotation (may be nil for non-expandable grants).
		expandableAnno := ga.Annotation
		expandableEntitlementsResourceMap := make(map[string][]string)
		if expandableAnno != nil {
			for _, entId := range expandableAnno.GetEntitlementIds() {
				parsedEnt, err := bid.ParseEntitlementBid(entId)
				if err != nil {
					l.Error("error parsing expandable entitlement bid", zap.Any("entitlementId", entId))
					continue
				}
				resourceBID, err := bid.MakeBid(parsedEnt.GetResource())
				if err != nil {
					l.Error("error making resource bid", zap.Any("parsedEnt.Resource", parsedEnt.GetResource()))
					continue
				}

				slugs, ok := expandableEntitlementsResourceMap[resourceBID]
				if !ok {
					slugs = make([]string, 0)
				}
				slugs = append(slugs, parsedEnt.GetSlug())
				expandableEntitlementsResourceMap[resourceBID] = slugs
			}
		}

		// Match by ID
		matchResourceMatchIDAnno, err := GetExternalResourceMatchIDAnnotation(annos)
		if err != nil {
			return err
		}
		if matchResourceMatchIDAnno != nil {
			if principal, ok := principalMap[matchResourceMatchIDAnno.GetId()]; ok {
				newGrant := newGrantForExternalPrincipal(grant, principal)
				expandedGrants = append(expandedGrants, newGrant)

				newGrantAnnos := annotations.Annotations(newGrant.GetAnnotations())

				newExpandableEntitlementIDs := make([]string, 0)
				if expandableAnno != nil {
					groupPrincipalBID, err := bid.MakeBid(grant.GetPrincipal())
					if err != nil {
						l.Error("error making group principal bid", zap.Error(err), zap.Any("grant.Principal", grant.GetPrincipal()))
						continue
					}

					principalEntitlementSlugs := expandableEntitlementsResourceMap[groupPrincipalBID]
					for _, slug := range principalEntitlementSlugs {
						newExpandableEntId := entitlement.NewEntitlementID(principal, slug)
						_, err := s.store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{EntitlementId: newExpandableEntId}.Build())
						if err != nil {
							if status.Code(err) == codes.NotFound {
								l.Error("found no entitlement with entitlement id generated from external source sync", zap.Any("entitlementId", newExpandableEntId))
								continue
							}
							return err
						}
						newExpandableEntitlementIDs = append(newExpandableEntitlementIDs, newExpandableEntId)
					}

					newExpandableAnno := v2.GrantExpandable_builder{
						EntitlementIds:  newExpandableEntitlementIDs,
						Shallow:         expandableAnno.GetShallow(),
						ResourceTypeIds: expandableAnno.GetResourceTypeIds(),
					}.Build()
					newGrantAnnos.Update(newExpandableAnno)
					newGrant.SetAnnotations(newGrantAnnos)
					expandedGrants = append(expandedGrants, newGrant)
				}
			}

			// We still want to delete the grant even if there are no matches
			// Since it does not correspond to any known user
			grantsToDelete = append(grantsToDelete, grant)
		}

		// Match by key/val
		matchExternalResource, err := GetExternalResourceMatchAnnotation(annos)
		if err != nil {
			return err
		}

		if matchExternalResource != nil {
			switch matchExternalResource.GetResourceType() {
			case v2.ResourceType_TRAIT_USER:
				for _, userPrincipal := range userPrincipals {
					userTrait, err := resource.GetUserTrait(userPrincipal)
					if err != nil {
						l.Error("error getting user trait", zap.Any("userPrincipal", userPrincipal))
						continue
					}
					if matchExternalResource.GetKey() == "email" {
						if userTraitContainsEmail(userTrait.GetEmails(), matchExternalResource.GetValue()) {
							newGrant := newGrantForExternalPrincipal(grant, userPrincipal)
							expandedGrants = append(expandedGrants, newGrant)
							// continue to next principal since we found an email match
							continue
						}
					}
					profileVal, ok := resource.GetProfileStringValue(resource.GetProfile(userPrincipal), matchExternalResource.GetKey())
					if ok && strings.EqualFold(profileVal, matchExternalResource.GetValue()) {
						newGrant := newGrantForExternalPrincipal(grant, userPrincipal)
						expandedGrants = append(expandedGrants, newGrant)
					}
				}
			case v2.ResourceType_TRAIT_GROUP:
				for _, groupPrincipal := range groupPrincipals {
					profileVal, ok := resource.GetProfileStringValue(resource.GetProfile(groupPrincipal), matchExternalResource.GetKey())
					if ok && strings.EqualFold(profileVal, matchExternalResource.GetValue()) {
						newGrant := newGrantForExternalPrincipal(grant, groupPrincipal)
						newGrantAnnos := annotations.Annotations(newGrant.GetAnnotations())

						newExpandableEntitlementIDs := make([]string, 0)
						if expandableAnno != nil {
							groupPrincipalBID, err := bid.MakeBid(grant.GetPrincipal())
							if err != nil {
								l.Error("error making group principal bid", zap.Error(err), zap.Any("grant.Principal", grant.GetPrincipal()))
								continue
							}

							principalEntitlementSlugs := expandableEntitlementsResourceMap[groupPrincipalBID]
							for _, slug := range principalEntitlementSlugs {
								newExpandableEntId := entitlement.NewEntitlementID(groupPrincipal, slug)
								_, err := s.store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{EntitlementId: newExpandableEntId}.Build())
								if err != nil {
									if status.Code(err) == codes.NotFound {
										l.Error("found no entitlement with entitlement id generated from external source sync", zap.Any("entitlementId", newExpandableEntId))
										continue
									}
									return err
								}
								newExpandableEntitlementIDs = append(newExpandableEntitlementIDs, newExpandableEntId)
							}

							newExpandableAnno := v2.GrantExpandable_builder{
								EntitlementIds:  newExpandableEntitlementIDs,
								Shallow:         expandableAnno.GetShallow(),
								ResourceTypeIds: expandableAnno.GetResourceTypeIds(),
							}.Build()
							newGrantAnnos.Update(newExpandableAnno)
							newGrant.SetAnnotations(newGrantAnnos)
							expandedGrants = append(expandedGrants, newGrant)
						}
					}
				}
			default:
				l.Error("unexpected external resource type trait", zap.Any("trait", matchExternalResource.GetResourceType()))
			}

			// We still want to delete the grant even if there are no matches
			grantsToDelete = append(grantsToDelete, grant)
		}
	}

	newGrantIDs := mapset.NewSet[string]()
	for _, ng := range expandedGrants {
		newGrantIDs.Add(ng.GetId())
	}

	err = s.store.PutGrants(ctx, expandedGrants...)
	if err != nil {
		return err
	}

	// Prefer the refs-based delete (exact structural identity) when the
	// store supports it; external ids are a lossy external contract and
	// stores keyed by structural identity cannot always resolve them.
	refsDeleter, _ := s.store.(grantByRefsDeleter)
	for _, grantToDelete := range grantsToDelete {
		if newGrantIDs.ContainsOne(grantToDelete.GetId()) {
			continue
		}
		if refsDeleter != nil {
			err = refsDeleter.DeleteGrantByRefs(ctx, grantToDelete)
		} else {
			err = s.store.DeleteGrant(ctx, grantToDelete.GetId())
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func userTraitContainsEmail(emails []*v2.UserTrait_Email, address string) bool {
	return slices.ContainsFunc(emails, func(e *v2.UserTrait_Email) bool {
		return strings.EqualFold(e.GetAddress(), address)
	})
}

// grantByRefsDeleter is the optional store fast path for deleting a grant
// by its structured refs instead of its lossy public id (Pebble implements
// it; SQLite resolves ids by exact string and does not need it).
type grantByRefsDeleter interface {
	DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error
}

func newGrantForExternalPrincipal(grant *v2.Grant, principal *v2.Resource) *v2.Grant {
	newGrant := v2.Grant_builder{
		Entitlement: grant.GetEntitlement(),
		Principal:   principal,
		Id:          batonGrant.NewGrantID(principal, grant.GetEntitlement()),
		Sources:     grant.GetSources(),
		Annotations: grant.GetAnnotations(),
	}.Build()
	return newGrant
}

func GetExternalResourceMatchAllAnnotation(annos annotations.Annotations) (*v2.ExternalResourceMatchAll, error) {
	externalResourceMatchAll := &v2.ExternalResourceMatchAll{}
	ok, err := annos.Pick(externalResourceMatchAll)
	if err != nil || !ok {
		return nil, err
	}
	return externalResourceMatchAll, nil
}

func GetExternalResourceMatchAnnotation(annos annotations.Annotations) (*v2.ExternalResourceMatch, error) {
	externalResourceMatch := &v2.ExternalResourceMatch{}
	ok, err := annos.Pick(externalResourceMatch)
	if err != nil || !ok {
		return nil, err
	}
	return externalResourceMatch, nil
}

func GetExternalResourceMatchIDAnnotation(annos annotations.Annotations) (*v2.ExternalResourceMatchID, error) {
	externalResourceMatchID := &v2.ExternalResourceMatchID{}
	ok, err := annos.Pick(externalResourceMatchID)
	if err != nil || !ok {
		return nil, err
	}
	return externalResourceMatchID, nil
}

func GetExpandableAnnotation(annos annotations.Annotations) (*v2.GrantExpandable, error) {
	expandableAnno := &v2.GrantExpandable{}
	ok, err := annos.Pick(expandableAnno)
	if err != nil || !ok {
		return nil, err
	}
	return expandableAnno, nil
}

// expandGrantsForEntitlements expands grants for the given entitlement.
// This method delegates to the expand.Expander for the actual expansion logic.
func (s *syncer) expandGrantsForEntitlements(ctx context.Context, action *Action) error {
	ctx, span := tracer.Start(ctx, "syncer.expandGrantsForEntitlements")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)
	graph := s.state.EntitlementGraph(ctx)

	s.counts.LogExpandProgress(ctx, graph.Actions)

	// Create an expander and run a single step.
	// The expander needs Reader methods (on s.store) plus StoreExpandedGrants
	// (on s.store.Grants()). An inline adapter composes them so expand
	// stays decoupled from C1ZStore.
	expander := expand.NewExpander(expanderStoreAdapter{s.store}, graph)
	err = expander.RunSingleStep(ctx)
	if err != nil {
		l.Error("expandGrantsForEntitlements: error during expansion", zap.Error(err))
		// If max depth exceeded, finish the action before returning the error
		// to prevent the state machine from getting stuck
		if errors.Is(err, expand.ErrMaxDepthExceeded) {
			s.state.FinishAction(ctx, action)
		}
		return err
	}

	if expander.IsDone(ctx) {
		l.Debug("expandGrantsForEntitlements: graph is expanded")
		s.state.FinishAction(ctx, action)
	}

	return nil
}

func (s *syncer) loadStore(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.loadStore")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if s.store != nil {
		return nil
	}

	storeOpts := []dotc1z.C1ZOption{dotc1z.WithTmpDir(s.tmpDir)}
	if s.storageEngine != "" {
		storeOpts = append(storeOpts, dotc1z.WithEngine(s.storageEngine))
	}
	store, err := dotc1z.NewStore(ctx, s.c1zPath, storeOpts...)
	if err != nil {
		return err
	}

	if s.setSessionStore != nil {
		// Instrumented so session-store cost is attributable in the sync
		// stats instead of vanishing into inflated connector-call latency
		// (e.g. a broken backend whose every request times out before the
		// connector falls back to real work).
		kind := "c1z_" + store.Metadata().Engine
		s.setSessionStore.SetSessionStore(ctx, session.NewInstrumentedSessionStore(store.SessionStore(), kind, "", s.recordSessionOp))
	}
	s.store = store

	// Now that s.store is populated, wire the expand progress log's size
	// provider. NewSyncer could not do this when the caller used
	// WithC1ZPath because s.store was still nil at that point.
	s.wireCountsDBSizeProvider()

	return nil
}

// wireCountsDBSizeProvider attaches the store's DBSizeProvider capability
// (if implemented) to s.counts so LogExpandProgress emits decompressed_bytes
// and growth delta during long expansions. Idempotent: may be called from
// both NewSyncer (WithConnectorStore case) and loadStore (WithC1ZPath case).
func (s *syncer) wireCountsDBSizeProvider() {
	if s.counts == nil || s.store == nil {
		return
	}
	if sp, ok := s.store.(connectorstore.DBSizeProvider); ok {
		s.counts.SetDBSizeProvider(sp)
	}
}

// Close closes the store so the c1z is flushed to disk.
//
// Store close runs on a context detached from the caller's cancellation.
// The caller may be a Temporal activity whose deadline has already fired;
// we still need to commit the c1z cleanly. The detached context is bounded
// by dotc1z.FinalizeTimeout() so a wedged finalize cannot pin a worker
// indefinitely. A new-root span linked to syncer.Close keeps the finalize
// subtree from inflating very long sync traces.
func (s *syncer) Close(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.Close")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	parentCtxErrLabel := uotel.ClassifyCtxErr(ctx.Err())
	timeout := dotc1z.FinalizeTimeout()
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	// Propagate the original caller's ctx-error label across the detach
	// so downstream finalize spans (e.g. C1File.finalize) can record
	// what the caller actually saw — without this, the nested span
	// would re-classify our already-detached ctx and always report
	// parent_ctx_err="nil".
	finalizeCtx = uotel.WithParentCtxErrLabel(finalizeCtx, parentCtxErrLabel)
	finalizeCtx, finalizeSpan := uotel.StartWithLink(finalizeCtx, tracer, "syncer.finalize")
	uotel.SetSyncIdentityAttrs(finalizeCtx, finalizeSpan)
	finalizeSpan.SetAttributes(
		attribute.Bool("c1z.finalize.cancel_observed", parentCtxErrLabel != "nil"),
		attribute.String("c1z.finalize.parent_ctx_err", parentCtxErrLabel),
		attribute.Int64("c1z.finalize.timeout_seconds", int64(timeout.Seconds())),
	)
	defer func() { uotel.EndSpanWithError(finalizeSpan, err) }()

	var errs []error

	var storeCloseErr error
	if s.store != nil {
		storeCloseErr = s.store.Close(finalizeCtx)
		if storeCloseErr != nil {
			errs = append(errs, fmt.Errorf("error closing store: %w", storeCloseErr))
		}
	}

	// The external resource reader is read-only and has no durable
	// state to commit — closing it on the caller's ctx (not the
	// detached finalizeCtx) keeps a hung close from holding the
	// syncer past the caller's deadline for no commit-correctness
	// benefit.
	if s.externalResourceReader != nil {
		if err := s.externalResourceReader.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("error closing external resource reader: %w", err))
		}
	}

	// Read-only previous-sync replay source; same close rationale as the
	// external resource reader above.
	if s.previousSyncReader != nil {
		if err := s.previousSyncReader.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("error closing previous-sync reader: %w", err))
		}
	}

	err = errors.Join(errs...)
	return err
}

type SyncOpt func(s *syncer)

// WithRunDuration sets a `time.Duration` for `NewSyncer` Options.
// `d` represents a duration. The elapsed time between two instants as an int64 nanosecond count.
func WithRunDuration(d time.Duration) SyncOpt {
	return func(s *syncer) {
		if d > 0 {
			s.runDuration = d
		}
	}
}

// WithTransitionHandler sets a `transitionHandler` for `NewSyncer` Options.
func WithTransitionHandler(f func(s Action)) SyncOpt {
	return func(s *syncer) {
		if f != nil {
			s.transitionHandler = f
		}
	}
}

// WithProgressHandler sets a `progressHandler` for `NewSyncer` Options.
// The progress handler is called for sync action, such as listing resources, entitlements, grants, etc.
// If running in parallel mode, this function must be thread-safe.
func WithProgressHandler(f func(s *Progress)) SyncOpt {
	return func(s *syncer) {
		if f != nil {
			s.progressHandler = f
		}
	}
}

// WithConnectorStore sets the connector store to use. This is the preferred option.
// Either this or WithC1ZPath must be provided to create a new syncer.
func WithConnectorStore(store c1zstore.Store) SyncOpt {
	return func(s *syncer) {
		s.store = store
	}
}

// WithC1ZPath sets the path to the c1z file.
// Either this or WithConnectorStore must be provided to create a new syncer.
func WithC1ZPath(path string) SyncOpt {
	return func(s *syncer) {
		s.c1zPath = path
	}
}

func WithTmpDir(path string) SyncOpt {
	return func(s *syncer) {
		s.tmpDir = path
	}
}

// WithStorageEngine selects the dotc1z storage engine when opening the c1z
// file via WithC1ZPath. Empty uses the baton-sdk default.
func WithStorageEngine(engine c1zstore.Engine) SyncOpt {
	return func(s *syncer) {
		s.storageEngine = engine
	}
}

// WithSkipFullSync skips syncing entirely.
func WithSkipFullSync() SyncOpt {
	return func(s *syncer) {
		s.skipFullSync = true
	}
}

func WithExternalResourceC1ZPath(path string) SyncOpt {
	return func(s *syncer) {
		s.externalResourceC1ZPath = path
	}
}

// WithPreviousSyncC1ZPath points ETag-replay at a separate c1z holding
// the previous sync, instead of reading a previous sync from inside the
// live store.
//
// This is required for the single-sync v3 (Pebble) engine: a Pebble c1z
// holds exactly one sync by contract, so there is no in-file "previous
// sync" to replay from (StartNewSync replaces the prior sync). Supplying
// the prior run's c1z here lets the syncer recover unchanged resources'
// ETags and carry their grants forward across runs. When unset, replay
// falls back to reading a previous sync from the live store (the SQLite
// multi-sync behavior), so existing callers are unaffected.
//
// The file is opened read-only and engine-agnostically (the magic byte
// selects SQLite or Pebble), so the previous-sync c1z may use either
// engine.
func WithPreviousSyncC1ZPath(path string) SyncOpt {
	return func(s *syncer) {
		s.previousSyncC1ZPath = path
		s.previousSyncC1ZPathOptional = false
	}
}

// WithOptionalPreviousSyncC1ZPath is WithPreviousSyncC1ZPath with
// best-effort semantics: if the file is missing, corrupt, or written by
// an incompatible SDK, NewSyncer logs and proceeds WITHOUT replay
// instead of failing. Intended for cache-style replay sources the
// caller maintains automatically (the service-mode previous-sync spare)
// — a bad cache file must never fail a sync. Callers that name a
// specific file deliberately should use WithPreviousSyncC1ZPath, which
// surfaces open failures.
func WithOptionalPreviousSyncC1ZPath(path string) SyncOpt {
	return func(s *syncer) {
		s.previousSyncC1ZPath = path
		s.previousSyncC1ZPathOptional = true
	}
}

func WithExternalResourceEntitlementIdFilter(entitlementId string) SyncOpt {
	return func(s *syncer) {
		s.externalResourceEntitlementIdFilter = entitlementId
	}
}

func WithTargetedSyncResources(resources []*v2.Resource) SyncOpt {
	return func(s *syncer) {
		s.targetedSyncResources = resources
		if len(resources) > 0 {
			s.syncType = connectorstore.SyncTypePartial
			return
		}
		// No targeted resource IDs, so we need to update the sync type to either full or resources only.
		WithSkipEntitlementsAndGrants(s.skipEntitlementsAndGrants)(s)
	}
}

func WithSessionStore(sessionStore sessions.SetSessionStore) SyncOpt {
	return func(s *syncer) {
		s.setSessionStore = sessionStore
	}
}

// WithSyncResourceTypes sets the resource types to sync.
// If empty (the default), all resource types will be synced.
func WithSyncResourceTypes(resourceTypeIDs []string) SyncOpt {
	return func(s *syncer) {
		s.syncResourceTypes = resourceTypeIDs
	}
}

// WithOnlyExpandGrants sets whether to skip syncing resources and only expand grants.
func WithOnlyExpandGrants() SyncOpt {
	return func(s *syncer) {
		s.onlyExpandGrants = true
	}
}

// WithDontExpandGrants sets whether to skip expanding grants.
// This is used for speeding up service mode connectors and reducing their c1z upload size.
// C1 will process the uploaded c1z and expand grants itself.
func WithDontExpandGrants() SyncOpt {
	return func(s *syncer) {
		s.dontExpandGrants = true
	}
}
func WithSyncID(syncID string) SyncOpt {
	return func(s *syncer) {
		s.syncID = syncID
	}
}

// WithSkipEntitlementsAndGrants sets whether to skip syncing entitlements and grants for resources.
// If true, only resources will be synced.
func WithSkipEntitlementsAndGrants(skip bool) SyncOpt {
	return func(s *syncer) {
		s.skipEntitlementsAndGrants = skip
		// Partial syncs can skip entitlements and grants, so don't update the sync type in that case.
		if s.syncType == connectorstore.SyncTypePartial {
			return
		}
		if skip {
			s.syncType = connectorstore.SyncTypeResourcesOnly
		} else {
			s.syncType = connectorstore.SyncTypeFull
		}
	}
}

// WithSkipGrants sets whether to skip syncing grants for resources.
// Entitlements will still be synced.
func WithSkipGrants(skip bool) SyncOpt {
	return func(s *syncer) {
		s.skipGrants = skip
	}
}

// NormalizeWorkerCount maps raw worker-count inputs (CLI / config sentinels) to the syncer's
// internal worker count: -1 selects min(max(GOMAXPROCS, 1), 4); any other value uses max(count, 0).
func NormalizeWorkerCount(count int) int {
	if count == -1 {
		return min(max(runtime.GOMAXPROCS(0), 1), 4)
	}
	return max(count, 1)
}

// WithMetricsHandler attaches a metrics.Handler that the syncer forwards to
// progresslog.NewProgressCounts so the grant-expansion OTel instruments
// (baton.sync.expand.actions_remaining / actions_burned /
// decompressed_bytes / decompressed_bytes_delta) actually reach the
// configured exporter instead of the default no-op handler.
//
// Callers should pre-tag the handler with the dimensions they want to slice
// by (e.g. tenant_id, connector_id) via Handler.WithTags before passing it
// in — baton-sdk has no view of those identifiers.
func WithMetricsHandler(h metrics.Handler) SyncOpt {
	return func(s *syncer) {
		if h == nil {
			return
		}
		s.metricsHandler = h
	}
}

// WithWorkerCount sets the number of workers to use.
// If <=1, 1 worker is used (default). If > 1, parallel sync is used.
// If -1, the number of workers is set to the number of CPU cores or 4, whichever is lower.
// If < -1, 1 worker is used. (Nothing should do this, but there's no way to return an error in this option.)
func WithWorkerCount(count int) SyncOpt {
	return func(s *syncer) {
		s.workerCount = NormalizeWorkerCount(count)
	}
}

// WithSyncIdentity stamps connector identity onto sync and dotc1z spans (and
// the c1z size metric) so a single connector's work is filterable in APM.
// Attribute keys match the pprof.Do labels set by the platform sync activity,
// so spans and CPU profiles line up. Sync injects this into the run context
// via uotel.WithSyncIdentity, which is how it reaches dotc1z spans too.
func WithSyncIdentity(id uotel.SyncIdentity) SyncOpt {
	return func(s *syncer) {
		s.syncIdentity = id
	}
}

// NewSyncer returns a new syncer object.
func NewSyncer(ctx context.Context, c types.ConnectorClient, opts ...SyncOpt) (Syncer, error) {
	s := &syncer{
		connector:   c,
		syncType:    connectorstore.SyncTypeFull,
		workerCount: 1,
	}

	for _, o := range opts {
		o(s)
	}

	if s.store == nil && s.c1zPath == "" {
		return nil, errors.New("a connector store writer or a db path must be provided")
	}

	progressLogOpts := []progresslog.Option{}
	if s.metricsHandler != nil {
		progressLogOpts = append(progressLogOpts, progresslog.WithMetricsHandler(s.metricsHandler))
	}
	s.counts = progresslog.NewProgressCounts(ctx, progressLogOpts...)
	// Wire the DBSizeProvider now if the store is already set (WithConnectorStore
	// case). For WithC1ZPath, the store is populated later inside loadStore,
	// which calls wireCountsDBSizeProvider again. Without this split the feature
	// would ship dead for every c1z-path caller — see syncer.loadStore.
	s.wireCountsDBSizeProvider()

	if s.externalResourceC1ZPath != "" {
		externalC1ZReader, err := dotc1z.NewStore(ctx, s.externalResourceC1ZPath, dotc1z.WithTmpDir(s.tmpDir), dotc1z.WithReadOnly(true))
		if err != nil {
			return nil, err
		}
		s.externalResourceReader = externalC1ZReader
	}

	if s.previousSyncC1ZPath != "" {
		// Open the previous-sync c1z read-only and engine-agnostically
		// (NewStore selects the engine from the file's magic byte), so a
		// Pebble or SQLite prior run both work as a replay source.
		previousSyncStore, err := dotc1z.NewStore(ctx, s.previousSyncC1ZPath,
			dotc1z.WithReadOnly(true),
			dotc1z.WithTmpDir(s.tmpDir),
		)
		switch {
		case err == nil:
			s.previousSyncReader = previousSyncStore
		case s.previousSyncC1ZPathOptional:
			// Best-effort replay source (see WithOptionalPreviousSyncC1ZPath):
			// a missing/corrupt/incompatible cache file degrades to a sync
			// without ETag replay, never a failed sync. The caller that
			// maintains the cache replaces it after its next successful
			// upload, so a bad file self-heals.
			ctxzap.Extract(ctx).Warn("previous-sync c1z unusable; syncing without etag replay",
				zap.String("previous_sync_c1z_path", s.previousSyncC1ZPath),
				zap.Error(err),
			)
		default:
			return nil, fmt.Errorf("error opening previous-sync c1z %q: %w", s.previousSyncC1ZPath, err)
		}
	}

	return s, nil
}
