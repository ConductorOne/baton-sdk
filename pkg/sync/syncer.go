package sync

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/conductorone/baton-sdk/pkg/sync/expand"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	c1zpb "github.com/conductorone/baton-sdk/pb/c1/c1z/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	"github.com/conductorone/baton-sdk/pkg/types"
)

var tracer = otel.Tracer("baton-sdk/sync")

const defaultMaxDepth int64 = 20

var maxDepth, _ = strconv.ParseInt(os.Getenv("BATON_GRAPH_EXPAND_MAX_DEPTH"), 10, 64)
var dontFixCycles, _ = strconv.ParseBool(os.Getenv("BATON_DONT_FIX_CYCLES"))

var ErrSyncNotComplete = fmt.Errorf("sync exited without finishing")

type Syncer interface {
	Sync(context.Context) error
	Close(context.Context) error
}

type ProgressCounts struct {
	ResourceTypes        int
	Resources            map[string]int
	EntitlementsProgress map[string]int
	LastEntitlementLog   map[string]time.Time
	GrantsProgress       map[string]int
	LastGrantLog         map[string]time.Time
	LastActionLog        time.Time
}

const maxLogFrequency = 10 * time.Second

// TODO: use a mutex or a syncmap for when this code becomes parallel
func NewProgressCounts() *ProgressCounts {
	return &ProgressCounts{
		Resources:            make(map[string]int),
		EntitlementsProgress: make(map[string]int),
		LastEntitlementLog:   make(map[string]time.Time),
		GrantsProgress:       make(map[string]int),
		LastGrantLog:         make(map[string]time.Time),
		LastActionLog:        time.Time{},
	}
}

func (p *ProgressCounts) LogResourceTypesProgress(ctx context.Context) {
	l := ctxzap.Extract(ctx)
	l.Info("Synced resource types", zap.Int("count", p.ResourceTypes))
}

func (p *ProgressCounts) LogResourcesProgress(ctx context.Context, resourceType string) {
	l := ctxzap.Extract(ctx)
	resources := p.Resources[resourceType]
	l.Info("Synced resources", zap.String("resource_type_id", resourceType), zap.Int("count", resources))
}

func (p *ProgressCounts) LogEntitlementsProgress(ctx context.Context, resourceType string) {
	entitlementsProgress := p.EntitlementsProgress[resourceType]
	resources := p.Resources[resourceType]

	l := ctxzap.Extract(ctx)
	if resources == 0 {
		// if resuming sync, resource counts will be zero, so don't calculate percentage. just log every 10 seconds.
		if time.Since(p.LastEntitlementLog[resourceType]) > maxLogFrequency {
			l.Info("Syncing entitlements",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", entitlementsProgress),
			)
			p.LastEntitlementLog[resourceType] = time.Now()
		}
		return
	}

	percentComplete := (entitlementsProgress * 100) / resources

	switch {
	case entitlementsProgress > resources:
		l.Error("more entitlement resources than resources",
			zap.String("resource_type_id", resourceType),
			zap.Int("synced", entitlementsProgress),
			zap.Int("total", resources),
		)
	case percentComplete == 100:
		l.Info("Synced entitlements",
			zap.String("resource_type_id", resourceType),
			zap.Int("count", entitlementsProgress),
			zap.Int("total", resources),
		)
		p.LastEntitlementLog[resourceType] = time.Time{}
	case time.Since(p.LastEntitlementLog[resourceType]) > maxLogFrequency:
		l.Info("Syncing entitlements",
			zap.String("resource_type_id", resourceType),
			zap.Int("synced", entitlementsProgress),
			zap.Int("total", resources),
			zap.Int("percent_complete", percentComplete),
		)
		p.LastEntitlementLog[resourceType] = time.Now()
	}
}

func (p *ProgressCounts) LogGrantsProgress(ctx context.Context, resourceType string) {
	grantsProgress := p.GrantsProgress[resourceType]
	resources := p.Resources[resourceType]

	l := ctxzap.Extract(ctx)
	if resources == 0 {
		// if resuming sync, resource counts will be zero, so don't calculate percentage. just log every 10 seconds.
		if time.Since(p.LastGrantLog[resourceType]) > maxLogFrequency {
			l.Info("Syncing grants",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", grantsProgress),
			)
			p.LastGrantLog[resourceType] = time.Now()
		}
		return
	}

	percentComplete := (grantsProgress * 100) / resources

	switch {
	case grantsProgress > resources:
		l.Error("more grant resources than resources",
			zap.String("resource_type_id", resourceType),
			zap.Int("synced", grantsProgress),
			zap.Int("total", resources),
		)
	case percentComplete == 100:
		l.Info("Synced grants",
			zap.String("resource_type_id", resourceType),
			zap.Int("count", grantsProgress),
			zap.Int("total", resources),
		)
		p.LastGrantLog[resourceType] = time.Time{}
	case time.Since(p.LastGrantLog[resourceType]) > maxLogFrequency:
		l.Info("Syncing grants",
			zap.String("resource_type_id", resourceType),
			zap.Int("synced", grantsProgress),
			zap.Int("total", resources),
			zap.Int("percent_complete", percentComplete),
		)
		p.LastGrantLog[resourceType] = time.Now()
	}
}

func (p *ProgressCounts) LogExpandProgress(ctx context.Context, actions []*expand.EntitlementGraphAction) {
	actionsLen := len(actions)
	if time.Since(p.LastActionLog) < maxLogFrequency {
		return
	}
	p.LastActionLog = time.Now()

	l := ctxzap.Extract(ctx)
	l.Info("Expanding grants", zap.Int("actions_remaining", actionsLen))
}

// syncer orchestrates a connector sync and stores the results using the provided datasource.Writer.
type syncer struct {
	c1zManager         manager.Manager
	c1zPath            string
	store              connectorstore.Writer
	connector          types.ConnectorClient
	state              State
	runDuration        time.Duration
	transitionHandler  func(s Action)
	progressHandler    func(p *Progress)
	tmpDir             string
	skipFullSync       bool
	lastCheckPointTime time.Time
	counts             *ProgressCounts

	skipEGForResourceType map[string]bool
}

const minCheckpointInterval = 10 * time.Second

// Checkpoint marshals the current state and stores it.
func (s *syncer) Checkpoint(ctx context.Context, force bool) error {
	if !force && !s.lastCheckPointTime.IsZero() && time.Since(s.lastCheckPointTime) < minCheckpointInterval {
		return nil
	}
	ctx, span := tracer.Start(ctx, "syncer.Checkpoint")
	defer span.End()

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

var attempts = 0

func shouldWaitAndRetry(ctx context.Context, err error) bool {
	ctx, span := tracer.Start(ctx, "syncer.shouldWaitAndRetry")
	defer span.End()

	if err == nil {
		attempts = 0
		return true
	}
	if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
		return false
	}

	attempts++
	l := ctxzap.Extract(ctx)

	// use linear time by default
	var wait time.Duration = time.Duration(attempts) * time.Second

	// If error contains rate limit data, use that instead
	if st, ok := status.FromError(err); ok {
		details := st.Details()
		for _, detail := range details {
			if rlData, ok := detail.(*v2.RateLimitDescription); ok {
				waitResetAt := time.Until(rlData.ResetAt.AsTime())
				if waitResetAt <= 0 {
					continue
				}
				duration := time.Duration(rlData.Limit)
				if duration <= 0 {
					continue
				}
				waitResetAt /= duration
				// Round up to the nearest second to make sure we don't hit the rate limit again
				waitResetAt = time.Duration(math.Ceil(waitResetAt.Seconds())) * time.Second
				if waitResetAt > 0 {
					wait = waitResetAt
					break
				}
			}
		}
	}

	l.Warn("retrying operation", zap.Error(err), zap.Duration("wait", wait))

	for {
		select {
		case <-time.After(wait):
			return true
		case <-ctx.Done():
			return false
		}
	}
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

// Sync starts the syncing process. The sync process is driven by the action stack that is part of the state object.
// For each page of data that is required to be fetched from the connector, a new action is pushed on to the stack. Once
// an action is completed, it is popped off of the queue. Before processing each action, we checkpoint the state object
// into the datasource. This allows for graceful resumes if a sync is interrupted.
func (s *syncer) Sync(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.Sync")
	defer span.End()

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

	err := s.loadStore(ctx)
	if err != nil {
		return err
	}

	_, err = s.connector.Validate(ctx, &v2.ConnectorServiceValidateRequest{})
	if err != nil {
		return err
	}

	syncID, newSync, err := s.store.StartSync(ctx)
	if err != nil {
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

	state := &state{}
	err = state.Unmarshal(currentStep)
	if err != nil {
		return err
	}
	s.state = state

	var warnings []error
	for s.state.Current() != nil {
		err = s.Checkpoint(ctx, false)
		if err != nil {
			return err
		}

		// TODO: count actions divided by warnings and error if warning percentage is too high
		if len(warnings) > 10 {
			return fmt.Errorf("too many warnings, exiting sync. warnings: %v", warnings)
		}
		select {
		case <-runCtx.Done():
			err = context.Cause(runCtx)
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				l.Debug("sync run duration has expired, exiting sync early", zap.String("sync_id", syncID))
				return ErrSyncNotComplete
			default:
				l.Error("sync context cancelled", zap.String("sync_id", syncID), zap.Error(err))
				return err
			}
		default:
		}

		stateAction := s.state.Current()

		switch stateAction.Op {
		case InitOp:
			s.state.FinishAction(ctx)
			// FIXME(jirwin): Disabling syncing assets for now
			// s.state.PushAction(ctx, Action{Op: SyncAssetsOp})
			s.state.PushAction(ctx, Action{Op: SyncGrantExpansionOp})
			s.state.PushAction(ctx, Action{Op: SyncGrantsOp})
			s.state.PushAction(ctx, Action{Op: SyncEntitlementsOp})
			s.state.PushAction(ctx, Action{Op: SyncResourcesOp})
			s.state.PushAction(ctx, Action{Op: SyncResourceTypesOp})

			err = s.Checkpoint(ctx, true)
			if err != nil {
				return err
			}
			continue

		case SyncResourceTypesOp:
			err = s.SyncResourceTypes(ctx)
			if !shouldWaitAndRetry(ctx, err) {
				return err
			}
			continue

		case SyncResourcesOp:
			err = s.SyncResources(ctx)
			if !shouldWaitAndRetry(ctx, err) {
				return err
			}
			continue

		case SyncEntitlementsOp:
			err = s.SyncEntitlements(ctx)
			if isWarning(ctx, err) {
				l.Warn("skipping sync entitlement action", zap.Any("stateAction", stateAction), zap.Error(err))
				warnings = append(warnings, err)
				s.state.FinishAction(ctx)
				continue
			}
			if !shouldWaitAndRetry(ctx, err) {
				return err
			}
			continue

		case SyncGrantsOp:
			err = s.SyncGrants(ctx)
			if isWarning(ctx, err) {
				l.Warn("skipping sync grant action", zap.Any("stateAction", stateAction), zap.Error(err))
				warnings = append(warnings, err)
				s.state.FinishAction(ctx)
				continue
			}
			if !shouldWaitAndRetry(ctx, err) {
				return err
			}
			continue

		case SyncAssetsOp:
			err = s.SyncAssets(ctx)
			if !shouldWaitAndRetry(ctx, err) {
				return err
			}
			continue

		case SyncGrantExpansionOp:
			if !s.state.NeedsExpansion() {
				l.Debug("skipping grant expansion, no grants to expand")
				s.state.FinishAction(ctx)
				continue
			}

			err = s.SyncGrantExpansion(ctx)
			if !shouldWaitAndRetry(ctx, err) {
				return err
			}
			continue
		default:
			return fmt.Errorf("unexpected sync step")
		}
	}

	// Force a checkpoint to clear sync_token.
	err = s.Checkpoint(ctx, true)
	if err != nil {
		return err
	}

	err = s.store.EndSync(ctx)
	if err != nil {
		return err
	}

	l.Info("Sync complete.")

	err = s.store.Cleanup(ctx)
	if err != nil {
		return err
	}

	_, err = s.connector.Cleanup(ctx, &v2.ConnectorServiceCleanupRequest{})
	if err != nil {
		l.Error("error clearing connector caches", zap.Error(err))
	}

	if len(warnings) > 0 {
		l.Warn("sync completed with warnings", zap.Int("warning_count", len(warnings)), zap.Any("warnings", warnings))
	}
	return nil
}

func (s *syncer) SkipSync(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.SkipSync")
	defer span.End()

	l := ctxzap.Extract(ctx)
	l.Info("skipping sync")

	var runCanc context.CancelFunc
	if s.runDuration > 0 {
		_, runCanc = context.WithTimeout(ctx, s.runDuration)
	}
	if runCanc != nil {
		defer runCanc()
	}

	err := s.loadStore(ctx)
	if err != nil {
		return err
	}

	_, err = s.connector.Validate(ctx, &v2.ConnectorServiceValidateRequest{})
	if err != nil {
		return err
	}

	_, err = s.store.StartNewSync(ctx)
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

// SyncResourceTypes calls the ListResourceType() connector endpoint and persists the results in to the datasource.
func (s *syncer) SyncResourceTypes(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.SyncResourceTypes")
	defer span.End()

	pageToken := s.state.PageToken(ctx)

	if pageToken == "" {
		ctxzap.Extract(ctx).Info("Syncing resource types...")
		s.handleInitialActionForStep(ctx, *s.state.Current())
	}

	err := s.loadStore(ctx)
	if err != nil {
		return err
	}

	resp, err := s.connector.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{PageToken: pageToken})
	if err != nil {
		return err
	}

	err = s.store.PutResourceTypes(ctx, resp.List...)
	if err != nil {
		return err
	}

	s.counts.ResourceTypes += len(resp.List)
	s.handleProgress(ctx, s.state.Current(), len(resp.List))

	if resp.NextPageToken == "" {
		s.counts.LogResourceTypesProgress(ctx)
		s.state.FinishAction(ctx)
		return nil
	}

	err = s.state.NextPage(ctx, resp.NextPageToken)
	if err != nil {
		return err
	}

	return nil
}

// getSubResources fetches the sub resource types from a resources' annotations.
func (s *syncer) getSubResources(ctx context.Context, parent *v2.Resource) error {
	ctx, span := tracer.Start(ctx, "syncer.getSubResources")
	defer span.End()

	for _, a := range parent.Annotations {
		if a.MessageIs((*v2.ChildResourceType)(nil)) {
			crt := &v2.ChildResourceType{}
			err := a.UnmarshalTo(crt)
			if err != nil {
				return err
			}

			childAction := Action{
				Op:                   SyncResourcesOp,
				ResourceTypeID:       crt.ResourceTypeId,
				ParentResourceID:     parent.Id.Resource,
				ParentResourceTypeID: parent.Id.ResourceType,
			}
			s.state.PushAction(ctx, childAction)
		}
	}

	return nil
}

// SyncResources handles fetching all of the resources from the connector given the provided resource types. For each
// resource, we gather any child resource types it may emit, and traverse the resource tree.
func (s *syncer) SyncResources(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.SyncResources")
	defer span.End()

	if s.state.Current().ResourceTypeID == "" {
		pageToken := s.state.PageToken(ctx)

		if pageToken == "" {
			ctxzap.Extract(ctx).Info("Syncing resources...")
			s.handleInitialActionForStep(ctx, *s.state.Current())
		}

		resp, err := s.store.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{PageToken: pageToken})
		if err != nil {
			return err
		}

		if resp.NextPageToken != "" {
			err = s.state.NextPage(ctx, resp.NextPageToken)
			if err != nil {
				return err
			}
		} else {
			s.state.FinishAction(ctx)
		}

		for _, rt := range resp.List {
			s.state.PushAction(ctx, Action{Op: SyncResourcesOp, ResourceTypeID: rt.Id})
		}

		return nil
	}

	return s.syncResources(ctx)
}

// syncResources fetches a given resource from the connector, and returns a slice of new child resources to fetch.
func (s *syncer) syncResources(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.syncResources")
	defer span.End()

	req := &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: s.state.ResourceTypeID(ctx),
		PageToken:      s.state.PageToken(ctx),
	}
	if s.state.ParentResourceTypeID(ctx) != "" && s.state.ParentResourceID(ctx) != "" {
		req.ParentResourceId = &v2.ResourceId{
			ResourceType: s.state.ParentResourceTypeID(ctx),
			Resource:     s.state.ParentResourceID(ctx),
		}
	}

	resp, err := s.connector.ListResources(ctx, req)
	if err != nil {
		return err
	}

	s.handleProgress(ctx, s.state.Current(), len(resp.List))

	resourceTypeId := s.state.ResourceTypeID(ctx)
	s.counts.Resources[resourceTypeId] += len(resp.List)

	if resp.NextPageToken == "" {
		s.counts.LogResourcesProgress(ctx, resourceTypeId)
		s.state.FinishAction(ctx)
	} else {
		err = s.state.NextPage(ctx, resp.NextPageToken)
		if err != nil {
			return err
		}
	}

	bulkPutResoruces := []*v2.Resource{}
	for _, r := range resp.List {
		// Check if we've already synced this resource, skip it if we have
		_, err = s.store.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
			ResourceId: &v2.ResourceId{ResourceType: r.Id.ResourceType, Resource: r.Id.Resource},
		})
		if err == nil {
			continue
		}

		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		err = s.validateResourceTraits(ctx, r)
		if err != nil {
			return err
		}

		// Set the resource creation source
		r.CreationSource = v2.Resource_CREATION_SOURCE_CONNECTOR_LIST_RESOURCES

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

	return nil
}

func (s *syncer) validateResourceTraits(ctx context.Context, r *v2.Resource) error {
	ctx, span := tracer.Start(ctx, "syncer.validateResourceTraits")
	defer span.End()

	resourceTypeResponse, err := s.store.GetResourceType(ctx, &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
		ResourceTypeId: r.Id.ResourceType,
	})
	if err != nil {
		return err
	}

	for _, t := range resourceTypeResponse.ResourceType.Traits {
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
			annos := annotations.Annotations(r.Annotations)
			if !annos.Contains(trait) {
				ctxzap.Extract(ctx).Error(
					"resource was missing expected trait",
					zap.String("trait", string(trait.ProtoReflect().Descriptor().Name())),
					zap.String("resource_type_id", r.Id.ResourceType),
					zap.String("resource_id", r.Id.Resource),
				)
				return fmt.Errorf("resource was missing expected trait %s", trait.ProtoReflect().Descriptor().Name())
			}
		}
	}

	return nil
}

// shouldSkipEntitlementsAndGrants determines if we should sync entitlements for a given resource. We cache the
// result of this function for each resource type to avoid constant lookups in the database.
func (s *syncer) shouldSkipEntitlementsAndGrants(ctx context.Context, r *v2.Resource) (bool, error) {
	ctx, span := tracer.Start(ctx, "syncer.shouldSkipEntitlementsAndGrants")
	defer span.End()

	// We've checked this resource type, so we can return what we have cached directly.
	if skip, ok := s.skipEGForResourceType[r.Id.ResourceType]; ok {
		return skip, nil
	}

	rt, err := s.store.GetResourceType(ctx, &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
		ResourceTypeId: r.Id.ResourceType,
	})
	if err != nil {
		return false, err
	}

	rtAnnos := annotations.Annotations(rt.ResourceType.Annotations)

	skipEntitlements := rtAnnos.Contains(&v2.SkipEntitlementsAndGrants{})
	s.skipEGForResourceType[r.Id.ResourceType] = skipEntitlements

	return skipEntitlements, nil
}

// SyncEntitlements fetches the entitlements from the connector. It first lists each resource from the datastore,
// and pushes an action to fetch the entitlements for each resource.
func (s *syncer) SyncEntitlements(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.SyncEntitlements")
	defer span.End()

	if s.state.ResourceTypeID(ctx) == "" && s.state.ResourceID(ctx) == "" {
		pageToken := s.state.PageToken(ctx)

		if pageToken == "" {
			ctxzap.Extract(ctx).Info("Syncing entitlements...")
			s.handleInitialActionForStep(ctx, *s.state.Current())
		}

		resp, err := s.store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{PageToken: pageToken})
		if err != nil {
			return err
		}

		// We want to take action on the next page before we push any new actions
		if resp.NextPageToken != "" {
			err = s.state.NextPage(ctx, resp.NextPageToken)
			if err != nil {
				return err
			}
		} else {
			s.state.FinishAction(ctx)
		}

		for _, r := range resp.List {
			shouldSkipEntitlements, err := s.shouldSkipEntitlementsAndGrants(ctx, r)
			if err != nil {
				return err
			}
			if shouldSkipEntitlements {
				continue
			}
			s.state.PushAction(ctx, Action{Op: SyncEntitlementsOp, ResourceID: r.Id.Resource, ResourceTypeID: r.Id.ResourceType})
		}

		return nil
	}

	err := s.syncEntitlementsForResource(ctx, &v2.ResourceId{
		ResourceType: s.state.ResourceTypeID(ctx),
		Resource:     s.state.ResourceID(ctx),
	})
	if err != nil {
		return err
	}

	return nil
}

// syncEntitlementsForResource fetches the entitlements for a specific resource from the connector.
func (s *syncer) syncEntitlementsForResource(ctx context.Context, resourceID *v2.ResourceId) error {
	ctx, span := tracer.Start(ctx, "syncer.syncEntitlementsForResource")
	defer span.End()

	resourceResponse, err := s.store.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: resourceID,
	})
	if err != nil {
		return err
	}

	pageToken := s.state.PageToken(ctx)

	resp, err := s.connector.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{
		Resource:  resourceResponse.Resource,
		PageToken: pageToken,
	})
	if err != nil {
		return err
	}
	err = s.store.PutEntitlements(ctx, resp.List...)
	if err != nil {
		return err
	}

	s.handleProgress(ctx, s.state.Current(), len(resp.List))

	if resp.NextPageToken != "" {
		err = s.state.NextPage(ctx, resp.NextPageToken)
		if err != nil {
			return err
		}
	} else {
		s.counts.EntitlementsProgress[resourceID.ResourceType] += 1
		s.counts.LogEntitlementsProgress(ctx, resourceID.ResourceType)

		s.state.FinishAction(ctx)
	}

	return nil
}

// syncAssetsForResource looks up a resource given the input ID. From there it looks to see if there are any traits that
// include references to an asset. For each AssetRef, we then call GetAsset on the connector and stream the asset from the connector.
// Once we have the entire asset, we put it in the database.
func (s *syncer) syncAssetsForResource(ctx context.Context, resourceID *v2.ResourceId) error {
	ctx, span := tracer.Start(ctx, "syncer.syncAssetsForResource")
	defer span.End()

	l := ctxzap.Extract(ctx)
	resourceResponse, err := s.store.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: resourceID,
	})
	if err != nil {
		return err
	}

	var assetRefs []*v2.AssetRef

	rAnnos := annotations.Annotations(resourceResponse.Resource.Annotations)

	userTrait := &v2.UserTrait{}
	ok, err := rAnnos.Pick(userTrait)
	if err != nil {
		return err
	}
	if ok {
		assetRefs = append(assetRefs, userTrait.Icon)
	}

	grpTrait := &v2.GroupTrait{}
	ok, err = rAnnos.Pick(grpTrait)
	if err != nil {
		return err
	}
	if ok {
		assetRefs = append(assetRefs, grpTrait.Icon)
	}

	appTrait := &v2.AppTrait{}
	ok, err = rAnnos.Pick(appTrait)
	if err != nil {
		return err
	}
	if ok {
		assetRefs = append(assetRefs, appTrait.Icon, appTrait.Logo)
	}

	for _, assetRef := range assetRefs {
		if assetRef == nil {
			continue
		}

		l.Debug("fetching asset", zap.String("asset_ref_id", assetRef.Id))
		resp, err := s.connector.GetAsset(ctx, &v2.AssetServiceGetAssetRequest{Asset: assetRef})
		if err != nil {
			return err
		}

		// FIXME(jirwin): if the return from the client is nil, skip this asset
		// Temporary until we can implement assets on the platform side
		if resp == nil {
			continue
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
				return err
			}

			l.Debug("received asset message")

			switch assetMsg := msg.Msg.(type) {
			case *v2.AssetServiceGetAssetResponse_Metadata_:
				metadata = assetMsg.Metadata

			case *v2.AssetServiceGetAssetResponse_Data_:
				l.Debug("Received data for asset")
				_, err := io.Copy(assetBytes, bytes.NewReader(assetMsg.Data.Data))
				if err != nil {
					_ = resp.CloseSend()
					return err
				}
			}
		}

		if metadata == nil {
			return fmt.Errorf("no metadata received, unable to store asset")
		}

		err = s.store.PutAsset(ctx, assetRef, metadata.ContentType, assetBytes.Bytes())
		if err != nil {
			return err
		}
	}

	s.state.FinishAction(ctx)
	return nil
}

// SyncAssets iterates each resource in the data store, and adds an action to fetch all of the assets for that resource.
func (s *syncer) SyncAssets(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.SyncAssets")
	defer span.End()

	if s.state.ResourceTypeID(ctx) == "" && s.state.ResourceID(ctx) == "" {
		pageToken := s.state.PageToken(ctx)

		if pageToken == "" {
			ctxzap.Extract(ctx).Info("Syncing assets...")
			s.handleInitialActionForStep(ctx, *s.state.Current())
		}

		resp, err := s.store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{PageToken: pageToken})
		if err != nil {
			return err
		}

		// We want to take action on the next page before we push any new actions
		if resp.NextPageToken != "" {
			err = s.state.NextPage(ctx, resp.NextPageToken)
			if err != nil {
				return err
			}
		} else {
			s.state.FinishAction(ctx)
		}

		for _, r := range resp.List {
			s.state.PushAction(ctx, Action{Op: SyncAssetsOp, ResourceID: r.Id.Resource, ResourceTypeID: r.Id.ResourceType})
		}

		return nil
	}

	err := s.syncAssetsForResource(ctx, &v2.ResourceId{
		ResourceType: s.state.ResourceTypeID(ctx),
		Resource:     s.state.ResourceID(ctx),
	})
	if err != nil {
		ctxzap.Extract(ctx).Error("error syncing assets", zap.Error(err))
		return err
	}

	return nil
}

// SyncGrantExpansion documentation pending.
func (s *syncer) SyncGrantExpansion(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.SyncGrantExpansion")
	defer span.End()

	l := ctxzap.Extract(ctx)
	entitlementGraph := s.state.EntitlementGraph(ctx)
	if !entitlementGraph.Loaded {
		pageToken := s.state.PageToken(ctx)

		if pageToken == "" {
			l.Info("Expanding grants...")
			s.handleInitialActionForStep(ctx, *s.state.Current())
		}

		resp, err := s.store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{PageToken: pageToken})
		if err != nil {
			return err
		}

		// We want to take action on the next page before we push any new actions
		if resp.NextPageToken != "" {
			err = s.state.NextPage(ctx, resp.NextPageToken)
			if err != nil {
				return err
			}
		} else {
			l.Info("Finished loading entitlement graph", zap.Int("edges", len(entitlementGraph.Edges)))
			entitlementGraph.Loaded = true
		}

		for _, grant := range resp.List {
			annos := annotations.Annotations(grant.Annotations)
			expandable := &v2.GrantExpandable{}
			_, err := annos.Pick(expandable)
			if err != nil {
				return err
			}
			if len(expandable.GetEntitlementIds()) == 0 {
				continue
			}

			principalID := grant.GetPrincipal().GetId()
			if principalID == nil {
				return fmt.Errorf("principal id was nil")
			}

			// FIXME(morgabra) Log and skip some of the error paths here?
			for _, srcEntitlementID := range expandable.EntitlementIds {
				l.Debug(
					"Expandable entitlement found",
					zap.String("src_entitlement_id", srcEntitlementID),
					zap.String("dst_entitlement_id", grant.GetEntitlement().GetId()),
				)

				srcEntitlement, err := s.store.GetEntitlement(ctx, &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
					EntitlementId: srcEntitlementID,
				})
				if err != nil {
					l.Error("error fetching source entitlement",
						zap.String("src_entitlement_id", srcEntitlementID),
						zap.String("dst_entitlement_id", grant.GetEntitlement().GetId()),
						zap.Error(err),
					)
					continue
				}

				// The expand annotation points at entitlements by id. Those entitlements' resource should match
				// the current grant's principal, so we don't allow expanding arbitrary entitlements.
				sourceEntitlementResourceID := srcEntitlement.GetEntitlement().GetResource().GetId()
				if sourceEntitlementResourceID == nil {
					return fmt.Errorf("source entitlement resource id was nil")
				}
				if principalID.ResourceType != sourceEntitlementResourceID.ResourceType ||
					principalID.Resource != sourceEntitlementResourceID.Resource {
					l.Error(
						"source entitlement resource id did not match grant principal id",
						zap.String("grant_principal_id", principalID.String()),
						zap.String("source_entitlement_resource_id", sourceEntitlementResourceID.String()))

					return fmt.Errorf("source entitlement resource id did not match grant principal id")
				}

				entitlementGraph.AddEntitlement(grant.Entitlement)
				entitlementGraph.AddEntitlement(srcEntitlement.GetEntitlement())
				err = entitlementGraph.AddEdge(ctx,
					srcEntitlement.GetEntitlement().GetId(),
					grant.GetEntitlement().GetId(),
					expandable.Shallow,
					expandable.ResourceTypeIds,
				)
				if err != nil {
					return fmt.Errorf("error adding edge to graph: %w", err)
				}
			}
		}
		return nil
	}

	if entitlementGraph.Loaded {
		cycle := entitlementGraph.GetFirstCycle()
		if cycle != nil {
			l.Warn(
				"cycle detected in entitlement graph",
				zap.Any("cycle", cycle),
			)
			l.Debug("initial graph", zap.Any("initial graph", entitlementGraph))
			if dontFixCycles {
				return fmt.Errorf("cycles detected in entitlement graph")
			}

			err := entitlementGraph.FixCycles()
			if err != nil {
				return err
			}
		}
	}

	err := s.expandGrantsForEntitlements(ctx)
	if err != nil {
		return err
	}

	return nil
}

// SyncGrants fetches the grants for each resource from the connector. It iterates each resource
// from the datastore, and pushes a new action to sync the grants for each individual resource.
func (s *syncer) SyncGrants(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.SyncGrants")
	defer span.End()

	if s.state.ResourceTypeID(ctx) == "" && s.state.ResourceID(ctx) == "" {
		pageToken := s.state.PageToken(ctx)

		if pageToken == "" {
			ctxzap.Extract(ctx).Info("Syncing grants...")
			s.handleInitialActionForStep(ctx, *s.state.Current())
		}

		resp, err := s.store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{PageToken: pageToken})
		if err != nil {
			return err
		}

		// We want to take action on the next page before we push any new actions
		if resp.NextPageToken != "" {
			err = s.state.NextPage(ctx, resp.NextPageToken)
			if err != nil {
				return err
			}
		} else {
			s.state.FinishAction(ctx)
		}

		for _, r := range resp.List {
			shouldSkip, err := s.shouldSkipEntitlementsAndGrants(ctx, r)
			if err != nil {
				return err
			}

			if shouldSkip {
				continue
			}
			s.state.PushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: r.Id.Resource, ResourceTypeID: r.Id.ResourceType})
		}

		return nil
	}
	err := s.syncGrantsForResource(ctx, &v2.ResourceId{
		ResourceType: s.state.ResourceTypeID(ctx),
		Resource:     s.state.ResourceID(ctx),
	})
	if err != nil {
		return err
	}

	return nil
}

type latestSyncFetcher interface {
	LatestFinishedSync(ctx context.Context) (string, error)
}

func (s *syncer) fetchResourceForPreviousSync(ctx context.Context, resourceID *v2.ResourceId) (string, *v2.ETag, error) {
	ctx, span := tracer.Start(ctx, "syncer.fetchResourceForPreviousSync")
	defer span.End()

	l := ctxzap.Extract(ctx)

	var previousSyncID string
	var err error

	if psf, ok := s.store.(latestSyncFetcher); ok {
		previousSyncID, err = psf.LatestFinishedSync(ctx)
		if err != nil {
			return "", nil, err
		}
	}

	if previousSyncID == "" {
		return "", nil, nil
	}

	var lastSyncResourceReqAnnos annotations.Annotations
	lastSyncResourceReqAnnos.Update(&c1zpb.SyncDetails{Id: previousSyncID})
	prevResource, err := s.store.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId:  resourceID,
		Annotations: lastSyncResourceReqAnnos,
	})
	// If we get an error while attempting to look up the previous sync, we should just log it and continue.
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			l.Debug(
				"resource was not found in previous sync",
				zap.String("resource_id", resourceID.Resource),
				zap.String("resource_type_id", resourceID.ResourceType),
			)
			return "", nil, nil
		}

		l.Error("error fetching resource for previous sync", zap.Error(err))
		return "", nil, err
	}

	pETag := &v2.ETag{}
	prevAnnos := annotations.Annotations(prevResource.Resource.GetAnnotations())
	ok, err := prevAnnos.Pick(pETag)
	if err != nil {
		return "", nil, err
	}
	if ok {
		return previousSyncID, pETag, nil
	}

	return previousSyncID, nil, nil
}

func (s *syncer) fetchEtaggedGrantsForResource(
	ctx context.Context,
	resource *v2.Resource,
	prevEtag *v2.ETag,
	prevSyncID string,
	grantResponse *v2.GrantsServiceListGrantsResponse,
) ([]*v2.Grant, bool, error) {
	ctx, span := tracer.Start(ctx, "syncer.fetchEtaggedGrantsForResource")
	defer span.End()

	respAnnos := annotations.Annotations(grantResponse.GetAnnotations())
	etagMatch := &v2.ETagMatch{}
	hasMatch, err := respAnnos.Pick(etagMatch)
	if err != nil {
		return nil, false, err
	}

	if !hasMatch {
		return nil, false, nil
	}

	var ret []*v2.Grant

	// No previous etag, so an etag match is not possible
	if prevEtag == nil {
		return nil, false, errors.New("connector returned an etag match but there is no previous sync generation to use")
	}

	// The previous etag is for a different entitlement
	if prevEtag.EntitlementId != etagMatch.EntitlementId {
		return nil, false, errors.New("connector returned an etag match but the entitlement id does not match the previous sync")
	}

	// We have a previous sync, and the connector would like to use the previous sync results
	var npt string
	// Fetch the grants for this resource from the previous sync, and store them in the current sync.
	storeAnnos := annotations.Annotations{}
	storeAnnos.Update(&c1zpb.SyncDetails{
		Id: prevSyncID,
	})

	for {
		prevGrantsResp, err := s.store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{
			Resource:    resource,
			Annotations: storeAnnos,
			PageToken:   npt,
		})
		if err != nil {
			return nil, false, err
		}

		for _, g := range prevGrantsResp.List {
			if g.Entitlement.Id != etagMatch.EntitlementId {
				continue
			}
			ret = append(ret, g)
		}

		if prevGrantsResp.NextPageToken == "" {
			break
		}
		npt = prevGrantsResp.NextPageToken
	}

	return ret, true, nil
}

// syncGrantsForResource fetches the grants for a specific resource from the connector.
func (s *syncer) syncGrantsForResource(ctx context.Context, resourceID *v2.ResourceId) error {
	ctx, span := tracer.Start(ctx, "syncer.syncGrantsForResource")
	defer span.End()

	resourceResponse, err := s.store.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: resourceID,
	})
	if err != nil {
		return err
	}

	resource := resourceResponse.Resource

	var prevSyncID string
	var prevEtag *v2.ETag
	var etagMatch bool
	var grants []*v2.Grant

	resourceAnnos := annotations.Annotations(resource.GetAnnotations())
	pageToken := s.state.PageToken(ctx)

	prevSyncID, prevEtag, err = s.fetchResourceForPreviousSync(ctx, resourceID)
	if err != nil {
		return err
	}
	resourceAnnos.Update(prevEtag)
	resource.Annotations = resourceAnnos

	resp, err := s.connector.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{Resource: resource, PageToken: pageToken})
	if err != nil {
		return err
	}

	// Fetch any etagged grants for this resource
	var etaggedGrants []*v2.Grant
	etaggedGrants, etagMatch, err = s.fetchEtaggedGrantsForResource(ctx, resource, prevEtag, prevSyncID, resp)
	if err != nil {
		return err
	}
	grants = append(grants, etaggedGrants...)

	// We want to process any grants from the previous sync first so that if there is a conflict, the newer data takes precedence
	grants = append(grants, resp.List...)

	for _, grant := range grants {
		grantAnnos := annotations.Annotations(grant.GetAnnotations())
		if grantAnnos.Contains(&v2.GrantExpandable{}) {
			s.state.SetNeedsExpansion()
		}
	}
	err = s.store.PutGrants(ctx, grants...)
	if err != nil {
		return err
	}

	s.handleProgress(ctx, s.state.Current(), len(grants))

	// We may want to update the etag on the resource. If we matched a previous etag, then we should use that.
	// Otherwise, we should use the etag from the response if provided.
	var updatedETag *v2.ETag

	if etagMatch {
		updatedETag = prevEtag
	} else {
		newETag := &v2.ETag{}
		respAnnos := annotations.Annotations(resp.GetAnnotations())
		ok, err := respAnnos.Pick(newETag)
		if err != nil {
			return err
		}
		if ok {
			updatedETag = newETag
		}
	}

	if updatedETag != nil {
		resourceAnnos.Update(updatedETag)
		resource.Annotations = resourceAnnos
		err = s.store.PutResources(ctx, resource)
		if err != nil {
			return err
		}
	}

	if resp.NextPageToken != "" {
		err = s.state.NextPage(ctx, resp.NextPageToken)
		if err != nil {
			return err
		}
		return nil
	}

	s.counts.GrantsProgress[resourceID.ResourceType] += 1
	s.counts.LogGrantsProgress(ctx, resourceID.ResourceType)
	s.state.FinishAction(ctx)

	return nil
}

func (s *syncer) runGrantExpandActions(ctx context.Context) (bool, error) {
	ctx, span := tracer.Start(ctx, "syncer.runGrantExpandActions")
	defer span.End()

	l := ctxzap.Extract(ctx)

	graph := s.state.EntitlementGraph(ctx)
	l = l.With(zap.Int("depth", graph.Depth))

	// Peek the next action on the stack
	if len(graph.Actions) == 0 {
		l.Debug("runGrantExpandActions: no actions", zap.Any("graph", graph))
		return true, nil
	}
	action := graph.Actions[0]

	l = l.With(zap.String("source_entitlement_id", action.SourceEntitlementID), zap.String("descendant_entitlement_id", action.DescendantEntitlementID))

	// Fetch source and descendant entitlement
	sourceEntitlement, err := s.store.GetEntitlement(ctx, &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
		EntitlementId: action.SourceEntitlementID,
	})
	if err != nil {
		l.Error("runGrantExpandActions: error fetching source entitlement", zap.Error(err))
		return false, fmt.Errorf("runGrantExpandActions: error fetching source entitlement: %w", err)
	}

	descendantEntitlement, err := s.store.GetEntitlement(ctx, &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
		EntitlementId: action.DescendantEntitlementID,
	})
	if err != nil {
		l.Error("runGrantExpandActions: error fetching descendant entitlement", zap.Error(err))
		return false, fmt.Errorf("runGrantExpandActions: error fetching descendant entitlement: %w", err)
	}

	// Fetch a page of source grants
	sourceGrants, err := s.store.ListGrantsForEntitlement(ctx, &reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
		Entitlement: sourceEntitlement.GetEntitlement(),
		PageToken:   action.PageToken,
	})
	if err != nil {
		l.Error("runGrantExpandActions: error fetching source grants", zap.Error(err))
		return false, fmt.Errorf("runGrantExpandActions: error fetching source grants: %w", err)
	}

	var newGrants []*v2.Grant = make([]*v2.Grant, 0)
	for _, sourceGrant := range sourceGrants.List {
		// Skip this grant if it is not for a resource type we care about
		if len(action.ResourceTypeIDs) > 0 {
			relevantResourceType := false
			for _, resourceTypeID := range action.ResourceTypeIDs {
				if sourceGrant.GetPrincipal().Id.ResourceType == resourceTypeID {
					relevantResourceType = true
					break
				}
			}

			if !relevantResourceType {
				continue
			}
		}

		// If this is a shallow action, then we only want to expand grants that have no sources which indicates that it was directly assigned.
		if action.Shallow {
			// If we have no sources, this is a direct grant
			foundDirectGrant := len(sourceGrant.GetSources().GetSources()) == 0
			// If the source grant has sources, then we need to see if any of them are the source entitlement itself
			for src := range sourceGrant.GetSources().GetSources() {
				if src == sourceEntitlement.GetEntitlement().GetId() {
					foundDirectGrant = true
					break
				}
			}

			// This is not a direct grant, so skip it since we are a shallow action
			if !foundDirectGrant {
				continue
			}
		}

		// Unroll all grants for the principal on the descendant entitlement. This should, on average, be... 1.
		descendantGrants := make([]*v2.Grant, 0, 1)
		pageToken := ""
		for {
			req := &reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
				Entitlement: descendantEntitlement.GetEntitlement(),
				PrincipalId: sourceGrant.GetPrincipal().GetId(),
				PageToken:   pageToken,
				Annotations: nil,
			}

			resp, err := s.store.ListGrantsForEntitlement(ctx, req)
			if err != nil {
				l.Error("runGrantExpandActions: error fetching descendant grants", zap.Error(err))
				return false, fmt.Errorf("runGrantExpandActions: error fetching descendant grants: %w", err)
			}

			descendantGrants = append(descendantGrants, resp.List...)
			pageToken = resp.NextPageToken
			if pageToken == "" {
				break
			}
		}

		// If we have no grants for the principal in the descendant entitlement, make one.
		directGrant := true
		if len(descendantGrants) == 0 {
			directGrant = false
			// TODO(morgabra): This is kinda gnarly, grant ID won't have any special meaning.
			// FIXME(morgabra): We should probably conflict check with grant id?
			descendantGrant, err := s.newExpandedGrant(ctx, descendantEntitlement.Entitlement, sourceGrant.GetPrincipal())
			if err != nil {
				l.Error("runGrantExpandActions: error creating new grant", zap.Error(err))
				return false, fmt.Errorf("runGrantExpandActions: error creating new grant: %w", err)
			}
			descendantGrants = append(descendantGrants, descendantGrant)
			l.Debug(
				"runGrantExpandActions: created new grant for expansion",
				zap.String("grant_id", descendantGrant.GetId()),
			)
		}

		// Add the source entitlement as a source to all descendant grants.
		for _, descendantGrant := range descendantGrants {
			sources := descendantGrant.GetSources()
			if sources == nil {
				sources = &v2.GrantSources{}
				descendantGrant.Sources = sources
			}
			sourcesMap := sources.GetSources()
			if sourcesMap == nil {
				sourcesMap = make(map[string]*v2.GrantSources_GrantSource)
				sources.Sources = sourcesMap
			}

			if directGrant && len(sources.Sources) == 0 {
				// If we are already granted this entitlement, make sure to add ourselves as a source.
				sourcesMap[descendantGrant.GetEntitlement().GetId()] = &v2.GrantSources_GrantSource{}
			}
			// Include the source grant as a source.
			sourcesMap[sourceGrant.GetEntitlement().GetId()] = &v2.GrantSources_GrantSource{}

			l.Debug(
				"runGrantExpandActions: updating sources for descendant grant",
				zap.String("grant_id", descendantGrant.GetId()),
				zap.Any("sources", sources),
			)
		}
		newGrants = append(newGrants, descendantGrants...)
	}

	err = s.store.PutGrants(ctx, newGrants...)
	if err != nil {
		l.Error("runGrantExpandActions: error updating descendant grants", zap.Error(err))
		return false, fmt.Errorf("runGrantExpandActions: error updating descendant grants: %w", err)
	}

	// If we have no more pages of work, pop the action off the stack and mark this edge in the graph as done
	action.PageToken = sourceGrants.NextPageToken
	if action.PageToken == "" {
		graph.MarkEdgeExpanded(action.SourceEntitlementID, action.DescendantEntitlementID)
		graph.Actions = graph.Actions[1:]
	}
	return false, nil
}

func (s *syncer) newExpandedGrant(_ context.Context, descEntitlement *v2.Entitlement, principal *v2.Resource) (*v2.Grant, error) {
	enResource := descEntitlement.GetResource()
	if enResource == nil {
		return nil, fmt.Errorf("newExpandedGrant: entitlement has no resource")
	}

	if principal == nil {
		return nil, fmt.Errorf("newExpandedGrant: principal is nil")
	}

	// Add immutable annotation since this function is only called if no direct grant exists
	var annos annotations.Annotations
	annos.Update(&v2.GrantImmutable{})

	grant := &v2.Grant{
		Id:          fmt.Sprintf("%s:%s:%s", descEntitlement.Id, principal.Id.ResourceType, principal.Id.Resource),
		Entitlement: descEntitlement,
		Principal:   principal,
		Annotations: annos,
	}

	return grant, nil
}

// expandGrantsForEntitlements expands grants for the given entitlement.
func (s *syncer) expandGrantsForEntitlements(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.expandGrantsForEntitlements")
	defer span.End()

	l := ctxzap.Extract(ctx)

	graph := s.state.EntitlementGraph(ctx)
	l = l.With(zap.Int("depth", graph.Depth))
	l.Debug("expandGrantsForEntitlements: start", zap.Any("graph", graph))

	s.counts.LogExpandProgress(ctx, graph.Actions)

	actionsDone, err := s.runGrantExpandActions(ctx)
	if err != nil {
		// Skip action and delete the edge that caused the error.
		erroredAction := graph.Actions[0]
		l.Error("expandGrantsForEntitlements: error running graph action", zap.Error(err), zap.Any("action", erroredAction))
		_ = graph.DeleteEdge(ctx, erroredAction.SourceEntitlementID, erroredAction.DescendantEntitlementID)
		graph.Actions = graph.Actions[1:]
		if len(graph.Actions) == 0 {
			actionsDone = true
		}
		// TODO: return a warning
	}
	if !actionsDone {
		return nil
	}

	if maxDepth == 0 {
		maxDepth = defaultMaxDepth
	}

	if int64(graph.Depth) > maxDepth {
		l.Error(
			"expandGrantsForEntitlements: exceeded max depth",
			zap.Any("graph", graph),
			zap.Int64("max_depth", maxDepth),
		)
		s.state.FinishAction(ctx)
		return fmt.Errorf("expandGrantsForEntitlements: exceeded max depth (%d)", maxDepth)
	}

	// TODO(morgabra) Yield here after some amount of work?
	// traverse edges or call some sort of getEntitlements
	for _, sourceEntitlementID := range graph.GetEntitlements() {
		// We've already expanded this entitlement, so skip it.
		if graph.IsEntitlementExpanded(sourceEntitlementID) {
			continue
		}

		// We have ancestors who have not been expanded yet, so we can't expand ourselves.
		if graph.HasUnexpandedAncestors(sourceEntitlementID) {
			l.Debug("expandGrantsForEntitlements: skipping source entitlement because it has unexpanded ancestors", zap.String("source_entitlement_id", sourceEntitlementID))
			continue
		}

		for descendantEntitlementID, grantInfo := range graph.GetDescendantEntitlements(sourceEntitlementID) {
			if grantInfo.IsExpanded {
				continue
			}
			graph.Actions = append(graph.Actions, &expand.EntitlementGraphAction{
				SourceEntitlementID:     sourceEntitlementID,
				DescendantEntitlementID: descendantEntitlementID,
				PageToken:               "",
				Shallow:                 grantInfo.IsShallow,
				ResourceTypeIDs:         grantInfo.ResourceTypeIDs,
			})
		}
	}

	if graph.IsExpanded() {
		l.Debug("expandGrantsForEntitlements: graph is expanded", zap.Any("graph", graph))
		s.state.FinishAction(ctx)
		return nil
	}

	graph.Depth++
	l.Debug("expandGrantsForEntitlements: graph is not expanded", zap.Any("graph", graph))
	return nil
}

func (s *syncer) loadStore(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.loadStore")
	defer span.End()

	if s.store != nil {
		return nil
	}

	if s.c1zManager == nil {
		m, err := manager.New(ctx, s.c1zPath, manager.WithTmpDir(s.tmpDir))
		if err != nil {
			return err
		}
		s.c1zManager = m
	}

	store, err := s.c1zManager.LoadC1Z(ctx)
	if err != nil {
		return err
	}

	s.store = store

	return nil
}

// Close closes the datastorage to ensure it is updated on disk.
func (s *syncer) Close(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.Close")
	defer span.End()

	var err error
	if s.store != nil {
		err = s.store.Close()
		if err != nil {
			return fmt.Errorf("error closing store: %w", err)
		}
	}

	if s.c1zManager != nil {
		err = s.c1zManager.SaveC1Z(ctx)
		if err != nil {
			return err
		}

		err = s.c1zManager.Close(ctx)
		if err != nil {
			return err
		}
	}

	return nil
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

// WithProgress sets a `progressHandler` for `NewSyncer` Options.
func WithProgressHandler(f func(s *Progress)) SyncOpt {
	return func(s *syncer) {
		if f != nil {
			s.progressHandler = f
		}
	}
}

func WithConnectorStore(store connectorstore.Writer) SyncOpt {
	return func(s *syncer) {
		s.store = store
	}
}

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

func WithSkipFullSync() SyncOpt {
	return func(s *syncer) {
		s.skipFullSync = true
	}
}

// NewSyncer returns a new syncer object.
func NewSyncer(ctx context.Context, c types.ConnectorClient, opts ...SyncOpt) (Syncer, error) {
	s := &syncer{
		connector:             c,
		skipEGForResourceType: make(map[string]bool),
		counts:                NewProgressCounts(),
	}

	for _, o := range opts {
		o(s)
	}

	if s.store == nil && s.c1zPath == "" {
		return nil, errors.New("a connector store writer or a db path must be provided")
	}

	return s, nil
}
