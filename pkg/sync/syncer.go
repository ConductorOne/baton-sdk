package sync

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/conductorone/baton-sdk/pkg/types/entitlement"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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
	case percentComplete == 100:
		l.Info("Synced entitlements",
			zap.String("resource_type_id", resourceType),
			zap.Int("count", entitlementsProgress),
			zap.Int("total", resources),
		)
		p.LastEntitlementLog[resourceType] = time.Time{}
	case time.Since(p.LastEntitlementLog[resourceType]) > maxLogFrequency:
		if entitlementsProgress > resources {
			l.Warn("more entitlement resources than resources",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", entitlementsProgress),
				zap.Int("total", resources),
			)
		} else {
			l.Info("Syncing entitlements",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", entitlementsProgress),
				zap.Int("total", resources),
				zap.Int("percent_complete", percentComplete),
			)
		}
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
	case percentComplete == 100:
		l.Info("Synced grants",
			zap.String("resource_type_id", resourceType),
			zap.Int("count", grantsProgress),
			zap.Int("total", resources),
		)
		p.LastGrantLog[resourceType] = time.Time{}
	case time.Since(p.LastGrantLog[resourceType]) > maxLogFrequency:
		if grantsProgress > resources {
			l.Warn("more grant resources than resources",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", grantsProgress),
				zap.Int("total", resources),
			)
		} else {
			l.Info("Syncing grants",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", grantsProgress),
				zap.Int("total", resources),
				zap.Int("percent_complete", percentComplete),
			)
		}
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
	c1zManager                          manager.Manager
	c1zPath                             string
	externalResourceC1ZPath             string
	externalResourceEntitlementIdFilter string
	store                               connectorstore.Writer
	externalResourceReader              connectorstore.Reader
	connector                           types.ConnectorClient
	state                               State
	runDuration                         time.Duration
	transitionHandler                   func(s Action)
	progressHandler                     func(p *Progress)
	tmpDir                              string
	skipFullSync                        bool
	lastCheckPointTime                  time.Time
	counts                              *ProgressCounts
	targetedSyncResourceIDs             []string
	onlyExpandGrants                    bool
	dontExpandGrants                    bool
	syncID                              string
	skipEGForResourceType               map[string]bool
	skipEntitlementsAndGrants           bool
	resourceTypeTraits                  map[string][]v2.ResourceType_Trait
	syncType                            connectorstore.SyncType
	injectSyncIDAnnotation              bool
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
	// If targetedSyncResourceIDs is not set, find the most recent unfinished sync of our desired sync type & resume it (regardless of partial or full).
	//   If there are no unfinished syncs of our desired sync type, start a new sync.
	// If targetedSyncResourceIDs is set, start a new partial sync. Use the most recent completed sync as the parent sync ID (if it exists).

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
	if len(s.targetedSyncResourceIDs) == 0 {
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

	// Validate any targeted resource IDs before starting a sync.
	targetedResources := []*v2.Resource{}
	for _, resourceID := range s.targetedSyncResourceIDs {
		r, err := bid.ParseResourceBid(resourceID)
		if err != nil {
			return fmt.Errorf("error parsing resource id %s: %w", resourceID, err)
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
		err = errors.New("no syncID found after starting or resuming sync")
		l.Error("no syncID found after starting or resuming sync", zap.Error(err))
		return err
	}

	// Add ActiveSync to context once after we have the syncID
	if syncID != "" {
		ctx = sessions.SetSyncIDInContext(ctx, syncID)
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

	retryer := retry.NewRetryer(ctx, retry.RetryConfig{
		MaxAttempts:  0,
		InitialDelay: 1 * time.Second,
		MaxDelay:     0,
	})

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

			if s.skipEntitlementsAndGrants {
				s.state.SetShouldSkipEntitlementsAndGrants()
			}
			if len(targetedResources) > 0 {
				for _, r := range targetedResources {
					s.state.PushAction(ctx, Action{
						Op:                   SyncTargetedResourceOp,
						ResourceID:           r.GetId().GetResource(),
						ResourceTypeID:       r.GetId().GetResourceType(),
						ParentResourceID:     r.GetParentResourceId().GetResource(),
						ParentResourceTypeID: r.GetParentResourceId().GetResourceType(),
					})
				}
				s.state.SetShouldFetchRelatedResources()
				s.state.PushAction(ctx, Action{Op: SyncResourceTypesOp})
				err = s.Checkpoint(ctx, true)
				if err != nil {
					return err
				}
				// Don't do grant expansion or external resources in partial syncs, as we likely lack related resources/entitlements/grants
				continue
			}

			// FIXME(jirwin): Disabling syncing assets for now
			// s.state.PushAction(ctx, Action{Op: SyncAssetsOp})
			if !s.state.ShouldSkipEntitlementsAndGrants() {
				s.state.PushAction(ctx, Action{Op: SyncGrantExpansionOp})
			}
			if s.externalResourceReader != nil {
				s.state.PushAction(ctx, Action{Op: SyncExternalResourcesOp})
			}
			if s.onlyExpandGrants {
				s.state.SetNeedsExpansion()
				err = s.Checkpoint(ctx, true)
				if err != nil {
					return err
				}
				continue
			}
			if !s.state.ShouldSkipEntitlementsAndGrants() {
				s.state.PushAction(ctx, Action{Op: SyncGrantsOp})
				s.state.PushAction(ctx, Action{Op: SyncEntitlementsOp})
			}
			s.state.PushAction(ctx, Action{Op: SyncResourcesOp})
			s.state.PushAction(ctx, Action{Op: SyncResourceTypesOp})

			err = s.Checkpoint(ctx, true)
			if err != nil {
				return err
			}
			continue

		case SyncResourceTypesOp:
			err = s.SyncResourceTypes(ctx)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return err
			}
			continue

		case SyncResourcesOp:
			err = s.SyncResources(ctx)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return err
			}
			continue

		case SyncTargetedResourceOp:
			err = s.SyncTargetedResource(ctx)
			if isWarning(ctx, err) {
				l.Warn("skipping sync targeted resource action", zap.Any("stateAction", stateAction), zap.Error(err))
				warnings = append(warnings, err)
				s.state.FinishAction(ctx)
				continue
			}
			if !retryer.ShouldWaitAndRetry(ctx, err) {
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
			if !retryer.ShouldWaitAndRetry(ctx, err) {
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
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return err
			}
			continue

		case SyncExternalResourcesOp:
			err = s.SyncExternalResources(ctx)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return err
			}
			continue
		case SyncAssetsOp:
			err = s.SyncAssets(ctx)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
				return err
			}
			continue

		case SyncGrantExpansionOp:
			if s.dontExpandGrants || !s.state.NeedsExpansion() {
				l.Debug("skipping grant expansion, no grants to expand")
				s.state.FinishAction(ctx)
				continue
			}

			err = s.SyncGrantExpansion(ctx)
			if !retryer.ShouldWaitAndRetry(ctx, err) {
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

	_, err = s.connector.Cleanup(ctx, v2.ConnectorServiceCleanupRequest_builder{
		ActiveSyncId: s.getActiveSyncID(),
	}.Build())
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

	resp, err := s.connector.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
		PageToken:    pageToken,
		ActiveSyncId: s.getActiveSyncID(),
	}.Build())
	if err != nil {
		return err
	}

	err = s.store.PutResourceTypes(ctx, resp.GetList()...)
	if err != nil {
		return err
	}

	s.counts.ResourceTypes += len(resp.GetList())
	s.handleProgress(ctx, s.state.Current(), len(resp.GetList()))

	if resp.GetNextPageToken() == "" {
		s.counts.LogResourceTypesProgress(ctx)
		s.state.FinishAction(ctx)
		return nil
	}

	err = s.state.NextPage(ctx, resp.GetNextPageToken())
	if err != nil {
		return err
	}

	return nil
}

// getSubResources fetches the sub resource types from a resources' annotations.
func (s *syncer) getSubResources(ctx context.Context, parent *v2.Resource) error {
	ctx, span := tracer.Start(ctx, "syncer.getSubResources")
	defer span.End()

	for _, a := range parent.GetAnnotations() {
		if a.MessageIs((*v2.ChildResourceType)(nil)) {
			crt := &v2.ChildResourceType{}
			err := a.UnmarshalTo(crt)
			if err != nil {
				return err
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
	defer span.End()

	resourceResp, err := s.connector.GetResource(ctx,
		v2.ResourceGetterServiceGetResourceRequest_builder{
			ResourceId:       resourceID,
			ParentResourceId: parentResourceID,
			ActiveSyncId:     s.getActiveSyncID(),
		}.Build(),
	)
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

func (s *syncer) SyncTargetedResource(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.SyncTargetedResource")
	defer span.End()

	resourceID := s.state.ResourceID(ctx)
	resourceTypeID := s.state.ResourceTypeID(ctx)
	if resourceID == "" || resourceTypeID == "" {
		return errors.New("cannot get resource without a resource target")
	}

	parentResourceID := s.state.ParentResourceID(ctx)
	parentResourceTypeID := s.state.ParentResourceTypeID(ctx)
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
		s.state.FinishAction(ctx)
		return nil
	}

	// Save our resource in the DB
	if err := s.store.PutResources(ctx, resource); err != nil {
		return err
	}

	s.state.FinishAction(ctx)

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

	shouldSkipEnts, err := s.shouldSkipEntitlementsAndGrants(ctx, resource)
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
func (s *syncer) SyncResources(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.SyncResources")
	defer span.End()

	if s.state.Current().ResourceTypeID == "" {
		pageToken := s.state.PageToken(ctx)

		if pageToken == "" {
			ctxzap.Extract(ctx).Info("Syncing resources...")
			s.handleInitialActionForStep(ctx, *s.state.Current())
		}

		resp, err := s.store.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageToken: pageToken}.Build())
		if err != nil {
			return err
		}

		if resp.GetNextPageToken() != "" {
			err = s.state.NextPage(ctx, resp.GetNextPageToken())
			if err != nil {
				return err
			}
		} else {
			s.state.FinishAction(ctx)
		}

		for _, rt := range resp.GetList() {
			action := Action{Op: SyncResourcesOp, ResourceTypeID: rt.GetId()}
			// If this request specified a parent resource, only queue up syncing resources for children of the parent resource
			if s.state.Current() != nil && s.state.Current().ParentResourceTypeID != "" && s.state.Current().ParentResourceID != "" {
				action.ParentResourceID = s.state.Current().ParentResourceID
				action.ParentResourceTypeID = s.state.Current().ParentResourceTypeID
			}

			s.state.PushAction(ctx, action)
		}

		return nil
	}

	return s.syncResources(ctx)
}

// syncResources fetches a given resource from the connector, and returns a slice of new child resources to fetch.
func (s *syncer) syncResources(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.syncResources")
	defer span.End()

	req := v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: s.state.ResourceTypeID(ctx),
		PageToken:      s.state.PageToken(ctx),
		ActiveSyncId:   s.getActiveSyncID(),
	}.Build()
	if s.state.ParentResourceTypeID(ctx) != "" && s.state.ParentResourceID(ctx) != "" {
		req.SetParentResourceId(v2.ResourceId_builder{
			ResourceType: s.state.ParentResourceTypeID(ctx),
			Resource:     s.state.ParentResourceID(ctx),
		}.Build())
	}

	resp, err := s.connector.ListResources(ctx, req)
	if err != nil {
		return err
	}

	s.handleProgress(ctx, s.state.Current(), len(resp.GetList()))

	resourceTypeId := s.state.ResourceTypeID(ctx)
	s.counts.Resources[resourceTypeId] += len(resp.GetList())

	if resp.GetNextPageToken() == "" {
		s.counts.LogResourcesProgress(ctx, resourceTypeId)
		s.state.FinishAction(ctx)
	} else {
		err = s.state.NextPage(ctx, resp.GetNextPageToken())
		if err != nil {
			return err
		}
	}

	bulkPutResoruces := []*v2.Resource{}
	for _, r := range resp.GetList() {
		// Check if we've already synced this resource, skip it if we have
		_, err = s.store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
			ResourceId: v2.ResourceId_builder{ResourceType: r.GetId().GetResourceType(), Resource: r.GetId().GetResource()}.Build(),
		}.Build())
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
		r.SetCreationSource(v2.Resource_CREATION_SOURCE_CONNECTOR_LIST_RESOURCES)

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

	resourceTypeTraits, ok := s.resourceTypeTraits[r.GetId().GetResourceType()]
	if !ok {
		resourceTypeResponse, err := s.store.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
			ResourceTypeId: r.GetId().GetResourceType(),
		}.Build())
		if err != nil {
			return err
		}
		resourceTypeTraits = resourceTypeResponse.GetResourceType().GetTraits()
		s.resourceTypeTraits[r.GetId().GetResourceType()] = resourceTypeTraits
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
func (s *syncer) shouldSkipEntitlementsAndGrants(ctx context.Context, r *v2.Resource) (bool, error) {
	ctx, span := tracer.Start(ctx, "syncer.shouldSkipEntitlementsAndGrants")
	defer span.End()

	if s.state.ShouldSkipEntitlementsAndGrants() {
		return true, nil
	}

	rAnnos := annotations.Annotations(r.GetAnnotations())
	if rAnnos.Contains(&v2.SkipEntitlementsAndGrants{}) {
		return true, nil
	}

	// We've checked this resource type, so we can return what we have cached directly.
	if skip, ok := s.skipEGForResourceType[r.GetId().GetResourceType()]; ok {
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
	s.skipEGForResourceType[r.GetId().GetResourceType()] = skipEntitlements

	return skipEntitlements, nil
}

func (s *syncer) shouldSkipGrants(ctx context.Context, r *v2.Resource) (bool, error) {
	annos := annotations.Annotations(r.GetAnnotations())
	if annos.Contains(&v2.SkipGrants{}) {
		return true, nil
	}

	return s.shouldSkipEntitlementsAndGrants(ctx, r)
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

		resp, err := s.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageToken: pageToken}.Build())
		if err != nil {
			return err
		}

		// We want to take action on the next page before we push any new actions
		if resp.GetNextPageToken() != "" {
			err = s.state.NextPage(ctx, resp.GetNextPageToken())
			if err != nil {
				return err
			}
		} else {
			s.state.FinishAction(ctx)
		}

		for _, r := range resp.GetList() {
			shouldSkipEntitlements, err := s.shouldSkipEntitlementsAndGrants(ctx, r)
			if err != nil {
				return err
			}
			if shouldSkipEntitlements {
				continue
			}
			s.state.PushAction(ctx, Action{Op: SyncEntitlementsOp, ResourceID: r.GetId().GetResource(), ResourceTypeID: r.GetId().GetResourceType()})
		}

		return nil
	}

	err := s.syncEntitlementsForResource(ctx, v2.ResourceId_builder{
		ResourceType: s.state.ResourceTypeID(ctx),
		Resource:     s.state.ResourceID(ctx),
	}.Build())
	if err != nil {
		return err
	}

	return nil
}

// syncEntitlementsForResource fetches the entitlements for a specific resource from the connector.
func (s *syncer) syncEntitlementsForResource(ctx context.Context, resourceID *v2.ResourceId) error {
	ctx, span := tracer.Start(ctx, "syncer.syncEntitlementsForResource")
	defer span.End()

	resourceResponse, err := s.store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: resourceID,
	}.Build())
	if err != nil {
		return err
	}

	pageToken := s.state.PageToken(ctx)

	resource := resourceResponse.GetResource()

	resp, err := s.connector.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource:     resource,
		PageToken:    pageToken,
		ActiveSyncId: s.getActiveSyncID(),
	}.Build())
	if err != nil {
		return err
	}
	err = s.store.PutEntitlements(ctx, resp.GetList()...)
	if err != nil {
		return err
	}

	s.handleProgress(ctx, s.state.Current(), len(resp.GetList()))

	if resp.GetNextPageToken() != "" {
		err = s.state.NextPage(ctx, resp.GetNextPageToken())
		if err != nil {
			return err
		}
	} else {
		s.counts.EntitlementsProgress[resourceID.GetResourceType()] += 1
		s.counts.LogEntitlementsProgress(ctx, resourceID.GetResourceType())

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
	resourceResponse, err := s.store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: resourceID,
	}.Build())
	if err != nil {
		return err
	}

	var assetRefs []*v2.AssetRef

	rAnnos := annotations.Annotations(resourceResponse.GetResource().GetAnnotations())

	userTrait := &v2.UserTrait{}
	ok, err := rAnnos.Pick(userTrait)
	if err != nil {
		return err
	}
	if ok {
		assetRefs = append(assetRefs, userTrait.GetIcon())
	}

	grpTrait := &v2.GroupTrait{}
	ok, err = rAnnos.Pick(grpTrait)
	if err != nil {
		return err
	}
	if ok {
		assetRefs = append(assetRefs, grpTrait.GetIcon())
	}

	appTrait := &v2.AppTrait{}
	ok, err = rAnnos.Pick(appTrait)
	if err != nil {
		return err
	}
	if ok {
		assetRefs = append(assetRefs, appTrait.GetIcon(), appTrait.GetLogo())
	}

	for _, assetRef := range assetRefs {
		if assetRef == nil {
			continue
		}

		l.Debug("fetching asset", zap.String("asset_ref_id", assetRef.GetId()))
		resp, err := s.connector.GetAsset(ctx, v2.AssetServiceGetAssetRequest_builder{Asset: assetRef}.Build())
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

		err = s.store.PutAsset(ctx, assetRef, metadata.GetContentType(), assetBytes.Bytes())
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

		resp, err := s.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageToken: pageToken}.Build())
		if err != nil {
			return err
		}

		// We want to take action on the next page before we push any new actions
		if resp.GetNextPageToken() != "" {
			err = s.state.NextPage(ctx, resp.GetNextPageToken())
			if err != nil {
				return err
			}
		} else {
			s.state.FinishAction(ctx)
		}

		for _, r := range resp.GetList() {
			s.state.PushAction(ctx, Action{Op: SyncAssetsOp, ResourceID: r.GetId().GetResource(), ResourceTypeID: r.GetId().GetResourceType()})
		}

		return nil
	}

	err := s.syncAssetsForResource(ctx, v2.ResourceId_builder{
		ResourceType: s.state.ResourceTypeID(ctx),
		Resource:     s.state.ResourceID(ctx),
	}.Build())
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
		resp, err := s.store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		if err != nil {
			return err
		}

		// We want to take action on the next page before we push any new actions
		if resp.GetNextPageToken() != "" {
			err = s.state.NextPage(ctx, resp.GetNextPageToken())
			if err != nil {
				return err
			}
		} else {
			l.Debug("Finished loading grants to expand")
			entitlementGraph.Loaded = true
		}

		for _, grant := range resp.GetList() {
			annos := annotations.Annotations(grant.GetAnnotations())
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
			for _, srcEntitlementID := range expandable.GetEntitlementIds() {
				l.Debug(
					"Expandable entitlement found",
					zap.String("src_entitlement_id", srcEntitlementID),
					zap.String("dst_entitlement_id", grant.GetEntitlement().GetId()),
				)

				srcEntitlement, err := s.store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
					EntitlementId: srcEntitlementID,
				}.Build())
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
				if principalID.GetResourceType() != sourceEntitlementResourceID.GetResourceType() ||
					principalID.GetResource() != sourceEntitlementResourceID.GetResource() {
					l.Error(
						"source entitlement resource id did not match grant principal id",
						zap.String("grant_principal_id", principalID.String()),
						zap.String("source_entitlement_resource_id", sourceEntitlementResourceID.String()))

					return fmt.Errorf("source entitlement resource id did not match grant principal id")
				}

				entitlementGraph.AddEntitlement(grant.GetEntitlement())
				entitlementGraph.AddEntitlement(srcEntitlement.GetEntitlement())
				err = entitlementGraph.AddEdge(ctx,
					srcEntitlement.GetEntitlement().GetId(),
					grant.GetEntitlement().GetId(),
					expandable.GetShallow(),
					expandable.GetResourceTypeIds(),
				)
				if err != nil {
					return fmt.Errorf("error adding edge to graph: %w", err)
				}
			}
		}
		if entitlementGraph.Loaded {
			l.Info("Finished loading entitlement graph", zap.Int("edges", len(entitlementGraph.Edges)))
		}
		return nil
	}

	if entitlementGraph.Loaded {
		comps, sccMetrics := entitlementGraph.ComputeCyclicComponents(ctx)
		if len(comps) > 0 {
			// Log a sample cycle
			l.Warn(
				"cycle detected in entitlement graph",
				zap.Any("cycle", comps[0]),
				zap.Any("scc_metrics", sccMetrics),
			)
			l.Debug("initial graph stats",
				zap.Int("edges", len(entitlementGraph.Edges)),
				zap.Int("nodes", len(entitlementGraph.Nodes)),
				zap.Int("actions", len(entitlementGraph.Actions)),
				zap.Int("depth", entitlementGraph.Depth),
				zap.Bool("has_no_cycles", entitlementGraph.HasNoCycles),
			)
			if dontFixCycles {
				return fmt.Errorf("cycles detected in entitlement graph")
			}
			err := entitlementGraph.FixCyclesFromComponents(ctx, comps)
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

		resp, err := s.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageToken: pageToken}.Build())
		if err != nil {
			return err
		}

		// We want to take action on the next page before we push any new actions
		if resp.GetNextPageToken() != "" {
			err = s.state.NextPage(ctx, resp.GetNextPageToken())
			if err != nil {
				return err
			}
		} else {
			s.state.FinishAction(ctx)
		}

		for _, r := range resp.GetList() {
			shouldSkip, err := s.shouldSkipGrants(ctx, r)
			if err != nil {
				return err
			}

			if shouldSkip {
				continue
			}
			s.state.PushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: r.GetId().GetResource(), ResourceTypeID: r.GetId().GetResourceType()})
		}

		return nil
	}
	err := s.syncGrantsForResource(ctx, v2.ResourceId_builder{
		ResourceType: s.state.ResourceTypeID(ctx),
		Resource:     s.state.ResourceID(ctx),
	}.Build())
	if err != nil {
		return err
	}

	return nil
}

type latestSyncFetcher interface {
	LatestFinishedSync(ctx context.Context, syncType connectorstore.SyncType) (string, error)
}

func (s *syncer) fetchResourceForPreviousSync(ctx context.Context, resourceID *v2.ResourceId) (string, *v2.ETag, error) {
	ctx, span := tracer.Start(ctx, "syncer.fetchResourceForPreviousSync")
	defer span.End()

	l := ctxzap.Extract(ctx)

	var previousSyncID string
	var err error

	if psf, ok := s.store.(latestSyncFetcher); ok {
		previousSyncID, err = psf.LatestFinishedSync(ctx, connectorstore.SyncTypeFull)
		if err != nil {
			return "", nil, err
		}
	}

	if previousSyncID == "" {
		return "", nil, nil
	}

	var lastSyncResourceReqAnnos annotations.Annotations
	lastSyncResourceReqAnnos.Update(c1zpb.SyncDetails_builder{Id: previousSyncID}.Build())
	prevResource, err := s.store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId:  resourceID,
		Annotations: lastSyncResourceReqAnnos,
	}.Build())
	// If we get an error while attempting to look up the previous sync, we should just log it and continue.
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			l.Debug(
				"resource was not found in previous sync",
				zap.String("resource_id", resourceID.GetResource()),
				zap.String("resource_type_id", resourceID.GetResourceType()),
			)
			return "", nil, nil
		}

		l.Error("error fetching resource for previous sync", zap.Error(err))
		return "", nil, err
	}

	pETag := &v2.ETag{}
	prevAnnos := annotations.Annotations(prevResource.GetResource().GetAnnotations())
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
	if prevEtag.GetEntitlementId() != etagMatch.GetEntitlementId() {
		return nil, false, errors.New("connector returned an etag match but the entitlement id does not match the previous sync")
	}

	// We have a previous sync, and the connector would like to use the previous sync results
	var npt string
	// Fetch the grants for this resource from the previous sync, and store them in the current sync.
	storeAnnos := annotations.Annotations{}
	storeAnnos.Update(c1zpb.SyncDetails_builder{
		Id: prevSyncID,
	}.Build())
	for {
		prevGrantsResp, err := s.store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			Resource:    resource,
			Annotations: storeAnnos,
			PageToken:   npt,
		}.Build())
		if err != nil {
			return nil, false, err
		}

		for _, g := range prevGrantsResp.GetList() {
			if g.GetEntitlement().GetId() != etagMatch.GetEntitlementId() {
				continue
			}
			ret = append(ret, g)
		}

		if prevGrantsResp.GetNextPageToken() == "" {
			break
		}
		npt = prevGrantsResp.GetNextPageToken()
	}

	return ret, true, nil
}

// syncGrantsForResource fetches the grants for a specific resource from the connector.
func (s *syncer) syncGrantsForResource(ctx context.Context, resourceID *v2.ResourceId) error {
	ctx, span := tracer.Start(ctx, "syncer.syncGrantsForResource")
	defer span.End()

	resourceResponse, err := s.store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: resourceID,
	}.Build())
	if err != nil {
		return err
	}

	resource := resourceResponse.GetResource()

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
	resource.SetAnnotations(resourceAnnos)

	resp, err := s.connector.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource:     resource,
		PageToken:    pageToken,
		ActiveSyncId: s.getActiveSyncID(),
	}.Build())
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
	grants = append(grants, resp.GetList()...)

	l := ctxzap.Extract(ctx)
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
			if !errors.Is(err, sql.ErrNoRows) {
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
		resource.SetAnnotations(resourceAnnos)
		err = s.store.PutResources(ctx, resource)
		if err != nil {
			return err
		}
	}

	if resp.GetNextPageToken() != "" {
		err = s.state.NextPage(ctx, resp.GetNextPageToken())
		if err != nil {
			return err
		}
		return nil
	}

	s.counts.GrantsProgress[resourceID.GetResourceType()] += 1
	s.counts.LogGrantsProgress(ctx, resourceID.GetResourceType())
	s.state.FinishAction(ctx)

	return nil
}

func (s *syncer) SyncExternalResources(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.SyncExternalResources")
	defer span.End()

	l := ctxzap.Extract(ctx)
	l.Info("Syncing external resources")

	if s.externalResourceEntitlementIdFilter != "" {
		return s.SyncExternalResourcesWithGrantToEntitlement(ctx, s.externalResourceEntitlementIdFilter)
	} else {
		return s.SyncExternalResourcesUsersAndGroups(ctx)
	}
}

func (s *syncer) SyncExternalResourcesWithGrantToEntitlement(ctx context.Context, entitlementId string) error {
	ctx, span := tracer.Start(ctx, "syncer.SyncExternalResourcesWithGrantToEntitlement")
	defer span.End()

	l := ctxzap.Extract(ctx)
	l.Info("Syncing external baton resources with grants to entitlement...")

	skipEGForResourceType := make(map[string]bool)

	filterEntitlement, err := s.externalResourceReader.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: entitlementId,
	}.Build())
	if err != nil {
		return err
	}

	grants, err := s.listExternalGrantsForEntitlement(ctx, filterEntitlement.GetEntitlement())
	if err != nil {
		return err
	}

	ents := make([]*v2.Entitlement, 0)
	principals := make([]*v2.Resource, 0)
	resourceTypes := make([]*v2.ResourceType, 0)
	resourceTypeIDs := mapset.NewSet[string]()
	resourceIDs := make(map[string]*v2.ResourceId)

	grantsForEnts := make([]*v2.Grant, 0)

	for _, g := range grants {
		resourceTypeIDs.Add(g.GetPrincipal().GetId().GetResourceType())
		resourceIDs[g.GetPrincipal().GetId().GetResource()] = g.GetPrincipal().GetId()
	}

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

	for _, resourceId := range resourceIDs {
		resourceResp, err := s.externalResourceReader.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{ResourceId: resourceId}.Build())
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
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
	}

	for _, ent := range ents {
		rAnnos := annotations.Annotations(ent.GetResource().GetAnnotations())
		if rAnnos.Contains(&v2.SkipGrants{}) {
			continue
		}
		grantsForEnt, err := s.listExternalGrantsForEntitlement(ctx, ent)
		if err != nil {
			return err
		}
		grantsForEnts = append(grantsForEnts, grantsForEnt...)
	}

	err = s.store.PutResourceTypes(ctx, resourceTypes...)
	if err != nil {
		return err
	}

	err = s.store.PutResources(ctx, principals...)
	if err != nil {
		return err
	}

	err = s.store.PutEntitlements(ctx, ents...)
	if err != nil {
		return err
	}

	err = s.store.PutGrants(ctx, grantsForEnts...)
	if err != nil {
		return err
	}

	l.Info("Synced external resources for entitlement",
		zap.Int("resource_type_count", len(resourceTypes)),
		zap.Int("resource_count", len(principals)),
		zap.Int("entitlement_count", len(ents)),
		zap.Int("grant_count", len(grantsForEnts)),
	)

	err = s.processGrantsWithExternalPrincipals(ctx, principals)
	if err != nil {
		return err
	}

	s.state.FinishAction(ctx)

	return nil
}

func (s *syncer) SyncExternalResourcesUsersAndGroups(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.SyncExternalResourcesUsersAndGroups")
	defer span.End()

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
	grantsForEnts := make([]*v2.Grant, 0)
	for _, rt := range resourceTypes {
		for _, t := range rt.GetTraits() {
			if t == v2.ResourceType_TRAIT_USER || t == v2.ResourceType_TRAIT_GROUP {
				userAndGroupResourceTypes = append(userAndGroupResourceTypes, rt)
				continue
			}
		}
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
	}

	for _, ent := range ents {
		rAnnos := annotations.Annotations(ent.GetResource().GetAnnotations())
		if rAnnos.Contains(&v2.SkipGrants{}) {
			continue
		}
		grantsForEnt, err := s.listExternalGrantsForEntitlement(ctx, ent)
		if err != nil {
			return err
		}
		grantsForEnts = append(grantsForEnts, grantsForEnt...)
	}

	err = s.store.PutResourceTypes(ctx, userAndGroupResourceTypes...)
	if err != nil {
		return err
	}

	err = s.store.PutResources(ctx, principals...)
	if err != nil {
		return err
	}

	err = s.store.PutEntitlements(ctx, ents...)
	if err != nil {
		return err
	}

	err = s.store.PutGrants(ctx, grantsForEnts...)
	if err != nil {
		return err
	}

	l.Info("Synced external resources",
		zap.Int("resource_type_count", len(userAndGroupResourceTypes)),
		zap.Int("resource_count", len(principals)),
		zap.Int("entitlement_count", len(ents)),
		zap.Int("grant_count", len(grantsForEnts)),
	)

	err = s.processGrantsWithExternalPrincipals(ctx, principals)
	if err != nil {
		return err
	}

	s.state.FinishAction(ctx)

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
	return ents, nil
}

func (s *syncer) listExternalGrantsForEntitlement(ctx context.Context, ent *v2.Entitlement) ([]*v2.Grant, error) {
	grantsForEnts := make([]*v2.Grant, 0)
	entitlementGrantPageToken := ""
	for {
		grantsForEntitlementResp, err := s.externalResourceReader.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: ent,
			PageToken:   entitlementGrantPageToken,
		}.Build())
		if err != nil {
			return nil, err
		}
		grantsForEnts = append(grantsForEnts, grantsForEntitlementResp.GetList()...)
		entitlementGrantPageToken = grantsForEntitlementResp.GetNextPageToken()
		if entitlementGrantPageToken == "" {
			break
		}
	}
	return grantsForEnts, nil
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

func (s *syncer) listAllGrants(ctx context.Context) ([]*v2.Grant, error) {
	grants := make([]*v2.Grant, 0)
	pageToken := ""
	for {
		grantsResp, err := s.store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageToken: pageToken,
		}.Build())
		if err != nil {
			return nil, err
		}

		grants = append(grants, grantsResp.GetList()...)
		pageToken = grantsResp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return grants, nil
}

func (s *syncer) processGrantsWithExternalPrincipals(ctx context.Context, principals []*v2.Resource) error {
	ctx, span := tracer.Start(ctx, "processGrantsWithExternalPrincipals")
	defer span.End()

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

	grantsToDelete := make([]string, 0)
	expandedGrants := make([]*v2.Grant, 0)

	grants, err := s.listAllGrants(ctx)
	if err != nil {
		return err
	}

	for _, grant := range grants {
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
			grantsToDelete = append(grantsToDelete, grant.GetId())
			continue
		}

		expandableAnno, err := GetExpandableAnnotation(annos)
		if err != nil {
			return err
		}
		expandableEntitlementsResourceMap := make(map[string][]*v2.Entitlement)
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
				entitlementMap, ok := expandableEntitlementsResourceMap[resourceBID]
				if !ok {
					entitlementMap = make([]*v2.Entitlement, 0)
				}
				entitlementMap = append(entitlementMap, parsedEnt)
				expandableEntitlementsResourceMap[resourceBID] = entitlementMap
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

					principalEntitlements := expandableEntitlementsResourceMap[groupPrincipalBID]
					for _, expandableGrant := range principalEntitlements {
						newExpandableEntId := entitlement.NewEntitlementID(principal, expandableGrant.GetSlug())
						_, err := s.store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{EntitlementId: newExpandableEntId}.Build())
						if err != nil {
							if errors.Is(err, sql.ErrNoRows) {
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
			grantsToDelete = append(grantsToDelete, grant.GetId())
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
					profileVal, ok := resource.GetProfileStringValue(userTrait.GetProfile(), matchExternalResource.GetKey())
					if ok && strings.EqualFold(profileVal, matchExternalResource.GetValue()) {
						newGrant := newGrantForExternalPrincipal(grant, userPrincipal)
						expandedGrants = append(expandedGrants, newGrant)
					}
				}
			case v2.ResourceType_TRAIT_GROUP:
				for _, groupPrincipal := range groupPrincipals {
					groupTrait, err := resource.GetGroupTrait(groupPrincipal)
					if err != nil {
						l.Error("error getting group trait", zap.Any("groupPrincipal", groupPrincipal))
						continue
					}
					profileVal, ok := resource.GetProfileStringValue(groupTrait.GetProfile(), matchExternalResource.GetKey())
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

							principalEntitlements := expandableEntitlementsResourceMap[groupPrincipalBID]
							for _, expandableGrant := range principalEntitlements {
								newExpandableEntId := entitlement.NewEntitlementID(groupPrincipal, expandableGrant.GetSlug())
								_, err := s.store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{EntitlementId: newExpandableEntId}.Build())
								if err != nil {
									if errors.Is(err, sql.ErrNoRows) {
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
			grantsToDelete = append(grantsToDelete, grant.GetId())
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

	for _, grantId := range grantsToDelete {
		if newGrantIDs.ContainsOne(grantId) {
			continue
		}
		err = s.store.DeleteGrant(ctx, grantId)
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

func (s *syncer) runGrantExpandActions(ctx context.Context) (bool, error) {
	ctx, span := tracer.Start(ctx, "syncer.runGrantExpandActions")
	defer span.End()

	l := ctxzap.Extract(ctx)

	graph := s.state.EntitlementGraph(ctx)
	l = l.With(zap.Int("depth", graph.Depth))

	// Peek the next action on the stack
	if len(graph.Actions) == 0 {
		l.Debug("runGrantExpandActions: no actions") // zap.Any("graph", graph),

		return true, nil
	}
	action := graph.Actions[0]

	l = l.With(zap.String("source_entitlement_id", action.SourceEntitlementID), zap.String("descendant_entitlement_id", action.DescendantEntitlementID))

	// Fetch source and descendant entitlement
	sourceEntitlement, err := s.store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: action.SourceEntitlementID,
	}.Build())
	if err != nil {
		l.Error("runGrantExpandActions: error fetching source entitlement", zap.Error(err))
		return false, fmt.Errorf("runGrantExpandActions: error fetching source entitlement: %w", err)
	}

	descendantEntitlement, err := s.store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: action.DescendantEntitlementID,
	}.Build())
	if err != nil {
		l.Error("runGrantExpandActions: error fetching descendant entitlement", zap.Error(err))
		return false, fmt.Errorf("runGrantExpandActions: error fetching descendant entitlement: %w", err)
	}

	// Fetch a page of source grants
	sourceGrants, err := s.store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: sourceEntitlement.GetEntitlement(),
		PageToken:   action.PageToken,
	}.Build())
	if err != nil {
		l.Error("runGrantExpandActions: error fetching source grants", zap.Error(err))
		return false, fmt.Errorf("runGrantExpandActions: error fetching source grants: %w", err)
	}

	var newGrants = make([]*v2.Grant, 0)
	for _, sourceGrant := range sourceGrants.GetList() {
		// Skip this grant if it is not for a resource type we care about
		if len(action.ResourceTypeIDs) > 0 {
			relevantResourceType := false
			for _, resourceTypeID := range action.ResourceTypeIDs {
				if sourceGrant.GetPrincipal().GetId().GetResourceType() == resourceTypeID {
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
			req := reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
				Entitlement: descendantEntitlement.GetEntitlement(),
				PrincipalId: sourceGrant.GetPrincipal().GetId(),
				PageToken:   pageToken,
				Annotations: nil,
			}.Build()

			resp, err := s.store.ListGrantsForEntitlement(ctx, req)
			if err != nil {
				l.Error("runGrantExpandActions: error fetching descendant grants", zap.Error(err))
				return false, fmt.Errorf("runGrantExpandActions: error fetching descendant grants: %w", err)
			}

			descendantGrants = append(descendantGrants, resp.GetList()...)
			pageToken = resp.GetNextPageToken()
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
			descendantGrant, err := s.newExpandedGrant(ctx, descendantEntitlement.GetEntitlement(), sourceGrant.GetPrincipal())
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
				descendantGrant.SetSources(sources)
			}
			sourcesMap := sources.GetSources()
			if sourcesMap == nil {
				sourcesMap = make(map[string]*v2.GrantSources_GrantSource)
				sources.SetSources(sourcesMap)
			}

			if directGrant && len(sources.GetSources()) == 0 {
				// If we are already granted this entitlement, make sure to add ourselves as a source.
				sourcesMap[descendantGrant.GetEntitlement().GetId()] = &v2.GrantSources_GrantSource{}
			}
			// Include the source grant as a source.
			sourcesMap[sourceGrant.GetEntitlement().GetId()] = &v2.GrantSources_GrantSource{}
		}
		newGrants = append(newGrants, descendantGrants...)
	}

	err = s.store.PutGrants(ctx, newGrants...)
	if err != nil {
		l.Error("runGrantExpandActions: error updating descendant grants", zap.Error(err))
		return false, fmt.Errorf("runGrantExpandActions: error updating descendant grants: %w", err)
	}

	// If we have no more pages of work, pop the action off the stack and mark this edge in the graph as done
	action.PageToken = sourceGrants.GetNextPageToken()
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

	grant := v2.Grant_builder{
		Id:          fmt.Sprintf("%s:%s:%s", descEntitlement.GetId(), principal.GetId().GetResourceType(), principal.GetId().GetResource()),
		Entitlement: descEntitlement,
		Principal:   principal,
		Annotations: annos,
	}.Build()

	return grant, nil
}

// expandGrantsForEntitlements expands grants for the given entitlement.
func (s *syncer) expandGrantsForEntitlements(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.expandGrantsForEntitlements")
	defer span.End()

	l := ctxzap.Extract(ctx)

	graph := s.state.EntitlementGraph(ctx)
	l = l.With(zap.Int("depth", graph.Depth))
	l.Debug("expandGrantsForEntitlements: start") // zap.Any("graph", graph)

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
			// zap.Any("graph", graph),
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
		l.Debug("expandGrantsForEntitlements: graph is expanded") // zap.Any("graph", graph)
		s.state.FinishAction(ctx)
		return nil
	}

	graph.Depth++
	l.Debug("expandGrantsForEntitlements: graph is not expanded") // zap.Any("graph", graph)
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

	if s.externalResourceReader != nil {
		err = s.externalResourceReader.Close()
		if err != nil {
			return fmt.Errorf("error closing external resource reader: %w", err)
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

func WithExternalResourceC1ZPath(path string) SyncOpt {
	return func(s *syncer) {
		s.externalResourceC1ZPath = path
	}
}

func WithExternalResourceEntitlementIdFilter(entitlementId string) SyncOpt {
	return func(s *syncer) {
		s.externalResourceEntitlementIdFilter = entitlementId
	}
}

func WithTargetedSyncResourceIDs(resourceIDs []string) SyncOpt {
	return func(s *syncer) {
		s.targetedSyncResourceIDs = resourceIDs
		if len(resourceIDs) > 0 {
			s.syncType = connectorstore.SyncTypePartial
			return
		}
		// No targeted resource IDs, so we need to update the sync type to either full or resources only.
		WithSkipEntitlementsAndGrants(s.skipEntitlementsAndGrants)(s)
	}
}

func WithOnlyExpandGrants() SyncOpt {
	return func(s *syncer) {
		s.onlyExpandGrants = true
	}
}

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

// NewSyncer returns a new syncer object.
func NewSyncer(ctx context.Context, c types.ConnectorClient, opts ...SyncOpt) (Syncer, error) {
	s := &syncer{
		connector:             c,
		skipEGForResourceType: make(map[string]bool),
		resourceTypeTraits:    make(map[string][]v2.ResourceType_Trait),
		counts:                NewProgressCounts(),
		syncType:              connectorstore.SyncTypeFull,
	}

	for _, o := range opts {
		o(s)
	}

	if s.store == nil && s.c1zPath == "" {
		return nil, errors.New("a connector store writer or a db path must be provided")
	}

	if s.externalResourceC1ZPath != "" {
		externalC1ZReader, err := dotc1z.NewExternalC1FileReader(ctx, s.tmpDir, s.externalResourceC1ZPath)
		if err != nil {
			return nil, err
		}
		s.externalResourceReader = externalC1ZReader
	}

	return s, nil
}
