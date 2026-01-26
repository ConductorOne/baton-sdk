package sync

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"slices"
	"strconv"
	"strings"
	native_sync "sync"
	"sync/atomic"
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

var dontFixCycles, _ = strconv.ParseBool(os.Getenv("BATON_DONT_FIX_CYCLES"))

var ErrSyncNotComplete = fmt.Errorf("sync exited without finishing")
var ErrTooManyWarnings = fmt.Errorf("too many warnings, exiting sync")
var ErrNoSyncIDFound = fmt.Errorf("no syncID found after starting or resuming sync")

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
	targetedSyncResources               []*v2.Resource
	onlyExpandGrants                    bool
	dontExpandGrants                    bool
	syncID                              string
	skipEGForResourceType               map[string]bool
	skipEntitlementsForResourceType     map[string]bool
	skipEntitlementsAndGrants           bool
	skipGrants                          bool
	resourceTypeTraits                  map[string][]v2.ResourceType_Trait
	syncType                            connectorstore.SyncType
	injectSyncIDAnnotation              bool
	setSessionStore                     sessions.SetSessionStore
	syncResourceTypes                   []string
	previousSyncMu                      native_sync.Mutex
	previousSyncIDPtr                   atomic.Pointer[string]
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

func (s *syncer) getPreviousFullSyncID(ctx context.Context) (string, error) {
	if ptr := s.previousSyncIDPtr.Load(); ptr != nil {
		return *ptr, nil
	}

	s.previousSyncMu.Lock()
	defer s.previousSyncMu.Unlock()

	if ptr := s.previousSyncIDPtr.Load(); ptr != nil {
		return *ptr, nil
	}

	psf, ok := s.store.(latestSyncFetcher)
	if !ok {
		empty := ""
		s.previousSyncIDPtr.Store(&empty)
		return "", nil
	}

	previousSyncID, err := psf.LatestFinishedSync(ctx, connectorstore.SyncTypeFull)
	if err == nil {
		s.previousSyncIDPtr.Store(&previousSyncID)
	}

	return previousSyncID, err
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

	state := &state{}
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
		)
	}

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

		// If we have more than 10 warnings and more than 10% of actions ended in a warning, exit the sync.
		if len(warnings) > 10 {
			completedActionsCount := s.state.GetCompletedActionsCount()
			if completedActionsCount > 0 && float64(len(warnings))/float64(completedActionsCount) > 0.1 {
				return fmt.Errorf("%w: warnings: %v completed actions: %d", ErrTooManyWarnings, warnings, completedActionsCount)
			}
		}
		select {
		case <-runCtx.Done():
			err = context.Cause(runCtx)
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				l.Info("sync run duration has expired, exiting sync early", zap.String("sync_id", syncID))
				// It would be nice to remove this once we're more confident in the checkpointing logic.
				checkpointErr := s.Checkpoint(ctx, true)
				if checkpointErr != nil {
					l.Error("error checkpointing before exiting sync", zap.Error(checkpointErr))
				}
				return errors.Join(checkpointErr, ErrSyncNotComplete)
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
			if s.skipGrants {
				s.state.SetShouldSkipGrants()
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
				if !s.state.ShouldSkipGrants() {
					s.state.PushAction(ctx, Action{Op: SyncGrantsOp})
				}

				s.state.PushAction(ctx, Action{Op: SyncEntitlementsOp})

				s.state.PushAction(ctx, Action{Op: SyncStaticEntitlementsOp})
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

		case SyncStaticEntitlementsOp:
			err = s.SyncStaticEntitlements(ctx)
			if isWarning(ctx, err) {
				l.Warn("skipping sync static entitlements action", zap.Any("stateAction", stateAction), zap.Error(err))
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

	// Force a checkpoint to clear completed actions & entitlement graph in sync_token.
	s.state.ClearEntitlementGraph(ctx)

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

func (s *syncer) listAllResourceTypes(ctx context.Context) iter.Seq2[[]*v2.ResourceType, error] {
	return func(yield func([]*v2.ResourceType, error) bool) {
		pageToken := ""
		for {
			resp, err := s.connector.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageToken: pageToken}.Build())
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

	s.counts.ResourceTypes += len(resourceTypes)
	s.handleProgress(ctx, s.state.Current(), len(resourceTypes))

	if resp.GetNextPageToken() == "" {
		s.counts.LogResourceTypesProgress(ctx)

		if len(s.syncResourceTypes) > 0 {
			validResourceTypesResp, err := s.store.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageToken: pageToken}.Build())
			if err != nil {
				return err
			}
			err = validateSyncResourceTypesFilter(s.syncResourceTypes, validResourceTypesResp.GetList())
			if err != nil {
				return err
			}
		}

		s.state.FinishAction(ctx)
		return nil
	}

	err = s.state.NextPage(ctx, resp.GetNextPageToken())
	if err != nil {
		return err
	}

	return nil
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
func (s *syncer) getSubResources(ctx context.Context, parent *v2.Resource) error {
	ctx, span := tracer.Start(ctx, "syncer.getSubResources")
	defer span.End()

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

		if err != nil && !errors.Is(err, sql.ErrNoRows) {
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

	if s.state.ShouldSkipGrants() {
		return true, nil
	}

	return s.shouldSkipEntitlementsAndGrants(ctx, r)
}

func (s *syncer) shouldSkipEntitlements(ctx context.Context, r *v2.Resource) (bool, error) {
	ctx, span := tracer.Start(ctx, "syncer.shouldSkipEntitlements")
	defer span.End()

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

	if skip, ok := s.skipEntitlementsForResourceType[r.GetId().GetResourceType()]; ok {
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
	s.skipEntitlementsForResourceType[r.GetId().GetResourceType()] = skipEntitlements

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
			shouldSkipEntitlements, err := s.shouldSkipEntitlements(ctx, r)
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

func (s *syncer) SyncStaticEntitlements(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.SyncStaticEntitlements")
	defer span.End()

	if s.state.ResourceTypeID(ctx) != "" {
		return s.syncStaticEntitlementsForResourceType(ctx, s.state.ResourceTypeID(ctx))
	}

	ctxzap.Extract(ctx).Info("Syncing static entitlements...")
	s.handleInitialActionForStep(ctx, *s.state.Current())

	s.state.FinishAction(ctx)
	for rts, err := range s.listAllResourceTypes(ctx) {
		if err != nil {
			return err
		}
		for _, rt := range rts {
			// Queue up actions to sync static entitlements for each resource type
			s.state.PushAction(ctx, Action{Op: SyncStaticEntitlementsOp, ResourceTypeID: rt.GetId()})
		}
	}

	return nil
}

func (s *syncer) syncStaticEntitlementsForResourceType(ctx context.Context, resourceTypeID string) error {
	ctx, span := tracer.Start(ctx, "syncer.syncStaticEntitlementsForResource")
	defer span.End()

	resp, err := s.connector.ListStaticEntitlements(ctx, v2.EntitlementsServiceListStaticEntitlementsRequest_builder{
		ResourceTypeId: resourceTypeID,
		PageToken:      s.state.PageToken(ctx),
		ActiveSyncId:   s.getActiveSyncID(),
	}.Build())
	if err != nil {
		// Ignore prefixError if we're calling a lambda with an old version of baton-sdk.
		if strings.Contains(err.Error(), `unable to resolve \"type.googleapis.com/c1.connector.v2.EntitlementsServiceListStaticEntitlementsRequest\": \"not found\"","errorType":"prefixError"`) {
			l := ctxzap.Extract(ctx)
			l.Info("ignoring prefixError when calling ListStaticEntitlements", zap.Error(err))
			s.state.FinishAction(ctx)
			return nil
		}

		return err
	}

	for _, ent := range resp.GetList() {
		resourcePageToken := ""
		for {
			// Get all resources of resource type and create entitlements for each one.
			resourcesResp, err := s.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
				ResourceTypeId: resourceTypeID,
				PageToken:      resourcePageToken,
				ActiveSyncId:   s.getActiveSyncID(),
			}.Build())
			if err != nil {
				return err
			}
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

				entitlements = append(entitlements, &v2.Entitlement{
					Resource:    resource,
					Id:          entitlement.NewEntitlementID(resource, ent.GetSlug()),
					DisplayName: displayName,
					Description: description,
					GrantableTo: ent.GetGrantableTo(),
					Annotations: ent.GetAnnotations(),
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

	s.handleProgress(ctx, s.state.Current(), len(resp.GetList()))

	if resp.GetNextPageToken() != "" {
		err = s.state.NextPage(ctx, resp.GetNextPageToken())
		if err != nil {
			return err
		}
	} else {
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

// SyncGrantExpansion handles the grant expansion phase of sync.
// It first loads the entitlement graph from grants, fixes any cycles, then runs expansion.
func (s *syncer) SyncGrantExpansion(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.SyncGrantExpansion")
	defer span.End()

	entitlementGraph := s.state.EntitlementGraph(ctx)

	// Phase 1: Load the entitlement graph from grants (paginated)
	if !entitlementGraph.Loaded {
		err := s.loadEntitlementGraph(ctx, entitlementGraph)
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
	err := s.expandGrantsForEntitlements(ctx)
	if err != nil {
		return err
	}

	return nil
}

// loadEntitlementGraph loads one page of grants and adds expandable relationships to the graph.
// This method handles pagination via the syncer's state machine.
func (s *syncer) loadEntitlementGraph(ctx context.Context, graph *expand.EntitlementGraph) error {
	l := ctxzap.Extract(ctx)
	pageToken := s.state.PageToken(ctx)

	if pageToken == "" {
		l.Info("Expanding grants...")
		s.handleInitialActionForStep(ctx, *s.state.Current())
	}

	resp, err := s.store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
	if err != nil {
		return err
	}

	// Handle pagination
	if resp.GetNextPageToken() != "" {
		err = s.state.NextPage(ctx, resp.GetNextPageToken())
		if err != nil {
			return err
		}
	} else {
		l.Debug("Finished loading grants to expand")
		graph.Loaded = true
	}

	// Process grants and add edges to the graph
	updatedGrants := make([]*v2.Grant, 0)
	for _, grant := range resp.GetList() {
		err := s.processGrantForGraph(ctx, grant, graph)
		if err != nil {
			return err
		}

		// Remove expandable annotation from descendant grant now that we've added it to the graph.
		// That way if this sync is part of a compaction, expanding grants at the end of compaction won't redo work.
		newAnnos := make(annotations.Annotations, 0)
		updated := false
		for _, anno := range grant.GetAnnotations() {
			if anno.MessageIs(&v2.GrantExpandable{}) {
				updated = true
			} else {
				newAnnos = append(newAnnos, anno)
			}
		}
		if !updated {
			continue
		}

		grant.SetAnnotations(newAnnos)
		l.Debug("removed expandable annotation from grant", zap.String("grant_id", grant.GetId()))
		updatedGrants = append(updatedGrants, grant)
		updatedGrants, err = expand.PutGrantsInChunks(ctx, s.store, updatedGrants, 10000)
		if err != nil {
			return err
		}
	}

	_, err = expand.PutGrantsInChunks(ctx, s.store, updatedGrants, 0)
	if err != nil {
		return err
	}

	if graph.Loaded {
		l.Info("Finished loading entitlement graph", zap.Int("edges", len(graph.Edges)))
	}
	return nil
}

// processGrantForGraph examines a grant for expandable annotations and adds edges to the graph.
func (s *syncer) processGrantForGraph(ctx context.Context, grant *v2.Grant, graph *expand.EntitlementGraph) error {
	l := ctxzap.Extract(ctx)

	annos := annotations.Annotations(grant.GetAnnotations())
	expandable := &v2.GrantExpandable{}
	_, err := annos.Pick(expandable)
	if err != nil {
		return err
	}
	if len(expandable.GetEntitlementIds()) == 0 {
		return nil
	}

	principalID := grant.GetPrincipal().GetId()
	if principalID == nil {
		return fmt.Errorf("principal id was nil")
	}

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

		graph.AddEntitlement(grant.GetEntitlement())
		graph.AddEntitlement(srcEntitlement.GetEntitlement())
		err = graph.AddEdge(ctx,
			srcEntitlement.GetEntitlement().GetId(),
			grant.GetEntitlement().GetId(),
			expandable.GetShallow(),
			expandable.GetResourceTypeIds(),
		)
		if err != nil {
			return fmt.Errorf("error adding edge to graph: %w", err)
		}
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

	previousSyncID, err := s.getPreviousFullSyncID(ctx)
	if err != nil {
		return "", nil, err
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
	// TODO(kans): do the request again to get the grants, but this time don't use the etag match!
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
	resourcesToInsertMap := make(map[string]*v2.Resource, 0)
	respAnnos := annotations.Annotations(resp.GetAnnotations())
	insertResourceGrants := respAnnos.Contains(&v2.InsertResourceGrants{})

	for _, grant := range grants {
		grantAnnos := annotations.Annotations(grant.GetAnnotations())
		if !s.dontExpandGrants && grantAnnos.Contains(&v2.GrantExpandable{}) {
			s.state.SetNeedsExpansion()
		}
		if grantAnnos.ContainsAny(&v2.ExternalResourceMatchAll{}, &v2.ExternalResourceMatch{}, &v2.ExternalResourceMatchID{}) {
			s.state.SetHasExternalResourcesGrants()
		}

		if insertResourceGrants {
			resource := grant.GetEntitlement().GetResource()
			bid, err := bid.MakeBid(resource)
			if err != nil {
				return err
			}
			resourcesToInsertMap[bid] = resource
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

	if len(resourcesToInsertMap) > 0 {
		resourcesToInsert := make([]*v2.Resource, 0)
		for _, resource := range resourcesToInsertMap {
			resourcesToInsert = append(resourcesToInsert, resource)
		}
		err = s.store.PutResources(ctx, resourcesToInsert...)
		if err != nil {
			return err
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

func (s *syncer) listAllGrants(ctx context.Context) iter.Seq2[[]*v2.Grant, error] {
	return func(yield func([]*v2.Grant, error) bool) {
		pageToken := ""
		for {
			grantsResp, err := s.store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
				PageToken: pageToken,
			}.Build())
			if err != nil {
				_ = yield(nil, err)
				return
			}

			if len(grantsResp.GetList()) > 0 {
				if !yield(grantsResp.GetList(), err) {
					return
				}
			}
			pageToken = grantsResp.GetNextPageToken()
			if pageToken == "" {
				return
			}
		}
	}
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

	for grants, err := range s.listAllGrants(ctx) {
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
	}

	newGrantIDs := mapset.NewSet[string]()
	for _, ng := range expandedGrants {
		newGrantIDs.Add(ng.GetId())
	}

	err := s.store.PutGrants(ctx, expandedGrants...)
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

// expandGrantsForEntitlements expands grants for the given entitlement.
// This method delegates to the expand.Expander for the actual expansion logic.
func (s *syncer) expandGrantsForEntitlements(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "syncer.expandGrantsForEntitlements")
	defer span.End()

	l := ctxzap.Extract(ctx)
	graph := s.state.EntitlementGraph(ctx)

	s.counts.LogExpandProgress(ctx, graph.Actions)

	// Create an expander and run a single step
	expander := expand.NewExpander(s.store, graph)
	err := expander.RunSingleStep(ctx)
	if err != nil {
		l.Error("expandGrantsForEntitlements: error during expansion", zap.Error(err))
		// If max depth exceeded, finish the action before returning the error
		// to prevent the state machine from getting stuck
		if errors.Is(err, expand.ErrMaxDepthExceeded) {
			s.state.FinishAction(ctx)
		}
		return err
	}

	if expander.IsDone(ctx) {
		l.Debug("expandGrantsForEntitlements: graph is expanded")
		s.state.FinishAction(ctx)
	}

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

	if s.setSessionStore != nil {
		s.setSessionStore.SetSessionStore(ctx, store)
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
		err = s.store.Close(ctx)
		if err != nil {
			return fmt.Errorf("error closing store: %w", err)
		}
	}

	if s.externalResourceReader != nil {
		err = s.externalResourceReader.Close(ctx)
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

func WithSyncResourceTypes(resourceTypeIDs []string) SyncOpt {
	return func(s *syncer) {
		s.syncResourceTypes = resourceTypeIDs
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

func WithSkipGrants(skip bool) SyncOpt {
	return func(s *syncer) {
		s.skipGrants = skip
	}
}

// NewSyncer returns a new syncer object.
func NewSyncer(ctx context.Context, c types.ConnectorClient, opts ...SyncOpt) (Syncer, error) {
	s := &syncer{
		connector:                       c,
		skipEGForResourceType:           make(map[string]bool),
		skipEntitlementsForResourceType: make(map[string]bool),
		resourceTypeTraits:              make(map[string][]v2.ResourceType_Trait),
		counts:                          NewProgressCounts(),
		syncType:                        connectorstore.SyncTypeFull,
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
