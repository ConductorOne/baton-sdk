package sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var (
	ErrSyncNotComplete = fmt.Errorf("sync exited without finishing")
)

type Syncer interface {
	Sync(ctx context.Context) error
	Close() error
}

// syncer orchestrates a connector sync and stores the results using the provided datasource.Writer.
type syncer struct {
	store             connectorstore.Writer
	connector         types.ClientWrapper
	state             State
	runDuration       time.Duration
	transitionHandler func(s Action)
}

// Checkpoint marshals the current state and stores it.
func (s *syncer) Checkpoint(ctx context.Context) error {
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

// Sync starts the syncing process. The sync process is driven by the action stack that is part of the state object.
// For each page of data that is required to be fetched from the connector, a new action is pushed on to the stack. Once
// an action is completed, it is popped off of the queue. Before procesing each action, we checkpoint the state object
// into the datasouce. This allows for graceful resumes if a sync is interrupted.
func (s *syncer) Sync(ctx context.Context) error {
	l := ctxzap.Extract(ctx)

	runCtx := ctx
	var runCanc context.CancelFunc
	if s.runDuration > 0 {
		runCtx, runCanc = context.WithTimeout(ctx, s.runDuration)
	}
	if runCanc != nil {
		defer runCanc()
	}

	c, err := s.connector.C(ctx)
	if err != nil {
		return err
	}

	_, err = c.Validate(ctx, &v2.ConnectorServiceValidateRequest{})
	if err != nil {
		return err
	}

	syncID, newSync, err := s.store.StartSync(ctx)
	if err != nil {
		return err
	}

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

	for s.state.Current() != nil {
		err = s.Checkpoint(ctx)
		if err != nil {
			return err
		}

		select {
		case <-runCtx.Done():
			l.Info("sync run duration has expired, exiting sync early", zap.String("sync_id", syncID))
			return ErrSyncNotComplete
		default:
		}

		stateAction := s.state.Current()

		switch stateAction.Op {
		case InitOp:
			s.state.FinishAction(ctx)
			// FIXME(jirwin): Disabling syncing assets for now
			// s.state.PushAction(ctx, Action{Op: SyncAssetsOp})
			s.state.PushAction(ctx, Action{Op: SyncGrantsOp})
			s.state.PushAction(ctx, Action{Op: SyncEntitlementsOp})
			s.state.PushAction(ctx, Action{Op: SyncResourcesOp})
			s.state.PushAction(ctx, Action{Op: SyncResourceTypesOp})

			err = s.Checkpoint(ctx)
			if err != nil {
				return err
			}
			continue

		case SyncResourceTypesOp:
			err = s.SyncResourceTypes(ctx)
			if err != nil {
				return err
			}
			continue

		case SyncResourcesOp:
			err = s.SyncResources(ctx)
			if err != nil {
				return err
			}
			continue

		case SyncEntitlementsOp:
			err = s.SyncEntitlements(ctx)
			if err != nil {
				return err
			}
			continue

		case SyncGrantsOp:
			err = s.SyncGrants(ctx)
			if err != nil {
				return err
			}
			continue

		case SyncAssetsOp:
			err = s.SyncAssets(ctx)
			if err != nil {
				return err
			}
			continue

		default:
			return fmt.Errorf("unexpected sync step")
		}
	}

	err = s.store.EndSync(ctx)
	if err != nil {
		return err
	}

	l.Info("Sync complete.")

	return nil
}

// SyncResourceTypes calls the ListResourceType() connector endpoint and persists the results in to the datasource.
func (s *syncer) SyncResourceTypes(ctx context.Context) error {
	pageToken := s.state.PageToken(ctx)

	if pageToken == "" {
		ctxzap.Extract(ctx).Info("Syncing resource types...")
		s.handleInitialActionForStep(ctx, *s.state.Current())
	}

	c, err := s.connector.C(ctx)
	if err != nil {
		return err
	}

	resp, err := c.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{PageToken: pageToken})
	if err != nil {
		return err
	}

	for _, rt := range resp.List {
		err = s.store.PutResourceType(ctx, rt)
		if err != nil {
			return err
		}
	}

	if resp.NextPageToken == "" {
		s.state.FinishAction(ctx)
		return nil
	}

	err = s.state.NextPage(ctx, resp.NextPageToken)
	if err != nil {
		return err
	}

	return nil
}

// subResource is used to track the specific resources that have been visited to avoid infinite loops.
type subResource struct {
	resourceTypeId   string
	parentResourceId *v2.ResourceId
}

// getSubResources fetches the sub resource types from a resources' annotations.
func (s *syncer) getSubResources(ctx context.Context, parent *v2.Resource) ([]subResource, error) {
	var subResources []subResource

	for _, a := range parent.Annotations {
		if a.MessageIs((*v2.ChildResourceType)(nil)) {
			crt := &v2.ChildResourceType{}
			err := a.UnmarshalTo(crt)
			if err != nil {
				return nil, err
			}

			subResources = append(subResources, subResource{
				parentResourceId: parent.Id,
				resourceTypeId:   crt.ResourceTypeId,
			})
		}
	}

	return subResources, nil
}

// SyncResources handles fetching all of the resources from the connector given the provided resource types. For each
// resource, we gather any child resource types it may emit, and traverse the resource tree. Currently this will checkpoint
// for each root resource type. Additional work to track the history across actions is required for more fine grained
// checkpointing.
func (s *syncer) SyncResources(ctx context.Context) error {
	if s.state.Current().ResourceTypeID == "" {
		ctxzap.Extract(ctx).Info("Syncing resources...")
		s.handleInitialActionForStep(ctx, *s.state.Current())

		pageToken := s.state.PageToken(ctx)

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

	visited := make(map[subResource]struct{})
	subResources := []subResource{{resourceTypeId: s.state.Current().ResourceTypeID}}

	for len(subResources) > 0 {
		// If the context is cancelled, bail from the loop
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		subR := subResources[0]
		subResources = subResources[1:]
		// If we've seen this subresource before, skip it
		if _, ok := visited[subR]; ok {
			continue
		}

		nested, err := s.syncResources(ctx, subR.resourceTypeId, subR.parentResourceId)
		if err != nil {
			return err
		}

		visited[subR] = struct{}{}
		subResources = append(subResources, nested...)
	}

	s.state.FinishAction(ctx)

	return nil
}

// syncResources fetches a given resource from the connector, and returns a slice of new child resources to fetch.
func (s *syncer) syncResources(ctx context.Context, resourceTypeID string, parentResourceID *v2.ResourceId) ([]subResource, error) {
	var ret []subResource

	pageToken := ""
	for {
		req := &v2.ResourcesServiceListResourcesRequest{
			ResourceTypeId:   resourceTypeID,
			ParentResourceId: parentResourceID,
			PageToken:        pageToken,
		}

		c, err := s.connector.C(ctx)
		if err != nil {
			return nil, err
		}

		resp, err := c.ListResources(ctx, req)
		if err != nil {
			return nil, err
		}

		for _, r := range resp.List {
			err = s.validateResourceTraits(ctx, r)
			if err != nil {
				return nil, err
			}

			err = s.store.PutResource(ctx, r)
			if err != nil {
				return nil, err
			}
			subResources, err := s.getSubResources(ctx, r)
			if err != nil {
				return nil, err
			}
			ret = append(ret, subResources...)
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	return ret, nil
}

func (s *syncer) validateResourceTraits(ctx context.Context, r *v2.Resource) error {
	rt, err := s.store.GetResourceType(ctx, &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
		ResourceTypeId: r.Id.ResourceType,
	})
	if err != nil {
		return err
	}

	for _, t := range rt.Traits {
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

// SyncEntitlements fetches the entitlements from the connector. It first lists each resource from the datastore,
// and pushes an action to fetch the entitelments for each resource.
func (s *syncer) SyncEntitlements(ctx context.Context) error {
	if s.state.ResourceTypeID(ctx) == "" && s.state.ResourceID(ctx) == "" {
		ctxzap.Extract(ctx).Info("Syncing entitlements...")
		s.handleInitialActionForStep(ctx, *s.state.Current())

		pageToken := s.state.PageToken(ctx)

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
	resource, err := s.store.GetResource(ctx, &reader_v2.ResourceTypesReaderServiceGetResourceRequest{
		ResourceId: resourceID,
	})
	if err != nil {
		return err
	}

	pageToken := s.state.PageToken(ctx)

	c, err := s.connector.C(ctx)
	if err != nil {
		return err
	}

	resp, err := c.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{Resource: resource, PageToken: pageToken})
	if err != nil {
		return err
	}
	for _, e := range resp.List {
		err = s.store.PutEntitlement(ctx, e)
		if err != nil {
			return err
		}
	}

	if resp.NextPageToken != "" {
		err = s.state.NextPage(ctx, resp.NextPageToken)
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
	l := ctxzap.Extract(ctx)
	resource, err := s.store.GetResource(ctx, &reader_v2.ResourceTypesReaderServiceGetResourceRequest{
		ResourceId: resourceID,
	})
	if err != nil {
		return err
	}

	var assetRefs []*v2.AssetRef

	c, err := s.connector.C(ctx)
	if err != nil {
		return err
	}

	rAnnos := annotations.Annotations(resource.Annotations)

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
		resp, err := c.GetAsset(ctx, &v2.AssetServiceGetAssetRequest{Asset: assetRef})
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
	if s.state.ResourceTypeID(ctx) == "" && s.state.ResourceID(ctx) == "" {
		ctxzap.Extract(ctx).Info("Syncing assets...")
		s.handleInitialActionForStep(ctx, *s.state.Current())

		pageToken := s.state.PageToken(ctx)

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

// SyncGrants fetches the grants for each resource from the connector. It iterates each resource
// from the datastore, and pushes a new action to sync the grants for each individual resource.
func (s *syncer) SyncGrants(ctx context.Context) error {
	if s.state.ResourceTypeID(ctx) == "" && s.state.ResourceID(ctx) == "" {
		ctxzap.Extract(ctx).Info("Syncing grants...")
		s.handleInitialActionForStep(ctx, *s.state.Current())

		pageToken := s.state.PageToken(ctx)

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

// syncGrantsForResource fetches the grants for a specific resource from the connector.
func (s *syncer) syncGrantsForResource(ctx context.Context, resourceID *v2.ResourceId) error {
	resource, err := s.store.GetResource(ctx, &reader_v2.ResourceTypesReaderServiceGetResourceRequest{
		ResourceId: resourceID,
	})
	if err != nil {
		return err
	}

	pageToken := s.state.PageToken(ctx)

	c, err := s.connector.C(ctx)
	if err != nil {
		return err
	}

	resp, err := c.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{Resource: resource, PageToken: pageToken})
	if err != nil {
		return err
	}
	for _, grant := range resp.List {
		err = s.store.PutGrant(ctx, grant)
		if err != nil {
			return err
		}
	}

	if resp.NextPageToken != "" {
		err = s.state.NextPage(ctx, resp.NextPageToken)
		if err != nil {
			return err
		}
	} else {
		s.state.FinishAction(ctx)
	}

	return nil
}

// Close closes the datastorage to ensure it is updated on disk.
func (s *syncer) Close() error {
	err := s.store.Close()
	if err != nil {
		return fmt.Errorf("error closing store: %w", err)
	}

	if s.connector != nil {
		err = s.connector.Close()
		if err != nil {
			return fmt.Errorf("error closing connector: %w", err)
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

// NewSyncer returns a new syncer object.
func NewSyncer(store connectorstore.Writer, c types.ClientWrapper, opts ...SyncOpt) Syncer {
	s := &syncer{
		store:     store,
		connector: c,
	}

	for _, o := range opts {
		o(s)
	}

	return s
}
