package sync

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	c1zpb "github.com/conductorone/baton-sdk/pb/c1/c1z/v1"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	"github.com/conductorone/baton-sdk/pkg/types"
)

var (
	ErrSyncNotComplete = fmt.Errorf("sync exited without finishing")
)

type Syncer interface {
	Sync(ctx context.Context) error
	Close(ctx context.Context) error
}

// syncer orchestrates a connector sync and stores the results using the provided datasource.Writer.
type syncer struct {
	c1zManager        manager.Manager
	c1zPath           string
	store             connectorstore.Writer
	connector         types.ConnectorClient
	state             State
	runDuration       time.Duration
	transitionHandler func(s Action)
	progressHandler   func(p *Progress)

	skipEGForResourceType map[string]bool
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

func (s *syncer) handleProgress(ctx context.Context, a *Action, c int) {
	if s.progressHandler != nil {
		s.progressHandler(NewProgress(a, uint32(c)))
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
			err = context.Cause(runCtx)
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				l.Info("sync run duration has expired, exiting sync early", zap.String("sync_id", syncID))
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

	err = s.store.Cleanup(ctx)
	if err != nil {
		return err
	}

	return nil
}

// SyncResourceTypes calls the ListResourceType() connector endpoint and persists the results in to the datasource.
func (s *syncer) SyncResourceTypes(ctx context.Context) error {
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

	for _, rt := range resp.List {
		err = s.store.PutResourceType(ctx, rt)
		if err != nil {
			return err
		}
	}

	s.handleProgress(ctx, s.state.Current(), len(resp.List))

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

// getSubResources fetches the sub resource types from a resources' annotations.
func (s *syncer) getSubResources(ctx context.Context, parent *v2.Resource) error {
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

	return s.syncResources(ctx)
}

// syncResources fetches a given resource from the connector, and returns a slice of new child resources to fetch.
func (s *syncer) syncResources(ctx context.Context) error {
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

	if resp.NextPageToken == "" {
		s.state.FinishAction(ctx)
	} else {
		err = s.state.NextPage(ctx, resp.NextPageToken)
		if err != nil {
			return err
		}
	}

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

		err = s.store.PutResource(ctx, r)
		if err != nil {
			return err
		}

		err = s.getSubResources(ctx, r)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *syncer) validateResourceTraits(ctx context.Context, r *v2.Resource) error {
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
	for _, e := range resp.List {
		err = s.store.PutEntitlement(ctx, e)
		if err != nil {
			return err
		}
	}

	s.handleProgress(ctx, s.state.Current(), len(resp.List))

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

type lastestSyncFetcher interface {
	LatestFinishedSync(ctx context.Context) (string, error)
}

func (s *syncer) fetchResourceForPreviousSync(ctx context.Context, resourceID *v2.ResourceId) (string, *v2.ETag, error) {
	l := ctxzap.Extract(ctx)

	var previousSyncID string
	var err error

	if psf, ok := s.store.(lastestSyncFetcher); ok {
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
			PageSize:    1000,
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
		err = s.store.PutGrant(ctx, grant)
		if err != nil {
			return err
		}
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
		err = s.store.PutResource(ctx, resource)
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

	s.state.FinishAction(ctx)

	return nil
}

func (s *syncer) loadStore(ctx context.Context) error {
	if s.store != nil {
		return nil
	}

	if s.c1zManager == nil {
		m, err := manager.New(ctx, s.c1zPath)
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
	err := s.store.Close()
	if err != nil {
		return fmt.Errorf("error closing store: %w", err)
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

// NewSyncer returns a new syncer object.
func NewSyncer(ctx context.Context, c types.ConnectorClient, opts ...SyncOpt) (Syncer, error) {
	s := &syncer{
		connector:             c,
		skipEGForResourceType: make(map[string]bool),
	}

	for _, o := range opts {
		o(s)
	}

	if s.store == nil && s.c1zPath == "" {
		return nil, errors.New("a connector store writer or a db path must be provided")
	}

	return s, nil
}
