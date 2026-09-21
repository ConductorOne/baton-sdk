package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *syncer) syncLedgerResources(ctx context.Context, action *Action) error {
	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncResources")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	invocation, ok := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
	if !ok {
		return errors.New("resource ledger handler requires an open page")
	}
	start := time.Now()
	defer func() { invocation.page.row.PageDuration = time.Since(start) }()
	defer func() { uotel.EndSpanWithError(span, err) }()

	if action.ResourceTypeID == "" {
		if action.PageToken == "" {
			ctxzap.Extract(ctx).Info("Syncing resources...")
			s.handleInitialActionForStep(ctx, *action)
			invocation.afterCommit = append(invocation.afterCommit, func() { s.resourcesPhaseRanHere = true })
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
			if action.ParentResourceTypeID != "" && action.ParentResourceID != "" {
				newAction.ParentResourceID = action.ParentResourceID
				newAction.ParentResourceTypeID = action.ParentResourceTypeID
			}

			actions = append(actions, newAction)
		}

		return s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken(), actions...)
	}

	return s.collectLedgerResources(ctx, action)
}

func (s *syncer) collectLedgerResources(ctx context.Context, action *Action) error {
	invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
	var children []Action
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
	s.recordLedgerConnectorResponse(ctx, invocation, "list-resources", time.Since(start), resp.GetAnnotations())
	recordLedgerConnectorError(invocation, err)
	s.recordLedgerSessionUsage(invocation, resp.GetAnnotations())
	if err != nil {
		return err
	}

	collection := ledgerCollection(invocation)
	recordLedgerList(collection, &collection.ResourcesReceived, len(resp.GetList()), resp.GetNextPageToken())
	resources, err := s.filterLedgerResources(invocation, resp.GetList())
	if err != nil {
		return err
	}
	bulkPutResources := []*v2.Resource{}
	for _, r := range resources {
		validatedResource := false

		_, err = s.store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
			ResourceId: v2.ResourceId_builder{ResourceType: r.GetId().GetResourceType(), Resource: r.GetId().GetResource()}.Build(),
		}.Build())
		if err == nil {
			err = s.validateResourceTraits(ctx, r)
			if err != nil {
				return err
			}
			validatedResource = true

			if !s.hasChildResources(r) {
				bulkPutResources = append(bulkPutResources, r)
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

		bulkPutResources = append(bulkPutResources, r)

		childTypes, err := childResourceTypeIDs(r)
		if err != nil {
			return err
		}
		children = append(children, s.pendingChildResourceActions(childTypes, r.GetId().GetResourceType(), r.GetId().GetResource())...)
	}

	if len(bulkPutResources) > 0 {
		err = invocation.page.writer.PutResources(ctx, bulkPutResources...)
		if err != nil {
			return err
		}
	}

	progressAction := *action
	invocation.afterCommit = append(invocation.afterCommit, func() {
		s.handleProgress(ctx, &progressAction, len(resp.GetList()))
		s.counts.AddResources(action.ResourceTypeID, len(resp.GetList()))
		if resp.GetNextPageToken() == "" {
			s.counts.LogResourcesProgress(ctx, action.ResourceTypeID)
		}
	})
	invocation.resourceChildren = true
	return s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken(), children...)
}

func (s *syncer) filterLedgerResources(invocation *ledgerInvocation, values []*v2.Resource) ([]*v2.Resource, error) {
	var kept []*v2.Resource
	var invalid uint64
	for _, value := range values {
		reason, err := validateConnectorResource(value)
		if err != nil {
			return nil, err
		}
		if reason != "" {
			invalid++
			continue
		}
		kept = append(kept, value)
	}
	if invocation.page.observations.Counters == nil {
		invocation.page.observations.Counters = make(map[string]uint64)
	}
	ledgerCollection(invocation).ResourcesExcludedInvalid += invalid
	invocation.page.observations.Counters["ingest.invalid_resources_observed"] += invalid
	invocation.afterCommit = append(invocation.afterCommit, func() { s.ingestFilterStats.invalidResourcesObserved.Add(invalid) })
	return kept, nil
}
