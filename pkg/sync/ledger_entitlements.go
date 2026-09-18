package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

func (s *syncer) syncLedgerEntitlements(ctx context.Context, action *Action) error {
	invocation, ok := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
	if !ok {
		return errors.New("entitlement ledger handler requires an open page")
	}
	start := time.Now()
	defer func() { invocation.page.row.PageDuration = time.Since(start) }()

	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncEntitlements")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if action.ResourceTypeID == "" && action.ResourceID == "" {
		actions := make([]Action, 0)
		pageToken := action.PageToken
		plannedTypeScoped := false

		if pageToken == "" {
			ctxzap.Extract(ctx).Info("Syncing entitlements...")
			s.handleInitialActionForStep(ctx, *action)
		}

		if !action.TypeScopedPlanned {
			typeScoped, typeScopedErr := s.typeScopedEntitlementsResourceTypes(ctx)
			if typeScopedErr != nil {
				err = fmt.Errorf("sync-entitlements: error listing type-scoped resource types: %w", typeScopedErr)
				return err
			}
			for _, rtID := range typeScoped {
				actions = append(actions, Action{Op: SyncEntitlementsOp, ResourceTypeID: rtID, TypeScoped: true})
			}
			plannedTypeScoped = true
		}

		resp, listResourcesErr := s.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			PageToken:    pageToken,
			ActiveSyncId: s.getActiveSyncID(),
		}.Build())
		if listResourcesErr != nil {
			err = listResourcesErr
			return err
		}

		for _, r := range resp.GetList() {
			shouldSkipEntitlements, shouldSkipErr := s.shouldSkipEntitlements(ctx, r)
			if shouldSkipErr != nil {
				err = shouldSkipErr
				return err
			}
			if shouldSkipEntitlements {
				continue
			}
			typeScoped, typeScopedErr := s.resourceTypeHasTypeScopedEntitlements(ctx, r.GetId().GetResourceType())
			if typeScopedErr != nil {
				err = typeScopedErr
				return err
			}
			if typeScoped {
				continue
			}
			actions = append(actions, Action{Op: SyncEntitlementsOp, ResourceID: r.GetId().GetResource(), ResourceTypeID: r.GetId().GetResourceType()})
		}

		if nextPageErr := s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken(), actions...); nextPageErr != nil {
			err = nextPageErr
			return err
		}
		if plannedTypeScoped {
			invocation.page.row.TypeScopedPlanned = true
		}
		return nil
	}

	err = s.collectLedgerEntitlements(ctx, action)
	if err != nil {
		return err
	}

	return nil
}

func (s *syncer) collectLedgerEntitlements(ctx context.Context, action *Action) error {
	invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
	typeScoped := action.TypeScoped
	resourceID := v2.ResourceId_builder{
		ResourceType: action.ResourceTypeID,
		Resource:     action.ResourceID,
	}.Build()

	var resource *v2.Resource
	var reqAnnos annotations.Annotations
	if typeScoped {
		resource, reqAnnos = typeScopedRequestStub(action.ResourceTypeID, &v2.TypeScopedEntitlements{})
	} else {
		resourceResponse, err := s.store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
			ResourceId: resourceID,
		}.Build())
		if err != nil {
			return err
		}
		resource = resourceResponse.GetResource()
	}

	start := time.Now()
	resp, err := s.connector.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource:     resource,
		PageToken:    action.PageToken,
		ActiveSyncId: s.getActiveSyncID(),
		Annotations:  reqAnnos,
	}.Build())
	s.recordLedgerConnectorResponse(ctx, invocation, "list-entitlements", time.Since(start), resp.GetAnnotations())
	s.recordLedgerSessionUsage(invocation, resp.GetAnnotations())
	if err != nil {
		return err
	}
	entitlements, err := s.filterLedgerEntitlements(ctx, invocation, resp.GetList())
	if err != nil {
		return fmt.Errorf("sync-entitlements: filtering disabled-type references: %w", err)
	}
	err = invocation.page.writer.PutEntitlements(ctx, entitlements...)
	if err != nil {
		return err
	}

	progressAction := *action
	invocation.afterCommit = append(invocation.afterCommit, func() {
		s.handleProgress(ctx, &progressAction, len(entitlements))
		progressIncrement, countOnly := collectionProgressIncrement(&progressAction, len(entitlements), resp.GetNextPageToken() != "")
		if countOnly {
			s.counts.SetEntitlementsCountOnly(resourceID.GetResourceType())
		}
		if progressIncrement > 0 || countOnly {
			s.counts.AddEntitlementsProgress(resourceID.GetResourceType(), progressIncrement)
			s.counts.LogEntitlementsProgress(ctx, resourceID.GetResourceType())
		}
	})
	spawned, err := s.collectEnqueuedPageTokens(ctx, "sync-entitlements", SyncEntitlementsOp, action, annotations.Annotations(resp.GetAnnotations()))
	if err != nil {
		return err
	}
	return s.nextPageOrFinishAction(ctx, action, resp.GetNextPageToken(), spawned...)
}

func (s *syncer) filterLedgerEntitlements(ctx context.Context, invocation *ledgerInvocation, values []*v2.Entitlement) ([]*v2.Entitlement, error) {
	var kept []*v2.Entitlement
	var dropped, invalid uint64
	for _, value := range values {
		if s.cfg.syncType == connectorstore.SyncTypeFull && value.GetResource().GetId() != nil {
			exists, err := s.scheduledResourceTypeExists(ctx, value.GetResource().GetId().GetResourceType())
			if err != nil {
				return nil, err
			}
			if !exists {
				dropped++
				continue
			}
		}
		reason, err := validateConnectorEntitlement(value)
		if err != nil {
			return nil, err
		}
		if reason != "" {
			invalid++
			continue
		}
		kept = append(kept, value)
	}
	page := invocation.page
	if page.observations.Counters == nil {
		page.observations.Counters = make(map[string]uint64)
	}
	page.observations.Counters["ingest.entitlements_dropped"] += dropped
	page.observations.Counters["ingest.invalid_entitlements_observed"] += invalid
	if dropped > 0 {
		if err := page.setFact(ledgerFactIngestKnown); err != nil {
			return nil, err
		}
		if err := page.setFact(ledgerFactIngestBlocked); err != nil {
			return nil, err
		}
		page.observations.Flags |= ingestQualityReasonEntitlementDropped
	}
	invocation.afterCommit = append(invocation.afterCommit, func() {
		s.ingestFilterStats.invalidEntitlementsObserved.Add(invalid)
		s.ingestFilterStats.entitlementsDropped.Add(dropped)
		if dropped > 0 {
			s.ingestFilterStats.blockReplay(ingestQualityReasonEntitlementDropped)
		}
	})
	return kept, nil
}
