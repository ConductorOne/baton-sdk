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
)

func (s *syncer) syncLedgerEntitlements(ctx context.Context, page *ledgerPage, action *Action) error {
	if page == nil {
		return errors.New("entitlement ledger handler requires an open page")
	}
	start := time.Now()
	defer page.recordDuration(start)

	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncEntitlements")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if action.ResourceTypeID == "" && action.ResourceID == "" {
		var actions []Action
		var nextPageToken string
		var plannedTypeScoped bool
		actions, nextPageToken, plannedTypeScoped, err = s.planRootEntitlementActions(ctx, action)
		if err != nil {
			return err
		}
		if nextPageErr := page.stage(action, nextPageToken, actions); nextPageErr != nil {
			err = nextPageErr
			return err
		}
		if plannedTypeScoped {
			page.markTypeScopedPlanned()
		}
		return nil
	}

	err = s.collectLedgerEntitlements(ctx, page, action)
	if err != nil {
		return err
	}

	return nil
}

func (s *syncer) collectLedgerEntitlements(ctx context.Context, page *ledgerPage, action *Action) error {
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
	s.recordLedgerConnectorResponse(ctx, page, "list-entitlements", time.Since(start), resp.GetAnnotations())
	recordLedgerConnectorError(page, err)
	s.recordLedgerSessionUsage(page, resp.GetAnnotations())
	if err != nil {
		return markActionListCall(err)
	}
	collection := ledgerCollection(page)
	recordLedgerList(collection, &collection.EntitlementsReceived, resp.GetList(), resp.GetNextPageToken())
	entitlements, err := s.filterLedgerEntitlements(ctx, page, resp.GetList())
	if err != nil {
		return fmt.Errorf("sync-entitlements: filtering disabled-type references: %w", err)
	}
	err = page.PutEntitlements(ctx, entitlements...)
	if err != nil {
		return err
	}

	progressAction := *action
	page.afterCommit = append(page.afterCommit, func() {
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
	return page.stage(action, resp.GetNextPageToken(), spawned)
}

func (s *syncer) filterLedgerEntitlements(ctx context.Context, page *ledgerPage, values []*v2.Entitlement) ([]*v2.Entitlement, error) {
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
	collection := ledgerCollection(page)
	collection.EntitlementsExcludedByType += dropped
	collection.EntitlementsExcludedInvalid += invalid
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
	page.afterCommit = append(page.afterCommit, func() {
		s.ingestFilterStats.invalidEntitlementsObserved.Add(invalid)
		s.ingestFilterStats.entitlementsDropped.Add(dropped)
		if dropped > 0 {
			s.ingestFilterStats.blockReplay(ingestQualityReasonEntitlementDropped)
		}
	})
	return kept, nil
}
