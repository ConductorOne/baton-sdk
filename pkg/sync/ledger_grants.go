package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

func (s *syncer) syncLedgerGrants(ctx context.Context, page *ledgerPage, action *Action) error {
	if page == nil {
		return errors.New("grant ledger handler requires an open page")
	}
	start := time.Now()
	defer page.recordDuration(start)

	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncGrants")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if action.ResourceTypeID == "" && action.ResourceID == "" {
		var actions []Action
		var nextPageToken string
		var plannedTypeScoped bool
		actions, nextPageToken, plannedTypeScoped, err = s.planRootGrantActions(ctx, action)
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
	err = s.collectLedgerGrants(ctx, page, action)
	if err != nil {
		return err
	}

	return nil
}

func (s *syncer) collectLedgerGrants(ctx context.Context, page *ledgerPage, action *Action) error {
	var filterStats ingestFilterStats
	filterStats.markKnown()
	typeScoped := action.TypeScoped
	resourceID := v2.ResourceId_builder{
		ResourceType: action.ResourceTypeID,
		Resource:     action.ResourceID,
	}.Build()

	var resource *v2.Resource
	var reqAnnos annotations.Annotations
	if typeScoped {
		resource, reqAnnos = typeScopedRequestStub(action.ResourceTypeID, &v2.TypeScopedGrants{})
	} else {
		resourceResponse, err := s.store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
			ResourceId: resourceID,
		}.Build())
		if err != nil {
			return fmt.Errorf("sync-grants-for-resource: error getting resource: %w", err)
		}
		resource = resourceResponse.GetResource()
	}

	start := time.Now()
	resp, err := s.connector.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource:     resource,
		PageToken:    action.PageToken,
		ActiveSyncId: s.getActiveSyncID(),
		Annotations:  reqAnnos,
	}.Build())
	s.recordLedgerConnectorResponse(ctx, page, "list-grants", time.Since(start), resp.GetAnnotations())
	recordLedgerConnectorError(page, err)
	s.recordLedgerSessionUsage(page, resp.GetAnnotations())
	if err != nil {
		return fmt.Errorf("sync-grants-for-resource: error listing grants: %w", markActionListCall(err))
	}

	collection := ledgerCollection(page)
	recordLedgerList(collection, &collection.GrantsReceived, resp.GetList(), resp.GetNextPageToken())
	grants := resp.GetList()

	l := ctxzap.Extract(ctx)
	resourcesToInsertMap := make(map[string]*v2.Resource, 0)
	respAnnos := annotations.Annotations(resp.GetAnnotations())
	insertResourceGrants := respAnnos.Contains(&v2.InsertResourceGrants{})

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

		var validInsertGrants []*v2.Grant
		for i, g := range grants {
			resource := g.GetEntitlement().GetResource()
			reason, err := validateConnectorResource(resource)
			if err != nil {
				return fmt.Errorf("sync-grants-for-resource: validating grant-discovered resource: %w", err)
			}
			if reason != "" {
				filterStats.invalidResourcesObserved.Add(1)
				if validInsertGrants == nil {
					validInsertGrants = make([]*v2.Grant, 0, len(grants)-1)
					validInsertGrants = append(validInsertGrants, grants[:i]...)
				}
				continue
			}
			ok, err := s.filterFreshGrantResourceWithStats(ctx, resource, &filterStats)
			if err != nil {
				return fmt.Errorf("sync-grants-for-resource: filtering grant-discovered resource: %w", err)
			}
			if !ok {
				if validInsertGrants != nil {
					validInsertGrants = append(validInsertGrants, g)
				}
				continue
			}
			bid, err := bid.MakeBid(resource)
			if err != nil {
				return err
			}
			resourcesToInsertMap[bid] = resource
			if validInsertGrants != nil {
				validInsertGrants = append(validInsertGrants, g)
			}
		}
		if validInsertGrants != nil {
			grants = validInsertGrants
		}
	}

	grants, err = s.filterFreshGrantsWithStats(ctx, grants, &filterStats)
	if err != nil {
		return fmt.Errorf("sync-grants-for-resource: filtering disabled-type references: %w", err)
	}

	if err := s.stageLedgerFilterStats(page, &filterStats); err != nil {
		return err
	}

	for _, grant := range grants {
		grantAnnos := annotations.Annotations(grant.GetAnnotations())
		if !s.cfg.dontExpandGrants && grantAnnos.Contains(&v2.GrantExpandable{}) {
			if err := page.setFact(factNeedsExpansion); err != nil {
				return err
			}
		}
		if grantAnnos.ContainsAny(&v2.ExternalResourceMatchAll{}, &v2.ExternalResourceMatch{}, &v2.ExternalResourceMatchID{}) {
			if err := page.setFact(factHasExternalResourceGrants); err != nil {
				return err
			}
		}

		if !s.run.hasFact(factShouldFetchRelatedResources) {
			continue
		}
		entitlementResource := grant.GetEntitlement().GetResource()
		_, err := page.GetResource(ctx, entitlementResource.GetId().GetResourceType(), entitlementResource.GetId().GetResource())
		if err != nil {
			if status.Code(err) != codes.NotFound {
				return err
			}

			erId := entitlementResource.GetId()
			prId := entitlementResource.GetParentResourceId()
			resource, err := s.getLedgerResourceFromConnector(ctx, page, erId, prId)
			if err != nil {
				l.Error("error fetching entitlement resource", zap.Error(err))
				return err
			}
			if resource == nil {
				continue
			}
			if err := s.putLedgerResources(ctx, page, resource); err != nil {
				return err
			}
		}
	}

	if len(resourcesToInsertMap) > 0 {
		resourcesToInsert := make([]*v2.Resource, 0)
		for _, resource := range resourcesToInsertMap {
			resourcesToInsert = append(resourcesToInsert, resource)
		}
		err = s.putLedgerResources(ctx, page, resourcesToInsert...)
		if err != nil {
			return fmt.Errorf("sync-grants-for-resource: error putting resources: %w", err)
		}
	}

	err = page.PutGrants(ctx, grants...)
	if err != nil {
		return fmt.Errorf("sync-grants-for-resource: error putting grants: %w", err)
	}

	progressAction := *action
	page.afterCommit = append(page.afterCommit, func() {
		s.handleProgress(ctx, &progressAction, len(grants))

		progressIncrement, countOnly := collectionProgressIncrement(&progressAction, len(grants), resp.GetNextPageToken() != "")
		if countOnly {
			s.counts.SetGrantsCountOnly(resourceID.GetResourceType())
		}
		if progressIncrement > 0 || countOnly {
			s.counts.AddGrantsProgress(resourceID.GetResourceType(), progressIncrement)
			s.counts.LogGrantsProgress(ctx, resourceID.GetResourceType())
		}
	})
	spawned, err := s.collectEnqueuedPageTokens(ctx, "sync-grants-for-resource", SyncGrantsOp, action, respAnnos)
	if err != nil {
		return err
	}
	return page.stage(action, resp.GetNextPageToken(), spawned)
}

func (s *syncer) putLedgerResources(ctx context.Context, page *ledgerPage, resources ...*v2.Resource) error {
	resources, err := s.filterLedgerResources(page, resources)
	if err != nil {
		return err
	}
	return page.PutResources(ctx, resources...)
}

func (s *syncer) stageLedgerFilterStats(page *ledgerPage, stats *ingestFilterStats) error {
	q := stats.snapshot()
	if q == nil {
		return nil
	}
	if q.SourceCacheReplayBlocked {
		if err := page.setFact(ledgerFactIngestKnown); err != nil {
			return err
		}
		if err := page.setFact(ledgerFactIngestBlocked); err != nil {
			return err
		}
	}
	collection := ledgerCollection(page)
	collection.GrantsExcludedByType += q.GrantsDropped
	collection.DerivedResourcesExcludedByType += q.GrantResourcesDropped
	page.addObservations(ledgerIngestCounters(q))
	page.afterCommit = append(page.afterCommit, func() {
		s.ingestFilterStats.entitlementsDropped.Add(q.EntitlementsDropped)
		s.ingestFilterStats.grantsDropped.Add(q.GrantsDropped)
		s.ingestFilterStats.grantResourcesDropped.Add(q.GrantResourcesDropped)
		s.ingestFilterStats.expansionTypesDropped.Add(q.ExpansionResourceTypesDropped)
		s.ingestFilterStats.expansionsDropped.Add(q.ExpansionsDropped)
		s.ingestFilterStats.invalidResourceTypesObserved.Add(q.InvalidResourceTypesObserved)
		s.ingestFilterStats.invalidResourcesObserved.Add(q.InvalidResourcesObserved)
		s.ingestFilterStats.invalidEntitlementsObserved.Add(q.InvalidEntitlementsObserved)
		if q.SourceCacheReplayBlocked {
			s.ingestFilterStats.blockReplay(q.ReasonFlags)
		}
	})
	return nil
}
