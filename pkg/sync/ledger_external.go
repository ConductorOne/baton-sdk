package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const ledgerExternalMatchCursor = "ledger-external-match"
const ledgerFactExternalPrincipals = "sync.external_principals"

type ledgerExternalPrincipal struct {
	ResourceTypeID string `json:"resource_type_id"`
	ResourceID     string `json:"resource_id"`
}

func (s *syncer) syncLedgerExternalResources(ctx context.Context, action *Action) error {
	invocation, ok := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
	if !ok {
		return errors.New("external resource ledger handler requires an open page")
	}
	start := time.Now()
	defer func() { invocation.page.row.PageDuration = time.Since(start) }()
	ctx, span := uotel.StartWithLink(ctx, tracer, "syncer.SyncExternalResources")
	uotel.SetSyncIdentityAttrs(ctx, span)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	switch action.PageToken {
	case "":
		if s.externalResourceReader == nil {
			return errors.New("external resource reader is not configured")
		}
		var principals []*v2.Resource
		if s.cfg.externalResourceEntitlementIdFilter != "" {
			principals, err = s.importLedgerExternalForEntitlement(ctx, invocation, s.cfg.externalResourceEntitlementIdFilter)
		} else {
			principals, err = s.importLedgerExternalUsersAndGroups(ctx, invocation)
		}
		if err != nil {
			return err
		}
		identities := make([]ledgerExternalPrincipal, 0, len(principals))
		for _, principal := range principals {
			identities = append(identities, ledgerExternalPrincipal{ResourceTypeID: principal.GetId().GetResourceType(), ResourceID: principal.GetId().GetResource()})
		}
		value, err := json.Marshal(identities)
		if err != nil {
			return err
		}
		if err := invocation.page.setFactValue(ledgerFactExternalPrincipals, string(value)); err != nil {
			return err
		}
		return s.nextPageOrFinishAction(ctx, action, ledgerExternalMatchCursor)
	case ledgerExternalMatchCursor:
		facts, err := s.ledger.store.LedgerFacts(ctx)
		if err != nil {
			return err
		}
		value, found := facts[ledgerFactExternalPrincipals]
		if !found {
			return errors.New("external matching has no committed principal identities")
		}
		var identities []ledgerExternalPrincipal
		if err := json.Unmarshal([]byte(value), &identities); err != nil {
			return fmt.Errorf("decode external principal identities: %w", err)
		}
		if identities == nil {
			return errors.New("external matching principal identities are null")
		}
		principals := make([]*v2.Resource, 0, len(identities))
		for _, identity := range identities {
			if identity.ResourceTypeID == "" || identity.ResourceID == "" {
				return errors.New("external matching principal has an incomplete identity")
			}
			response, err := s.store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
				ResourceId: v2.ResourceId_builder{ResourceType: identity.ResourceTypeID, Resource: identity.ResourceID}.Build(),
			}.Build())
			if err != nil {
				return err
			}
			principals = append(principals, response.GetResource())
		}
		if err := s.processLedgerGrantsWithExternalPrincipals(ctx, invocation, principals); err != nil {
			return err
		}
		return s.nextPageOrFinishAction(ctx, action, "")
	default:
		return errors.New("unknown external resource ledger cursor")
	}
}

func (s *syncer) importLedgerExternalForEntitlement(ctx context.Context, invocation *ledgerInvocation, entitlementId string) ([]*v2.Resource, error) {
	ctx, span := tracer.Start(ctx, "syncer.SyncExternalResourcesWithGrantToEntitlement")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)
	var importedGrants []*v2.Grant
	l.Info("Syncing external baton resources with grants to entitlement...")

	skipEGForResourceType := make(map[string]bool)

	filterEntitlement, err := s.externalResourceReader.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: entitlementId,
	}.Build())
	if err != nil {
		return nil, err
	}

	resourceTypeIDs := mapset.NewSet[string]()
	resourceIDs := make(map[string]*v2.ResourceId)

	for grants, err := range s.listExternalGrantsForEntitlement(ctx, filterEntitlement.GetEntitlement()) {
		if err != nil {
			return nil, err
		}
		for _, g := range grants {
			resourceTypeIDs.Add(g.GetPrincipal().GetId().GetResourceType())
			resourceIDs[g.GetPrincipal().GetId().GetResource()] = g.GetPrincipal().GetId()
		}
	}

	matchTraits := s.externalMatchTraits()
	resourceTypes := make([]*v2.ResourceType, 0)
	for _, resourceTypeId := range resourceTypeIDs.ToSlice() {
		resourceTypeResp, err := s.externalResourceReader.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{ResourceTypeId: resourceTypeId}.Build())
		if err != nil {
			return nil, err
		}
		for _, t := range resourceTypeResp.GetResourceType().GetTraits() {
			if matchTraits[t] {
				resourceTypes = append(resourceTypes, resourceTypeResp.GetResourceType())
				break
			}
		}

		rtAnnos := annotations.Annotations(resourceTypeResp.GetResourceType().GetAnnotations())
		skipEntitlements := rtAnnos.Contains(&v2.SkipEntitlementsAndGrants{})
		skipEGForResourceType[resourceTypeResp.GetResourceType().GetId()] = skipEntitlements
	}

	err = invocation.page.writer.PutResourceTypes(ctx, resourceTypes...)
	if err != nil {
		return nil, err
	}

	principals := make([]*v2.Resource, 0)
	for _, resourceKey := range slices.Sorted(maps.Keys(resourceIDs)) {
		resourceId := resourceIDs[resourceKey]
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
			return nil, err
		}
		resourceVal := resourceResp.GetResource()
		resourceAnnos := annotations.Annotations(resourceVal.GetAnnotations())
		batonID := &v2.BatonID{}
		resourceAnnos.Update(batonID)
		resourceVal.SetAnnotations(resourceAnnos)
		principals = append(principals, resourceVal)
	}

	stale, err := s.collectLedgerStaleExternalPrincipals(ctx, principals)
	if err != nil {
		return nil, err
	}
	err = invocation.page.writer.PutResources(ctx, principals...)
	if err != nil {
		return nil, err
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
			return nil, err
		}
		ents = append(ents, resourceEnts...)
		entsCount += len(resourceEnts)
	}

	err = invocation.page.writer.PutEntitlements(ctx, ents...)
	if err != nil {
		return nil, err
	}

	grantsForEntsCount := 0
	for _, ent := range ents {
		rAnnos := annotations.Annotations(ent.GetResource().GetAnnotations())
		if rAnnos.Contains(&v2.SkipGrants{}) {
			continue
		}
		for grants, err := range s.listExternalGrantsForEntitlement(ctx, ent) {
			if err != nil {
				return nil, err
			}
			grantsForEntsCount += len(grants)
			importedGrants = append(importedGrants, grants...)
			err = invocation.page.writer.PutGrants(ctx, grants...)
			if err != nil {
				return nil, err
			}
		}
	}

	l.Info("Synced external resources for entitlement",
		zap.Int("resource_type_count", len(resourceTypes)),
		zap.Int("resource_count", len(principals)),
		zap.Int("entitlement_count", entsCount),
		zap.Int("grant_count", grantsForEntsCount),
	)

	err = s.stageLedgerStaleExternal(ctx, invocation, stale, ents, importedGrants)
	if err != nil {
		return nil, err
	}

	return principals, nil
}

func (s *syncer) importLedgerExternalUsersAndGroups(ctx context.Context, invocation *ledgerInvocation) ([]*v2.Resource, error) {
	ctx, span := tracer.Start(ctx, "syncer.SyncExternalResourcesUsersAndGroups")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)
	var importedGrants []*v2.Grant
	l.Info("Syncing external resources for users and groups...")

	skipEGForResourceType := make(map[string]bool)

	resourceTypes, err := s.listExternalResourceTypes(ctx)
	if err != nil {
		return nil, err
	}

	matchTraits := s.externalMatchTraits()
	matchableResourceTypes := make([]*v2.ResourceType, 0)
	ents := make([]*v2.Entitlement, 0)
	principals := make([]*v2.Resource, 0)
	for _, rt := range resourceTypes {
		for _, t := range rt.GetTraits() {
			if matchTraits[t] {
				matchableResourceTypes = append(matchableResourceTypes, rt)
				break
			}
		}
	}

	err = invocation.page.writer.PutResourceTypes(ctx, matchableResourceTypes...)
	if err != nil {
		return nil, err
	}

	for _, rt := range matchableResourceTypes {
		rtAnnos := annotations.Annotations(rt.GetAnnotations())
		skipEntitlements := rtAnnos.Contains(&v2.SkipEntitlementsAndGrants{})
		skipEGForResourceType[rt.GetId()] = skipEntitlements

		resourceListResp, err := s.listExternalResourcesForResourceType(ctx, rt.GetId())
		if err != nil {
			return nil, err
		}

		for _, resourceVal := range resourceListResp {
			resourceAnnos := annotations.Annotations(resourceVal.GetAnnotations())
			batonID := &v2.BatonID{}
			resourceAnnos.Update(batonID)
			resourceVal.SetAnnotations(resourceAnnos)
			principals = append(principals, resourceVal)
		}
	}

	stale, err := s.collectLedgerStaleExternalPrincipals(ctx, principals)
	if err != nil {
		return nil, err
	}
	err = invocation.page.writer.PutResources(ctx, principals...)
	if err != nil {
		return nil, err
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
			return nil, err
		}
		ents = append(ents, resourceEnts...)
		entsCount += len(resourceEnts)
		err = invocation.page.writer.PutEntitlements(ctx, resourceEnts...)
		if err != nil {
			return nil, err
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
				return nil, err
			}
			grantsForEntsCount += len(grants)
			importedGrants = append(importedGrants, grants...)
			err = invocation.page.writer.PutGrants(ctx, grants...)
			if err != nil {
				return nil, err
			}
		}
	}

	l.Info("Synced external resources",
		zap.Int("resource_type_count", len(matchableResourceTypes)),
		zap.Int("resource_count", principalsCount),
		zap.Int("entitlement_count", entsCount),
		zap.Int("grant_count", grantsForEntsCount),
	)

	err = s.stageLedgerStaleExternal(ctx, invocation, stale, ents, importedGrants)
	if err != nil {
		return nil, err
	}

	return principals, nil
}
