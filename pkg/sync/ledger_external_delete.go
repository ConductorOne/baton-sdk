package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

type ledgerExternalStale struct {
	resources    []*v2.ResourceId
	entitlements []*v2.Entitlement
	grants       []*v2.Grant
}

func ledgerGrantIdentity(grant *v2.Grant) [6]string {
	ent := grant.GetEntitlement()
	return [6]string{grant.GetId(), ent.GetId(), ent.GetResource().GetId().GetResourceType(), ent.GetResource().GetId().GetResource(),
		grant.GetPrincipal().GetId().GetResourceType(), grant.GetPrincipal().GetId().GetResource()}
}

func ledgerEntitlementIdentity(ent *v2.Entitlement) [3]string {
	return [3]string{ent.GetId(), ent.GetResource().GetId().GetResourceType(), ent.GetResource().GetId().GetResource()}
}

func (s *syncer) stageLedgerStaleExternal(
	ctx context.Context, invocation *ledgerInvocation, stale *ledgerExternalStale, importedEnts []*v2.Entitlement, importedGrants []*v2.Grant,
) error {
	grantPuts := make(map[[6]string]bool, len(importedGrants))
	for _, grant := range importedGrants {
		grantPuts[ledgerGrantIdentity(grant)] = true
	}
	entPuts := make(map[[3]string]bool, len(importedEnts))
	for _, ent := range importedEnts {
		entPuts[ledgerEntitlementIdentity(ent)] = true
	}
	var grants []*v2.Grant
	for _, grant := range stale.grants {
		if !grantPuts[ledgerGrantIdentity(grant)] {
			grants = append(grants, grant)
		}
	}
	if err := invocation.page.writer.DeleteGrants(ctx, grants...); err != nil {
		return err
	}
	var entitlements []*v2.Entitlement
	for _, ent := range stale.entitlements {
		if !entPuts[ledgerEntitlementIdentity(ent)] {
			entitlements = append(entitlements, ent)
		}
	}
	if err := invocation.page.writer.DeleteEntitlements(ctx, entitlements...); err != nil {
		return err
	}
	resources := make([]*v2.Resource, 0, len(stale.resources))
	for _, id := range stale.resources {
		resources = append(resources, v2.Resource_builder{Id: id}.Build())
	}
	return invocation.page.writer.DeleteResources(ctx, resources...)
}

func (s *syncer) collectLedgerStaleExternalPrincipals(
	ctx context.Context,
	current []*v2.Resource,
) (*ledgerExternalStale, error) {
	currentIDs := make(map[string]struct{}, len(current))
	for _, principal := range current {
		id := principal.GetId()
		currentIDs[id.GetResourceType()+"\x00"+id.GetResource()] = struct{}{}
	}

	var staleIDs []*v2.ResourceId
	pageToken := ""
	for {
		response, err := s.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			PageToken: pageToken,
		}.Build())
		if err != nil {
			return nil, err
		}
		for _, candidate := range response.GetList() {
			candidateAnnos := annotations.Annotations(candidate.GetAnnotations())
			if !candidateAnnos.Contains(&v2.BatonID{}) {
				continue
			}
			id := candidate.GetId()
			if _, ok := currentIDs[id.GetResourceType()+"\x00"+id.GetResource()]; ok {
				continue
			}
			staleIDs = append(staleIDs, id)
		}
		pageToken = response.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	if len(staleIDs) == 0 {
		return &ledgerExternalStale{}, nil
	}

	staleKeys := make(map[string]struct{}, len(staleIDs))
	for _, id := range staleIDs {
		staleKeys[id.GetResourceType()+"\x00"+id.GetResource()] = struct{}{}
	}
	var staleGrants []*v2.Grant
	for grantWithAnnotations, err := range s.store.Grants().ListWithAnnotations(ctx) {
		if err != nil {
			return nil, err
		}
		grant := grantWithAnnotations.Grant
		principalID := grant.GetPrincipal().GetId()
		entitlementResourceID := grant.GetEntitlement().GetResource().GetId()
		_, stalePrincipal := staleKeys[principalID.GetResourceType()+"\x00"+principalID.GetResource()]
		_, staleEntitlement := staleKeys[entitlementResourceID.GetResourceType()+"\x00"+entitlementResourceID.GetResource()]
		if stalePrincipal || staleEntitlement {
			staleGrants = append(staleGrants, grant)
		}
	}

	var staleEntitlements []*v2.Entitlement
	pageToken = ""
	for {
		response, err := s.store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
			PageToken: pageToken,
		}.Build())
		if err != nil {
			return nil, err
		}
		for _, candidate := range response.GetList() {
			resourceID := candidate.GetResource().GetId()
			if _, stale := staleKeys[resourceID.GetResourceType()+"\x00"+resourceID.GetResource()]; !stale {
				continue
			}
			staleEntitlements = append(staleEntitlements, candidate)
		}
		pageToken = response.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}

	return &ledgerExternalStale{resources: staleIDs, entitlements: staleEntitlements, grants: staleGrants}, nil
}
