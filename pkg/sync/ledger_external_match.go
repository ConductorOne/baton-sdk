package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *syncer) processLedgerGrantsWithExternalPrincipals(ctx context.Context, invocation *ledgerInvocation, principals []*v2.Resource) error {
	ctx, span := tracer.Start(ctx, "processGrantsWithExternalPrincipals")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if !s.run.hasFact(factHasExternalResourceGrants) {
		return nil
	}

	l := ctxzap.Extract(ctx)

	matchTraits := s.externalMatchTraits()
	principalsByTrait := make(map[v2.ResourceType_Trait][]*v2.Resource, len(matchTraits))
	principalMap := make(map[string]*v2.Resource)

	for _, principal := range principals {
		rAnnos := annotations.Annotations(principal.GetAnnotations())
		batonID := &v2.BatonID{}
		if !rAnnos.Contains(batonID) {
			continue
		}
		for trait := range matchTraits {
			traitMsg := resourceTraitMessage(trait)
			if traitMsg != nil && rAnnos.Contains(traitMsg) {
				principalsByTrait[trait] = append(principalsByTrait[trait], principal)
			}
		}
		principalID := principal.GetId().GetResource()
		if principalID == "" {
			l.Error("principal resource id was empty")
			continue
		}
		principalMap[principalID] = principal
	}

	indexByTrait := make(map[v2.ResourceType_Trait]*externalPrincipalIndex, len(matchTraits))
	principalCounts := make(map[string]int, len(matchTraits))
	for trait := range matchTraits {
		traitPrincipals := principalsByTrait[trait]
		if trait == v2.ResourceType_TRAIT_USER {
			indexByTrait[trait] = newExternalUserPrincipalIndex(traitPrincipals, l)
		} else {
			indexByTrait[trait] = newExternalPrincipalIndex(traitPrincipals)
		}
		principalCounts[trait.String()] = len(traitPrincipals)
	}

	l.Info("matching grants against external principals",
		zap.Any("principals_by_trait", principalCounts),
	)

	grantsToDelete := make([]*v2.Grant, 0)
	expandedGrants := make([]*v2.Grant, 0)
	grantsScanned := 0

	for ga, err := range invocation.page.writer.ListGrantsWithAnnotations(ctx) {
		if err != nil {
			return err
		}

		grantsScanned++
		if grantsScanned%externalMatchProgressLogInterval == 0 {
			l.Debug("matching grants against external principals: progress",
				zap.Int("grants_scanned", grantsScanned),
				zap.Int("expanded_grants", len(expandedGrants)),
				zap.Int("grants_to_delete", len(grantsToDelete)),
			)
		}

		grant := ga.Grant
		annos := annotations.Annotations(grant.GetAnnotations())
		if !annos.ContainsAny(&v2.ExternalResourceMatchAll{}, &v2.ExternalResourceMatch{}, &v2.ExternalResourceMatchID{}) {
			continue
		}

		matchResourceMatchAllAnno, err := GetExternalResourceMatchAllAnnotation(annos)
		if err != nil {
			return err
		}
		if matchResourceMatchAllAnno != nil {
			trait := matchResourceMatchAllAnno.GetResourceType()
			if !matchTraits[trait] {
				l.Error("unexpected external resource type trait", zap.Any("trait", trait))
			}
			for _, principal := range principalsByTrait[trait] {
				newGrant := newGrantForExternalPrincipal(grant, principal)
				expandedGrants = append(expandedGrants, newGrant)
			}
			grantsToDelete = append(grantsToDelete, grant)
			continue
		}

		expandableAnno := ga.Annotation
		expandableEntitlementsResourceMap := make(map[string][]string)
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

				slugs, ok := expandableEntitlementsResourceMap[resourceBID]
				if !ok {
					slugs = make([]string, 0)
				}
				slugs = append(slugs, parsedEnt.GetSlug())
				expandableEntitlementsResourceMap[resourceBID] = slugs
			}
		}

		matchResourceMatchIDAnno, err := GetExternalResourceMatchIDAnnotation(annos)
		if err != nil {
			return err
		}
		if matchResourceMatchIDAnno != nil {
			if principal, ok := principalMap[matchResourceMatchIDAnno.GetId()]; ok {
				newGrant := newGrantForExternalPrincipal(grant, principal)

				newGrantAnnos := annotations.Annotations(newGrant.GetAnnotations())

				newExpandableEntitlementIDs := make([]string, 0)
				if expandableAnno != nil {
					groupPrincipalBID, err := bid.MakeBid(grant.GetPrincipal())
					if err != nil {
						l.Error("error making group principal bid", zap.Error(err), zap.Any("grant.Principal", grant.GetPrincipal()))
						continue
					}

					principalEntitlementSlugs := expandableEntitlementsResourceMap[groupPrincipalBID]
					for _, slug := range principalEntitlementSlugs {
						newExpandableEntId := entitlement.NewEntitlementID(principal, slug)
						_, err := invocation.page.writer.GetEntitlement(ctx, newExpandableEntId)
						if err != nil {
							if status.Code(err) == codes.NotFound {
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
				}
				expandedGrants = append(expandedGrants, newGrant)
			}

			grantsToDelete = append(grantsToDelete, grant)
		}

		matchExternalResource, err := GetExternalResourceMatchAnnotation(annos)
		if err != nil {
			return err
		}

		if matchExternalResource != nil {
			trait := matchExternalResource.GetResourceType()
			matchKey := matchExternalResource.GetKey()
			matchValue := matchExternalResource.GetValue()
			switch {
			case trait == v2.ResourceType_TRAIT_USER:
				idx := indexByTrait[v2.ResourceType_TRAIT_USER]
				if idx == nil {
					break
				}
				positions := idx.matchProfile(matchKey, matchValue)
				if matchKey == "email" {
					positions = mergePositions(idx.matchUserTraitEmail(matchValue), positions)
				}
				for _, i := range positions {
					newGrant := newGrantForExternalPrincipal(grant, idx.principalAt(i))
					expandedGrants = append(expandedGrants, newGrant)
				}
			case matchTraits[trait]:
				idx := indexByTrait[trait]
				for _, i := range idx.matchProfile(matchKey, matchValue) {
					newGrant, err := s.matchProfileAndExpand(
						ctx, l, grant, idx.principalAt(i),
						expandableAnno, expandableEntitlementsResourceMap,
					)
					if err != nil {
						return err
					}
					if newGrant != nil {
						expandedGrants = append(expandedGrants, newGrant)
					}
				}
			default:
				l.Error("unexpected external resource type trait", zap.Any("trait", trait))
			}

			grantsToDelete = append(grantsToDelete, grant)
		}
	}

	l.Debug("matched grants against external principals",
		zap.Int("grants_scanned", grantsScanned),
		zap.Int("expanded_grants", len(expandedGrants)),
		zap.Int("grants_to_delete", len(grantsToDelete)),
	)

	newGrantIDs := mapset.NewSet[string]()
	for _, ng := range expandedGrants {
		newGrantIDs.Add(ng.GetId())
	}

	err = invocation.page.writer.PutGrants(ctx, expandedGrants...)
	if err != nil {
		return err
	}

	pendingDeletes := make([]*v2.Grant, 0, len(grantsToDelete))
	for _, grantToDelete := range grantsToDelete {
		if newGrantIDs.ContainsOne(grantToDelete.GetId()) {
			continue
		}
		pendingDeletes = append(pendingDeletes, grantToDelete)
	}

	return invocation.page.writer.DeleteGrants(ctx, pendingDeletes...)
}
