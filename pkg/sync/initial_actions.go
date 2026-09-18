package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func (s *syncer) initialActions(targetedResources []*v2.Resource) ([]Action, []string) {
	var actions []Action
	var facts []string
	skipEntitlements := s.cfg.skipEntitlementsAndGrants || s.run.hasFact(factShouldSkipEntitlementsAndGrants)
	skipGrants := s.cfg.skipGrants || s.run.hasFact(factShouldSkipGrants)
	if s.cfg.skipEntitlementsAndGrants {
		facts = append(facts, factShouldSkipEntitlementsAndGrants)
	}
	if s.cfg.skipGrants {
		facts = append(facts, factShouldSkipGrants)
	}
	if len(targetedResources) > 0 {
		for _, r := range targetedResources {
			actions = append(actions, Action{Op: SyncTargetedResourceOp,
				ResourceID: r.GetId().GetResource(), ResourceTypeID: r.GetId().GetResourceType(),
				ParentResourceID: r.GetParentResourceId().GetResource(), ParentResourceTypeID: r.GetParentResourceId().GetResourceType()})
		}
		facts = append(facts, factShouldFetchRelatedResources)
		actions = append(actions, Action{Op: SyncResourceTypesOp})
		return actions, facts
	}
	if !skipEntitlements {
		actions = append(actions, Action{Op: SyncGrantExpansionOp})
	}
	if s.externalResourceReader != nil {
		actions = append(actions, Action{Op: SyncExternalResourcesOp})
	}
	if s.cfg.onlyExpandGrants {
		facts = append(facts, factNeedsExpansion)
		return actions, facts
	}
	if !skipEntitlements {
		if !skipGrants {
			actions = append(actions, Action{Op: SyncGrantsOp})
		}
		actions = append(actions, Action{Op: SyncEntitlementsOp}, Action{Op: SyncStaticEntitlementsOp})
	}
	actions = append(actions, Action{Op: SyncResourcesOp}, Action{Op: SyncResourceTypesOp})
	return actions, facts
}

func (s *syncer) initializeAction(ctx context.Context, action *Action, targetedResources []*v2.Resource) error {
	children, facts := s.initialActions(targetedResources)
	if s.ledgered {
		invocation, ok := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
		if !ok {
			return errors.New("ledger Init requires an open page")
		}
		for _, fact := range facts {
			if err := invocation.page.setFact(fact); err != nil {
				return err
			}
		}
		return s.nextPageOrFinishAction(ctx, action, "", children...)
	}
	s.finishAction(ctx, action)
	for _, fact := range facts {
		s.run.setFact(fact)
	}
	for _, child := range children {
		s.run.pushAction(ctx, child)
	}
	return s.Checkpoint(ctx, true)
}
