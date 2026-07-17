package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Fresh connector-page filtering for references into resource types that were
// not scheduled for the current full sync. This stays in the sync engine:
// hosted connector calls are stateless, while the engine has already stored
// the selected resource-type set before entitlement and grant collection.

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

type ingestFilterStats struct {
	entitlementsDropped   atomic.Int64
	grantsDropped         atomic.Int64
	grantResourcesDropped atomic.Int64
	expansionTypesDropped atomic.Int64
	expansionsDropped     atomic.Int64
}

// scheduledResourceTypeExists reads the store's resource-type keyspace rather
// than the raw option list. That handles the default "all types" selection and
// resumes correctly. Definitive answers are cached across parallel workers.
func (s *syncer) scheduledResourceTypeExists(ctx context.Context, resourceTypeID string) (bool, error) {
	// Empty refs are malformed data, not evidence of a disabled type. Keep
	// them on the normal write path so existing validation reports them.
	if resourceTypeID == "" {
		return true, nil
	}
	if exists, ok := s.scheduledResourceTypes.Load(resourceTypeID); ok {
		return exists, nil
	}
	_, err := s.store.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	if err != nil && status.Code(err) != codes.NotFound {
		// A read failure must never be converted into a drop verdict.
		return false, fmt.Errorf("fresh-ingest resource-type probe for %q: %w", resourceTypeID, err)
	}
	exists := err == nil
	if !exists {
		// The external-resource phase runs after entitlement and grant
		// collection and copies user/group resource types (and their
		// resources) from the external c1z into this sync. A reference that
		// is dangling at collection time can therefore resolve by the time
		// uplift reads the store, so it must not be dropped.
		exists, err = s.externalResourceTypeWillBeCopied(ctx, resourceTypeID)
		if err != nil {
			return false, err
		}
	}
	s.scheduledResourceTypes.Store(resourceTypeID, exists)
	return exists, nil
}

// externalResourceTypeWillBeCopied reports whether the later external-resource
// phase will copy resourceTypeID into this sync: an external reader is
// configured and the type exists there carrying a user or group trait — the
// only kinds SyncExternalResourcesUsersAndGroups and
// SyncExternalResourcesWithGrantToEntitlement copy.
func (s *syncer) externalResourceTypeWillBeCopied(ctx context.Context, resourceTypeID string) (bool, error) {
	if s.externalResourceReader == nil {
		return false, nil
	}
	resp, err := s.externalResourceReader.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return false, nil
		}
		// A read failure must never be converted into a drop verdict.
		return false, fmt.Errorf("fresh-ingest external resource-type probe for %q: %w", resourceTypeID, err)
	}
	for _, trait := range resp.GetResourceType().GetTraits() {
		if trait == v2.ResourceType_TRAIT_USER || trait == v2.ResourceType_TRAIT_GROUP {
			return true, nil
		}
	}
	return false, nil
}

func (s *syncer) filterFreshEntitlements(
	ctx context.Context,
	entitlements []*v2.Entitlement,
) ([]*v2.Entitlement, error) {
	if s.syncType != connectorstore.SyncTypeFull || len(entitlements) == 0 {
		return entitlements, nil
	}
	out := make([]*v2.Entitlement, 0, len(entitlements))
	for _, entitlement := range entitlements {
		// A literal nil entry carries nothing to store or validate; engines
		// skip nil writes inconsistently (pebble silently, sqlite not at
		// all), so drop it here.
		if entitlement == nil {
			continue
		}
		// A missing resource ref is malformed-but-present data, not evidence
		// of a disabled type. Keep it on the normal write path so existing
		// validation reports it.
		if entitlement.GetResource().GetId() == nil {
			out = append(out, entitlement)
			continue
		}
		exists, err := s.scheduledResourceTypeExists(ctx, entitlement.GetResource().GetId().GetResourceType())
		if err != nil {
			return nil, err
		}
		if !exists {
			s.ingestFilterStats.entitlementsDropped.Add(1)
			continue
		}
		out = append(out, entitlement)
	}
	return out, nil
}

func hasExternalResourceMatch(annos annotations.Annotations) bool {
	return annos.ContainsAny(
		&v2.ExternalResourceMatchAll{},
		&v2.ExternalResourceMatch{},
		&v2.ExternalResourceMatchID{},
	)
}

// filterGrantExpansionTypes intersects GrantExpandable.ResourceTypeIds with
// scheduled types. If a non-empty filter becomes empty, the expansion
// annotation must be removed: an empty ResourceTypeIds list means unfiltered
// expansion and would incorrectly widen the connector's request.
//
// External-match carriers remain intact because their placeholder references
// are transformed by the later external-resource matching phase.
func (s *syncer) filterGrantExpansionTypes(
	ctx context.Context,
	grant *v2.Grant,
	annos annotations.Annotations,
) (*v2.Grant, error) {
	if grant == nil || hasExternalResourceMatch(annos) {
		return grant, nil
	}
	expandable := &v2.GrantExpandable{}
	hasExpandable, err := annos.Pick(expandable)
	if err != nil {
		return nil, fmt.Errorf("fresh-ingest: parsing GrantExpandable on grant %q: %w", grant.GetId(), err)
	}
	if !hasExpandable || len(expandable.GetResourceTypeIds()) == 0 {
		return grant, nil
	}

	filtered := make([]string, 0, len(expandable.GetResourceTypeIds()))
	for _, resourceTypeID := range expandable.GetResourceTypeIds() {
		exists, err := s.scheduledResourceTypeExists(ctx, resourceTypeID)
		if err != nil {
			return nil, err
		}
		if exists {
			filtered = append(filtered, resourceTypeID)
			continue
		}
		s.ingestFilterStats.expansionTypesDropped.Add(1)
	}
	if len(filtered) == len(expandable.GetResourceTypeIds()) {
		return grant, nil
	}

	rewritten := make(annotations.Annotations, 0, len(annos))
	for _, anno := range annos {
		if !anno.MessageIs(expandable) {
			rewritten = append(rewritten, anno)
		}
	}
	if len(filtered) > 0 {
		expandable.SetResourceTypeIds(filtered)
		encoded, err := anypb.New(expandable)
		if err != nil {
			return nil, fmt.Errorf("fresh-ingest: encoding filtered GrantExpandable on grant %q: %w", grant.GetId(), err)
		}
		rewritten = append(rewritten, encoded)
	} else {
		s.ingestFilterStats.expansionsDropped.Add(1)
	}
	filteredGrant := proto.Clone(grant).(*v2.Grant)
	filteredGrant.SetAnnotations(rewritten)
	return filteredGrant, nil
}

func (s *syncer) filterFreshGrants(ctx context.Context, grants []*v2.Grant) ([]*v2.Grant, error) {
	if s.syncType != connectorstore.SyncTypeFull || len(grants) == 0 {
		return grants, nil
	}
	out := make([]*v2.Grant, 0, len(grants))
	for _, grant := range grants {
		// A literal nil entry carries nothing to store or validate; drop it
		// rather than relying on engine-dependent nil handling at write time.
		if grant == nil {
			continue
		}
		// Missing refs are malformed-but-present data, not evidence of a
		// disabled type. Keep them on the normal write path so existing
		// validation reports them.
		if grant.GetEntitlement().GetResource().GetId() == nil || grant.GetPrincipal().GetId() == nil {
			out = append(out, grant)
			continue
		}
		annos := annotations.Annotations(grant.GetAnnotations())

		entitlementTypeExists, err := s.scheduledResourceTypeExists(
			ctx,
			grant.GetEntitlement().GetResource().GetId().GetResourceType(),
		)
		if err != nil {
			return nil, err
		}
		// No InsertResourceGrants exemption: uplift can never use a grant
		// whose entitlement resource type is absent from the sync. Without
		// the type row the resource fails uplift, so no AppResource,
		// AppEntitlement, or binding is ever created — inserting the
		// resource row alone does not resurrect the chain.
		if !entitlementTypeExists {
			s.ingestFilterStats.grantsDropped.Add(1)
			continue
		}

		principalTypeExists, err := s.scheduledResourceTypeExists(ctx, grant.GetPrincipal().GetId().GetResourceType())
		if err != nil {
			return nil, err
		}
		// External match annotations own placeholder principals. This does
		// not exempt the entitlement reference checked above.
		if !principalTypeExists && !hasExternalResourceMatch(annos) {
			s.ingestFilterStats.grantsDropped.Add(1)
			continue
		}

		grant, err = s.filterGrantExpansionTypes(ctx, grant, annos)
		if err != nil {
			return nil, err
		}
		out = append(out, grant)
	}
	return out, nil
}

// filterFreshGrantResource reports whether a grant-discovered resource
// (InsertResourceGrants) references a scheduled resource type. Uplift skips
// resources whose type is absent from the sync's resource types, so storing
// them is dead data even though the resource row itself could be written.
func (s *syncer) filterFreshGrantResource(ctx context.Context, resource *v2.Resource) (bool, error) {
	if s.syncType != connectorstore.SyncTypeFull || resource.GetId() == nil {
		return true, nil
	}
	exists, err := s.scheduledResourceTypeExists(ctx, resource.GetId().GetResourceType())
	if err != nil {
		return false, err
	}
	if !exists {
		s.ingestFilterStats.grantResourcesDropped.Add(1)
	}
	return exists, nil
}

func (s *syncer) logIngestFilterSummary(ctx context.Context) {
	entitlements := s.ingestFilterStats.entitlementsDropped.Load()
	grants := s.ingestFilterStats.grantsDropped.Load()
	grantResources := s.ingestFilterStats.grantResourcesDropped.Load()
	expansionTypes := s.ingestFilterStats.expansionTypesDropped.Load()
	expansions := s.ingestFilterStats.expansionsDropped.Load()
	if entitlements == 0 && grants == 0 && grantResources == 0 && expansionTypes == 0 && expansions == 0 {
		return
	}
	// One aggregate warning per sync: never log individual rows or refs.
	ctxzap.Extract(ctx).Warn("fresh ingest filtered references into resource types not scheduled for this sync",
		zap.Int64("entitlements_dropped", entitlements),
		zap.Int64("grants_dropped", grants),
		zap.Int64("grant_discovered_resources_dropped", grantResources),
		zap.Int64("expansion_resource_types_dropped", expansionTypes),
		zap.Int64("expansions_dropped", expansions),
	)
}
