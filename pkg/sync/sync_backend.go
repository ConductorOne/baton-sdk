package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	storage_v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// DB size is not part of syncBackend. The progress log asserts
// connectorstore.DBSizeProvider at its call.
type syncBackend interface {
	c1zstore.Store
	c1zstore.PageLedgerStore
	resourceRecordDeleter
	entitlementRecordDeleter
	grantByRefsDeleter
	grantsByRefsBatchDeleter
	expandedGrantLayerStorer
	newExpandedGrantStorer
	newExpandedGrantContributionStorer
	dotc1z.IngestInvariantStore
	c1zstore.IngestInvariantVerificationWriter
	c1zstore.GrantGenerationDigestReader
	EntitlementGraphStore
}

type resourceRecordDeleter interface {
	DeleteResourceRecord(ctx context.Context, resourceTypeID, resourceID string) error
}

type entitlementRecordDeleter interface {
	DeleteEntitlementByRefs(ctx context.Context, entitlement *v2.Entitlement) error
}

// grantByRefsDeleter deletes a grant by its structured refs instead of its
// lossy public id.
type grantByRefsDeleter interface {
	DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error
}

// grantsByRefsBatchDeleter deletes N grants in a bounded number of commits
// instead of one durable commit per grant.
type grantsByRefsBatchDeleter interface {
	DeleteGrantsByRefs(ctx context.Context, grants ...*v2.Grant) error
}

// newExpandedGrantStorer persists caller-proven-new expanded grants without
// read-before-write.
type newExpandedGrantStorer interface {
	StoreNewExpandedGrants(context.Context, ...*v2.Grant) error
}

// newExpandedGrantContributionStorer is the contribution-shaped form of
// newExpandedGrantStorer: destination plus principal refs, without
// materializing v2.Grants.
type newExpandedGrantContributionStorer interface {
	StoreNewExpandedGrantContributions(context.Context, *v2.Entitlement, []*storage_v3.PrincipalRef, []batonGrant.Sources) error
}

// expandedGrantLayerStorer is the layer-scoped synthesized-grant session.
type expandedGrantLayerStorer interface {
	BeginExpandedGrantLayer(ctx context.Context) (bool, error)
	AddExpandedGrantLayerContributions(ctx context.Context, dest *v2.Entitlement, principals []*storage_v3.PrincipalRef, sources []batonGrant.Sources) error
	FinishExpandedGrantLayer(ctx context.Context) error
	AbortExpandedGrantLayer(ctx context.Context) error
}
