package dotc1z

import (
	"context"
)

// IngestInvariantStore is the optional store capability backing the
// syncer's post-collection ingestion invariants (pkg/sync
// ingest_invariants.go): the ordered-key scans the referential
// invariants (I3/I7/I8/I9) ride, plus the point probes their verdicts
// need. Pebble-only; the syncer degrades per-invariant when the store
// lacks it (SQLite artifacts are never scanned).
//
// This is deliberately the READ-ONLY half of the invariant surface:
// the default-mode drop mechanics (DeleteGrantsForPrincipal and
// friends) arrive with the dangling-reference drop policy, and the
// replay-specific facts (external-match existence bits, source-cache
// scope scans) arrive with source-cache replay.
type IngestInvariantStore interface {
	// ForEachDistinctGrantEntitlementResource visits each distinct
	// entitlement resource referenced by any grant: one seek per
	// distinct resource, never O(grants).
	ForEachDistinctGrantEntitlementResource(ctx context.Context, visit func(resourceTypeID, resourceID string) error) error

	// GrantsForEntResourceCarryInsertFact reports whether any grant
	// under the entitlement resource carries InsertResourceGrants.
	// Value-reading; reserved for dangling-reference probes.
	GrantsForEntResourceCarryInsertFact(ctx context.Context, resourceTypeID, resourceID string) (bool, error)

	// GrantsForEntitlementAllCarryInsertFact reports whether every grant
	// under the entitlement identity carries InsertResourceGrants — the
	// per-grant side of I8's InsertResourceGrants exemption.
	// Value-reading; reserved for dangling-reference probes.
	GrantsForEntitlementAllCarryInsertFact(ctx context.Context, entitlementID, entResourceTypeID, entResourceID string) (bool, error)

	// HasResourceRecord reports whether a resource row exists.
	HasResourceRecord(ctx context.Context, resourceTypeID, resourceID string) (bool, error)

	// ForEachDistinctEntitlementResource visits each distinct resource
	// referenced by any entitlement row: one seek per distinct
	// resource, never O(entitlements).
	ForEachDistinctEntitlementResource(ctx context.Context, visit func(resourceTypeID, resourceID string) error) error

	// ForEachDanglingGrantEntitlement visits each distinct entitlement
	// referenced by grants that has no entitlement row: one seek plus
	// one point probe per distinct entitlement, never O(grants).
	ForEachDanglingGrantEntitlement(ctx context.Context, visit func(entitlementID, resourceTypeID, resourceID string) error) error

	// EnsureGrantIndexes forces a pending deferred grant-index build to
	// run now, making by_principal complete for the dangling-principal
	// scan. Near-free: the same build would run at EndSync.
	EnsureGrantIndexes(ctx context.Context) error

	// ForEachDanglingGrantPrincipal visits each distinct principal
	// referenced by grants that has no resource row. matchAnnotatedOnly
	// reports that every grant of the principal is an unprocessed
	// ExternalResourceMatch* carrier (exempt shape); carrierGrants is
	// the per-grant count of that population.
	ForEachDanglingGrantPrincipal(ctx context.Context, visit func(principalRT, principalID string, matchAnnotatedOnly bool, carrierGrants int64) error) error
}

var _ IngestInvariantStore = (*pebbleStore)(nil)

func (s *pebbleStore) ForEachDistinctGrantEntitlementResource(ctx context.Context, visit func(resourceTypeID, resourceID string) error) error {
	return s.Engine.ForEachDistinctGrantEntitlementResource(ctx, visit)
}

func (s *pebbleStore) GrantsForEntResourceCarryInsertFact(ctx context.Context, resourceTypeID, resourceID string) (bool, error) {
	return s.Engine.GrantsForEntResourceCarryInsertFact(ctx, resourceTypeID, resourceID)
}

func (s *pebbleStore) GrantsForEntitlementAllCarryInsertFact(ctx context.Context, entitlementID, entResourceTypeID, entResourceID string) (bool, error) {
	return s.Engine.GrantsForEntitlementAllCarryInsertFact(ctx, entitlementID, entResourceTypeID, entResourceID)
}

func (s *pebbleStore) HasResourceRecord(ctx context.Context, resourceTypeID, resourceID string) (bool, error) {
	return s.Engine.HasResourceRecord(ctx, resourceTypeID, resourceID)
}

func (s *pebbleStore) ForEachDistinctEntitlementResource(ctx context.Context, visit func(resourceTypeID, resourceID string) error) error {
	return s.Engine.ForEachDistinctEntitlementResource(ctx, visit)
}

func (s *pebbleStore) ForEachDanglingGrantEntitlement(ctx context.Context, visit func(entitlementID, resourceTypeID, resourceID string) error) error {
	return s.Engine.ForEachDanglingGrantEntitlement(ctx, visit)
}

func (s *pebbleStore) EnsureGrantIndexes(ctx context.Context) error {
	return s.Engine.EnsureGrantIndexes(ctx)
}

func (s *pebbleStore) ForEachDanglingGrantPrincipal(ctx context.Context, visit func(principalRT, principalID string, matchAnnotatedOnly bool, carrierGrants int64) error) error {
	return s.Engine.ForEachDanglingGrantPrincipal(ctx, visit)
}
