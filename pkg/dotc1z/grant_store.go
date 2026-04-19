package dotc1z

import (
	"context"
	"iter"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// GrantStore is the grant-specific slice of C1ZStore. Each method maps to a
// specific caller intent in the sync pipeline — no mode enums, no option
// structs with behavior switches.
type GrantStore interface {
	// StoreExpandedGrants writes grants produced by the expander back to
	// storage. The expander has already consumed each grant's
	// GrantExpandable annotation and performed expansion; storage must
	// persist the grant payload without re-extracting expansion metadata
	// (otherwise we'd corrupt expansion state that has already been
	// marked "done"). For robustness, any residual GrantExpandable
	// annotation on the input is stripped from the persisted payload.
	//
	// Called by pkg/sync/expand.PutGrantsInChunks.
	StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error

	// PendingExpansion yields one PendingExpansion per grant whose
	// expansion metadata still needs processing (needs_expansion=1).
	// The grant payload is NOT materialized — only identity plus the
	// parsed expansion annotation — because the caller only needs
	// expansion metadata and this keeps the hot path cheap.
	//
	// Pagination is handled internally; callers just range over the
	// sequence. Early termination (break) stops the underlying SQL
	// paging.
	//
	// ITERATION CONTRACT: on error, the sequence yields a final
	// (zero-value PendingExpansion, err) pair and terminates. Callers
	// MUST check the second return value on every iteration:
	//
	//     for pe, err := range store.Grants().PendingExpansion(ctx) {
	//         if err != nil { return err }
	//         // ... use pe ...
	//     }
	//
	// Single-variable ranging (for pe := range seq) silently drops
	// errors and is a bug.
	//
	// Called by pkg/sync.syncer.ExpandGrants.
	PendingExpansion(ctx context.Context) iter.Seq2[PendingExpansion, error]

	// ListWithAnnotations yields every grant in the current sync,
	// paired with its expansion annotation if any. Used by the
	// external-principal post-processing step which needs full grant
	// payloads plus expansion.
	//
	// ITERATION CONTRACT: same as PendingExpansion. On error, the
	// sequence yields (zero-value GrantAnnotation, err) then
	// terminates. Callers MUST check err on every iteration.
	//
	// Called by pkg/sync.syncer.listAllGrantsWithExpansion.
	ListWithAnnotations(ctx context.Context) iter.Seq2[GrantAnnotation, error]
}

// PendingExpansion is a lightweight row yielded by GrantStore.PendingExpansion.
// It carries grant identity and the parsed expansion annotation without
// materializing the full grant payload — this keeps the expansion-worker
// hot path out of proto.Unmarshal.
//
// The syncer uses GrantExternalID to look up the grant and uses Annotation
// to decide what expansion work to enqueue.
type PendingExpansion struct {
	// GrantExternalID is the external id of the grant this expansion
	// applies to (matches v2.Grant.Id).
	GrantExternalID string

	// TargetEntitlementID is the entitlement id granted to the principal.
	TargetEntitlementID string

	// PrincipalResourceTypeID and PrincipalResourceID identify the principal.
	PrincipalResourceTypeID string
	PrincipalResourceID     string

	// Annotation is the grant's expansion annotation. Non-nil by
	// construction: a row only appears in PendingExpansion if it has one.
	Annotation *v2.GrantExpandable

	// NeedsExpansion is the current needs_expansion column value.
	// Always true for rows returned by PendingExpansion — included
	// for parity with the underlying row shape and future flexibility.
	NeedsExpansion bool
}

// GrantAnnotation is a row yielded by GrantStore.ListWithAnnotations.
//
// Grant is always populated. Annotation is nil when the grant has no
// GrantExpandable annotation.
//
// Identity fields (GrantExternalID, TargetEntitlementID,
// PrincipalResourceTypeID, PrincipalResourceID) are ALWAYS populated
// from the underlying grant proto, regardless of whether Annotation is
// nil, so callers can use them without branching.
//
// NeedsExpansion is only meaningful when Annotation is non-nil; it
// reports the stored needs_expansion column for expandable grants.
// For non-expandable grants it is false.
type GrantAnnotation struct {
	Grant      *v2.Grant
	Annotation *v2.GrantExpandable // nil if not expandable

	GrantExternalID         string
	TargetEntitlementID     string
	PrincipalResourceTypeID string
	PrincipalResourceID     string
	NeedsExpansion          bool
}
