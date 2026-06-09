package dotc1z

import "github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"

// The grant-store contract lives in pkg/dotc1z/c1zstore so storage engines
// can implement it without importing this package. These aliases preserve
// the historical dotc1z names.

// GrantStore is the grant-specific slice of C1ZStore. See
// c1zstore.GrantStore for the full contract.
type GrantStore = c1zstore.GrantStore

// PendingExpansion is a lightweight row yielded by
// GrantStore.PendingExpansion. See c1zstore.PendingExpansion.
type PendingExpansion = c1zstore.PendingExpansion

// GrantAnnotation is a row yielded by GrantStore.ListWithAnnotations. See
// c1zstore.GrantAnnotation.
type GrantAnnotation = c1zstore.GrantAnnotation
