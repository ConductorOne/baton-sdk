package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import "github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"

func ledgerCollection(invocation *ledgerInvocation) *c1zstore.LedgerCollectionStats {
	if invocation.page.row.Collection == nil {
		invocation.page.row.Collection = &c1zstore.LedgerCollectionStats{}
	}
	return invocation.page.row.Collection
}

func recordLedgerList[T any](c *c1zstore.LedgerCollectionStats, received *uint64, records []T, next string) {
	*received += uint64(len(records))
	c.ListResponses++
	if len(records) == 0 {
		c.EmptyListResponses++
		if next != "" {
			c.EmptyListResponsesWithContinuation++
		}
	}
}
