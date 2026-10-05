package pebble

import (
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerPageRetryObservations(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	id := grantsPageIdentity("group", "credential")
	input := &c1zstore.LedgerRow{ObservationsRecorded: true, ConnectorAttempts: 3, ConnectorErrors: 2,
		SDKRetryWaitDuration: 7 * time.Millisecond, SDKRateLimitWaitDuration: 11 * time.Millisecond,
		Collection: &c1zstore.LedgerCollectionStats{ListResponses: 1,
			EmptyListResponses:                 2,
			EmptyListResponsesWithContinuation: 3,
			ResourceTypesReceived:              4,
			ResourcesReceived:                  5,
			EntitlementsReceived:               6,
			GrantsReceived:                     7,
			ResourceTypesExcludedBySelection:   8,
			EntitlementsExcludedByType:         9,
			GrantsExcludedByType:               10,
			DerivedResourcesExcludedByType:     11,
			ResourceTypesExcludedInvalid:       12,
			ResourcesExcludedInvalid:           13,
			EntitlementsExcludedInvalid:        14}}
	require.NoError(t, e.Ledger().BeginPage().Commit(ctx, id, input))
	for _, scrub := range []bool{false, true} {
		if scrub {
			require.NoError(t, e.ledger.scrubTokens(ctx))
		}
		row, found, err := e.Ledger().GetRow(ctx, id)
		require.NoError(t, err)
		require.True(t, found)
		require.True(t, row.ObservationsRecorded)
		require.Equal(t, input.Collection, row.Collection)
		require.Equal(t, input.ConnectorAttempts, row.ConnectorAttempts)
		require.Equal(t, input.ConnectorErrors, row.ConnectorErrors)
		require.Equal(t, input.SDKRetryWaitDuration, row.SDKRetryWaitDuration)
		require.Equal(t, input.SDKRateLimitWaitDuration, row.SDKRateLimitWaitDuration)
	}
	oldID := grantsPageIdentity("old", "")
	require.NoError(t, e.Ledger().BeginPage().Commit(ctx, oldID, nil))
	old, found, err := e.Ledger().GetRow(ctx, oldID)
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, old.ObservationsRecorded)
	require.Nil(t, old.Collection)
}
