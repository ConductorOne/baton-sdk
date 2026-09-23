package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestLedgerCollectionReceivedAndExcluded(t *testing.T) {
	t.Run("resources", func(t *testing.T) {
		s, f, _ := resourcePageFixture(t, 1)
		id := ledgerIdentity(s.run.current())
		_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
		require.NoError(t, err)
		row, found, err := f.ledger.GetLedgerRow(t.Context(), id)
		require.NoError(t, err)
		require.True(t, found)
		require.NotNil(t, row.Collection)
		require.EqualValues(t, 3, row.Collection.ResourcesReceived)
		require.EqualValues(t, 1, row.Collection.ResourcesExcludedInvalid)
		require.EqualValues(t, 1, row.Collection.ListResponses)
		require.Zero(t, row.Collection.EmptyListResponses)
	})
	t.Run("entitlements", func(t *testing.T) {
		s, f, _ := entitlementPageFixture(t, false)
		id := ledgerIdentity(s.run.current())
		_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
		require.NoError(t, err)
		row, found, err := f.ledger.GetLedgerRow(t.Context(), id)
		require.NoError(t, err)
		require.True(t, found)
		require.NotNil(t, row.Collection)
		require.EqualValues(t, 4, row.Collection.EntitlementsReceived)
		require.EqualValues(t, 1, row.Collection.EntitlementsExcludedByType)
		require.EqualValues(t, 2, row.Collection.EntitlementsExcludedInvalid)
	})
	t.Run("selected-types", func(t *testing.T) {
		s, f, _ := resourceTypePageFixture(t, true)
		id := ledgerIdentity(s.run.current())
		require.NoError(t, runResourceTypePages(t, s))
		row, found, err := f.ledger.GetLedgerRow(t.Context(), id)
		require.NoError(t, err)
		require.True(t, found)
		require.NotNil(t, row.Collection)
		require.EqualValues(t, 2, row.Collection.ResourceTypesReceived)
		require.EqualValues(t, 1, row.Collection.ResourceTypesExcludedInvalid)
		id.PageToken = "page-2"
		row, found, err = f.ledger.GetLedgerRow(t.Context(), id)
		require.NoError(t, err)
		require.True(t, found)
		require.NotNil(t, row.Collection)
		require.EqualValues(t, 2, row.Collection.ResourceTypesReceived)
		require.EqualValues(t, 1, row.Collection.ResourceTypesExcludedBySelection)
	})
}

type ledgerEmptyThenInvalidConnector struct{ *mockConnector }

func (c *ledgerEmptyThenInvalidConnector) ListResources(_ context.Context, req *v2.ResourcesServiceListResourcesRequest, _ ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	if req.GetPageToken() == "" {
		return v2.ResourcesServiceListResourcesResponse_builder{NextPageToken: "next"}.Build(), nil
	}
	return v2.ResourcesServiceListResourcesResponse_builder{List: []*v2.Resource{nil}}.Build(), nil
}

func TestLedgerEmptyResponseDiffersFromFilteredResponse(t *testing.T) {
	s, f, _ := resourcePageFixture(t, 1)
	s.connector = &ledgerEmptyThenInvalidConnector{mockConnector: &mockConnector{}}
	id := ledgerIdentity(s.run.current())
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	first, found, err := f.ledger.GetLedgerRow(t.Context(), id)
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, first.Collection)
	require.Zero(t, first.ResourcesWritten)
	require.EqualValues(t, 1, first.Collection.EmptyListResponses)
	require.EqualValues(t, 1, first.Collection.EmptyListResponsesWithContinuation)
	id.PageToken = "next"
	second, found, err := f.ledger.GetLedgerRow(t.Context(), id)
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, second.Collection)
	require.Zero(t, second.ResourcesWritten)
	require.EqualValues(t, 1, second.Collection.ResourcesReceived)
	require.EqualValues(t, 1, second.Collection.ResourcesExcludedInvalid)
	require.Zero(t, second.Collection.EmptyListResponses)
	require.Zero(t, second.Collection.EmptyListResponsesWithContinuation)
}

func TestLedgerCollectionGrantDerivedExclusions(t *testing.T) {
	s, f, c := grantPageFixture(t, false)
	c.grants = append(c.grants, ledgerGrant("excluded-resource", "disabled", "two", "selected"))
	id := ledgerIdentity(s.run.current())
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	row, found, err := f.ledger.GetLedgerRow(t.Context(), id)
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, row.Collection)
	require.EqualValues(t, 3, row.Collection.GrantsReceived)
	require.Zero(t, row.Collection.ResourcesReceived)
	require.EqualValues(t, 2, row.Collection.GrantsExcludedByType)
	require.EqualValues(t, 1, row.Collection.DerivedResourcesExcludedByType)
}
