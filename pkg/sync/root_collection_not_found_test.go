package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type collectionStatusConnector struct {
	*mockConnector
	err   error
	calls int
}

func (c *collectionStatusConnector) ListResources(context.Context, *v2.ResourcesServiceListResourcesRequest, ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	return &v2.ResourcesServiceListResourcesResponse{}, nil
}

func TestRootResourceListErrorRetainsPendingPage(t *testing.T) {
	for _, workers := range []int{1, 4} {
		for _, pageToken := range []string{"", "next"} {
			for _, code := range []codes.Code{codes.NotFound, codes.PermissionDenied, codes.Unavailable} {
				for _, wrapped := range []bool{false, true} {
					t.Run(fmt.Sprintf("workers=%d/page=%q/code=%s/wrapped=%t", workers, pageToken, code, wrapped), func(t *testing.T) {
						s, f, _ := resourcePageFixture(t, workers)
						require.NoError(t, s.run.nextPage(t.Context(), s.run.current().ID, pageToken))
						action := s.run.current()
						failure := status.Error(code, "collection endpoint unavailable")
						if wrapped {
							failure = fmt.Errorf("listing users: %w", failure)
						}
						connector := &collectionStatusConnector{mockConnector: &mockConnector{}, err: failure}
						s.connector = connector
						seedLedgerTestRun(t, s, action)
						before := ledgerRawSnapshot(t, f.engine)
						f.audit.enter(ledgerHandler)
						warnings, err := s.syncParallel(t.Context(), retry.NewRetryer(t.Context(), retry.RetryConfig{MaxAttempts: 1}), []*Action{action}, s.syncLedgerResources)
						f.audit.enter(ledgerLifecycle)
						expectedCalls := 1
						if code == codes.Unavailable {
							expectedCalls = 2
						}
						require.Equal(t, expectedCalls, connector.calls)
						require.ErrorIs(t, err, failure)
						require.Equal(t, code, status.Code(err))
						require.Empty(t, warnings)
						require.Equal(t, action, s.run.current())
						require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
					})
				}
			}
		}
	}
}

func TestChildResourceListNotFoundCompletesWithWarning(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			s, f := newLedgerSchedulerFixture(t, workers)
			action := s.run.pushAction(t.Context(), Action{
				Op: SyncResourcesOp, ResourceTypeID: "child",
				ParentResourceTypeID: "parent", ParentResourceID: "deleted-parent",
			})
			failure := status.Error(codes.NotFound, "parent no longer exists")
			connector := &collectionStatusConnector{mockConnector: &mockConnector{}, err: failure}
			s.connector = connector
			seedLedgerTestRun(t, s, action)
			f.audit.enter(ledgerHandler)
			warnings, err := s.syncParallel(t.Context(), retry.NewRetryer(t.Context(), retry.RetryConfig{MaxAttempts: 1}), []*Action{action}, s.syncLedgerResources)
			f.audit.enter(ledgerLifecycle)
			require.Equal(t, 1, connector.calls)
			require.NoError(t, err)
			require.Len(t, warnings, 1)
			require.ErrorIs(t, warnings[0], failure)
			require.Nil(t, s.run.current())
		})
	}
}

func TestEmptyRootResourceListCompletes(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			s, f, _ := resourcePageFixture(t, workers)
			action := s.run.current()
			connector := &collectionStatusConnector{mockConnector: &mockConnector{}}
			s.connector = connector
			seedLedgerTestRun(t, s, action)
			f.audit.enter(ledgerHandler)
			warnings, err := s.syncParallel(t.Context(), retry.NewRetryer(t.Context(), retry.RetryConfig{MaxAttempts: 1}), []*Action{action}, s.syncLedgerResources)
			f.audit.enter(ledgerLifecycle)
			require.Equal(t, 1, connector.calls)
			require.NoError(t, err)
			require.Empty(t, warnings)
			require.Nil(t, s.run.current())
		})
	}
}

func TestActionWarningScope(t *testing.T) {
	cases := []struct {
		name    string
		action  *Action
		warning bool
	}{
		{name: "root-resources", action: &Action{Op: SyncResourcesOp}},
		{name: "parent-type-only", action: &Action{Op: SyncResourcesOp, ParentResourceTypeID: "parent"}},
		{name: "parent-id-only", action: &Action{Op: SyncResourcesOp, ParentResourceID: "one"}},
		{name: "child-resources", action: &Action{Op: SyncResourcesOp, ParentResourceTypeID: "parent", ParentResourceID: "one"}, warning: true},
		{name: "entitlements", action: &Action{Op: SyncEntitlementsOp, ResourceID: "one"}, warning: true},
		{name: "grants", action: &Action{Op: SyncGrantsOp, ResourceID: "one"}, warning: true},
		{name: "targeted-resource", action: &Action{Op: SyncTargetedResourceOp, ResourceID: "one"}, warning: true},
		{name: "unspecified-action", warning: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.warning, isActionWarning(t.Context(), tc.action, status.Error(codes.NotFound, "gone")))
			require.Equal(t, tc.warning, isActionWarning(t.Context(), tc.action, fmt.Errorf("wrapped: %w", status.Error(codes.NotFound, "gone"))))
			require.False(t, isActionWarning(t.Context(), tc.action, nil))
			require.False(t, isActionWarning(t.Context(), tc.action, status.Error(codes.PermissionDenied, "denied")))
			require.False(t, isActionWarning(t.Context(), tc.action, status.Error(codes.Unavailable, "unavailable")))
		})
	}
}
