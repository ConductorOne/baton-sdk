package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestLedgerConnectorCallTotalsIncludeRetriedCalls(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, f := newLedgerSchedulerFixture(t, workers)
				s.recordStats = true
				s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type"})

				calls := 0
				usage, err := anypb.New(v2.SessionStoreUsage_builder{Ops: []*v2.SessionStoreUsage_OpStats{
					v2.SessionStoreUsage_OpStats_builder{Op: "get", Count: 2, Errors: 1, Timeouts: 1, TotalMs: 6, MaxMs: 4}.Build(),
				}}.Build())
				require.NoError(t, err)
				wait, err := anypb.New(v2.RateLimitWaitReport_builder{WaitMs: 2}.Build())
				require.NoError(t, err)
				annos := []*anypb.Any{usage, wait}
				var rows []c1zstore.LedgerRow
				s.testHooks.ledgerCommitted = func(row c1zstore.LedgerRow) { rows = append(rows, row) }
				s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, page *ledgerPage) error {
					calls++
					invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
					s.recordLedgerConnectorResponse(ctx, invocation, "list-resources", 4*time.Millisecond, annos)
					s.recordLedgerSessionUsage(invocation, annos)
					if calls < 3 {
						require.NoError(t, page.setFact("failed-attempt"))
						page.observations.Counters = map[string]uint64{"failed-ingest": 7}
						require.NoError(t, page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "failed"}.Build()))
						err := status.Error(codes.Unavailable, "retry")
						recordLedgerConnectorError(invocation, err)
						return err
					}
					if calls == 3 || calls == 4 {
						return s.nextPageOrFinishAction(ctx, action, "next")
					}
					return s.nextPageOrFinishAction(ctx, action, "")
				}

				ctx := s.withRateLimitWaitObserver(t.Context())
				r := retry.NewRetryer(ctx, retry.RetryConfig{MaxAttempts: 4, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond})
				seedLedgerTestRun(t, s, nil)
				_, err = s.syncParallel(ctx, r, s.run.peekMatchingActions(ctx, SyncResourcesOp), s.SyncResources)
				require.NoError(t, err)
				require.Equal(t, 5, calls)

				stored, err := f.ledger.LedgerCounters(ctx)
				require.NoError(t, err)

				for _, key := range []string{"list-resources", "list-resources:type"} {
					assert.Equal(t, c1zstore.CallStat{Count: 5, TotalMs: 20, MaxMs: 4}, stored.ConnectorCalls[key])
					assert.Equal(t, ConnectorCallStat{Count: 5, TotalMs: 20, MaxMs: 4}, s.stats.connectorCallStats()[key])
				}
				assert.Equal(t, c1zstore.CallStat{Count: 10, Errors: 5, Timeouts: 5, TotalMs: 30, MaxMs: 4}, stored.SessionCalls["connector.get"])
				assert.Equal(t, SessionStoreStat{Count: 10, Errors: 5, Timeouts: 5, TotalMs: 30, MaxMs: 4}, s.stats.sessionStoreStats()["connector.get"])
				for _, key := range []string{"rate_limit_wait", "rate_limit_wait:type"} {
					assert.EqualValues(t, 10, stored.StepDurationsMs[key])
					assert.EqualValues(t, 10, s.stats.stepDurations()[key])
				}
				require.Len(t, rows, 3)
				assert.EqualValues(t, 3, rows[0].ConnectorAttempts)
				assert.EqualValues(t, 2, rows[0].ConnectorErrors)
				assert.EqualValues(t, 1, rows[1].ConnectorAttempts)
				assert.Zero(t, rows[1].ConnectorErrors)
				assert.EqualValues(t, 1, rows[2].ConnectorAttempts)
				assert.Zero(t, rows[2].ConnectorErrors)
				assert.EqualValues(t, 1, stored.Counters[ledgerCompletedActions])
				assert.Zero(t, stored.Counters["failed-ingest"])
				f.audit.enter(ledgerLifecycle)
				require.NoError(t, f.store.Close(ctx))
				f = openLedgerFixtureAt(t, f.path, false)
				require.NoError(t, f.store.SetCurrentSync(ctx, s.syncID))
				reopened, err := f.ledger.LedgerCounters(ctx)
				require.NoError(t, err)
				assert.Equal(t, stored, reopened)
				facts, err := f.ledger.LedgerFacts(ctx)
				require.NoError(t, err)
				assert.NotContains(t, facts, "failed-attempt")
				_, err = f.store.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{ResourceTypeId: "failed"}.Build())
				assert.Equal(t, codes.NotFound, status.Code(err))
			})
		})
	}
}
