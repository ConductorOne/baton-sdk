package pebble

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

type reportPrototypeCollection struct {
	Scope                                               c1zstore.LedgerActionIdentity
	Pages, Written, ZeroWritePages, TerminalPages       uint64
	PageMs, ConnectorMs, ReportedWaitMs, MaxConnectorMs uint64
	MissingContinuations, MissingChildren               uint64
	Outcome                                             string
}

type reportPrototypeSummary struct {
	GrantsRequest                         string
	Pages, Collections, ReferenceChecks   uint64
	MissingContinuations, MissingChildren uint64
	Top                                   []reportPrototypeCollection
}

func reportPrototype(ctx context.Context, e *Engine, emit func(reportPrototypeCollection)) (reportPrototypeSummary, error) {
	var result reportPrototypeSummary
	facts, err := e.ledger.Facts(ctx)
	if err != nil {
		return result, err
	}
	result.GrantsRequest = "request not recorded"
	if _, ok := facts["should_skip_grants"]; ok {
		result.GrantsRequest = "disabled by saved sync flag"
	}
	if _, ok := facts["should_skip_entitlements_and_grants"]; ok {
		result.GrantsRequest = "disabled with entitlements by saved sync flag"
	}
	var current *reportPrototypeCollection
	var scanErr error
	flush := func() {
		if current == nil {
			return
		}
		current.Outcome = "all recorded references resolve; endpoint outcome unavailable"
		if current.MissingContinuations > 0 || current.MissingChildren > 0 {
			current.Outcome = "recorded work has no matching completion"
		}
		result.Collections++
		result.MissingContinuations += current.MissingContinuations
		result.MissingChildren += current.MissingChildren
		if emit != nil {
			emit(*current)
		}
		result.Top = append(result.Top, *current)
		sort.Slice(result.Top, func(i, j int) bool { return result.Top[i].ConnectorMs > result.Top[j].ConnectorMs })
		if len(result.Top) > 10 {
			result.Top = result.Top[:10]
		}
	}
	missing := func(id c1zstore.LedgerActionIdentity) bool {
		result.ReferenceChecks++
		row, err := readLedgerRowRaw(e, id)
		if errors.Is(err, pebble.ErrNotFound) {
			return true
		}
		if err != nil {
			scanErr = err
			return false
		}
		return !ledgerIdentityMatches(id, row.GetIdentity(), row.GetScrubbed())
	}
	err = e.ledger.iterate(ctx, func(row *v3.LedgerRow) bool {
		if scanErr = ctx.Err(); scanErr != nil {
			return false
		}
		if row.GetScrubbed() {
			scanErr = errors.New("prototype requires original rows")
			return false
		}
		id := ledgerIdentityFromProto(row.GetIdentity())
		scope := id
		scope.PageToken = ""
		if current == nil || current.Scope != scope {
			flush()
			current = &reportPrototypeCollection{Scope: scope}
		}
		result.Pages++
		current.Pages++
		written := row.GetResourceTypesWritten() + row.GetResourcesWritten() + row.GetEntitlementsWritten() + row.GetGrantsWritten()
		current.Written += written
		if written == 0 {
			current.ZeroWritePages++
		}
		current.PageMs += row.GetPageMs()
		current.ConnectorMs += row.GetConnectorMs()
		current.ReportedWaitMs += row.GetWaitMs()
		current.MaxConnectorMs = max(current.MaxConnectorMs, row.GetConnectorMs())
		if row.GetNextPageToken() == "" {
			current.TerminalPages++
		} else {
			id.PageToken = row.GetNextPageToken()
			if missing(id) {
				current.MissingContinuations++
			}
		}
		for _, child := range row.GetChildren() {
			if missing(ledgerIdentityFromProto(child.GetIdentity())) {
				current.MissingChildren++
			}
		}
		return scanErr == nil
	})
	if err != nil {
		return result, err
	}
	if scanErr != nil {
		return result, scanErr
	}
	flush()
	return result, nil
}

func TestLedgerReportPrototype(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	put := func(resource, token, next string, records int, ms, wait uint64, children ...c1zstore.LedgerActionIdentity) {
		t.Helper()
		unit := e.ledger.newPageUnit()
		for n := 0; n < records; n++ {
			require.NoError(t, unit.StageGrants(testGrantRecord(resource, fmt.Sprintf("member-%s-%d", token, n))))
		}
		row := v3.LedgerRow_builder{NextPageToken: next, PageMs: ms + 10, ConnectorMs: ms, WaitMs: wait}.Build()
		for _, child := range children {
			row.SetChildren(append(row.GetChildren(), v3.LedgerChild_builder{Identity: ledgerIdentityToProto(child)}.Build()))
		}
		require.NoError(t, unit.Commit(ctx, grantsPageIdentity(resource, token), row))
	}
	put("team-a", "", "p2", 3, 200, 0)
	put("team-a", "p2", "", 2, 4000, 3000)
	put("team-b", "", "", 0, 100, 0)
	put("team-c", "", "p2", 1, 500, 0)
	put("team-d", "", "", 0, 50, 0, grantsPageIdentity("team-e", ""))
	var collections []reportPrototypeCollection
	report, err := reportPrototype(ctx, e, func(c reportPrototypeCollection) { collections = append(collections, c) })
	require.NoError(t, err)
	require.EqualValues(t, 5, report.Pages)
	require.EqualValues(t, 4, report.Collections)
	require.EqualValues(t, 1, report.MissingContinuations)
	require.EqualValues(t, 1, report.MissingChildren)
	require.Equal(t, "team-a", report.Top[0].Scope.ResourceID)
	require.EqualValues(t, 5, report.Top[0].Written)
	require.EqualValues(t, 3000, report.Top[0].ReportedWaitMs)
	require.Contains(t, collections[1].Outcome, "endpoint outcome unavailable")
	require.EqualValues(t, 1, collections[1].ZeroWritePages)
	encoded, err := json.MarshalIndent(collections, "", "  ")
	require.NoError(t, err)
	t.Log(string(encoded))
}

func TestLedgerReportPrototypeScope(t *testing.T) {
	for _, fact := range []string{"", "should_skip_grants", "should_skip_entitlements_and_grants"} {
		t.Run(fact, func(t *testing.T) {
			ctx := context.Background()
			e, _ := newTestEngine(t)
			_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			unit := e.ledger.newPageUnit()
			if fact != "" {
				require.NoError(t, unit.StageFact(fact))
			}
			require.NoError(t, unit.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "init"}, nil))
			report, err := reportPrototype(ctx, e, nil)
			require.NoError(t, err)
			if fact == "" {
				require.Equal(t, "request not recorded", report.GrantsRequest)
			} else {
				require.Contains(t, report.GrantsRequest, "disabled")
			}
			require.Zero(t, report.MissingChildren)
			t.Log(report.GrantsRequest)
		})
	}
}

func TestLedgerReportPrototypeFullScope(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	base := grantsPageIdentity("team-a", "")
	variants := []c1zstore.LedgerActionIdentity{base, base, base, base}
	variants[1].ParentResourceID = "parent-a"
	variants[2].ParentResourceID = "parent-b"
	variants[3].TypeScoped = true
	for _, id := range variants {
		require.NoError(t, e.ledger.newPageUnit().Commit(ctx, id, nil))
	}
	report, err := reportPrototype(ctx, e, nil)
	require.NoError(t, err)
	require.EqualValues(t, 4, report.Collections)
}

func BenchmarkLedgerReportPrototype(b *testing.B) {
	for _, pages := range []int{1000, 10000, 100000} {
		for _, perResource := range []int{1, 100} {
			b.Run(fmt.Sprintf("pages=%d/pages_per_resource=%d", pages, perResource), func(b *testing.B) {
				ctx := context.Background()
				e, err := Open(ctx, b.TempDir())
				require.NoError(b, err)
				b.Cleanup(func() { require.NoError(b, e.Close()) })
				_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(b, err)
				batch := e.db.NewRecordBatch()
				for n := 0; n < pages; n++ {
					id := grantsPageIdentity(fmt.Sprintf("group-%08d", n/perResource), fmt.Sprint(n%perResource))
					row := v3.LedgerRow_builder{Identity: ledgerIdentityToProto(id), GrantsWritten: 100, PageMs: 110, ConnectorMs: 100, WaitMs: 25}.Build()
					if n%perResource+1 < perResource {
						row.SetNextPageToken(fmt.Sprint(n%perResource + 1))
					}
					value, err := marshalRecord(row)
					require.NoError(b, err)
					require.NoError(b, batch.StageLedgerRow(encodeLedgerKey(id), value))
					if n%1000 == 999 {
						require.NoError(b, batch.Commit(pebble.NoSync))
						require.NoError(b, batch.Close())
						batch = e.db.NewRecordBatch()
					}
				}
				require.NoError(b, batch.Commit(pebble.NoSync))
				require.NoError(b, batch.Close())
				require.NoError(b, e.db.FlushMemtables())
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					report, err := reportPrototype(ctx, e, nil)
					require.NoError(b, err)
					require.EqualValues(b, pages, report.Pages)
					require.Zero(b, report.MissingContinuations)
				}
			})
		}
	}
}
