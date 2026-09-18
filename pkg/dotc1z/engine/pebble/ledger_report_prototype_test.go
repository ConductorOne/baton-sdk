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
	Scope                                                c1zstore.LedgerActionIdentity
	Pages, Written, ZeroWritePages, TerminalPages        uint64
	PageMs, ConnectorMs, ReportedWaitMs, MaxConnectorMs  uint64
	MissingContinuations, MissingChildren                uint64
	ConnectorPageMedian, ConnectorPageP95                reportPrototypeInterval
	WrittenPerPage                                       float64
	PagesPerThousandWrites, ConnectorMsPerThousandWrites *float64
}

type reportPrototypeSummary struct {
	GrantsDisabled, EntitlementsDisabled  *bool
	Written, ConnectorMs, ReportedWaitMs  uint64
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
	if _, ok := facts["should_skip_grants"]; ok {
		disabled := true
		result.GrantsDisabled = &disabled
	}
	if _, ok := facts["should_skip_entitlements_and_grants"]; ok {
		disabled := true
		result.GrantsDisabled = &disabled
		result.EntitlementsDisabled = &disabled
	}
	var current *reportPrototypeCollection
	var latency reportPrototypeHistogram
	var scanErr error
	flush := func() {
		if current == nil {
			return
		}
		current.ConnectorPageMedian = latency.quantile(50)
		current.ConnectorPageP95 = latency.quantile(95)
		current.WrittenPerPage = float64(current.Written) / float64(current.Pages)
		if current.Written > 0 {
			pages := float64(current.Pages) * 1000 / float64(current.Written)
			ms := float64(current.ConnectorMs) * 1000 / float64(current.Written)
			current.PagesPerThousandWrites = &pages
			current.ConnectorMsPerThousandWrites = &ms
		}
		result.Collections++
		result.MissingContinuations += current.MissingContinuations
		result.MissingChildren += current.MissingChildren
		if emit != nil {
			emit(*current)
		}
		result.Top = append(result.Top, *current)
		sort.Slice(result.Top, func(i, j int) bool { return reportPrototypeRankBefore(result.Top[i], result.Top[j]) })
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
			latency = reportPrototypeHistogram{}
		}
		result.Pages++
		current.Pages++
		written := row.GetResourceTypesWritten() + row.GetResourcesWritten() + row.GetEntitlementsWritten() + row.GetGrantsWritten()
		current.Written += written
		result.Written += written
		if written == 0 {
			current.ZeroWritePages++
		}
		current.PageMs += row.GetPageMs()
		current.ConnectorMs += row.GetConnectorMs()
		result.ConnectorMs += row.GetConnectorMs()
		result.ReportedWaitMs += row.GetWaitMs()
		latency.add(row.GetConnectorMs())
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
	require.EqualValues(t, 4850, report.ConnectorMs)
	require.EqualValues(t, 6, report.Written)
	require.InDelta(t, 86.597938, *reportPrototypeShare(report.Top[0].ConnectorMs, report.ConnectorMs), 0.000001)
	require.Equal(t, reportPrototypeInterval{128, 255, true}, report.Top[0].ConnectorPageMedian)
	require.Equal(t, reportPrototypeInterval{2048, 4095, true}, report.Top[0].ConnectorPageP95)
	require.Equal(t, 2.5, report.Top[0].WrittenPerPage)
	require.Equal(t, 400.0, *report.Top[0].PagesPerThousandWrites)
	require.Equal(t, 840000.0, *report.Top[0].ConnectorMsPerThousandWrites)
	require.Nil(t, collections[1].ConnectorMsPerThousandWrites)
	require.Nil(t, collections[1].PagesPerThousandWrites)
	require.EqualValues(t, 1, collections[1].ZeroWritePages)
	encoded, err := json.MarshalIndent(collections, "", "  ")
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "p2")
	t.Log(string(encoded))
	require.NoError(t, exportReportPrototype(report))
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
				require.Nil(t, report.GrantsDisabled)
			} else {
				require.NotNil(t, report.GrantsDisabled)
				require.True(t, *report.GrantsDisabled)
			}
			require.Zero(t, report.MissingChildren)
			t.Log(report.GrantsDisabled)
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
					html, err := renderReportPrototype(report)
					require.NoError(b, err)
					b.ReportMetric(float64(len(html)), "report-bytes")
				}
			})
		}
	}
}

func TestLedgerReportTopLimit(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	for n := 10; n >= 0; n-- {
		id := grantsPageIdentity(fmt.Sprintf("team-%02d", n), "")
		row := v3.LedgerRow_builder{ConnectorMs: 100}.Build()
		require.NoError(t, e.ledger.newPageUnit().Commit(ctx, id, row))
	}
	report, err := reportPrototype(ctx, e, nil)
	require.NoError(t, err)
	require.Len(t, report.Top, 10)
	require.EqualValues(t, 11, report.Collections)
	require.EqualValues(t, 1100, report.ConnectorMs)
	require.Equal(t, "team-00", report.Top[0].Scope.ResourceID)
	require.Equal(t, "team-09", report.Top[9].Scope.ResourceID)
	require.InDelta(t, 9.090909, *reportPrototypeShare(report.Top[0].ConnectorMs, report.ConnectorMs), 0.000001)
}
