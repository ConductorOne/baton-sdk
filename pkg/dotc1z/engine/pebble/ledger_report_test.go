package pebble

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerReport(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	put := func(resource, token, next string, records uint64, ms, wait uint64, children ...c1zstore.LedgerActionIdentity) {
		t.Helper()
		unit := e.ledger.newPageUnit()
		for n := uint64(0); n < records; n++ {
			require.NoError(t, unit.StageGrants(testGrantRecord(resource, fmt.Sprintf("member-%s-%d", token, n))))
		}
		collection := v3.LedgerCollectionStats_builder{ListResponses: 1, GrantsReceived: records}.Build()
		if records == 0 {
			collection.SetEmptyListResponses(1)
		}
		if records == 0 && next != "" {
			collection.SetEmptyListResponsesWithContinuation(1)
		}
		row := v3.LedgerRow_builder{NextPageToken: next, PageMs: ms + 10, ConnectorMs: ms, WaitMs: wait,
			ObservationsRecorded: true, ConnectorAttempts: 1, Collection: collection}.Build()
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
	var collections []ledgerReportCollection
	report, err := ledgerReport(ctx, e, func(c ledgerReportCollection) { collections = append(collections, c) })
	require.NoError(t, err)
	require.EqualValues(t, 5, report.Pages)
	require.EqualValues(t, 4, report.Collections)
	require.EqualValues(t, 2, report.Continuations)
	require.EqualValues(t, 1, report.Children)
	require.Equal(t, "team-a", report.Top[0].Scope.ResourceID)
	require.EqualValues(t, 5, report.Top[0].Written)
	require.EqualValues(t, 3000, report.Top[0].ReportedWaitMs)
	require.EqualValues(t, 4850, report.ConnectorMs)
	require.EqualValues(t, 6, report.Written)
	require.InDelta(t, 86.597938, *ledgerReportShare(report.Top[0].ConnectorMs, report.ConnectorMs), 0.000001)
	require.Equal(t, ledgerReportInterval{128, 255, true}, report.Top[0].ConnectorPageMedian)
	require.Equal(t, ledgerReportInterval{2048, 4095, true}, report.Top[0].ConnectorPageP95)
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
}

func TestLedgerReportScope(t *testing.T) {
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
			report, err := ledgerReport(ctx, e, nil)
			require.NoError(t, err)
			if fact == "" {
				require.Nil(t, report.GrantsDisabled)
			} else {
				require.NotNil(t, report.GrantsDisabled)
				require.True(t, *report.GrantsDisabled)
			}
			require.Zero(t, report.Children)
			t.Log(report.GrantsDisabled)
		})
	}
}

func TestLedgerReportFullScope(t *testing.T) {
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
	report, err := ledgerReport(ctx, e, nil)
	require.NoError(t, err)
	require.EqualValues(t, 4, report.Collections)
}

func BenchmarkLedgerReport(b *testing.B) {
	for _, pages := range []int{10000, 100000, 1000000} {
		for _, shape := range []struct {
			name          string
			perResource   int
			distinctTypes bool
		}{
			{"many-resources", 1, false}, {"pagination-100", 100, false}, {"single-chain", pages, false}, {"many-types", 1, true},
		} {
			perResource := shape.perResource
			b.Run(fmt.Sprintf("pages=%d/%s", pages, shape.name), func(b *testing.B) {
				ctx := context.Background()
				e, err := Open(ctx, b.TempDir())
				require.NoError(b, err)
				b.Cleanup(func() { require.NoError(b, e.Close()) })
				_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(b, err)
				batch := e.db.NewRecordBatch()
				for n := 0; n < pages; n++ {
					id := grantsPageIdentity(fmt.Sprintf("group-%08d", n/perResource), fmt.Sprint(n%perResource))
					if shape.distinctTypes {
						id.ResourceTypeID = id.ResourceID
						id.ResourceID = "resource"
					}
					row := v3.LedgerRow_builder{Identity: ledgerIdentityToProto(id), GrantsWritten: 100, PageMs: 110, ConnectorMs: 100, WaitMs: 25,
						ObservationsRecorded: true, ConnectorAttempts: 1,
						Collection: v3.LedgerCollectionStats_builder{ListResponses: 1, GrantsReceived: 102, GrantsExcludedByType: 2}.Build(),
					}.Build()
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
				stopMemory := startReportMemory(b)
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					report, err := ledgerReport(ctx, e, nil)
					require.NoError(b, err)
					require.EqualValues(b, pages, report.Pages)
					require.EqualValues(b, pages/perResource, report.Collections)
					payload, err := renderLedgerReport(report)
					require.NoError(b, err)
					b.ReportMetric(float64(len(payload)), "report-bytes")
				}
				b.StopTimer()
				stopMemory()
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
	report, err := ledgerReport(ctx, e, nil)
	require.NoError(t, err)
	require.Len(t, report.Top, 10)
	require.EqualValues(t, 11, report.Collections)
	require.EqualValues(t, 1100, report.ConnectorMs)
	require.Equal(t, "team-00", report.Top[0].Scope.ResourceID)
	require.Equal(t, "team-09", report.Top[9].Scope.ResourceID)
	require.InDelta(t, 9.090909, *ledgerReportShare(report.Top[0].ConnectorMs, report.ConnectorMs), 0.000001)
}

func TestLedgerReportGenerationDoesNotWrite(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := t.Context()
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.Ledger().BeginPage().Commit(ctx, grantsPageIdentity("group", "secret"), &c1zstore.LedgerRow{ConnectorAttempts: 1}))
	before := dumpKeyRange(t, e, nil, nil)
	data, err := e.GenerateLedgerReport(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.Equal(t, before, dumpKeyRange(t, e, nil, nil))
}
