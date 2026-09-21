package pebble

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"testing"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

type reportPrototypeTestIter struct {
	keys, values        [][]byte
	index, walks, reads int
	err                 error
}

func (i *reportPrototypeTestIter) First() bool   { i.walks++; i.index = 0; return i.Valid() }
func (i *reportPrototypeTestIter) Valid() bool   { return i.index < len(i.keys) }
func (i *reportPrototypeTestIter) Next() bool    { i.index++; return i.Valid() }
func (i *reportPrototypeTestIter) Key() []byte   { return i.keys[i.index] }
func (i *reportPrototypeTestIter) Value() []byte { i.reads++; return i.values[i.index] }
func (i *reportPrototypeTestIter) Error() error  { return i.err }

func reportPrototypeTestRows(t *testing.T, types int) *reportPrototypeTestIter {
	t.Helper()
	i := &reportPrototypeTestIter{}
	for ty := 0; ty < types; ty++ {
		for resource := 0; resource < 2; resource++ {
			for page := 0; page < 2; page++ {
				id := c1zstore.LedgerActionIdentity{Op: "grants", ResourceTypeID: fmt.Sprintf("type-%02d", ty), ResourceID: fmt.Sprint(resource), PageToken: fmt.Sprint(page)}
				row := v3.LedgerRow_builder{Identity: ledgerIdentityToProto(id), ConnectorMs: 10, GrantsWritten: 2}.Build()
				if page == 0 {
					row.SetNextPageToken("1")
				}
				data, err := marshalRecord(row)
				require.NoError(t, err)
				i.keys = append(i.keys, encodeLedgerKey(id))
				i.values = append(i.values, data)
			}
		}
	}
	order := make([]int, len(i.keys))
	for n := range order {
		order[n] = n
	}
	sort.Slice(order, func(a, b int) bool { return bytes.Compare(i.keys[order[a]], i.keys[order[b]]) < 0 })
	keys := make([][]byte, 0, len(order))
	values := make([][]byte, 0, len(order))
	for _, n := range order {
		keys = append(keys, i.keys[n])
		values = append(values, i.values[n])
	}
	i.keys = keys
	i.values = values
	return i
}

func TestLedgerReportSingleWalk(t *testing.T) {
	i := reportPrototypeTestRows(t, 12)
	i.keys = append(i.keys, encodeLedgerFactKey("should_skip_grants"))
	i.values = append(i.values, []byte("ignored fact payload"))
	groups, collections := 0, 0
	r, err := reportPrototypeScan(context.Background(), i, func(c reportPrototypeCollection) {
		collections++
		require.EqualValues(t, 2, c.Pages)
	}, func(c reportPrototypeCollection) error {
		groups++
		require.EqualValues(t, 4, c.Pages)
		require.EqualValues(t, 2, c.Collections)
		require.EqualValues(t, 40, c.ConnectorMs)
		require.EqualValues(t, 8, c.Written)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, i.walks)
	require.Equal(t, 48, i.reads)
	require.EqualValues(t, 49, r.LedgerKeysScanned)
	require.Equal(t, 12, groups)
	require.Equal(t, 24, collections)
	require.Len(t, r.Top, 10)
	require.Len(t, r.TopOperationTypes, 10)
	require.EqualValues(t, 24, r.Continuations)
	require.True(t, *r.GrantsDisabled)
	data, err := renderReportPrototype(r)
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, false, decoded["reference_validation_performed"])
	require.Nil(t, decoded["missing_continuation_references"])
	require.Nil(t, decoded["missing_child_references"])
}

func TestLedgerReportStreamErrors(t *testing.T) {
	boom := errors.New("iterator or sink failed")
	i := reportPrototypeTestRows(t, 1)
	i.err = boom
	_, err := reportPrototypeScan(context.Background(), i, nil, nil)
	require.ErrorIs(t, err, boom)
	i = reportPrototypeTestRows(t, 1)
	_, err = reportPrototypeScan(context.Background(), i, nil, func(reportPrototypeCollection) error { return boom })
	require.ErrorIs(t, err, boom)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	i = reportPrototypeTestRows(t, 1)
	_, err = reportPrototypeScan(ctx, i, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, i.reads)
	i = reportPrototypeTestRows(t, 1)
	i.values[0] = []byte{0xff}
	_, err = reportPrototypeScan(context.Background(), i, nil, nil)
	require.Error(t, err)
}

func TestLedgerReportProjection(t *testing.T) {
	id := grantsPageIdentity("group-a", "credential-token")
	id.ParentResourceTypeID = "org"
	id.ParentResourceID = "parent"
	id.TypeScoped = true
	for _, next := range []string{"", "next-token"} {
		for _, scrub := range []bool{false, true} {
			row := v3.LedgerRow_builder{Identity: ledgerIdentityToProto(id), NextPageToken: next,
				ResourceTypesWritten: 1, ResourcesWritten: 2, EntitlementsWritten: 3, GrantsWritten: 4, PageMs: 100, ConnectorMs: 80, WaitMs: 30,
				Children: []*v3.LedgerChild{v3.LedgerChild_builder{Identity: ledgerIdentityToProto(id)}.Build()},
			}.Build()
			if scrub {
				scrubLedgerRow(row)
			}
			data, err := marshalRecord(row)
			require.NoError(t, err)
			data = protowire.AppendTag(data, 100, protowire.BytesType)
			data = protowire.AppendBytes(data, []byte("unknown"))
			got, err := reportPrototypeProject(data)
			require.NoError(t, err)
			scope := id
			scope.PageToken = ""
			require.Equal(t, scope, got.scope)
			require.EqualValues(t, 10, got.written)
			require.EqualValues(t, 100, got.pageMS)
			require.EqualValues(t, 80, got.connectorMS)
			require.EqualValues(t, 30, got.waitMS)
			require.EqualValues(t, 1, got.children)
			require.True(t, got.paginationKnown)
			require.Equal(t, next == "", got.terminal)
			data = protowire.AppendTag(data, 10, protowire.VarintType)
			data = protowire.AppendVarint(data, 5)
			got, err = reportPrototypeProject(data)
			require.NoError(t, err)
			require.EqualValues(t, 11, got.written)
		}
	}
	row := v3.LedgerRow_builder{Identity: ledgerIdentityToProto(id), Scrubbed: true}.Build()
	data, err := marshalRecord(row)
	require.NoError(t, err)
	got, err := reportPrototypeProject(data)
	require.NoError(t, err)
	require.False(t, got.paginationKnown)
	_, err = reportPrototypeProject(nil)
	require.Error(t, err)
}

func TestLedgerReportWidePageAllocations(t *testing.T) {
	row := v3.LedgerRow_builder{Identity: ledgerIdentityToProto(grantsPageIdentity("group", "secret"))}.Build()
	data, err := marshalRecord(row)
	require.NoError(t, err)
	child, err := marshalRecord(v3.LedgerChild_builder{Identity: row.GetIdentity()}.Build())
	require.NoError(t, err)
	field := protowire.AppendTag(nil, 4, protowire.BytesType)
	field = protowire.AppendBytes(field, child)
	var got reportPrototypeProjected
	narrow := testing.AllocsPerRun(5, func() { got, err = reportPrototypeProject(data) })
	wide := append(bytes.Clone(data), bytes.Repeat(field, 100000)...)
	wideAllocs := testing.AllocsPerRun(5, func() { got, err = reportPrototypeProject(wide) })
	require.NoError(t, err)
	require.EqualValues(t, 100000, got.children)
	require.LessOrEqual(t, wideAllocs, narrow)
	t.Logf("100000 children: encoded_bytes=%d narrow_allocs=%g wide_allocs=%g", len(wide), narrow, wideAllocs)
}

func TestLedgerReportWriteFamilies(t *testing.T) {
	iter := &reportPrototypeTestIter{}
	for _, token := range []string{"", "next"} {
		id := grantsPageIdentity("group", token)
		row := v3.LedgerRow_builder{Identity: ledgerIdentityToProto(id), ResourceTypesWritten: 1,
			ResourcesWritten: 2, EntitlementsWritten: 3, GrantsWritten: 4,
			ObservationsRecorded: token == "", ConnectorAttempts: 3, ConnectorErrors: 2, SdkRetryWaitMs: 7, SdkRateLimitWaitMs: 11}.Build()
		if token == "" {
			row.SetCollection(v3.LedgerCollectionStats_builder{
				ListResponses:                      1,
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
				EntitlementsExcludedInvalid:        14,
			}.Build())
		}
		if token != "" {
			row.SetConnectorAttempts(0)
			row.SetConnectorErrors(0)
			row.SetSdkRetryWaitMs(0)
			row.SetSdkRateLimitWaitMs(0)
		}

		data, err := marshalRecord(row)
		require.NoError(t, err)
		iter.keys = append(iter.keys, encodeLedgerKey(id))
		iter.values = append(iter.values, data)
	}
	report, err := reportPrototypeScan(context.Background(), iter, nil, nil)
	require.NoError(t, err)
	data, err := renderReportPrototype(report)
	require.NoError(t, err)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(data, &payload))
	want := map[string]any{"resource_types": float64(2), "resources": float64(4), "entitlements": float64(6), "grants": float64(8)}
	require.Equal(t, want, payload["record_writes_by_family"])
	for _, key := range []string{"top_collections_by_connector_ms", "top_operation_types_by_connector_ms"} {
		groups := payload[key].([]any)
		require.Len(t, groups, 1)
		require.Equal(t, want, groups[0].(map[string]any)["record_writes_by_family"])
	}
	wantAttempts := map[string]any{"pages_with_observations": float64(1), "connector_attempts": float64(3), "connector_errors": float64(2),
		"sdk_retry_wait_sum_ms": float64(7), "sdk_rate_limit_wait_sum_ms": float64(11)}
	require.Equal(t, wantAttempts, payload["attempt_observations"])
	for _, key := range []string{"top_collections_by_connector_ms", "top_operation_types_by_connector_ms"} {
		require.Equal(t, wantAttempts, payload[key].([]any)[0].(map[string]any)["attempt_observations"])
	}
	wantCollection := map[string]any{
		"list_responses":                         float64(1),
		"empty_list_responses":                   float64(2),
		"empty_list_responses_with_continuation": float64(3),
		"resource_types_received":                float64(4),
		"resources_received":                     float64(5),
		"entitlements_received":                  float64(6),
		"grants_received":                        float64(7),
		"resource_types_excluded_by_selection":   float64(8),
		"entitlements_excluded_by_type":          float64(9),
		"grants_excluded_by_type":                float64(10),
		"derived_resources_excluded_by_type":     float64(11),
		"resource_types_excluded_invalid":        float64(12),
		"resources_excluded_invalid":             float64(13),
		"entitlements_excluded_invalid":          float64(14),
	}
	require.Equal(t, wantCollection, payload["collection_observations"])
	require.EqualValues(t, 1, payload["pages_with_collection_observations"])
	for _, key := range []string{"top_collections_by_connector_ms", "top_operation_types_by_connector_ms"} {
		group := payload[key].([]any)[0].(map[string]any)
		require.Equal(t, wantCollection, group["collection_observations"])
		require.EqualValues(t, 1, group["pages_with_collection_observations"])
	}
	require.EqualValues(t, 20, report.Written)
	require.Equal(t, 1, iter.walks)
	require.Equal(t, 2, iter.reads)
}
