package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func canonicalLedgerSnapshot(rows []ledgerKV) ([]ledgerKV, error) {
	out := make([]ledgerKV, 0, len(rows))
	var buckets []*v3.LedgerCounterBucket
	options := make(map[string]bool)
	for _, row := range rows {
		copyRow := ledgerKV{key: bytes.Clone(row.key), value: bytes.Clone(row.value)}
		if len(row.key) >= 3 && row.key[0] == 0x03 && row.key[1] == 0x0c {
			var normalized proto.Message
			switch row.key[2] {
			case 0:
				value := &v3.LedgerRow{}
				if err := proto.Unmarshal(row.value, value); err != nil {
					return nil, fmt.Errorf("canonical ledger row: %w", err)
				}
				value.SetAttempt("")
				value.SetCommittedAt(nil)
				value.SetPageMs(0)
				value.SetConnectorMs(0)
				value.SetWaitMs(0)
				normalized = value
			case 1:
				name := string(row.key[3:])
				if name == c1zstore.LedgerFactReportOptions || name == c1zstore.LedgerFactFirstReportOptions {
					if len(row.value) < 2 || row.value[0] != 2 {
						return nil, fmt.Errorf("invalid option fact")
					}
					var snapshot map[string]json.RawMessage
					if err := json.Unmarshal(row.value[1:], &snapshot); err != nil {
						return nil, err
					}
					delete(snapshot, "attempt")
					encoded, err := json.Marshal(snapshot)
					if err != nil {
						return nil, err
					}
					options[string(encoded)] = true
					continue
				}
			case 2:
				value := &v3.LedgerCounterBucket{}
				if err := proto.Unmarshal(row.value, value); err != nil {
					return nil, fmt.Errorf("canonical counter bucket: %w", err)
				}
				buckets = append(buckets, value)
				continue
			case 3:
				value := &v3.LedgerFrontier{}
				if err := proto.Unmarshal(row.value, value); err != nil {
					return nil, fmt.Errorf("canonical frontier: %w", err)
				}
				value.SetAttempt("")
				value.SetTakenOverAt(nil)
				normalized = value
			}
			if normalized != nil {
				encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(normalized)
				if err != nil {
					return nil, err
				}
				copyRow.value = encoded
			}
		}
		out = append(out, copyRow)
	}
	for option := range options {
		out = append(out, ledgerKV{key: append([]byte{3, 12, 1}, []byte("canonical-options:"+option)...), value: []byte(option)})
	}
	if len(buckets) != 0 {
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(foldCanonicalLedgerBuckets(buckets))
		if err != nil {
			return nil, err
		}
		out = append(out, ledgerKV{key: []byte{3, 12, 2}, value: encoded})
	}
	slices.SortFunc(out, func(a, b ledgerKV) int { return bytes.Compare(a.key, b.key) })
	return out, nil
}

func ledgerSnapshotWithFoldedCounters(t *testing.T, e *engine.Engine) []ledgerKV {
	t.Helper()
	var rows []ledgerKV
	var buckets []*v3.LedgerCounterBucket
	for _, row := range ledgerRawSnapshot(t, e) {
		if bytes.HasPrefix(row.key, []byte{3, 12, 2}) {
			bucket := &v3.LedgerCounterBucket{}
			require.NoError(t, proto.Unmarshal(row.value, bucket))
			buckets = append(buckets, bucket)
		} else {
			rows = append(rows, row)
		}
	}
	if len(buckets) != 0 {
		value, err := proto.MarshalOptions{Deterministic: true}.Marshal(foldCanonicalLedgerBuckets(buckets))
		require.NoError(t, err)
		rows = append(rows, ledgerKV{key: []byte{3, 12, 2}, value: value})
	}
	slices.SortFunc(rows, func(a, b ledgerKV) int { return bytes.Compare(a.key, b.key) })
	return rows
}

func TestLedgerCanonicalRowNormalization(t *testing.T) {
	first := v3.LedgerRow_builder{
		Attempt: "first", CommittedAt: &timestamppb.Timestamp{Seconds: 1}, PageMs: 2, ConnectorMs: 1,
		NextPageToken: "next", ResourcesWritten: 3,
	}.Build()
	second := proto.Clone(first).(*v3.LedgerRow)
	second.SetAttempt("second")
	second.SetCommittedAt(&timestamppb.Timestamp{Seconds: 4})
	second.SetPageMs(8)
	second.SetWaitMs(2)
	snapshot := func(value *v3.LedgerRow) []ledgerKV {
		encoded, err := proto.Marshal(value)
		require.NoError(t, err)
		normalized, err := canonicalLedgerSnapshot([]ledgerKV{{key: []byte{3, 12, 0, 1}, value: encoded}})
		require.NoError(t, err)
		return normalized
	}
	expected := snapshot(first)
	require.True(t, equalLedgerSnapshot(expected, snapshot(second)))
	second.SetResourcesWritten(2)
	require.False(t, equalLedgerSnapshot(expected, snapshot(second)))
	second.SetResourcesWritten(3)
	second.SetNextPageToken("wrong-child-cursor")
	require.False(t, equalLedgerSnapshot(expected, snapshot(second)))
	require.Equal(t, "first", first.GetAttempt(), "normalization must not mutate the source")
}

func TestLedgerCanonicalRetainsOtherFamilies(t *testing.T) {
	for _, family := range []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 255} {
		t.Run(fmt.Sprint(family), func(t *testing.T) {
			before := []ledgerKV{{key: []byte{3, family, 0}, value: []byte("record-value")}}
			actual, err := canonicalLedgerSnapshot(before)
			require.NoError(t, err)
			require.True(t, equalLedgerSnapshot(before, actual))
			actual[0].value[0] = 'X'
			require.False(t, equalLedgerSnapshot(before, actual))
		})
	}
	for _, subfamily := range []byte{0, 3} {
		_, err := canonicalLedgerSnapshot([]ledgerKV{{key: []byte{3, 12, subfamily}, value: []byte{0xff}}})
		require.Error(t, err, "unreadable ledger values cannot disappear during normalization")
	}
}

func foldCanonicalLedgerBuckets(buckets []*v3.LedgerCounterBucket) *v3.LedgerCounterBucket {
	counters := make(map[string]uint64)
	durations := make(map[string]int64)
	connector := make(map[string]*v3.CallStat)
	session := make(map[string]*v3.CallStat)
	var flags uint64
	addCalls := func(dst, src map[string]*v3.CallStat) {
		for key, value := range src {
			prior := dst[key]
			dst[key] = v3.CallStat_builder{
				Count: prior.GetCount() + value.GetCount(), TotalMs: prior.GetTotalMs() + value.GetTotalMs(),
				MaxMs: max(prior.GetMaxMs(), value.GetMaxMs()), Errors: prior.GetErrors() + value.GetErrors(), Timeouts: prior.GetTimeouts() + value.GetTimeouts(),
			}.Build()
		}
	}
	for _, bucket := range buckets {
		for key, value := range bucket.GetCounters() {
			counters[key] += value
		}
		for key, value := range bucket.GetStepDurationsMs() {
			durations[key] += value
		}
		flags |= bucket.GetFlags()
		addCalls(connector, bucket.GetConnectorCalls())
		addCalls(session, bucket.GetSessionCalls())
	}
	return v3.LedgerCounterBucket_builder{Counters: counters, Flags: flags, StepDurationsMs: durations, ConnectorCalls: connector, SessionCalls: session}.Build()
}

func TestLedgerCanonicalFoldsCommittedBuckets(t *testing.T) {
	snapshot := func(buckets ...*v3.LedgerCounterBucket) []ledgerKV {
		var rows []ledgerKV
		for i, bucket := range buckets {
			encoded, err := proto.Marshal(bucket)
			require.NoError(t, err)
			rows = append(rows, ledgerKV{key: []byte{3, 12, 2, byte(i + 1)}, value: encoded})
		}
		canonical, err := canonicalLedgerSnapshot(rows)
		require.NoError(t, err)
		return canonical
	}
	first := v3.LedgerCounterBucket_builder{Counters: map[string]uint64{"records": 3}, Flags: 1}.Build()
	second := v3.LedgerCounterBucket_builder{Counters: map[string]uint64{"records": 4}, Flags: 2}.Build()
	whole := v3.LedgerCounterBucket_builder{Counters: map[string]uint64{"records": 7}, Flags: 3}.Build()
	first.SetConnectorCalls(map[string]*v3.CallStat{"list": v3.CallStat_builder{Count: 1, TotalMs: 5, MaxMs: 5}.Build()})
	second.SetConnectorCalls(map[string]*v3.CallStat{"list": v3.CallStat_builder{Count: 2, TotalMs: 9, MaxMs: 7}.Build()})
	whole.SetConnectorCalls(map[string]*v3.CallStat{"list": v3.CallStat_builder{Count: 3, TotalMs: 14, MaxMs: 7}.Build()})
	first.SetSessionCalls(map[string]*v3.CallStat{"get": v3.CallStat_builder{Count: 1, Errors: 1, TotalMs: 5, MaxMs: 5}.Build()})
	second.SetSessionCalls(map[string]*v3.CallStat{"get": v3.CallStat_builder{Count: 2, Timeouts: 1, TotalMs: 9, MaxMs: 7}.Build()})
	whole.SetSessionCalls(map[string]*v3.CallStat{"get": v3.CallStat_builder{Count: 3, Errors: 1, Timeouts: 1, TotalMs: 14, MaxMs: 7}.Build()})
	first.SetStepDurationsMs(map[string]int64{"collection": 5})
	second.SetStepDurationsMs(map[string]int64{"collection": 7})
	whole.SetStepDurationsMs(map[string]int64{"collection": 12})
	require.Equal(t, snapshot(whole), snapshot(first, second))
	second.SetCounters(map[string]uint64{"records": 3})
	require.NotEqual(t, snapshot(whole), snapshot(first, second))
}
