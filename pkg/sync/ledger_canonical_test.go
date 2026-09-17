package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"bytes"
	"fmt"
	"testing"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func canonicalLedgerSnapshot(rows []ledgerKV) ([]ledgerKV, error) {
	out := make([]ledgerKV, 0, len(rows))
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
	return out, nil
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
