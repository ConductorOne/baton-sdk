package v3

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// A manifest's sync_runs projection describes the retained sync history
// of one c1z file — the SDK's retention default is a handful of syncs
// (defaultCleanupSyncLimit), and every honest file carries single-digit
// runs. Only the 16 MiB byte cap bounded the count on parse, so a
// hostile envelope materialized a SyncRunSummary (plus its nested
// Stats) per entry into heap: one 16 MiB manifest forced ~857 MiB in
// the shared worker per open. The count cap belongs at this import
// boundary (per the #1164 review's import-boundary position); a
// manifest claiming more runs than any legitimate file rejects.

func TestSecurity_ManifestSyncRunCountCapped(t *testing.T) {
	mkEntry := func(i int) []byte {
		val, err := proto.Marshal(c1zv3.SyncRunSummary_builder{
			SyncId:    "000000000000000000000000000" + string(rune('0'+i%10)),
			Type:      v3.SyncType_SYNC_TYPE_FULL,
			StartedAt: timestamppb.New(time.Unix(int64(i), 0).UTC()),
			EndedAt:   timestamppb.New(time.Unix(int64(i)+1, 0).UTC()),
		}.Build())
		require.NoError(t, err)
		return val
	}
	field40 := func(entries [][]byte) []byte {
		var buf bytes.Buffer
		for _, e := range entries {
			buf.Write(protowire.AppendTag(nil, 40, protowire.BytesType))
			buf.Write(protowire.AppendVarint(nil, uint64(len(e))))
			buf.Write(e)
		}
		return buf.Bytes()
	}

	t.Run("legitimate count parses", func(t *testing.T) {
		body := append(field40([][]byte{mkEntry(0), mkEntry(1)}), []byte{1<<3 | 2, 7, 'p', 'e', 'b', 'b', 'l', 'e', '3'}...)
		m, err := unmarshalManifestHeader(body)
		require.NoError(t, err)
		require.Len(t, m.GetSyncRuns(), 2)
		require.Equal(t, "pebble3", m.GetEngine())
	})

	t.Run("hostile count rejects with the cap named", func(t *testing.T) {
		entries := make([][]byte, maxManifestSyncRuns+1)
		for i := range entries {
			entries[i] = mkEntry(i)
		}
		body := append(field40(entries), []byte{1<<3 | 2, 7, 'p', 'e', 'b', 'b', 'l', 'e', '3'}...)
		_, err := unmarshalManifestHeader(body)
		require.Error(t, err)
		require.Contains(t, err.Error(), "sync_runs")
		require.Contains(t, err.Error(), "manifest")
	})
}
