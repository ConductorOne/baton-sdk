package pebble

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
	"github.com/stretchr/testify/require"
)

func TestBoundSyncUnstarted(t *testing.T) {
	for _, family := range []byte{0, rawdb.TypeResourceType, rawdb.TypeResource, rawdb.TypeEntitlement, rawdb.TypeGrant, rawdb.TypeAsset,
		rawdb.TypeIndex, rawdb.TypeCounter, rawdb.TypeSession, rawdb.TypeDigest, rawdb.TypeSourceCache, rawdb.TypeLedger} {
		t.Run(fmt.Sprintf("family-%02x", family), func(t *testing.T) {
			e, _ := newTestEngine(t)
			empty, err := e.BoundSyncUnstarted(t.Context())
			require.NoError(t, err)
			require.False(t, empty)
			_, err = e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			if family != 0 {
				require.NoError(t, e.db.UnsafeForTesting().Set([]byte{rawdb.VersionV3, family, 0x20}, []byte("unparsed-state"), pebble.Sync))
			}
			empty, err = e.BoundSyncUnstarted(t.Context())
			require.NoError(t, err)
			require.Equal(t, family == 0 || family == rawdb.TypeSession, empty)
		})
	}
	for _, kind := range []string{"token", "frontier", "stamp-only", "archive", "finished", "unreadable-run", "cancelled", "closed"} {
		t.Run(kind, func(t *testing.T) {
			e, _ := newTestEngine(t)
			id, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			ctx := t.Context()
			switch kind {
			case "token":
				require.NoError(t, e.CheckpointSync(ctx, "legacy-state"))
			case "frontier":
				require.NoError(t, e.CheckpointSync(ctx, "legacy-state"))
				_, err = e.Ledger().Takeover(ctx, "attempt", nil, c1zstore.LedgerCounters{})
				require.NoError(t, err)
			case "stamp-only":
				require.NoError(t, e.withWrite(e.Ledger().markInFlightLocked))
			case "archive":
				require.NoError(t, e.db.UnsafeForTesting().Set(ledgerArchiveKey(), []byte("unreadable-archive"), pebble.Sync))
			case "finished":
				require.NoError(t, e.EndSync(ctx))
				require.NoError(t, e.SetCurrentSync(ctx, id))
			case "unreadable-run":
				require.NoError(t, e.db.UnsafeForTesting().Set(rawdb.SyncRunKey(), []byte{0xff}, pebble.Sync))
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "closed":
				require.NoError(t, e.Close())
			}
			empty, err := e.BoundSyncUnstarted(ctx)
			switch kind {
			case "cancelled":
				require.ErrorIs(t, err, context.Canceled)
			case "closed":
				require.ErrorIs(t, err, ErrEngineClosing)
			case "unreadable-run":
				require.Error(t, err)
			default:
				require.NoError(t, err)
			}
			require.Equal(t, kind == "stamp-only", empty)
		})
	}
}
