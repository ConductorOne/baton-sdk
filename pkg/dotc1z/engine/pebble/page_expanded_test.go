package pebble

import (
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func pageExpandedSeed(e *Engine) *v3.GrantRecord {
	rec := V2GrantToV3(e.CurrentSyncID(), scGrant("member", "target", true))
	rec.SetSourceScopeKey("scope-original")
	rec.SetDiscoveredAt(timestamppb.New(time.Unix(1234, 0)))
	return rec
}

func TestPageExpandedMatchesDirectPreservation(t *testing.T) {
	for _, scenario := range []string{"existing", "new", "nil-time"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := t.Context()
			engines := make([]*Engine, 2)
			for i := range engines {
				engines[i], _ = newTestEngine(t)
				_, err := engines[i].StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				seed := pageExpandedSeed(engines[i])
				if scenario == "nil-time" {
					seed.SetDiscoveredAt(nil)
				}
				if scenario != "new" {
					require.NoError(t, engines[i].PutGrantRecords(ctx, seed))
				}
			}
			rewrite := scGrant("member", "target", true)
			direct, paged := engines[0], engines[1]
			require.NoError(t, (pebbleGrantStore{e: direct}).StoreExpandedGrants(ctx, rewrite))
			w := paged.Ledger().BeginPage()
			require.NoError(t, w.StoreExpandedGrants(ctx, rewrite))
			require.NoError(t, w.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "expand"}, nil))
			want, err := direct.GetGrantRecord(ctx, rewrite.GetId())
			require.NoError(t, err)
			got, err := paged.GetGrantRecord(ctx, rewrite.GetId())
			require.NoError(t, err)
			require.NotNil(t, got.GetDiscoveredAt())
			if scenario == "existing" {
				require.True(t, proto.Equal(want.GetDiscoveredAt(), got.GetDiscoveredAt()))
			}
			got.SetDiscoveredAt(want.GetDiscoveredAt())
			require.True(t, proto.Equal(want, got), "direct=%v page=%v", want, got)
			require.True(t, paged.db.DeferredIdxPending())
			require.Equal(t, countKeys(t, direct, GrantByNeedsExpansionLowerBound()), countKeys(t, paged, GrantByNeedsExpansionLowerBound()))
			require.Equal(t, countKeys(t, direct, GrantBySourceScopeLowerBound()), countKeys(t, paged, GrantBySourceScopeLowerBound()))
			poisoned, err := paged.SourceCachePoisoned(ctx, "grants", "scope-original")
			require.NoError(t, err)
			require.False(t, poisoned)
			require.NoError(t, paged.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
			require.False(t, paged.db.DeferredIdxPending())
			count := 0
			require.NoError(t, paged.IterateGrantsByPrincipal(ctx, "user", "target", func(*v3.GrantRecord) bool { count++; return true }))
			require.Equal(t, 1, count)
		})
	}
}

func TestPageExpandedWriteOrder(t *testing.T) {
	for _, order := range []string{"ordinary-expanded", "expanded-ordinary", "expanded-expanded", "delete"} {
		t.Run(order, func(t *testing.T) {
			ctx := t.Context()
			e, _ := newTestEngine(t)
			_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			w := e.Ledger().BeginPage()
			normal := scGrant("member", "target", true)
			first := scGrant("member", "target", true)
			last := scGrant("member", "target", false)
			last.SetAnnotations([]*anypb.Any{{TypeUrl: "test/last", Value: []byte("last")}})
			switch order {
			case "ordinary-expanded":
				require.NoError(t, w.PutGrants(ctx, normal))
				require.NoError(t, w.StoreExpandedGrants(ctx, first, last))
			case "expanded-ordinary":
				require.NoError(t, w.StoreExpandedGrants(ctx, first))
				require.NoError(t, w.PutGrants(ctx, normal))
			case "expanded-expanded":
				require.NoError(t, w.StoreExpandedGrants(ctx, first))
				require.NoError(t, w.StoreExpandedGrants(ctx, last))
			case "delete":
				require.NoError(t, w.DeleteGrants(ctx, last))
				require.NoError(t, w.StoreExpandedGrants(ctx, first, last))
			}
			id := c1zstore.LedgerActionIdentity{Op: "expand"}
			require.NoError(t, w.Commit(ctx, id, nil))
			row, found, err := e.Ledger().GetRow(ctx, id)
			require.NoError(t, err)
			require.True(t, found)
			rec, err := e.GetGrantRecord(ctx, normal.GetId())
			if order == "delete" {
				require.ErrorIs(t, err, pebble.ErrNotFound)
				require.Zero(t, row.GrantsWritten)
				return
			}
			require.NoError(t, err)
			require.Equal(t, uint64(1), row.GrantsWritten)
			require.Equal(t, order != "expanded-expanded", rec.GetNeedsExpansion())
			if order != "expanded-ordinary" {
				require.Equal(t, "test/last", rec.GetAnnotations()[0].GetTypeUrl())
			}
			require.ErrorIs(t, w.StoreExpandedGrants(ctx, last), ErrPageUnitCommitted)
		})
	}
}

func TestPageExpandedFailureRetryUsesCurrentPrior(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	seed := pageExpandedSeed(e)
	require.NoError(t, e.PutGrantRecords(ctx, seed))
	w := e.Ledger().BeginPage()
	defer w.Discard()
	rewrite := scGrant("member", "target", false)
	require.NoError(t, w.StoreExpandedGrants(ctx, rewrite))
	seed.SetDiscoveredAt(timestamppb.New(time.Unix(2345, 0)))
	require.NoError(t, e.PutGrantRecords(ctx, seed))
	injected := errors.New("expanded page commit failed")
	e.db.SetRecordCommitTestHook(func() error { return injected })
	id := c1zstore.LedgerActionIdentity{Op: "expand"}
	require.ErrorIs(t, w.Commit(ctx, id, nil), injected)
	e.db.SetRecordCommitTestHook(nil)
	got, err := e.GetGrantRecord(ctx, rewrite.GetId())
	require.NoError(t, err)
	require.True(t, proto.Equal(seed, got))
	_, found, err := e.Ledger().GetRow(ctx, id)
	require.NoError(t, err)
	require.False(t, found)
	seed.SetDiscoveredAt(timestamppb.New(time.Unix(3456, 0)))
	seed.SetNeedsExpansion(false)
	seed.SetExpansion(nil)
	require.NoError(t, e.PutGrantRecords(ctx, seed))
	require.NoError(t, w.Commit(ctx, id, nil))
	got, err = e.GetGrantRecord(ctx, rewrite.GetId())
	require.NoError(t, err)
	require.True(t, proto.Equal(seed, got))
	require.Zero(t, countKeys(t, e, GrantByNeedsExpansionLowerBound()))
}

func TestPageExpandedFullIdentityAndWriterLifetime(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	w := e.Ledger().BeginPage()
	require.ErrorIs(t, w.StoreExpandedGrants(ctx), ErrNoCurrentSync)
	w.Discard()
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	seed := pageExpandedSeed(e)
	require.NoError(t, e.PutGrantRecords(ctx, seed))
	other := scGrant("member", "other", true)
	other.SetId(seed.GetExternalId())
	w = e.Ledger().BeginPage()
	require.NoError(t, w.StoreExpandedGrants(ctx, nil, other))
	require.NoError(t, w.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "expand"}, nil))
	id, err := grantIdentityFromRecord(seed)
	require.NoError(t, err)
	value, closer, err := e.db.Get(encodeGrantIdentityKey(id))
	require.NoError(t, err)
	var got v3.GrantRecord
	require.NoError(t, unmarshalRecord(value, &got))
	require.NoError(t, closer.Close())
	require.True(t, proto.Equal(seed, &got))
	otherID, err := grantIdentityFromRecord(V2GrantToV3(e.CurrentSyncID(), other))
	require.NoError(t, err)
	value, closer, err = e.db.Get(encodeGrantIdentityKey(otherID))
	require.NoError(t, err)
	got.Reset()
	require.NoError(t, unmarshalRecord(value, &got))
	require.NoError(t, closer.Close())
	require.False(t, got.GetNeedsExpansion())
	require.Nil(t, got.GetExpansion())
	require.Empty(t, got.GetSourceScopeKey())
	recs, _, err := e.PaginateGrantsByNeedsExpansion(ctx, "", 100)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	w = e.Ledger().BeginPage()
	require.NoError(t, w.StoreExpandedGrants(ctx, &v2.Grant{}))
	require.Error(t, w.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "invalid"}, nil))
	w.Discard()
	require.ErrorIs(t, w.StoreExpandedGrants(ctx, other), ErrPageUnitCommitted)
	w = e.Ledger().BeginPage()
	require.NoError(t, w.StoreExpandedGrants(ctx, other))
	require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.ErrorIs(t, w.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "old"}, nil), ErrPageUnitForeignSync)
	w.Discard()
}
