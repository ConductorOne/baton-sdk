package pebble

import (
	"context"
	"errors"
	"fmt"
	"testing"

	cockroachpebble "github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

type scopedDeleteVerificationCase struct {
	name          string
	commitKind    string
	primaryPrefix []byte
	indexPrefix   []byte
	prepare       func(*testing.T, *Adapter) func(context.Context) (int64, error)
}

// C10/C12/C15/C16: scoped tombstones commit in bounded chunks. If a later
// chunk fails, the method reports only rows whose deletes landed, preserves
// primary/index agreement, and the exact retry converges.
func TestVerificationScopedDeleteBatchBoundAndInterruptedRetry(t *testing.T) {
	const rows = 5
	cases := []scopedDeleteVerificationCase{
		{
			name:          "grant-principals",
			commitKind:    "grant-principals",
			primaryPrefix: encodeGrantPrefix(),
			indexPrefix:   GrantBySourceScopeLowerBound(),
			prepare: func(t *testing.T, a *Adapter) func(context.Context) (int64, error) {
				grants := make([]*v2.Grant, 0, rows)
				for i := range rows {
					grants = append(grants, scGrant(fmt.Sprintf("member-%d", i), "alice", false))
				}
				require.NoError(t, a.PutGrants(sourcecache.WithScope(t.Context(), "scope-a"), grants...))
				return func(ctx context.Context) (int64, error) {
					return a.PebbleEngine().DeleteGrantsByPrincipalsInScope(
						ctx,
						"scope-a",
						map[string]struct{}{"alice": {}},
					)
				}
			},
		},
		{
			name:          "grant-external-ids",
			commitKind:    "grant-external-ids",
			primaryPrefix: encodeGrantPrefix(),
			indexPrefix:   GrantBySourceScopeLowerBound(),
			prepare: func(t *testing.T, a *Adapter) func(context.Context) (int64, error) {
				grants := make([]*v2.Grant, 0, rows)
				ids := make(map[string]struct{}, rows)
				for i := range rows {
					grant := scGrant(fmt.Sprintf("member-%d", i), fmt.Sprintf("user-%d", i), false)
					grants = append(grants, grant)
					ids[grant.GetId()] = struct{}{}
				}
				require.NoError(t, a.PutGrants(sourcecache.WithScope(t.Context(), "scope-a"), grants...))
				return func(ctx context.Context) (int64, error) {
					return a.PebbleEngine().DeleteGrantsByExternalIDsInScope(ctx, "scope-a", ids)
				}
			},
		},
		{
			name:          "resources",
			commitKind:    "resources",
			primaryPrefix: encodeResourcePrefix(),
			indexPrefix:   ResourceBySourceScopeLowerBound(),
			prepare: func(t *testing.T, a *Adapter) func(context.Context) (int64, error) {
				records := make([]*v3.ResourceRecord, 0, rows)
				for i := range rows {
					records = append(records, v3.ResourceRecord_builder{
						ResourceTypeId: fmt.Sprintf("type-%d", i),
						ResourceId:     "shared",
						SourceScopeKey: "scope-a",
					}.Build())
				}
				require.NoError(t, a.PebbleEngine().PutResourceRecords(t.Context(), records...))
				return func(ctx context.Context) (int64, error) {
					return a.PebbleEngine().DeleteResourcesByIDsInScope(
						ctx,
						"scope-a",
						map[string]struct{}{"shared": {}},
					)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			a := newAdapter(t)
			_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			remove := tc.prepare(t, a)
			require.Equal(t, rows, countKeys(t, a.PebbleEngine(), tc.primaryPrefix))
			require.Equal(t, rows, countKeys(t, a.PebbleEngine(), tc.indexPrefix))

			a.PebbleEngine().test.sourceCacheDeleteBatchRows = 2
			injected := errors.New("verification scoped delete commit failure")
			commitCalls := 0
			a.PebbleEngine().test.sourceCacheDeleteCommitHook = func(kind string, batchRows int, _ bool) error {
				require.Equal(t, tc.commitKind, kind)
				require.LessOrEqual(t, batchRows, 2)
				commitCalls++
				if commitCalls == 2 {
					return injected
				}
				return nil
			}

			deleted, err := remove(ctx)
			require.ErrorIs(t, err, injected)
			require.Equal(t, int64(2), deleted, "only the first committed chunk may be reported")
			require.Equal(t, rows-2, countKeys(t, a.PebbleEngine(), tc.primaryPrefix))
			require.Equal(t, rows-2, countKeys(t, a.PebbleEngine(), tc.indexPrefix))
			require.NoError(t, auditSourceScopeBiconditional(a.PebbleEngine()))

			a.PebbleEngine().test.sourceCacheDeleteCommitHook = nil
			deleted, err = remove(ctx)
			require.NoError(t, err)
			require.Equal(t, int64(rows-2), deleted)
			require.Zero(t, countKeys(t, a.PebbleEngine(), tc.primaryPrefix))
			require.Zero(t, countKeys(t, a.PebbleEngine(), tc.indexPrefix))
			require.NoError(t, auditSourceScopeBiconditional(a.PebbleEngine()))
		})
	}
}

// A Pebble batch returns to a process-global pool when closed. The helper must
// relinquish its pointer immediately after the final commit so deferred cleanup
// cannot close a batch that another engine has since acquired from that pool.
func TestVerificationScopedDeleteBatchFinalCloseOwnership(t *testing.T) {
	ctx := t.Context()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	grant := scGrant("member", "alice", false)
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-a"), grant))
	rec, err := a.PebbleEngine().GetGrantRecord(ctx, grant.GetId())
	require.NoError(t, err)
	id, err := grantIdentityFromRecord(rec)
	require.NoError(t, err)

	deletes := newSourceCacheDeleteBatch(a.PebbleEngine(), "ownership", cockroachpebble.NoSync)
	require.NoError(t, deletes.batch.StageSourceScopeOrphanIndexDelete(
		encodeGrantBySourceScopeIndexKey("scope-a", id),
	))
	require.NoError(t, deletes.staged(false))
	require.NoError(t, deletes.commit(true))
	require.Nil(t, deletes.batch, "final commit must relinquish ownership of the pooled batch")

	require.NotPanics(t, deletes.close)
	require.Nil(t, deletes.batch)
	require.NotPanics(t, deletes.close, "deferred cleanup must be idempotent after final commit")
}
