package expand

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type ledgerBoundaryRecorder struct {
	ExpanderStore
	batches [][]*v2.Grant
	limit   int
}

func (s *ledgerBoundaryRecorder) StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	if s.limit >= 0 && len(s.batches) == s.limit {
		return errStoreInterrupted
	}
	if err := s.ExpanderStore.StoreExpandedGrants(ctx, grants...); err != nil {
		return err
	}
	batch := make([]*v2.Grant, len(grants))
	for i, grant := range grants {
		batch[i] = proto.Clone(grant).(*v2.Grant)
	}
	s.batches = append(s.batches, batch)
	return nil
}

func TestLedgerExpansionFlushQualification(t *testing.T) {
	priorChunk := expansionDirtyFlushChunk
	expansionDirtyFlushChunk = 2
	t.Cleanup(func() { expansionDirtyFlushChunk = priorChunk })
	cases := append(parityCases(), cyclicCases()...)
	multi := parityCases()[0]
	multi.name = "multi_chunk_chain"
	multi.grants = nil
	for i := range 7 {
		multi.grants = append(multi.grants, sqliteGrantSpec{id: fmt.Sprintf("grant:%d", i), entitlementID: "ent:a", principalRT: "user", principalID: fmt.Sprintf("person-%d", i)})
	}
	cases = append(cases, multi)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			newStore := func() c1zstore.Store {
				store, err := dotc1z.NewStore(t.Context(), filepath.Join(t.TempDir(), "boundary.c1z"), dotc1z.WithEngine(c1zstore.EnginePebble))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
				_, err = store.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				seedSQLiteBaseData(t, t.Context(), store, tc)
				return store
			}
			referenceStore := newStore()
			reference := &ledgerBoundaryRecorder{ExpanderStore: benchmarkExpanderStore{store: referenceStore}, limit: -1}
			graph := buildGraphFromCase(t, t.Context(), tc, c1zstore.EnginePebble)
			require.NoError(t, NewExpander(reference, graph).RunTopologicalMergeProjection(t.Context()))
			want := snapshotOpenStoreGrants(t, t.Context(), referenceStore)
			for cut := 0; cut <= len(reference.batches); cut++ {
				t.Run(fmt.Sprintf("cut-%d", cut), func(t *testing.T) {
					store := newStore()
					recorder := &ledgerBoundaryRecorder{ExpanderStore: benchmarkExpanderStore{store: store}, limit: cut}
					graph := buildGraphFromCase(t, t.Context(), tc, c1zstore.EnginePebble)
					err := NewExpander(recorder, graph).RunTopologicalMergeProjection(t.Context())
					if cut < len(reference.batches) {
						require.ErrorIs(t, err, errStoreInterrupted)
					} else {
						require.NoError(t, err)
					}
					recorder.limit = -1
					graph = buildGraphFromCase(t, t.Context(), tc, c1zstore.EnginePebble)
					require.NoError(t, NewExpander(recorder, graph).RunTopologicalMergeProjection(t.Context()))
					assertStoreSnapshotsEqual(t, want, snapshotOpenStoreGrants(t, t.Context(), store), tc.name)
					require.Len(t, recorder.batches, len(reference.batches), "flush count changes after resume")
					for i, batch := range reference.batches {
						require.Len(t, recorder.batches[i], len(batch), "flush %d changes size after resume", i)
						for j, grant := range batch {
							require.Truef(t, proto.Equal(grant, recorder.batches[i][j]), "flush %d grant %d changes after resume", i, j)
						}
					}
					t.Logf("committed_batches=%d final_batches=%d", cut, len(recorder.batches))
				})
			}
		})
	}
}
