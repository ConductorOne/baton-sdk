package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// storeCapsMatrix is the engine capability matrix: for each engine, whether
// resolveStoreCaps finds each optional capability on a real store.
//
// Every failure mode of this resolution is silent. A capability that resolves
// nil when the engine does implement it does not error — it downgrades:
// unlayered grant expansion, the per-grant delete loop instead of
// DeleteGrantsByRefs, no preserved entitlement graph, no ingest-invariant
// verification marker. Nothing else in the suite would notice, so the matrix
// is asserted here rather than described in field comments.
//
// Adding a field to storeCaps without adding it here fails the test.
var storeCapsMatrix = map[c1zstore.Engine]map[string]bool{
	c1zstore.EnginePebble: {
		"pageLedger":               true,
		"writeHook":                true,
		"entitlementGraph":         true,
		"grantDigest":              true,
		"ingestFacts":              true,
		"ingestVerification":       true,
		"dbSize":                   true,
		"resourceDeleter":          true,
		"entitlementDeleter":       true,
		"grantRefsDeleter":         true,
		"grantBatchDeleter":        true,
		"newExpandedGrants":        true,
		"newExpandedContributions": true,
		"expandedGrantLayer":       true,
	},
}

func TestStoreCapsEngineMatrix(t *testing.T) {
	for engine, expected := range storeCapsMatrix {
		t.Run(string(engine), func(t *testing.T) {
			ctx := context.Background()
			store, err := dotc1z.NewStore(ctx,
				filepath.Join(t.TempDir(), "caps.c1z"),
				dotc1z.WithEngine(engine))
			require.NoError(t, err)
			defer store.Close(ctx)
			_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)

			// VisibleFields flattens the embedded readerCaps so the matrix
			// is one flat map over every capability; the embedded struct
			// itself is skipped as a non-capability.
			caps := reflect.ValueOf(resolveStoreCaps(store))
			var seen int
			for _, sf := range reflect.VisibleFields(caps.Type()) {
				if sf.Anonymous {
					continue
				}
				seen++
				field := caps.FieldByIndex(sf.Index)
				require.Equal(t, reflect.Interface, field.Kind(),
					"storeCaps.%s is not an interface; this test assumes capabilities are nil-able interfaces", sf.Name)
				want, listed := expected[sf.Name]
				require.True(t, listed, "storeCaps.%s is missing from the engine matrix; add it", sf.Name)
				require.Equal(t, want, !field.IsNil(), "storeCaps.%s on %s", sf.Name, engine)
			}
			require.Equal(t, len(expected), seen,
				"the matrix must list every storeCaps field exactly once")

			// The reader-side resolver sees the same store through a
			// narrower type and must agree with the embedded subset.
			require.Equal(t, resolveStoreCaps(store).readerCaps, resolveReaderCaps(store),
				"resolveReaderCaps must match the readerCaps embedded by resolveStoreCaps on %s", engine)
		})
	}
}

// TestResolveStoreCapsNilStore pins the guards: a nil store, and a store
// whose sub-stores are nil, resolve to an all-nil capability set rather than
// panicking. Partial test doubles depend on this.
func TestResolveStoreCapsNilStore(t *testing.T) {
	require.Equal(t, storeCaps{}, resolveStoreCaps(nil))

	caps := resolveStoreCaps(nilSubStoreStore{})
	require.Nil(t, caps.ingestVerification)
	require.Nil(t, caps.expandedGrantLayer)
	require.Nil(t, caps.newExpandedGrants)
	require.Nil(t, caps.newExpandedContributions)
}

// nilSubStoreStore is a store whose SyncMeta and Grants sub-stores are nil,
// the shape partial doubles take.
type nilSubStoreStore struct {
	c1zstore.Store
}

func (nilSubStoreStore) SyncMeta() c1zstore.SyncMeta { return nil }
func (nilSubStoreStore) Grants() c1zstore.GrantStore { return nil }
