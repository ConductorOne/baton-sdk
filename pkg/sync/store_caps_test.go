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
		"entitlementGraph":         true,
		"grantDigest":              true,
		"ingestFacts":              true,
		"ingestVerification":       true,
		"dbSize":                   true,
		"resourceDeleter":          true,
		"entitlementDeleter":       true,
		"grantRefsDeleter":         true,
		"grantBatchDeleter":        true,
		"grantPrincipalKeys":       true,
		"principalSortedGrants":    true,
		"newExpandedGrants":        true,
		"newExpandedContributions": true,
		"expandedGrantLayer":       true,
	},
	c1zstore.EngineSQLite: {
		"entitlementGraph":   false,
		"grantDigest":        false,
		"ingestFacts":        false,
		"ingestVerification": true,
		"dbSize":             true,
		// SQLite deletes by public grant id and implements none of the
		// record-level or refs-based deletes.
		"resourceDeleter":    false,
		"entitlementDeleter": false,
		"grantRefsDeleter":   false,
		"grantBatchDeleter":  false,
		"grantPrincipalKeys": false,
		// Present but answers false — see the effective-answer assertion
		// below. Satisfying the interface is not the same as the capability.
		"principalSortedGrants":    true,
		"newExpandedGrants":        false,
		"newExpandedContributions": false,
		"expandedGrantLayer":       false,
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

			caps := reflect.ValueOf(resolveStoreCaps(store))
			require.Equal(t, caps.NumField(), len(expected),
				"the matrix must list every storeCaps field exactly once")
			for i := range caps.NumField() {
				name := caps.Type().Field(i).Name
				field := caps.Field(i)
				require.Equal(t, reflect.Interface, field.Kind(),
					"storeCaps.%s is not an interface; this test assumes capabilities are nil-able interfaces", name)
				want, listed := expected[name]
				require.True(t, listed, "storeCaps.%s is missing from the engine matrix; add it", name)
				require.Equal(t, want, !field.IsNil(), "storeCaps.%s on %s", name, engine)
			}

			// Interface satisfaction is not the answer. SQLite implements
			// GrantsForEntitlementPrincipalSorted and returns false, so the
			// non-nil capability above must not be read as "grants are
			// sorted" — the expander's buffering fallback depends on this.
			require.Equal(t, engine == c1zstore.EnginePebble,
				NewExpanderStore(store).GrantsForEntitlementPrincipalSorted(),
				"effective principal-sort answer on %s", engine)

			// The reader-side resolver sees the same store through a
			// narrower type and must agree on the capabilities it covers.
			readerCaps := resolveReaderCaps(store)
			require.Equal(t, expected["entitlementGraph"], readerCaps.entitlementGraph != nil)
			require.Equal(t, expected["grantDigest"], readerCaps.grantDigest != nil)
			require.Equal(t, expected["ingestFacts"], readerCaps.ingestFacts != nil)
			require.Equal(t, expected["dbSize"], readerCaps.dbSize != nil)
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
