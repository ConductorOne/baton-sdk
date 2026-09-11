package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// factTokenFields maps each fact name to the token struct field
// marshalToken/unmarshalToken translate it through. Adding a fact means
// adding a row here.
var factTokenFields = map[string]string{
	factNeedsExpansion:                  "NeedsExpansion",
	factHasExternalResourceGrants:       "HasExternalResourceGrants",
	factShouldFetchRelatedResources:     "ShouldFetchRelatedResources",
	factShouldSkipEntitlementsAndGrants: "ShouldSkipEntitlementsAndGrants",
	factShouldSkipGrants:                "ShouldSkipGrants",
}

// TestFactNamesMatchTokenJSONNames pins the five fact name VALUES.
//
// The values are inert in this package: syncFacts uses them only as map
// keys, and the token's wire names come from the struct tags on
// serializedTokenV0/V1, not from these constants. So editing a value
// today changes no bytes and no behavior, and no existing test notices.
//
// That stops being true in CXE-1358, which stores facts under these
// names in the page ledger's fact keyspace. That keyspace is
// append-only: a renamed value would read as a fact the artifact never
// established, so a resume would silently redo or skip work rather than
// fail. Pinning the values against the token's JSON names — which the
// golden corpus in testdata/tokens already holds still — keeps the two
// spellings from drifting before the ledger makes the drift durable.
func TestFactNamesMatchTokenJSONNames(t *testing.T) {
	require.Len(t, factTokenFields, 5, "a fact was added or removed without updating factTokenFields")

	for _, tokenType := range []reflect.Type{
		reflect.TypeOf(serializedTokenV0{}),
		reflect.TypeOf(serializedTokenV1{}),
	} {
		t.Run(tokenType.Name(), func(t *testing.T) {
			for factName, fieldName := range factTokenFields {
				field, ok := tokenType.FieldByName(fieldName)
				require.True(t, ok, "%s has no field %s (fact %q)", tokenType.Name(), fieldName, factName)
				jsonName := strings.Split(field.Tag.Get("json"), ",")[0]
				require.Equal(t, factName, jsonName,
					"fact %q and %s.%s's JSON name %q disagree; CXE-1358 stores facts under the fact name in an append-only keyspace",
					factName, tokenType.Name(), fieldName, jsonName)
			}
		})
	}
}

// TestFactNamesAreDistinct catches a copy-paste that points two facts at
// one key. syncFacts is a map, so the duplicate would silently make the
// two facts the same bit rather than fail anywhere.
func TestFactNamesAreDistinct(t *testing.T) {
	facts := []string{
		factNeedsExpansion,
		factHasExternalResourceGrants,
		factShouldFetchRelatedResources,
		factShouldSkipEntitlementsAndGrants,
		factShouldSkipGrants,
	}
	seen := make(map[string]bool, len(facts))
	for _, f := range facts {
		require.NotEmpty(t, f, "a fact name is empty")
		require.False(t, seen[f], "fact name %q is used twice; two facts would share one bit", f)
		seen[f] = true
	}
	require.Len(t, seen, len(facts))
}
