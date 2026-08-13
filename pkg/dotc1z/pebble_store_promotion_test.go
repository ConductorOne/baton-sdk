package dotc1z

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// guardedMutationWrappers is the set of pebbleStore methods that mutate.
// TestPebbleStoreMutationWrapperInventory proves, via the AST, that every
// declaration bearing one of these names calls withMutation, so a wrapper
// that stops taking admission fails there rather than at runtime.
//
// What this list is not: proof that the store exposes no other writes. It
// covers the wrappers we know about. The store's method set being closed —
// the engine is a named field, not embedded, so nothing is promoted — is what
// keeps the unknown set small enough to review; see
// TestPebbleStoreEngineIsNotEmbedded.
var guardedMutationWrappers = []string{
	"GenerateSyncDiff", "MarkSyncSupportsDiff", "MarkIngestInvariantsVerified",
	"ClearIngestInvariantVerification", "RecalculateStats", "NormalizeForFixtureSave",
	"StartNewSync", "StartNewSyncWithID", "ResumeSync", "StartOrResumeSync",
	"SetCurrentSync", "CheckpointSync", "EndSync", "PutAsset",
	"SetSupportsDiff", "SetSyncLink", "PutGrants", "UnsafePutUniqueGrants",
	"PutResourceTypes", "PutResources", "PutEntitlements", "DeleteGrant",
	"DeleteGrantByRefs", "DeleteResourceRecord", "DeleteEntitlementByRefs",
	"StoreExpandedGrants", "StoreNewExpandedGrants", "StoreNewExpandedGrantContributions",
	"BeginExpandedGrantLayer", "AddExpandedGrantLayerContributions",
	"FinishExpandedGrantLayer", "AbortExpandedGrantLayer",
	"Set", "SetMany", "Delete", "Clear", "EnsureGrantIndexes",
}

// TestPebbleStoreEngineIsNotEmbedded pins what actually keeps engine writes
// off the store: the engine is reached through a named field, so the store's
// method set is exactly what this package declares. Re-embedding would
// promote all of it at once — every engine mutator callable as s.PutX(...)
// with no admission and no dirty bit — and a promoted method is
// indistinguishable from a declared one by reflection, so assert on the field
// itself rather than trying to recognize the damage afterwards.
func TestPebbleStoreEngineIsNotEmbedded(t *testing.T) {
	storeType := reflect.TypeOf(pebbleStore{})
	field, ok := storeType.FieldByName("Engine")
	require.True(t, ok, "pebbleStore should keep its engine in a field named Engine")
	require.False(t, field.Anonymous,
		"pebbleStore must not embed *pebble.Engine: embedding promotes every engine mutator onto the "+
			"store, bypassing withMutation. Keep it a named field and forward reads explicitly in "+
			"pebble_store_reads.go.")
}
