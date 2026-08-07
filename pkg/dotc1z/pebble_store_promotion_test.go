package dotc1z

import (
	"reflect"
	"strings"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/stretchr/testify/require"
)

// guardedMutationWrappers is the set of pebbleStore methods that are allowed
// to mutate, and it is the single source of truth for both directions of the
// guard:
//
//   - TestPebbleStoreMutationWrapperInventory proves, via the AST, that each
//     name here actually calls withMutation. Adding a name without wiring the
//     guard fails there.
//   - TestPebbleStoreDoesNotPromoteEngineMutators treats this as the allowlist
//     of mutators permitted on the store. Exposing a mutator without adding it
//     here fails there.
//
// Keeping one list means neither test can be satisfied by editing the other's
// copy, which is what a second hand-maintained list would eventually invite.
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

// mutatingEnginePrefixes names the verbs the engine uses for methods that
// write. A method matching one of these must never become reachable as a
// pebbleStore method except through a withMutation wrapper.
//
// This is a heuristic and it is the weaker half of the guard. A mutator named
// outside this list is not detected, so do not treat a pass as proof that the
// store exposes no unguarded writes. The structural protection is that the
// engine is a named field: the store's method set is closed to what
// pebble_store.go and pebble_store_reads.go declare, and this list is the net
// under anyone hand-writing a passthrough. Widen it when the engine gains a
// mutating verb that is not already here.
var mutatingEnginePrefixes = []string{
	"Abort", "Add", "Apply", "Begin", "Build", "Checkpoint", "Clear", "Commit",
	"Compact", "Delete", "Drop", "End", "Ensure", "Finish", "Flush", "Ingest",
	"Init", "Invalidate", "Mark", "Merge", "Migrate", "New", "Persist", "Prune",
	"Purge", "Put", "Repair", "Replace", "Reset", "Resume", "Rollback", "Save",
	"Seal", "Set", "Stash", "Start", "Store", "Truncate", "Unsafe", "Upsert",
	"Wipe", "Write",
}

func isMutatingEngineMethod(name string) bool {
	for _, prefix := range mutatingEnginePrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// TestPebbleStoreDoesNotPromoteEngineMutators is the reverse of
// TestPebbleStoreMutationWrapperInventory. That test asks whether each
// wrapper we already know about calls withMutation, so it can only fail for
// a method someone remembered to list. This one asks the opposite question —
// which engine mutators are reachable on the store at all — and therefore
// fails for a method nobody thought about.
//
// It exists because pebbleStore used to embed *pebble.Engine. Embedding
// promoted every engine method onto the store, so an engine mutator was
// callable as s.PutSyncRunRecord(...) with no admission, no dirty bit, and
// nothing in the AST inventory able to see it. The embedding is now a named
// field; this test is what keeps it from quietly coming back, whether by
// re-embedding or by adding a hand-written passthrough.
//
// A store method matching a mutating verb is allowed only if it is a
// deliberate guarded wrapper, which the inventory test separately proves
// calls withMutation. Anything else — in particular a name that exists on
// the engine and is NOT in that inventory — means a mutator escaped.
func TestPebbleStoreDoesNotPromoteEngineMutators(t *testing.T) {
	engineMutators := make(map[string]bool)
	engineType := reflect.TypeOf((*pebble.Engine)(nil))
	for i := 0; i < engineType.NumMethod(); i++ {
		name := engineType.Method(i).Name
		if isMutatingEngineMethod(name) {
			engineMutators[name] = true
		}
	}
	require.NotEmpty(t, engineMutators, "fixture: expected the engine to expose mutating methods")

	guarded := make(map[string]bool, len(guardedMutationWrappers))
	for _, name := range guardedMutationWrappers {
		guarded[name] = true
	}

	storeType := reflect.TypeOf((*pebbleStore)(nil))
	var escaped []string
	for i := 0; i < storeType.NumMethod(); i++ {
		name := storeType.Method(i).Name
		if !engineMutators[name] || guarded[name] {
			continue
		}
		escaped = append(escaped, name)
	}

	require.Emptyf(t, escaped, "engine mutators reachable on pebbleStore without a guarded wrapper: %v.\n"+
		"Either route the method through withMutation and add it to the guarded list here and to "+
		"TestPebbleStoreMutationWrapperInventory, or stop exposing it on the store. Do not re-embed "+
		"*pebble.Engine: that promotes every mutator at once.", escaped)
}

// TestPebbleStoreEngineIsNotEmbedded pins the structural precondition the
// test above depends on. Re-embedding would promote the whole engine surface
// again, and a promoted method is indistinguishable from a declared one by
// reflection alone, so assert on the field itself.
func TestPebbleStoreEngineIsNotEmbedded(t *testing.T) {
	storeType := reflect.TypeOf(pebbleStore{})
	field, ok := storeType.FieldByName("Engine")
	require.True(t, ok, "pebbleStore should keep its engine in a field named Engine")
	require.False(t, field.Anonymous,
		"pebbleStore must not embed *pebble.Engine: embedding promotes every engine mutator onto the "+
			"store, bypassing withMutation. Keep it a named field and forward reads explicitly in "+
			"pebble_store_reads.go.")
}
