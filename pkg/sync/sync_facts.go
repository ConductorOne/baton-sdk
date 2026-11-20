package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Fact names, spelled as the sync token's JSON field names.
//
// The values are inert here: syncFacts uses them only as map keys, and the
// token's wire names come from the struct tags on serializedTokenV0/V1. The
// match is a convention this package does not depend on, so
// TestFactNamesMatchTokenJSONNames checks it rather than leaving it to the eye.
//
// CXE-1358 is what makes the values durable, storing facts under these names
// in the page ledger's append-only fact keyspace. Renaming one after that
// reads as a fact the artifact never established.
const (
	factNeedsExpansion                  = "needs_expansion"
	factHasExternalResourceGrants       = "has_external_resource_grants"
	factShouldFetchRelatedResources     = "should_fetch_related_resources"
	factShouldSkipEntitlementsAndGrants = "should_skip_entitlements_and_grants"
	factShouldSkipGrants                = "should_skip_grants"
)

// syncFacts is the set of sync-level facts a run has established. A fact is
// set by whatever observed it — a grant carrying an expandable annotation
// sets needs_expansion — and is not cleared: a resume that re-observes it
// writes the same value, and a resume that does not still needs the fact the
// earlier attempt established.
//
// It carries no mutex. It lives inside runState because facts are part of
// what a resume needs, and runState's mutex guards it; callers hold that
// mutex.
type syncFacts struct {
	established map[string]bool
}

func (f *syncFacts) set(name string) {
	if f.established == nil {
		f.established = make(map[string]bool)
	}
	f.established[name] = true
}

func (f *syncFacts) has(name string) bool {
	return f.established[name]
}
