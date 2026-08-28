package chaosconnector

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	"google.golang.org/protobuf/proto"
)

// Source-cache replay support (Phase 6b — see
// docs/verification/sync-replay-6b/plan.md, "Harness" in the placement map).
//
// A scenario opts a scope into source-cache behavior by declaring a
// SourceCacheSpec for the serve scope alongside its page graph. At serve
// time the reference connector consults the SDK-provided lookup
// (SyncOpAttrs.Lookup) exactly the way a real capable connector would:
// lookup the previous validator for the scope, compare it to the current
// epoch's validator, and serve either the warm branch (a declared alternate
// root page, typically carrying a SourceCacheReplay annotation) or the cold
// branch (the normal root, typically carrying SourceCacheRecord
// annotations). Every consult is recorded as a SourceCacheLookupEvent so
// suites can assert hit/miss/served-warm exactly (oracle OR1, OR5).
//
// The page annotations themselves are declared data (Page.Annotations),
// never synthesized here: adversarial suites need full control over shape,
// including invalid ones.

// SourceCacheSpec drives the reference connector's source-cache behavior
// for one serve scope (the key of the Resources/Entitlements/Grants page
// maps). The connector consults the lookup only on the scope's ROOT request
// (empty page token); non-root tokens address whichever branch's pages.
type SourceCacheSpec struct {
	// ScopeKey is the connector-declared source-cache scope key passed to
	// the lookup and (by convention) carried in the branch pages'
	// SourceCacheRecord / SourceCacheReplay annotations.
	ScopeKey string

	// Validator is the current epoch's validator for this scope. A lookup
	// hit whose stored validator equals it means "upstream unchanged":
	// serve the warm branch. A miss or a different (stale) validator means
	// fetch fresh: serve the cold branch.
	Validator string

	// WarmRoot is the page token served in place of the root when the
	// lookup hit matched Validator. Empty means the scope declares no warm
	// branch and always serves cold (lookup outcomes are still recorded).
	WarmRoot string
}

// SourceCacheLookupEvent is one connector-side lookup consult, in serve
// order. It is the instrument for gate conformance (a cold sync shows
// Hit=false everywhere) and generational fetch accounting (ServedWarm=false
// on a spec-bearing scope is a fresh fetch).
type SourceCacheLookupEvent struct {
	RowKind  sourcecache.RowKind
	ScopeKey string
	// Hit is the lookup's found result.
	Hit bool
	// PreviousValidator is the stored validator when Hit.
	PreviousValidator string
	// Matched reports Hit && PreviousValidator == spec.Validator.
	Matched bool
	// ServedWarm reports that the warm branch root replaced the cold root.
	ServedWarm bool
	// LookupError is non-empty when the lookup returned an error (the
	// package contract says implementations return misses instead; a
	// non-empty value is itself a finding).
	LookupError string
	// LookupWasNil reports that SyncOpAttrs.Lookup arrived nil. The
	// contract says connectors never observe nil (NoopLookup substitutes);
	// true is itself a finding (criterion R1).
	LookupWasNil bool
}

// SetSourceCacheCapability arms (or, with nil, disarms) the capability
// annotation the reference connector attaches to its Validate response.
// Mutable run state on purpose: generational suites rotate generations and
// fingerprints between syncs of one run.
func (r *Run) SetSourceCacheCapability(capability *v2.SourceCacheCapability) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if capability == nil {
		r.sourceCacheCapability = nil
		return
	}
	r.sourceCacheCapability = proto.Clone(capability).(*v2.SourceCacheCapability)
}

// SourceCacheCapability returns an isolated copy of the armed capability,
// or nil when the connector does not declare one.
func (r *Run) SourceCacheCapability() *v2.SourceCacheCapability {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.sourceCacheCapability == nil {
		return nil
	}
	return proto.Clone(r.sourceCacheCapability).(*v2.SourceCacheCapability)
}

// SourceCacheLookupEvents returns an isolated snapshot in serve order.
func (r *Run) SourceCacheLookupEvents() []SourceCacheLookupEvent {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]SourceCacheLookupEvent(nil), r.sourceCacheEvents...)
}

// ResetSourceCacheLookupEvents clears the event log. Generational suites
// call it between syncs so each generation's events read from zero.
func (r *Run) ResetSourceCacheLookupEvents() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sourceCacheEvents = nil
}

func (r *Run) recordSourceCacheLookup(event SourceCacheLookupEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sourceCacheEvents = append(r.sourceCacheEvents, event)
}

// consultSourceCache performs the reference connector's per-scope lookup
// consult and returns the page token to serve. Non-root tokens and scopes
// without a spec pass through untouched. A nil lookup is treated as
// NoopLookup — SyncOpAttrs.Lookup is contractually never nil, but the
// harness must not crash while proving exactly that contract.
func (r *Run) consultSourceCache(
	ctx context.Context,
	lookup sourcecache.Lookup,
	kind sourcecache.RowKind,
	spec *SourceCacheSpec,
	token string,
) string {
	if spec == nil || token != "" {
		return token
	}
	event := SourceCacheLookupEvent{RowKind: kind, ScopeKey: spec.ScopeKey}
	if lookup == nil {
		event.LookupWasNil = true
		lookup = sourcecache.NoopLookup{}
	}
	entry, found, err := lookup.LookupPreviousSourceCache(ctx, kind, spec.ScopeKey)
	if err != nil {
		event.LookupError = err.Error()
	}
	event.Hit = found
	event.PreviousValidator = entry.CacheValidator
	event.Matched = found && entry.CacheValidator == spec.Validator
	if event.Matched && spec.WarmRoot != "" {
		event.ServedWarm = true
		token = spec.WarmRoot
	}
	r.recordSourceCacheLookup(event)
	return token
}

func cloneSourceCacheSpecs(in map[string]*SourceCacheSpec) map[string]*SourceCacheSpec {
	if in == nil {
		return nil
	}
	out := make(map[string]*SourceCacheSpec, len(in))
	for scope, spec := range in {
		if spec == nil {
			out[scope] = nil
			continue
		}
		copied := *spec
		out[scope] = &copied
	}
	return out
}
