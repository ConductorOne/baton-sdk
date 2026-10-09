package c1zsanitize

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// parallelTransform applies fn to each index [0,n) using up to GOMAXPROCS
// workers that pull indices off a shared counter. fn must write only to its
// own index i of a pre-sized output slice, so order is preserved with no
// channels. The transform is CPU-bound (HMAC + proto marshal/unmarshal) and
// touches only concurrency-safe shared state (pooled HMAC, mutex-guarded
// caches), so fanning it out is safe.
func parallelTransform(n int, fn func(i int)) {
	workers := runtime.GOMAXPROCS(0)
	if workers > n {
		workers = n
	}
	if workers <= 1 {
		for i := 0; i < n; i++ {
			fn(i)
		}
		return
	}
	var idx int64 = -1
	var wg sync.WaitGroup
	// A panic in fn must unwind to the caller, not crash the process from an
	// unrecovered worker goroutine. Capture the first panic, stop handing out
	// work, and re-panic on the calling goroutine after the workers drain — the
	// same failure path a sequential fn would take.
	var panicOnce sync.Once
	var panicVal any
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicOnce.Do(func() { panicVal = r })
					atomic.StoreInt64(&idx, int64(n)) // halt remaining pulls
				}
			}()
			for {
				i := int(atomic.AddInt64(&idx, 1))
				if i >= n {
					return
				}
				fn(i)
			}
		}()
	}
	wg.Wait()
	if panicVal != nil {
		panic(panicVal)
	}
}

// listPageSize bounds each atomic ledger page.
const listPageSize = 10000

// transformID rewrites a composite baton identifier one ':'-delimited
// component at a time. A component is preserved verbatim only when it
// is a declared resource-type token; connector-defined type names are
// structural schema, not tenant data, and preserving them keeps a
// sanitized id's embedded type tokens equal to the separately-
// sanitized resource/principal type fields. EVERY other component is
// HMAC'd — including an opaque id that sits in a position the canonical
// grammar (entitlement "type:id:perm", grant "entId:type:id") reserves
// for a type.
//
// This is deliberately fail-closed: positional grammar is not trusted
// to decide what is safe to keep, because connectors emit
// non-canonical ids — e.g. a grant id carrying a tenant group UUID in a
// type slot — and trusting position would let that UUID survive in
// cleartext. HMAC is deterministic, so a component that is also a
// resource or principal id still hashes to the same value as the
// separately-sanitized structured field, keeping cross-references
// coherent. A single-component id (no ':') has no structural token to
// keep and is HMAC'd whole.
func (s *sanitizer) transformID(id string) string {
	if id == "" {
		return ""
	}
	if !strings.Contains(id, ":") {
		return s.id(id)
	}
	parts := strings.Split(id, ":")
	for i, p := range parts {
		if p != "" && s.isKnownResourceType(p) {
			continue
		}
		parts[i] = s.id(p)
	}
	return strings.Join(parts, ":")
}

func (s *sanitizer) isKnownResourceType(token string) bool {
	_, ok := s.knownResourceTypes[token]
	return ok
}

// transformResourceType preserves the resource type's id and trait
// enum but rewrites display name, description, and annotations.
// resource_type.id is connector-defined (e.g. "user") and treated as
// non-tenant; see §7 question 1 in the investigation for the caveat.
func (s *sanitizer) transformResourceType(in *v2.ResourceType, refs *assetRefSet) *v2.ResourceType {
	if in == nil {
		return nil
	}
	// transformResourceType is pure w.r.t. knownResourceTypes: registration
	// happens before ledger work starts, so the set is complete and read-only.
	// This holds
	// whether the call is for a declared type or an embedded one (an
	// entitlement's GrantableTo, a grant's slice), which is what makes
	// transformID's known-type decision order-independent.
	annos := s.transformAnnotations(in.GetAnnotations(), refs)
	return v2.ResourceType_builder{
		Id:                in.GetId(),
		DisplayName:       s.id(in.GetDisplayName()),
		Description:       s.id(in.GetDescription()),
		Traits:            in.GetTraits(),
		Annotations:       annos,
		SourcedExternally: in.GetSourcedExternally(),
	}.Build()
}

// transformResource rewrites the resource id (preserving the type
// portion), parent id, display name, description, annotations, and the
// resource-level profile/icon/status/created_at attributes.
// baton_resource is preserved as it's a flag, not identity.
func (s *sanitizer) transformResource(in *v2.Resource, refs *assetRefSet) *v2.Resource {
	if in == nil {
		return nil
	}
	annos := s.transformAnnotations(in.GetAnnotations(), refs)
	out := v2.Resource_builder{
		Id:               s.transformResourceID(in.GetId()),
		ParentResourceId: s.transformResourceID(in.GetParentResourceId()),
		DisplayName:      s.id(in.GetDisplayName()),
		Description:      s.id(in.GetDescription()),
		Annotations:      annos,
		BatonResource:    in.GetBatonResource(),
		Profile:          s.sanitizeStruct(in.GetProfile()),
		Icon:             s.sanitizeAssetRef(in.GetIcon(), refs),
	}.Build()
	if in.HasStatus() {
		out.SetStatus(v2.Status_builder{
			Status:  in.GetStatus().GetStatus(),
			Details: s.id(in.GetStatus().GetDetails()),
		}.Build())
	}
	if in.HasCreatedAt() {
		out.SetCreatedAt(s.shifter.shift(in.GetCreatedAt()))
	}
	return out
}

func (s *sanitizer) transformResourceID(in *v2.ResourceId) *v2.ResourceId {
	if in == nil {
		return nil
	}
	if in.GetResource() == "" && in.GetResourceType() == "" {
		return nil
	}
	// resource_type is preserved verbatim here (it is connector-defined
	// schema). transformID, by contrast, HMACs a type token that is NOT a
	// declared resource type. So an UNDECLARED type token both survives in
	// cleartext in this field and gets HMAC'd inside composite ids. Behavior
	// is unchanged; emit a once-per-token tripwire so a connector that emits
	// undeclared type tokens shows up rather than silently producing
	// mismatched representations.
	s.warnUndeclaredResourceType(in.GetResourceType())
	return v2.ResourceId_builder{
		ResourceType:  in.GetResourceType(),
		Resource:      s.id(in.GetResource()),
		BatonResource: in.GetBatonResource(),
	}.Build()
}

// warnUndeclaredResourceType logs once per resource-type token that appears in
// a resource id but was not declared by the source sync. Called from the
// concurrent transform workers, so the dedup set is guarded by statsMu; the
// log fires outside the lock.
func (s *sanitizer) warnUndeclaredResourceType(rt string) {
	if rt == "" || s.isKnownResourceType(rt) {
		return
	}
	s.statsMu.Lock()
	if _, seen := s.warnedUndeclaredTypes[rt]; seen {
		s.statsMu.Unlock()
		return
	}
	s.warnedUndeclaredTypes[rt] = struct{}{}
	s.statsMu.Unlock()
	s.log.Warn("c1zsanitize: resource id references an undeclared resource type; preserved verbatim here but HMAC'd inside composite ids",
		zap.String("resource_type", rt))
}

func (s *sanitizer) transformEntitlement(in *v2.Entitlement, refs *assetRefSet) *v2.Entitlement {
	if in == nil {
		return nil
	}
	annos := s.transformAnnotations(in.GetAnnotations(), refs)
	out := v2.Entitlement_builder{
		Id:          s.transformID(in.GetId()),
		Resource:    s.transformResource(in.GetResource(), refs),
		DisplayName: s.id(in.GetDisplayName()),
		Description: s.id(in.GetDescription()),
		GrantableTo: s.transformResourceTypeSlice(in.GetGrantableTo(), refs),
		Purpose:     in.GetPurpose(),
		Slug:        s.id(in.GetSlug()),
		Annotations: annos,
	}.Build()
	return out
}

func (s *sanitizer) transformResourceTypeSlice(in []*v2.ResourceType, refs *assetRefSet) []*v2.ResourceType {
	if len(in) == 0 {
		return nil
	}
	out := make([]*v2.ResourceType, 0, len(in))
	for _, rt := range in {
		out = append(out, s.transformResourceType(rt, refs))
	}
	return out
}

func (s *sanitizer) transformGrant(in *v2.Grant, refs *assetRefSet, cache *grantSubCache) *v2.Grant {
	if in == nil {
		return nil
	}
	annos := s.transformAnnotations(in.GetAnnotations(), refs)
	return v2.Grant_builder{
		Id:          s.transformID(in.GetId()),
		Entitlement: s.cachedEntitlement(in.GetEntitlement(), refs, cache),
		Principal:   s.cachedPrincipal(in.GetPrincipal(), refs, cache),
		Sources:     s.transformGrantSources(in.GetSources()),
		Annotations: annos,
	}.Build()
}

// maxCachedPrincipals bounds the per-sync principal cache. Entitlements are
// cached unconditionally (their cardinality is small — typically 100–10,000x
// fewer than grants — and dotc1z emits grants grouped by entitlement, so the
// hit rate is very high). Principal repeats are scattered, so the cache is
// capped: past the cap, principals are still transformed, just not cached.
const maxCachedPrincipals = 1_000_000

// grantSubCache memoizes the transformed embedded Entitlement and Principal
// of a grant within one sync. transformGrant re-derives both for every grant
// otherwise, and the annotation path (UnmarshalNew → handler → anypb.New) is
// the expensive part. Cached protos are SHARED across many output grants:
// this is safe ONLY because nothing in this package mutates a message after
// .Build() and the writer only marshals — mutating a cached message would be
// a cross-grant data-corruption bug. The cache is process-local to one
// sanitizer run; s.id output is secret-scoped, not
// sync-scoped, so correctness does not depend on the process-local lifetime.
//
// Assumption: within one sync, every embedded copy of a given entitlement
// (or principal) id is identical. This holds for SDK-produced c1z files,
// where the connector emits one object per id. The verify flag turns on a
// proto.Equal cross-check against a fresh transform for the first
// verifySampleLimit hits — cheap insurance, off by default.
type grantSubCache struct {
	// mu guards the maps and the verify counters. The grant transform fans out
	// across workers, so the cache is shared. The expensive transform on a miss
	// runs OUTSIDE the lock (double-checked insert), so the critical sections
	// are only map lookups/inserts — a plain Mutex is enough and avoids the
	// RWMutex bookkeeping for what is, after warmup, a tiny read-mostly map
	// (entitlement cardinality is orders of magnitude below grant count).
	mu sync.Mutex
	// SHARED: the cached *v2.Entitlement / *v2.Resource values below are
	// referenced by multiple output grants. Never mutate one after Build().
	// Mutation after caching = cross-grant corruption.
	entitlements map[string]*v2.Entitlement // key: SOURCE entitlement id
	principals   map[string]*v2.Resource    // key: srcType + "\x00" + srcResource
	verify       bool
	// Independent sample counters so the entitlement guard cannot exhaust
	// the principal guard's budget (and vice versa).
	verifySeenEntitlements int
	verifySeenPrincipals   int
}

const verifySampleLimit = 1000

func newGrantSubCache(verify bool) *grantSubCache {
	return &grantSubCache{
		entitlements: map[string]*v2.Entitlement{},
		principals:   map[string]*v2.Resource{},
		verify:       verify,
	}
}

// cachedEntitlement returns the transformed embedded entitlement, memoized by
// source entitlement id. On a cache MISS the normal transform path runs,
// which registers any asset refs via sanitizeAssetRef → refs.add; on a HIT
// those refs were already registered when the entry was created within this
// same sync, so nothing is lost.
func (s *sanitizer) cachedEntitlement(in *v2.Entitlement, refs *assetRefSet, cache *grantSubCache) *v2.Entitlement {
	if in == nil {
		return nil
	}
	if cache == nil {
		return s.transformEntitlement(in, refs)
	}
	key := in.GetId()
	if key == "" {
		return s.transformEntitlement(in, refs) // nothing to key on
	}
	cache.mu.Lock()
	got, ok := cache.entitlements[key]
	cache.mu.Unlock()
	if ok {
		s.verifyCachedEntitlement(in, refs, got, cache)
		// SHARED: returned to many output grants — never mutate after Build().
		return got
	}
	// Transform outside the lock. Two workers may race to fill the same key;
	// both produce proto-equal results (the transform is a pure function of
	// (secret, input)), so the double-check below just keeps one shared pointer.
	out := s.transformEntitlement(in, refs)
	cache.mu.Lock()
	if existing, ok := cache.entitlements[key]; ok {
		out = existing
	} else {
		// SHARED once stored: referenced by every later grant with this id.
		cache.entitlements[key] = out
	}
	cache.mu.Unlock()
	return out
}

// cachedPrincipal mirrors cachedEntitlement for the grant's principal,
// capped at maxCachedPrincipals to bound memory.
func (s *sanitizer) cachedPrincipal(in *v2.Resource, refs *assetRefSet, cache *grantSubCache) *v2.Resource {
	if in == nil {
		return nil
	}
	if cache == nil {
		return s.transformResource(in, refs)
	}
	// Key only on the identifying component. A principal with an empty
	// resource id has nothing that uniquely identifies it, so it must not be
	// cached — otherwise two distinct empty-resource principals with different
	// display names would conflate on a non-empty-but-meaningless key like
	// "user\x00" and the second grant would inherit the first's transform.
	key := ""
	if id := in.GetId(); id != nil && id.GetResource() != "" {
		key = id.GetResourceType() + "\x00" + id.GetResource()
	}
	if key == "" {
		return s.transformResource(in, refs) // uncacheable empty id
	}
	cache.mu.Lock()
	got, ok := cache.principals[key]
	cache.mu.Unlock()
	if ok {
		s.verifyCachedPrincipal(in, refs, got, cache)
		// SHARED: returned to many output grants — never mutate after Build().
		return got
	}
	out := s.transformResource(in, refs)
	cache.mu.Lock()
	if existing, ok := cache.principals[key]; ok {
		out = existing
	} else if len(cache.principals) < maxCachedPrincipals {
		// SHARED once stored: referenced by every later grant with this id.
		cache.principals[key] = out
	}
	cache.mu.Unlock()
	return out
}

// verifyCachedEntitlement is the off-by-default cache-correctness guard: it
// re-transforms a sample of cache hits and logs a warning if the cached proto
// differs from the fresh one, catching any source that violates the
// one-object-per-id assumption.
func (s *sanitizer) verifyCachedEntitlement(in *v2.Entitlement, refs *assetRefSet, cached *v2.Entitlement, cache *grantSubCache) {
	if !cache.verify {
		return
	}
	cache.mu.Lock()
	if cache.verifySeenEntitlements >= verifySampleLimit {
		cache.mu.Unlock()
		return
	}
	cache.verifySeenEntitlements++
	cache.mu.Unlock()
	fresh := s.transformEntitlement(in, refs)
	if !proto.Equal(fresh, cached) {
		s.log.Warn("c1zsanitize: grant sub-cache mismatch; embedded entitlement differs across grants for the same id",
			zap.String("entitlement_id", in.GetId()))
	}
}

// verifyCachedPrincipal mirrors verifyCachedEntitlement for the principal
// cache: same off-by-default toggle, same proto.Equal-against-a-fresh-transform
// contract, catching any source that emits diverging principal objects for the
// same resource id within a sync.
func (s *sanitizer) verifyCachedPrincipal(in *v2.Resource, refs *assetRefSet, cached *v2.Resource, cache *grantSubCache) {
	if !cache.verify {
		return
	}
	cache.mu.Lock()
	if cache.verifySeenPrincipals >= verifySampleLimit {
		cache.mu.Unlock()
		return
	}
	cache.verifySeenPrincipals++
	cache.mu.Unlock()
	fresh := s.transformResource(in, refs)
	if !proto.Equal(fresh, cached) {
		s.log.Warn("c1zsanitize: grant sub-cache mismatch; embedded principal differs across grants for the same id",
			zap.String("principal_resource_type", in.GetId().GetResourceType()),
			zap.String("principal_resource_id", in.GetId().GetResource()))
	}
}

// transformGrantSources rebuilds the sources map with sanitized keys.
// Map keys are source entitlement IDs; values carry only is_direct.
func (s *sanitizer) transformGrantSources(in *v2.GrantSources) *v2.GrantSources {
	if in == nil {
		return nil
	}
	srcMap := in.GetSources()
	if len(srcMap) == 0 {
		return v2.GrantSources_builder{}.Build()
	}
	out := make(map[string]*v2.GrantSources_GrantSource, len(srcMap))
	for srcEntitlementID, gs := range srcMap {
		out[s.transformID(srcEntitlementID)] = v2.GrantSources_GrantSource_builder{
			IsDirect: gs.GetIsDirect(),
		}.Build()
	}
	return v2.GrantSources_builder{Sources: out}.Build()
}
