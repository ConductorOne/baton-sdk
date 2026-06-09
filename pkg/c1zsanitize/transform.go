package c1zsanitize

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// listPageSize is the source read page, which also bounds how many
// rows each dst Put batches into one transaction. Larger pages mean
// fewer commits on large syncs; the dst writer still sub-chunks each
// statement under SQLite's parameter ceiling, so this stays safe.
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

func (s *sanitizer) copyResourceTypes(
	ctx context.Context,
	src connectorstore.Reader,
	dst connectorstore.Writer,
	srcSyncID string,
	refs *assetRefSet,
) error {
	// Two-pass: buffer EVERY resource-type row and register all ids BEFORE
	// transforming any of them. Resource types number in the tens, so
	// buffering the whole listing is trivial. This makes knownResourceTypes
	// complete before transformResourceType (or a ChildResourceType handler)
	// ever consults it, so a row that references a type declared later in the
	// listing — or on a later page — resolves against the full set instead of
	// HMAC-ing a not-yet-seen token. transformResourceType is therefore pure
	// w.r.t. the set (it never writes it), so its output does not depend on
	// the order rows arrive in.
	var rows []*v2.ResourceType
	readDur := time.Duration(0)
	pageToken := ""
	for {
		req := v2.ResourceTypesServiceListResourceTypesRequest_builder{
			PageSize:    listPageSize,
			PageToken:   pageToken,
			Annotations: syncIDAnnotations(srcSyncID),
		}.Build()
		readStart := time.Now()
		resp, err := src.ListResourceTypes(ctx, req)
		readDur += time.Since(readStart)
		if err != nil {
			return fmt.Errorf("list resource types: %w", err)
		}
		for _, rt := range resp.GetList() {
			if id := rt.GetId(); id != "" {
				s.knownResourceTypes[id] = struct{}{}
			}
			rows = append(rows, rt)
		}
		if resp.GetNextPageToken() == "" {
			break
		}
		pageToken = resp.GetNextPageToken()
	}

	xformStart := time.Now()
	out := make([]*v2.ResourceType, 0, len(rows))
	for _, rt := range rows {
		out = append(out, s.transformResourceType(rt, refs))
	}
	xformDur := time.Since(xformStart)
	// Sort by output id for destination unique-index locality.
	sort.Slice(out, func(i, j int) bool { return out[i].GetId() < out[j].GetId() })
	putStart := time.Now()
	if len(out) > 0 {
		if err := dst.PutResourceTypes(ctx, out...); err != nil {
			return fmt.Errorf("put resource types: %w", err)
		}
	}
	s.logPage(srcSyncID, "resource_types", 0, len(out), readDur, xformDur, time.Since(putStart))
	return nil
}

func (s *sanitizer) copyResources(
	ctx context.Context,
	src connectorstore.Reader,
	dst connectorstore.Writer,
	srcSyncID string,
	refs *assetRefSet,
) error {
	pageToken := ""
	page := 0
	for {
		req := v2.ResourcesServiceListResourcesRequest_builder{
			PageSize:    listPageSize,
			PageToken:   pageToken,
			Annotations: syncIDAnnotations(srcSyncID),
		}.Build()
		readStart := time.Now()
		resp, err := src.ListResources(ctx, req)
		readDur := time.Since(readStart)
		if err != nil {
			return fmt.Errorf("list resources: %w", err)
		}
		xformStart := time.Now()
		out := make([]*v2.Resource, 0, len(resp.GetList()))
		for _, r := range resp.GetList() {
			out = append(out, s.transformResource(r, refs))
		}
		xformDur := time.Since(xformStart)
		sortByResourceID(out)
		putStart := time.Now()
		if len(out) > 0 {
			if err := dst.PutResources(ctx, out...); err != nil {
				return fmt.Errorf("put resources: %w", err)
			}
		}
		s.logPage(srcSyncID, "resources", page, len(resp.GetList()), readDur, xformDur, time.Since(putStart))
		if resp.GetNextPageToken() == "" {
			return nil
		}
		pageToken = resp.GetNextPageToken()
		page++
	}
}

func (s *sanitizer) copyEntitlements(
	ctx context.Context,
	src connectorstore.Reader,
	dst connectorstore.Writer,
	srcSyncID string,
	refs *assetRefSet,
) error {
	pageToken := ""
	page := 0
	for {
		req := v2.EntitlementsServiceListEntitlementsRequest_builder{
			PageSize:    listPageSize,
			PageToken:   pageToken,
			Annotations: syncIDAnnotations(srcSyncID),
		}.Build()
		readStart := time.Now()
		resp, err := src.ListEntitlements(ctx, req)
		readDur := time.Since(readStart)
		if err != nil {
			return fmt.Errorf("list entitlements: %w", err)
		}
		xformStart := time.Now()
		out := make([]*v2.Entitlement, 0, len(resp.GetList()))
		for _, e := range resp.GetList() {
			out = append(out, s.transformEntitlement(e, refs))
		}
		xformDur := time.Since(xformStart)
		sort.Slice(out, func(i, j int) bool { return out[i].GetId() < out[j].GetId() })
		putStart := time.Now()
		if len(out) > 0 {
			if err := dst.PutEntitlements(ctx, out...); err != nil {
				return fmt.Errorf("put entitlements: %w", err)
			}
		}
		s.logPage(srcSyncID, "entitlements", page, len(resp.GetList()), readDur, xformDur, time.Since(putStart))
		if resp.GetNextPageToken() == "" {
			return nil
		}
		pageToken = resp.GetNextPageToken()
		page++
	}
}

func (s *sanitizer) copyGrants(
	ctx context.Context,
	src connectorstore.Reader,
	dst connectorstore.Writer,
	srcSyncID string,
	refs *assetRefSet,
	cache *grantSubCache,
) error {
	pageToken := ""
	page := 0
	for {
		req := v2.GrantsServiceListGrantsRequest_builder{
			PageSize:    listPageSize,
			PageToken:   pageToken,
			Annotations: syncIDAnnotations(srcSyncID),
		}.Build()
		readStart := time.Now()
		resp, err := src.ListGrants(ctx, req)
		readDur := time.Since(readStart)
		if err != nil {
			return fmt.Errorf("list grants: %w", err)
		}
		xformStart := time.Now()
		out := make([]*v2.Grant, 0, len(resp.GetList()))
		for _, g := range resp.GetList() {
			out = append(out, s.transformGrant(g, refs, cache))
		}
		xformDur := time.Since(xformStart)
		sort.Slice(out, func(i, j int) bool { return out[i].GetId() < out[j].GetId() })
		putStart := time.Now()
		if len(out) > 0 {
			if err := dst.PutGrants(ctx, out...); err != nil {
				return fmt.Errorf("put grants: %w", err)
			}
		}
		s.logPage(srcSyncID, "grants", page, len(resp.GetList()), readDur, xformDur, time.Since(putStart))
		if resp.GetNextPageToken() == "" {
			if cache != nil {
				s.log.Info("c1zsanitize: grant sub-cache stats",
					zap.String("sync_id", srcSyncID),
					zap.Int("entitlements_cached", len(cache.entitlements)),
					zap.Int("principals_cached", len(cache.principals)))
			}
			return nil
		}
		pageToken = resp.GetNextPageToken()
		page++
	}
}

// sortByResourceID orders resources by output (type, resource) id for
// destination unique-index locality.
func sortByResourceID(rs []*v2.Resource) {
	sort.Slice(rs, func(i, j int) bool {
		a, b := rs[i].GetId(), rs[j].GetId()
		if a.GetResourceType() != b.GetResourceType() {
			return a.GetResourceType() < b.GetResourceType()
		}
		return a.GetResource() < b.GetResource()
	})
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
	// happens up front in copyResourceTypes' buffering pre-pass, so the set is
	// complete and read-only by the time any transform consults it. This holds
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
// portion), parent id, display name, description, and annotations.
// baton_resource is preserved as it's a flag, not identity.
func (s *sanitizer) transformResource(in *v2.Resource, refs *assetRefSet) *v2.Resource {
	if in == nil {
		return nil
	}
	annos := s.transformAnnotations(in.GetAnnotations(), refs)
	return v2.Resource_builder{
		Id:               s.transformResourceID(in.GetId()),
		ParentResourceId: s.transformResourceID(in.GetParentResourceId()),
		DisplayName:      s.id(in.GetDisplayName()),
		Description:      s.id(in.GetDescription()),
		Annotations:      annos,
		BatonResource:    in.GetBatonResource(),
	}.Build()
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
// a resource id but was never declared in copyResourceTypes. Not safe for
// concurrent callers — the transform stage is sequential today; revisit the
// dedup map if it is ever parallelized.
func (s *sanitizer) warnUndeclaredResourceType(rt string) {
	if rt == "" || s.isKnownResourceType(rt) {
		return
	}
	if _, seen := s.warnedUndeclaredTypes[rt]; seen {
		return
	}
	s.warnedUndeclaredTypes[rt] = struct{}{}
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
// a cross-grant data-corruption bug. The cache is per-sync (reset in
// sanitizeSync) to bound memory; s.id output is secret-scoped, not
// sync-scoped, so correctness does not depend on the lifetime.
//
// Assumption: within one sync, every embedded copy of a given entitlement
// (or principal) id is identical. This holds for SDK-produced c1z files,
// where the connector emits one object per id. The verify flag turns on a
// proto.Equal cross-check against a fresh transform for the first
// verifySampleLimit hits — cheap insurance, off by default.
type grantSubCache struct {
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
	if key != "" {
		if got, ok := cache.entitlements[key]; ok {
			s.verifyCachedEntitlement(in, refs, got, cache)
			// SHARED: returned to many output grants — never mutate after Build().
			return got
		}
	}
	out := s.transformEntitlement(in, refs)
	if key != "" {
		// SHARED once stored: referenced by every later grant with this id.
		// Never mutate after Build() — mutation = cross-grant corruption.
		cache.entitlements[key] = out
	}
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
	if key != "" {
		if got, ok := cache.principals[key]; ok {
			s.verifyCachedPrincipal(in, refs, got, cache)
			// SHARED: returned to many output grants — never mutate after Build().
			return got
		}
	}
	out := s.transformResource(in, refs)
	if key != "" && len(cache.principals) < maxCachedPrincipals {
		// SHARED once stored: referenced by every later grant with this id.
		// Never mutate after Build() — mutation = cross-grant corruption.
		cache.principals[key] = out
	}
	return out
}

// verifyCachedEntitlement is the off-by-default cache-correctness guard: it
// re-transforms a sample of cache hits and logs a warning if the cached proto
// differs from the fresh one, catching any source that violates the
// one-object-per-id assumption.
func (s *sanitizer) verifyCachedEntitlement(in *v2.Entitlement, refs *assetRefSet, cached *v2.Entitlement, cache *grantSubCache) {
	if !cache.verify || cache.verifySeenEntitlements >= verifySampleLimit {
		return
	}
	cache.verifySeenEntitlements++
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
	if !cache.verify || cache.verifySeenPrincipals >= verifySampleLimit {
		return
	}
	cache.verifySeenPrincipals++
	fresh := s.transformResource(in, refs)
	if !proto.Equal(fresh, cached) {
		s.log.Warn("c1zsanitize: grant sub-cache mismatch; embedded principal differs across grants for the same id",
			zap.String("principal_resource_type", in.GetId().GetResourceType()),
			zap.String("principal_resource_id", in.GetId().GetResource()))
	}
}

// logPage emits one Info line per page with read/transform/put timings — the
// permanent regression canary for the per-record-cost-grows-with-n class of
// slowdown. One line per 10k rows is cheap.
func (s *sanitizer) logPage(syncID, phase string, page, rows int, read, transform, put time.Duration) {
	s.log.Info("c1zsanitize: page",
		zap.String("sync_id", syncID),
		zap.String("phase", phase),
		zap.Int("page", page),
		zap.Int("rows", rows),
		zap.Duration("read", read),
		zap.Duration("transform", transform),
		zap.Duration("put", put),
	)
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
