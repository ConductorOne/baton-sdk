package c1zsanitize

import (
	"context"
	"fmt"
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
	// knownResourceTypes is populated only here; freeze it on return so
	// later phases treat it as read-only (see inResourceTypesPhase / C-2).
	s.inResourceTypesPhase = true
	defer func() { s.inResourceTypesPhase = false }()

	pageToken := ""
	page := 0
	for {
		req := v2.ResourceTypesServiceListResourceTypesRequest_builder{
			PageSize:    listPageSize,
			PageToken:   pageToken,
			Annotations: syncIDAnnotations(srcSyncID),
		}.Build()
		readStart := time.Now()
		resp, err := src.ListResourceTypes(ctx, req)
		readDur := time.Since(readStart)
		if err != nil {
			return fmt.Errorf("list resource types: %w", err)
		}
		xformStart := time.Now()
		out := make([]*v2.ResourceType, 0, len(resp.GetList()))
		for _, rt := range resp.GetList() {
			out = append(out, s.transformResourceType(rt, refs))
		}
		xformDur := time.Since(xformStart)
		putStart := time.Now()
		if len(out) > 0 {
			if err := dst.PutResourceTypes(ctx, out...); err != nil {
				return fmt.Errorf("put resource types: %w", err)
			}
		}
		s.logPage("resource_types", page, len(resp.GetList()), readDur, xformDur, time.Since(putStart))
		if resp.GetNextPageToken() == "" {
			return nil
		}
		pageToken = resp.GetNextPageToken()
		page++
	}
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
		putStart := time.Now()
		if len(out) > 0 {
			if err := dst.PutResources(ctx, out...); err != nil {
				return fmt.Errorf("put resources: %w", err)
			}
		}
		s.logPage("resources", page, len(resp.GetList()), readDur, xformDur, time.Since(putStart))
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
		putStart := time.Now()
		if len(out) > 0 {
			if err := dst.PutEntitlements(ctx, out...); err != nil {
				return fmt.Errorf("put entitlements: %w", err)
			}
		}
		s.logPage("entitlements", page, len(resp.GetList()), readDur, xformDur, time.Since(putStart))
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
		putStart := time.Now()
		if len(out) > 0 {
			if err := dst.PutGrants(ctx, out...); err != nil {
				return fmt.Errorf("put grants: %w", err)
			}
		}
		s.logPage("grants", page, len(resp.GetList()), readDur, xformDur, time.Since(putStart))
		if resp.GetNextPageToken() == "" {
			return nil
		}
		pageToken = resp.GetNextPageToken()
		page++
	}
}

// transformResourceType preserves the resource type's id and trait
// enum but rewrites display name, description, and annotations.
// resource_type.id is connector-defined (e.g. "user") and treated as
// non-tenant; see §7 question 1 in the investigation for the caveat.
func (s *sanitizer) transformResourceType(in *v2.ResourceType, refs *assetRefSet) *v2.ResourceType {
	if in == nil {
		return nil
	}
	if id := in.GetId(); id != "" {
		s.knownResourceTypes[id] = struct{}{}
	}
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
	return v2.ResourceId_builder{
		ResourceType:  in.GetResourceType(),
		Resource:      s.id(in.GetResource()),
		BatonResource: in.GetBatonResource(),
	}.Build()
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
	entitlements map[string]*v2.Entitlement // key: SOURCE entitlement id
	principals   map[string]*v2.Resource    // key: srcType + "\x00" + srcResource
	verify       bool
	verifySeen   int
}

const verifySampleLimit = 1000

func newGrantSubCache() *grantSubCache {
	return &grantSubCache{
		entitlements: map[string]*v2.Entitlement{},
		principals:   map[string]*v2.Resource{},
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
			return got
		}
	}
	out := s.transformEntitlement(in, refs)
	if key != "" {
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
	key := ""
	if id := in.GetId(); id != nil {
		key = id.GetResourceType() + "\x00" + id.GetResource()
	}
	if key != "" {
		if got, ok := cache.principals[key]; ok {
			return got
		}
	}
	out := s.transformResource(in, refs)
	if key != "" && len(cache.principals) < maxCachedPrincipals {
		cache.principals[key] = out
	}
	return out
}

// verifyCachedEntitlement is the off-by-default cache-correctness guard: it
// re-transforms a sample of cache hits and logs a warning if the cached proto
// differs from the fresh one, catching any source that violates the
// one-object-per-id assumption.
func (s *sanitizer) verifyCachedEntitlement(in *v2.Entitlement, refs *assetRefSet, cached *v2.Entitlement, cache *grantSubCache) {
	if !cache.verify || cache.verifySeen >= verifySampleLimit {
		return
	}
	cache.verifySeen++
	fresh := s.transformEntitlement(in, refs)
	if !proto.Equal(fresh, cached) {
		s.log.Warn("c1zsanitize: grant sub-cache mismatch; embedded entitlement differs across grants for the same id",
			zap.String("entitlement_id", in.GetId()))
	}
}

// logPage emits one Info line per page with read/transform/put timings — the
// permanent regression canary for the per-record-cost-grows-with-n class of
// slowdown. One line per 10k rows is cheap.
func (s *sanitizer) logPage(phase string, page, rows int, read, transform, put time.Duration) {
	s.log.Info("c1zsanitize: page",
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
