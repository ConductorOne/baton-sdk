package pebble

import (
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// Translation layer: v2 connector wire types ↔ v3 storage record
// types. The v2 connector contract is unchanged; the engine
// translates inbound writes and outbound reads through these helpers
// so the on-disk format can evolve without breaking connector code.
//
// One-to-one mapping for every field that exists in both shapes.
// Fields present only in v3 (e.g. discovered_at) are populated at
// the caller boundary; fields present only in v2 (e.g. resource
// traits as full enums) are flattened to strings here.

// --- Grant ---

// V2GrantToV3 produces a v3.GrantRecord from a v2.Grant. The caller
// supplies sync_id since v2.Grant has no sync_id field (the connector
// writes are scoped to the engine's current sync).
func V2GrantToV3(syncID string, g *v2.Grant) *v3.GrantRecord {
	if g == nil {
		return nil
	}
	return v3.GrantRecord_builder{
		SyncId:      syncID,
		ExternalId:  g.GetId(),
		Entitlement: entitlementToRef(g.GetEntitlement()),
		Principal:   resourceToPrincipalRef(g.GetPrincipal()),
		Annotations: g.GetAnnotations(),
		Sources:     v2GrantSourcesToV3(g.GetSources()),
	}.Build()
}

// V3GrantToV2 hydrates a v2.Grant from a v3.GrantRecord. The output
// has stub Entitlement / Principal — only the identity fields are
// populated. Callers that need the full v2.Entitlement / v2.Resource
// must hydrate via separate Get on those record types.
//
// This stub-hydration approach matches the v3 design where references
// are identity-only.
func V3GrantToV2(r *v3.GrantRecord) *v2.Grant {
	if r == nil {
		return nil
	}
	return v2.Grant_builder{
		Id:          r.GetExternalId(),
		Entitlement: entitlementRefToStub(r.GetEntitlement()),
		Principal:   principalRefToStubResource(r.GetPrincipal()),
		Annotations: r.GetAnnotations(),
		Sources:     v3GrantSourcesToV2(r.GetSources()),
	}.Build()
}

func entitlementToRef(e *v2.Entitlement) *v3.EntitlementRef {
	if e == nil {
		return nil
	}
	res := e.GetResource()
	return v3.EntitlementRef_builder{
		ResourceTypeId: res.GetId().GetResourceType(),
		ResourceId:     res.GetId().GetResource(),
		EntitlementId:  e.GetId(),
	}.Build()
}

func resourceToPrincipalRef(r *v2.Resource) *v3.PrincipalRef {
	if r == nil {
		return nil
	}
	return v3.PrincipalRef_builder{
		ResourceTypeId: r.GetId().GetResourceType(),
		ResourceId:     r.GetId().GetResource(),
	}.Build()
}

// grantTranslateArena batches v3.GrantRecord / EntitlementRef /
// PrincipalRef allocations for one PutGrants call. The default
// V2GrantToV3 builder pattern heap-allocates each of the three structs
// individually — 3 × N allocations for N grants. With N=1M that's 3M
// tiny live objects, which dominates GC scan cost during the parallel-
// build phase (~440 ms scanObjectsSmall CPU in profile).
//
// The arena pre-allocates three contiguous backing arrays sized to N,
// and hands out &arena.grantRecords[i] / etc. as the result. GC sees
// 3 large objects instead of 3M small ones; the bytes used per grant
// are identical. Pointers into pre-sized slices are stable: since we
// never grow past the initial capacity, no reallocation invalidates
// earlier pointers.
//
// Lifetime: the arena lives for one PutGrants call. After
// engine.PutGrantRecords returns, the adapter drops the arena and the
// underlying arrays become garbage. The proto.Marshal calls in the
// engine treat &arena.grantRecords[i] like any other heap-allocated
// proto message — the generated Set* / Get* methods are simple field
// reads/writes that don't care where the struct lives.
type grantTranslateArena struct {
	grantRecords    []v3.GrantRecord
	entitlementRefs []v3.EntitlementRef
	principalRefs   []v3.PrincipalRef
}

// newGrantTranslateArena pre-allocates the backing arrays. Caller
// passes the expected number of grants; the arena handles up to that
// many translations without growing.
func newGrantTranslateArena(n int) *grantTranslateArena {
	return &grantTranslateArena{
		grantRecords:    make([]v3.GrantRecord, 0, n),
		entitlementRefs: make([]v3.EntitlementRef, 0, n),
		principalRefs:   make([]v3.PrincipalRef, 0, n),
	}
}

// translateV2Grant is the arena-allocating counterpart to V2GrantToV3.
// Returns a pointer into the arena's grantRecords slice; the caller
// must not retain it past the arena's lifetime. Behaviorally
// equivalent to V2GrantToV3 for all valid inputs.
func (a *grantTranslateArena) translateV2Grant(syncID string, g *v2.Grant) *v3.GrantRecord {
	if g == nil {
		return nil
	}
	var entRef *v3.EntitlementRef
	if e := g.GetEntitlement(); e != nil {
		res := e.GetResource()
		a.entitlementRefs = append(a.entitlementRefs, v3.EntitlementRef{})
		entRef = &a.entitlementRefs[len(a.entitlementRefs)-1]
		entRef.SetResourceTypeId(res.GetId().GetResourceType())
		entRef.SetResourceId(res.GetId().GetResource())
		entRef.SetEntitlementId(e.GetId())
	}
	var princRef *v3.PrincipalRef
	if p := g.GetPrincipal(); p != nil {
		a.principalRefs = append(a.principalRefs, v3.PrincipalRef{})
		princRef = &a.principalRefs[len(a.principalRefs)-1]
		princRef.SetResourceTypeId(p.GetId().GetResourceType())
		princRef.SetResourceId(p.GetId().GetResource())
	}
	a.grantRecords = append(a.grantRecords, v3.GrantRecord{})
	rec := &a.grantRecords[len(a.grantRecords)-1]
	rec.SetSyncId(syncID)
	rec.SetExternalId(g.GetId())
	if entRef != nil {
		rec.SetEntitlement(entRef)
	}
	if princRef != nil {
		rec.SetPrincipal(princRef)
	}
	if ann := g.GetAnnotations(); len(ann) > 0 {
		rec.SetAnnotations(ann)
	}
	if src := v2GrantSourcesToV3(g.GetSources()); src != nil {
		rec.SetSources(src)
	}
	return rec
}

func entitlementRefToStub(ref *v3.EntitlementRef) *v2.Entitlement {
	if ref == nil {
		return nil
	}
	return v2.Entitlement_builder{
		Id: ref.GetEntitlementId(),
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: ref.GetResourceTypeId(),
				Resource:     ref.GetResourceId(),
			}.Build(),
		}.Build(),
	}.Build()
}

func principalRefToStubResource(ref *v3.PrincipalRef) *v2.Resource {
	if ref == nil {
		return nil
	}
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: ref.GetResourceTypeId(),
			Resource:     ref.GetResourceId(),
		}.Build(),
	}.Build()
}

func v2GrantSourcesToV3(s *v2.GrantSources) map[string]*v3.GrantSourceRecord {
	if s == nil || len(s.GetSources()) == 0 {
		return nil
	}
	out := make(map[string]*v3.GrantSourceRecord, len(s.GetSources()))
	for k, v := range s.GetSources() {
		out[k] = v3.GrantSourceRecord_builder{
			IsDirect: v.GetIsDirect(),
		}.Build()
	}
	return out
}

func v3GrantSourcesToV2(m map[string]*v3.GrantSourceRecord) *v2.GrantSources {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]*v2.GrantSources_GrantSource, len(m))
	for k, v := range m {
		out[k] = v2.GrantSources_GrantSource_builder{
			IsDirect: v.GetIsDirect(),
		}.Build()
	}
	return v2.GrantSources_builder{Sources: out}.Build()
}

// --- Resource ---

// V2ResourceToV3 maps v2.Resource → v3.ResourceRecord. Caller supplies sync_id.
func V2ResourceToV3(syncID string, r *v2.Resource) *v3.ResourceRecord {
	if r == nil {
		return nil
	}
	var parent *v3.ResourceRef
	if pid := r.GetParentResourceId(); pid != nil && pid.GetResource() != "" {
		parent = v3.ResourceRef_builder{
			ResourceTypeId: pid.GetResourceType(),
			ResourceId:     pid.GetResource(),
		}.Build()
	}
	return v3.ResourceRecord_builder{
		SyncId:         syncID,
		ResourceTypeId: r.GetId().GetResourceType(),
		ResourceId:     r.GetId().GetResource(),
		DisplayName:    r.GetDisplayName(),
		Description:    r.GetDescription(),
		Parent:         parent,
		Annotations:    r.GetAnnotations(),
	}.Build()
}

// V3ResourceToV2 hydrates a v2.Resource from a v3.ResourceRecord.
// Only the fields that exist in both shapes are populated.
func V3ResourceToV2(r *v3.ResourceRecord) *v2.Resource {
	if r == nil {
		return nil
	}
	var parent *v2.ResourceId
	if pref := r.GetParent(); pref != nil && pref.GetResourceId() != "" {
		parent = v2.ResourceId_builder{
			ResourceType: pref.GetResourceTypeId(),
			Resource:     pref.GetResourceId(),
		}.Build()
	}
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: r.GetResourceTypeId(),
			Resource:     r.GetResourceId(),
		}.Build(),
		ParentResourceId: parent,
		DisplayName:      r.GetDisplayName(),
		Description:      r.GetDescription(),
		Annotations:      r.GetAnnotations(),
	}.Build()
}

// --- ResourceType ---

// V2ResourceTypeToV3 maps v2.ResourceType → v3.ResourceTypeRecord.
// Traits are flattened to their string names (TRAIT_USER → "USER").
func V2ResourceTypeToV3(syncID string, rt *v2.ResourceType) *v3.ResourceTypeRecord {
	if rt == nil {
		return nil
	}
	traits := make([]string, 0, len(rt.GetTraits()))
	for _, t := range rt.GetTraits() {
		traits = append(traits, traitToString(t))
	}
	return v3.ResourceTypeRecord_builder{
		SyncId:      syncID,
		ExternalId:  rt.GetId(),
		DisplayName: rt.GetDisplayName(),
		Traits:      traits,
		Annotations: rt.GetAnnotations(),
	}.Build()
}

// V3ResourceTypeToV2 reverses V2ResourceTypeToV3. Unknown trait
// strings become TRAIT_UNSPECIFIED — round-tripping a known trait
// preserves it.
func V3ResourceTypeToV2(r *v3.ResourceTypeRecord) *v2.ResourceType {
	if r == nil {
		return nil
	}
	traits := make([]v2.ResourceType_Trait, 0, len(r.GetTraits()))
	for _, s := range r.GetTraits() {
		traits = append(traits, stringToTrait(s))
	}
	return v2.ResourceType_builder{
		Id:          r.GetExternalId(),
		DisplayName: r.GetDisplayName(),
		Traits:      traits,
		Annotations: r.GetAnnotations(),
	}.Build()
}

func traitToString(t v2.ResourceType_Trait) string {
	// Use the generated name map, strip the TRAIT_ prefix for brevity.
	n, ok := v2.ResourceType_Trait_name[int32(t)]
	if !ok {
		return ""
	}
	return strings.TrimPrefix(n, "TRAIT_")
}

func stringToTrait(s string) v2.ResourceType_Trait {
	v, ok := v2.ResourceType_Trait_value["TRAIT_"+s]
	if !ok {
		return v2.ResourceType_TRAIT_UNSPECIFIED
	}
	return v2.ResourceType_Trait(v)
}

// --- Entitlement ---

// V2EntitlementToV3 maps v2.Entitlement → v3.EntitlementRecord.
func V2EntitlementToV3(syncID string, e *v2.Entitlement) *v3.EntitlementRecord {
	if e == nil {
		return nil
	}
	var res *v3.ResourceRef
	if r := e.GetResource(); r != nil {
		res = v3.ResourceRef_builder{
			ResourceTypeId: r.GetId().GetResourceType(),
			ResourceId:     r.GetId().GetResource(),
		}.Build()
	}
	return v3.EntitlementRecord_builder{
		SyncId:      syncID,
		ExternalId:  e.GetId(),
		Resource:    res,
		DisplayName: e.GetDisplayName(),
		Description: e.GetDescription(),
		Purpose:     purposeToString(e.GetPurpose()),
		Annotations: e.GetAnnotations(),
	}.Build()
}

// V3EntitlementToV2 reverses V2EntitlementToV3. The Resource side is
// hydrated as a stub (identity only); callers that need the full v2
// Resource must hydrate via the engine's GetResourceRecord.
func V3EntitlementToV2(r *v3.EntitlementRecord) *v2.Entitlement {
	if r == nil {
		return nil
	}
	var resource *v2.Resource
	if ref := r.GetResource(); ref != nil {
		resource = v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: ref.GetResourceTypeId(),
				Resource:     ref.GetResourceId(),
			}.Build(),
		}.Build()
	}
	return v2.Entitlement_builder{
		Id:          r.GetExternalId(),
		Resource:    resource,
		DisplayName: r.GetDisplayName(),
		Description: r.GetDescription(),
		Purpose:     stringToPurpose(r.GetPurpose()),
		Annotations: r.GetAnnotations(),
	}.Build()
}

func purposeToString(p v2.Entitlement_PurposeValue) string {
	if p == v2.Entitlement_PURPOSE_VALUE_UNSPECIFIED {
		return ""
	}
	n, ok := v2.Entitlement_PurposeValue_name[int32(p)]
	if !ok {
		return ""
	}
	return strings.TrimPrefix(n, "PURPOSE_VALUE_")
}

func stringToPurpose(s string) v2.Entitlement_PurposeValue {
	if s == "" {
		return v2.Entitlement_PURPOSE_VALUE_UNSPECIFIED
	}
	v, ok := v2.Entitlement_PurposeValue_value["PURPOSE_VALUE_"+s]
	if !ok {
		return v2.Entitlement_PURPOSE_VALUE_UNSPECIFIED
	}
	return v2.Entitlement_PurposeValue(v)
}
