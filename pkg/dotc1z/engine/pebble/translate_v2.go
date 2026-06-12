package pebble

import (
	"strings"

	"google.golang.org/protobuf/types/known/anypb"

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

// V2GrantToV3 produces a v3.GrantRecord from a v2.Grant. The syncID
// parameter is retained for call-site symmetry with other translators;
// v3 data records no longer store sync_id in the value because sync scope is
// encoded in Pebble keys by the engine write path.
//
// The GrantExpandable annotation (if any) is extracted into the
// dedicated Expansion field and stripped from Annotations, and
// NeedsExpansion is set to true. This matches the SQLite path
// (extractAndStripExpansion in pkg/dotc1z/grants.go). Without
// this extraction the grant expansion pipeline silently no-ops on
// Pebble — idxGrantByNeedsExpansion stays empty and PendingExpansion
// returns no grants.
func V2GrantToV3(syncID string, g *v2.Grant) *v3.GrantRecord {
	if g == nil {
		return nil
	}
	expansion, annotations := extractV2Expansion(g.GetAnnotations())
	return v3.GrantRecord_builder{
		ExternalId:     g.GetId(),
		Entitlement:    entitlementToRef(g.GetEntitlement()),
		Principal:      resourceToPrincipalRef(g.GetPrincipal()),
		Annotations:    annotations,
		Sources:        v2GrantSourcesToV3(g.GetSources()),
		Expansion:      expansion,
		NeedsExpansion: expansion != nil,
	}.Build()
}

// extractV2Expansion pulls the GrantExpandable annotation out of the
// annotation list (if present) and returns it as a v3
// GrantExpandableRecord plus the filtered annotation list. Returns
// (nil, original) when no GrantExpandable annotation exists or the
// payload has no usable entitlement_ids — matches the SQLite
// extractAndStripExpansion contract.
func extractV2Expansion(anns []*anypb.Any) (*v3.GrantExpandableRecord, []*anypb.Any) {
	if len(anns) == 0 {
		return nil, anns
	}
	out := make([]*anypb.Any, 0, len(anns))
	var found *v2.GrantExpandable
	for _, a := range anns {
		if a == nil {
			out = append(out, a)
			continue
		}
		if a.MessageIs((*v2.GrantExpandable)(nil)) && found == nil {
			candidate := &v2.GrantExpandable{}
			if err := a.UnmarshalTo(candidate); err == nil {
				found = candidate
				continue
			}
		}
		out = append(out, a)
	}
	if found == nil {
		return nil, anns
	}
	// Mirror SQLite: only treat as expandable when there's at
	// least one non-whitespace entitlement id. Otherwise the
	// annotation is still stripped but no expansion payload is
	// recorded.
	hasValid := false
	for _, id := range found.GetEntitlementIds() {
		if strings.TrimSpace(id) != "" {
			hasValid = true
			break
		}
	}
	if !hasValid {
		return nil, out
	}
	return v3.GrantExpandableRecord_builder{
		EntitlementIds:  found.GetEntitlementIds(),
		Shallow:         found.GetShallow(),
		ResourceTypeIds: found.GetResourceTypeIds(),
	}.Build(), out
}

// V3GrantToV2 hydrates a v2.Grant from a v3.GrantRecord. The output
// has stub Entitlement / Principal — only the identity fields are
// populated. Callers that need the full v2.Entitlement / v2.Resource
// must hydrate via separate Get on those record types.
//
// This stub-hydration approach matches the v3 design where references
// are identity-only.
//
// The expansion payload stored in the dedicated Expansion field is
// re-attached to the v2.Grant's Annotations list so downstream
// callers (the syncer's expander, ListWithAnnotations consumers)
// see the GrantExpandable annotation that V2GrantToV3 stripped at
// write time. Matches the SQLite read path.
func V3GrantToV2(r *v3.GrantRecord) *v2.Grant {
	if r == nil {
		return nil
	}
	anns := r.GetAnnotations()
	if annotation := expansionRecordToV2(r.GetExpansion()); annotation != nil {
		if a, err := anypb.New(annotation); err == nil {
			anns = append(anns, a)
		}
	}
	return v2.Grant_builder{
		Id:          r.GetExternalId(),
		Entitlement: entitlementRefToStub(r.GetEntitlement()),
		Principal:   principalRefToStubResource(r.GetPrincipal()),
		Annotations: anns,
		Sources:     v3GrantSourcesToV2(r.GetSources()),
	}.Build()
}

// expansionRecordToV2 translates a v3 GrantExpandableRecord into the
// v2 GrantExpandable annotation. Returns nil when the record has no
// expansion, so callers that gate on `Annotation != nil` (e.g. the
// syncer's processGrantsWithExternalPrincipals and c1's
// fileClientWrapper) stay correct. Mirrors the SQLite reader's
// translation in pkg/dotc1z/c1file_store.go's
// grantAnnotationRowsFromInternal.
func expansionRecordToV2(exp *v3.GrantExpandableRecord) *v2.GrantExpandable {
	if exp == nil {
		return nil
	}
	return v2.GrantExpandable_builder{
		EntitlementIds:  exp.GetEntitlementIds(),
		Shallow:         exp.GetShallow(),
		ResourceTypeIds: exp.GetResourceTypeIds(),
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
	parent := r.GetParentResourceId()
	return v3.PrincipalRef_builder{
		ResourceTypeId:       r.GetId().GetResourceType(),
		ResourceId:           r.GetId().GetResource(),
		ParentResourceTypeId: parent.GetResourceType(),
		ParentResourceId:     parent.GetResource(),
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
		if parent := p.GetParentResourceId(); parent != nil {
			princRef.SetParentResourceTypeId(parent.GetResourceType())
			princRef.SetParentResourceId(parent.GetResource())
		}
	}
	a.grantRecords = append(a.grantRecords, v3.GrantRecord{})
	rec := &a.grantRecords[len(a.grantRecords)-1]
	rec.SetExternalId(g.GetId())
	if entRef != nil {
		rec.SetEntitlement(entRef)
	}
	if princRef != nil {
		rec.SetPrincipal(princRef)
	}
	expansion, anns := extractV2Expansion(g.GetAnnotations())
	if len(anns) > 0 {
		rec.SetAnnotations(anns)
	}
	if expansion != nil {
		rec.SetExpansion(expansion)
		rec.SetNeedsExpansion(true)
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
	var parent *v2.ResourceId
	if ref.GetParentResourceId() != "" {
		parent = v2.ResourceId_builder{
			ResourceType: ref.GetParentResourceTypeId(),
			Resource:     ref.GetParentResourceId(),
		}.Build()
	}
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: ref.GetResourceTypeId(),
			Resource:     ref.GetResourceId(),
		}.Build(),
		ParentResourceId: parent,
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

// V2ResourceToV3 maps v2.Resource → v3.ResourceRecord. syncID is not stored in
// the value; Pebble keys carry sync scope.
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
		ExternalId:        rt.GetId(),
		DisplayName:       rt.GetDisplayName(),
		Traits:            traits,
		Annotations:       rt.GetAnnotations(),
		Description:       rt.GetDescription(),
		SourcedExternally: rt.GetSourcedExternally(),
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
		Id:                r.GetExternalId(),
		DisplayName:       r.GetDisplayName(),
		Traits:            traits,
		Annotations:       r.GetAnnotations(),
		Description:       r.GetDescription(),
		SourcedExternally: r.GetSourcedExternally(),
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
// Captures `slug` and `grantable_to` from v2 — these were silently
// dropped before, breaking pkg/sync/syncer's entitlement-id
// construction and the access-review report's principal-type
// narrowing. grantable_to is stored as a list of resource_type ids;
// the full v2.ResourceType is rehydrated on read via the
// resource_types table when callers need it.
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
	var grantableTo []string
	if gs := e.GetGrantableTo(); len(gs) > 0 {
		grantableTo = make([]string, 0, len(gs))
		for _, rt := range gs {
			if id := rt.GetId(); id != "" {
				grantableTo = append(grantableTo, id)
			}
		}
	}
	return v3.EntitlementRecord_builder{
		ExternalId:                 e.GetId(),
		Resource:                   res,
		DisplayName:                e.GetDisplayName(),
		Description:                e.GetDescription(),
		Purpose:                    purposeToString(e.GetPurpose()),
		Annotations:                e.GetAnnotations(),
		Slug:                       e.GetSlug(),
		GrantableToResourceTypeIds: grantableTo,
	}.Build()
}

// V3EntitlementToV2 reverses V2EntitlementToV3. The Resource side is
// hydrated as a stub (identity only); callers that need the full v2
// Resource must hydrate via the engine's GetResourceRecord. The
// grantable_to list is hydrated as ResourceType id-stubs — callers
// who need the full ResourceType (display name, traits) must look
// it up via the resource_types table.
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
	var grantableTo []*v2.ResourceType
	if ids := r.GetGrantableToResourceTypeIds(); len(ids) > 0 {
		grantableTo = make([]*v2.ResourceType, 0, len(ids))
		for _, id := range ids {
			grantableTo = append(grantableTo, v2.ResourceType_builder{Id: id}.Build())
		}
	}
	return v2.Entitlement_builder{
		Id:          r.GetExternalId(),
		Resource:    resource,
		DisplayName: r.GetDisplayName(),
		Description: r.GetDescription(),
		Purpose:     stringToPurpose(r.GetPurpose()),
		Annotations: r.GetAnnotations(),
		Slug:        r.GetSlug(),
		GrantableTo: grantableTo,
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
