package pebble

import (
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// grantReadArena pre-allocates GrantRecord + nested EntitlementRef
// + PrincipalRef slots for a single PaginateGrantsBySync call.
// nextSlot returns the i-th GrantRecord with its nested pointers
// already pointing into the arena's per-row EntitlementRef /
// PrincipalRef slices.
//
// The vtproto-generated GrantRecord.UnmarshalVT uses the pattern:
//
//	if m.Entitlement == nil { m.Entitlement = &EntitlementRef{} }
//	if err := m.Entitlement.UnmarshalVT(...); ...
//
// — i.e. it reuses an existing nested pointer instead of always
// heap-allocating a new one. Pre-pointing those at our arena
// collapses the per-record nested allocations (2 mallocs each at
// 1M scale = 2M tiny objects → ~50ms scanObjectsSmall in profile)
// into 3 slice allocations sized to the page limit.
//
// Lifetime. The arena lives for the duration of one
// PaginateGrantsBySync call. The returned []*v3.GrantRecord
// elements all point into the arena's backing arrays; the caller
// retains those pointers (the engine returns them up through the
// adapter). Go's GC keeps the backing arrays alive as long as any
// element pointer is live, which is correct — the arena's
// "lifetime" is the union of all retained record pointers.
//
// Pointer stability. We pre-size each slice to exactly `n` and
// never append past that; the runtime never reallocates a slice
// that doesn't grow. So &arena.ents[i] is stable for the rest of
// the program's view of arena.ents[i].
type grantReadArena struct {
	grants []v3.GrantRecord
	ents   []v3.EntitlementRef
	princs []v3.PrincipalRef
}

func newGrantReadArena(n int) *grantReadArena {
	return &grantReadArena{
		grants: make([]v3.GrantRecord, n),
		ents:   make([]v3.EntitlementRef, n),
		princs: make([]v3.PrincipalRef, n),
	}
}

// nextSlot returns slot i with Entitlement + Principal pre-pointed
// at the i-th arena nested slots, ready for UnmarshalVT. Callers
// must ensure 0 <= i < cap(arena.grants); we don't bounds-check
// because the call site knows the limit.
func (a *grantReadArena) nextSlot(i int) *v3.GrantRecord {
	g := &a.grants[i]
	*g = v3.GrantRecord{}
	g.Entitlement = &a.ents[i]
	g.Principal = &a.princs[i]
	*g.Entitlement = v3.EntitlementRef{}
	*g.Principal = v3.PrincipalRef{}
	return g
}

// reconcileAbsentFields restores the proto "absent ⇒ nil"
// semantic. The arena pre-points Entitlement / Principal at zero
// structs so vtproto can UnmarshalVT into them without allocating;
// for records whose stored bytes don't include the field, the
// nested struct stays zero. We then null the pointer out so
// callers see GetEntitlement() == nil (proto-correct) instead of
// a non-nil pointer to a zero struct (arena leak).
//
// "All exported scalar fields zero" is the practical signal —
// EntitlementRef / PrincipalRef contain only string fields plus
// protoimpl bookkeeping that UnmarshalVT doesn't touch. A
// stored-but-deliberately-all-empty record (vanishingly rare) is
// also treated as absent; that's a documented trade.
func (a *grantReadArena) reconcileAbsentFields(g *v3.GrantRecord) {
	if g.Entitlement != nil &&
		g.Entitlement.ResourceTypeId == "" &&
		g.Entitlement.ResourceId == "" &&
		g.Entitlement.EntitlementId == "" {
		g.Entitlement = nil
	}
	if g.Principal != nil &&
		g.Principal.ResourceTypeId == "" &&
		g.Principal.ResourceId == "" {
		g.Principal = nil
	}
}
