import C1z.Codec

/-!
# Structural source identities

Models `pkg/dotc1z/engine/pebble/identity.go` and the primary-key
encoders in `keys.go`. Each record kind has a structural identity; the
primary key is a deterministic function of that identity and nothing
else. In particular a grant's own `external_id` is NOT part of its
identity or key (identity.go `grantIdentity`), and an entitlement's raw
`external_id` is stored through the strip rule
(`entitlementIdentityFromParts`): if it begins with
`resource_type_id ":" resource_id ":"` that prefix is dropped and a flag
byte `"1"` is recorded, otherwise the flag is `"0"` and the whole id is
the tail.

Proved here:

- the strip rule round-trips, so the entitlement identity is a bijective
  re-encoding of `(rt, rid, external_id)`;
- each identity's key tuple is injective, hence (with `Codec`) distinct
  identities never share a primary key;
- equal raw ids under different owners coexist;
- grants that differ only in `external_id` share a key (an honest
  negative result: the store keeps one row for them, last write wins).

Out of scope: the bare-id lookup paths (`lookup.go`), index keys, the
`WellFormed` checks' enforcement site (`identity.go` rejects empty
components for entitlements and grants; resources and resource types
accept empty ids — see the research notes in README).
-/

namespace C1z

open Codec

/-- `':'`. -/
def colon : Byte := 0x3a

/-- Identity of a resource type: its external id (`encodeResourceTypeKey`). -/
structure ResourceTypeId where
  extId : Bytes
  deriving Repr, DecidableEq

/-- Identity of a resource: owning resource type id and resource id
(`encodeResourceKey`). -/
structure ResourceId where
  rt : Bytes
  rid : Bytes
  deriving Repr, DecidableEq

/-- Structural identity of an entitlement as the producer states it:
owning resource plus the raw external id. -/
structure EntitlementId where
  rt : Bytes
  rid : Bytes
  ext : Bytes
  deriving Repr, DecidableEq

/-- Structural identity of a grant: entitlement identity plus principal
resource type and id. The grant `external_id` is deliberately absent. -/
structure GrantId where
  ent : EntitlementId
  prt : Bytes
  prid : Bytes
  deriving Repr, DecidableEq

/-- Components the engine stores for an entitlement
(`entitlementIdentity` in identity.go). -/
structure EntitlementStored where
  rt : Bytes
  rid : Bytes
  stripped : Bool
  tail : Bytes
  deriving Repr, DecidableEq

/-- `rt ":" rid ":"`, the prefix the strip rule removes. -/
def entPrefix (rt rid : Bytes) : Bytes := rt ++ colon :: rid ++ [colon]

/-- The strip rule (`entitlementIdentityFromParts`). -/
def compressEnt (e : EntitlementId) : EntitlementStored :=
  if entPrefix e.rt e.rid <+: e.ext then
    { rt := e.rt, rid := e.rid, stripped := true, tail := e.ext.drop (entPrefix e.rt e.rid).length }
  else
    { rt := e.rt, rid := e.rid, stripped := false, tail := e.ext }

/-- Rebuild the raw external id (`entitlementIdentity.externalID`). -/
def expandEnt (s : EntitlementStored) : EntitlementId :=
  { rt := s.rt, rid := s.rid, ext := if s.stripped then entPrefix s.rt s.rid ++ s.tail else s.tail }

/-- The flag key component (`idFlagStripped = "1"`, `idFlagOpaque = "0"`). -/
def flagBytes : Bool → Bytes
  | true => [0x31]
  | false => [0x30]

/-! ## Key tuples -/

def ResourceTypeId.tuple (r : ResourceTypeId) : List Bytes := [r.extId]

def ResourceId.tuple (r : ResourceId) : List Bytes := [r.rt, r.rid]

def EntitlementStored.tuple (s : EntitlementStored) : List Bytes :=
  [s.rt, s.rid, flagBytes s.stripped, s.tail]

def EntitlementId.tuple (e : EntitlementId) : List Bytes := (compressEnt e).tuple

def GrantId.tuple (g : GrantId) : List Bytes := g.ent.tuple ++ [g.prt, g.prid]

def ResourceTypeId.key (r : ResourceTypeId) : Bytes := encodeKey .resourceType r.tuple
def ResourceId.key (r : ResourceId) : Bytes := encodeKey .resource r.tuple
def EntitlementId.key (e : EntitlementId) : Bytes := encodeKey .entitlement e.tuple
def GrantId.key (g : GrantId) : Bytes := encodeKey .grant g.tuple

/-! ## Strip rule -/

/-- The strip rule loses nothing. -/
theorem expandEnt_compressEnt (e : EntitlementId) : expandEnt (compressEnt e) = e := by
  unfold compressEnt
  split
  next hp =>
    obtain ⟨t, ht⟩ := hp
    simp only [expandEnt, ite_true]
    rw [← ht, List.drop_left, ht]
  next =>
    simp only [expandEnt, Bool.false_eq_true, ite_false]

theorem compressEnt_injective {e f : EntitlementId} (h : compressEnt e = compressEnt f) : e = f := by
  rw [← expandEnt_compressEnt e, ← expandEnt_compressEnt f, h]

/-- The flag is faithful: a stored identity with `stripped = true` always
expands to an id carrying the prefix, and one with `stripped = false`
never does. So the two stored shapes partition the external ids for a
fixed owner. -/
theorem compressEnt_stripped_iff (e : EntitlementId) :
    (compressEnt e).stripped = true ↔ entPrefix e.rt e.rid <+: e.ext := by
  unfold compressEnt
  split <;> simp_all

/-! ## Tuple injectivity -/

theorem EntitlementId.tuple_length (e : EntitlementId) : e.tuple.length = 4 := rfl

theorem EntitlementId.tuple_ne_nil (e : EntitlementId) : e.tuple ≠ [] := by
  intro h
  have := congrArg List.length h
  rw [EntitlementId.tuple_length] at this
  exact absurd this (by decide)

theorem GrantId.tuple_length (g : GrantId) : g.tuple.length = 6 := by
  simp only [GrantId.tuple, List.length_append, EntitlementId.tuple_length, List.length_cons, List.length_nil]

theorem flagBytes_injective {a b : Bool} (h : flagBytes a = flagBytes b) : a = b := by
  cases a <;> cases b
  all_goals first | rfl | exact absurd h (by decide)

theorem EntitlementStored.tuple_injective {s t : EntitlementStored} (h : s.tuple = t.tuple) : s = t := by
  obtain ⟨a, b, c, d⟩ := s
  obtain ⟨a', b', c', d'⟩ := t
  simp only [EntitlementStored.tuple, List.cons.injEq, and_true] at h
  obtain ⟨rfl, rfl, hf, rfl⟩ := h
  rw [flagBytes_injective hf]

theorem EntitlementId.tuple_injective {e f : EntitlementId} (h : e.tuple = f.tuple) : e = f := by
  exact compressEnt_injective (EntitlementStored.tuple_injective h)

theorem GrantId.tuple_injective {g h : GrantId} (heq : g.tuple = h.tuple) : g = h := by
  obtain ⟨e, p, q⟩ := g
  obtain ⟨e', p', q'⟩ := h
  simp only [GrantId.tuple] at heq
  obtain ⟨h1, h2⟩ := List.append_inj heq (by rw [EntitlementId.tuple_length, EntitlementId.tuple_length])
  simp only [List.cons.injEq, and_true] at h2
  obtain ⟨rfl, rfl⟩ := h2
  rw [EntitlementId.tuple_injective h1]

theorem ResourceId.tuple_injective {r s : ResourceId} (h : r.tuple = s.tuple) : r = s := by
  obtain ⟨a, b⟩ := r
  obtain ⟨a', b'⟩ := s
  simp only [ResourceId.tuple, List.cons.injEq, and_true] at h
  obtain ⟨rfl, rfl⟩ := h
  rfl

/-! ## Key injectivity (no aliasing) -/

theorem ResourceTypeId.key_injective {r s : ResourceTypeId} (h : r.key = s.key) : r = s := by
  obtain ⟨a⟩ := r
  obtain ⟨b⟩ := s
  have h2 := (encodeKey_injective (es := ResourceTypeId.tuple ⟨a⟩) (fs := ResourceTypeId.tuple ⟨b⟩) rfl h).2
  simp only [ResourceTypeId.tuple, List.cons.injEq, and_true] at h2
  rw [h2]

theorem ResourceId.key_injective {r s : ResourceId} (h : r.key = s.key) : r = s := by
  exact ResourceId.tuple_injective (encodeKey_injective (es := r.tuple) (fs := s.tuple) rfl h).2

theorem EntitlementId.key_injective {e f : EntitlementId} (h : e.key = f.key) : e = f := by
  exact EntitlementId.tuple_injective (encodeKey_injective (es := e.tuple) (fs := f.tuple) (by rw [EntitlementId.tuple_length, EntitlementId.tuple_length]) h).2

theorem GrantId.key_injective {g h : GrantId} (heq : g.key = h.key) : g = h := by
  exact GrantId.tuple_injective (encodeKey_injective (es := g.tuple) (fs := h.tuple) (by rw [GrantId.tuple_length, GrantId.tuple_length]) heq).2

/-- Keys of different kinds never collide, whatever their tails. -/
theorem key_kind_disjoint {j k : Kind} {es fs : List Bytes} (hne : j ≠ k) :
    encodeKey j es ≠ encodeKey k fs := by
  intro h
  unfold encodeKey at h
  exact hne (header_injective (List.append_inj_left h rfl))

/-! ## Coexistence of equal raw ids -/

/-- The same resource id under two resource types gives two keys. -/
theorem ResourceId.key_ne_of_rt_ne {r s : ResourceId} (h : r.rt ≠ s.rt) : r.key ≠ s.key := by
  exact fun hk => h (congrArg ResourceId.rt (ResourceId.key_injective hk))

/-- The same entitlement external id on two resources gives two keys. -/
theorem EntitlementId.key_ne_of_rid_ne {e f : EntitlementId} (h : e.rid ≠ f.rid) : e.key ≠ f.key := by
  exact fun hk => h (congrArg EntitlementId.rid (EntitlementId.key_injective hk))

/-- All grants of one entitlement lie under that entitlement's scan prefix,
which is why "grants of entitlement X" is a prefix scan (`encodeGrantPrefix`
family in keys.go). -/
theorem GrantId.key_under_entitlement_prefix (g : GrantId) :
    encodeScanPrefix .grant g.ent.tuple <+: g.key := by
  unfold GrantId.key
  rw [encodeScanPrefix_isPrefix_iff (EntitlementId.tuple_ne_nil g.ent)]
  refine ⟨rfl, List.prefix_append _ _, ?_⟩
  rw [GrantId.tuple_length, EntitlementId.tuple_length]
  decide

/-- Only grants of entitlement `e` lie under `e`'s scan prefix. -/
theorem GrantId.ent_eq_of_key_under_prefix {e : EntitlementId} {g : GrantId}
    (h : encodeScanPrefix .grant e.tuple <+: g.key) : g.ent = e := by
  unfold GrantId.key at h
  rw [encodeScanPrefix_isPrefix_iff (EntitlementId.tuple_ne_nil e)] at h
  obtain ⟨-, ⟨t, ht⟩, -⟩ := h
  exact (EntitlementId.tuple_injective
    (List.append_inj_left ht (by rw [EntitlementId.tuple_length, EntitlementId.tuple_length]))).symm

/-! ## Grant records and the public id -/

/-- A grant as written: its structural identity plus the producer's
`external_id`, which the store keeps only in the value. -/
structure GrantRecord where
  id : GrantId
  externalId : Bytes
  deriving Repr, DecidableEq

def GrantRecord.key (r : GrantRecord) : Bytes := r.id.key

/-- Two grant records share a primary key exactly when their structural
identities agree; `external_id` plays no part. The store therefore holds
one row for records that differ only in `external_id`. -/
theorem GrantRecord.key_eq_iff (r s : GrantRecord) : r.key = s.key ↔ r.id = s.id := by
  exact ⟨GrantId.key_injective, congrArg GrantId.key⟩

/-- The public grant id when `external_id` is empty
(`grantIdentity.externalID` in identity.go): `ent_ext ":" prt ":" prid`. -/
def GrantId.publicId (g : GrantId) : Bytes := g.ent.ext ++ colon :: g.prt ++ colon :: g.prid

/-- Distinct structural identities can print the same public id, which is
why bare-id grant lookup can be ambiguous (`ErrAmbiguousExternalID`). -/
theorem publicId_not_injective :
    ∃ g h : GrantId, g ≠ h ∧ g.publicId = h.publicId := by
  refine ⟨⟨⟨[], [], [0x61]⟩, [0x62, colon, 0x63], [0x64]⟩,
    ⟨⟨[], [], [0x61]⟩, [0x62], [0x63, colon, 0x64]⟩, by decide, by decide⟩

/-! ## Ingest-time well-formedness

`identity.go` rejects entitlements with an empty owner and grants with
any empty component; resources and resource types accept empty ids.
These predicates state what the engine requires; nothing above depends
on them. -/

def EntitlementId.WellFormed (e : EntitlementId) : Prop := e.rt ≠ [] ∧ e.rid ≠ []

def GrantId.WellFormed (g : GrantId) : Prop :=
  g.ent.rt ≠ [] ∧ g.ent.rid ≠ [] ∧ g.ent.ext ≠ [] ∧ g.prt ≠ [] ∧ g.prid ≠ []

instance (e : EntitlementId) : Decidable e.WellFormed := by unfold EntitlementId.WellFormed; infer_instance
instance (g : GrantId) : Decidable g.WellFormed := by unfold GrantId.WellFormed; infer_instance

/-! ## Witnesses -/

/-- `("app", "1", "app:1:member")` strips to tail `"member"`. -/
example :
    compressEnt { rt := [0x61, 0x70, 0x70], rid := [0x31], ext := [0x61, 0x70, 0x70, 0x3a, 0x31, 0x3a, 0x6d] }
      = { rt := [0x61, 0x70, 0x70], rid := [0x31], stripped := true, tail := [0x6d] } := by
  decide

/-- An id that does not carry the owner prefix is stored opaque. -/
example :
    compressEnt { rt := [0x61], rid := [0x31], ext := [0x6d] }
      = { rt := [0x61], rid := [0x31], stripped := false, tail := [0x6d] } := by
  decide

/-- Owner components may contain colons; the rule is a byte-prefix test. -/
example :
    (compressEnt { rt := [0x61, 0x3a], rid := [0x31], ext := [0x61, 0x3a, 0x3a, 0x31, 0x3a] }).stripped = true := by
  decide

end C1z
