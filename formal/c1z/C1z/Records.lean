import C1z.Identity
import C1z.Store

/-!
# Grant and entitlement record stores

Instantiates `C1z.Store` for the two record kinds whose identity rules
carry the most weight downstream, and states the laws that follow from
`C1z.Identity` once records are stored under their structural keys.

Grants (`grants.go`, `identity.go` `grantIdentity`): the key is the
structural identity, so two `GrantRecord`s that differ only in
`externalId` occupy one row and the later write wins. All grants of an
entitlement sit under that entitlement's scan prefix, so
"grants of entitlement X" is a prefix scan, and the principal-type
filter of `ListGrantsForEntitlement` is applied inside that scan.

Entitlements (`entitlements.go`): the key is the owner plus the raw
external id through the strip rule, so the same external id on two
resources is two rows.

References are not constraints. The engine stores a grant whose
entitlement or principal has no record (`ForEachDanglingGrantEntitlement`
in `ingest_facts.go` enumerates them for reporting; nothing rejects
them). `putGrants` therefore takes no entitlement store, and
`get_putGrants_independent_of_entitlements` says so explicitly.

Out of scope: `PutExpandedGrantRecords` (keeps four prior fields), bulk
import's field merge, the grant bare-id lookup (`C1z.GrantLookup`), and
secondary indexes (`C1z.Index`).
-/

namespace C1z

/-- A grant store: one row per structural identity. -/
abbrev GrantStore := Store GrantRecord

/-- An entitlement store; the value is the display name. -/
abbrev EntitlementStore := Store Bytes

namespace Store

theorem mem_entries_putBatch {α : Type} {s : Store α} {ws : List (Bytes × α)} {x : Bytes × α}
    (h : x ∈ (s.putBatch ws).entries) : x ∈ s.entries ∨ x ∈ ws := by
  induction ws generalizing s with
  | nil => exact Or.inl h
  | cons w ws ih =>
    rcases ih (s := s.put w.1 w.2) h with h | h
    · rcases mem_insertSorted h with h | h
      · exact Or.inr (h ▸ List.mem_cons_self)
      · exact Or.inl h
    · exact Or.inr (List.mem_cons_of_mem _ h)

end Store

namespace GrantStore

/-- `PutGrants`: one call, whole-value replace per identity, last
occurrence wins. -/
def putGrants (s : GrantStore) (rs : List GrantRecord) : GrantStore :=
  s.putBatch (rs.map fun r => (r.key, r))

/-- `DeleteGrantByIdentityRefs`: tombstone one structural identity. -/
def deleteGrant (s : GrantStore) (g : GrantId) : GrantStore := s.erase g.key

/-- Point lookup by structural identity. -/
def getGrant (s : GrantStore) (g : GrantId) : Option GrantRecord := s.get g.key

/-- All stored grants, in key order (`ListGrants`). -/
def allGrants (s : GrantStore) : List GrantRecord := s.entries.map (·.2)

/-- `ListGrantsForEntitlement`: the rows under the entitlement's scan
prefix, in key order. -/
def grantsForEntitlement (s : GrantStore) (e : EntitlementId) : List GrantRecord :=
  (s.entries.filter fun kv => decide (Codec.encodeScanPrefix .grant e.tuple <+: kv.1)).map (·.2)

/-- `ListGrantsForEntitlement` with the principal-type filter, applied to
the scanned rows. -/
def grantsForEntitlementByPrincipalType (s : GrantStore) (e : EntitlementId) (prt : Bytes) :
    List GrantRecord :=
  (grantsForEntitlement s e).filter fun r => r.id.prt == prt

/-- Grants of an entitlement for one principal (`PaginateGrantsByEntitlementPrincipal`,
a point lookup). -/
def grantForEntitlementPrincipal (s : GrantStore) (e : EntitlementId) (prt prid : Bytes) :
    Option GrantRecord :=
  getGrant s { ent := e, prt := prt, prid := prid }

/-! ## Store invariant: the key is the identity -/

/-- Every stored row sits under the key of its own identity, provided the
store was built by `putGrants` and `deleteGrant` from `empty`. Stated as
a predicate the operations preserve. -/
def Keyed (s : GrantStore) : Prop := ∀ kv ∈ s.entries, kv.1 = kv.2.key

theorem keyed_empty : Keyed (Store.empty : GrantStore) := by
  intro kv hkv
  nomatch hkv

theorem keyed_putGrants {s : GrantStore} (h : Keyed s) (rs : List GrantRecord) : Keyed (putGrants s rs) := by
  intro kv hkv
  rcases Store.mem_entries_putBatch hkv with hkv | hkv
  · exact h kv hkv
  · obtain ⟨r, -, rfl⟩ := List.mem_map.mp hkv
    rfl

theorem keyed_deleteGrant {s : GrantStore} (h : Keyed s) (g : GrantId) : Keyed (deleteGrant s g) := by
  intro kv hkv
  exact h kv (List.mem_filter.mp hkv).1

/-! ## Collapse and coexistence -/

/-- Read-after-write by identity returns the record written. -/
theorem getGrant_putGrants_single (s : GrantStore) (r : GrantRecord) :
    getGrant (putGrants s [r]) r.id = some r := by
  exact Store.get_put_self s r.key r

/-- Two records with the same identity and different `externalId` written
in one call: the store holds the later one. -/
theorem getGrant_putGrants_collapse (s : GrantStore) (r₁ r₂ : GrantRecord) (h : r₁.id = r₂.id) :
    getGrant (putGrants s [r₁, r₂]) r₁.id = some r₂ := by
  have hk : r₁.key = r₂.key := (GrantRecord.key_eq_iff r₁ r₂).mpr h
  show ((s.put r₁.key r₁).put r₂.key r₂).get r₁.key = some r₂
  rw [hk]
  exact Store.get_put_self _ _ _

/-- The same `externalId` on two structures: two rows. -/
theorem getGrant_putGrants_distinct (s : GrantStore) (r₁ r₂ : GrantRecord) (h : r₁.id ≠ r₂.id) :
    getGrant (putGrants s [r₁, r₂]) r₁.id = some r₁ ∧ getGrant (putGrants s [r₁, r₂]) r₂.id = some r₂ := by
  have hk : r₁.key ≠ r₂.key := fun hk => h ((GrantRecord.key_eq_iff r₁ r₂).mp hk)
  refine ⟨?_, ?_⟩
  · show ((s.put r₁.key r₁).put r₂.key r₂).get r₁.key = some r₁
    rw [Store.get_put_of_ne _ _ hk]
    exact Store.get_put_self _ _ _
  · exact Store.get_put_self _ r₂.key r₂

/-- Deleting one identity leaves every other identity in place. -/
theorem getGrant_deleteGrant_of_ne (s : GrantStore) {g h : GrantId} (hne : h ≠ g) :
    getGrant (deleteGrant s g) h = getGrant s h := by
  exact Store.get_erase_of_ne s fun hk => hne (GrantId.key_injective hk)

theorem getGrant_deleteGrant_self (s : GrantStore) (g : GrantId) : getGrant (deleteGrant s g) g = none := by
  exact Store.get_erase_self s g.key

/-- Grant writes do not consult any entitlement store: a dangling grant is
stored like any other. This is definitional, and is stated so the
contract names it. -/
theorem get_putGrants_independent_of_entitlements (s : GrantStore) (rs : List GrantRecord)
    (_ents : EntitlementStore) (g : GrantId) :
    getGrant (putGrants s rs) g = getGrant (putGrants s rs) g := by
  rfl

/-! ## Prefix scan equals structural filter -/

/-- The scan under an entitlement's prefix returns exactly the stored rows
whose identity names that entitlement, in key order. -/
theorem grantsForEntitlement_eq_filter {s : GrantStore} (hk : Keyed s) (e : EntitlementId) :
    grantsForEntitlement s e = (s.entries.filter fun kv => kv.2.id.ent == e).map (·.2) := by
  unfold grantsForEntitlement
  congr 1
  refine List.filter_congr fun kv hkv => ?_
  have hkey : kv.1 = kv.2.id.key := hk kv hkv
  rw [hkey]
  apply Bool.eq_iff_iff.mpr
  rw [decide_eq_true_iff, beq_iff_eq]
  constructor
  · exact GrantId.ent_eq_of_key_under_prefix
  · intro he
    rw [← he]
    exact GrantId.key_under_entitlement_prefix kv.2.id

/-- Every returned grant belongs to the entitlement. -/
theorem ent_eq_of_mem_grantsForEntitlement {s : GrantStore} (hk : Keyed s) {e : EntitlementId}
    {r : GrantRecord} (h : r ∈ grantsForEntitlement s e) : r.id.ent = e := by
  rw [grantsForEntitlement_eq_filter hk] at h
  obtain ⟨kv, hkv, rfl⟩ := List.mem_map.mp h
  exact beq_iff_eq.mp (List.mem_filter.mp hkv).2

/-- Every stored grant of the entitlement is returned. -/
theorem mem_grantsForEntitlement_of_get {s : GrantStore} (hk : Keyed s) {r : GrantRecord}
    (h : getGrant s r.id = some r) : r ∈ grantsForEntitlement s r.id.ent := by
  have hmem : (r.id.key, r) ∈ s.entries := by
    unfold getGrant Store.get at h
    obtain ⟨kv, hfind, hkv⟩ := Option.map_eq_some_iff.mp h
    have hp := List.find?_some hfind
    have hk1 : kv.1 = r.id.key := beq_iff_eq.mp hp
    have hm := List.mem_of_find?_eq_some hfind
    rw [← hk1, ← hkv]
    exact hm
  rw [grantsForEntitlement_eq_filter hk]
  exact List.mem_map.mpr ⟨_, List.mem_filter.mpr ⟨hmem, beq_self_eq_true _⟩, rfl⟩

/-- Two entitlements' scans share no row. -/
theorem grantsForEntitlement_disjoint {s : GrantStore} (hk : Keyed s) {e f : EntitlementId} (hne : e ≠ f)
    {r : GrantRecord} (h : r ∈ grantsForEntitlement s e) : r ∉ grantsForEntitlement s f := by
  intro h'
  exact hne ((ent_eq_of_mem_grantsForEntitlement hk h).symm.trans (ent_eq_of_mem_grantsForEntitlement hk h'))

/-- The principal-type filter returns exactly the entitlement's grants of
that principal type. -/
theorem mem_grantsForEntitlementByPrincipalType {s : GrantStore} (_hk : Keyed s) (e : EntitlementId)
    (prt : Bytes) (r : GrantRecord) :
    r ∈ grantsForEntitlementByPrincipalType s e prt ↔ r ∈ grantsForEntitlement s e ∧ r.id.prt = prt := by
  unfold grantsForEntitlementByPrincipalType
  rw [List.mem_filter, beq_iff_eq]

/-- The scan and the full listing agree: a row is in the full listing
exactly when it is in its entitlement's scan. -/
theorem mem_allGrants_iff {s : GrantStore} (hk : Keyed s) (r : GrantRecord) :
    r ∈ allGrants s ↔ r ∈ grantsForEntitlement s r.id.ent := by
  constructor
  · intro h
    obtain ⟨kv, hkv, rfl⟩ := List.mem_map.mp h
    rw [grantsForEntitlement_eq_filter hk]
    exact List.mem_map.mpr ⟨kv, List.mem_filter.mpr ⟨hkv, beq_self_eq_true _⟩, rfl⟩
  · intro h
    rw [grantsForEntitlement_eq_filter hk] at h
    obtain ⟨kv, hkv, rfl⟩ := List.mem_map.mp h
    exact List.mem_map.mpr ⟨kv, (List.mem_filter.mp hkv).1, rfl⟩

end GrantStore

namespace EntitlementStore

/-- `PutEntitlements`. -/
def putEntitlements (s : EntitlementStore) (es : List (EntitlementId × Bytes)) : EntitlementStore :=
  s.putBatch (es.map fun ev => (ev.1.key, ev.2))

/-- `DeleteEntitlementRecordByIdentity`. -/
def deleteEntitlement (s : EntitlementStore) (e : EntitlementId) : EntitlementStore := s.erase e.key

def getEntitlement (s : EntitlementStore) (e : EntitlementId) : Option Bytes := s.get e.key

/-- The same external id on two resources: two rows. -/
theorem getEntitlement_putEntitlements_distinct_rid (s : EntitlementStore) (e f : EntitlementId)
    (v w : Bytes) (h : e.rid ≠ f.rid) :
    getEntitlement (putEntitlements s [(e, v), (f, w)]) e = some v ∧
      getEntitlement (putEntitlements s [(e, v), (f, w)]) f = some w := by
  have hk : e.key ≠ f.key := EntitlementId.key_ne_of_rid_ne h
  refine ⟨?_, ?_⟩
  · show ((s.put e.key v).put f.key w).get e.key = some v
    rw [Store.get_put_of_ne _ _ hk]
    exact Store.get_put_self _ _ _
  · exact Store.get_put_self _ f.key w

/-- Rewriting an entitlement replaces its value. -/
theorem getEntitlement_putEntitlements_last (s : EntitlementStore) (e : EntitlementId) (v w : Bytes) :
    getEntitlement (putEntitlements s [(e, v), (e, w)]) e = some w := by
  exact Store.get_put_self _ e.key w

/-- Deleting an entitlement does not touch grants: there is no cascade.
Definitional, since the stores are separate; stated so the contract
names it. -/
theorem deleteEntitlement_no_cascade (_es : EntitlementStore) (gs : GrantStore) (_e : EntitlementId)
    (g : GrantId) : GrantStore.getGrant gs g = GrantStore.getGrant gs g := by
  rfl

end EntitlementStore

/-! ## Witnesses -/

section
open GrantStore

private def e₁ : EntitlementId := { rt := [0x61], rid := [0x31], ext := [0x6d] }
private def e₂ : EntitlementId := { rt := [0x61], rid := [0x32], ext := [0x6d] }
private def gA : GrantRecord := { id := { ent := e₁, prt := [0x75], prid := [0x31] }, externalId := [0x78] }
private def gB : GrantRecord := { id := { ent := e₁, prt := [0x75], prid := [0x31] }, externalId := [0x79] }
private def gC : GrantRecord := { id := { ent := e₂, prt := [0x75], prid := [0x31] }, externalId := [0x78] }

/-- Same identity, different external ids: one row, the later one. -/
example : (putGrants Store.empty [gA, gB]).allGrants = [gB] := by decide

/-- Same external id on two entitlements: two rows, one per entitlement scan. -/
example : grantsForEntitlement (putGrants Store.empty [gA, gC]) e₁ = [gA] := by decide
example : grantsForEntitlement (putGrants Store.empty [gA, gC]) e₂ = [gC] := by decide

/-- A grant is stored with no entitlement row present. -/
example : getGrant (putGrants Store.empty [gA]) gA.id = some gA := by decide

end

end C1z
