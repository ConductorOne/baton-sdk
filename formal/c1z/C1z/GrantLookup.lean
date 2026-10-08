import C1z.Records
import C1z.Result

/-!
# Grant lookup by bare external id

Models `resolveGrantIdentityByExternalID` in `lookup.go`, the path
behind `GetGrant` and `DeleteGrant` when the caller supplies only a
grant id string. The rule is not the exactly-one match over stored ids
that `C1z.Result.resolveBare` describes for entitlements; it has two
phases and the second runs only when the first finds nothing.

Phase 1, candidates: the query is split at colons into
`entitlement id : principal type : principal id`, and for each split a
grant identity is reconstructed and probed by key. A probe hits when the
row exists and its stored `external_id` is empty or equals the query.
Reconstruction reaches a stored grant exactly when its rebuilt public
id equals the query and its entitlement identity is reachable: stripped
entitlement identities are reachable through the direct five-way split,
and opaque ones only when an entitlement row with that exact identity
exists (the in-memory multimap resolves the entitlement id to stored
rows). Fewer than two colons yields no candidates; more than 64 colons
is ambiguous outright. A single hit is found, several are ambiguous.

Phase 2, scan: only when phase 1 found nothing, every grant whose stored
`external_id` equals the query counts, and the scan stops at the second
hit with ambiguous.

Consequences this module states, each pinned by a `grant_bare_id`
oracle case:

- `found_mem`, `found_matches`: a found grant is stored and either
  carries the query as its `external_id` or has an empty `external_id`
  and the query as its public id.
- `masking`: a stored grant whose `external_id` equals the query can be
  hidden by a phase-1 hit on a different grant, with no ambiguity
  reported.
- `opaque_unreachable`: a grant with an empty `external_id` and an opaque
  entitlement id is not found by its public id until an entitlement row
  with that identity exists, even though `ListGrants` shows that id.
- `custom_ext_hides_public`: a grant with a non-empty custom `external_id`
  is not found by its public id.
- `empty_query`: the empty query is found by scan among grants with an
  empty `external_id`.

Out of scope: the 4096-candidate cap (unreachable below 19 colons), the
error message's match count (the scan reports 2 because it stops
early), and `DeleteGrant`'s no-op on not-found.
-/

namespace C1z
namespace GrantLookup

open Result

/-- `max colons` before `resolveGrantIdentityByExternalID` returns
ambiguous without probing (`lookup.go`). -/
def maxColons : Nat := 64

def colons (q : Bytes) : Nat := q.count colon

/-- The public id a reader shows: the stored `external_id` if non-empty,
otherwise the rebuilt `ent_ext:prt:prid` (`publicGrantRecordID`). -/
def publicIdOf (r : GrantRecord) : Bytes := if r.externalId = [] then r.id.publicId else r.externalId

/-- Whether candidate reconstruction can reach a stored grant's identity. -/
def reachable (ents : EntitlementStore) (r : GrantRecord) : Bool :=
  (compressEnt r.id.ent).stripped || (ents.getEntitlement r.id.ent).isSome

/-- Phase 1 hits. -/
def candidates (s : GrantStore) (ents : EntitlementStore) (q : Bytes) : List GrantRecord :=
  if colons q < 2 then []
  else s.allGrants.filter fun r =>
    r.id.publicId == q && (r.externalId == [] || r.externalId == q) && reachable ents r

/-- Phase 2 hits. -/
def scan (s : GrantStore) (q : Bytes) : List GrantRecord :=
  s.allGrants.filter fun r => r.externalId == q

/-- `resolveGrantIdentityByExternalID`. -/
def resolve (s : GrantStore) (ents : EntitlementStore) (q : Bytes) : Resolution GrantRecord :=
  if maxColons < colons q then .ambiguous
  else match candidates s ents q with
    | [] => resolveBare (scan s q)
    | [r] => .found r
    | _ :: _ :: _ => .ambiguous

/-! ## Soundness -/

/-- A found grant is a phase-1 candidate or the single scan hit. -/
private theorem mem_of_found {s : GrantStore} {ents : EntitlementStore} {q : Bytes} {r : GrantRecord}
    (h : resolve s ents q = .found r) : r ∈ candidates s ents q ∨ r ∈ scan s q := by
  unfold resolve at h
  split at h
  · cases h
  · split at h
    · rw [resolveBare_found_iff] at h
      rw [h]
      exact Or.inr (List.mem_singleton_self _)
    · rename_i r' hc
      cases h
      rw [hc]
      exact Or.inl (List.mem_singleton_self _)
    · cases h

private theorem mem_candidates {s : GrantStore} {ents : EntitlementStore} {q : Bytes} {r : GrantRecord}
    (h : r ∈ candidates s ents q) :
    r ∈ s.allGrants ∧ r.id.publicId = q ∧ (r.externalId = [] ∨ r.externalId = q) := by
  unfold candidates at h
  split at h
  · exact absurd h List.not_mem_nil
  · simp only [List.mem_filter, Bool.and_eq_true, Bool.or_eq_true, beq_iff_eq] at h
    exact ⟨h.1, h.2.1.1, h.2.1.2⟩

private theorem mem_scan {s : GrantStore} {q : Bytes} {r : GrantRecord} (h : r ∈ scan s q) :
    r ∈ s.allGrants ∧ r.externalId = q := by
  simp only [scan, List.mem_filter, beq_iff_eq] at h
  exact h

/-- A found grant is a stored grant. -/
theorem found_mem {s : GrantStore} {ents : EntitlementStore} {q : Bytes} {r : GrantRecord}
    (h : resolve s ents q = .found r) : r ∈ s.allGrants := by
  rcases mem_of_found h with h | h
  · exact (mem_candidates h).1
  · exact (mem_scan h).1

/-- A found grant carries the query as its stored id, or has an empty
stored id and the query as its public id. -/
theorem found_matches {s : GrantStore} {ents : EntitlementStore} {q : Bytes} {r : GrantRecord}
    (h : resolve s ents q = .found r) :
    r.externalId = q ∨ (r.externalId = [] ∧ r.id.publicId = q) := by
  rcases mem_of_found h with h | h
  · obtain ⟨_, hp, he | he⟩ := mem_candidates h
    · exact Or.inr ⟨he, hp⟩
    · exact Or.inl he
  · exact Or.inl (mem_scan h).2

/-- When no candidate hits, the result is the exactly-one rule over stored
ids. -/
theorem resolve_eq_resolveBare_scan {s : GrantStore} {ents : EntitlementStore} {q : Bytes}
    (hc : candidates s ents q = []) (hq : colons q ≤ maxColons) :
    resolve s ents q = resolveBare (scan s q) := by
  unfold resolve
  simp only [Nat.not_lt.mpr hq, ↓reduceIte, hc]

/-- Two stored grants with the query as stored id and no candidate hit:
ambiguous. -/
theorem ambiguous_of_two_scan {s : GrantStore} {ents : EntitlementStore} {q : Bytes}
    (hc : candidates s ents q = []) (hq : colons q ≤ maxColons) {r₁ r₂ : GrantRecord}
    (h₁ : r₁ ∈ s.allGrants) (h₂ : r₂ ∈ s.allGrants) (hne : r₁ ≠ r₂)
    (e₁ : r₁.externalId = q) (e₂ : r₂.externalId = q) : resolve s ents q = .ambiguous := by
  rw [resolve_eq_resolveBare_scan hc hq, resolveBare_ambiguous_iff]
  have m₁ : r₁ ∈ scan s q := by simp only [scan, List.mem_filter, beq_iff_eq]; exact ⟨h₁, e₁⟩
  have m₂ : r₂ ∈ scan s q := by simp only [scan, List.mem_filter, beq_iff_eq]; exact ⟨h₂, e₂⟩
  match hl : scan s q with
  | [] => rw [hl] at m₁; exact absurd m₁ List.not_mem_nil
  | [a] =>
    rw [hl, List.mem_singleton] at m₁ m₂
    exact absurd (m₁.trans m₂.symm) hne
  | _ :: _ :: _ => simp only [List.length_cons]; omega

/-! ## Witness data

`group`, `g1`, `member`, `user`, `u1` as bytes. -/

private def bGroup : Bytes := [0x67, 0x72, 0x6f, 0x75, 0x70]
private def bG1 : Bytes := [0x67, 0x31]
private def bMember : Bytes := [0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72]
private def bUser : Bytes := [0x75, 0x73, 0x65, 0x72]
private def bU1 : Bytes := [0x75, 0x31]

/-- Stripped entitlement `group:g1:member` on `(group, g1)`. -/
private def eStripped : EntitlementId := { rt := bGroup, rid := bG1, ext := bGroup ++ colon :: bG1 ++ colon :: bMember }
/-- Opaque entitlement `member` on `(group, g1)`. -/
private def eOpaque : EntitlementId := { rt := bGroup, rid := bG1, ext := bMember }

private def gPublic : GrantRecord := { id := { ent := eStripped, prt := bUser, prid := bU1 }, externalId := [] }
private def gOpaque : GrantRecord := { id := { ent := eOpaque, prt := bUser, prid := bU1 }, externalId := [] }
private def gCustom : GrantRecord := { id := { ent := eStripped, prt := bUser, prid := bU1 }, externalId := [0x63] }
/-- Empty-id grant on a stripped entitlement; its public id is the masking query. -/
private def gMask : GrantRecord := gPublic
/-- A different structure whose stored id is `gMask`'s public id. -/
private def gMasked : GrantRecord :=
  { id := { ent := eOpaque, prt := bGroup, prid := bG1 }, externalId := gPublic.id.publicId }

/-! ## Negative results, each with a concrete instance -/

/-- Masking: a grant whose stored id equals the query is hidden by a
phase-1 hit on another grant, and no ambiguity is reported. -/
theorem masking :
    ∃ (s : GrantStore) (ents : EntitlementStore) (q : Bytes) (r₁ r₂ : GrantRecord),
      r₂ ∈ s.allGrants ∧ r₂.externalId = q ∧ r₁ ≠ r₂ ∧ resolve s ents q = .found r₁ := by
  refine ⟨GrantStore.putGrants Store.empty [gMask, gMasked], Store.empty, gMask.id.publicId, gMask, gMasked,
    by decide, by decide, by decide, by decide⟩

/-- An empty-id grant with an opaque entitlement id is not found by its
public id without an entitlement row, and is found once the row exists. -/
theorem opaque_unreachable :
    ∃ (s : GrantStore) (ents : EntitlementStore) (r : GrantRecord) (v : Bytes),
      r ∈ s.allGrants ∧ r.externalId = [] ∧ (compressEnt r.id.ent).stripped = false ∧
      resolve s Store.empty r.id.publicId = .notFound ∧
      resolve s (EntitlementStore.putEntitlements ents [(r.id.ent, v)]) r.id.publicId = .found r := by
  refine ⟨GrantStore.putGrants Store.empty [gOpaque], Store.empty, gOpaque, [],
    by decide, by decide, by decide, by decide, by decide⟩

/-- A grant with a custom stored id is not found by its rebuilt public id. -/
theorem custom_ext_hides_public :
    ∃ (s : GrantStore) (r : GrantRecord),
      r ∈ s.allGrants ∧ r.externalId ≠ [] ∧ r.externalId ≠ r.id.publicId ∧
      resolve s Store.empty r.id.publicId = .notFound ∧ resolve s Store.empty r.externalId = .found r := by
  refine ⟨GrantStore.putGrants Store.empty [gCustom], gCustom, by decide, by decide, by decide, by decide, by decide⟩

/-- The empty query resolves by scan among empty-id grants. -/
theorem empty_query (s : GrantStore) (ents : EntitlementStore) :
    resolve s ents [] = resolveBare (s.allGrants.filter fun r => r.externalId == []) := by
  rfl

/-! ## Witnesses -/

/-- Found by public id through the direct split. -/
example : resolve (GrantStore.putGrants Store.empty [gPublic]) Store.empty gPublic.id.publicId = .found gPublic := by decide

/-- Opaque: not found without the entitlement row, found with it. -/
example : resolve (GrantStore.putGrants Store.empty [gOpaque]) Store.empty gOpaque.id.publicId = .notFound := by decide
example :
    resolve (GrantStore.putGrants Store.empty [gOpaque]) (EntitlementStore.putEntitlements Store.empty [(eOpaque, [])]) gOpaque.id.publicId
      = .found gOpaque := by decide

/-- Custom id: the public id misses, the custom id hits by scan. -/
example : resolve (GrantStore.putGrants Store.empty [gCustom]) Store.empty gCustom.id.publicId = .notFound := by decide
example : resolve (GrantStore.putGrants Store.empty [gCustom]) Store.empty [0x63] = .found gCustom := by decide

end GrantLookup
end C1z
