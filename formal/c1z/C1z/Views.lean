import C1z.Records
import C1z.Index
import C1z.Stream

/-!
# Agreement between reader views

One statement, `views_agree`, that the read paths a consumer can choose
between describe the same store: point lookup by structural identity,
the full paginated listing, the per-entitlement scan, the patient
stream over each of those, and the `by_principal` index walk once the
index is complete. The pieces are proved in `C1z.Records`, `C1z.Index`
and `C1z.Stream`; this module names the combined contract and states
the known exceptions next to it.

Exceptions the engine has, each stated as its own theorem elsewhere and
listed here so the agreement claim is read with them:

- The index-backed views (`ListGrantsForPrincipal`,
  `ListGrantsForResourceType`, the type-only `StreamGrants`) agree only
  when the `by_principal` index is complete; a deferred write of a new
  identity is invisible to them until `EndSync`
  (`IndexedGrants.grantsForPrincipal_deferred_incomplete`).
- Lookup by bare id is not a view of the store but a resolution rule
  (`GrantLookup.resolve`, `Result.resolveBare`): a grant that every
  listing shows may be unreachable by its public id, or masked.
- `ListGrantsForEntitlement` with a bare entitlement id that resolves to
  nothing returns an empty success, not an error.

Field agreement is deliberately limited to what the model carries: the
structural identity and the stored `external_id`. Display fields,
annotations, sources, and `discovered_at` are compared by the Go test
where both views return them, and are not part of this theorem.

Bulk reads (`ListResourcesByIds`, `ListEntitlementsByIds` in
`adapter_reader.go`) are one point lookup per requested id, in request
order, with a missing id skipped silently, a repeated id repeated, and,
for the bare entitlement ids, an ambiguous id failing the whole call.
The theorems say what a consumer may and may not conclude: every
returned record is the point lookup of its own id (no misassociation),
the returned ids are a subsequence of the request, and absence is not
marked, so the caller must diff the request against the response.
-/

namespace C1z
namespace Views

open GrantStore IndexedGrants

private def gV : GrantRecord :=
  { id := { ent := { rt := [0x61], rid := [0x31], ext := [0x6d] }, prt := [0x75], prid := [0x31] },
    externalId := [0x78] }

/-- In a key-sorted list, two entries with the same key are the same entry. -/
private theorem entry_eq_of_key_eq {α : Type} {l : List (Bytes × α)} (hs : KeysSorted l)
    {a b : Bytes × α} (ha : a ∈ l) (hb : b ∈ l) (hab : a.1 = b.1) : a = b := by
  induction l with
  | nil => nomatch ha
  | cons c cs ih =>
    have hc := List.pairwise_cons.mp hs
    rcases List.mem_cons.mp ha with rfl | ha' <;> rcases List.mem_cons.mp hb with rfl | hb'
    · rfl
    · have h := hc.1 b hb'
      rw [hab, lexLt.irrefl] at h
      exact absurd h Bool.false_ne_true
    · have h := hc.1 a ha'
      rw [← hab, lexLt.irrefl] at h
      exact absurd h Bool.false_ne_true
    · exact ih hc.2 ha' hb'

private theorem records_map_record {α : Type} (l : List α) : Stream.records (l.map Result.Yield.record) = l := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    unfold Stream.records at ih ⊢
    rw [List.map_cons, List.filterMap_cons, ih]

/-- The stream a patient consumer sees over a given grant row source. -/
def streamRecords (rows : List GrantRecord) : List GrantRecord :=
  Stream.records (Stream.run rows (fun _ => true) Stream.Consumer.patient)

/-- Existence and content agree between point lookup and the full
listing: a record is listed exactly when the lookup of its identity
returns it. -/
theorem list_point_agree {s : GrantStore} (hk : Keyed s) (r : GrantRecord) :
    r ∈ allGrants s ↔ getGrant s r.id = some r := by
  constructor
  · intro h
    obtain ⟨kv, hkv, rfl⟩ := List.mem_map.mp h
    have hkey : kv.1 = kv.2.id.key := hk kv hkv
    unfold getGrant Store.get
    have hsome : (s.entries.find? fun e => e.1 == kv.2.id.key).isSome = true :=
      List.find?_isSome.mpr ⟨kv, hkv, beq_iff_eq.mpr hkey⟩
    obtain ⟨kv', hfind⟩ := Option.isSome_iff_exists.mp hsome
    have hp := List.find?_some hfind
    have hk' : kv'.1 = kv.2.id.key := beq_iff_eq.mp hp
    have heq : kv' = kv :=
      entry_eq_of_key_eq s.sorted (List.mem_of_find?_eq_some hfind) hkv (hk'.trans hkey.symm)
    rw [hfind, heq]
    rfl
  · intro h
    unfold getGrant Store.get at h
    obtain ⟨kv, hfind, hkv⟩ := Option.map_eq_some_iff.mp h
    exact List.mem_map.mpr ⟨kv, List.mem_of_find?_eq_some hfind, hkv⟩

/-- The per-entitlement scan and the full listing agree: a record is in
its entitlement's scan exactly when it is listed. -/
theorem scan_list_agree {s : GrantStore} (hk : Keyed s) (r : GrantRecord) :
    r ∈ grantsForEntitlement s r.id.ent ↔ r ∈ allGrants s := by
  exact (mem_allGrants_iff hk r).symm

/-- The patient stream over any row source yields exactly that source. -/
theorem stream_list_agree (rows : List GrantRecord) : streamRecords rows = rows := by
  unfold streamRecords
  rw [Stream.run_eq_filter, List.filter_eq_self.mpr fun _ _ => rfl]
  exact records_map_record rows

/-- The index walk agrees with the primary filter once the index is complete. -/
theorem index_primary_agree {x : IndexedGrants} (hc : x.index.Complete x.store) (prt prid : Bytes) :
    x.grantsForPrincipal prt prid = x.grantsForPrincipalPrimary prt prid := by
  exact grantsForPrincipal_eq_of_complete hc prt prid

/-- A record is in the primary principal filter exactly when it is listed
with that principal. -/
theorem principal_list_agree (x : IndexedGrants) (prt prid : Bytes) (r : GrantRecord) :
    r ∈ x.grantsForPrincipalPrimary prt prid ↔ r ∈ allGrants x.store ∧ r.id.prt = prt ∧ r.id.prid = prid := by
  simp only [grantsForPrincipalPrimary, allGrants, List.mem_map, List.mem_filter, Bool.and_eq_true, beq_iff_eq]
  constructor
  · rintro ⟨kv, ⟨hkv, hp, hq⟩, rfl⟩
    exact ⟨⟨kv, hkv, rfl⟩, hp, hq⟩
  · rintro ⟨⟨kv, hkv, rfl⟩, hp, hq⟩
    exact ⟨kv, ⟨hkv, hp, hq⟩, rfl⟩

/-- The combined contract. For a store built by the engine's own writes
(`Keyed`) whose index is complete (after `EndSync`), every view agrees
on existence: point lookup, full listing, entitlement scan, the
patient streams over listing and scan, the index walk, and the primary
principal filter. -/
theorem views_agree {x : IndexedGrants} (hk : Keyed x.store) (hc : x.index.Complete x.store)
    (r : GrantRecord) :
    (getGrant x.store r.id = some r ↔ r ∈ allGrants x.store) ∧
    (r ∈ allGrants x.store ↔ r ∈ grantsForEntitlement x.store r.id.ent) ∧
    (r ∈ allGrants x.store ↔ r ∈ streamRecords (allGrants x.store)) ∧
    (r ∈ grantsForEntitlement x.store r.id.ent ↔ r ∈ streamRecords (grantsForEntitlement x.store r.id.ent)) ∧
    (r ∈ allGrants x.store ↔ r ∈ x.grantsForPrincipal r.id.prt r.id.prid) ∧
    (r ∈ allGrants x.store ↔ r ∈ x.grantsForPrincipalType r.id.prt) := by
  refine ⟨(list_point_agree hk r).symm, (scan_list_agree hk r).symm, ?_, ?_, ?_, ?_⟩
  · rw [stream_list_agree]
  · rw [stream_list_agree]
  · rw [index_primary_agree hc, principal_list_agree]
    exact ⟨fun h => ⟨h, rfl, rfl⟩, fun h => h.1⟩
  · rw [mem_grantsForPrincipalType]
    exact ⟨fun h => ⟨h, rfl, hc r h⟩, fun h => h.1⟩

/-- Negative result, restated here next to the contract: without a
complete index, the index walk can disagree with every other view. -/
theorem views_disagree_without_complete_index :
    ∃ (x : IndexedGrants) (r : GrantRecord),
      Keyed x.store ∧ r ∈ allGrants x.store ∧ r ∉ x.grantsForPrincipal r.id.prt r.id.prid := by
  refine ⟨IndexedGrants.empty.putGrantsDeferred [gV], gV, keyed_putGrants keyed_empty [gV], by decide, by decide⟩

/-! ## Bulk reads by id -/

/-- `ListResourcesByIds`: one structural lookup per id, in request order,
missing ids skipped. -/
def bulkResources {α : Type} (s : Store α) (ids : List ResourceId) : List (ResourceId × α) :=
  ids.filterMap fun id => (s.get id.key).map fun v => (id, v)

/-- `ListEntitlementsByIds`: one bare-id lookup per id over the stored
entitlement identities; a missing id is skipped and an ambiguous id
fails the call. -/
def bulkEntitlements (ents : List EntitlementId) (ids : List Bytes) : Option (List EntitlementId) :=
  ids.foldl (fun acc id =>
    acc.bind fun out =>
      match Result.resolveBare (ents.filter fun e => e.ext == id) with
      | .found e => some (out ++ [e])
      | .notFound => some out
      | .ambiguous => none) (some [])

/-- One step of the `bulkEntitlements` fold. -/
def bulkStep (ents : List EntitlementId) (acc : Option (List EntitlementId)) (id : Bytes) :
    Option (List EntitlementId) :=
  acc.bind fun out =>
    match Result.resolveBare (ents.filter fun e => e.ext == id) with
    | .found e => some (out ++ [e])
    | .notFound => some out
    | .ambiguous => none

theorem bulkEntitlements_eq_foldl (ents : List EntitlementId) (ids : List Bytes) :
    bulkEntitlements ents ids = ids.foldl (bulkStep ents) (some []) := rfl

/-- Once the accumulator is `none`, it stays `none`. -/
theorem foldl_bulkStep_none (ents : List EntitlementId) (ids : List Bytes) :
    ids.foldl (bulkStep ents) none = none := by
  induction ids with
  | nil => rfl
  | cons _ _ ih => exact ih

/-- An ambiguous id turns the accumulator to `none`. -/
theorem bulkStep_ambiguous (ents : List EntitlementId) (acc : Option (List EntitlementId)) (id : Bytes)
    (h : 2 ≤ (ents.filter fun e => e.ext == id).length) : bulkStep ents acc id = none := by
  cases acc with
  | none => rfl
  | some out =>
    unfold bulkStep
    rw [Option.bind_some, (Result.resolveBare_ambiguous_iff _).mpr h]

/-- The fold keeps every accumulated entitlement unique by its id. -/
theorem foldl_bulkStep_inv (ents : List EntitlementId) (ids : List Bytes) (out₀ out : List EntitlementId)
    (h₀ : ∀ e ∈ out₀, ents.filter (fun e' => e'.ext == e.ext) = [e])
    (h : ids.foldl (bulkStep ents) (some out₀) = some out) :
    ∀ e ∈ out, ents.filter (fun e' => e'.ext == e.ext) = [e] := by
  induction ids generalizing out₀ with
  | nil =>
    rw [List.foldl_nil, Option.some.injEq] at h
    exact h ▸ h₀
  | cons a t ih =>
    rw [List.foldl_cons] at h
    unfold bulkStep at h
    rw [Option.bind_some] at h
    split at h
    · rename_i e he
      have hf := (Result.resolveBare_found_iff _ _).mp he
      have hea : e.ext = a := by
        have hm : e ∈ ents.filter fun e' => e'.ext == a := by rw [hf]; exact List.mem_singleton_self e
        exact beq_iff_eq.mp (List.mem_filter.mp hm).2
      refine ih (out₀ ++ [e]) ?_ h
      intro e' he'
      rcases List.mem_append.mp he' with h' | h'
      · exact h₀ e' h'
      · rw [List.mem_singleton.mp h', hea]
        exact hf
    · exact ih out₀ h₀ h
    · have h' : t.foldl (bulkStep ents) none = some out := h
      rw [foldl_bulkStep_none] at h'
      cases h'

/-- No misassociation: every returned record is the point lookup of the id
it is paired with. -/
theorem bulkResources_assoc {α : Type} (s : Store α) (ids : List ResourceId) :
    ∀ p ∈ bulkResources s ids, s.get p.1.key = some p.2 := by
  intro p hp
  unfold bulkResources at hp
  obtain ⟨id, -, h⟩ := List.mem_filterMap.mp hp
  obtain ⟨v, hv, rfl⟩ := Option.map_eq_some_iff.mp h
  exact hv

/-- The returned ids are a subsequence of the request: request order,
nothing invented, repeats kept. -/
theorem bulkResources_sublist {α : Type} (s : Store α) (ids : List ResourceId) :
    List.Sublist ((bulkResources s ids).map (·.1)) ids := by
  induction ids with
  | nil => exact List.Sublist.slnil
  | cons a t ih =>
    unfold bulkResources
    rw [List.filterMap_cons]
    cases hg : s.get a.key with
    | none => exact ih.cons a
    | some v => exact ih.cons_cons a

/-- A requested id is returned exactly when its lookup succeeds. -/
theorem mem_bulkResources_iff {α : Type} (s : Store α) (ids : List ResourceId) (id : ResourceId) (v : α) :
    (id, v) ∈ bulkResources s ids ↔ id ∈ ids ∧ s.get id.key = some v := by
  unfold bulkResources
  constructor
  · intro hm
    obtain ⟨a, ha, h⟩ := List.mem_filterMap.mp hm
    obtain ⟨w, hw, he⟩ := Option.map_eq_some_iff.mp h
    rw [Prod.mk.injEq] at he
    obtain ⟨rfl, rfl⟩ := he
    exact ⟨ha, hw⟩
  · intro ⟨hid, hv⟩
    exact List.mem_filterMap.mpr ⟨id, hid, by rw [hv]; rfl⟩

/-- Negative result: absence is not marked. A request with a missing id
returns fewer records, and the response alone cannot say which id was
missing. -/
theorem bulkResources_absence_unmarked {α : Type} (s : Store α) (ids : List ResourceId) (id : ResourceId)
    (h : s.get id.key = none) : bulkResources s (ids ++ [id]) = bulkResources s ids := by
  unfold bulkResources
  simp [List.filterMap_append, h]

/-- A bulk entitlement read fails as a whole on any ambiguous id. -/
theorem bulkEntitlements_none_of_ambiguous (ents : List EntitlementId) (ids : List Bytes) (id : Bytes)
    (hid : id ∈ ids) (h : 2 ≤ (ents.filter fun e => e.ext == id).length) :
    bulkEntitlements ents ids = none := by
  obtain ⟨pre, post, rfl⟩ := List.append_of_mem hid
  rw [bulkEntitlements_eq_foldl, List.foldl_append, List.foldl_cons, bulkStep_ambiguous ents _ id h,
    foldl_bulkStep_none]

/-- When it succeeds, every returned entitlement is the unique one with
its id among the stored identities. -/
theorem bulkEntitlements_some_unique (ents : List EntitlementId) (ids : List Bytes) (out : List EntitlementId)
    (h : bulkEntitlements ents ids = some out) :
    ∀ e ∈ out, ents.filter (fun e' => e'.ext == e.ext) = [e] := by
  rw [bulkEntitlements_eq_foldl] at h
  exact foldl_bulkStep_inv ents ids [] out (fun _ he => nomatch he) h

/-! ## Witness -/

example : getGrant (IndexedGrants.empty.putGrants [gV]).store gV.id = some gV := by decide
example : streamRecords (allGrants (IndexedGrants.empty.putGrants [gV]).store) = [gV] := by decide
example : (IndexedGrants.empty.putGrantsDeferred [gV]).grantsForPrincipal [0x75] [0x31] = [] := by decide

end Views
end C1z
