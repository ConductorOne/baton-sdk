import C1z.Records

/-!
# The `by_principal` secondary index and hidden rows

Models the grant `by_principal` index (`idxGrantByPrincipal` in
`keys.go`) as the engine maintains it, including the deferred write
path whose entries `EndSync` builds later (`deferred_index.go`
`BuildDeferredGrantIndexes`).

The index is a set of grant identities. A plain grant write
(`PutGrants`, `StageGrantPutInline`) adds its identity and a delete
removes it, in the same batch as the primary row
(`internal/rawdb/records.go`). A write through the deferred path
(`PutExpandedGrantRecords`, `StageGrantPutDeferred`) arms the deferred
marker and adds no entry; an entry that already exists for that
identity stays, since the index key is derived from the primary key.
No reader checks the marker (`DeferredIdxPending` is consulted only by
`EndSync`), so an index-backed read before `EndSync` returns a subset
of the true answer with a successful status. `EndSync` rebuilds the
index from the primary rows, after which the index view equals the
primary view.

An index entry whose primary row is missing (a dangling entry, which
arises from a crash between batches or from corruption, never from the
ordinary write path) is skipped by the reader. The model keeps dangling
entries representable so the read law can say they are invisible, which
is also the `visible = false` case of `C1z.Paginate.page`.

What this module proves: the index view is always a subset of the
primary view; it equals the primary view when the index is complete;
`EndSync`'s rebuild makes it complete; and there is a concrete state
where the index view is empty while the primary view is not. What it
does not prove, because the engine does not provide it: any signal that
distinguishes the incomplete view from an empty result.

Out of scope: the `by_needs_expansion` and source-scope indexes, the
digest index, the four prior fields `PutExpandedGrantRecords` keeps on
overwrite, and the `EndSync` corner where a store with no grants skips
the index excise so planted entries survive.
-/

namespace C1z

/-- The `by_principal` index as the set of grant identities it holds,
plus the deferral flag. -/
structure PrincipalIndex where
  entries : List GrantId
  deferred : Bool
  deriving Repr, DecidableEq

namespace PrincipalIndex

def empty : PrincipalIndex := { entries := [], deferred := false }

/-- A plain grant write's effect on the index: add the identity.
Duplicates are harmless; the index is read as a set. -/
def noteWrite (ix : PrincipalIndex) (g : GrantId) : PrincipalIndex :=
  { ix with entries := g :: ix.entries.filter (· != g) }

/-- A deferred-path grant write's effect on the index: arm the marker and
add nothing. An existing entry for the identity stays. -/
def noteWriteDeferred (ix : PrincipalIndex) (_g : GrantId) : PrincipalIndex :=
  { ix with deferred := true }

/-- A grant delete removes the entry in the same batch as the primary row. -/
def noteDelete (ix : PrincipalIndex) (g : GrantId) : PrincipalIndex :=
  { ix with entries := ix.entries.filter (· != g) }

/-- `BuildDeferredGrantIndexes` at `EndSync`: rebuild from the primary rows
and clear the flag. -/
def rebuild (s : GrantStore) : PrincipalIndex :=
  { entries := s.allGrants.map (·.id), deferred := false }

/-- The index is complete for a store when every stored grant has an entry. -/
def Complete (ix : PrincipalIndex) (s : GrantStore) : Prop :=
  ∀ r ∈ s.allGrants, r.id ∈ ix.entries

end PrincipalIndex

/-- Grant store and index together. -/
structure IndexedGrants where
  store : GrantStore
  index : PrincipalIndex

namespace IndexedGrants

def empty : IndexedGrants := { store := Store.empty, index := PrincipalIndex.empty }

def putGrants (x : IndexedGrants) (rs : List GrantRecord) : IndexedGrants :=
  { store := x.store.putGrants rs
    index := rs.foldl (fun ix r => ix.noteWrite r.id) x.index }

def deleteGrant (x : IndexedGrants) (g : GrantId) : IndexedGrants :=
  { store := x.store.deleteGrant g, index := x.index.noteDelete g }

/-- `PutExpandedGrantRecords`: same replace semantics on the primary row,
no index entry for the written identities. -/
def putGrantsDeferred (x : IndexedGrants) (rs : List GrantRecord) : IndexedGrants :=
  { store := x.store.putGrants rs
    index := rs.foldl (fun ix r => ix.noteWriteDeferred r.id) x.index }

/-- `EndSync`'s index rebuild. -/
def endSyncRebuild (x : IndexedGrants) : IndexedGrants :=
  { x with index := PrincipalIndex.rebuild x.store }

/-- `ListGrantsForPrincipal`: walk the index entries for the principal,
fetch each primary row, skip dangling entries. Returned in primary key
order so the result is comparable to the primary view. -/
def grantsForPrincipal (x : IndexedGrants) (prt prid : Bytes) : List GrantRecord :=
  (x.store.entries.filter fun kv =>
    kv.2.id.prt == prt && kv.2.id.prid == prid && x.index.entries.contains kv.2.id).map (·.2)

/-- The `by_principal` index key tuple of a grant identity
(`idxGrantByPrincipal`: principal type, principal id, then the
entitlement components). -/
def principalIndexTuple (g : GrantId) : List Bytes := [g.prt, g.prid] ++ g.ent.tuple

/-- Insertion sort by `lexLt` on encoded index keys. -/
def insertByIndexKey (r : GrantRecord) : List GrantRecord → List GrantRecord
  | [] => [r]
  | x :: xs =>
    if lexLt (Codec.encodeTuple (principalIndexTuple r.id)) (Codec.encodeTuple (principalIndexTuple x.id)) then
      r :: x :: xs
    else x :: insertByIndexKey r xs

def sortByIndexKey (rs : List GrantRecord) : List GrantRecord := rs.foldr insertByIndexKey []

/-- `ListGrantsForResourceType` and the type-only `StreamGrants`: walk the
`by_principal` index under a principal type, in index key order, fetching
each primary row and skipping dangling entries. -/
def grantsForPrincipalType (x : IndexedGrants) (prt : Bytes) : List GrantRecord :=
  sortByIndexKey ((x.store.entries.filter fun kv =>
    kv.2.id.prt == prt && x.index.entries.contains kv.2.id).map (·.2))

/-- The primary view: a full scan filtered by principal. -/
def grantsForPrincipalPrimary (x : IndexedGrants) (prt prid : Bytes) : List GrantRecord :=
  (x.store.entries.filter fun kv => kv.2.id.prt == prt && kv.2.id.prid == prid).map (·.2)

/-! ## Laws -/

private theorem mem_entries_putBatch {α : Type} {s : Store α} {ws : List (Bytes × α)} {kv : Bytes × α}
    (h : kv ∈ (s.putBatch ws).entries) : kv ∈ s.entries ∨ kv ∈ ws := by
  induction ws generalizing s with
  | nil => exact Or.inl h
  | cons w ws ih =>
    rcases ih (s := s.put w.1 w.2) h with h' | h'
    · rcases Store.mem_insertSorted h' with h'' | h''
      · exact Or.inr (h'' ▸ List.mem_cons_self)
      · exact Or.inl h''
    · exact Or.inr (List.mem_cons_of_mem _ h')

private theorem mem_foldl_noteWrite (ix : PrincipalIndex) (rs : List GrantRecord)
    (g : GrantId) :
    g ∈ (rs.foldl (fun ix r => ix.noteWrite r.id) ix).entries ↔ g ∈ ix.entries ∨ g ∈ rs.map (·.id) := by
  induction rs generalizing ix with
  | nil => simp
  | cons r rs ih =>
    simp only [List.foldl_cons, List.map_cons, List.mem_cons]
    rw [ih]
    simp only [PrincipalIndex.noteWrite, List.mem_cons, List.mem_filter, bne_iff_ne, ne_eq]
    by_cases h : g = r.id <;> simp [h]

private theorem mem_insertByIndexKey (x r : GrantRecord) (l : List GrantRecord) :
    x ∈ insertByIndexKey r l ↔ x = r ∨ x ∈ l := by
  induction l with
  | nil => simp only [insertByIndexKey, List.mem_singleton, List.not_mem_nil, or_false]
  | cons y ys ih =>
    unfold insertByIndexKey
    split
    · exact List.mem_cons
    · simp only [List.mem_cons, ih]
      constructor
      · rintro (h | h | h)
        · exact Or.inr (Or.inl h)
        · exact Or.inl h
        · exact Or.inr (Or.inr h)
      · rintro (h | h | h)
        · exact Or.inr (Or.inl h)
        · exact Or.inl h
        · exact Or.inr (Or.inr h)

private theorem mem_sortByIndexKey (r : GrantRecord) (rs : List GrantRecord) :
    r ∈ sortByIndexKey rs ↔ r ∈ rs := by
  induction rs with
  | nil => exact Iff.rfl
  | cons y ys ih =>
    unfold sortByIndexKey at ih ⊢
    rw [List.foldr_cons, mem_insertByIndexKey, ih, List.mem_cons]

/-- Membership in the type-only index walk: a stored grant of that
principal type with an index entry. -/
theorem mem_grantsForPrincipalType (x : IndexedGrants) (prt : Bytes) (r : GrantRecord) :
    r ∈ x.grantsForPrincipalType prt ↔ r ∈ x.store.allGrants ∧ r.id.prt = prt ∧ r.id ∈ x.index.entries := by
  unfold grantsForPrincipalType
  rw [mem_sortByIndexKey]
  simp only [GrantStore.allGrants, List.mem_map, List.mem_filter, Bool.and_eq_true, beq_iff_eq,
    List.contains_iff_mem]
  constructor
  · rintro ⟨kv, ⟨hkv, hp, hi⟩, rfl⟩
    exact ⟨⟨kv, hkv, rfl⟩, hp, hi⟩
  · rintro ⟨⟨kv, hkv, rfl⟩, hp, hi⟩
    exact ⟨kv, ⟨hkv, hp, hi⟩, rfl⟩

/-- The index view never returns a row the primary view lacks, dangling
entries included. -/
theorem grantsForPrincipal_subset (x : IndexedGrants) (prt prid : Bytes) :
    ∀ r ∈ x.grantsForPrincipal prt prid, r ∈ x.grantsForPrincipalPrimary prt prid := by
  intro r hr
  simp only [grantsForPrincipal, List.mem_map, List.mem_filter, Bool.and_eq_true] at hr
  obtain ⟨kv, ⟨hkv, ⟨hp, _⟩⟩, rfl⟩ := hr
  simp only [grantsForPrincipalPrimary, List.mem_map, List.mem_filter, Bool.and_eq_true]
  exact ⟨kv, ⟨hkv, hp⟩, rfl⟩

/-- Dangling entries are invisible: the index view depends only on
entries that have a primary row. -/
theorem grantsForPrincipal_eq_of_complete {x : IndexedGrants} (hc : x.index.Complete x.store)
    (prt prid : Bytes) : x.grantsForPrincipal prt prid = x.grantsForPrincipalPrimary prt prid := by
  unfold grantsForPrincipal grantsForPrincipalPrimary
  congr 1
  apply List.filter_congr
  intro kv hkv
  have hmem : kv.2.id ∈ x.index.entries :=
    hc kv.2 (List.mem_map.mpr ⟨kv, hkv, rfl⟩)
  have hcon : x.index.entries.contains kv.2.id = true := List.contains_iff_mem.mpr hmem
  rw [hcon, Bool.and_true]

/-- `EndSync`'s rebuild yields a complete index. -/
theorem complete_endSyncRebuild (x : IndexedGrants) : x.endSyncRebuild.index.Complete x.endSyncRebuild.store := by
  intro r hr
  exact List.mem_map.mpr ⟨r, hr, rfl⟩

/-- After the rebuild, the index-backed read equals the primary view. -/
theorem grantsForPrincipal_endSyncRebuild (x : IndexedGrants) (prt prid : Bytes) :
    x.endSyncRebuild.grantsForPrincipal prt prid = x.endSyncRebuild.grantsForPrincipalPrimary prt prid := grantsForPrincipal_eq_of_complete (complete_endSyncRebuild x) prt prid

/-- Plain writes keep the index complete, whether or not the marker is armed. -/
theorem complete_putGrants {x : IndexedGrants} (hc : x.index.Complete x.store) (rs : List GrantRecord) :
    (x.putGrants rs).index.Complete (x.putGrants rs).store := by
  intro r hr
  simp only [putGrants, GrantStore.allGrants, GrantStore.putGrants, List.mem_map] at hr ⊢
  obtain ⟨kv, hkv, rfl⟩ := hr
  rw [mem_foldl_noteWrite]
  rcases mem_entries_putBatch hkv with h | h
  · exact Or.inl (hc kv.2 (List.mem_map.mpr ⟨kv, h, rfl⟩))
  · obtain ⟨r, hr, rfl⟩ := List.mem_map.mp h
    exact Or.inr (List.mem_map.mpr ⟨r, hr, rfl⟩)

/-- Deletes keep the index complete in either mode. -/
theorem complete_deleteGrant {x : IndexedGrants} (hk : GrantStore.Keyed x.store) (hc : x.index.Complete x.store)
    (g : GrantId) :
    (x.deleteGrant g).index.Complete (x.deleteGrant g).store := by
  intro r hr
  simp only [deleteGrant, GrantStore.allGrants, GrantStore.deleteGrant, Store.erase, List.mem_map,
    List.mem_filter, bne_iff_ne, ne_eq] at hr
  obtain ⟨kv, ⟨hkv, hne⟩, rfl⟩ := hr
  simp only [deleteGrant, PrincipalIndex.noteDelete, List.mem_filter, bne_iff_ne, ne_eq]
  refine ⟨hc kv.2 (List.mem_map.mpr ⟨kv, hkv, rfl⟩), fun h => hne ?_⟩
  rw [hk kv hkv, GrantRecord.key, h]

/-- A deferred-path write never adds an entry; the index entries after it
are exactly the entries before it. -/
theorem entries_putGrantsDeferred (x : IndexedGrants) (rs : List GrantRecord) :
    (x.putGrantsDeferred rs).index.entries = x.index.entries := by
  unfold putGrantsDeferred
  simp only
  generalize x.index = ix
  induction rs generalizing ix with
  | nil => rfl
  | cons r rs ih =>
    rw [List.foldl_cons, ih]
    rfl

/-- A deferred-path write of a new identity leaves the index incomplete. -/
theorem not_complete_putGrantsDeferred_of_new {x : IndexedGrants} (r : GrantRecord)
    (hnew : r.id ∉ x.index.entries) :
    ¬ (x.putGrantsDeferred [r]).index.Complete (x.putGrantsDeferred [r]).store := by
  intro hc
  have hget := GrantStore.getGrant_putGrants_single x.store r
  simp only [GrantStore.getGrant, Store.get, Option.map_eq_some_iff] at hget
  obtain ⟨kv, hfind, hkv⟩ := hget
  have hmem : r ∈ (x.putGrantsDeferred [r]).store.allGrants :=
    List.mem_map.mpr ⟨kv, List.mem_of_find?_eq_some hfind, hkv⟩
  have h := hc r hmem
  rw [entries_putGrantsDeferred] at h
  exact hnew h

/-- A deferred-path overwrite of an identity that already has an entry
keeps the index complete. -/
theorem complete_putGrantsDeferred_of_mem {x : IndexedGrants} (hc : x.index.Complete x.store)
    (r : GrantRecord) (hmem : r.id ∈ x.index.entries) :
    (x.putGrantsDeferred [r]).index.Complete (x.putGrantsDeferred [r]).store := by
  intro r' hr'
  rw [entries_putGrantsDeferred]
  simp only [putGrantsDeferred, GrantStore.allGrants, GrantStore.putGrants, List.mem_map] at hr'
  obtain ⟨kv, hkv, rfl⟩ := hr'
  rcases Store.mem_entries_putBatch hkv with h | h
  · exact hc kv.2 (List.mem_map.mpr ⟨kv, h, rfl⟩)
  · simp only [List.map_cons, List.map_nil, List.mem_singleton] at h
    rw [h]
    exact hmem

/-- Negative result: a deferred-path write of a new identity makes the
index-backed read of that principal empty while the primary view is not.
Nothing in the result distinguishes this from an empty principal. -/
theorem grantsForPrincipal_deferred_incomplete :
    ∃ (x : IndexedGrants) (prt prid : Bytes),
      x.grantsForPrincipal prt prid = [] ∧ x.grantsForPrincipalPrimary prt prid ≠ [] := ⟨IndexedGrants.empty.putGrantsDeferred
      [{ id := { ent := { rt := [0x61], rid := [0x31], ext := [0x6d] }, prt := [0x75], prid := [0x31] },
         externalId := [] }],
    [0x75], [0x31], by decide, by decide⟩

/-- The deferred path writes the same primary rows as the plain path, so
the store invariant holds for it too. -/
theorem keyed_putGrantsDeferred {x : IndexedGrants} (hk : GrantStore.Keyed x.store) (rs : List GrantRecord) :
    GrantStore.Keyed (x.putGrantsDeferred rs).store :=
  GrantStore.keyed_putGrants hk rs

/-- A second `EndSync` without writes changes nothing. -/
theorem endSyncRebuild_idempotent (x : IndexedGrants) : x.endSyncRebuild.endSyncRebuild = x.endSyncRebuild :=
  rfl

/-! ## Witnesses -/

private def gD : GrantRecord :=
  { id := { ent := { rt := [0x61], rid := [0x31], ext := [0x6d] }, prt := [0x75], prid := [0x31] },
    externalId := [] }

/-- Normal mode: index and primary views agree. -/
example : (IndexedGrants.empty.putGrants [gD]).grantsForPrincipal [0x75] [0x31] = [gD] := by decide

/-- Deferred path on a new identity: the index view is empty, the primary view is not. -/
example : (IndexedGrants.empty.putGrantsDeferred [gD]).grantsForPrincipal [0x75] [0x31] = [] := by decide
example : (IndexedGrants.empty.putGrantsDeferred [gD]).grantsForPrincipalPrimary [0x75] [0x31] = [gD] := by
  decide

/-- A plain write while the marker is armed is still indexed. -/
example : ((IndexedGrants.empty.putGrantsDeferred [gD]).putGrants [gD]).grantsForPrincipal [0x75] [0x31] = [gD] := by
  decide

/-- Deferred overwrite of an already-indexed identity stays visible. -/
example : ((IndexedGrants.empty.putGrants [gD]).putGrantsDeferred [{ gD with externalId := [0x7a] }]).grantsForPrincipal
    [0x75] [0x31] = [{ gD with externalId := [0x7a] }] := by decide

/-- `EndSync` repairs the deferred gap. -/
example : (IndexedGrants.empty.putGrantsDeferred [gD]).endSyncRebuild.grantsForPrincipal [0x75] [0x31] = [gD] := by
  decide

end IndexedGrants
end C1z
