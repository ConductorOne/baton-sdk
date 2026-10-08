import C1z.Order

/-!
# Logical record store

Models one primary keyspace of the Pebble engine as a finite map from
key bytes to a value, kept as a strictly sorted association list so
that enumeration order is key order (`C1z.lexLt`).

Write semantics modeled, from `internal/rawdb/records.go` (whole-value
`Set`), `grants.go` / `resources.go` / `entitlements.go` /
`resource_types.go` (per-call "keep only the LAST occurrence" dedup),
and `page_unit.go` (one batch, one commit):

- `put` replaces the whole value: last write wins, no field merge;
- `putBatch` applies a list in order, so the last occurrence of a key
  in the batch is what the store holds;
- `erase` removes a key and nothing else;
- a batch either lands entirely or not at all (`putBatch` is a pure
  function of the prior store; failure is modeled by not applying it).

Out of scope: `PutExpandedGrantRecords` (keeps four prior fields),
`BulkSyncImport` and the id-index migration (field-merge fold
`mergeDuplicateGrantValues`), `UnsafePutUniqueGrantRecords` (producer
obligation, no dedup), chunked batch deletes (`DeleteGrantsByIdentityRefs`
commits in chunks of 1000 and is not all-or-nothing), secondary indexes,
digests, and `discovered_at` stamping.
-/

namespace C1z

/-- Strictly ascending by key. -/
def KeysSorted {α : Type} (l : List (Bytes × α)) : Prop :=
  l.Pairwise (fun a b => lexLt a.1 b.1 = true)

/-- One primary keyspace: a sorted association list from key bytes to values. -/
structure Store (α : Type) where
  entries : List (Bytes × α)
  sorted : KeysSorted entries

namespace Store

variable {α : Type}

def empty : Store α := ⟨[], List.Pairwise.nil⟩

/-- Point lookup (`pebble.Get` on the primary key). -/
def get (s : Store α) (k : Bytes) : Option α :=
  (s.entries.find? (fun e => e.1 == k)).map (·.2)

/-- Insert-or-replace into a sorted list. -/
def insertSorted (k : Bytes) (v : α) : List (Bytes × α) → List (Bytes × α)
  | [] => [(k, v)]
  | e :: es =>
    if lexLt k e.1 then (k, v) :: e :: es
    else if k = e.1 then (k, v) :: es
    else e :: insertSorted k v es

theorem mem_insertSorted {k : Bytes} {v : α} {l : List (Bytes × α)} {x : Bytes × α}
    (h : x ∈ insertSorted k v l) : x = (k, v) ∨ x ∈ l := by
  induction l with
  | nil =>
    simp only [insertSorted, List.mem_singleton] at h
    exact Or.inl h
  | cons e es ih =>
    simp only [insertSorted] at h
    split at h
    · rcases List.mem_cons.mp h with h | h
      · exact Or.inl h
      · exact Or.inr h
    · split at h
      · rcases List.mem_cons.mp h with h | h
        · exact Or.inl h
        · exact Or.inr (List.mem_cons_of_mem _ h)
      · rcases List.mem_cons.mp h with h | h
        · exact Or.inr (h ▸ List.mem_cons_self)
        · rcases ih h with h | h
          · exact Or.inl h
          · exact Or.inr (List.mem_cons_of_mem _ h)

theorem keysSorted_insertSorted {l : List (Bytes × α)} (h : KeysSorted l) (k : Bytes) (v : α) :
    KeysSorted (insertSorted k v l) := by
  induction l with
  | nil => exact List.pairwise_singleton _ _
  | cons e es ih =>
    have ⟨hhd, htl⟩ := List.pairwise_cons.mp h
    by_cases h1 : lexLt k e.1 = true
    · rw [insertSorted, ite_eq_left h1]
      refine List.Pairwise.cons ?_ h
      intro x hx
      rcases List.mem_cons.mp hx with rfl | hx
      · exact h1
      · exact lexLt.trans h1 (hhd x hx)
    · rw [insertSorted, ite_eq_right h1]
      by_cases h2 : k = e.1
      · rw [ite_eq_left h2]
        refine List.Pairwise.cons ?_ htl
        intro x hx
        rw [h2]
        exact hhd x hx
      · rw [ite_eq_right h2]
        refine List.Pairwise.cons ?_ (ih htl)
        intro x hx
        rcases mem_insertSorted hx with rfl | hx
        · rcases lexLt.total h2 with h3 | h3
          · exact absurd h3 h1
          · exact h3
        · exact hhd x hx

/-- Whole-value replace (`rb.core.b.Set(key, val, nil)`). -/
def put (s : Store α) (k : Bytes) (v : α) : Store α :=
  ⟨insertSorted k v s.entries, keysSorted_insertSorted s.sorted k v⟩

theorem keysSorted_filter {l : List (Bytes × α)} (h : KeysSorted l) (p : Bytes × α → Bool) :
    KeysSorted (l.filter p) := by
  exact List.Pairwise.filter p h

/-- Tombstone one key (`DeleteXxxRecord` on an existing row). -/
def erase (s : Store α) (k : Bytes) : Store α :=
  ⟨s.entries.filter (fun e => e.1 != k), keysSorted_filter s.sorted _⟩

/-- Apply a batch of writes in order. -/
def putBatch (s : Store α) (ws : List (Bytes × α)) : Store α :=
  ws.foldl (fun acc w => acc.put w.1 w.2) s

/-- Enumeration in key order. -/
def keys (s : Store α) : List Bytes := s.entries.map (·.1)

/-- Keep only the last occurrence of each key, preserving the order of
those last occurrences (the per-call dedup in `stageGrantRecords` and
siblings). -/
def dedupLast : List (Bytes × α) → List (Bytes × α)
  | [] => []
  | w :: ws => if ws.any (fun x => x.1 == w.1) then dedupLast ws else w :: dedupLast ws

/-! ## Point semantics -/

theorem find?_insertSorted_self (l : List (Bytes × α)) (k : Bytes) (v : α) :
    (insertSorted k v l).find? (fun e => e.1 == k) = some (k, v) := by
  induction l with
  | nil => simp [insertSorted]
  | cons e es ih =>
    simp only [insertSorted]
    split
    · simp
    · split
      · simp
      · rename_i h2
        rw [List.find?_cons_of_neg (p := fun (x : Bytes × α) => x.1 == k) (fun hc => h2 (beq_iff_eq.mp hc).symm)]
        exact ih

theorem find?_insertSorted_of_ne (l : List (Bytes × α)) {k j : Bytes} (v : α) (h : j ≠ k) :
    (insertSorted k v l).find? (fun e => e.1 == j) = l.find? (fun e => e.1 == j) := by
  induction l with
  | nil => simp [insertSorted, Ne.symm h]
  | cons e es ih =>
    simp only [insertSorted]
    split
    · simp [Ne.symm h]
    · split
      · rename_i h2
        subst h2
        simp [Ne.symm h]
      · by_cases h3 : e.1 = j
        · rw [List.find?_cons_of_pos (by simpa using h3), List.find?_cons_of_pos (by simpa using h3)]
        · rw [List.find?_cons_of_neg (by simpa using h3), List.find?_cons_of_neg (by simpa using h3)]
          exact ih

theorem insertSorted_insertSorted (l : List (Bytes × α)) (k : Bytes) (v w : α) :
    insertSorted k w (insertSorted k v l) = insertSorted k w l := by
  induction l with
  | nil => simp [insertSorted, lexLt.irrefl]
  | cons e es ih =>
    by_cases h1 : lexLt k e.1 = true
    · simp [insertSorted, h1, lexLt.irrefl]
    · by_cases h2 : k = e.1
      · subst h2
        simp [insertSorted, lexLt.irrefl]
      · simp only [insertSorted, h1, h2, Bool.false_eq_true, ↓reduceIte]
        rw [ih]

theorem ext {s t : Store α} (h : s.entries = t.entries) : s = t := by
  cases s
  cases t
  cases h
  rfl

/-- Read-after-write returns the written value. -/
theorem get_put_self (s : Store α) (k : Bytes) (v : α) : (s.put k v).get k = some v := by
  exact congrArg (Option.map (·.2)) (find?_insertSorted_self s.entries k v)

/-- Writing one key leaves every other key unchanged. -/
theorem get_put_of_ne (s : Store α) {k j : Bytes} (v : α) (h : j ≠ k) :
    (s.put k v).get j = s.get j := by
  exact congrArg (Option.map (·.2)) (find?_insertSorted_of_ne s.entries v h)

/-- Last write wins: the second value replaces the first entirely. -/
theorem put_put_last (s : Store α) (k : Bytes) (v w : α) : (s.put k v).put k w = s.put k w := by
  exact ext (insertSorted_insertSorted s.entries k v w)

/-- Repeating a write is idempotent. -/
theorem put_put_self (s : Store α) (k : Bytes) (v : α) : (s.put k v).put k v = s.put k v := by
  exact put_put_last s k v v

theorem get_erase_self (s : Store α) (k : Bytes) : (s.erase k).get k = none := by
  simp only [get, erase]
  rw [(List.find?_eq_none).mpr]
  · rfl
  · intro x hx
    simp only [List.mem_filter, bne_iff_ne, ne_eq] at hx
    simpa using hx.2

theorem get_erase_of_ne (s : Store α) {k j : Bytes} (h : j ≠ k) : (s.erase k).get j = s.get j := by
  simp only [get, erase, List.find?_filter]
  congr 2
  funext e
  by_cases he : e.1 = j
  · subst he
    simp [h]
  · simp [he]

theorem get_empty (k : Bytes) : (empty : Store α).get k = none := by
  rfl

/-! ## Batch semantics -/

/-- A batch holds the last occurrence of each key it mentions and leaves
the rest of the store alone. -/
theorem get_putBatch (s : Store α) (ws : List (Bytes × α)) (k : Bytes) :
    (s.putBatch ws).get k =
      match (ws.reverse.find? (fun w => w.1 == k)) with
      | some w => some w.2
      | none => s.get k := by
  induction ws generalizing s with
  | nil => rfl
  | cons w ws ih =>
    show (putBatch (s.put w.1 w.2) ws).get k = _
    rw [ih, List.reverse_cons, List.find?_append]
    cases hf : ws.reverse.find? (fun w => w.1 == k) with
    | some w' => rfl
    | none =>
      by_cases hw : w.1 = k
      · subst hw
        simp [get_put_self]
      · simp [hw, get_put_of_ne s w.2 (Ne.symm hw)]

theorem find?_eq_none_of_lt {l : List (Bytes × α)} {k : Bytes}
    (h : ∀ x ∈ l, lexLt k x.1 = true) : l.find? (fun e => e.1 == k) = none := by
  refine List.find?_eq_none.mpr ?_
  intro x hx hp
  have hk : x.1 = k := by simpa using hp
  have := h x hx
  rw [hk, lexLt.irrefl] at this
  exact Bool.false_ne_true this

theorem find?_cons_lt_none {b : Bytes × α} {bs : List (Bytes × α)} {k : Bytes}
    (hs : KeysSorted (b :: bs)) (hk : lexLt k b.1 = true) :
    (b :: bs).find? (fun e => e.1 == k) = none := by
  refine find?_eq_none_of_lt ?_
  intro x hx
  rcases List.mem_cons.mp hx with rfl | hx
  · exact hk
  · exact lexLt.trans hk ((List.pairwise_cons.mp hs).1 x hx)

theorem list_ext_of_find?_eq {l₁ l₂ : List (Bytes × α)} (h₁ : KeysSorted l₁) (h₂ : KeysSorted l₂)
    (h : ∀ k, (l₁.find? (fun e => e.1 == k)).map (·.2) = (l₂.find? (fun e => e.1 == k)).map (·.2)) :
    l₁ = l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
    cases l₂ with
    | nil => rfl
    | cons b bs =>
      have := h b.1
      simp at this
  | cons a as ih =>
    cases l₂ with
    | nil =>
      have := h a.1
      simp at this
    | cons b bs =>
      have ⟨ha, has⟩ := List.pairwise_cons.mp h₁
      have ⟨hb, hbs⟩ := List.pairwise_cons.mp h₂
      by_cases hab : a.1 = b.1
      · have hv := h a.1
        rw [List.find?_cons_of_pos (by simp), List.find?_cons_of_pos (by simp [hab])] at hv
        simp only [Option.map_some, Option.some.injEq] at hv
        have heq : a = b := Prod.ext hab hv
        subst heq
        congr 1
        refine ih has hbs ?_
        intro k
        by_cases hk : a.1 = k
        · subst hk
          rw [find?_eq_none_of_lt ha, find?_eq_none_of_lt hb]
        · have := h k
          rw [List.find?_cons_of_neg (by simpa using hk), List.find?_cons_of_neg (by simpa using hk)] at this
          exact this
      · exfalso
        rcases lexLt.total hab with hlt | hlt
        · have := h a.1
          rw [find?_cons_lt_none h₂ hlt, List.find?_cons_of_pos (by simp)] at this
          simp at this
        · have := h b.1
          rw [find?_cons_lt_none h₁ hlt, List.find?_cons_of_pos (by simp)] at this
          simp at this

theorem putBatch_put_of_any (s : Store α) (k : Bytes) (v : α) (ws : List (Bytes × α))
    (hany : ws.any (fun x => x.1 == k) = true) : (s.put k v).putBatch ws = s.putBatch ws := by
  apply ext
  refine list_ext_of_find?_eq ((s.put k v).putBatch ws).sorted (s.putBatch ws).sorted ?_
  intro j
  show ((s.put k v).putBatch ws).get j = (s.putBatch ws).get j
  rw [get_putBatch, get_putBatch]
  cases hf : ws.reverse.find? (fun w => w.1 == j) with
  | some w' => rfl
  | none =>
    have hjk : j ≠ k := by
      rintro rfl
      have ⟨x, hx, hp⟩ := List.any_eq_true.mp hany
      exact List.find?_eq_none.mp hf x (List.mem_reverse.mpr hx) hp
    exact get_put_of_ne s v hjk

/-- Pre-deduplicating a batch to last occurrences does not change the
result, which is why the engine may stage only the last occurrence. -/
theorem putBatch_dedupLast (s : Store α) (ws : List (Bytes × α)) :
    s.putBatch (dedupLast ws) = s.putBatch ws := by
  induction ws generalizing s with
  | nil => rfl
  | cons w ws ih =>
    simp only [dedupLast]
    split
    · rename_i hany
      rw [ih]
      exact (putBatch_put_of_any s w.1 w.2 ws hany).symm
    · exact ih (s.put w.1 w.2)

/-- With pairwise distinct keys, a key lookup does not depend on order. -/
theorem find?_perm_of_nodup {l l' : List (Bytes × α)} (hp : l.Perm l') (hnd : (l.map (·.1)).Nodup)
    (k : Bytes) : l.find? (fun w => w.1 == k) = l'.find? (fun w => w.1 == k) := by
  have uniq : ∀ a ∈ l, ∀ b ∈ l, a.1 = b.1 → a = b := by
    clear hp
    induction l with
    | nil => simp
    | cons x xs ih =>
      rw [List.map_cons, List.nodup_cons] at hnd
      have hx : ∀ y ∈ xs, y.1 ≠ x.1 := fun y hy hyx => hnd.1 (List.mem_map.mpr ⟨y, hy, hyx⟩)
      intro a ha b hb hab
      rcases List.mem_cons.mp ha with hax | ha' <;> rcases List.mem_cons.mp hb with hbx | hb'
      · rw [hax, hbx]
      · exact absurd (hax ▸ hab).symm (hx b hb')
      · exact absurd (hbx ▸ hab) (hx a ha')
      · exact ih hnd.2 a ha' b hb' hab
  cases h : l.find? (fun w => w.1 == k) with
  | none =>
    refine (List.find?_eq_none.mpr fun x hx => ?_).symm
    exact List.find?_eq_none.mp h x (hp.mem_iff.mpr hx)
  | some w =>
    have hw := List.mem_of_find?_eq_some h
    have hwk := List.find?_some h
    cases h' : l'.find? (fun w => w.1 == k) with
    | none => exact absurd hwk (List.find?_eq_none.mp h' w (hp.mem_iff.mp hw))
    | some w' =>
      have hw' := hp.mem_iff.mpr (List.mem_of_find?_eq_some h')
      have hwk' := List.find?_some h'
      simp only [beq_iff_eq] at hwk hwk'
      rw [uniq w hw w' hw' (hwk.trans hwk'.symm)]

/-- Delete after put: the put is cancelled entirely. -/
theorem erase_put_self (s : Store α) (k : Bytes) (v : α) : (s.put k v).erase k = s.erase k := by
  apply ext
  refine list_ext_of_find?_eq ((s.put k v).erase k).sorted (s.erase k).sorted ?_
  intro j
  show ((s.put k v).erase k).get j = (s.erase k).get j
  by_cases hj : j = k
  · subst hj
    rw [get_erase_self, get_erase_self]
  · rw [get_erase_of_ne _ hj, get_erase_of_ne _ hj, get_put_of_ne s v hj]

/-- Put after delete: the delete is undone. -/
theorem put_erase_self (s : Store α) (k : Bytes) (v : α) : (s.erase k).put k v = s.put k v := by
  apply ext
  refine list_ext_of_find?_eq ((s.erase k).put k v).sorted (s.put k v).sorted ?_
  intro j
  show ((s.erase k).put k v).get j = (s.put k v).get j
  by_cases hj : j = k
  · subst hj
    rw [get_put_self, get_put_self]
  · rw [get_put_of_ne _ v hj, get_put_of_ne _ v hj, get_erase_of_ne _ hj]

/-- Insertion order independence: a batch with pairwise distinct keys
yields the same store in any order. -/
theorem putBatch_perm (s : Store α) {ws ws' : List (Bytes × α)} (hp : ws.Perm ws')
    (hnodup : (ws.map (·.1)).Nodup) : s.putBatch ws = s.putBatch ws' := by
  apply ext
  refine list_ext_of_find?_eq (s.putBatch ws).sorted (s.putBatch ws').sorted ?_
  intro j
  show (s.putBatch ws).get j = (s.putBatch ws').get j
  rw [get_putBatch, get_putBatch,
    find?_perm_of_nodup ((List.reverse_perm ws).trans (hp.trans (List.reverse_perm ws').symm)) (by rw [List.map_reverse]; exact (List.reverse_perm _).nodup_iff.mpr hnodup) j]

/-! ## Enumeration -/

/-- Enumeration never emits a key twice. -/
theorem keys_nodup (s : Store α) : s.keys.Nodup := by
  unfold keys List.Nodup
  rw [List.pairwise_map]
  refine List.Pairwise.imp ?_ s.sorted
  intro a b hab heq
  rw [heq, lexLt.irrefl] at hab
  exact Bool.false_ne_true hab

/-- Enumeration and point lookup agree on existence. -/
theorem mem_keys_iff (s : Store α) (k : Bytes) : k ∈ s.keys ↔ (s.get k).isSome := by
  simp only [keys, get, Option.isSome_map, List.find?_isSome, List.mem_map, beq_iff_eq]

/-- Enumeration after a write contains the written key. -/
theorem mem_keys_put (s : Store α) (k : Bytes) (v : α) : k ∈ (s.put k v).keys := by
  rw [mem_keys_iff, get_put_self]
  rfl

/-- Two stores with the same point semantics enumerate identically. -/
theorem ext_of_get_eq {s t : Store α} (h : ∀ k, s.get k = t.get k) : s.entries = t.entries := by
  obtain ⟨l₁, h₁⟩ := s
  obtain ⟨l₂, h₂⟩ := t
  simp only [get] at h
  exact list_ext_of_find?_eq h₁ h₂ h

/-! ## Witnesses -/

example : ((empty : Store Nat).put [2] 20 |>.put [1] 10 |>.put [2] 21).keys = [[1], [2]] := by decide
example : ((empty : Store Nat).put [2] 20 |>.put [1] 10 |>.put [2] 21).get [2] = some 21 := by decide
example : (dedupLast [([1], 1), ([2], 2), ([1], 3)] : List (Bytes × Nat)) = [([2], 2), ([1], 3)] := by decide

end Store
end C1z
