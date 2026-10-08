import C1z.Records

/-!
# Grant digests

Models `pkg/dotc1z/engine/pebble/digest.go`, `grant_digest.go`,
`grant_digest_build.go`, and the invalidation in
`internal/rawdb/records.go` (`stageGrantDigestInvalidation`).

A digest partition is one entitlement's structural identity. Its root
is a fold over the content hashes of the grants under that entitlement:
a count and an XOR of 64-bit hashes (`digest.go` combiner). The global
root is the same fold over every grant. Leaves bucket a partition's
grants by the top `width` bits of a hash of the principal, where
`width` depends only on the count.

The hash itself (xxHash64 over a hand-built byte string) is not
modeled; `h` is an abstract function. What the model fixes is the
canonicalization: which fields of a grant feed the hash
(`GrantContent`) and which do not (`external_id`, `discovered_at`,
`expansion`, `needs_expansion`, `source_scope_key`, other annotations,
and the reference fields inside each source). Everything proved here
holds for every `h`, so it holds for xxHash64.

The one property a consumer wants and cannot have is also stated:
equal roots do not imply equal content, even for an injective hash,
because XOR is not injective on sets (`fold_not_injective`). Treating
equal roots as equal content is a collision assumption that this
module makes explicit rather than proving.

Lifecycle: digests are built at `EndSync`. A grant write or delete
after sealing removes the whole partition of its entitlement and the
global root (`invalidate`), so those read as absent, not stale, while
other partitions are untouched. A later `EndSync` rebuilds the missing
partitions and the global root from the primary rows, which equals a
fresh build (`repair_eq_build`).

Out of scope: the ABI version stamp (v2 adds `immutable` and
`is_direct` to the hash; the model is v2 only), bucket leaf storage
and sparse levels beyond `width`, the `by_entitlement_principal_hash`
index, the `ComputeEntitlementBucketDigest` result on an invalidated
partition (it returns zeros that look like "no grants"), and the
read-only-open staleness flag.
-/

namespace C1z
namespace Digest

/-- A hash value; the engine uses 64 bits. -/
abbrev Hash := Nat

/-- A grant source fact as the hash sees it: source key and `is_direct`.
The reference fields inside a source are excluded. -/
structure SourceFact where
  key : Bytes
  isDirect : Bool
  deriving Repr, DecidableEq

/-- The fields of a grant that feed its content hash
(`grantContentHash64`): the identity tuple, the `GrantImmutable`
annotation flag, and the sorted, key-deduplicated source facts. -/
structure GrantContent where
  id : GrantId
  immutable : Bool
  sources : List SourceFact
  deriving Repr, DecidableEq

/-- A stored grant with the fields a digest may or may not see. -/
structure GrantFull where
  id : GrantId
  externalId : Bytes
  immutable : Bool
  sources : List SourceFact
  discoveredAt : Nat
  needsExpansion : Bool
  deriving Repr, DecidableEq

/-- Canonicalization: project away every excluded field. Sources are
sorted by key with last-wins on duplicate keys in the engine; the model
takes them as already canonical. -/
def canonical (g : GrantFull) : GrantContent :=
  { id := g.id, immutable := g.immutable, sources := g.sources }

/-- A digest node: count and XOR fold (`digest.go` leaf/root values). -/
structure Node where
  count : Nat
  xor : Hash
  deriving Repr, DecidableEq

def Node.zero : Node := { count := 0, xor := 0 }

def Node.combine (a b : Node) : Node := { count := a.count + b.count, xor := a.xor ^^^ b.xor }

/-- Fold a list of contents under hash `h`. -/
def fold (h : GrantContent → Hash) (cs : List GrantContent) : Node :=
  { count := cs.length, xor := cs.foldl (fun acc c => acc ^^^ h c) 0 }

/-- `chooseDigestWidth`: the smallest width in `0..16` whose average
bucket holds at most 512 records. -/
def targetBucket : Nat := 512
def maxWidth : Nat := 16

def chooseWidth (count : Nat) : Nat :=
  ((List.range (maxWidth + 1)).find? fun w => count ≤ targetBucket * 2 ^ w).getD maxWidth

/-! ## Fold laws -/

private theorem xorFold_eq (h : GrantContent → Hash) (a : Hash) (l : List GrantContent) :
    l.foldl (fun acc c => acc ^^^ h c) a = a ^^^ l.foldl (fun acc c => acc ^^^ h c) 0 := by
  induction l generalizing a with
  | nil => exact (Nat.xor_zero a).symm
  | cons c cs ih =>
    simp only [List.foldl_cons]
    rw [ih, ih (0 ^^^ h c), Nat.zero_xor, Nat.xor_assoc]

private theorem xorFold_perm (h : GrantContent → Hash) {a b : List GrantContent} (p : a.Perm b) (x : Hash) :
    a.foldl (fun acc c => acc ^^^ h c) x = b.foldl (fun acc c => acc ^^^ h c) x := by
  induction p generalizing x with
  | nil => rfl
  | cons c _ ih => exact ih _
  | swap c d l =>
    simp only [List.foldl_cons]
    rw [Nat.xor_assoc, Nat.xor_assoc, Nat.xor_comm (h d)]
  | trans _ _ ih₁ ih₂ => exact (ih₁ x).trans (ih₂ x)

private theorem combine_zero_left (a : Node) : Node.zero.combine a = a := by
  cases a
  simp only [Node.zero, Node.combine, Nat.zero_add, Nat.zero_xor]

private theorem combine_assoc (a b c : Node) : (a.combine b).combine c = a.combine (b.combine c) := by
  simp only [Node.combine, Nat.add_assoc, Nat.xor_assoc]

private theorem foldl_combine (a : Node) (ns : List Node) :
    ns.foldl Node.combine a = a.combine (ns.foldl Node.combine Node.zero) := by
  induction ns generalizing a with
  | nil =>
    cases a
    simp only [List.foldl_nil, Node.zero, Node.combine, Nat.add_zero, Nat.xor_zero]
  | cons n ns ih =>
    simp only [List.foldl_cons]
    rw [ih, ih (Node.zero.combine n), combine_zero_left, combine_assoc]

theorem fold_nil (h : GrantContent → Hash) : fold h [] = Node.zero := by
  rfl

/-- Splitting a fold anywhere and combining gives the same node, which
is why chunked builds, bucket leaves, and partition roots all agree. -/
theorem fold_append (h : GrantContent → Hash) (a b : List GrantContent) :
    fold h (a ++ b) = (fold h a).combine (fold h b) := by
  simp only [fold, Node.combine, List.length_append, List.foldl_append]
  rw [xorFold_eq]

/-- Insertion order does not matter. -/
theorem fold_perm (h : GrantContent → Hash) {a b : List GrantContent} (p : a.Perm b) :
    fold h a = fold h b := by
  simp only [fold, p.length_eq, xorFold_perm h p]

/-- Equal canonical content gives equal digests. -/
theorem fold_eq_of_content_eq (h : GrantContent → Hash) {a b : List GrantFull}
    (hc : a.map canonical = b.map canonical) : fold h (a.map canonical) = fold h (b.map canonical) := by
  rw [hc]

/-- `external_id`, `discovered_at`, and `needs_expansion` do not affect the
digest. -/
theorem canonical_ignores_excluded (g : GrantFull) (ext : Bytes) (t : Nat) (ne : Bool) :
    canonical { g with externalId := ext, discoveredAt := t, needsExpansion := ne } = canonical g := by
  rfl

/-- A content that differs from the others only in its principal id. -/
private def cN (n : Nat) : GrantContent :=
  { id := { ent := { rt := [0x61], rid := [0x31], ext := [0x6d] }, prt := [0x75], prid := [n] },
    immutable := false, sources := [] }

/-- Negative result: equal nodes do not imply equal content, even for an
injective hash. Two distinct two-element sets can share count and XOR. -/
theorem fold_not_injective :
    ∃ (h : GrantContent → Hash) (a b : List GrantContent),
      (∀ x ∈ a ++ b, ∀ y ∈ a ++ b, h x = h y → x = y) ∧ fold h a = fold h b ∧ ∀ c, c ∈ a → c ∉ b := by
  refine ⟨fun c => c.id.prid.headD 0, [cN 1, cN 2], [cN 0, cN 3], ?_, ?_, ?_⟩ <;> decide

/-- The width is bounded and meets its target when it can. -/
theorem chooseWidth_le (count : Nat) : chooseWidth count ≤ maxWidth := by
  unfold chooseWidth
  cases hf : (List.range (maxWidth + 1)).find? fun w => count ≤ targetBucket * 2 ^ w with
  | none => exact Nat.le_refl _
  | some w =>
    have hm := List.mem_range.mp (List.mem_of_find?_eq_some hf)
    exact Nat.le_of_lt_succ hm

theorem chooseWidth_spec (count : Nat) (h : count ≤ targetBucket * 2 ^ maxWidth) :
    count ≤ targetBucket * 2 ^ chooseWidth count := by
  unfold chooseWidth
  cases hf : (List.range (maxWidth + 1)).find? fun w => count ≤ targetBucket * 2 ^ w with
  | none => exact h
  | some w =>
    have hw := List.find?_some hf
    exact of_decide_eq_true hw

theorem chooseWidth_zero : chooseWidth 0 = 0 := by
  decide

/-! ## Partitions over a grant store -/

/-- The content of a stored grant record. The store's `GrantRecord` carries
only the identity and `external_id`; immutability and sources are taken
from a side function so the store laws of `C1z.Records` apply unchanged. -/
def contentOf (facts : GrantId → Bool × List SourceFact) (r : GrantRecord) : GrantContent :=
  { id := r.id, immutable := (facts r.id).1, sources := (facts r.id).2 }

/-- A partition root: the fold over the entitlement's grants. -/
def partitionRoot (h : GrantContent → Hash) (facts : GrantId → Bool × List SourceFact) (s : GrantStore)
    (e : EntitlementId) : Node :=
  fold h ((s.grantsForEntitlement e).map (contentOf facts))

/-- The global root: the fold over every grant. -/
def globalRoot (h : GrantContent → Hash) (facts : GrantId → Bool × List SourceFact) (s : GrantStore) : Node :=
  fold h (s.allGrants.map (contentOf facts))

/-- The entitlements that own at least one grant, in first-seen order. -/
def grantEntitlements (s : GrantStore) : List EntitlementId :=
  (s.allGrants.map (·.id.ent)).eraseDups

/-- A partition root's count is the number of grants under the entitlement. -/
theorem partitionRoot_count (h : GrantContent → Hash) (facts : GrantId → Bool × List SourceFact)
    (s : GrantStore) (e : EntitlementId) :
    (partitionRoot h facts s e).count = (s.grantsForEntitlement e).length := by
  simp only [partitionRoot, fold, List.length_map]

/-- A fold over a list equals the combination of the folds over its
groups by `key`, taken in first-seen key order. -/
private theorem fold_group {α β : Type} [DecidableEq β] (h : GrantContent → Hash) (g : α → GrantContent)
    (key : α → β) (l : List α) :
    fold h (l.map g) =
      ((l.map key).eraseDups.map fun k => fold h ((l.filter fun x => key x == k).map g)).foldl
        Node.combine Node.zero := by
  match l with
  | [] => rfl
  | x :: xs =>
    have ih := fold_group h g key (xs.filter ((fun b => !b == key x) ∘ key))
    rw [List.map_cons (f := key), List.eraseDups_cons, List.filter_map, List.map_cons (a := key x), List.foldl_cons,
      foldl_combine, combine_zero_left]
    have hrest :
        (((xs.filter ((fun b => !b == key x) ∘ key)).map key).eraseDups.map fun k =>
            fold h (((x :: xs).filter fun y => key y == k).map g)) =
          (((xs.filter ((fun b => !b == key x) ∘ key)).map key).eraseDups.map fun k =>
            fold h (((xs.filter ((fun b => !b == key x) ∘ key)).filter fun y => key y == k).map g)) := by
      apply List.map_congr_left
      intro k hk
      obtain ⟨y, hy, rfl⟩ := List.mem_map.mp (List.mem_eraseDups.mp hk)
      have hyx : key y ≠ key x := by
        have := (List.mem_filter.mp hy).2
        simpa using this
      rw [List.filter_cons_of_neg (by simpa using Ne.symm hyx), List.filter_filter]
      congr 2
      apply List.filter_congr
      intro z _
      by_cases hz : key z = key y
      · simp [hz, hyx]
      · simp [hz]
    rw [hrest, ← ih, List.filter_cons_of_pos (p := fun y => key y == key x) (beq_self_eq_true (key x))]
    have hp : (x :: xs).Perm ((x :: xs.filter fun y => key y == key x) ++
        xs.filter ((fun b => !b == key x) ∘ key)) :=
      List.Perm.cons x (List.filter_append_perm (fun y => key y == key x) xs).symm
    rw [fold_perm h (hp.map g), List.map_append, fold_append]
termination_by l.length
decreasing_by
  have := List.length_filter_le ((fun b => !b == key x) ∘ key) xs
  simp only [List.length_cons]
  omega

/-- Writing two values that agree under `f` at one key leaves every
key-filtered, `f`-mapped view equal. -/
private theorem filter_map_insertSorted_congr {α γ : Type} (p : Bytes → Bool) (f : α → γ) (k : Bytes)
    (v w : α) (l : List (Bytes × α)) (hf : f v = f w) :
    ((Store.insertSorted k v l).filter fun kv => p kv.1).map (fun kv => f kv.2) =
      ((Store.insertSorted k w l).filter fun kv => p kv.1).map (fun kv => f kv.2) := by
  induction l with
  | nil =>
    cases hp : p k <;> simp [Store.insertSorted, hp, hf]
  | cons e es ih =>
    simp only [Store.insertSorted]
    split
    · cases hp : p k <;> simp [hp, hf]
    · split
      · cases hp : p k <;> simp [hp, hf]
      · cases hp : p e.1 <;> simp [hp, ih]

/-- Filtering out the written key undoes the write. -/
private theorem filter_insertSorted_of_false {α : Type} (q : Bytes × α → Bool) (k : Bytes) (v : α)
    (l : List (Bytes × α)) (hq : q (k, v) = false) (hl : ∀ e ∈ l, e.1 = k → q e = false) :
    (Store.insertSorted k v l).filter q = l.filter q := by
  induction l with
  | nil => simp [Store.insertSorted, hq]
  | cons e es ih =>
    have hes : ∀ e' ∈ es, e'.1 = k → q e' = false := fun e' he' => hl e' (List.mem_cons_of_mem _ he')
    simp only [Store.insertSorted]
    split
    · rw [List.filter_cons_of_neg (by simp [hq])]
    · split
      · rename_i hke
        rw [List.filter_cons_of_neg (by simp [hq]),
          List.filter_cons_of_neg (by simp [hl e List.mem_cons_self hke.symm])]
      · rw [List.filter_cons, List.filter_cons, ih hes]

/-- A grant write leaves every other entitlement's scan unchanged. -/
private theorem grantsForEntitlement_putGrants_of_ne (s : GrantStore) (r : GrantRecord) {f : EntitlementId}
    (hne : f ≠ r.id.ent) : (s.putGrants [r]).grantsForEntitlement f = s.grantsForEntitlement f := by
  show ((Store.insertSorted r.key r s.entries).filter _).map _ = (s.entries.filter _).map _
  have hr : decide (Codec.encodeScanPrefix .grant f.tuple <+: r.key) = false := by
    apply decide_eq_false
    intro hpre
    exact hne (GrantId.ent_eq_of_key_under_prefix hpre).symm
  rw [filter_insertSorted_of_false _ _ _ _ hr]
  intro e _ he
  rw [he]
  exact hr

/-- Erasing a grant leaves every other entitlement's scan unchanged. -/
private theorem grantsForEntitlement_deleteGrant_of_ne (s : GrantStore) (g : GrantId) {f : EntitlementId}
    (hne : f ≠ g.ent) : (s.deleteGrant g).grantsForEntitlement f = s.grantsForEntitlement f := by
  show ((s.entries.filter (fun e => e.1 != g.key)).filter _).map _ = (s.entries.filter _).map _
  rw [List.filter_filter]
  congr 1
  apply List.filter_congr
  intro kv _
  cases hpre : decide (Codec.encodeScanPrefix .grant f.tuple <+: kv.1) with
  | false => simp only [Bool.false_and]
  | true =>
    simp only [Bool.true_and, bne_iff_ne, ne_eq]
    intro hk
    rw [hk] at hpre
    exact hne (GrantId.ent_eq_of_key_under_prefix (of_decide_eq_true hpre)).symm

/-- The global root is the combination of the partition roots. -/
theorem globalRoot_eq_combine {s : GrantStore} (hk : GrantStore.Keyed s) (h : GrantContent → Hash)
    (facts : GrantId → Bool × List SourceFact) :
    globalRoot h facts s = (grantEntitlements s).foldl (fun acc e => acc.combine (partitionRoot h facts s e)) Node.zero := by
  have hg : ∀ e, s.grantsForEntitlement e = s.allGrants.filter (fun x => x.id.ent == e) := by
    intro e
    rw [GrantStore.grantsForEntitlement_eq_filter hk, GrantStore.allGrants, List.filter_map]
    rfl
  unfold globalRoot partitionRoot grantEntitlements
  simp only [hg]
  rw [fold_group h (contentOf facts) (fun x : GrantRecord => x.id.ent), List.foldl_map]

/-- Grants differing only in `external_id` give the same partition root:
the store keeps one of them, and its content is the same either way. -/
theorem partitionRoot_putGrants_externalId (h : GrantContent → Hash)
    (facts : GrantId → Bool × List SourceFact) (s : GrantStore) (r : GrantRecord) (ext : Bytes) :
    partitionRoot h facts (s.putGrants [r]) r.id.ent =
      partitionRoot h facts (s.putGrants [{ r with externalId := ext }]) r.id.ent := by
  unfold partitionRoot GrantStore.grantsForEntitlement
  rw [List.map_map, List.map_map]
  congr 1
  exact filter_map_insertSorted_congr (fun b => decide (Codec.encodeScanPrefix .grant r.id.ent.tuple <+: b)) (contentOf facts) r.key r { r with externalId := ext } s.entries rfl

/-! ## Lifecycle: build, invalidate, repair -/

/-- Stored digest state: present partition roots and the global root.
`none` is absent, which the API reports as `found = false`. -/
structure State where
  partitions : List (EntitlementId × Node)
  global : Option Node
  deriving Repr, DecidableEq

def State.lookup (st : State) (e : EntitlementId) : Option Node :=
  (st.partitions.find? fun p => p.1 == e).map (·.2)

/-- `EndSync`'s build: a root for every entitlement that owns a grant and
for every entitlement record (zero-grant roots), plus the global root. -/
def build (h : GrantContent → Hash) (facts : GrantId → Bool × List SourceFact) (s : GrantStore)
    (ents : List EntitlementId) : State :=
  let es := (grantEntitlements s ++ ents).eraseDups
  { partitions := es.map fun e => (e, partitionRoot h facts s e)
    global := some (globalRoot h facts s) }

/-- A grant write or delete under entitlement `e` after sealing
(`stageGrantDigestInvalidation`): drop `e`'s partition and the global root. -/
def State.invalidate (st : State) (e : EntitlementId) : State :=
  { partitions := st.partitions.filter fun p => p.1 != e, global := none }

/-- The digest effect of a grant write: a put always stages a row and so
always invalidates (`StageGrantPutInline`). -/
def State.afterPut (st : State) (r : GrantRecord) : State := st.invalidate r.id.ent

/-- The digest effect of a structural delete: `DeleteGrantByIdentityRefs`
stages nothing when the row is absent, so it invalidates nothing. Only
a delete that removes a stored row invalidates its partition. -/
def State.afterDelete (st : State) (s : GrantStore) (g : GrantId) : State :=
  if (s.getGrant g).isSome then st.invalidate g.ent else st

/-- `RepairMissingGrantDigests`: keep present partitions, rebuild missing
ones, recompute the global root. -/
def repair (h : GrantContent → Hash) (facts : GrantId → Bool × List SourceFact) (s : GrantStore)
    (ents : List EntitlementId) (st : State) : State :=
  let es := (grantEntitlements s ++ ents).eraseDups
  { partitions := es.map fun e =>
      match st.lookup e with
      | some n => (e, n)
      | none => (e, partitionRoot h facts s e)
    global := some (globalRoot h facts s) }

/-- The invariant a stored state satisfies when every present partition
root is the fold of the current store. -/
def State.Accurate (h : GrantContent → Hash) (facts : GrantId → Bool × List SourceFact) (s : GrantStore)
    (st : State) : Prop :=
  ∀ e n, st.lookup e = some n → n = partitionRoot h facts s e

theorem build_accurate (h : GrantContent → Hash) (facts : GrantId → Bool × List SourceFact) (s : GrantStore)
    (ents : List EntitlementId) : (build h facts s ents).Accurate h facts s := by
  intro e n hl
  simp only [State.lookup, build, List.find?_map] at hl
  obtain ⟨p, hp, rfl⟩ := Option.map_eq_some_iff.mp hl
  obtain ⟨e', hf, rfl⟩ := Option.map_eq_some_iff.mp hp
  have he := List.find?_some hf
  simp only [Function.comp_apply, beq_iff_eq] at he
  subst he
  rfl

/-- After invalidating `e`, `e` reads absent. -/
theorem lookup_invalidate_self (st : State) (e : EntitlementId) : (st.invalidate e).lookup e = none := by
  simp only [State.lookup, State.invalidate]
  rw [List.find?_eq_none.mpr]
  · rfl
  · intro x hx hp
    have h1 := (List.mem_filter.mp hx).2
    rw [beq_iff_eq.mp hp] at h1
    exact absurd h1 (by simp)

/-- Other partitions are untouched. -/
theorem lookup_invalidate_of_ne (st : State) {e f : EntitlementId} (hne : f ≠ e) :
    (st.invalidate e).lookup f = st.lookup f := by
  simp only [State.lookup, State.invalidate, List.find?_filter]
  congr 2
  funext p
  by_cases hp : p.1 = f
  · subst hp
    simp [hne]
  · simp [hp]

theorem global_invalidate (st : State) (e : EntitlementId) : (st.invalidate e).global = none := by
  rfl

/-- Invalidation preserves accuracy of what remains, for any store the
remaining partitions were accurate for. -/
theorem accurate_invalidate {h : GrantContent → Hash} {facts : GrantId → Bool × List SourceFact}
    {s : GrantStore} {st : State} (ha : st.Accurate h facts s) (e : EntitlementId) :
    (st.invalidate e).Accurate h facts s := by
  intro f n hl
  by_cases hfe : f = e
  · subst hfe
    rw [lookup_invalidate_self] at hl
    exact absurd hl (by simp)
  · rw [lookup_invalidate_of_ne st hfe] at hl
    exact ha f n hl

/-- Repairing an accurate state yields the fresh build. -/
theorem repair_eq_build {h : GrantContent → Hash} {facts : GrantId → Bool × List SourceFact} {s : GrantStore}
    (ents : List EntitlementId) {st : State} (ha : st.Accurate h facts s) :
    repair h facts s ents st = build h facts s ents := by
  simp only [repair, build, State.mk.injEq, and_true]
  apply List.map_congr_left
  intro e _
  split
  · rename_i n hn
    rw [ha e n hn]
  · rfl

/-- Deleting an absent grant leaves the digest state, and its accuracy,
untouched: the store is unchanged, and so is the state. -/
theorem afterDelete_absent {st : State} {s : GrantStore} {g : GrantId} (h : s.getGrant g = none) :
    st.afterDelete s g = st := by
  unfold State.afterDelete
  simp only [h, Option.isSome_none, Bool.false_eq_true, ↓reduceIte]

/-- Deleting a stored grant under `e` invalidates `e`, and the result is
accurate for the store without that grant. -/
theorem accurate_afterDelete_present {h : GrantContent → Hash} {facts : GrantId → Bool × List SourceFact}
    {s : GrantStore} {st : State} (ha : st.Accurate h facts s) (g : GrantId) (hp : (s.getGrant g).isSome) :
    (st.afterDelete s g).Accurate h facts (s.deleteGrant g) := by
  simp only [State.afterDelete, hp, ↓reduceIte]
  intro f n hl
  by_cases hfe : f = g.ent
  · subst hfe
    rw [lookup_invalidate_self] at hl
    exact absurd hl (by simp)
  · rw [lookup_invalidate_of_ne st hfe] at hl
    rw [ha f n hl]
    unfold partitionRoot
    rw [grantsForEntitlement_deleteGrant_of_ne s g hfe]

/-- Writing a grant under `e` and invalidating `e` leaves the state
accurate for the new store, because the only partition whose fold
changed is gone. -/
theorem accurate_invalidate_putGrants {h : GrantContent → Hash} {facts : GrantId → Bool × List SourceFact}
    {s : GrantStore} (_hk : GrantStore.Keyed s) {st : State} (ha : st.Accurate h facts s) (r : GrantRecord) :
    (st.invalidate r.id.ent).Accurate h facts (s.putGrants [r]) := by
  intro f n hl
  by_cases hfe : f = r.id.ent
  · subst hfe
    rw [lookup_invalidate_self] at hl
    exact absurd hl (by simp)
  · rw [lookup_invalidate_of_ne st hfe] at hl
    rw [ha f n hl]
    unfold partitionRoot
    rw [grantsForEntitlement_putGrants_of_ne s r hfe]

/-! ## Witnesses -/

private def cA : GrantContent :=
  { id := { ent := { rt := [0x61], rid := [0x31], ext := [0x6d] }, prt := [0x75], prid := [0x31] },
    immutable := false, sources := [] }
private def cB : GrantContent := { cA with id := { cA.id with prid := [0x32] } }

/-- Order independence on a concrete hash. -/
example : fold (fun c => c.id.prid.length + c.id.prid.headD 0) [cA, cB] =
    fold (fun c => c.id.prid.length + c.id.prid.headD 0) [cB, cA] := by decide

/-- XOR collision on two-element sets: `{1, 2}` and `{0, 3}` share count 2 and XOR 3. -/
example : (1 ^^^ 2 : Nat) = (0 ^^^ 3 : Nat) := by decide

example : chooseWidth 0 = 0 := by decide
example : chooseWidth 512 = 0 := by decide
example : chooseWidth 513 = 1 := by decide
example : chooseWidth 1025 = 2 := by decide

end Digest
end C1z
