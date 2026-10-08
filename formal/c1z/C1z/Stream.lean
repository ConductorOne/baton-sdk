import C1z.Result
import C1z.Records
import C1z.Index

/-!
# Streaming readers

Models `pkg/dotc1z/engine/pebble/adapter_streaming.go` (`StreamGrants`,
`StreamResources`, `StreamEntitlements`) against the contract in
`pkg/connectorstore/streaming.go`.

A stream walks rows in key order and, for each scanned row, first
checks the context, then applies the post-filter, then yields. The
callback records the first error and stops; nothing is yielded after
it. The consumer may stop early, which ends the stream with no error
and no completion signal.

Two consequences the model states and the `stream` oracle family
replays:

- The context check runs before the post-filter, so a cancelled
  context over a keyspace with scanned rows yields the cancellation
  error even when no row matches, and over an empty keyspace yields
  nothing at all (`run_cancelled_empty`, `run_cancelled_nonempty`).
- Without cancellation or early stop, the records yielded are exactly
  the filtered collection in order, the same list the paginated reader
  returns (`run_eq_filter`).

Which rows a stream scans depends on the filter (`StreamGrants`'s
switch): an entitlement id scans that entitlement's prefix; a principal
type alone walks the `by_principal` index, with the deferred-index gap
of `C1z.Index`; anything else is a full primary scan. Those are
`grantRows` below.

Out of scope: `IncludeExpansion` (a no-op on Pebble), the wrapped error
text on the index path (the model yields `cancelled`; the engine wraps
it, and `errors.Is` still holds), the bare-id resolution of the
entitlement filter (the oracle only generates ids that resolve to
exactly one entitlement row), writes during iteration (primary scans
read a point-in-time iterator; the index path mixes a snapshot with
live point reads), and the lookup-map build's own cancellation check.
-/

namespace C1z
namespace Stream

open Result

/-- Consumer behavior: cancel the context after receiving this many
records, and stop iterating after receiving this many records.
`cancelAfter = some 0` is a context cancelled before the stream starts. -/
structure Consumer where
  cancelAfter : Option Nat
  breakAfter : Option Nat
  deriving Repr, DecidableEq

def Consumer.patient : Consumer := { cancelAfter := none, breakAfter := none }

/-- Walk the rows. `received` counts records yielded so far; `cancelled`
is whether the context is cancelled when the next row is scanned. -/
def go {α : Type} (keep : α → Bool) (c : Consumer) :
    List α → Nat → Bool → List (Yield α)
  | [], _, _ => []
  | r :: rs, received, cancelled =>
    if cancelled then [.error .cancelled]
    else if keep r then
      let received' := received + 1
      if c.breakAfter = some received' then [.record r]
      else .record r :: go keep c rs received' (c.cancelAfter = some received')
    else go keep c rs received cancelled

/-- The yields of a stream over `rows` with post-filter `keep`. -/
def run {α : Type} (rows : List α) (keep : α → Bool) (c : Consumer) : List (Yield α) :=
  if c.breakAfter = some 0 then [] else go keep c rows 0 (c.cancelAfter = some 0)

/-- The records among the yields, in order. -/
def records {α : Type} (ys : List (Yield α)) : List α :=
  ys.filterMap fun | .record a => some a | .error _ => none

/-- Which rows `StreamGrants` scans for a filter. -/
inductive GrantFilter where
  /-- No filter, or a principal id alone, or principal type and id: full primary scan. -/
  | primary
  /-- An entitlement id: that entitlement's prefix. -/
  | entitlement (e : EntitlementId)
  /-- A principal type alone: the `by_principal` index under that type. -/
  | principalType (prt : Bytes)
  deriving Repr, DecidableEq

/-- The rows a grant stream scans, before the post-filter. -/
def grantRows (x : IndexedGrants) : GrantFilter → List GrantRecord
  | .primary => x.store.allGrants
  | .entitlement e => x.store.grantsForEntitlement e
  | .principalType prt => x.grantsForPrincipalType prt

/-! ## Laws -/

/-- Every `go` output is a run of records, optionally followed by the
cancellation error. -/
theorem go_shape {α : Type} (keep : α → Bool) (c : Consumer) (rows : List α) :
    ∀ n b, ∃ ms : List α, go keep c rows n b = ms.map Yield.record ∨
      go keep c rows n b = ms.map Yield.record ++ [.error .cancelled] := by
  induction rows with
  | nil => intro n b; exact ⟨[], Or.inl rfl⟩
  | cons r rs ih =>
    intro n b
    cases b with
    | true => exact ⟨[], Or.inr rfl⟩
    | false =>
      simp only [go, Bool.false_eq_true, ↓reduceIte]
      by_cases hk : keep r = true
      · simp only [hk, ↓reduceIte]
        split
        · exact ⟨[r], Or.inl rfl⟩
        · obtain ⟨ms, h | h⟩ := ih (n + 1) (decide (c.cancelAfter = some (n + 1)))
          · exact ⟨r :: ms, Or.inl (by rw [h]; rfl)⟩
          · exact ⟨r :: ms, Or.inr (by rw [h]; rfl)⟩
      · simp only [hk, Bool.false_eq_true, ↓reduceIte]
        exact ih n false

theorem run_shape {α : Type} (rows : List α) (keep : α → Bool) (c : Consumer) :
    ∃ ms : List α, run rows keep c = ms.map Yield.record ∨
      run rows keep c = ms.map Yield.record ++ [.error .cancelled] := by
  unfold run
  split
  · exact ⟨[], Or.inl rfl⟩
  · exact go_shape keep c rows 0 _

theorem errorTerminal_map_record {α : Type} (ms : List α) : ErrorTerminal (ms.map Yield.record) := by
  intro i e h
  rw [List.getElem?_map] at h
  cases hm : ms[i]? <;> simp [hm] at h

theorem errorTerminal_map_record_append {α : Type} (ms : List α) (e₀ : ListError) :
    ErrorTerminal (ms.map Yield.record ++ [.error e₀]) := by
  intro i e h
  have hlt := (List.getElem?_eq_some_iff.1 h).1
  simp only [List.length_append, List.length_map, List.length_singleton] at hlt ⊢
  rcases Nat.lt_or_ge i ms.length with hi | hi
  · rw [List.getElem?_append_left (by simpa only [List.length_map] using hi), List.getElem?_map] at h
    cases hm : ms[i]? <;> simp [hm] at h
  · omega

theorem records_go_prefix {α : Type} (keep : α → Bool) (c : Consumer) (rows : List α) :
    ∀ n b, records (go keep c rows n b) <+: rows.filter keep := by
  induction rows with
  | nil => intro n b; exact List.nil_prefix
  | cons r rs ih =>
    intro n b
    cases b with
    | true => exact List.nil_prefix
    | false =>
      simp only [go, Bool.false_eq_true, ↓reduceIte]
      by_cases hk : keep r = true
      · simp only [hk, ↓reduceIte, List.filter_cons_of_pos hk]
        split
        · exact (List.prefix_cons_inj r).2 List.nil_prefix
        · exact (List.prefix_cons_inj r).2 (ih _ _)
      · simp only [hk, Bool.false_eq_true, ↓reduceIte, List.filter_cons_of_neg hk]
        exact ih n false

theorem go_patient {α : Type} (keep : α → Bool) (rows : List α) :
    ∀ n, go keep Consumer.patient rows n false = (rows.filter keep).map Yield.record := by
  induction rows with
  | nil => intro n; rfl
  | cons r rs ih =>
    intro n
    simp only [go, Bool.false_eq_true, ↓reduceIte]
    by_cases hk : keep r = true
    · simp only [hk, ↓reduceIte, List.filter_cons_of_pos hk]
      simp only [Consumer.patient, reduceCtorEq, ↓reduceIte, decide_false, List.map_cons]
      exact congrArg _ (ih _)
    · simp only [hk, Bool.false_eq_true, ↓reduceIte, List.filter_cons_of_neg hk]
      exact ih n

theorem go_break {α : Type} (keep : α → Bool) (rows : List α) :
    ∀ m n, 0 < n → n ≤ (rows.filter keep).length →
      go keep { cancelAfter := none, breakAfter := some (m + n) } rows m false =
        ((rows.filter keep).take n).map Yield.record := by
  induction rows with
  | nil => intro m n hn hlen; simp only [List.filter_nil, List.length_nil] at hlen; omega
  | cons r rs ih =>
    intro m n hn hlen
    simp only [go, Bool.false_eq_true, ↓reduceIte]
    by_cases hk : keep r = true
    · simp only [hk, ↓reduceIte]
      rw [List.filter_cons_of_pos hk] at hlen ⊢
      obtain ⟨k, rfl⟩ : ∃ k, n = k + 1 := ⟨n - 1, by omega⟩
      by_cases hk0 : k = 0
      · subst hk0
        simp
      · have hne : ¬ (some (m + (k + 1)) = some (m + 1)) := by
          simp only [Option.some.injEq]; omega
        simp only [hne, ↓reduceIte]
        have := ih (m + 1) k (by omega) (by simp only [List.length_cons] at hlen; omega)
        rw [show m + 1 + k = m + (k + 1) by omega] at this
        simp only [reduceCtorEq, decide_false, this, List.take_succ_cons, List.map_cons]
    · simp only [hk, Bool.false_eq_true, ↓reduceIte, List.filter_cons_of_neg hk] at hlen ⊢
      exact ih m n hn hlen


/-- Every yield list has at most one error, and it is last. -/
theorem run_error_terminal {α : Type} (rows : List α) (keep : α → Bool) (c : Consumer) :
    ErrorTerminal (run rows keep c) := by
  obtain ⟨ms, h | h⟩ := run_shape rows keep c <;> rw [h]
  · exact errorTerminal_map_record ms
  · exact errorTerminal_map_record_append ms _

theorem run_at_most_one_error {α : Type} (rows : List α) (keep : α → Bool) (c : Consumer) :
    ((run rows keep c).filter fun | .error _ => true | .record _ => false).length ≤ 1 := by
  obtain ⟨ms, h | h⟩ := run_shape rows keep c <;> rw [h]
  · simp only [List.filter_map, Function.comp_def]
    rw [List.filter_eq_nil_iff.2 (fun _ _ => Bool.false_ne_true)]
    exact Nat.zero_le 1
  · simp [List.filter_append, List.filter_map]

/-- A patient consumer receives exactly the filtered collection, in order. -/
theorem run_eq_filter {α : Type} (rows : List α) (keep : α → Bool) :
    run rows keep Consumer.patient = (rows.filter keep).map Yield.record := by
  unfold run
  simp only [Consumer.patient, reduceCtorEq, ↓reduceIte, decide_false]
  exact go_patient keep rows 0

/-- The records received are always a prefix of the filtered collection. -/
theorem records_run_prefix {α : Type} (rows : List α) (keep : α → Bool) (c : Consumer) :
    records (run rows keep c) <+: rows.filter keep := by
  unfold run
  split
  · exact List.nil_prefix
  · exact records_go_prefix keep c rows 0 _

/-- A cancelled context over an empty keyspace yields nothing: no record
and no error. -/
theorem run_cancelled_empty {α : Type} (keep : α → Bool) (c : Consumer) (_h : c.cancelAfter = some 0)
    (hb : c.breakAfter ≠ some 0) : run ([] : List α) keep c = [] := by
  unfold run
  simp only [hb, ↓reduceIte]
  rfl

/-- A cancelled context over a non-empty keyspace yields exactly the
cancellation error, whether or not any row matches the filter. -/
theorem run_cancelled_nonempty {α : Type} (rows : List α) (keep : α → Bool) (c : Consumer)
    (h : c.cancelAfter = some 0) (hb : c.breakAfter ≠ some 0) (hne : rows ≠ []) :
    run rows keep c = [.error .cancelled] := by
  unfold run
  cases rows with
  | nil => exact absurd rfl hne
  | cons r rs => simp only [hb, h, ↓reduceIte, decide_true, go]

/-- Early stop: a consumer that stops after `n` records receives `n`
records and no error, when at least `n` rows match. -/
theorem run_break {α : Type} (rows : List α) (keep : α → Bool) (n : Nat) (hn : 0 < n)
    (hlen : n ≤ (rows.filter keep).length) :
    run rows keep { cancelAfter := none, breakAfter := some n } =
      ((rows.filter keep).take n).map Yield.record := by
  unfold run
  have hb : ¬ (some n = some 0) := by simp only [Option.some.injEq]; omega
  simp only [hb, ↓reduceIte, reduceCtorEq, decide_false]
  simpa only [Nat.zero_add] using go_break keep rows 0 n hn hlen

/-- The consumer's verdict: a patient stream exhausts; a cancelled stream
over a non-empty keyspace fails. -/
theorem streamEnd_run_patient {α : Type} (rows : List α) (keep : α → Bool) :
    streamEnd (run rows keep Consumer.patient) = .exhausted := by
  rw [run_eq_filter]
  apply streamEnd_exhausted_of_no_error
  intro y hy e he
  subst he
  simp at hy

theorem streamEnd_run_cancelled {α : Type} (rows : List α) (keep : α → Bool) (c : Consumer)
    (h : c.cancelAfter = some 0) (hb : c.breakAfter ≠ some 0) (hne : rows ≠ []) :
    streamEnd (run rows keep c) = .failed .cancelled := by
  rw [run_cancelled_nonempty rows keep c h hb hne]
  rfl

/-- Negative result: early stop is indistinguishable from exhaustion in
the yields themselves when the consumer stops exactly at the last match. -/
theorem run_break_eq_patient_at_end {α : Type} (rows : List α) (keep : α → Bool)
    (hpos : 0 < (rows.filter keep).length) :
    run rows keep { cancelAfter := none, breakAfter := some (rows.filter keep).length } =
      run rows keep Consumer.patient := by
  rw [run_break rows keep _ hpos (Nat.le_refl _), List.take_length, run_eq_filter]

/-- The unfiltered grant stream and the paginated listing agree. -/
theorem grantRows_primary (x : IndexedGrants) : grantRows x .primary = x.store.allGrants := rfl

/-- The entitlement-filtered stream and `ListGrantsForEntitlement` agree. -/
theorem grantRows_entitlement (x : IndexedGrants) (e : EntitlementId) :
    grantRows x (.entitlement e) = x.store.grantsForEntitlement e := rfl

/-! ## Witnesses -/

example : run [1, 2, 3] (fun _ => true) Consumer.patient = [.record 1, .record 2, .record 3] := by decide
example : run [1, 2, 3] (fun n => n != 2) Consumer.patient = [.record 1, .record 3] := by decide
example : run ([] : List Nat) (fun _ => true) { cancelAfter := some 0, breakAfter := none } = [] := by decide
example : run [1, 2, 3] (fun _ => false) { cancelAfter := some 0, breakAfter := none } = [.error .cancelled] := by
  decide
/-- Cancel after the first record: the next scanned row yields the error. -/
example : run [1, 2, 3] (fun _ => true) { cancelAfter := some 1, breakAfter := none }
    = [.record 1, .error .cancelled] := by decide
/-- Cancel after the first record when it was the last row: clean end. -/
example : run [1] (fun _ => true) { cancelAfter := some 1, breakAfter := none } = [.record 1] := by decide
/-- Cancel after the first record with only non-matching rows left: still an error. -/
example : run [1, 2] (fun n => n == 1) { cancelAfter := some 1, breakAfter := none }
    = [.record 1, .error .cancelled] := by decide
example : run [1, 2, 3] (fun _ => true) { cancelAfter := none, breakAfter := some 2 } = [.record 1, .record 2] := by
  decide

end Stream
end C1z
