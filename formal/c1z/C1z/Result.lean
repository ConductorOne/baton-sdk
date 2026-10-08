/-!
# Result algebra

The outcomes a reader can observe, and what a consumer may conclude
from them. From `pkg/dotc1z/engine/pebble/paginate.go`,
`adapter_reader.go`, `adapter_streaming.go`, `lookup.go` and
`pkg/connectorstore/streaming.go`.

The point of this module is to give downstream consumers named
definitions to import, so that "complete snapshot" means exactly one
thing: a traversal that reached a page with no next token and no error.
A failed, cancelled, or abandoned traversal is not complete, and a
missing or deferred secondary index is indistinguishable from an empty
result on the index-backed readers (a documented non-guarantee, not a
theorem).

Also modeled: the bare-id resolution rule shared by entitlement and
grant lookup (`resolveEntitlementIdentityByExternalID`,
`resolveGrantIdentityByExternalID`): zero matches is not found, one is
returned, several is `ErrAmbiguousExternalID`.

Out of scope: the error classes' exact Go sentinels beyond their names,
gRPC status mapping, and the batched `ListGrantsForEntitlements` token.
-/

namespace C1z
namespace Result

/-- Distinguishable list-call failures. -/
inductive ListError where
  | noCurrentSync
  | invalidPageToken
  | ambiguousExternalId
  | decode
  | io
  | cancelled
  deriving Repr, DecidableEq

/-- One list call either returns a page or fails; a failed call returns
no partial page (`paginate.go` returns `nil, ""` on error). -/
inductive ListOutcome (α : Type) where
  | page (items : List α) (next : Option String)
  | failure (e : ListError)
  deriving Repr

/-- How a traversal ended. -/
inductive TraversalEnd where
  /-- The last page carried no next token. -/
  | exhausted
  /-- A call failed. -/
  | failed (e : ListError)
  /-- The consumer stopped early (stream `break`, or a token not followed). -/
  | abandoned
  deriving Repr, DecidableEq

/-- A traversal is complete only when it exhausted the query. -/
def complete : TraversalEnd → Bool
  | .exhausted => true
  | _ => false

theorem complete_iff (t : TraversalEnd) : complete t = true ↔ t = .exhausted := by
  cases t <;> simp [complete]

/-- A failed traversal is never complete, whatever already arrived. -/
theorem not_complete_failed (e : ListError) : complete (.failed e) = false := by
  rfl

/-- Early consumer termination does not establish exhaustion. -/
theorem not_complete_abandoned : complete .abandoned = false := by
  rfl

/-- A single call's view: records only on success; exhaustion is a
successful page with no token; an error is neither. -/
def ListOutcome.isExhausted {α : Type} : ListOutcome α → Bool
  | .page _ none => true
  | _ => false

theorem ListOutcome.isExhausted_failure {α : Type} (e : ListError) :
    (ListOutcome.failure e : ListOutcome α).isExhausted = false := by
  rfl

/-- A failure yields no items, so an error never looks like a result set. -/
def ListOutcome.items {α : Type} : ListOutcome α → List α
  | .page items _ => items
  | .failure _ => []

/-! ## Streaming -/

/-- A stream is a finite sequence of yields; the Pebble streams yield at
most one error and nothing after it (`adapter_streaming.go`). -/
inductive Yield (α : Type) where
  | record (a : α)
  | error (e : ListError)
  deriving Repr

/-- The terminal-error contract: an error, if any, is the last yield. -/
def ErrorTerminal {α : Type} (ys : List (Yield α)) : Prop :=
  ∀ i e, ys[i]? = some (.error e) → i + 1 = ys.length

/-- The consumer's verdict on a stream it read to the end. -/
def streamEnd {α : Type} : List (Yield α) → TraversalEnd
  | [] => .exhausted
  | [.error e] => .failed e
  | _ :: ys => streamEnd ys

theorem streamEnd_failed_of_error {α : Type} (ys : List (Yield α)) (e : ListError)
    (h : ErrorTerminal (ys ++ [.error e])) : streamEnd (ys ++ [.error e]) = .failed e := by
  induction ys with
  | nil => rfl
  | cons y ys ih =>
    have ht : ErrorTerminal (ys ++ [.error e]) := by
      intro i e' hi
      have := h (i + 1) e' (by simpa only [List.cons_append, List.getElem?_cons_succ] using hi)
      simpa only [List.cons_append, List.length_cons, Nat.add_right_cancel_iff] using this
    obtain ⟨z, zs, hz⟩ : ∃ z zs, ys ++ [Yield.error e] = z :: zs := by
      cases ys with
      | nil => exact ⟨_, _, rfl⟩
      | cons a as => exact ⟨_, _, rfl⟩
    have step : streamEnd (y :: z :: zs) = streamEnd (z :: zs) := by cases y <;> rfl
    rw [List.cons_append, hz, step, ← hz]
    exact ih ht

theorem streamEnd_exhausted_of_no_error {α : Type} (ys : List (Yield α))
    (h : ∀ y ∈ ys, ∀ e, y ≠ .error e) : streamEnd ys = .exhausted := by
  induction ys with
  | nil => rfl
  | cons y ys ih =>
    have ih' := ih (fun y' hy' => h y' (List.mem_cons_of_mem _ hy'))
    cases ys with
    | nil =>
      cases y with
      | record a => rfl
      | error e => exact absurd rfl (h _ List.mem_cons_self e)
    | cons z zs =>
      have step : streamEnd (y :: z :: zs) = streamEnd (z :: zs) := by cases y <;> rfl
      rw [step]
      exact ih'

/-! ## Bare-id resolution -/

inductive Resolution (ι : Type) where
  | notFound
  | found (id : ι)
  | ambiguous
  deriving Repr, DecidableEq

/-- The exactly-one rule for bare external ids (`lookup.go`). -/
def resolveBare {ι : Type} : List ι → Resolution ι
  | [] => .notFound
  | [i] => .found i
  | _ :: _ :: _ => .ambiguous

theorem resolveBare_found_iff {ι : Type} (ms : List ι) (i : ι) :
    resolveBare ms = .found i ↔ ms = [i] := by
  match ms with
  | [] => simp [resolveBare]
  | [j] => simp [resolveBare, eq_comm]
  | _ :: _ :: _ => simp [resolveBare]

theorem resolveBare_ambiguous_iff {ι : Type} (ms : List ι) :
    resolveBare ms = .ambiguous ↔ 2 ≤ ms.length := by
  match ms with
  | [] => simp [resolveBare]
  | [j] => simp [resolveBare]
  | _ :: _ :: _ => simp [resolveBare]

/-- A bare-id lookup never picks one of several matches arbitrarily. -/
theorem resolveBare_found_imp_unique {ι : Type} {ms : List ι} {i j : ι}
    (h : resolveBare ms = .found i) (hj : j ∈ ms) : j = i := by
  rw [resolveBare_found_iff] at h
  subst h
  simpa only [List.mem_singleton] using hj

/-! ## Witnesses -/

example : resolveBare ([] : List Nat) = .notFound := by decide
example : resolveBare [7] = .found 7 := by decide
example : resolveBare [7, 8] = .ambiguous := by decide
example : streamEnd ([.record 1, .record 2, .error .io] : List (Yield Nat)) = .failed .io := by rfl
example : streamEnd ([.record 1, .record 2] : List (Yield Nat)) = .exhausted := by rfl

end Result
end C1z
