import C1z.Basic

/-!
# Bytewise lexicographic order

Pebble orders keys by `bytes.Compare`, which is shortlex-free plain
lexicographic order on bytes: a proper prefix sorts first. This module
defines that order on `Bytes` and proves it is a strict total order,
which `C1z.Store` and `C1z.Paginate` need to reason about sorted
enumeration and resume-after-cursor.

Out of scope: `codec.KeyUpperBound` and the all-`0xff` edge case; scans
are modeled by prefix filtering, not by an upper bound key.
-/

namespace C1z

/-- Bytewise lexicographic `<` (`bytes.Compare(a, b) < 0`). -/
def lexLt : Bytes → Bytes → Bool
  | [], [] => false
  | [], _ :: _ => true
  | _ :: _, [] => false
  | a :: as, b :: bs => a < b || (a == b && lexLt as bs)

namespace lexLt

@[simp] theorem nil_nil : lexLt [] [] = false := rfl

@[simp] theorem nil_cons (b : Byte) (bs : Bytes) : lexLt [] (b :: bs) = true := rfl

@[simp] theorem cons_nil (a : Byte) (as : Bytes) : lexLt (a :: as) [] = false := rfl

theorem cons_cons {a b : Byte} {as bs : Bytes} :
    lexLt (a :: as) (b :: bs) = true ↔ a < b ∨ (a = b ∧ lexLt as bs = true) := by
  simp only [lexLt, Bool.or_eq_true, decide_eq_true_eq, Bool.and_eq_true, beq_iff_eq]

theorem irrefl (a : Bytes) : lexLt a a = false := by
  induction a with
  | nil => rfl
  | cons x xs ih =>
    cases h : lexLt (x :: xs) (x :: xs) with
    | false => rfl
    | true =>
      rcases cons_cons.mp h with hlt | ⟨_, hrec⟩
      · exact absurd hlt (Nat.lt_irrefl x)
      · rw [ih] at hrec
        exact absurd hrec Bool.false_ne_true

theorem trans {a b c : Bytes} (hab : lexLt a b = true) (hbc : lexLt b c = true) :
    lexLt a c = true := by
  induction a generalizing b c with
  | nil =>
    cases c with
    | nil =>
      cases b with
      | nil => exact absurd hbc Bool.false_ne_true
      | cons y ys => exact absurd hbc Bool.false_ne_true
    | cons z zs => rfl
  | cons x xs ih =>
    cases b with
    | nil => exact absurd hab Bool.false_ne_true
    | cons y ys =>
      cases c with
      | nil => exact absurd hbc Bool.false_ne_true
      | cons z zs =>
        apply cons_cons.mpr
        rcases cons_cons.mp hab with h₁ | ⟨h₁, h₁'⟩ <;>
          rcases cons_cons.mp hbc with h₂ | ⟨h₂, h₂'⟩
        · exact Or.inl (Nat.lt_trans h₁ h₂)
        · exact Or.inl (h₂ ▸ h₁)
        · exact Or.inl (h₁ ▸ h₂)
        · exact Or.inr ⟨h₁.trans h₂, ih h₁' h₂'⟩

theorem asymm {a b : Bytes} (hab : lexLt a b = true) : lexLt b a = false := by
  cases h : lexLt b a with
  | false => rfl
  | true =>
    have := trans hab h
    rw [irrefl] at this
    exact absurd this Bool.false_ne_true

/-- Trichotomy as a disjunction over all pairs. -/
theorem lt_or_eq_or_gt (a b : Bytes) : lexLt a b = true ∨ a = b ∨ lexLt b a = true := by
  induction a generalizing b with
  | nil =>
    cases b with
    | nil => exact Or.inr (Or.inl rfl)
    | cons y ys => exact Or.inl rfl
  | cons x xs ih =>
    cases b with
    | nil => exact Or.inr (Or.inr rfl)
    | cons y ys =>
      rcases Nat.lt_trichotomy x y with hlt | heq | hgt
      · exact Or.inl (cons_cons.mpr (Or.inl hlt))
      · subst heq
        rcases ih ys with h | h | h
        · exact Or.inl (cons_cons.mpr (Or.inr ⟨rfl, h⟩))
        · exact Or.inr (Or.inl (h ▸ rfl))
        · exact Or.inr (Or.inr (cons_cons.mpr (Or.inr ⟨rfl, h⟩)))
      · exact Or.inr (Or.inr (cons_cons.mpr (Or.inl hgt)))

/-- Trichotomy: distinct byte strings are ordered one way or the other. -/
theorem total {a b : Bytes} (hne : a ≠ b) : lexLt a b = true ∨ lexLt b a = true := by
  rcases lt_or_eq_or_gt a b with h | h | h
  · exact Or.inl h
  · exact absurd h hne
  · exact Or.inr h

theorem not_lt_iff (a b : Bytes) : lexLt a b = false ↔ a = b ∨ lexLt b a = true := by
  constructor
  · intro h
    rcases lt_or_eq_or_gt a b with h' | h' | h'
    · rw [h] at h'
      exact absurd h' Bool.false_ne_true
    · exact Or.inl h'
    · exact Or.inr h'
  · rintro (h | h)
    · subst h
      exact irrefl a
    · exact asymm h

theorem eq_of_not_lt_of_not_gt {a b : Bytes} (h₁ : lexLt a b = false) (h₂ : lexLt b a = false) :
    a = b := by
  rcases (not_lt_iff a b).mp h₁ with h | h
  · exact h
  · rw [h₂] at h
    exact absurd h Bool.false_ne_true

/-- A proper prefix sorts strictly first. -/
theorem lt_of_prefix_of_lt_length {a b : Bytes} (h : a <+: b) (hlen : a.length < b.length) :
    lexLt a b = true := by
  obtain ⟨t, rfl⟩ := h
  induction a with
  | nil =>
    cases t with
    | nil => exact absurd hlen (Nat.lt_irrefl _)
    | cons y ys => rfl
  | cons x xs ih =>
    apply cons_cons.mpr
    refine Or.inr ⟨rfl, ih ?_⟩
    simp only [List.cons_append, List.length_cons] at hlen
    omega

example : lexLt [1, 2] [1, 2, 0] = true := by decide
example : lexLt [1, 2] [1, 2] = false := by decide
example : lexLt [2] [1, 9, 9] = false := by decide

end lexLt
end C1z
