import C1z.Basic

/-!
# Tuple key codec

Models `pkg/dotc1z/engine/pebble/codec/tuple.go` (`appendEscaped`,
`AppendTupleSeparator`, `AppendTupleStrings`) and the key header
convention documented at the top of `pkg/dotc1z/engine/pebble/keys.go`:

    [ fixed header ][ 0x00 ][ tuple-encoded tail ]

Every element is escaped so it contains no bare `0x00`, and elements
are joined by a single `0x00`. The theorems here establish that the
encoding is injective for a fixed element count, and that a by-value
scan prefix (`header | 0x00 | elems | 0x00`) matches exactly the keys
whose leading elements equal those values.

Out of scope: integer tuple components (`AppendTupleInt32` and
friends), `KeyUpperBound`, decoding (`DecodeTupleStringTo`), and the
index-key headers (`versionV3 | typeIndex | idxXxx`). Only the primary
record keyspaces are modeled.

Bytes are modeled as `Nat` (`C1z.Basic`).
-/

namespace C1z
namespace Codec

/-- The element separator (`tupleSeparator` in tuple.go). -/
def separator : Byte := 0

/-- The escape byte (`tupleEscape` in tuple.go). -/
def escapeMark : Byte := 1

/-- Escape one byte: `0x00 ↦ 0x01 0x01`, `0x01 ↦ 0x01 0x02`, else itself. -/
def escapeByte : Byte → Bytes
  | 0 => [1, 1]
  | 1 => [1, 2]
  | b => [b]

/-- Escape a byte string (`appendEscaped` in tuple.go). -/
def escape (s : Bytes) : Bytes := s.flatMap escapeByte

/-- Encode a tuple tail: escaped elements joined by `separator`
(`AppendTupleStrings` in tuple.go). -/
def encodeTuple : List Bytes → Bytes
  | [] => []
  | [e] => escape e
  | e :: es => escape e ++ separator :: encodeTuple es

/-- The by-value scan prefix for leading elements `es`: the encoded
elements followed by one trailing separator. The trailing separator is
what keeps `"ent"` from matching `"entitlement-1"` (keys.go, "Key layout
convention"). -/
def scanPrefix (es : List Bytes) : Bytes := encodeTuple es ++ [separator]

/-- Primary record keyspaces. Discriminator bytes come from
`internal/rawdb/keyspace.go`. -/
inductive Kind where
  | resourceType
  | resource
  | entitlement
  | grant
  | syncRun
  deriving Repr, DecidableEq

/-- `rawdb.VersionV3`. -/
def versionV3 : Byte := 0x03

/-- The type-discriminator byte of a kind (`rawdb.TypeXxx`). -/
def Kind.discriminator : Kind → Byte
  | .resourceType => 0x01
  | .resource => 0x02
  | .entitlement => 0x03
  | .grant => 0x04
  | .syncRun => 0x06

/-- The two-byte primary-key header `versionV3 | typeXxx`. -/
def header (k : Kind) : Bytes := [versionV3, k.discriminator]

/-- A full primary key: header, separator, tuple-encoded tail
(`encodeResourceKey`, `appendEntitlementIdentityKey`, and friends in
keys.go). -/
def encodeKey (k : Kind) (tail : List Bytes) : Bytes :=
  header k ++ separator :: encodeTuple tail

/-- The by-value range-scan prefix for a kind and leading elements. -/
def encodeScanPrefix (k : Kind) (es : List Bytes) : Bytes :=
  header k ++ separator :: scanPrefix es

/-! ## Escaping -/

theorem escapeByte_ne_nil (b : Byte) : escapeByte b ≠ [] := by
  rcases b with _ | _ | b <;> simp [escapeByte]

/-- No escaped byte is the separator. -/
theorem separator_not_mem_escapeByte (b : Byte) : separator ∉ escapeByte b := by
  rcases b with _ | _ | b <;> simp [escapeByte, separator]

/-- No byte of an escaped string is the separator. -/
theorem separator_not_mem_escape (s : Bytes) : separator ∉ escape s := by
  simp only [escape, List.mem_flatMap, not_exists, not_and]
  intro b _
  exact separator_not_mem_escapeByte b

theorem escape_append (s t : Bytes) : escape (s ++ t) = escape s ++ escape t := by
  simp only [escape, List.flatMap_append]

private theorem escape_cons (b : Byte) (s : Bytes) : escape (b :: s) = escapeByte b ++ escape s := by
  simp only [escape, List.flatMap_cons]

/-- Escaping is injective. -/
theorem escape_injective {s t : Bytes} (h : escape s = escape t) : s = t := by
  induction s generalizing t with
  | nil =>
    cases t with
    | nil => rfl
    | cons b t =>
      rw [escape_cons] at h
      exact absurd (List.append_eq_nil_iff.mp h.symm).1 (escapeByte_ne_nil b)
  | cons a s ih =>
    cases t with
    | nil =>
      rw [escape_cons] at h
      exact absurd (List.append_eq_nil_iff.mp h).1 (escapeByte_ne_nil a)
    | cons b t =>
      rw [escape_cons, escape_cons] at h
      rcases a with _ | _ | a <;> rcases b with _ | _ | b <;>
        simp only [escapeByte, List.cons_append, List.nil_append, List.cons.injEq] at h
      case zero.zero => exact congrArg _ (ih h.2.2)
      case succ.zero.succ.zero => exact congrArg _ (ih h.2.2)
      case succ.succ.succ.succ =>
        obtain ⟨hab, hst⟩ := h
        rw [ih hst, Nat.succ.inj (Nat.succ.inj hab)]
      all_goals simp at h

/-! ## Tuples -/

/-- The first `0` marks the boundary when neither left part contains `0`. -/
private theorem split_at_zero {a b x y : Bytes} (ha : 0 ∉ a) (hb : 0 ∉ b)
    (h : a ++ 0 :: x = b ++ 0 :: y) : a = b ∧ x = y := by
  induction a generalizing b with
  | nil =>
    cases b with
    | nil => exact ⟨rfl, (List.cons.inj h).2⟩
    | cons c b =>
      simp only [List.nil_append, List.cons_append, List.cons.injEq] at h
      exact absurd (h.1 ▸ List.mem_cons_self) hb
  | cons c a ih =>
    cases b with
    | nil =>
      simp only [List.nil_append, List.cons_append, List.cons.injEq] at h
      exact absurd (h.1 ▸ List.mem_cons_self) ha
    | cons d b =>
      simp only [List.cons_append, List.cons.injEq] at h
      obtain ⟨hcd, hrest⟩ := h
      obtain ⟨hab, hxy⟩ := ih (fun hm => ha (List.mem_cons_of_mem c hm))
        (fun hm => hb (List.mem_cons_of_mem d hm)) hrest
      exact ⟨hcd ▸ hab ▸ rfl, hxy⟩

private theorem encodeTuple_cons_cons (e f : Bytes) (fs : List Bytes) :
    encodeTuple (e :: f :: fs) = escape e ++ 0 :: encodeTuple (f :: fs) := by
  simp only [encodeTuple, separator]

private theorem scanPrefix_singleton (e : Bytes) : scanPrefix [e] = escape e ++ 0 :: [] := by
  simp only [scanPrefix, encodeTuple, separator]

private theorem scanPrefix_cons_cons (e f : Bytes) (es : List Bytes) :
    scanPrefix (e :: f :: es) = escape e ++ 0 :: scanPrefix (f :: es) := by
  simp only [scanPrefix, encodeTuple_cons_cons, List.append_assoc, List.cons_append]

private theorem split_prefix_at_zero {a b x y : Bytes} (ha : 0 ∉ a) (hb : 0 ∉ b)
    (h : a ++ 0 :: x <+: b ++ 0 :: y) : a = b ∧ x <+: y := by
  obtain ⟨t, ht⟩ := h
  simp only [List.append_assoc, List.cons_append] at ht
  obtain ⟨hab, hxy⟩ := split_at_zero ha hb ht
  exact ⟨hab, t, hxy⟩

private theorem not_prefix_of_zero {a x b : Bytes} (hb : 0 ∉ b) (h : a ++ 0 :: x <+: b) : False := by
  obtain ⟨t, ht⟩ := h
  apply hb
  rw [← ht]
  simp

/-- Two tuples with the same element count and equal encodings are equal. -/
theorem encodeTuple_injective {es fs : List Bytes} (hlen : es.length = fs.length)
    (h : encodeTuple es = encodeTuple fs) : es = fs := by
  induction es generalizing fs with
  | nil =>
    cases fs with
    | nil => rfl
    | cons _ _ => simp at hlen
  | cons e es ih =>
    cases fs with
    | nil => simp at hlen
    | cons f fs =>
      simp only [List.length_cons, Nat.add_right_cancel_iff] at hlen
      cases es with
      | nil =>
        cases fs with
        | nil => exact congrArg (· :: []) (escape_injective h)
        | cons _ _ => simp at hlen
      | cons e₂ es =>
        cases fs with
        | nil => simp at hlen
        | cons f₂ fs =>
          obtain ⟨hef, hrest⟩ := split_at_zero (separator_not_mem_escape e)
            (separator_not_mem_escape f) h
          rw [escape_injective hef, ih hlen hrest]

/-- A tuple that properly extends `es` is encoded under `scanPrefix es`. -/
theorem scanPrefix_isPrefix_encodeTuple {es fs : List Bytes} (hne : es ≠ []) (hpre : es <+: fs)
    (hlt : es.length < fs.length) : scanPrefix es <+: encodeTuple fs := by
  induction es generalizing fs with
  | nil => exact absurd rfl hne
  | cons e es ih =>
    cases fs with
    | nil => simp at hlt
    | cons f fs =>
      obtain ⟨hef, hpre'⟩ := List.cons_prefix_cons.mp hpre
      subst hef
      simp only [List.length_cons, Nat.add_lt_add_iff_right] at hlt
      cases fs with
      | nil => simp at hlt
      | cons f₂ fs =>
        rw [encodeTuple_cons_cons]
        cases es with
        | nil =>
          rw [scanPrefix_singleton]
          exact (List.prefix_append_right_inj _).mpr (List.cons_prefix_cons.mpr ⟨rfl, List.nil_prefix⟩)
        | cons e₂ es =>
          rw [scanPrefix_cons_cons]
          exact (List.prefix_append_right_inj _).mpr
            (List.cons_prefix_cons.mpr ⟨rfl, ih (List.cons_ne_nil _ _) hpre' hlt⟩)

/-- Only tuples that extend `es` are encoded under `scanPrefix es`: the
trailing separator rules out an element that merely begins with the
scanned value. -/
theorem isPrefix_of_scanPrefix_isPrefix {es fs : List Bytes}
    (h : scanPrefix es <+: encodeTuple fs) : es <+: fs := by
  induction es generalizing fs with
  | nil => exact List.nil_prefix
  | cons e es ih =>
    cases fs with
    | nil =>
      obtain ⟨t, ht⟩ := h
      simp [scanPrefix, encodeTuple] at ht
    | cons f fs =>
      cases es with
      | nil =>
        cases fs with
        | nil =>
          rw [scanPrefix_singleton] at h
          exact (not_prefix_of_zero (separator_not_mem_escape f) h).elim
        | cons f₂ fs =>
          rw [scanPrefix_singleton, encodeTuple_cons_cons] at h
          obtain ⟨hef, _⟩ := split_prefix_at_zero (separator_not_mem_escape e)
            (separator_not_mem_escape f) h
          rw [escape_injective hef]
          exact List.cons_prefix_cons.mpr ⟨rfl, List.nil_prefix⟩
      | cons e₂ es =>
        cases fs with
        | nil =>
          rw [scanPrefix_cons_cons] at h
          exact (not_prefix_of_zero (separator_not_mem_escape f) h).elim
        | cons f₂ fs =>
          rw [scanPrefix_cons_cons, encodeTuple_cons_cons] at h
          obtain ⟨hef, hrest⟩ := split_prefix_at_zero (separator_not_mem_escape e)
            (separator_not_mem_escape f) h
          rw [escape_injective hef]
          exact List.cons_prefix_cons.mpr ⟨rfl, ih hrest⟩

/-! ## Keys -/

/-- Distinct kinds have distinct headers. -/
theorem header_injective {j k : Kind} (h : header j = header k) : j = k := by
  cases j <;> cases k <;> first | rfl | exact absurd h (by decide)

/-- Equal primary keys of equal arity come from the same kind and tail. -/
theorem encodeKey_injective {j k : Kind} {es fs : List Bytes} (hlen : es.length = fs.length)
    (h : encodeKey j es = encodeKey k fs) : j = k ∧ es = fs := by
  obtain ⟨hjk, htail⟩ := List.append_inj h rfl
  exact ⟨header_injective hjk, encodeTuple_injective hlen (List.cons.inj htail).2⟩

/-- A key lies under a by-value scan prefix exactly when it is of that
kind and its tail properly extends the scanned elements. -/
theorem encodeScanPrefix_isPrefix_iff {k j : Kind} {es fs : List Bytes} (hne : es ≠ []) :
    encodeScanPrefix k es <+: encodeKey j fs ↔ k = j ∧ es <+: fs ∧ es.length < fs.length := by
  constructor
  · rintro ⟨t, ht⟩
    simp only [encodeScanPrefix, encodeKey, List.append_assoc, List.cons_append] at ht
    obtain ⟨hkj, htail⟩ := List.append_inj ht rfl
    have hsp : scanPrefix es <+: encodeTuple fs := ⟨t, (List.cons.inj htail).2⟩
    have hpre := isPrefix_of_scanPrefix_isPrefix hsp
    refine ⟨header_injective hkj, hpre, ?_⟩
    rcases Nat.lt_or_eq_of_le hpre.length_le with hlt | heq
    · exact hlt
    · have hlen := hsp.length_le
      rw [← hpre.eq_of_length heq] at hlen
      simp only [scanPrefix, List.length_append, List.length_singleton] at hlen
      omega
  · rintro ⟨rfl, hpre, hlt⟩
    exact (List.prefix_append_right_inj _).mpr
      (List.cons_prefix_cons.mpr ⟨rfl, scanPrefix_isPrefix_encodeTuple hne hpre hlt⟩)

/-! ## Witnesses

Concrete instances showing the definitions compute as the Go codec does
(`codec_test.go` grid cases). These are also what the oracle emits. -/

example : escape [0x61, 0, 0x62] = [0x61, 1, 1, 0x62] := by decide
example : escape [1] = [1, 2] := by decide
example : encodeTuple [[0x61], [0x62]] = [0x61, 0, 0x62] := by decide
example : encodeKey .resource [[0x75], [0x31]] = [3, 2, 0, 0x75, 0, 0x31] := by decide
/-- `"us\0"` is not a prefix of the key for `("user", "1")`. -/
example : ¬ (encodeScanPrefix .resource [[0x75, 0x73]] <+: encodeKey .resource [[0x75, 0x73, 0x65, 0x72], [0x31]]) := by
  decide

end Codec
end C1z
