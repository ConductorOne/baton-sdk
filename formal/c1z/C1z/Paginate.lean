import C1z.Order
import C1z.Codec

/-!
# Pagination over a sorted keyspace

Models `pkg/dotc1z/engine/pebble/paginate.go`: `clampPageSize`,
`decodePageToken` / `rangeAfter`, and `iteratePrimaryPageWithKey`.

The engine iterates a prefix range in key order, resumes strictly after
the token's key (`LowerBound = cursor || 0x00`), emits up to `limit`
rows, and mints a next token (the last emitted raw key) only when the
page is full AND another raw key follows. The token is base64 of the
raw key; base64 is a bijection and is not modeled, so the token here is
the key itself. The engine rejects a token whose key does not start
with the call's scan prefix (`ErrInvalidPageToken`, "cursor does not
belong to this keyspace").

Two refinements the engine has and this model keeps:

- `visible` filters rows the iterator reads but does not emit (rows
  rejected by a `keep` predicate, index entries whose primary row is
  missing, undecodable keys). The full-page lookahead counts raw keys,
  not visible ones, so a full page can be followed by an empty terminal
  page. `page_next_isSome_imp_nonempty` shows the converse never
  happens: a token implies a non-empty page.
- `clampPageSize` maps `0` and anything above `maxPageSize` to
  `defaultPageSize` (both 10000).

Out of scope: snapshot isolation across pages (each page opens a fresh
iterator; theorems assume a static keyspace), `ListGrantsForEntitlements`'
batched token, the `PaginateGrantsByEntitlementPrincipal` point lookup,
index-backed scans (modeled only through `visible`), and base64.
-/

namespace C1z
namespace Paginate

/-- `MaxPageSize` in paginate.go. -/
def maxPageSize : Nat := 10000

/-- `DefaultPageSize` in paginate.go. -/
def defaultPageSize : Nat := 10000

/-- `clampPageSize`: `0` and oversize requests become the default. -/
def clampPageSize (n : Nat) : Nat :=
  if n = 0 ∨ maxPageSize < n then defaultPageSize else n

theorem clampPageSize_pos (n : Nat) : 0 < clampPageSize n := by
  unfold clampPageSize maxPageSize defaultPageSize
  split <;> omega

theorem clampPageSize_le (n : Nat) : clampPageSize n ≤ maxPageSize := by
  unfold clampPageSize maxPageSize defaultPageSize
  split <;> omega

theorem clampPageSize_eq_self {n : Nat} (h₀ : 0 < n) (h₁ : n ≤ maxPageSize) : clampPageSize n = n := by
  unfold clampPageSize
  simp only [show ¬(n = 0 ∨ maxPageSize < n) by omega, ite_false]

/-- One page: the emitted keys and the next token, if any. -/
structure Page where
  items : List Bytes
  next : Option Bytes
  deriving Repr, DecidableEq

/-- Keys strictly after the cursor (`rangeAfter`: `LowerBound = cursor || 0x00`). -/
def afterCursor (ks : List Bytes) : Option Bytes → List Bytes
  | none => ks
  | some c => ks.filter (fun k => lexLt c k)

/-- Read one page from the raw keys `ks` of a scan range, in order.
`visible` says which raw rows are emitted. -/
def page (visible : Bytes → Bool) (ks : List Bytes) (cursor : Option Bytes) (limit : Nat) : Page :=
  let rest := afterCursor ks cursor
  let items := (rest.filter visible).take limit
  let hasMore : Bool :=
    items.length == limit && limit != 0 &&
      match items.getLast? with
      | some last => !(rest.filter (fun k => lexLt last k)).isEmpty
      | none => false
  { items := items
    next := if hasMore then items.getLast? else none }

/-- Token validation (`rangeAfter`): a cursor must lie under the scan prefix. -/
inductive TokenCheck where
  | ok
  | invalidPageToken
  deriving Repr, DecidableEq

def checkCursor (pfx : Bytes) : Option Bytes → TokenCheck
  | none => .ok
  | some c => if pfx <+: c then .ok else .invalidPageToken

/-- Drive pages to exhaustion. `fuel` bounds the recursion; `ks.length + 1`
always suffices (`traverse_complete`). -/
def traverse (visible : Bytes → Bool) (ks : List Bytes) (limit : Nat) :
    Nat → Option Bytes → List (List Bytes)
  | 0, _ => []
  | fuel + 1, cursor =>
    let p := page visible ks cursor limit
    match p.next with
    | none => [p.items]
    | some tok => p.items :: traverse visible ks limit fuel (some tok)

/-! ## Page-local laws -/

private theorem page_items_eq (visible : Bytes → Bool) (ks : List Bytes) (c : Option Bytes) (limit : Nat) :
    (page visible ks c limit).items = ((afterCursor ks c).filter visible).take limit := rfl

private theorem page_next_eq_some_iff (visible : Bytes → Bool) (ks : List Bytes) (c : Option Bytes)
    (limit : Nat) (tok : Bytes) :
    (page visible ks c limit).next = some tok ↔
      (((afterCursor ks c).filter visible).take limit).length = limit ∧ 0 < limit ∧
        (((afterCursor ks c).filter visible).take limit).getLast? = some tok ∧
        ((afterCursor ks c).filter (fun k => lexLt tok k)).isEmpty = false := by
  simp only [page]
  generalize ((afterCursor ks c).filter visible).take limit = items
  cases hl : items.getLast? with
  | none => simp
  | some last =>
    by_cases hlen : items.length = limit
    case neg => simp [hlen]
    case pos =>
      by_cases h0 : limit = 0
      case pos => simp [h0]
      case neg =>
        have hlim : 0 < limit := by omega
        constructor
        · intro h
          refine ⟨hlen, hlim, ?_⟩
          cases hm : (List.filter (fun k => lexLt last k) (afterCursor ks c)).isEmpty
          case true => simp [hlen, hm] at h
          case false =>
            have e : last = tok := by simpa [hlen, h0, hm] using h
            subst e
            exact ⟨rfl, hm⟩
        · rintro ⟨-, -, h1, h2⟩
          cases h1
          simp [hlen, h0, h2]

/-- Every emitted key is a visible raw key strictly after the cursor. -/
theorem page_items_mem (visible : Bytes → Bool) (ks : List Bytes) (c : Option Bytes) (limit : Nat) :
    ∀ k ∈ (page visible ks c limit).items, k ∈ ks ∧ visible k = true ∧
      (∀ cur, c = some cur → lexLt cur k = true) := by
  intro k hk
  rw [page_items_eq] at hk
  obtain ⟨hR, hv⟩ := List.mem_filter.1 (List.mem_of_mem_take hk)
  cases c with
  | none => exact ⟨hR, hv, fun _ h => nomatch h⟩
  | some cur =>
    obtain ⟨hks, hlt⟩ := List.mem_filter.1 hR
    exact ⟨hks, hv, fun _ h => by cases h; exact hlt⟩

theorem page_items_length_le (visible : Bytes → Bool) (ks : List Bytes) (c : Option Bytes) (limit : Nat) :
    (page visible ks c limit).items.length ≤ limit := by
  rw [page_items_eq, List.length_take]
  omega

/-- The token, when present, is the last emitted key. -/
theorem page_next_eq_getLast (visible : Bytes → Bool) (ks : List Bytes) (c : Option Bytes) (limit : Nat)
    {tok : Bytes} (h : (page visible ks c limit).next = some tok) :
    (page visible ks c limit).items.getLast? = some tok := by
  rw [page_items_eq]
  exact ((page_next_eq_some_iff visible ks c limit tok).1 h).2.2.1

/-- A token is only minted on a full page. -/
theorem page_next_isSome_imp_full (visible : Bytes → Bool) (ks : List Bytes) (c : Option Bytes) (limit : Nat)
    (h : (page visible ks c limit).next.isSome) : (page visible ks c limit).items.length = limit := by
  obtain ⟨tok, htok⟩ := Option.isSome_iff_exists.1 h
  rw [page_items_eq]
  exact ((page_next_eq_some_iff visible ks c limit tok).1 htok).1

/-- An empty page never carries a token. -/
theorem page_next_isSome_imp_nonempty (visible : Bytes → Bool) (ks : List Bytes) (c : Option Bytes)
    (limit : Nat) (h : (page visible ks c limit).next.isSome) : (page visible ks c limit).items ≠ [] := by
  obtain ⟨tok, htok⟩ := Option.isSome_iff_exists.1 h
  have hlast := page_next_eq_getLast visible ks c limit htok
  intro hnil
  rw [hnil] at hlast
  exact nomatch hlast

private theorem filter_lt_getLast_take {L : List Bytes} (hL : L.Pairwise (fun a b => lexLt a b = true))
    (n : Nat) {last : Bytes} (h : (L.take n).getLast? = some last) :
    L.filter (fun k => lexLt last k) = L.drop n := by
  have hsplit := List.take_append_drop n L
  have hL' : (L.take n ++ L.drop n).Pairwise (fun a b => lexLt a b = true) := by rw [hsplit]; exact hL
  obtain ⟨-, -, hcross⟩ := List.pairwise_append.1 hL'
  have hmem := List.mem_of_getLast? h
  obtain ⟨ys, hys⟩ := List.getLast?_eq_some_iff.1 h
  have hT : (L.take n).Pairwise (fun a b => lexLt a b = true) := hL.sublist (List.take_sublist n L)
  rw [hys] at hT
  obtain ⟨-, -, hinit⟩ := List.pairwise_append.1 hT
  conv => lhs; rw [← hsplit]
  rw [List.filter_append, hys, List.filter_append]
  have h1 : ys.filter (fun k => lexLt last k) = [] := by
    rw [List.filter_eq_nil_iff]
    intro a ha
    rw [lexLt.asymm (hinit a ha last (List.mem_singleton_self last))]
    exact Bool.false_ne_true
  have h2 : [last].filter (fun k => lexLt last k) = [] := by
    simp [lexLt.irrefl]
  have h3 : (L.drop n).filter (fun k => lexLt last k) = L.drop n := by
    rw [List.filter_eq_self]
    intro b hb
    exact hcross last hmem b hb
  rw [h1, h2, h3, List.nil_append, List.nil_append]

private theorem filter_comm (R : List Bytes) (p q : Bytes → Bool) :
    (R.filter p).filter q = (R.filter q).filter p := by
  rw [List.filter_filter, List.filter_filter]
  exact List.filter_congr (fun x _ => Bool.and_comm (q x) (p x))

private theorem afterCursor_some_of_mem {ks : List Bytes} {cur : Option Bytes} {tok : Bytes}
    (h : tok ∈ afterCursor ks cur) :
    afterCursor ks (some tok) = (afterCursor ks cur).filter (fun k => lexLt tok k) := by
  cases cur with
  | none => rfl
  | some c =>
    have hct : lexLt c tok = true := (List.mem_filter.1 h).2
    show ks.filter (fun k => lexLt tok k) = (ks.filter (fun k => lexLt c k)).filter (fun k => lexLt tok k)
    rw [List.filter_filter]
    apply List.filter_congr
    intro x _
    cases htx : lexLt tok x with
    | false => rfl
    | true => rw [lexLt.trans hct htx]; rfl

private theorem afterCursor_sorted {ks : List Bytes} (hsorted : ks.Pairwise (fun a b => lexLt a b = true))
    (c : Option Bytes) : (afterCursor ks c).Pairwise (fun a b => lexLt a b = true) := by
  cases c with
  | none => exact hsorted
  | some cur => exact hsorted.filter _

/-- The lookahead filter, restricted to visible keys, is what the page did not emit. -/
private theorem lookahead_filter_visible {visible : Bytes → Bool} {ks : List Bytes} {c : Option Bytes}
    {limit : Nat} {last : Bytes} (hsorted : ks.Pairwise (fun a b => lexLt a b = true))
    (hl : (((afterCursor ks c).filter visible).take limit).getLast? = some last) :
    ((afterCursor ks c).filter (fun k => lexLt last k)).filter visible =
      ((afterCursor ks c).filter visible).drop limit := by
  rw [filter_comm]
  exact filter_lt_getLast_take ((afterCursor_sorted hsorted c).filter visible) limit hl

/-- A page with no token has emitted every remaining visible key. -/
theorem page_next_none_imp_exhausted (visible : Bytes → Bool) (ks : List Bytes) (c : Option Bytes)
    (limit : Nat) (hsorted : ks.Pairwise (fun a b => lexLt a b = true)) (hlimit : 0 < limit)
    (h : (page visible ks c limit).next = none) :
    (page visible ks c limit).items = (afterCursor ks c).filter visible := by
  rw [page_items_eq]
  by_cases hfull : (((afterCursor ks c).filter visible).take limit).length = limit
  case neg =>
    rw [List.length_take] at hfull
    apply List.take_of_length_le
    omega
  case pos =>
    cases hl : (((afterCursor ks c).filter visible).take limit).getLast? with
    | none =>
      rw [List.getLast?_eq_none_iff] at hl
      rw [hl, List.length_nil] at hfull
      omega
    | some last =>
      have hne : ((afterCursor ks c).filter (fun k => lexLt last k)).isEmpty = true := by
        cases he : ((afterCursor ks c).filter (fun k => lexLt last k)).isEmpty with
        | true => rfl
        | false =>
          have hs := (page_next_eq_some_iff visible ks c limit last).2 ⟨hfull, hlimit, hl, he⟩
          rw [h] at hs
          exact nomatch hs
      have hd : ((afterCursor ks c).filter visible).drop limit = [] := by
        rw [← lookahead_filter_visible hsorted hl, List.isEmpty_iff.1 hne, List.filter_nil]
      conv => rhs; rw [← List.take_append_drop limit ((afterCursor ks c).filter visible), hd, List.append_nil]

/-- A foreign cursor is rejected. -/
theorem checkCursor_invalid_of_not_prefix {pfx c : Bytes} (h : ¬ pfx <+: c) :
    checkCursor pfx (some c) = .invalidPageToken := by
  simp only [checkCursor, h, ite_false]

/-- A cursor minted by a page of this scan is accepted by the same scan. -/
theorem checkCursor_ok_of_page (pfx : Bytes) (visible : Bytes → Bool) (ks : List Bytes)
    (hks : ∀ k ∈ ks, pfx <+: k) (c : Option Bytes) (limit : Nat) {tok : Bytes}
    (h : (page visible ks c limit).next = some tok) : checkCursor pfx (some tok) = .ok := by
  have hmem : tok ∈ (page visible ks c limit).items := List.mem_of_getLast? (page_next_eq_getLast visible ks c limit h)
  have hks' := (page_items_mem visible ks c limit tok hmem).1
  simp only [checkCursor, hks tok hks', ite_true]

/-! ## Traversal laws -/

/-- Each minted token shrinks the remaining visible keys by one full page. -/
private theorem afterCursor_next_filter_visible {visible : Bytes → Bool} {ks : List Bytes} {c : Option Bytes}
    {limit : Nat} {tok : Bytes} (hsorted : ks.Pairwise (fun a b => lexLt a b = true))
    (h : (page visible ks c limit).next = some tok) :
    (afterCursor ks (some tok)).filter visible = ((afterCursor ks c).filter visible).drop limit := by
  obtain ⟨-, -, hl, -⟩ := (page_next_eq_some_iff visible ks c limit tok).1 h
  have hmem : tok ∈ afterCursor ks c :=
    (List.mem_filter.1 (List.mem_of_mem_take (List.mem_of_getLast? hl))).1
  rw [afterCursor_some_of_mem hmem]
  exact lookahead_filter_visible hsorted hl

private theorem traverse_flatten_aux (visible : Bytes → Bool) (ks : List Bytes) (limit : Nat)
    (hsorted : ks.Pairwise (fun a b => lexLt a b = true)) (hlimit : 0 < limit) :
    ∀ (fuel : Nat) (cursor : Option Bytes), ((afterCursor ks cursor).filter visible).length < fuel →
      (traverse visible ks limit fuel cursor).flatten = (afterCursor ks cursor).filter visible := by
  intro fuel
  induction fuel with
  | zero => intro _ h; omega
  | succ n ih =>
    intro cursor hlt
    cases hn : (page visible ks cursor limit).next with
    | none =>
      simp only [traverse, hn, List.flatten_cons, List.flatten_nil, List.append_nil]
      exact page_next_none_imp_exhausted visible ks cursor limit hsorted hlimit hn
    | some tok =>
      obtain ⟨hfull, -, -, -⟩ := (page_next_eq_some_iff visible ks cursor limit tok).1 hn
      rw [List.length_take] at hfull
      have hdrop := afterCursor_next_filter_visible hsorted hn
      simp only [traverse, hn, List.flatten_cons]
      rw [ih (some tok) (by rw [hdrop, List.length_drop]; omega), hdrop, page_items_eq, List.take_append_drop]

/-- A complete traversal with enough fuel returns exactly the visible
keys, in key order, once each, for any positive page size. -/
theorem traverse_complete (visible : Bytes → Bool) (ks : List Bytes) (limit : Nat)
    (hsorted : ks.Pairwise (fun a b => lexLt a b = true)) (hlimit : 0 < limit) :
    (traverse visible ks limit (ks.length + 1) none).flatten = ks.filter visible := by
  have hle := List.length_filter_le visible ks
  exact traverse_flatten_aux visible ks limit hsorted hlimit (ks.length + 1) none (by show (ks.filter visible).length < ks.length + 1; omega)

/-- Page size changes the partition, not the collection. -/
theorem traverse_flatten_eq_of_pos (visible : Bytes → Bool) (ks : List Bytes) {l m : Nat}
    (hsorted : ks.Pairwise (fun a b => lexLt a b = true)) (hl : 0 < l) (hm : 0 < m) :
    (traverse visible ks l (ks.length + 1) none).flatten =
      (traverse visible ks m (ks.length + 1) none).flatten := by
  rw [traverse_complete visible ks l hsorted hl, traverse_complete visible ks m hsorted hm]

/-- Traversal terminates: the last page has no token. -/
theorem traverse_terminates (visible : Bytes → Bool) (ks : List Bytes) (limit : Nat)
    (_hsorted : ks.Pairwise (fun a b => lexLt a b = true)) (_hlimit : 0 < limit) :
    ∃ pages, traverse visible ks limit (ks.length + 1) none = pages ∧ pages ≠ [] := by
  refine ⟨_, rfl, ?_⟩
  cases hn : (page visible ks none limit).next with
  | none => simp only [traverse, hn]; exact List.cons_ne_nil _ _
  | some tok => simp only [traverse, hn]; exact List.cons_ne_nil _ _

/-! ## Witnesses -/

/-- Three keys, page size 2: a full page with a token, then a terminal page. -/
example :
    traverse (fun _ => true) [[1], [2], [3]] 2 4 none = [[[1], [2]], [[3]]] := by decide

/-- A full page followed by an empty terminal page when the trailing raw
rows are not visible (dangling index entries, `keep` rejects). -/
example :
    traverse (fun k => k != [3]) [[1], [2], [3]] 2 4 none = [[[1], [2]], []] := by decide

/-- Exactly `limit` keys: no token, no trailing empty page. -/
example :
    traverse (fun _ => true) [[1], [2]] 2 3 none = [[[1], [2]]] := by decide

/-- A cursor that does not sit under the scan prefix is rejected. -/
example : checkCursor [3, 4, 0] (some [3, 2, 0, 9]) = .invalidPageToken := by decide

end Paginate
end C1z
