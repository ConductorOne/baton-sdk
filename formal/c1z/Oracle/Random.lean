import Oracle.Cases

/-!
# Seeded random inputs for `c1z-oracle --random N --seed S`

`Oracle.Random.families n seed` builds `n` inputs per family from a
SplitMix64 stream seeded with `seed`, threaded through the families in
schema order, so the output is a function of `(n, seed)` alone. Expected
values come from `Oracle.render`, the same model calls the fixed corpus
uses. Case names are `random/<family>/<i>`, `i` from 0.

Distributions:

- `keys`: kind uniform over the four kinds. Each component is 0..6
  bytes from `genByte`: `0x00`, `0x01`, `0x3a` each 20%, ASCII letters
  `a b g u` 30%, a byte in `0x80..0xff` 10%. Entitlement and grant `ext`
  starts with `entPrefix rt rid` half the time. Inputs failing
  `WellFormed` are redrawn (`untilWF`).
- `entitlement_strip`: as the entitlement `keys` case, `ext`
  stripped-shaped half the time.
- `writes`: `rt` from `genUtf8`; a pool of 1..5 rids; 1..6 ops, each a
  `put` (75%) of 1..4 `(rid, "v<k>")` pairs drawn from the pool, or a
  `delete` of a pool rid (80%) or a fresh rid.
- `pagination`: 0..12 distinct rids from `genUtf8`, shuffled; page size
  uniform over `{0, 1, 2, 3, 5, 7, len, len+1, 13}`.
- `bare_id`: 1..3 resources (`rt` from `group`/`role` or `genUtf8`), an
  external-id pool of 1..3, 1..5 entitlements with distinct
  `(rt, rid, ext)` and `ext` from the pool or (25%) stripped-shaped;
  `lookup` is an existing `ext` 75% of the time, else a fresh string.
- `sync`: 1..7 ops uniform over `start_new` (id from `s1 s2 s3`, type
  uniform over full/partial/resources_only), `write`, `end`, `resume`,
  `latest_finished` (filter uniform over any and the three types).
  `resume` names the most recent `start_new` id half the time once one
  exists, else an id from `s1 s2 s3 s4`; `s4` is never started.

`genUtf8` concatenates fragments that are whole code points or strings
(ASCII, `:`, U+0000, U+0001, `é`, `ü`, `€`, `日本`, `グ`, U+1D11E), so
every string in `writes`, `pagination`, `bare_id`, and `sync` is valid
UTF-8 by construction.

Not covered beyond the fixed corpus's list in `Oracle.Cases`:

- page sizes above 13 and more than 12 rids, so never more than one
  default page;
- `bare_id` lookups that hit only a stripped tail are rare (the tail is
  not drawn from the pool);
- `writes` never mixes resource types in one case;
- `sync` never uses an empty or non-ASCII id, and a non-`none`
  `latest_finished` result is rare (about 3% of those ops at seed 1,
  `N = 400`): it needs `end` then a matching filter.
-/

namespace Oracle.Random

open C1z

/-- SplitMix64 state. -/
structure Rng where
  s : UInt64

def Rng.next (g : Rng) : UInt64 × Rng :=
  let s := g.s + 0x9e3779b97f4a7c15
  let z := (s ^^^ (s >>> 30)) * 0xbf58476d1ce4e5b9
  let z := (z ^^^ (z >>> 27)) * 0x94d049bb133111eb
  (z ^^^ (z >>> 31), ⟨s⟩)

abbrev Gen := StateM Rng

/-- Uniform-ish in `0..n-1`; 0 when `n = 0`. -/
def below (n : Nat) : Gen Nat := do
  let (x, g) := (← get).next
  set g
  pure (if n == 0 then 0 else x.toNat % n)

/-- Uniform in `lo..hi`. -/
def range (lo hi : Nat) : Gen Nat := do pure (lo + (← below (hi + 1 - lo)))

/-- True with probability `k / n`. -/
def chance (k n : Nat) : Gen Bool := do pure ((← below n) < k)

def pick {α : Type} [Inhabited α] (xs : List α) : Gen α := do pure xs[← below xs.length]!

def shuffle {α : Type} [Inhabited α] (xs : List α) : Gen (List α) := do
  let mut rest := xs
  let mut out := []
  for _ in xs do
    let i ← below rest.length
    out := rest[i]! :: out
    rest := rest.eraseIdx i
  pure out

/-! ## arbitrary bytes (`keys`, `entitlement_strip`) -/

def genByte : Gen Nat := do
  let r ← below 10
  if r < 2 then pure 0x00
  else if r < 4 then pure 0x01
  else if r < 6 then pure 0x3a
  else if r < 9 then pick [0x61, 0x62, 0x67, 0x75]
  else pure (0x80 + (← below 128))

def genBytes : Gen Bytes := do
  let n ← below 7
  (List.range n).mapM fun _ => genByte

/-- `ext` is `entPrefix rt rid ++ tail` half the time. -/
def genExt (rt rid : Bytes) : Gen Bytes := do
  if ← chance 1 2 then pure (entPrefix rt rid ++ (← genBytes)) else genBytes

/-- Redraws until `ok` holds, at most 64 times. -/
def untilOk {α : Type} (ok : α → Bool) (g : Gen α) : Gen α := do
  let mut x ← g
  for _ in [0:64] do
    if ok x then break
    x ← g
  pure x

def genEnt : Gen EntitlementId :=
  untilOk (fun e => decide e.WellFormed) do
    let rt ← genBytes
    let rid ← genBytes
    pure ⟨rt, rid, ← genExt rt rid⟩

def genKey (name : String) : Gen KeyCase := do
  match ← below 4 with
  | 0 => pure (.resourceType name ⟨← genBytes⟩)
  | 1 => pure (.resource name ⟨← genBytes, ← genBytes⟩)
  | 2 => pure (.entitlement name (← genEnt))
  | _ =>
    let g ← untilOk (fun g => decide g.WellFormed) do
      pure (GrantId.mk (← genEnt) (← genBytes) (← genBytes))
    pure (.grant name g)

/-! ## valid UTF-8 (`writes`, `pagination`, `bare_id`, `sync`) -/

def fragments : List String :=
  ["a", "b", "c", "z", "Z", "0", "9", "u", "g", ":", ":", "-", "\x00", "\x01",
   "é", "ü", "€", "日本", "グ", "𝄞"]

/-- `lo..hi` fragments; never empty when `lo ≥ 1`. -/
def genUtf8 (lo hi : Nat) : Gen Bytes := do
  let n ← range lo hi
  let parts ← (List.range n).mapM fun _ => pick fragments
  pure (u (String.join parts))

def genWrite (name : String) : Gen WriteCase := do
  let rt ← genUtf8 1 3
  let poolN ← range 1 5
  let pool := ((← (List.range poolN).mapM fun _ => genUtf8 1 3)).eraseDups
  let nOps ← range 1 6
  let mut ops : Array WOp := #[]
  let mut v := 0
  for _ in [0:nOps] do
    if ← chance 3 4 then
      let bn ← range 1 4
      let mut batch : Array (Bytes × String) := #[]
      for _ in [0:bn] do
        v := v + 1
        batch := batch.push (← pick pool, s!"v{v}")
      ops := ops.push (.put batch.toList)
    else
      let rid ← if ← chance 4 5 then pick pool else genUtf8 1 3
      ops := ops.push (.delete rid)
  pure ⟨name, rt, ops.toList⟩

def genPage (name : String) : Gen PageCase := do
  let rt ← genUtf8 1 3
  let target ← below 13
  let mut rids : List Bytes := []
  for _ in [0:target * 8] do
    if rids.length < target then
      let r ← genUtf8 1 3
      unless rids.contains r do rids := rids ++ [r]
  let shuffled ← shuffle rids
  let len := shuffled.length
  let size ← pick [0, 1, 2, 3, 5, 7, len, len + 1, 13]
  pure ⟨name, rt, shuffled, size⟩

def genBare (name : String) : Gen BareCase := do
  let nRes ← range 1 3
  let resources ← (List.range nRes).mapM fun _ => do
    let rt ← if ← chance 1 2 then pick [u "group", u "role"] else genUtf8 1 2
    let rid ← if ← chance 1 2 then pick [u "g1", u "g2"] else genUtf8 1 2
    pure (rt, rid)
  let poolN ← range 1 3
  let pool ← (List.range poolN).mapM fun _ => do
    if ← chance 1 2 then pick [u "member", u "admin"] else genUtf8 1 3
  let nEnt ← range 1 5
  let mut ents : List EntitlementId := []
  for _ in [0:nEnt] do
    let (rt, rid) ← pick resources
    let ext ← if ← chance 1 4 then pure (entPrefix rt rid ++ (← pick pool)) else pick pool
    let e : EntitlementId := ⟨rt, rid, ext⟩
    unless ents.contains e do ents := ents ++ [e]
  let lookup ← if ← chance 3 4 then pick (ents.map (·.ext)) else genUtf8 1 3
  pure ⟨name, ents, lookup⟩

instance : Inhabited Sync.SyncType := ⟨.full⟩

def genSyncType : Gen Sync.SyncType := pick [.full, .partialSync, .resourcesOnly]

instance : Inhabited SOp := ⟨.write⟩

/-- `last` is the id of the most recent `start_new` in the case, if any. -/
def genSOp (last : Option String) : Gen SOp := do
  match ← below 5 with
  | 0 => pure (.startNew (← pick ["s1", "s2", "s3"]) (← genSyncType))
  | 1 => pure .write
  | 2 => pure .endSync
  | 3 =>
    match last with
    | some id => if ← chance 1 2 then pure (.resume id) else pure (.resume (← pick ["s1", "s2", "s3", "s4"]))
    | none => pure (.resume (← pick ["s1", "s2", "s3", "s4"]))
  | _ => pure (.latestFinished (← pick [none, some .full, some .partialSync, some .resourcesOnly]))

def genSync (name : String) : Gen SyncCase := do
  let n ← range 1 7
  let mut ops : Array SOp := #[]
  let mut last : Option String := none
  for _ in [0:n] do
    let op ← genSOp last
    if let .startNew id _ := op then last := some id
    ops := ops.push op
  pure ⟨name, ops.toList⟩

def many {α : Type} (fam : String) (n : Nat) (g : String → Gen α) : Gen (List α) :=
  (List.range n).mapM fun i => g s!"random/{fam}/{i}"

/-- `n` random cases per family, rendered by the model. -/
def families (n seed : Nat) : Except String Families :=
  let gen : Gen (Except String Families) := do
    let keys ← many "keys" n genKey
    let strip ← (List.range n).mapM fun _ => genEnt
    let writes ← many "writes" n genWrite
    let pages ← many "pagination" n genPage
    let bare ← many "bare_id" n genBare
    let sync ← many "sync" n genSync
    pure (render keys strip writes pages bare sync)
  (gen.run' ⟨UInt64.ofNat seed⟩).run

end Oracle.Random
