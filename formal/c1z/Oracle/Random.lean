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

- grant families: per-case pools of 1..3 entitlements (owner
  `group`/`role` and `g1`/`g2` half the time each, `ext` stripped-shaped
  half the time), 1..3 principals (`user`/`group`/`service` and
  `u1`/`u2` two thirds of the time), and 3 custom external ids from
  `genUtf8`. A grant picks its entitlement and principal from the pools;
  its `ext_id` is empty 40%, the public id 10%, else a pool id, so
  collapses and shared ids across structures are common.
- `grant_writes`: 1..6 ops, `put` (75%) of 1..4 grants or `delete` of a
  pool identity. `entitlement_writes`: a pool of 1..4 entitlements, 1..6
  ops, `put` (75%) of 1..4 `(entitlement, "v<k>")` pairs or `delete`
  of a pool entitlement (80%) or a fresh one.
- `grant_list`: 0..8 grants; the query entitlement is from the pool 85%;
  no filter, a principal-type filter, or a point lookup, a third each,
  with principal parts from the pool 75%; page size from
  `{0, 1, 2, 3, 5}`.
- `grants_by_principal`: 1..6 ops uniform over `put`, `put_deferred`,
  `delete`, `read` (a pool principal); then `end_sync` a third of the
  time, followed by one `read` half of those times.
- `grant_bare_id`: each pool entitlement gets an entitlement row with
  probability 1/2; 1..5 grants; `lookup` is a stored non-empty `ext_id`
  30%, a stored grant's public id 30% (also the fallback when every
  stored id is empty), a fresh `genUtf8` string 20%, `""` 20%.

- `stream`: kind `grants` 60%, `resources` 20%, `entitlements` 20%;
  `cancel_after` none 50% else `0..3`, `break_after` none 60% else
  `1..4` (`genConsumer`). Grants: grant pools as above, each pool
  entitlement gets a row 1/2, 0..6 grants, 1..3 deferred grants 40% of
  the time; `ent_ext` 1/3 (`genEntExt`: an `ext` unique among the rows,
  or `nomatch`), `prt` 1/2, `prid` 1/3, principal parts from the pool
  75%. Resources: 0..6 distinct owners from `genOwner`, `rt` filter half
  the time (an existing type 75%). Entitlements: 0..5 from
  `genGrantEnt`.
- `digest`: see `genDigest`; grants carry `immutable` 25% and 0..2
  sources (`genDGrant`). Grant counts stay at 0..5 plus 1..3 per `put`.
- `reopen`: see `genReopen`; ops are drawn against the model's
  `Sync.FileState` so every input satisfies the `render` checks.
- `views`: see `genViews`. `container`: see `genContainer`; ops are
  drawn against `CState`, `save_reopen` is read-only 30% and damaged 15%
  (`genDamage`).

`genUtf8` concatenates fragments that are whole code points or strings
(ASCII, `:`, U+0000, U+0001, `é`, `ü`, `€`, `日本`, `グ`, U+1D11E), so
every string in `writes`, `pagination`, `bare_id`, `sync`, and the grant
families is valid UTF-8 by construction.

Not covered beyond the fixed corpus's list in `Oracle.Cases`:

- page sizes above 13 and more than 12 rids, so never more than one
  default page;
- `bare_id` lookups that hit only a stripped tail are rare (the tail is
  not drawn from the pool);
- `writes` never mixes resource types in one case;
- `grant_list` rarely spans more than one page (about 3% of cases at
  seed 1, `N = 400`, a third of those ending in an empty filtered page),
  and `grant_bare_id` lookups with more than 64 colons never occur;
- `stream` never writes more than 9 grant rows;
- `digest` reads a `found: false` global root in about 1.5% of cases at seed 7, `N = 400` (it
  needs a `put` or `delete` then `read_global` before the next `seal`),
  and never reaches width 1 (only the fixed corpus does);
- `reopen` uses only `s1`/`s2` and never ages a sync more than once;
- `container` uses only `s1`/`s2`; about 15% of cases at seed 1,
  `N = 400`, never write a file (no `start_new` succeeds) and so carry
  no `save_reopen`; at most one `open_error` per case, always last;
- `views` writes at most 8 grant rows and 4 resources;
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

/-! ## grants and entitlements (`grant_writes` through `grant_bare_id`) -/

/-- A resource owner: `group`/`role` and `g1`/`g2` half the time each, so
owners repeat across draws. -/
def genOwner : Gen (Bytes × Bytes) := do
  let rt ← if ← chance 1 2 then pick [u "group", u "role"] else genUtf8 1 2
  let rid ← if ← chance 1 2 then pick [u "g1", u "g2"] else genUtf8 1 2
  pure (rt, rid)

/-- An entitlement with a non-empty `ext`: stripped-shaped half the time,
else opaque from `member`/`admin` or `genUtf8`. -/
def genGrantEnt : Gen EntitlementId := do
  let (rt, rid) ← genOwner
  let tail ← if ← chance 1 2 then pick [u "member", u "admin"] else genUtf8 1 3
  if ← chance 1 2 then pure ⟨rt, rid, entPrefix rt rid ++ tail⟩ else pure ⟨rt, rid, tail⟩

def genPrincipal : Gen (Bytes × Bytes) := do
  let prt ← if ← chance 2 3 then pick [u "user", u "group", u "service"] else genUtf8 1 2
  let prid ← if ← chance 2 3 then pick [u "u1", u "u2"] else genUtf8 1 2
  pure (prt, prid)

/-- Per-case pools: 1..3 distinct entitlements, 1..3 distinct principals,
and 3 custom external ids. -/
structure GrantPools where
  ents : List EntitlementId
  prins : List (Bytes × Bytes)
  extIds : List Bytes

def genPools : Gen GrantPools := do
  let ne ← range 1 3
  let ents := (← (List.range ne).mapM fun _ => genGrantEnt).eraseDups
  let np ← range 1 3
  let prins := (← (List.range np).mapM fun _ => genPrincipal).eraseDups
  let extIds ← (List.range 3).mapM fun _ => genUtf8 1 3
  pure ⟨ents, prins, extIds⟩

instance : Inhabited EntitlementId := ⟨⟨[], [], []⟩⟩

instance : Inhabited GrantRecord := ⟨⟨⟨default, [], []⟩, []⟩⟩

def genGrantId (p : GrantPools) : Gen GrantId := do
  let (prt, prid) ← pick p.prins
  pure ⟨← pick p.ents, prt, prid⟩

/-- `ext_id` empty 40%, the public id 10%, else from the custom pool. -/
def genGrant (p : GrantPools) : Gen GrantRecord := do
  let g ← genGrantId p
  let r ← below 10
  let ext ← if r < 4 then pure [] else if r < 5 then pure g.publicId else pick p.extIds
  pure ⟨g, ext⟩

def genGrantBatch (p : GrantPools) : Gen (List GrantRecord) := do
  let n ← range 1 4
  (List.range n).mapM fun _ => genGrant p

def genGrantWrite (name : String) : Gen GrantWriteCase := do
  let p ← genPools
  let n ← range 1 6
  let ops ← (List.range n).mapM fun _ => do
    if ← chance 3 4 then pure (GWOp.put (← genGrantBatch p)) else pure (GWOp.delete (← genGrantId p))
  pure ⟨name, ops⟩

def genEntWrite (name : String) : Gen EntWriteCase := do
  let ne ← range 1 4
  let pool := (← (List.range ne).mapM fun _ => genGrantEnt).eraseDups
  let n ← range 1 6
  let mut ops : Array EWOp := #[]
  let mut v := 0
  for _ in [0:n] do
    if ← chance 3 4 then
      let bn ← range 1 4
      let mut batch : Array (EntitlementId × String) := #[]
      for _ in [0:bn] do
        v := v + 1
        batch := batch.push (← pick pool, s!"v{v}")
      ops := ops.push (.put batch.toList)
    else
      let e ← if ← chance 4 5 then pick pool else genGrantEnt
      ops := ops.push (.delete e)
  pure ⟨name, ops.toList⟩

/-- Query entitlement from the pool 85%; filter none, principal type, or
point lookup, a third each; principal parts from the pool 75%. -/
def genGrantList (name : String) : Gen GrantListCase := do
  let p ← genPools
  let ng ← below 9
  let grants ← (List.range ng).mapM fun _ => genGrant p
  let ent ← if ← chance 17 20 then pick p.ents else genGrantEnt
  let (pt, pi) ← if ← chance 3 4 then pick p.prins else genPrincipal
  let pageSize ← pick [0, 1, 2, 3, 5]
  match ← below 3 with
  | 0 => pure { name, grants, ent, pageSize }
  | 1 => pure { name, grants, ent, prt := some pt, pageSize }
  | _ => pure { name, grants, ent, prt := some pt, prid := some pi, pageSize }

instance : Inhabited POp := ⟨.endSync⟩

/-- 1..6 ops uniform over put, put_deferred, delete, read; then `end_sync`
a third of the time, followed by one read half of those times. -/
def genByPrincipal (name : String) : Gen ByPrincipalCase := do
  let p ← genPools
  let read : Gen POp := do
    let (prt, prid) ← pick p.prins
    pure (.read prt prid)
  let n ← range 1 6
  let mut ops ← (List.range n).mapM fun _ => do
    match ← below 4 with
    | 0 => pure (POp.put (← genGrantBatch p))
    | 1 => pure (POp.putDeferred (← genGrantBatch p))
    | 2 => pure (POp.delete (← genGrantId p))
    | _ => read
  if ← chance 1 3 then
    ops := ops ++ [.endSync]
    if ← chance 1 2 then ops := ops ++ [← read]
  pure ⟨name, ops⟩

/-- Entitlement rows: each pool entitlement with probability 1/2. Lookup:
a stored `ext_id` 30% (falls back to a public id when every stored id is
empty), a public id of a stored grant 30%, a fresh string 20%, `""` 20%. -/
def genGrantBare (name : String) : Gen GrantBareCase := do
  let p ← genPools
  let ents ← p.ents.filterM fun _ => chance 1 2
  let ng ← range 1 5
  let grants ← (List.range ng).mapM fun _ => genGrant p
  let custom := (grants.map (·.externalId)).filter (!·.isEmpty)
  let r ← below 10
  let lookup ←
    if r < 3 && !custom.isEmpty then pick custom
    else if r < 6 then do pure (← pick grants).id.publicId
    else if r < 8 then genUtf8 1 3
    else pure []
  pure ⟨name, ents, grants, lookup⟩

/-! ## `stream`, `digest`, `reopen` -/

instance : Inhabited ResourceId := ⟨⟨[], []⟩⟩

/-- `cancel_after`: none half the time, else uniform over `0..3`.
`break_after`: none 60%, else uniform over `1..4`. -/
def genConsumer : Gen (Option Nat × Option Nat) := do
  let c ← if ← chance 1 2 then pure none else some <$> range 0 3
  let b ← if ← chance 3 5 then pure none else some <$> range 1 4
  pure (c, b)

/-- An `ent_ext` that resolves to exactly one entitlement row, or, when no
row's `ext` is unique (or 25% of the time), `nomatch`, which no generator
fragment can spell and which contains no colon. -/
def genEntExt (rows : List EntitlementId) : Gen Bytes := do
  let unique := rows.filter fun e => (rows.filter (·.ext == e.ext)).length == 1
  if unique.isEmpty || (← chance 1 4) then pure (u "nomatch") else pure (← pick unique).ext

def genStream (name : String) : Gen StreamCase := do
  let (cancelAfter, breakAfter) ← genConsumer
  let r ← below 5
  if r < 3 then
    let p ← genPools
    let ents ← p.ents.filterM fun _ => chance 1 2
    let ng ← below 7
    let grants ← (List.range ng).mapM fun _ => genGrant p
    let deferred ← if ← chance 2 5 then do
        let nd ← range 1 3
        (List.range nd).mapM fun _ => genGrant p
      else pure []
    let entExt ← if ← chance 1 3 then some <$> genEntExt ents else pure none
    let (pt, pi) ← if ← chance 3 4 then pick p.prins else genPrincipal
    let prt ← if ← chance 1 2 then pure (some pt) else pure none
    let prid ← if ← chance 1 3 then pure (some pi) else pure none
    pure { name, kind := .grants, ents, grants, deferred, entExt, prt, prid, cancelAfter, breakAfter }
  else if r < 4 then
    let n ← below 7
    let mut resources : List ResourceId := []
    for _ in [0:n] do
      let (rt, rid) ← genOwner
      let res : ResourceId := ⟨rt, rid⟩
      unless resources.any (·.key == res.key) do resources := resources ++ [res]
    let rt ← if ← chance 1 2 then pure none
      else if (← chance 3 4) && !resources.isEmpty then pure (some (← pick resources).rt)
      else some <$> genUtf8 1 2
    pure { name, kind := .resources, resources, rt, cancelAfter, breakAfter }
  else
    let n ← below 6
    let ents := (← (List.range n).mapM fun _ => genGrantEnt).eraseDups
    pure { name, kind := .entitlements, ents, cancelAfter, breakAfter }

/-- `immutable` 25%; 0..2 sources with distinct keys from `s1`, `s2`, or
`genUtf8`, `is_direct` half the time. -/
def genDGrant (p : GrantPools) : Gen DGrant := do
  let record ← genGrant p
  let immutable ← chance 1 4
  let ns ← below 3
  let mut sources : List Digest.SourceFact := []
  for _ in [0:ns] do
    let key ← if ← chance 2 3 then pick [u "s1", u "s2"] else genUtf8 1 2
    unless sources.any (·.key == key) do sources := sources ++ [⟨key, ← chance 1 2⟩]
  pure { record, immutable, sources }

instance : Inhabited DGrant := ⟨{ record := default }⟩

/-- Entitlement rows: each pool entitlement 1/2, plus a fresh zero-grant
one 1/4. 0..5 grants. Ops follow the seal/resume state machine: while
open, `seal` 40%, `put` 25%, `delete` 15%, `read` 10%, `read_global`
10% (reads only after the first seal, else `seal`); while sealed, `resume` 40%, `read` 40%, `read_global`
20%. 1..6 ops, then, if open, a `seal` 3/4 of the time followed by one
`read` and one `read_global`. -/
def genDigest (name : String) : Gen DigestCase := do
  let p ← genPools
  let mut ents ← p.ents.filterM fun _ => chance 1 2
  if ← chance 1 4 then
    let e ← genGrantEnt
    unless ents.contains e do ents := ents ++ [e]
  let readable := (p.ents ++ ents).eraseDups
  let ng ← below 6
  let grants ← (List.range ng).mapM fun _ => genDGrant p
  let n ← range 1 6
  let mut ops : Array DOp := #[]
  let mut sealed := false
  let mut sealedOnce := false
  for _ in [0:n] do
    let r ← below 20
    if sealed then
      if r < 8 then
        ops := ops.push .resume
        sealed := false
      else if r < 16 then ops := ops.push (.read (← pick readable))
      else ops := ops.push .readGlobal
    else
      if r < 8 then
        ops := ops.push .seal
        sealed := true
        sealedOnce := true
      else if r < 13 then
        let bn ← range 1 3
        ops := ops.push (.put (← (List.range bn).mapM fun _ => genDGrant p))
      else if r < 16 then ops := ops.push (.delete (← genGrantId p))
      else if sealedOnce then
        if r < 18 then ops := ops.push (.read (← pick readable)) else ops := ops.push .readGlobal
      else
        ops := ops.push .seal
        sealed := true
        sealedOnce := true
  if !sealed && (← chance 3 4) then
    ops := ops ++ #[.seal, .read (← pick readable), .readGlobal]
  pure ⟨name, ents, grants, ops.toList⟩

instance : Inhabited ROp := ⟨.reopen⟩

/-- Ops are drawn against the model state so `put`/`put_deferred` appear
only while a sync is bound and `age_sync` only directly after `reopen`
on a file with a record. The first op is `start_new` 80% of the time.
Then, from `below 20`: `start_new` 2, `put` 4, `put_deferred` 2, `end` 3,
`reopen` 3, `write` 1, `list_grants` 2, `read_by_principal` 2, `resume`
1 (out of the 20 the last slot is `latest_finished`). A `put` or
`put_deferred` drawn while unbound becomes `list_grants`. After
`reopen`, `age_sync` follows 40% of the time with `days` from
`{1, 6, 8, 30}`. 1..8 drawn ops. -/
def genReopen (name : String) : Gen ReopenCase := do
  let p ← genPools
  let ids := ["s1", "s2"]
  let mut s := Sync.opened none false
  let mut ops : Array ROp := #[]
  let mut last : Option String := none
  let first : Bool ← chance 4 5
  let n ← range 1 8
  for i in [0:n] do
    let r ← if i == 0 && first then pure 0 else below 20
    let bound := Sync.writeGate s == .allowed
    let op : ROp ←
      if r < 2 then pure (.startNew (← pick ids) (← genSyncType))
      else if r < 6 then if bound then pure (.put (← genGrantBatch p)) else pure .listGrants
      else if r < 8 then if bound then pure (.putDeferred (← genGrantBatch p)) else pure .listGrants
      else if r < 11 then pure .endSync
      else if r < 14 then pure .reopen
      else if r < 15 then pure .write
      else if r < 17 then pure .listGrants
      else if r < 19 then do
        let (prt, prid) ← pick p.prins
        pure (.readByPrincipal prt prid)
      else if (← chance 1 2) then
        match last with
        | some id => pure (.resume id)
        | none => pure (.resume (← pick ids))
      else pure (.latestFinished (← pick [none, some .full, some .partialSync, some .resourcesOnly]))
    ops := ops.push op
    match op with
    | .startNew id t =>
      match Sync.startNewSync s id t reopenNow with
      | .ok s' => s := s'; last := some id
      | .syncInProgress => pure ()
    | .endSync =>
      match Sync.endSync s reopenNow with
      | .ok s' => s := s'
      | .noCurrentSync => pure ()
    | .resume id =>
      match Sync.resumeSync s id with
      | .ok s' => s := s'
      | .notFound => pure ()
    | .reopen =>
      s := Sync.reopen s
      if s.run.isSome && (← chance 2 5) then
        let days ← pick [1, 6, 8, 30]
        ops := ops.push (.ageSync days)
        s := Sync.setStartedAt s (reopenNow - days * secondsPerDay)
    | _ => pure ()
  pure ⟨name, ops.toList⟩

/-! ## `views`, `container` -/

instance : Inhabited VQuery := ⟨.listGrants⟩

/-- Grant pools as above; 0..4 resources from `genOwner` (distinct,
values `r<i>`); entitlement rows: each pool entitlement 2/3, plus a fresh
one 1/3, plus 1/4 a twin of a drawn row (same `ext` under a `genOwner`
owner, so bare ids are ambiguous more often) (values `e<i>`); 0..6 grants, 1..2 deferred 40%; `end_sync`
half the time. 3..9 queries uniform over the nine views. Query
entitlements come from the pool 85%, principals from the pool 75%.
`stream_grants_for_entitlement` picks a row whose `ext` no other row
shares, and becomes `grants_for_entitlement` when there is none.
`resources_by_ids`: 1..4 ids, a stored resource 60% (so repeats are
common), else a fresh owner. `entitlements_by_ids`: 1..4 ids, a stored
row's `ext` 60%, a pool entitlement's `ext` 20% (missing when the row
was not drawn; ambiguous when two rows share it), else `genUtf8`. -/
def genViews (name : String) : Gen ViewsCase := do
  let p ← genPools
  let nr ← below 5
  let mut res : List (ResourceId × String) := []
  for i in [0:nr] do
    let (rt, rid) ← genOwner
    let r : ResourceId := ⟨rt, rid⟩
    unless res.any (·.1.key == r.key) do res := res ++ [(r, s!"r{i}")]
  let mut rows ← p.ents.filterM fun _ => chance 2 3
  if ← chance 1 3 then
    let e ← genGrantEnt
    unless rows.contains e do rows := rows ++ [e]
  if !rows.isEmpty && (← chance 1 4) then
    let e ← pick rows
    let (rt, rid) ← genOwner
    let twin : EntitlementId := ⟨rt, rid, e.ext⟩
    unless rows.contains twin do rows := rows ++ [twin]
  let resources := res
  let entIds := rows
  let ents := entIds.zipIdx.map fun (e, i) => (e, s!"e{i}")
  let ng ← below 7
  let grants ← (List.range ng).mapM fun _ => genGrant p
  let deferred ← if ← chance 2 5 then do
      let nd ← range 1 2
      (List.range nd).mapM fun _ => genGrant p
    else pure []
  let endSync ← chance 1 2
  let streamable := entIds.filter fun e => entIds.filter (·.ext == e.ext) == [e]
  let ent : Gen EntitlementId := do if ← chance 17 20 then pick p.ents else genGrantEnt
  let prin : Gen (Bytes × Bytes) := do if ← chance 3 4 then pick p.prins else genPrincipal
  let resId : Gen ResourceId := do
    if (← chance 3 5) && !resources.isEmpty then pure (← pick resources).1
    else
      let (rt, rid) ← genOwner
      pure ⟨rt, rid⟩
  let entBare : Gen Bytes := do
    let r ← below 5
    if r < 3 && !entIds.isEmpty then pure (← pick entIds).ext
    else if r < 4 then pure (← pick p.ents).ext
    else genUtf8 1 3
  let nq ← range 3 9
  let queries ← (List.range nq).mapM fun _ => do
    match ← below 9 with
    | 0 => pure VQuery.listGrants
    | 1 => pure .streamGrants
    | 2 => pure (.grantsForEnt (← ent))
    | 3 => if streamable.isEmpty then pure (.grantsForEnt (← ent)) else pure (.streamForEnt (← pick streamable))
    | 4 =>
      let (prt, prid) ← prin
      pure (.point (← ent) prt prid)
    | 5 =>
      let (prt, prid) ← prin
      pure (.forPrincipal prt prid)
    | 6 => pure (.forPrincipalType (← prin).1)
    | 7 =>
      let k ← range 1 4
      pure (.resourcesByIds (← (List.range k).mapM fun _ => resId))
    | _ =>
      let k ← range 1 4
      pure (.entsByIds (← (List.range k).mapM fun _ => entBare))
  pure { name, resources, ents, grants, deferred, endSync, queries }

instance : Inhabited COp := ⟨.listGrants⟩

instance : Inhabited Container.Damage := ⟨.truncateHeader⟩

/-- Damage 15% of the time, uniform over the five kinds. -/
def genDamage : Gen (Option Container.Damage) := do
  if ← chance 3 20 then some <$> pick [.truncateHeader, .badMagic, .badEngine, .flipPayloadByte, .truncateTail]
  else pure none

/-- `save_reopen` read-only 30%, with `genDamage`. -/
def genSaveReopen : Gen COp := do pure (.saveReopen (← chance 3 10) (← genDamage))

/-- Ops are drawn against the model's `CState` (advanced by `COp.step`)
so every input satisfies the `render` checks. The first op is
`start_new` 80% of the time. Then, from `below 20`: `start_new` 2, `put`
4, `put_deferred` 2, `end` 2, `save_reopen` 3, `write` 1, `age_sync` 1
(`days` from `{1, 6, 8, 30}`), `list_grants` 1, `read_by_principal` 2,
and `resume` or `latest_finished` 1 each. An op the state does not allow
becomes a read: `put`/`put_deferred` while unbound or read-only, and
`start_new`, `age_sync`, `resume` on a read-only store, become
`list_grants` (`end` becomes `write`, `resume` becomes
`latest_finished`); `save_reopen` before any sync record becomes
`list_grants`. 1..10 drawn ops, stopping at an `open_error`; a case
with a file and no `save_reopen` gets one appended, then `list_grants`
when the open succeeds. -/
def genContainer (name : String) : Gen ContainerCase := do
  let p ← genPools
  let ids := ["s1", "s2"]
  let mut st : CState := {}
  let mut ops : Array COp := #[]
  let mut last : Option String := none
  let mut reopened := false
  let first : Bool ← chance 4 5
  let n ← range 1 10
  for i in [0:n] do
    if st.failed then break
    let r ← if i == 0 && first then pure 0 else below 20
    let ro := st.readOnly
    let bound := Container.writeGate' { state := st.s, readOnly := ro } == .allowed
    let op : COp ←
      if r < 2 then if ro then pure .listGrants else pure (.startNew (← pick ids) (← genSyncType))
      else if r < 6 then if bound then pure (.put (← genGrantBatch p)) else pure .listGrants
      else if r < 8 then if bound then pure (.putDeferred (← genGrantBatch p)) else pure .listGrants
      else if r < 10 then if ro then pure .write else pure .endSync
      else if r < 13 then if st.hasFile then genSaveReopen else pure .listGrants
      else if r < 14 then pure .write
      else if r < 15 then
        if !ro && st.s.run.isSome then pure (.ageSync (← pick [1, 6, 8, 30])) else pure .listGrants
      else if r < 16 then pure .listGrants
      else if r < 18 then do
        let (prt, prid) ← pick p.prins
        pure (.readByPrincipal prt prid)
      else if r < 19 && !ro then
        match last with
        | some id => if ← chance 1 2 then pure (.resume id) else pure (.resume (← pick ids))
        | none => pure (.resume (← pick ids))
      else pure (.latestFinished (← pick [none, some .full, some .partialSync, some .resourcesOnly]))
    ops := ops.push op
    if let .startNew id _ := op then last := some id
    if let .saveReopen .. := op then reopened := true
    if let .ok (st', _) := op.step name st then st := st'
  if !reopened && !st.failed && st.hasFile then
    let op ← genSaveReopen
    ops := ops.push op
    if let .ok (st', _) := op.step name st then st := st'
    if !st.failed then ops := ops.push .listGrants
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
    let grantWrites ← many "grant_writes" n genGrantWrite
    let entWrites ← many "entitlement_writes" n genEntWrite
    let grantList ← many "grant_list" n genGrantList
    let byPrincipal ← many "grants_by_principal" n genByPrincipal
    let grantBare ← many "grant_bare_id" n genGrantBare
    let stream ← many "stream" n genStream
    let digest ← many "digest" n genDigest
    let reopen ← many "reopen" n genReopen
    let views ← many "views" n genViews
    let container ← many "container" n genContainer
    pure (render ⟨keys, strip, writes, pages, bare, sync, grantWrites, entWrites, grantList, byPrincipal, grantBare,
      stream, digest, reopen, views, container⟩)
  (gen.run' ⟨UInt64.ofNat seed⟩).run

end Oracle.Random
