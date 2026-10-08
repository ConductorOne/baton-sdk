import C1z.Identity
import C1z.Store
import C1z.Paginate
import C1z.Result
import C1z.Sync
import Oracle.Json

/-!
# Oracle case generators

Each family below is a hand-chosen list of inputs; every expected value
is computed by running the model (`C1z.Identity`, `C1z.Store`,
`C1z.Paginate`, `C1z.Result`, `C1z.Sync`). Nothing here writes an
expected output by hand. The schema is `ORACLE_SCHEMA.md`.
`Oracle.Random` and `Oracle.Request` pass their inputs through the same
`render`.

Deliberately not generated, so proved in Lean but never replayed
against the Pebble engine:

- `visible = false` rows: pagination always uses `fun _ => true`, so
  hidden-row skipping inside `Paginate.page` is not differentially tested;
- failure injection: I/O, decode, cancellation, and every
  `Result.ListError` arm; `Paginate.checkCursor` and invalid page tokens;
- malformed identities: entitlements and grants with an empty owner
  component (`EntitlementId.WellFormed`, `GrantId.WellFormed` fail),
  which the engine rejects; the generator refuses to emit them;
- page sizes above `Paginate.maxPageSize` (the other `clampPageSize`
  branch) and corpora longer than one default page;
- time: `discovered_at`, `startedAt`/`endedAt` values, the 7-day
  `latestUnfinished` fallback, and `resolveActiveSync`;
- `SyncType.unspecified`, which has no schema string;
- `StartNewSync` wiping records written by an earlier sync, and any
  multi-file behavior (compaction, `CloneSync`).
-/

namespace Oracle

open C1z

/-- UTF-8 bytes of a string as model `Bytes`. -/
def u (s : String) : Bytes := s.toUTF8.toList.map (·.toNat)

/-- Insertion sort by the model's key order. -/
def sortKeys (ks : List Bytes) : List Bytes :=
  ks.foldl (fun acc k => ins k acc) []
where
  ins (k : Bytes) : List Bytes → List Bytes
    | [] => [k]
    | x :: xs => if lexLt k x then k :: x :: xs else x :: ins k xs

/-- Look a key up in an association list built by the generator. -/
def lookupKey {α : Type} (tbl : List (Bytes × α)) (k : Bytes) : Except String α :=
  match tbl.find? (fun e => e.1 == k) with
  | some e => pure e.2
  | none => throw "key missing from generator lookup table"

/-! ## keys -/

inductive KeyCase where
  | resourceType (name : String) (r : ResourceTypeId)
  | resource (name : String) (r : ResourceId)
  | entitlement (name : String) (e : EntitlementId)
  | grant (name : String) (g : GrantId)

def KeyCase.toJ : KeyCase → Except String J
  | .resourceType n r => do
    pure <| .obj [("name", .str n), ("kind", .str "resource_type"), ("ext", ← hexJ r.extId), ("key", ← hexJ r.key)]
  | .resource n r => do
    pure <| .obj [("name", .str n), ("kind", .str "resource"), ("rt", ← hexJ r.rt), ("rid", ← hexJ r.rid),
      ("key", ← hexJ r.key)]
  | .entitlement n e => do
    unless decide e.WellFormed do throw s!"keys case {n}: entitlement not well formed"
    pure <| .obj [("name", .str n), ("kind", .str "entitlement"), ("rt", ← hexJ e.rt), ("rid", ← hexJ e.rid),
      ("ext", ← hexJ e.ext), ("key", ← hexJ e.key)]
  | .grant n g => do
    unless decide g.WellFormed do throw s!"keys case {n}: grant not well formed"
    pure <| .obj [("name", .str n), ("kind", .str "grant"), ("rt", ← hexJ g.ent.rt), ("rid", ← hexJ g.ent.rid),
      ("ext", ← hexJ g.ent.ext), ("prt", ← hexJ g.prt), ("prid", ← hexJ g.prid), ("key", ← hexJ g.key)]

def keyCases : List KeyCase := [
  .resourceType "resource type plain" ⟨u "user"⟩,
  .resourceType "resource type empty" ⟨[]⟩,
  .resourceType "resource type with 0x00 and 0x01" ⟨[0x61, 0x00, 0x01, 0x62]⟩,
  .resourceType "resource type with colon" ⟨u "app:user"⟩,
  .resourceType "resource type utf8" ⟨u "usuário"⟩,
  .resource "resource plain" ⟨u "u", u "1"⟩,
  .resource "resource empty rt and rid" ⟨[], []⟩,
  .resource "resource empty rid" ⟨u "u", []⟩,
  .resource "resource empty rt" ⟨[], u "1"⟩,
  .resource "resource with 0x00 and 0x01" ⟨[0x00, 0x75], [0x01, 0x00, 0x31, 0x01]⟩,
  .resource "resource colons in rt and rid" ⟨u "a:b", u "c:d:"⟩,
  .resource "resource utf8" ⟨u "グループ", u "é日本"⟩,
  .resource "resource same rid rt u" ⟨u "u", u "same"⟩,
  .resource "resource same rid other rt" ⟨u "g", u "same"⟩,
  .resource "resource rt prefix of other rt" ⟨u "uu", u "1"⟩,
  .entitlement "entitlement stripped" ⟨u "group", u "g1", u "group:g1:member"⟩,
  .entitlement "entitlement opaque" ⟨u "group", u "g1", u "member"⟩,
  .entitlement "entitlement empty ext" ⟨u "group", u "g1", []⟩,
  .entitlement "entitlement ext equals prefix" ⟨u "group", u "g1", u "group:g1:"⟩,
  .entitlement "entitlement colons in owner stripped" ⟨u "a:b", u "c:d", u "a:b:c:d:x"⟩,
  .entitlement "entitlement colons in owner opaque" ⟨u "a:b", u "c:d", u "a:b:c:x"⟩,
  .entitlement "entitlement with 0x00 and 0x01" ⟨[0x72, 0x00], [0x01], [0x72, 0x00, 0x3a, 0x01, 0x3a, 0x00, 0x01]⟩,
  .entitlement "entitlement utf8" ⟨u "équipe", u "日本", u "équipe:日本:membre"⟩,
  .entitlement "entitlement same ext on g1" ⟨u "group", u "g1", u "admin"⟩,
  .entitlement "entitlement same ext on g2" ⟨u "group", u "g2", u "admin"⟩,
  .entitlement "entitlement same ext other rt" ⟨u "role", u "g1", u "admin"⟩,
  .grant "grant stripped entitlement" ⟨⟨u "group", u "g1", u "group:g1:member"⟩, u "user", u "u1"⟩,
  .grant "grant opaque entitlement" ⟨⟨u "group", u "g1", u "member"⟩, u "user", u "u1"⟩,
  .grant "grant colons everywhere" ⟨⟨u "a:b", u "c:d", u "a:b:c:d:e:f"⟩, u "p:q", u "r:s"⟩,
  .grant "grant with 0x00 and 0x01" ⟨⟨u "group", [0x00, 0x01], u "m"⟩, [0x01], [0x00, 0x00]⟩,
  .grant "grant utf8" ⟨⟨u "グループ", u "g", u "グループ:g:所有者"⟩, u "usuário", u "é"⟩,
  .grant "grant same principal id other principal type" ⟨⟨u "group", u "g1", u "member"⟩, u "service", u "u1"⟩
]

/-! ## entitlement_strip -/

def stripCases : List EntitlementId := [
  ⟨u "group", u "g1", u "group:g1:member"⟩,
  ⟨u "group", u "g1", u "member"⟩,
  ⟨u "group", u "g1", u "group:g2:member"⟩,
  ⟨u "group", u "g1", u "group:g1"⟩,
  ⟨u "a:b", u "c:d", u "a:b:c:d:x"⟩,
  ⟨u "a:b", u "c:d", u "a:b:c:dx"⟩,
  ⟨u "group", u "g1", []⟩,
  ⟨u "group", u "g1", u "group:g1:"⟩,
  ⟨u "équipe", u "日本", u "équipe:日本:membre"⟩,
  ⟨[0x72, 0x00], [0x01], [0x72, 0x00, 0x3a, 0x01, 0x3a, 0x00]⟩
]

def stripToJ (e : EntitlementId) : Except String J := do
  unless decide e.WellFormed do throw "entitlement_strip case not well formed"
  let s := compressEnt e
  pure <| .obj [("rt", ← hexJ e.rt), ("rid", ← hexJ e.rid), ("ext", ← hexJ e.ext),
    ("stripped", .bool s.stripped), ("tail", ← hexJ s.tail)]

/-! ## writes -/

inductive WOp where
  | put (batch : List (Bytes × String))
  | delete (rid : Bytes)

structure WriteCase where
  name : String
  rt : Bytes
  ops : List WOp

def WOp.rids : WOp → List Bytes
  | .put b => b.map (·.1)
  | .delete r => [r]

def WOp.apply (rt : Bytes) (s : Store Bytes) : WOp → Store Bytes
  | .put b => s.putBatch (b.map fun (rid, v) => ((ResourceId.mk rt rid).key, u v))
  | .delete rid => s.erase (ResourceId.mk rt rid).key

def WOp.toJ : WOp → Except String J
  | .put b => do
    let items ← b.mapM fun (rid, v) => do pure (J.obj [("rid", ← hexJ rid), ("value", .str v)])
    pure <| .obj [("op", .str "put"), ("batch", .arr items)]
  | .delete rid => do pure <| .obj [("op", .str "delete"), ("rid", ← hexJ rid)]

def WriteCase.toJ (c : WriteCase) : Except String J := do
  for op in c.ops do
    for r in op.rids do
      if r.isEmpty then throw s!"writes case {c.name}: empty rid"
  if c.rt.isEmpty then throw s!"writes case {c.name}: empty rt"
  let store := c.ops.foldl (WOp.apply c.rt) Store.empty
  let tbl := (c.ops.flatMap WOp.rids).map fun rid => ((ResourceId.mk c.rt rid).key, rid)
  let final ← store.entries.mapM fun (k, v) => do
    let rid ← lookupKey tbl k
    let some name := String.fromUTF8? (ByteArray.mk (v.map UInt8.ofNat).toArray)
      | throw s!"writes case {c.name}: value is not UTF-8"
    pure (J.obj [("rid", ← hexJ rid), ("value", .str name)])
  pure <| .obj [("name", .str c.name), ("rt", ← hexJ c.rt), ("ops", .arr (← c.ops.mapM WOp.toJ)),
    ("final", .arr final)]

def writeCases : List WriteCase := [
  ⟨"single put", u "user", [.put [(u "1", "v1")]]⟩,
  ⟨"last write wins across calls", u "user", [.put [(u "1", "v1")], .put [(u "1", "v2")]]⟩,
  ⟨"last occurrence wins in one batch", u "user", [.put [(u "1", "v1"), (u "2", "w1"), (u "1", "v2")]]⟩,
  ⟨"two rids then delete one", u "user", [.put [(u "1", "v1"), (u "2", "w1")], .delete (u "1")]⟩,
  ⟨"delete of absent rid is a no-op", u "user", [.put [(u "1", "v1")], .delete (u "9")]⟩,
  ⟨"delete on empty store", u "user", [.delete (u "1")]⟩,
  ⟨"interleaved puts leave unrelated keys", u "user",
    [.put [(u "b", "b1")], .put [(u "a", "a1")], .put [(u "c", "c1")], .put [(u "a", "a2")], .put [(u "b", "b2")]]⟩,
  ⟨"put delete put", u "user", [.put [(u "1", "v1")], .delete (u "1"), .put [(u "1", "v3")]]⟩,
  ⟨"key order differs from insertion order", u "app:user",
    [.put [(u "é", "e-acute"), ([0x61, 0x00], "a-nul"), (u "a", "a"), ([0x61, 0x01], "a-soh"), (u "a:b", "a-colon")]]⟩
]

/-! ## pagination -/

structure PageCase where
  name : String
  rt : Bytes
  rids : List Bytes
  pageSize : Nat

/-- Pages of `Paginate.page` chained through `next`, each paired with
whether a next token was returned. Cross-checked against `traverse`. -/
def pagesWithNext (ks : List Bytes) (limit : Nat) : Except String (List (List Bytes × Bool)) := do
  let rec go : Nat → Option Bytes → List (List Bytes × Bool)
    | 0, _ => []
    | fuel + 1, cursor =>
      let p := Paginate.page (fun _ => true) ks cursor limit
      match p.next with
      | none => [(p.items, false)]
      | some tok => (p.items, true) :: go fuel (some tok)
  let fuel := ks.length + 1
  let ps := go fuel none
  if ps.map (·.1) != Paginate.traverse (fun _ => true) ks limit fuel none then
    throw "page chain disagrees with Paginate.traverse"
  pure ps

def PageCase.toJ (c : PageCase) : Except String J := do
  let tbl := c.rids.map fun rid => ((ResourceId.mk c.rt rid).key, rid)
  let ks := sortKeys (tbl.map (·.1))
  if ks.eraseDups.length != ks.length then throw s!"pagination case {c.name}: duplicate rid"
  if c.rt.isEmpty || c.rids.any List.isEmpty then throw s!"pagination case {c.name}: empty component"
  let ps ← pagesWithNext ks (Paginate.clampPageSize c.pageSize)
  let pages ← ps.mapM fun (items, hasNext) => do
    let rids ← items.mapM fun k => do hexJ (← lookupKey tbl k)
    pure (J.obj [("rids", .arr rids), ("has_next", .bool hasNext)])
  pure <| .obj [("name", .str c.name), ("rt", ← hexJ c.rt), ("rids", .arr (← c.rids.mapM hexJ)),
    ("page_size", .num c.pageSize), ("pages", .arr pages)]

/-- Insertion order is deliberately unsorted; `0x00`/`0x01` and UTF-8
make byte order differ from naive string order (`"a"` < `"a\x00"` <
`"a\x01"` < `"a:"` < `"b"` < `"é"`). -/
def tricky : List Bytes :=
  [u "é", u "b", [0x61, 0x01], u "a", u "a:", [0x61, 0x00], u "日本", u "Z", u "a:b"]

def threeRids : List Bytes := [u "3", u "1", u "2"]

def pageCases : List PageCase :=
  let sizes := [0, 1, 2, 3, tricky.length, tricky.length + 5]
  [ ⟨"empty type page size two", u "user", [], 2⟩,
    ⟨"one key page size one", u "user", [u "1"], 1⟩,
    ⟨"three keys page size one", u "user", threeRids, 1⟩,
    ⟨"three keys page size two", u "user", threeRids, 2⟩,
    ⟨"three keys page size three", u "user", threeRids, 3⟩,
    ⟨"three keys page size zero", u "user", threeRids, 0⟩,
    ⟨"four keys page size two", u "user", [u "d", u "b", u "a", u "c"], 2⟩ ] ++
  sizes.map fun n => ⟨s!"tricky bytes page size {n}", u "app:user", tricky, n⟩

/-! ## bare_id -/

structure BareCase where
  name : String
  ents : List EntitlementId
  lookup : Bytes

def entJ (e : EntitlementId) : Except String J := do
  pure <| .obj [("rt", ← hexJ e.rt), ("rid", ← hexJ e.rid), ("ext", ← hexJ e.ext)]

def BareCase.toJ (c : BareCase) : Except String J := do
  for e in c.ents do
    unless decide e.WellFormed do throw s!"bare_id case {c.name}: entitlement not well formed"
  let base := [("name", J.str c.name), ("entitlements", .arr (← c.ents.mapM entJ)), ("lookup", ← hexJ c.lookup)]
  match Result.resolveBare (c.ents.filter (·.ext == c.lookup)) with
  | .notFound => pure <| .obj (base ++ [("expected", .str "not_found")])
  | .found e => pure <| .obj (base ++ [("expected", .str "found"), ("found", ← entJ e)])
  | .ambiguous => pure <| .obj (base ++ [("expected", .str "ambiguous")])

def bareCases : List BareCase := [
  ⟨"no entitlements", [], u "member"⟩,
  ⟨"zero matches", [⟨u "group", u "g1", u "member"⟩, ⟨u "group", u "g2", u "admin"⟩], u "owner"⟩,
  ⟨"exactly one opaque match among several",
    [⟨u "group", u "g1", u "member"⟩, ⟨u "group", u "g1", u "admin"⟩, ⟨u "group", u "g2", u "owner"⟩], u "admin"⟩,
  ⟨"exactly one stripped match by full id",
    [⟨u "group", u "g1", u "group:g1:member"⟩, ⟨u "group", u "g2", u "group:g2:member"⟩], u "group:g1:member"⟩,
  ⟨"stripped tail alone does not match",
    [⟨u "group", u "g1", u "group:g1:member"⟩], u "member"⟩,
  ⟨"same external id on two resources",
    [⟨u "group", u "g1", u "admin"⟩, ⟨u "group", u "g2", u "admin"⟩, ⟨u "group", u "g3", u "member"⟩], u "admin"⟩,
  ⟨"same external id on different resource types",
    [⟨u "group", u "x", u "admin"⟩, ⟨u "role", u "x", u "admin"⟩], u "admin"⟩
]

/-! ## sync -/

inductive SOp where
  | startNew (id : String) (t : Sync.SyncType)
  | write
  | endSync
  | resume (id : String)
  | latestFinished (filter : Option Sync.SyncType)

def syncTypeStr : Sync.SyncType → Except String String
  | .full => pure "full"
  | .partialSync => pure "partial"
  | .resourcesOnly => pure "resources_only"
  | .unspecified => throw "SyncType.unspecified has no schema string"

/-- Run one op on the model; `now` is the op index (time is not compared). -/
def SOp.step (s : Sync.FileState) (now : Nat) : SOp → Except String (Sync.FileState × J)
  | .startNew id t => do
    let ty ← syncTypeStr t
    let res := fun r => J.obj [("op", .str "start_new"), ("id", .str id), ("type", .str ty), ("result", .str r)]
    match Sync.startNewSync s id t now with
    | .ok s' => pure (s', res "ok")
    | .syncInProgress => pure (s, res "sync_in_progress")
  | .write =>
    let res := fun r => J.obj [("op", .str "write"), ("result", .str r)]
    match Sync.writeGate s with
    | .allowed => pure (Sync.recordWrite s, res "allowed")
    | .noCurrentSync => pure (s, res "no_current_sync")
    | .engineSealed => pure (s, res "engine_sealed")
  | .endSync =>
    let res := fun r => J.obj [("op", .str "end"), ("result", .str r)]
    match Sync.endSync s now with
    | .ok s' => pure (s', res "ok")
    | .noCurrentSync => pure (s, res "no_current_sync")
  | .resume id =>
    let res := fun r => J.obj [("op", .str "resume"), ("id", .str id), ("result", .str r)]
    match Sync.resumeSync s id with
    | .ok s' => pure (s', res "ok")
    | .notFound => pure (s, res "not_found")
  | .latestFinished f => do
    let ty ← match f with
      | none => pure "any"
      | some t => syncTypeStr t
    let r := match Sync.latestFinished s f with
      | some run => run.id
      | none => "none"
    if r == "none" && (Sync.latestFinished s f).isSome then throw "sync id collides with \"none\""
    pure (s, .obj [("op", .str "latest_finished"), ("type", .str ty), ("result", .str r)])

structure SyncCase where
  name : String
  ops : List SOp

def SyncCase.toJ (c : SyncCase) : Except String J := do
  let mut s := Sync.opened none false
  let mut out : Array J := #[]
  let mut i := 0
  for op in c.ops do
    let (s', j) ← op.step s i
    s := s'
    out := out.push j
    i := i + 1
  pure <| .obj [("name", .str c.name), ("ops", .arr out.toList)]

def syncCases : List SyncCase := [
  ⟨"start then write is allowed", [.startNew "s1" .full, .write]⟩,
  ⟨"write on a fresh file has no current sync", [.write]⟩,
  ⟨"end unbinds writes", [.startNew "s1" .full, .write, .endSync, .write]⟩,
  ⟨"end then resume reopens writes", [.startNew "s1" .full, .endSync, .resume "s1", .write]⟩,
  ⟨"resume of unknown id", [.startNew "s1" .full, .resume "s2", .write]⟩,
  ⟨"resume on a fresh file", [.resume "s1", .write]⟩,
  ⟨"end with no sync", [.endSync]⟩,
  ⟨"second end after end", [.startNew "s1" .full, .endSync, .endSync]⟩,
  ⟨"latest finished full sync",
    [.startNew "s1" .full, .latestFinished none, .latestFinished (some .full), .endSync,
     .latestFinished none, .latestFinished (some .full), .latestFinished (some .partialSync),
     .latestFinished (some .resourcesOnly)]⟩,
  ⟨"latest finished partial sync",
    [.startNew "p1" .partialSync, .endSync, .latestFinished none, .latestFinished (some .partialSync),
     .latestFinished (some .full)]⟩,
  ⟨"latest finished resources only sync",
    [.startNew "r1" .resourcesOnly, .write, .endSync, .latestFinished (some .resourcesOnly),
     .latestFinished (some .full), .latestFinished none]⟩,
  ⟨"start new replaces finished sync",
    [.startNew "s1" .full, .endSync, .startNew "s2" .partialSync, .latestFinished none, .write,
     .resume "s1", .endSync, .latestFinished (some .partialSync)]⟩,
  ⟨"start while fresh refused", [.startNew "s1" .full, .startNew "s2" .full, .write]⟩,
  ⟨"resume then start replaces",
    [.startNew "s1" .full, .endSync, .resume "s1", .startNew "s2" .partialSync, .latestFinished none]⟩
]

/-! ## document -/

/-- The six families in schema order, each paired with its field name. -/
structure Families where
  keys : List J := []
  strip : List J := []
  writes : List J := []
  pages : List J := []
  bare : List J := []
  sync : List J := []

def Families.toList (f : Families) : List (String × List J) :=
  [("keys", f.keys), ("entitlement_strip", f.strip), ("writes", f.writes),
    ("pagination", f.pages), ("bare_id", f.bare), ("sync", f.sync)]

def Families.append (a b : Families) : Families :=
  ⟨a.keys ++ b.keys, a.strip ++ b.strip, a.writes ++ b.writes, a.pages ++ b.pages, a.bare ++ b.bare,
    a.sync ++ b.sync⟩

/-- `version`, `counts`, then each family, in schema order. -/
def Families.toDoc (f : Families) : J :=
  .obj ([("version", .num 1), ("counts", .obj (f.toList.map fun (n, xs) => (n, .num xs.length)))] ++
    f.toList.map fun (n, xs) => (n, .arr xs))

/-- Expected values for every input, computed by the model. -/
def render (keys : List KeyCase) (strip : List EntitlementId) (writes : List WriteCase) (pages : List PageCase)
    (bare : List BareCase) (sync : List SyncCase) : Except String Families := do
  pure ⟨← keys.mapM KeyCase.toJ, ← strip.mapM stripToJ, ← writes.mapM WriteCase.toJ,
    ← pages.mapM PageCase.toJ, ← bare.mapM BareCase.toJ, ← sync.mapM SyncCase.toJ⟩

/-- The fixed corpus. Fails if any family is empty. -/
def fixedFamilies : Except String Families := do
  let f ← render keyCases stripCases writeCases pageCases bareCases syncCases
  for (n, xs) in f.toList do
    if xs.isEmpty then throw s!"family {n} is empty"
  pure f

def document : Except String J := Families.toDoc <$> fixedFamilies

end Oracle
