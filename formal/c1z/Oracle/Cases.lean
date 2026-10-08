import C1z.Identity
import C1z.Store
import C1z.Paginate
import C1z.Result
import C1z.Sync
import C1z.Records
import C1z.Index
import C1z.GrantLookup
import C1z.Stream
import C1z.Digest
import C1z.Views
import C1z.Container
import Oracle.Json

/-!
# Oracle case generators

Each family below is a hand-chosen list of inputs; every expected value
is computed by running the model (`C1z.Identity`, `C1z.Store`,
`C1z.Paginate`, `C1z.Result`, `C1z.Sync`, `C1z.Records`, `C1z.Index`,
`C1z.GrantLookup`, `C1z.Stream`, `C1z.Digest`, `C1z.Views`,
`C1z.Container`). Nothing here writes an
expected output by hand. The schema is `ORACLE_SCHEMA.md`.
`Oracle.Random` and `Oracle.Request` pass their inputs through the same
`render`.

Deliberately not generated, so proved in Lean but never replayed
against the Pebble engine:

- `visible = false` rows outside `grant_list`: `pagination` always uses
  `fun _ => true`; only the `grant_list` principal-type filter exercises
  hidden-row skipping inside `Paginate.page`;
- the five grant families cover one `PutGrants` per `grant_list` and
  `grant_bare_id` case, never more than one default page, and no
  principal or entitlement records beyond `grant_bare_id`'s, `stream`'s,
  and `digest`'s entitlement rows; `IndexedGrants` is only replayed from
  an empty store;
- failure injection: I/O, decode, and every `Result.ListError` arm other
  than `cancelled` (which `stream` replays); `Paginate.checkCursor` and
  invalid page tokens;
- `stream`: `break_after = 0` (rejected), an `ent_ext` that matches two
  or more entitlement rows or matches none while containing a colon
  (rejected: the engine's bare-id resolution is not modeled), writes
  during iteration, and resources or entitlements written by grants;
- `digest`: hash values (`digestHash` is constant; only `found`, `count`,
  `width`, and content equality are emitted), bucket leaves, and
  invalidation of partitions whose content cannot change; distinct
  non-empty partitions never have equal content, because
  `Digest.GrantContent` includes the entitlement, so `equal_content`
  pairs are always two zero-grant partitions;
- malformed identities: entitlements and grants with an empty owner
  component (`EntitlementId.WellFormed`, `GrantId.WellFormed` fail),
  which the engine rejects; the generator refuses to emit them;
- page sizes above `Paginate.maxPageSize` (the other `clampPageSize`
  branch) and corpora longer than one default page;
- time: `discovered_at` and `endedAt` values; `startedAt`, the 7-day
  `latestUnfinished` fallback, and `resolveActiveSync` (without an
  annotation) appear only in `reopen`, at a fixed `reopenNow`, with
  `age_sync` days from `{1, 6, 8, 30}` in the random corpus (never the
  7-day boundary, which a wall-clock consumer cannot hit exactly);
- `SyncType.unspecified`, which has no schema string;
- multi-file behavior (compaction, `CloneSync`); `reopen` covers one
  file closed and reopened, and `StartNewSync` wiping grants written by
  an earlier sync;
- `views`: one `PutGrants` and one `PutExpandedGrantRecords` per case,
  no deletes, and bulk-read ids that are empty;
  `stream_grants_for_entitlement` only for an entitlement that is the
  sole row with its `ext`; field agreement beyond identity and
  `external_id`;
- `container`: the `TAR` and `TAR_ZSTD` encodings
  (`unsupportedEncoding`), damage other than the five `Container.Damage`
  kinds, a `save_reopen` with no file on disk (`Close` writes only a
  dirty store, so the first `save_reopen` needs a sync record), writes
  after a read-only open, and ops after an `open_error`; `write`, as in
  `reopen`, stores no grant, so the round trip is shown by a `put`.
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

/-! ## shared grant and entitlement helpers -/

/-- The bytes as a string when they are valid UTF-8 and every element is
below 256. -/
def utf8? (bs : Bytes) : Option String :=
  if bs.all (· < 256) then String.fromUTF8? (ByteArray.mk (bs.map UInt8.ofNat).toArray) else none

def needUtf8 (ctx : String) (bs : Bytes) : Except String Unit :=
  if (utf8? bs).isSome then pure () else throw s!"{ctx}: not valid UTF-8"

/-- `EntitlementId.WellFormed` and UTF-8 components. -/
def checkEnt (ctx : String) (e : EntitlementId) : Except String Unit := do
  unless decide e.WellFormed do throw s!"{ctx}: entitlement not well formed"
  for b in [e.rt, e.rid, e.ext] do needUtf8 ctx b

/-- `GrantId.WellFormed` and UTF-8 components. -/
def checkGrantId (ctx : String) (g : GrantId) : Except String Unit := do
  unless decide g.WellFormed do throw s!"{ctx}: grant not well formed"
  for b in [g.ent.rt, g.ent.rid, g.ent.ext, g.prt, g.prid] do needUtf8 ctx b

def checkGrant (ctx : String) (r : GrantRecord) : Except String Unit := do
  checkGrantId ctx r.id
  needUtf8 ctx r.externalId

def checkDistinctEnts (ctx : String) (es : List EntitlementId) : Except String Unit := do
  if es.eraseDups.length != es.length then throw s!"{ctx}: repeated entitlement identity"

def grantIdFields (g : GrantId) : Except String (List (String × J)) := do
  pure [("ent", ← entJ g.ent), ("prt", ← hexJ g.prt), ("prid", ← hexJ g.prid)]

def grantJ (r : GrantRecord) : Except String J := do
  pure <| .obj ((← grantIdFields r.id) ++ [("ext_id", ← hexJ r.externalId)])

def grantsJ (rs : List GrantRecord) : Except String J := do pure (.arr (← rs.mapM grantJ))

/-- Grant record from identity parts and an external id. -/
def gr (rt rid ext prt prid : String) (extId : String := "") : GrantRecord :=
  ⟨⟨⟨u rt, u rid, u ext⟩, u prt, u prid⟩, u extId⟩

def en (rt rid ext : String) : EntitlementId := ⟨u rt, u rid, u ext⟩

/-! ## grant_writes -/

inductive GWOp where
  | put (batch : List GrantRecord)
  | delete (g : GrantId)

structure GrantWriteCase where
  name : String
  ops : List GWOp

def GWOp.apply (s : GrantStore) : GWOp → GrantStore
  | .put b => s.putGrants b
  | .delete g => s.deleteGrant g

def GWOp.toJ (ctx : String) : GWOp → Except String J
  | .put b => do
    for r in b do checkGrant ctx r
    pure <| .obj [("op", .str "put"), ("batch", ← grantsJ b)]
  | .delete g => do
    checkGrantId ctx g
    pure <| .obj ([("op", .str "delete")] ++ (← grantIdFields g))

def GrantWriteCase.toJ (c : GrantWriteCase) : Except String J := do
  let ctx := s!"grant_writes case {c.name}"
  let ops ← c.ops.mapM (GWOp.toJ ctx)
  let s := c.ops.foldl GWOp.apply Store.empty
  pure <| .obj [("name", .str c.name), ("ops", .arr ops), ("final", ← grantsJ s.allGrants)]

/-- Stripped `group:g1:member`, opaque `admin` on `group/g1`, stripped on `group/g2`. -/
def grantWriteCases : List GrantWriteCase := [
  ⟨"single grant", [.put [gr "group" "g1" "group:g1:member" "user" "u1"]]⟩,
  ⟨"same identity different external id collapses",
    [.put [gr "group" "g1" "group:g1:member" "user" "u1" "x"], .put [gr "group" "g1" "group:g1:member" "user" "u1" "y"]]⟩,
  ⟨"same identity twice in one batch last wins",
    [.put [gr "group" "g1" "group:g1:member" "user" "u1" "x", gr "group" "g1" "group:g1:member" "user" "u1" ""]]⟩,
  ⟨"empty external id replaces custom one",
    [.put [gr "group" "g1" "admin" "user" "u1" "custom"], .put [gr "group" "g1" "admin" "user" "u1"]]⟩,
  ⟨"same external id on two structures",
    [.put [gr "group" "g1" "group:g1:member" "user" "u1" "x", gr "group" "g2" "group:g2:member" "user" "u1" "x"]]⟩,
  ⟨"same external id on two principals",
    [.put [gr "group" "g1" "admin" "user" "u1" "x"], .put [gr "group" "g1" "admin" "user" "u2" "x"]]⟩,
  ⟨"dangling grant with no entitlement or principal record",
    [.put [gr "role" "nowhere" "role:nowhere:owner" "service" "ghost" "dangling"]]⟩,
  ⟨"delete by identity then re-put",
    [.put [gr "group" "g1" "group:g1:member" "user" "u1" "x"],
     .delete ⟨en "group" "g1" "group:g1:member", u "user", u "u1"⟩,
     .put [gr "group" "g1" "group:g1:member" "user" "u1" "z"]]⟩,
  ⟨"delete of absent identity is a no-op",
    [.put [gr "group" "g1" "group:g1:member" "user" "u1"],
     .delete ⟨en "group" "g1" "group:g1:member", u "user", u "u2"⟩,
     .delete ⟨en "group" "g1" "member", u "user", u "u1"⟩]⟩,
  ⟨"delete on empty store", [.delete ⟨en "group" "g1" "admin", u "user", u "u1"⟩]⟩,
  ⟨"delete leaves other principal types",
    [.put [gr "group" "g1" "admin" "user" "u1", gr "group" "g1" "admin" "service" "u1"],
     .delete ⟨en "group" "g1" "admin", u "user", u "u1"⟩]⟩,
  ⟨"stripped and opaque entitlement with the same tail are distinct",
    [.put [gr "group" "g1" "group:g1:member" "user" "u1", gr "group" "g1" "member" "user" "u1"]]⟩,
  ⟨"external id equal to public id",
    [.put [gr "group" "g1" "group:g1:member" "user" "u1" "group:g1:member:user:u1"]]⟩,
  ⟨"key order differs from insertion order",
    [.put [gr "équipe" "日本" "équipe:日本:membre" "usuário" "é", gr "group" "g2" "admin" "user" "u1",
       gr "group" "g1" "admin" "user" "u2", gr "group" "g1" "admin" "group" "g9",
       gr "group" "g1" "admin" "user" "u1", gr "app:x" "c:d" "app:x:c:d:e" "p:q" "r:s"]]⟩
]

/-! ## entitlement_writes -/

inductive EWOp where
  | put (batch : List (EntitlementId × String))
  | delete (e : EntitlementId)

structure EntWriteCase where
  name : String
  ops : List EWOp

def EWOp.ents : EWOp → List EntitlementId
  | .put b => b.map (·.1)
  | .delete e => [e]

def EWOp.apply (s : EntitlementStore) : EWOp → EntitlementStore
  | .put b => s.putEntitlements (b.map fun (e, v) => (e, u v))
  | .delete e => s.deleteEntitlement e

def entValueJ (e : EntitlementId) (v : String) : Except String J := do
  pure <| .obj [("rt", ← hexJ e.rt), ("rid", ← hexJ e.rid), ("ext", ← hexJ e.ext), ("value", .str v)]

def EWOp.toJ (ctx : String) : EWOp → Except String J
  | .put b => do
    for (e, _) in b do checkEnt ctx e
    pure <| .obj [("op", .str "put"), ("batch", .arr (← b.mapM fun (e, v) => entValueJ e v))]
  | .delete e => do
    checkEnt ctx e
    pure <| .obj [("op", .str "delete"), ("rt", ← hexJ e.rt), ("rid", ← hexJ e.rid), ("ext", ← hexJ e.ext)]

def EntWriteCase.toJ (c : EntWriteCase) : Except String J := do
  let ctx := s!"entitlement_writes case {c.name}"
  let ops ← c.ops.mapM (EWOp.toJ ctx)
  let s := c.ops.foldl EWOp.apply Store.empty
  let tbl := (c.ops.flatMap EWOp.ents).map fun e => (e.key, e)
  let final ← s.entries.mapM fun (k, v) => do
    let e ← lookupKey tbl k
    let some name := utf8? v | throw s!"{ctx}: value is not UTF-8"
    entValueJ e name
  pure <| .obj [("name", .str c.name), ("ops", .arr ops), ("final", .arr final)]

def entWriteCases : List EntWriteCase := [
  ⟨"single put", [.put [(en "group" "g1" "group:g1:member", "v1")]]⟩,
  ⟨"same external id on two resources", [.put [(en "group" "g1" "admin", "v1"), (en "group" "g2" "admin", "w1")]]⟩,
  ⟨"same external id on two resource types", [.put [(en "group" "x" "admin", "v1"), (en "role" "x" "admin", "w1")]]⟩,
  ⟨"last write wins across calls", [.put [(en "group" "g1" "admin", "v1")], .put [(en "group" "g1" "admin", "v2")]]⟩,
  ⟨"last occurrence wins in one batch",
    [.put [(en "group" "g1" "admin", "v1"), (en "group" "g1" "member", "w1"), (en "group" "g1" "admin", "v2")]]⟩,
  ⟨"stripped and opaque with the same tail are two rows",
    [.put [(en "group" "g1" "group:g1:member", "stripped"), (en "group" "g1" "member", "opaque")]]⟩,
  ⟨"delete then re-put",
    [.put [(en "group" "g1" "admin", "v1")], .delete (en "group" "g1" "admin"), .put [(en "group" "g1" "admin", "v3")]]⟩,
  ⟨"delete of absent identity is a no-op",
    [.put [(en "group" "g1" "admin", "v1")], .delete (en "group" "g2" "admin"), .delete (en "group" "g1" "group:g1:admin")]⟩,
  ⟨"delete on empty store", [.delete (en "group" "g1" "admin")]⟩,
  ⟨"key order differs from insertion order",
    [.put [(en "équipe" "日本" "équipe:日本:membre", "e-acute"), (en "group" "g2" "admin", "g2"),
       (en "app:x" "c:d" "app:x:c:d:e", "colon"), (en "group" "g1" "admin", "g1-admin"),
       (en "group" "g1" "group:g1:member", "g1-member")]]⟩
]

/-! ## grant_list -/

structure GrantListCase where
  name : String
  grants : List GrantRecord
  ent : EntitlementId
  prt : Option Bytes := none
  prid : Option Bytes := none
  pageSize : Nat

/-- `pagesWithNext` with a visibility predicate. Cross-checked against `traverse`. -/
def pagesWithNextV (visible : Bytes → Bool) (ks : List Bytes) (limit : Nat) :
    Except String (List (List Bytes × Bool)) := do
  let rec go : Nat → Option Bytes → List (List Bytes × Bool)
    | 0, _ => []
    | fuel + 1, cursor =>
      let p := Paginate.page visible ks cursor limit
      match p.next with
      | none => [(p.items, false)]
      | some tok => (p.items, true) :: go fuel (some tok)
  let fuel := ks.length + 1
  let ps := go fuel none
  if ps.map (·.1) != Paginate.traverse visible ks limit fuel none then
    throw "page chain disagrees with Paginate.traverse"
  pure ps

def GrantListCase.toJ (c : GrantListCase) : Except String J := do
  let ctx := s!"grant_list case {c.name}"
  for r in c.grants do checkGrant ctx r
  checkEnt ctx c.ent
  for p in [c.prt, c.prid] do
    if let some b := p then
      if b.isEmpty then throw s!"{ctx}: empty principal component"
      needUtf8 ctx b
  let s := GrantStore.putGrants Store.empty c.grants
  let pages : List (List GrantRecord × Bool) ← match c.prt, c.prid with
    | none, some _ => throw s!"{ctx}: prid without prt"
    | some prt, some prid => pure [((GrantStore.grantForEntitlementPrincipal s c.ent prt prid).toList, false)]
    | prt, none => do
      let tbl := (GrantStore.grantsForEntitlement s c.ent).map fun r => (r.key, r)
      let visible : Bytes → Bool := match prt with
        | some t => fun k => match tbl.find? (·.1 == k) with
          | some (_, r) => r.id.prt == t
          | none => false
        | none => fun _ => true
      let ps ← pagesWithNextV visible (tbl.map (·.1)) (Paginate.clampPageSize c.pageSize)
      let ps ← ps.mapM fun (items, hn) => do pure (← items.mapM (lookupKey tbl), hn)
      let want := match prt with
        | some t => GrantStore.grantsForEntitlementByPrincipalType s c.ent t
        | none => GrantStore.grantsForEntitlement s c.ent
      if (ps.flatMap (·.1)).map (·.key) != want.map (·.key) then throw s!"{ctx}: pages disagree with the model listing"
      pure ps
  let opt := fun (k : String) (b : Option Bytes) => do
    match b with
    | some v => pure [(k, ← hexJ v)]
    | none => pure ([] : List (String × J))
  let query := J.obj ([("ent", ← entJ c.ent)] ++ (← opt "prt" c.prt) ++ (← opt "prid" c.prid))
  let pagesJ ← pages.mapM fun (rs, hn) => do pure (J.obj [("grants", ← grantsJ rs), ("has_next", .bool hn)])
  pure <| .obj [("name", .str c.name), ("grants", ← grantsJ c.grants), ("query", query),
    ("page_size", .num c.pageSize), ("pages", .arr pagesJ)]

/-- Grants on stripped `group:g1:member` (`eA`), opaque `admin` (`eB`),
stripped `group:g2:member` (`eC`), and opaque `member` on `group/g1`
(`eD`, the same tail as `eA`). Insertion order is unsorted. -/
def listGrants : List GrantRecord := [
  gr "group" "g1" "group:g1:member" "user" "u2",
  gr "group" "g2" "group:g2:member" "user" "u1",
  gr "group" "g1" "group:g1:member" "group" "g9" "nested",
  gr "group" "g1" "admin" "user" "u1" "custom",
  gr "group" "g1" "group:g1:member" "user" "u1",
  gr "group" "g1" "member" "user" "u1",
  gr "group" "g1" "group:g1:member" "service" "s1" "group:g1:member:service:s1"
]

def eA : EntitlementId := en "group" "g1" "group:g1:member"

/-- Two `group` principals sort before one `user` principal. -/
def trailingGrants : List GrantRecord := [
  gr "group" "g1" "group:g1:member" "user" "u1",
  gr "group" "g1" "group:g1:member" "group" "g8",
  gr "group" "g1" "group:g1:member" "group" "g9"
]

def grantListCases : List GrantListCase := [
  { name := "entitlement grants default page size", grants := listGrants, ent := eA, pageSize := 0 },
  { name := "entitlement grants page size one", grants := listGrants, ent := eA, pageSize := 1 },
  { name := "entitlement grants page size two", grants := listGrants, ent := eA, pageSize := 2 },
  { name := "entitlement grants page size four", grants := listGrants, ent := eA, pageSize := 4 },
  { name := "two entitlements disjoint", grants := listGrants, ent := en "group" "g2" "group:g2:member", pageSize := 2 },
  { name := "opaque entitlement", grants := listGrants, ent := en "group" "g1" "admin", pageSize := 2 },
  { name := "opaque entitlement with the stripped tail is separate", grants := listGrants,
    ent := en "group" "g1" "member", pageSize := 0 },
  { name := "unknown entitlement", grants := listGrants, ent := en "group" "g3" "group:g3:member", pageSize := 2 },
  { name := "no grants at all", grants := [], ent := eA, pageSize := 0 },
  { name := "principal type filter", grants := listGrants, ent := eA, prt := some (u "user"), pageSize := 0 },
  { name := "principal type filter page size one", grants := listGrants, ent := eA, prt := some (u "user"),
    pageSize := 1 },
  { name := "principal type filter no match", grants := listGrants, ent := eA, prt := some (u "role"), pageSize := 2 },
  { name := "principal type filter trailing empty page", grants := trailingGrants, ent := eA,
    prt := some (u "group"), pageSize := 2 },
  { name := "principal type filter last match ends the scan", grants := trailingGrants, ent := eA,
    prt := some (u "user"), pageSize := 1 },
  { name := "point lookup hit", grants := listGrants, ent := eA, prt := some (u "user"), prid := some (u "u1"),
    pageSize := 2 },
  { name := "point lookup carries custom external id", grants := listGrants, ent := en "group" "g1" "admin",
    prt := some (u "user"), prid := some (u "u1"), pageSize := 0 },
  { name := "point lookup miss on principal id", grants := listGrants, ent := eA, prt := some (u "user"),
    prid := some (u "u9"), pageSize := 2 },
  { name := "point lookup miss on principal type", grants := listGrants, ent := eA, prt := some (u "service"),
    prid := some (u "u1"), pageSize := 1 }
]

/-! ## grants_by_principal -/

inductive POp where
  | put (batch : List GrantRecord)
  | putDeferred (batch : List GrantRecord)
  | delete (g : GrantId)
  | read (prt prid : Bytes)
  | endSync

structure ByPrincipalCase where
  name : String
  ops : List POp

def POp.isRead : POp → Bool
  | .read _ _ => true
  | _ => false

def POp.isEnd : POp → Bool
  | .endSync => true
  | _ => false

def POp.step (ctx : String) (x : IndexedGrants) : POp → Except String (IndexedGrants × J)
  | .put b => do
    for r in b do checkGrant ctx r
    pure (x.putGrants b, .obj [("op", .str "put"), ("batch", ← grantsJ b)])
  | .putDeferred b => do
    for r in b do checkGrant ctx r
    pure (x.putGrantsDeferred b, .obj [("op", .str "put_deferred"), ("batch", ← grantsJ b)])
  | .delete g => do
    checkGrantId ctx g
    pure (x.deleteGrant g, .obj ([("op", .str "delete")] ++ (← grantIdFields g)))
  | .read prt prid => do
    if prt.isEmpty || prid.isEmpty then throw s!"{ctx}: empty principal component"
    needUtf8 ctx prt
    needUtf8 ctx prid
    pure (x, .obj [("op", .str "read"), ("prt", ← hexJ prt), ("prid", ← hexJ prid),
      ("grants", ← grantsJ (x.grantsForPrincipal prt prid))])
  | .endSync => pure (x.endSyncRebuild, .obj [("op", .str "end_sync")])

def ByPrincipalCase.toJ (c : ByPrincipalCase) : Except String J := do
  let ctx := s!"grants_by_principal case {c.name}"
  match c.ops.findIdx? POp.isEnd with
  | none => pure ()
  | some i =>
    let after := c.ops.drop (i + 1)
    unless after.length ≤ 1 && after.all POp.isRead do
      throw s!"{ctx}: end_sync must be last, optionally followed by one read"
  let mut x := IndexedGrants.empty
  let mut out : Array J := #[]
  for op in c.ops do
    let (x', j) ← op.step ctx x
    x := x'
    out := out.push j
  pure <| .obj [("name", .str c.name), ("ops", .arr out.toList)]

def pg1 : GrantRecord := gr "group" "g1" "group:g1:member" "user" "u1"
def pg2 : GrantRecord := gr "group" "g1" "group:g1:member" "user" "u2"

def byPrincipalCases : List ByPrincipalCase := [
  ⟨"plain put is visible", [.put [pg1], .read (u "user") (u "u1")]⟩,
  ⟨"deferred write invisible until end sync",
    [.putDeferred [pg1], .read (u "user") (u "u1"), .endSync, .read (u "user") (u "u1")]⟩,
  ⟨"deferred overwrite of indexed identity stays visible",
    [.put [pg1], .putDeferred [{ pg1 with externalId := u "x2" }], .read (u "user") (u "u1")]⟩,
  ⟨"plain put after deferred put is visible", [.putDeferred [pg1], .put [pg1], .read (u "user") (u "u1")]⟩,
  ⟨"delete then deferred put is invisible",
    [.put [pg1], .delete pg1.id, .putDeferred [pg1], .read (u "user") (u "u1")]⟩,
  ⟨"two principals only one deferred",
    [.put [pg1], .putDeferred [pg2], .read (u "user") (u "u1"), .read (u "user") (u "u2"), .endSync,
     .read (u "user") (u "u2")]⟩,
  ⟨"principal grants across entitlements in key order",
    [.put [gr "group" "g2" "group:g2:member" "user" "u1", gr "group" "g1" "admin" "user" "u1" "c",
       pg1, pg2], .read (u "user") (u "u1")]⟩,
  ⟨"same principal id other principal type excluded",
    [.put [pg1, gr "group" "g1" "group:g1:member" "service" "u1"], .read (u "user") (u "u1"),
     .read (u "service") (u "u1")]⟩,
  ⟨"delete removes the index entry", [.put [pg1, pg2], .delete pg1.id, .read (u "user") (u "u1"),
     .read (u "user") (u "u2")]⟩,
  ⟨"end sync rebuild after mixed writes",
    [.put [pg1], .putDeferred [pg2, gr "group" "g2" "group:g2:member" "user" "u1"], .read (u "user") (u "u1"),
     .endSync, .read (u "user") (u "u1")]⟩,
  ⟨"read of unknown principal", [.put [pg1], .read (u "user") (u "u9")]⟩
]

/-! ## grant_bare_id -/

structure GrantBareCase where
  name : String
  ents : List EntitlementId
  grants : List GrantRecord
  lookup : Bytes

def GrantBareCase.toJ (c : GrantBareCase) : Except String J := do
  let ctx := s!"grant_bare_id case {c.name}"
  for e in c.ents do checkEnt ctx e
  checkDistinctEnts ctx c.ents
  for r in c.grants do checkGrant ctx r
  needUtf8 ctx c.lookup
  let s := GrantStore.putGrants Store.empty c.grants
  let es := EntitlementStore.putEntitlements Store.empty (c.ents.map fun e => (e, []))
  let base := [("name", J.str c.name), ("entitlements", .arr (← c.ents.mapM entJ)), ("grants", ← grantsJ c.grants),
    ("lookup", ← hexJ c.lookup)]
  match GrantLookup.resolve s es c.lookup with
  | .notFound => pure <| .obj (base ++ [("expected", .str "not_found")])
  | .found r => pure <| .obj (base ++ [("expected", .str "found"), ("found", ← grantJ r)])
  | .ambiguous => pure <| .obj (base ++ [("expected", .str "ambiguous")])

def bPublic : GrantRecord := pg1
def bOpaque : GrantRecord := gr "group" "g1" "member" "user" "u1"
def bCustom : GrantRecord := gr "group" "g1" "group:g1:member" "user" "u1" "c"

def grantBareCases : List GrantBareCase := [
  ⟨"found by public id with empty external id", [], [bPublic], u "group:g1:member:user:u1"⟩,
  ⟨"no grants", [], [], u "group:g1:member:user:u1"⟩,
  ⟨"same stored external id on two structures",
    [], [gr "group" "g1" "group:g1:member" "user" "u1" "x", gr "group" "g2" "group:g2:member" "user" "u1" "x"], u "x"⟩,
  ⟨"opaque entitlement unreachable without row", [], [bOpaque], u "member:user:u1"⟩,
  ⟨"opaque entitlement found with row", [en "group" "g1" "member"], [bOpaque], u "member:user:u1"⟩,
  ⟨"opaque entitlement row on another resource does not help", [en "group" "g2" "member"], [bOpaque],
    u "member:user:u1"⟩,
  ⟨"custom external id hides public id", [], [bCustom], u "group:g1:member:user:u1"⟩,
  ⟨"custom external id found by custom id", [], [bCustom], u "c"⟩,
  ⟨"external id equal to public id found",
    [], [gr "group" "g1" "group:g1:member" "user" "u1" "group:g1:member:user:u1"], u "group:g1:member:user:u1"⟩,
  ⟨"public id hit masks stored external id on another grant",
    [], [bPublic, gr "group" "g2" "group:g2:member" "user" "u2" "group:g1:member:user:u1"], u "group:g1:member:user:u1"⟩,
  ⟨"public id collision across structures",
    [en "a" "c" "a:b:x"], [gr "a" "b" "a:b:x" "p" "q", gr "a" "c" "a:b:x" "p" "q"], u "a:b:x:p:q"⟩,
  ⟨"single colon lookup is scan only", [], [gr "group" "g1" "admin" "user" "u1" "x:y"], u "x:y"⟩,
  ⟨"single colon lookup misses public ids", [], [gr "a" "b" "a:b:x" "p" "q"], u "x:p"⟩,
  ⟨"empty lookup with one empty external id", [], [bPublic, gr "group" "g2" "admin" "user" "u1" "c"], []⟩,
  ⟨"empty lookup with two empty external ids", [], [bPublic, pg2], []⟩,
  ⟨"more than 64 colons is ambiguous", [], [gr "group" "g1" "admin" "user" "u1" (String.join (List.replicate 65 ":"))],
    u (String.join (List.replicate 65 ":"))⟩
]

/-! ## stream -/

inductive StreamKind where
  | grants
  | resources
  | entitlements
  deriving DecidableEq

def StreamKind.str : StreamKind → String
  | .grants => "grants"
  | .resources => "resources"
  | .entitlements => "entitlements"

/-- `entExt`, `prt`, `prid` apply to `grants`; `rt` to `resources`. -/
structure StreamCase where
  name : String
  kind : StreamKind
  ents : List EntitlementId := []
  resources : List ResourceId := []
  grants : List GrantRecord := []
  deferred : List GrantRecord := []
  entExt : Option Bytes := none
  prt : Option Bytes := none
  prid : Option Bytes := none
  rt : Option Bytes := none
  cancelAfter : Option Nat := none
  breakAfter : Option Nat := none

def optNumJ : Option Nat → J
  | some n => .num n
  | none => .null

def resJ (r : ResourceId) : Except String J := do
  pure <| .obj [("rt", ← hexJ r.rt), ("rid", ← hexJ r.rid)]

def yieldsJ {α : Type} (ys : List (Result.Yield α)) (f : α → Except String J) : Except String (List J) :=
  ys.mapM fun
    | .record a => f a
    | .error .cancelled => pure (J.obj [("error", .str "cancelled")])
    | .error _ => throw "stream yielded an error other than cancelled"

/-- The rows a grant stream scans for the case's filter (`ORACLE_SCHEMA.md`,
"stream"). An `ent_ext` matching two or more entitlement rows, or none
while containing a colon, is rejected. -/
def StreamCase.grantRows (c : StreamCase) (ctx : String) (x : IndexedGrants) : Except String (List GrantRecord) :=
  match c.entExt with
  | some ext =>
    match c.ents.filter (·.ext == ext) with
    | [e] => pure (Stream.grantRows x (.entitlement e))
    | [] => if ext.contains 0x3a then throw s!"{ctx}: ent_ext matches no entitlement and contains a colon"
            else pure []
    | _ => throw s!"{ctx}: ent_ext matches more than one entitlement"
  | none =>
    match c.prt, c.prid with
    | some t, none => pure (Stream.grantRows x (.principalType t))
    | _, _ => pure (Stream.grantRows x .primary)

def StreamCase.toJ (c : StreamCase) : Except String J := do
  let ctx := s!"stream case {c.name}"
  for e in c.ents do checkEnt ctx e
  checkDistinctEnts ctx c.ents
  for r in c.resources do
    if r.rt.isEmpty || r.rid.isEmpty then throw s!"{ctx}: empty resource component"
    needUtf8 ctx r.rt
    needUtf8 ctx r.rid
  if (c.resources.map (·.key)).eraseDups.length != c.resources.length then throw s!"{ctx}: repeated resource"
  for r in c.grants ++ c.deferred do checkGrant ctx r
  for b in [c.entExt, c.prt, c.prid, c.rt] do
    if let some v := b then
      if v.isEmpty then throw s!"{ctx}: empty filter component"
      needUtf8 ctx v
  if c.breakAfter == some 0 then throw s!"{ctx}: break_after must be at least 1"
  let consumer : Stream.Consumer := { cancelAfter := c.cancelAfter, breakAfter := c.breakAfter }
  let opt := fun (k : String) (b : Option Bytes) => do
    match b with
    | some v => pure [(k, ← hexJ v)]
    | none => pure ([] : List (String × J))
  let (filter, yields) ← match c.kind with
    | .grants => do
      if !c.resources.isEmpty || c.rt.isSome then throw s!"{ctx}: resources or rt on a grants stream"
      let x := (IndexedGrants.empty.putGrants c.grants).putGrantsDeferred c.deferred
      let rows ← c.grantRows ctx x
      let keep := fun (r : GrantRecord) => c.prt.all (· == r.id.prt) && c.prid.all (· == r.id.prid)
      let f := J.obj ((← opt "ent_ext" c.entExt) ++ (← opt "prt" c.prt) ++ (← opt "prid" c.prid))
      let ys ← yieldsJ (Stream.run rows keep consumer) fun r => do pure (J.obj [("grant", ← grantJ r)])
      pure (f, ys)
    | .resources => do
      if !c.grants.isEmpty || !c.deferred.isEmpty || c.entExt.isSome || c.prt.isSome || c.prid.isSome then
        throw s!"{ctx}: grant rows or grant filter on a resources stream"
      let tbl := c.resources.map fun r => (r.key, r)
      let rows ← (sortKeys (tbl.map (·.1))).mapM (lookupKey tbl)
      let keep := fun (r : ResourceId) => c.rt.all (· == r.rt)
      let ys ← yieldsJ (Stream.run rows keep consumer) fun r => do pure (J.obj [("resource", ← resJ r)])
      pure (J.obj (← opt "rt" c.rt), ys)
    | .entitlements => do
      if !c.resources.isEmpty || !c.grants.isEmpty || !c.deferred.isEmpty || c.entExt.isSome || c.prt.isSome ||
          c.prid.isSome || c.rt.isSome then
        throw s!"{ctx}: rows or filter other than entitlements on an entitlements stream"
      let tbl := c.ents.map fun e => (e.key, e)
      let rows ← (sortKeys (tbl.map (·.1))).mapM (lookupKey tbl)
      let ys ← yieldsJ (Stream.run rows (fun _ => true) consumer) fun e => do
        pure (J.obj [("entitlement", ← entJ e)])
      pure (J.obj [], ys)
  pure <| .obj [("name", .str c.name), ("kind", .str c.kind.str), ("entitlements", .arr (← c.ents.mapM entJ)),
    ("resources", .arr (← c.resources.mapM resJ)), ("grants", ← grantsJ c.grants), ("deferred", ← grantsJ c.deferred),
    ("filter", filter),
    ("consumer", .obj [("cancel_after", optNumJ c.cancelAfter), ("break_after", optNumJ c.breakAfter)]),
    ("yields", .arr yields)]

/-- Stripped `group:g1:member` (`eA`) and opaque `admin` on `group/g1`,
with user, group, and service principals. -/
def streamGrants : List GrantRecord := [
  gr "group" "g1" "group:g1:member" "user" "u2",
  gr "group" "g1" "admin" "user" "u1" "custom",
  gr "group" "g1" "group:g1:member" "user" "u1",
  gr "group" "g1" "group:g1:member" "service" "s1",
  gr "group" "g1" "admin" "group" "g9"
]

def streamEnts : List EntitlementId := [eA, en "group" "g1" "admin"]

def streamResources : List ResourceId := [⟨u "user", u "u2"⟩, ⟨u "group", u "g1"⟩, ⟨u "user", u "u1"⟩]

def cancelled0 : Option Nat := some 0

def streamCases : List StreamCase := [
  { name := "grants patient unfiltered", kind := .grants, ents := streamEnts, grants := streamGrants },
  { name := "grants principal type and id post-filter", kind := .grants, ents := streamEnts, grants := streamGrants,
    prt := some (u "user"), prid := some (u "u1") },
  { name := "grants principal id alone is a primary post-filter", kind := .grants, ents := streamEnts,
    grants := streamGrants, prid := some (u "u1") },
  { name := "grants entitlement filter stripped", kind := .grants, ents := streamEnts, grants := streamGrants,
    entExt := some (u "group:g1:member") },
  { name := "grants entitlement filter opaque", kind := .grants, ents := streamEnts, grants := streamGrants,
    entExt := some (u "admin") },
  { name := "grants entitlement filter with principal type post-filter", kind := .grants, ents := streamEnts,
    grants := streamGrants, entExt := some (u "group:g1:member"), prt := some (u "user") },
  { name := "grants entitlement filter unknown without colon is empty", kind := .grants, ents := streamEnts,
    grants := streamGrants, entExt := some (u "owner") },
  { name := "grants principal type walks the index in index order", kind := .grants, ents := streamEnts,
    grants := streamGrants ++ [gr "group" "g2" "group:g2:member" "user" "u0"],
    deferred := [gr "group" "g2" "group:g2:member" "user" "u3"], prt := some (u "user") },
  { name := "grants cancelled over empty keyspace yields nothing", kind := .grants, cancelAfter := cancelled0 },
  { name := "grants cancelled over non-empty keyspace with no match yields the error", kind := .grants,
    ents := streamEnts, grants := streamGrants, prt := some (u "role"), prid := some (u "r1"),
    cancelAfter := cancelled0 },
  { name := "grants cancel after one with more rows", kind := .grants, ents := streamEnts, grants := streamGrants,
    cancelAfter := some 1 },
  { name := "grants cancel after one at the last row ends cleanly", kind := .grants,
    grants := [gr "group" "g1" "admin" "user" "u1"], cancelAfter := some 1 },
  { name := "grants cancel after one with only non-matching rows left", kind := .grants, ents := streamEnts,
    grants := [gr "group" "g1" "admin" "user" "u1", gr "group" "g1" "admin" "user" "u2"], prt := some (u "user"),
    prid := some (u "u1"), cancelAfter := some 1 },
  { name := "grants break after two", kind := .grants, ents := streamEnts, grants := streamGrants,
    breakAfter := some 2 },
  { name := "grants break exactly at the last match", kind := .grants, ents := streamEnts, grants := streamGrants,
    prt := some (u "user"), prid := some (u "u1"), breakAfter := some 2 },
  { name := "resources type filter patient", kind := .resources, resources := streamResources, rt := some (u "user") },
  { name := "resources type filter cancelled with other types present", kind := .resources,
    resources := [⟨u "group", u "g1"⟩, ⟨u "group", u "g2"⟩], rt := some (u "user"), cancelAfter := cancelled0 },
  { name := "resources unfiltered break after one", kind := .resources, resources := streamResources,
    breakAfter := some 1 },
  { name := "entitlements patient", kind := .entitlements,
    ents := [en "group" "g2" "admin", eA, en "équipe" "日本" "équipe:日本:membre", en "group" "g1" "admin"] },
  { name := "entitlements cancel after two", kind := .entitlements,
    ents := [en "group" "g2" "admin", eA, en "group" "g1" "admin"], cancelAfter := some 2 }
]

/-! ## digest -/

/-- A grant with the fields its content hash reads (`Digest.contentOf`). -/
structure DGrant where
  record : GrantRecord
  immutable : Bool := false
  sources : List Digest.SourceFact := []

inductive DOp where
  | seal
  | read (e : EntitlementId)
  | readGlobal
  | resume
  | put (batch : List DGrant)
  | delete (g : GrantId)

structure DigestCase where
  name : String
  ents : List EntitlementId
  grants : List DGrant
  ops : List DOp

/-- Insertion sort of source facts by key (`sortGrantSourceFacts`). -/
def sortSources (ss : List Digest.SourceFact) : List Digest.SourceFact :=
  ss.foldl (fun acc s => ins s acc) []
where
  ins (s : Digest.SourceFact) : List Digest.SourceFact → List Digest.SourceFact
    | [] => [s]
    | x :: xs => if lexLt s.key x.key then s :: x :: xs else x :: ins s xs

/-- Immutability and sources by identity, newest first; unknown
identities read `(false, [])`. -/
abbrev Facts := List (GrantId × (Bool × List Digest.SourceFact))

def Facts.fn (fs : Facts) (g : GrantId) : Bool × List Digest.SourceFact :=
  ((fs.find? (·.1 == g)).map (·.2)).getD (false, [])

/-- Record a batch; a later occurrence of an identity wins. -/
def Facts.put (fs : Facts) (b : List DGrant) : Facts :=
  b.foldl (fun acc d => (d.record.id, (d.immutable, sortSources d.sources)) :: acc) fs

/-- The hash is not modeled; only counts and presence are compared. -/
def digestHash : Digest.GrantContent → Digest.Hash := fun _ => 0

def checkDGrant (ctx : String) (d : DGrant) : Except String Unit := do
  checkGrant ctx d.record
  for s in d.sources do needUtf8 ctx s.key
  if (d.sources.map (·.key)).eraseDups.length != d.sources.length then throw s!"{ctx}: repeated source key"

def dgrantJ (d : DGrant) : Except String J := do
  let srcs ← d.sources.mapM fun s => do pure (J.obj [("key", ← hexJ s.key), ("is_direct", .bool s.isDirect)])
  pure <| .obj ((← grantIdFields d.record.id) ++ [("ext_id", ← hexJ d.record.externalId), ("immutable", .bool d.immutable),
    ("sources", .arr srcs)])

def maxContentPairs : Nat := 20

def DigestCase.toJ (c : DigestCase) : Except String J := do
  let ctx := s!"digest case {c.name}"
  for e in c.ents do checkEnt ctx e
  checkDistinctEnts ctx c.ents
  for d in c.grants do checkDGrant ctx d
  let mut store := GrantStore.putGrants Store.empty (c.grants.map (·.record))
  let mut facts : Facts := Facts.put [] c.grants
  let mut st : Digest.State := { partitions := [], global := none }
  let mut sealedOnce := false
  let mut sealed := false
  let mut last : Option (GrantStore × Facts × Digest.State) := none
  let mut out : Array J := #[]
  for op in c.ops do
    match op with
    | .seal =>
      if sealed then throw s!"{ctx}: seal while sealed"
      st := if sealedOnce then Digest.repair digestHash facts.fn store c.ents st
        else Digest.build digestHash facts.fn store c.ents
      sealedOnce := true
      sealed := true
      last := some (store, facts, st)
      out := out.push (.obj [("op", .str "seal")])
    | .resume =>
      unless sealed do throw s!"{ctx}: resume while not sealed"
      sealed := false
      out := out.push (.obj [("op", .str "resume")])
    | .read e =>
      checkEnt ctx e
      let (found, count) := match st.lookup e with
        | some n => (true, n.count)
        | none => (false, 0)
      out := out.push (.obj [("op", .str "read"), ("ent", ← entJ e), ("found", .bool found), ("count", .num count),
        ("width", .num (Digest.chooseWidth count))])
    | .readGlobal =>
      let (found, count) := match st.global with
        | some n => (true, n.count)
        | none => (false, 0)
      out := out.push (.obj [("op", .str "read_global"), ("found", .bool found), ("count", .num count)])
    | .put b =>
      if sealed then throw s!"{ctx}: put after seal without resume"
      for d in b do checkDGrant ctx d
      store := store.putGrants (b.map (·.record))
      facts := facts.put b
      for d in b do st := st.afterPut d.record
      out := out.push (.obj [("op", .str "put"), ("batch", .arr (← b.mapM dgrantJ))])
    | .delete g =>
      if sealed then throw s!"{ctx}: delete after seal without resume"
      checkGrantId ctx g
      st := st.afterDelete store g
      store := store.deleteGrant g
      out := out.push (.obj ([("op", .str "delete")] ++ (← grantIdFields g)))
  let (eq, ne) : List (EntitlementId × EntitlementId) × List (EntitlementId × EntitlementId) := match last with
    | none => ([], [])
    | some (s, fs, st') =>
      let content := fun (e : EntitlementId) => (s.grantsForEntitlement e).map (Digest.contentOf fs.fn)
      let es : List EntitlementId := st'.partitions.map (·.1)
      let pairs := (es.zipIdx).flatMap fun (a, i) => (es.drop (i + 1)).map fun b => (a, b)
      let eq := pairs.filter fun (a, b) => content a == content b
      let ne := pairs.filter fun (a, b) => content a != content b
      (eq.take maxContentPairs, ne.take maxContentPairs)
  let pairJ := fun (p : EntitlementId × EntitlementId) => do pure (J.arr [← entJ p.1, ← entJ p.2])
  pure <| .obj [("name", .str c.name), ("entitlements", .arr (← c.ents.mapM entJ)),
    ("grants", .arr (← c.grants.mapM dgrantJ)), ("ops", .arr out.toList),
    ("equal_content", .arr (← eq.mapM pairJ)), ("distinct_content", .arr (← ne.mapM pairJ))]

def dg (rt rid ext prt prid : String) (extId : String := "") (immutable : Bool := false)
    (sources : List (String × Bool) := []) : DGrant :=
  { record := gr rt rid ext prt prid extId, immutable, sources := sources.map fun (k, d) => ⟨u k, d⟩ }

def dE1 : EntitlementId := eA
def dE2 : EntitlementId := en "group" "g2" "group:g2:member"

/-- 513 grants under `eA`: `chooseWidth 513 = 1`. -/
def wideGrants : List DGrant :=
  (List.range 513).map fun i => dg "group" "g1" "group:g1:member" "user" s!"u{i}"

def digestCases : List DigestCase := [
  ⟨"same principals on two entitlements with different external ids",
    [dE1, dE2],
    [dg "group" "g1" "group:g1:member" "user" "u1" "x", dg "group" "g1" "group:g1:member" "user" "u2" "y",
     dg "group" "g2" "group:g2:member" "user" "u1" "p", dg "group" "g2" "group:g2:member" "user" "u2"],
    [.seal, .read dE1, .read dE2, .readGlobal]⟩,
  ⟨"two zero-grant entitlement records have equal content",
    [dE1, dE2, en "group" "g1" "admin"],
    [dg "group" "g1" "admin" "user" "u1"],
    [.seal, .read dE1, .read dE2, .read (en "group" "g1" "admin"), .readGlobal]⟩,
  ⟨"different principals are distinct",
    [dE1, dE2],
    [dg "group" "g1" "group:g1:member" "user" "u1", dg "group" "g2" "group:g2:member" "user" "u2"],
    [.seal, .read dE1, .read dE2]⟩,
  ⟨"immutable flag difference is distinct",
    [],
    [dg "group" "g1" "group:g1:member" "user" "u1" (immutable := true),
     dg "group" "g2" "group:g2:member" "user" "u1"],
    [.seal, .read dE1, .read dE2]⟩,
  ⟨"sources difference is distinct",
    [],
    [dg "group" "g1" "group:g1:member" "user" "u1" (sources := [("s1", true)]),
     dg "group" "g2" "group:g2:member" "user" "u1" (sources := [("s1", true), ("s2", false)]),
     dg "group" "g1" "admin" "user" "u1" (sources := [("s2", false), ("s1", true)])],
    [.seal, .read dE1, .read dE2, .read (en "group" "g1" "admin")]⟩,
  ⟨"flipped is_direct on the same source keys is distinct",
    [],
    [dg "group" "g1" "group:g1:member" "user" "u1" (sources := [("s1", true)]),
     dg "group" "g2" "group:g2:member" "user" "u1" (sources := [("s1", false)])],
    [.seal, .read dE1, .read dE2]⟩,
  ⟨"zero-grant entitlement record is found with count zero",
    [en "group" "g3" "admin"], [dg "group" "g1" "group:g1:member" "user" "u1"],
    [.seal, .read (en "group" "g3" "admin"), .readGlobal]⟩,
  ⟨"grant without entitlement record still has a partition",
    [dE2], [dg "role" "nowhere" "role:nowhere:owner" "service" "ghost"],
    [.seal, .read (en "role" "nowhere" "role:nowhere:owner"), .read dE2, .read dE1, .readGlobal]⟩,
  ⟨"write after seal invalidates partition and global",
    [dE1, dE2],
    [dg "group" "g1" "group:g1:member" "user" "u1", dg "group" "g2" "group:g2:member" "user" "u1"],
    [.seal, .read dE1, .read dE2, .readGlobal, .resume, .put [dg "group" "g1" "group:g1:member" "user" "u2"],
     .read dE1, .read dE2, .readGlobal, .seal, .read dE1, .read dE2, .readGlobal]⟩,
  ⟨"delete of absent grant after seal keeps digests",
    [dE1, dE2],
    [dg "group" "g1" "group:g1:member" "user" "u1", dg "group" "g2" "group:g2:member" "user" "u1"],
    [.seal, .resume, .delete ⟨dE1, u "user", u "absent"⟩, .read dE1, .read dE2, .readGlobal,
     .delete ⟨dE1, u "user", u "u1"⟩, .read dE1, .read dE2, .readGlobal]⟩,
  ⟨"delete after seal invalidates partition and global",
    [dE1, dE2],
    [dg "group" "g1" "group:g1:member" "user" "u1", dg "group" "g1" "group:g1:member" "user" "u2",
     dg "group" "g2" "group:g2:member" "user" "u1"],
    [.seal, .resume, .delete ⟨dE1, u "user", u "u1"⟩, .read dE1, .read dE2, .readGlobal, .seal, .read dE1,
     .read dE2, .readGlobal]⟩,
  ⟨"external id rewrite after seal keeps the count",
    [dE1],
    [dg "group" "g1" "group:g1:member" "user" "u1" "x"],
    [.seal, .read dE1, .resume, .put [dg "group" "g1" "group:g1:member" "user" "u1" "y"], .read dE1, .seal, .read dE1,
     .readGlobal]⟩,
  ⟨"reads before the first seal are absent",
    [dE1], [dg "group" "g1" "group:g1:member" "user" "u1"],
    [.read dE1, .readGlobal, .seal, .read dE1, .readGlobal]⟩,
  ⟨"513 grants under one entitlement use width one",
    [dE1], wideGrants, [.seal, .read dE1, .readGlobal]⟩
]

/-! ## reopen -/

inductive ROp where
  | startNew (id : String) (t : Sync.SyncType)
  | put (batch : List GrantRecord)
  | putDeferred (batch : List GrantRecord)
  | endSync
  | reopen
  | ageSync (days : Nat)
  | write
  | listGrants
  | readByPrincipal (prt prid : Bytes)
  | resume (id : String)
  | latestFinished (filter : Option Sync.SyncType)

structure ReopenCase where
  name : String
  ops : List ROp

/-- The model clock for the `reopen` family. -/
def reopenNow : Nat := 1000000000

def secondsPerDay : Nat := 86400

def checkSyncId (ctx id : String) : Except String Unit := do
  if id.isEmpty || id == "none" then throw s!"{ctx}: sync id must be non-empty and not \"none\""

/-- One op on the model. `afterReopen` is whether the previous op was `reopen`. -/
def ROp.step (ctx : String) (s : Sync.FileState) (x : IndexedGrants) (afterReopen : Bool) :
    ROp → Except String (Sync.FileState × IndexedGrants × J)
  | .startNew id t => do
    checkSyncId ctx id
    let ty ← syncTypeStr t
    let res := fun r => J.obj [("op", .str "start_new"), ("id", .str id), ("type", .str ty), ("result", .str r)]
    match Sync.startNewSync s id t reopenNow with
    | .ok s' => pure (s', IndexedGrants.empty, res "ok")
    | .syncInProgress => pure (s, x, res "sync_in_progress")
  | .put b => do
    unless Sync.writeGate s == .allowed do throw s!"{ctx}: put while no sync is bound"
    for r in b do checkGrant ctx r
    pure (Sync.recordWrite s, x.putGrants b, .obj [("op", .str "put"), ("batch", ← grantsJ b)])
  | .putDeferred b => do
    unless Sync.writeGate s == .allowed do throw s!"{ctx}: put_deferred while no sync is bound"
    for r in b do checkGrant ctx r
    pure (Sync.recordWrite s, x.putGrantsDeferred b, .obj [("op", .str "put_deferred"), ("batch", ← grantsJ b)])
  | .endSync =>
    let res := fun r => J.obj [("op", .str "end"), ("result", .str r)]
    match Sync.endSync s reopenNow with
    | .ok s' => pure (s', x.endSyncRebuild, res "ok")
    | .noCurrentSync => pure (s, x, res "no_current_sync")
  | .reopen => pure (Sync.reopen s, x, .obj [("op", .str "reopen")])
  | .ageSync days => do
    unless afterReopen do throw s!"{ctx}: age_sync must directly follow reopen"
    if s.run.isNone then throw s!"{ctx}: age_sync with no sync record"
    if days * secondsPerDay > reopenNow then throw s!"{ctx}: age_sync days out of range"
    pure (Sync.setStartedAt s (reopenNow - days * secondsPerDay), x, .obj [("op", .str "age_sync"), ("days", .num days)])
  | .write =>
    let res := fun r => J.obj [("op", .str "write"), ("result", .str r)]
    match Sync.writeGate s with
    | .allowed => pure (Sync.recordWrite s, x, res "allowed")
    | .noCurrentSync => pure (s, x, res "no_current_sync")
    | .engineSealed => pure (s, x, res "engine_sealed")
  | .listGrants => do
    match Sync.resolveActiveSync s none reopenNow with
    | none => pure (s, x, .obj [("op", .str "list_grants"), ("result", .str "no_current_sync"), ("grants", .arr [])])
    | some _ => pure (s, x, .obj [("op", .str "list_grants"), ("result", .str "ok"), ("grants", ← grantsJ x.store.allGrants)])
  | .readByPrincipal prt prid => do
    if prt.isEmpty || prid.isEmpty then throw s!"{ctx}: empty principal component"
    needUtf8 ctx prt
    needUtf8 ctx prid
    let base := [("op", J.str "read_by_principal"), ("prt", ← hexJ prt), ("prid", ← hexJ prid)]
    match Sync.resolveActiveSync s none reopenNow with
    | none => pure (s, x, .obj (base ++ [("result", .str "no_current_sync"), ("grants", .arr [])]))
    | some _ => pure (s, x, .obj (base ++ [("result", .str "ok"), ("grants", ← grantsJ (x.grantsForPrincipal prt prid))]))
  | .resume id => do
    checkSyncId ctx id
    let res := fun r => J.obj [("op", .str "resume"), ("id", .str id), ("result", .str r)]
    match Sync.resumeSync s id with
    | .ok s' => pure (s', x, res "ok")
    | .notFound => pure (s, x, res "not_found")
  | .latestFinished f => do
    let ty ← match f with
      | none => pure "any"
      | some t => syncTypeStr t
    let r := match Sync.latestFinished s f with
      | some run => run.id
      | none => "none"
    pure (s, x, .obj [("op", .str "latest_finished"), ("type", .str ty), ("result", .str r)])

def ROp.isReopen : ROp → Bool
  | .reopen => true
  | _ => false

def ReopenCase.toJ (c : ReopenCase) : Except String J := do
  let ctx := s!"reopen case {c.name}"
  let mut s := Sync.opened none false
  let mut x := IndexedGrants.empty
  let mut afterReopen := false
  let mut out : Array J := #[]
  for op in c.ops do
    let (s', x', j) ← op.step ctx s x afterReopen
    s := s'
    x := x'
    afterReopen := op.isReopen
    out := out.push j
  pure <| .obj [("name", .str c.name), ("ops", .arr out.toList)]

def rg1 : GrantRecord := pg1
def rg2 : GrantRecord := gr "group" "g2" "group:g2:member" "user" "u1"

def reopenCases : List ReopenCase := [
  ⟨"unfinished sync readable after reopen within cutoff",
    [.startNew "s1" .full, .put [rg1, pg2], .reopen, .listGrants, .write, .resume "s1", .write, .endSync]⟩,
  ⟨"finished sync readable after reopen and resumable",
    [.startNew "s1" .full, .put [rg1], .endSync, .reopen, .listGrants, .latestFinished none, .resume "s1", .write,
     .put [pg2], .listGrants]⟩,
  ⟨"start new after reopen wipes the keyspace",
    [.startNew "s1" .full, .put [rg1], .endSync, .reopen, .startNew "s2" .partialSync, .listGrants,
     .latestFinished none]⟩,
  ⟨"unfinished sync older than the cutoff does not resolve",
    [.startNew "s1" .full, .put [rg1], .reopen, .ageSync 8, .listGrants, .readByPrincipal (u "user") (u "u1"),
     .latestFinished none, .startNew "s2" .full, .listGrants]⟩,
  ⟨"unfinished sync within the cutoff resolves",
    [.startNew "s1" .full, .put [rg1], .reopen, .ageSync 6, .listGrants]⟩,
  ⟨"finished sync resolves whatever its age",
    [.startNew "s1" .resourcesOnly, .put [rg1], .endSync, .reopen, .ageSync 30, .listGrants,
     .latestFinished (some .resourcesOnly)]⟩,
  ⟨"deferred write invisible across reopen until end",
    [.startNew "s1" .full, .put [rg1], .putDeferred [rg2], .reopen, .readByPrincipal (u "user") (u "u1"),
     .resume "s1", .endSync, .readByPrincipal (u "user") (u "u1")]⟩,
  ⟨"reopen on a fresh file has no current sync", [.reopen, .listGrants, .readByPrincipal (u "user") (u "u1"),
     .write, .latestFinished none]⟩,
  ⟨"end after reopen without resume", [.startNew "s1" .full, .put [rg1], .reopen, .endSync, .latestFinished none]⟩,
  ⟨"reopen clears the fresh flag", [.startNew "s1" .full, .reopen, .startNew "s2" .full, .write]⟩,
  ⟨"reopen after end unseals but stays unbound", [.startNew "s1" .full, .endSync, .write, .reopen, .write,
     .resume "s1", .write]⟩
]

/-! ## views -/

inductive VQuery where
  | listGrants
  | streamGrants
  | grantsForEnt (e : EntitlementId)
  | streamForEnt (e : EntitlementId)
  | point (e : EntitlementId) (prt prid : Bytes)
  | forPrincipal (prt prid : Bytes)
  | forPrincipalType (prt : Bytes)
  | resourcesByIds (ids : List ResourceId)
  | entsByIds (ids : List Bytes)

/-- Rows written to one store, optionally sealed by `EndSync`, then
queried through each view. Resource and entitlement values are display
names. -/
structure ViewsCase where
  name : String
  resources : List (ResourceId × String) := []
  ents : List (EntitlementId × String) := []
  grants : List GrantRecord := []
  deferred : List GrantRecord := []
  endSync : Bool := false
  queries : List VQuery

def checkPrincipal (ctx : String) (prt prid : Bytes) : Except String Unit := do
  if prt.isEmpty || prid.isEmpty then throw s!"{ctx}: empty principal component"
  needUtf8 ctx prt
  needUtf8 ctx prid

def checkResource (ctx : String) (r : ResourceId) : Except String Unit := do
  if r.rt.isEmpty || r.rid.isEmpty then throw s!"{ctx}: empty resource component"
  needUtf8 ctx r.rt
  needUtf8 ctx r.rid

def valueStr (ctx : String) (v : Bytes) : Except String String :=
  match utf8? v with
  | some s => pure s
  | none => throw s!"{ctx}: value is not UTF-8"

def resValueJ (r : ResourceId) (v : String) : Except String J := do
  pure <| .obj [("rt", ← hexJ r.rt), ("rid", ← hexJ r.rid), ("value", .str v)]

/-- One query against the store. `ents` are the stored entitlement
identities and `es` their values. -/
def VQuery.toJ (ctx : String) (x : IndexedGrants) (rs : Store Bytes) (ents : List EntitlementId)
    (es : EntitlementStore) : VQuery → Except String J
  | .listGrants => do
    pure (.obj [("view", .str "list_grants"), ("grants", ← grantsJ (GrantStore.allGrants x.store))])
  | .streamGrants => do
    pure (.obj [("view", .str "stream_grants"),
      ("grants", ← grantsJ (Views.streamRecords (GrantStore.allGrants x.store)))])
  | .grantsForEnt e => do
    checkEnt ctx e
    pure (.obj [("view", .str "grants_for_entitlement"), ("ent", ← entJ e),
      ("grants", ← grantsJ (GrantStore.grantsForEntitlement x.store e))])
  | .streamForEnt e => do
    checkEnt ctx e
    unless ents.filter (·.ext == e.ext) == [e] do
      throw s!"{ctx}: stream_grants_for_entitlement ent must be the only entitlement row with its ext"
    pure (.obj [("view", .str "stream_grants_for_entitlement"), ("ent", ← entJ e),
      ("grants", ← grantsJ (Views.streamRecords (GrantStore.grantsForEntitlement x.store e)))])
  | .point e prt prid => do
    checkEnt ctx e
    checkPrincipal ctx prt prid
    let base := [("view", J.str "point_grant"), ("ent", ← entJ e), ("prt", ← hexJ prt), ("prid", ← hexJ prid)]
    match GrantStore.getGrant x.store ⟨e, prt, prid⟩ with
    | some r => pure (.obj (base ++ [("found", .bool true), ("grant", ← grantJ r)]))
    | none => pure (.obj (base ++ [("found", .bool false)]))
  | .forPrincipal prt prid => do
    checkPrincipal ctx prt prid
    pure (.obj [("view", .str "grants_for_principal"), ("prt", ← hexJ prt), ("prid", ← hexJ prid),
      ("grants", ← grantsJ (x.grantsForPrincipal prt prid))])
  | .forPrincipalType prt => do
    if prt.isEmpty then throw s!"{ctx}: empty principal type"
    needUtf8 ctx prt
    pure (.obj [("view", .str "grants_for_principal_type"), ("prt", ← hexJ prt),
      ("grants", ← grantsJ (x.grantsForPrincipalType prt))])
  | .resourcesByIds ids => do
    for r in ids do checkResource ctx r
    let found ← (Views.bulkResources rs ids).mapM fun (r, v) => do resValueJ r (← valueStr ctx v)
    pure (.obj [("view", .str "resources_by_ids"), ("ids", .arr (← ids.mapM resJ)), ("found", .arr found)])
  | .entsByIds ids => do
    for i in ids do
      if i.isEmpty then throw s!"{ctx}: empty entitlement id"
      needUtf8 ctx i
    let base := [("view", J.str "entitlements_by_ids"), ("ids", .arr (← ids.mapM hexJ))]
    match Views.bulkEntitlements ents ids with
    | none => pure (.obj (base ++ [("result", .str "ambiguous"), ("found", .arr [])]))
    | some found =>
      let fs ← found.mapM fun e => do
        let some v := EntitlementStore.getEntitlement es e | throw s!"{ctx}: entitlement without a value"
        entValueJ e (← valueStr ctx v)
      pure (.obj (base ++ [("result", .str "ok"), ("found", .arr fs)]))

def ViewsCase.toJ (c : ViewsCase) : Except String J := do
  let ctx := s!"views case {c.name}"
  for (r, _) in c.resources do checkResource ctx r
  if (c.resources.map (·.1.key)).eraseDups.length != c.resources.length then throw s!"{ctx}: repeated resource"
  let ents := c.ents.map (·.1)
  for e in ents do checkEnt ctx e
  checkDistinctEnts ctx ents
  for r in c.grants ++ c.deferred do checkGrant ctx r
  let x0 := (IndexedGrants.empty.putGrants c.grants).putGrantsDeferred c.deferred
  let x := if c.endSync then x0.endSyncRebuild else x0
  let rs : Store Bytes := Store.empty.putBatch (c.resources.map fun (r, v) => (r.key, u v))
  let es := EntitlementStore.putEntitlements Store.empty (c.ents.map fun (e, v) => (e, u v))
  let qs ← c.queries.mapM (VQuery.toJ ctx x rs ents es)
  pure <| .obj [("name", .str c.name), ("resources", .arr (← c.resources.mapM fun (r, v) => resValueJ r v)),
    ("entitlements", .arr (← c.ents.mapM fun (e, v) => entValueJ e v)), ("grants", ← grantsJ c.grants),
    ("deferred", ← grantsJ c.deferred), ("end_sync", .bool c.endSync), ("queries", .arr qs)]

def vEG2 : EntitlementId := en "group" "g2" "group:g2:member"
def vAdmin : EntitlementId := en "group" "g1" "admin"

def vResources : List (ResourceId × String) :=
  [(⟨u "user", u "u2"⟩, "r1"), (⟨u "group", u "g1"⟩, "r2"), (⟨u "user", u "u1"⟩, "r3")]

def vEnts : List (EntitlementId × String) := [(eA, "e1"), (vAdmin, "e2"), (vEG2, "e3")]

def vGrants : List GrantRecord := streamGrants ++ [gr "group" "g2" "group:g2:member" "user" "u1"]

/-- A new identity, so a deferred write leaves it out of the `by_principal` index. -/
def vDeferred : GrantRecord := gr "group" "g2" "group:g2:member" "user" "u3"

/-- Every view kind once or more over the store. -/
def vAllQueries : List VQuery := [
  .listGrants, .streamGrants, .grantsForEnt eA, .grantsForEnt vEG2, .streamForEnt eA, .streamForEnt vAdmin,
  .streamForEnt vEG2, .point eA (u "user") (u "u1"), .point eA (u "user") (u "u9"),
  .point vEG2 (u "user") (u "u3"), .forPrincipal (u "user") (u "u1"), .forPrincipal (u "user") (u "u3"),
  .forPrincipalType (u "user"), .forPrincipalType (u "group"),
  .resourcesByIds [⟨u "user", u "u1"⟩, ⟨u "group", u "g1"⟩],
  .entsByIds [u "group:g1:member", u "admin"]
]

def viewsCases : List ViewsCase := [
  { name := "every view agrees after end sync", resources := vResources, ents := vEnts, grants := vGrants,
    deferred := [vDeferred], endSync := true, queries := vAllQueries },
  { name := "deferred grant missing only from the index views before end sync", resources := vResources,
    ents := vEnts, grants := vGrants, deferred := [vDeferred], queries := vAllQueries },
  { name := "bulk reads skip missing ids and keep repeats in request order", resources := vResources,
    ents := vEnts, grants := vGrants, endSync := true,
    queries := [
      .resourcesByIds [⟨u "user", u "u2"⟩, ⟨u "user", u "u9"⟩, ⟨u "user", u "u1"⟩, ⟨u "user", u "u2"⟩,
        ⟨u "role", u "g1"⟩],
      .entsByIds [u "group:g2:member", u "nomatch", u "admin", u "group:g2:member"],
      .entsByIds [u "nomatch"]] },
  { name := "bulk entitlement read with an ambiguous id fails the whole call",
    ents := [(eA, "e1"), (vAdmin, "e2"), (en "group" "g2" "admin", "e3")], endSync := true,
    queries := [.entsByIds [u "group:g1:member", u "admin"], .entsByIds [u "admin"],
      .entsByIds [u "group:g1:member", u "nomatch"]] },
  { name := "empty store", endSync := true, queries := [
      .listGrants, .streamGrants, .grantsForEnt eA, .point eA (u "user") (u "u1"),
      .forPrincipal (u "user") (u "u1"), .forPrincipalType (u "user"),
      .resourcesByIds [⟨u "user", u "u1"⟩], .entsByIds [u "admin"]] },
  { name := "empty store without end sync", queries := [.listGrants, .forPrincipalType (u "user")] }
]

/-! ## container -/

inductive COp where
  | startNew (id : String) (t : Sync.SyncType)
  | put (batch : List GrantRecord)
  | putDeferred (batch : List GrantRecord)
  | endSync
  | ageSync (days : Nat)
  | saveReopen (readOnly : Bool) (dmg : Option Container.Damage)
  | write
  | listGrants
  | readByPrincipal (prt prid : Bytes)
  | resume (id : String)
  | latestFinished (filter : Option Sync.SyncType)

structure ContainerCase where
  name : String
  ops : List COp

def damageStr : Container.Damage → String
  | .truncateHeader => "truncate_header"
  | .badMagic => "bad_magic"
  | .badEngine => "bad_engine"
  | .flipPayloadByte => "flip_payload_byte"
  | .truncateTail => "truncate_tail"

/-- Model state of one `container` case. `saved` is whether a
`save_reopen` has succeeded, so a file exists on disk; `failed` is
whether the last `save_reopen` returned `open_error`. -/
structure CState where
  s : Sync.FileState := Sync.opened none false
  x : IndexedGrants := IndexedGrants.empty
  readOnly : Bool := false
  saved : Bool := false
  failed : Bool := false

/-- `Close` writes a file only for a dirty store: before the first
`save_reopen`, that needs a sync record (every op that can succeed on a
fresh store follows a `start_new`). -/
def CState.hasFile (st : CState) : Bool := st.saved || st.s.run.isSome

/-- The ops whose behavior is the `reopen` family's. -/
def COp.asROp : COp → Option ROp
  | .startNew id t => some (.startNew id t)
  | .endSync => some .endSync
  | .listGrants => some .listGrants
  | .readByPrincipal prt prid => some (.readByPrincipal prt prid)
  | .resume id => some (.resume id)
  | .latestFinished f => some (.latestFinished f)
  | _ => none

/-- Ops a read-only store refuses to replay: everything but reads,
`write` (which answers `read_only`), and `save_reopen`. -/
def COp.mutates : COp → Bool
  | .startNew .. | .put _ | .putDeferred _ | .endSync | .ageSync _ | .resume _ => true
  | _ => false

def COp.step (ctx : String) (st : CState) (op : COp) : Except String (CState × J) := do
  if st.readOnly && op.mutates then throw s!"{ctx}: mutating op on a read-only store"
  let opened : Container.Opened := { state := st.s, readOnly := st.readOnly }
  match op with
  | .put b => do
    unless Container.writeGate' opened == .allowed do throw s!"{ctx}: put while no sync is bound"
    for r in b do checkGrant ctx r
    pure ({ st with s := Sync.recordWrite st.s, x := st.x.putGrants b }, .obj [("op", .str "put"), ("batch", ← grantsJ b)])
  | .putDeferred b => do
    unless Container.writeGate' opened == .allowed do throw s!"{ctx}: put_deferred while no sync is bound"
    for r in b do checkGrant ctx r
    pure ({ st with s := Sync.recordWrite st.s, x := st.x.putGrantsDeferred b },
      .obj [("op", .str "put_deferred"), ("batch", ← grantsJ b)])
  | .ageSync days => do
    if st.s.run.isNone then throw s!"{ctx}: age_sync with no sync record"
    if days * secondsPerDay > reopenNow then throw s!"{ctx}: age_sync days out of range"
    pure ({ st with s := Sync.setStartedAt st.s (reopenNow - days * secondsPerDay) },
      .obj [("op", .str "age_sync"), ("days", .num days)])
  | .write =>
    let res := fun r => J.obj [("op", .str "write"), ("result", .str r)]
    match Container.writeGate' opened with
    | .allowed => pure ({ st with s := Sync.recordWrite st.s }, res "allowed")
    | .noCurrentSync => pure (st, res "no_current_sync")
    | .engineSealed => pure (st, res "engine_sealed")
    | .readOnly => pure (st, res "read_only")
  | .saveReopen ro d => do
    unless st.hasFile do throw s!"{ctx}: save_reopen before anything was written (Close writes only a dirty store)"
    let sealed := Container.sealArtifact st.s
    let a := match d with
      | some k => Container.damage sealed k
      | none => sealed
    let base := [("op", J.str "save_reopen"), ("readonly", .bool ro),
      ("damage", match d with | some k => .str (damageStr k) | none => .null)]
    match Container.openArtifact a ro reopenNow with
    | .error _ => pure ({ st with failed := true }, .obj (base ++ [("result", .str "open_error")]))
    | .ok o => pure ({ st with s := o.state, readOnly := o.readOnly, saved := true },
        .obj (base ++ [("result", .str "ok")]))
  | op =>
    match op.asROp with
    | some r => do
      let (s', x', j) ← r.step ctx st.s st.x false
      pure ({ st with s := s', x := x' }, j)
    | none => throw s!"{ctx}: unhandled op"

def ContainerCase.toJ (c : ContainerCase) : Except String J := do
  let ctx := s!"container case {c.name}"
  let mut st : CState := {}
  let mut out : Array J := #[]
  for op in c.ops do
    if st.failed then throw s!"{ctx}: op after open_error"
    let (st', j) ← op.step ctx st
    st := st'
    out := out.push j
  pure <| .obj [("name", .str c.name), ("ops", .arr out.toList)]

/-- A finished file holding `rg1`, sealed with the given damage. -/
def damagedOpen (d : Container.Damage) (ro : Bool := false) : List COp :=
  [.startNew "s1" .full, .put [rg1], .endSync, .saveReopen ro (some d)]

def containerCases : List ContainerCase := [
  ⟨"finished file reopened writable accepts writes and round trips",
    [.startNew "s1" .full, .put [rg1], .endSync, .saveReopen false none, .write, .put [pg2],
     .saveReopen false none, .listGrants, .latestFinished none]⟩,
  ⟨"finished file reopened read only refuses writes",
    [.startNew "s1" .full, .put [rg1], .endSync, .saveReopen true none, .write, .listGrants,
     .readByPrincipal (u "user") (u "u1")]⟩,
  ⟨"read only open then writable open",
    [.startNew "s1" .full, .put [rg1], .endSync, .saveReopen true none, .write, .saveReopen false none, .write,
     .put [pg2], .saveReopen false none, .listGrants]⟩,
  ⟨"unfinished file within cutoff binds the unfinished sync",
    [.startNew "s1" .full, .put [rg1], .saveReopen false none, .listGrants, .write, .endSync,
     .latestFinished none]⟩,
  ⟨"stale unfinished file reopened read only reports no current sync",
    [.startNew "s1" .full, .put [rg1], .ageSync 8, .saveReopen true none, .write, .listGrants]⟩,
  ⟨"unfinished file older than cutoff has no current sync",
    [.startNew "s1" .full, .put [rg1], .ageSync 8, .saveReopen false none, .listGrants, .write,
     .latestFinished none]⟩,
  ⟨"deferred grant invisible across reopen until end",
    [.startNew "s1" .full, .put [rg1], .putDeferred [rg2], .saveReopen false none,
     .readByPrincipal (u "user") (u "u1"), .endSync, .readByPrincipal (u "user") (u "u1")]⟩,
  ⟨"truncated header fails the open", damagedOpen .truncateHeader⟩,
  ⟨"bad magic fails the open", damagedOpen .badMagic⟩,
  ⟨"unknown engine fails the open", damagedOpen .badEngine⟩,
  ⟨"flipped payload byte fails the open", damagedOpen .flipPayloadByte⟩,
  ⟨"truncated tail fails a read only open", damagedOpen .truncateTail true⟩,
  ⟨"file with only a started sync reopened",
    [.startNew "s1" .full, .saveReopen false none, .listGrants, .write, .latestFinished none]⟩
]

/-! ## document -/

/-- The families in schema order, each paired with its field name. -/
structure Families where
  keys : List J := []
  strip : List J := []
  writes : List J := []
  pages : List J := []
  bare : List J := []
  sync : List J := []
  grantWrites : List J := []
  entWrites : List J := []
  grantList : List J := []
  byPrincipal : List J := []
  grantBare : List J := []
  stream : List J := []
  digest : List J := []
  reopen : List J := []
  views : List J := []
  container : List J := []

def Families.toList (f : Families) : List (String × List J) :=
  [("keys", f.keys), ("entitlement_strip", f.strip), ("writes", f.writes),
    ("pagination", f.pages), ("bare_id", f.bare), ("sync", f.sync),
    ("grant_writes", f.grantWrites), ("entitlement_writes", f.entWrites), ("grant_list", f.grantList),
    ("grants_by_principal", f.byPrincipal), ("grant_bare_id", f.grantBare),
    ("stream", f.stream), ("digest", f.digest), ("reopen", f.reopen),
    ("views", f.views), ("container", f.container)]

def Families.append (a b : Families) : Families :=
  ⟨a.keys ++ b.keys, a.strip ++ b.strip, a.writes ++ b.writes, a.pages ++ b.pages, a.bare ++ b.bare,
    a.sync ++ b.sync, a.grantWrites ++ b.grantWrites, a.entWrites ++ b.entWrites, a.grantList ++ b.grantList,
    a.byPrincipal ++ b.byPrincipal, a.grantBare ++ b.grantBare, a.stream ++ b.stream, a.digest ++ b.digest,
    a.reopen ++ b.reopen, a.views ++ b.views, a.container ++ b.container⟩

/-- `version`, `counts`, then each family, in schema order. -/
def Families.toDoc (f : Families) : J :=
  .obj ([("version", .num 1), ("counts", .obj (f.toList.map fun (n, xs) => (n, .num xs.length)))] ++
    f.toList.map fun (n, xs) => (n, .arr xs))

/-- Generator inputs for every family. -/
structure Inputs where
  keys : List KeyCase := []
  strip : List EntitlementId := []
  writes : List WriteCase := []
  pages : List PageCase := []
  bare : List BareCase := []
  sync : List SyncCase := []
  grantWrites : List GrantWriteCase := []
  entWrites : List EntWriteCase := []
  grantList : List GrantListCase := []
  byPrincipal : List ByPrincipalCase := []
  grantBare : List GrantBareCase := []
  stream : List StreamCase := []
  digest : List DigestCase := []
  reopen : List ReopenCase := []
  views : List ViewsCase := []
  container : List ContainerCase := []

/-- Expected values for every input, computed by the model. -/
def render (i : Inputs) : Except String Families := do
  pure ⟨← i.keys.mapM KeyCase.toJ, ← i.strip.mapM stripToJ, ← i.writes.mapM WriteCase.toJ,
    ← i.pages.mapM PageCase.toJ, ← i.bare.mapM BareCase.toJ, ← i.sync.mapM SyncCase.toJ,
    ← i.grantWrites.mapM GrantWriteCase.toJ, ← i.entWrites.mapM EntWriteCase.toJ,
    ← i.grantList.mapM GrantListCase.toJ, ← i.byPrincipal.mapM ByPrincipalCase.toJ,
    ← i.grantBare.mapM GrantBareCase.toJ, ← i.stream.mapM StreamCase.toJ, ← i.digest.mapM DigestCase.toJ,
    ← i.reopen.mapM ReopenCase.toJ, ← i.views.mapM ViewsCase.toJ, ← i.container.mapM ContainerCase.toJ⟩

/-- The fixed corpus. Fails if any family is empty. -/
def fixedInputs : Inputs where
  keys := keyCases
  strip := stripCases
  writes := writeCases
  pages := pageCases
  bare := bareCases
  sync := syncCases
  grantWrites := grantWriteCases
  entWrites := entWriteCases
  grantList := grantListCases
  byPrincipal := byPrincipalCases
  grantBare := grantBareCases
  stream := streamCases
  digest := digestCases
  reopen := reopenCases
  views := viewsCases
  container := containerCases

def fixedFamilies : Except String Families := do
  let f ← render fixedInputs
  for (n, xs) in f.toList do
    if xs.isEmpty then throw s!"family {n} is empty"
  pure f

def document : Except String J := Families.toDoc <$> fixedFamilies

end Oracle
