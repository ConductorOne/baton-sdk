import Lean.Data.Json
import Oracle.Cases

/-!
# Request documents for `c1z-oracle --respond`

`Oracle.Request.parse` decodes a request document (`ORACLE_SCHEMA.md`,
"Request document") into the generator input types of `Oracle.Cases`.
It rejects unknown and missing fields, `counts`, any `version` other
than 1, hex that is not lowercase even-length, and unknown `kind`, `op`,
or `type` strings. `bare_id` and `grant_bare_id` entitlements within one
case must have distinct `(rt, rid, ext)`: the engine stores one row per
identity, so a repeated identity would make `resolveBare` count it twice.
A `grant_list` query with `prid` and no `prt` is rejected. `WellFormed`,
the UTF-8 checks on the grant families, the `end_sync` placement rule of
`grants_by_principal`, and the other per-family input checks run in
`Oracle.render`.
-/

namespace Oracle.Request

open Lean (Json)
open C1z

abbrev P := Except String

/-- The object's fields, after checking every key is in `req ++ opt` and
every key in `req` is present. -/
def fields (ctx : String) (j : Json) (req opt : List String) : P (List (String × Json)) := do
  let .obj m := j | throw s!"{ctx}: expected an object"
  let kvs := m.toList
  for (k, _) in kvs do
    unless req.contains k || opt.contains k do throw s!"{ctx}: unknown field \"{k}\""
  for k in req do
    unless kvs.any (·.1 == k) do throw s!"{ctx}: missing field \"{k}\""
  pure kvs

def get (ctx : String) (kvs : List (String × Json)) (k : String) : P Json :=
  match kvs.find? (·.1 == k) with
  | some (_, v) => pure v
  | none => throw s!"{ctx}: missing field \"{k}\""

def str (ctx : String) : Json → P String
  | .str s => pure s
  | _ => throw s!"{ctx}: expected a string"

def nat (ctx : String) : Json → P Nat
  | .num ⟨m, 0⟩ => if m ≥ 0 then pure m.toNat else throw s!"{ctx}: expected a non-negative integer"
  | _ => throw s!"{ctx}: expected a non-negative integer"

def arr (ctx : String) : Json → P (List Json)
  | .arr xs => pure xs.toList
  | _ => throw s!"{ctx}: expected an array"

def hexVal (c : Char) : Option Nat :=
  let n := c.toNat
  if 48 ≤ n && n ≤ 57 then some (n - 48)
  else if 97 ≤ n && n ≤ 102 then some (n - 87)
  else none

def hexChars (ctx : String) : List Char → P Bytes
  | [] => pure []
  | [_] => throw s!"{ctx}: odd-length hex"
  | a :: b :: rest =>
    match hexVal a, hexVal b with
    | some x, some y => do pure ((x * 16 + y) :: (← hexChars ctx rest))
    | _, _ => throw s!"{ctx}: not lowercase hex"

def hex (ctx : String) (j : Json) : P Bytes := do hexChars ctx (← str ctx j).toList

def hexField (ctx : String) (kvs : List (String × Json)) (k : String) : P Bytes := do
  hex s!"{ctx}.{k}" (← get ctx kvs k)

def strField (ctx : String) (kvs : List (String × Json)) (k : String) : P String := do
  str s!"{ctx}.{k}" (← get ctx kvs k)

def arrField (ctx : String) (kvs : List (String × Json)) (k : String) : P (List Json) := do
  arr s!"{ctx}.{k}" (← get ctx kvs k)

def indexed {α : Type} (xs : List Json) (f : Nat → Json → P α) : P (List α) :=
  (xs.zipIdx).mapM fun (x, i) => f i x

def syncType (ctx : String) : String → P Sync.SyncType
  | "full" => pure .full
  | "partial" => pure .partialSync
  | "resources_only" => pure .resourcesOnly
  | t => throw s!"{ctx}: unknown sync type \"{t}\""

/-! ## families -/

def keyCase (i : Nat) (j : Json) : P KeyCase := do
  let ctx := s!"keys[{i}]"
  let kvs ← fields ctx j ["name", "kind"] ["rt", "rid", "ext", "prt", "prid"]
  let name ← strField ctx kvs "name"
  let kind ← strField ctx kvs "kind"
  let need ← match kind with
    | "resource_type" => pure ["ext"]
    | "resource" => pure ["rt", "rid"]
    | "entitlement" => pure ["rt", "rid", "ext"]
    | "grant" => pure ["rt", "rid", "ext", "prt", "prid"]
    | k => throw s!"{ctx}: unknown kind \"{k}\""
  let kvs ← fields ctx j (["name", "kind"] ++ need) []
  let h := hexField ctx kvs
  match kind with
  | "resource_type" => pure (.resourceType name ⟨← h "ext"⟩)
  | "resource" => pure (.resource name ⟨← h "rt", ← h "rid"⟩)
  | "entitlement" => pure (.entitlement name ⟨← h "rt", ← h "rid", ← h "ext"⟩)
  | _ => pure (.grant name ⟨⟨← h "rt", ← h "rid", ← h "ext"⟩, ← h "prt", ← h "prid"⟩)

def entitlement (ctx : String) (j : Json) : P EntitlementId := do
  let kvs ← fields ctx j ["rt", "rid", "ext"] []
  pure ⟨← hexField ctx kvs "rt", ← hexField ctx kvs "rid", ← hexField ctx kvs "ext"⟩

def stripCase (i : Nat) (j : Json) : P EntitlementId := entitlement s!"entitlement_strip[{i}]" j

def wop (ctx : String) (j : Json) : P WOp := do
  let kvs ← fields ctx j ["op"] ["batch", "rid"]
  match ← strField ctx kvs "op" with
  | "put" =>
    let kvs ← fields ctx j ["op", "batch"] []
    let items ← arrField ctx kvs "batch"
    let batch ← indexed items fun k x => do
      let c := s!"{ctx}.batch[{k}]"
      let kv ← fields c x ["rid", "value"] []
      pure (← hexField c kv "rid", ← strField c kv "value")
    pure (.put batch)
  | "delete" =>
    let kvs ← fields ctx j ["op", "rid"] []
    pure (.delete (← hexField ctx kvs "rid"))
  | o => throw s!"{ctx}: unknown op \"{o}\""

def writeCase (i : Nat) (j : Json) : P WriteCase := do
  let ctx := s!"writes[{i}]"
  let kvs ← fields ctx j ["name", "rt", "ops"] []
  let ops ← indexed (← arrField ctx kvs "ops") fun k x => wop s!"{ctx}.ops[{k}]" x
  pure ⟨← strField ctx kvs "name", ← hexField ctx kvs "rt", ops⟩

def pageCase (i : Nat) (j : Json) : P PageCase := do
  let ctx := s!"pagination[{i}]"
  let kvs ← fields ctx j ["name", "rt", "rids", "page_size"] []
  let rids ← indexed (← arrField ctx kvs "rids") fun k x => hex s!"{ctx}.rids[{k}]" x
  pure ⟨← strField ctx kvs "name", ← hexField ctx kvs "rt", rids, ← nat s!"{ctx}.page_size" (← get ctx kvs "page_size")⟩

def bareCase (i : Nat) (j : Json) : P BareCase := do
  let ctx := s!"bare_id[{i}]"
  let kvs ← fields ctx j ["name", "entitlements", "lookup"] []
  let ents ← indexed (← arrField ctx kvs "entitlements") fun k x => entitlement s!"{ctx}.entitlements[{k}]" x
  if ents.eraseDups.length != ents.length then throw s!"{ctx}: repeated entitlement identity"
  pure ⟨← strField ctx kvs "name", ents, ← hexField ctx kvs "lookup"⟩

def sop (ctx : String) (j : Json) : P SOp := do
  let kvs ← fields ctx j ["op"] ["id", "type"]
  match ← strField ctx kvs "op" with
  | "start_new" =>
    let kvs ← fields ctx j ["op", "id", "type"] []
    pure (.startNew (← strField ctx kvs "id") (← syncType ctx (← strField ctx kvs "type")))
  | "write" => do let _ ← fields ctx j ["op"] []; pure .write
  | "end" => do let _ ← fields ctx j ["op"] []; pure .endSync
  | "resume" =>
    let kvs ← fields ctx j ["op", "id"] []
    pure (.resume (← strField ctx kvs "id"))
  | "latest_finished" =>
    let kvs ← fields ctx j ["op", "type"] []
    match ← strField ctx kvs "type" with
    | "any" => pure (.latestFinished none)
    | t => pure (.latestFinished (some (← syncType ctx t)))
  | o => throw s!"{ctx}: unknown op \"{o}\""

def syncCase (i : Nat) (j : Json) : P SyncCase := do
  let ctx := s!"sync[{i}]"
  let kvs ← fields ctx j ["name", "ops"] []
  let ops ← indexed (← arrField ctx kvs "ops") fun k x => sop s!"{ctx}.ops[{k}]" x
  pure ⟨← strField ctx kvs "name", ops⟩

/-! ## grant families -/

def entField (ctx : String) (kvs : List (String × Json)) (k : String) : P EntitlementId := do
  entitlement s!"{ctx}.{k}" (← get ctx kvs k)

def grant (ctx : String) (j : Json) : P GrantRecord := do
  let kvs ← fields ctx j ["ent", "prt", "prid", "ext_id"] []
  pure ⟨⟨← entField ctx kvs "ent", ← hexField ctx kvs "prt", ← hexField ctx kvs "prid"⟩, ← hexField ctx kvs "ext_id"⟩

def grantList (ctx : String) (kvs : List (String × Json)) (k : String) : P (List GrantRecord) := do
  indexed (← arrField ctx kvs k) fun i x => grant s!"{ctx}.{k}[{i}]" x

/-- The structural identity carried by a `delete` op. -/
def grantIdOf (ctx : String) (kvs : List (String × Json)) : P GrantId := do
  pure ⟨← entField ctx kvs "ent", ← hexField ctx kvs "prt", ← hexField ctx kvs "prid"⟩

def gwop (ctx : String) (j : Json) : P GWOp := do
  let kvs ← fields ctx j ["op"] ["batch", "ent", "prt", "prid"]
  match ← strField ctx kvs "op" with
  | "put" =>
    let kvs ← fields ctx j ["op", "batch"] []
    pure (.put (← grantList ctx kvs "batch"))
  | "delete" =>
    let kvs ← fields ctx j ["op", "ent", "prt", "prid"] []
    pure (.delete (← grantIdOf ctx kvs))
  | o => throw s!"{ctx}: unknown op \"{o}\""

def grantWriteCase (i : Nat) (j : Json) : P GrantWriteCase := do
  let ctx := s!"grant_writes[{i}]"
  let kvs ← fields ctx j ["name", "ops"] []
  let ops ← indexed (← arrField ctx kvs "ops") fun k x => gwop s!"{ctx}.ops[{k}]" x
  pure ⟨← strField ctx kvs "name", ops⟩

def ewop (ctx : String) (j : Json) : P EWOp := do
  let kvs ← fields ctx j ["op"] ["batch", "rt", "rid", "ext"]
  match ← strField ctx kvs "op" with
  | "put" =>
    let kvs ← fields ctx j ["op", "batch"] []
    let batch ← indexed (← arrField ctx kvs "batch") fun k x => do
      let c := s!"{ctx}.batch[{k}]"
      let kv ← fields c x ["rt", "rid", "ext", "value"] []
      pure (⟨← hexField c kv "rt", ← hexField c kv "rid", ← hexField c kv "ext"⟩, ← strField c kv "value")
    pure (.put batch)
  | "delete" =>
    let kvs ← fields ctx j ["op", "rt", "rid", "ext"] []
    pure (.delete ⟨← hexField ctx kvs "rt", ← hexField ctx kvs "rid", ← hexField ctx kvs "ext"⟩)
  | o => throw s!"{ctx}: unknown op \"{o}\""

def entWriteCase (i : Nat) (j : Json) : P EntWriteCase := do
  let ctx := s!"entitlement_writes[{i}]"
  let kvs ← fields ctx j ["name", "ops"] []
  let ops ← indexed (← arrField ctx kvs "ops") fun k x => ewop s!"{ctx}.ops[{k}]" x
  pure ⟨← strField ctx kvs "name", ops⟩

def optHex (ctx : String) (kvs : List (String × Json)) (k : String) : P (Option Bytes) :=
  match kvs.find? (·.1 == k) with
  | none => pure none
  | some (_, v) => some <$> hex s!"{ctx}.{k}" v

def grantListCase (i : Nat) (j : Json) : P GrantListCase := do
  let ctx := s!"grant_list[{i}]"
  let kvs ← fields ctx j ["name", "grants", "query", "page_size"] []
  let qctx := s!"{ctx}.query"
  let q ← fields qctx (← get ctx kvs "query") ["ent"] ["prt", "prid"]
  let prt ← optHex qctx q "prt"
  let prid ← optHex qctx q "prid"
  if prt.isNone && prid.isSome then throw s!"{qctx}: prid without prt"
  pure { name := ← strField ctx kvs "name", grants := ← grantList ctx kvs "grants", ent := ← entField qctx q "ent",
         prt, prid, pageSize := ← nat s!"{ctx}.page_size" (← get ctx kvs "page_size") }

def pop (ctx : String) (j : Json) : P POp := do
  let kvs ← fields ctx j ["op"] ["batch", "ent", "prt", "prid"]
  match ← strField ctx kvs "op" with
  | "put" =>
    let kvs ← fields ctx j ["op", "batch"] []
    pure (.put (← grantList ctx kvs "batch"))
  | "put_deferred" =>
    let kvs ← fields ctx j ["op", "batch"] []
    pure (.putDeferred (← grantList ctx kvs "batch"))
  | "delete" =>
    let kvs ← fields ctx j ["op", "ent", "prt", "prid"] []
    pure (.delete (← grantIdOf ctx kvs))
  | "read" =>
    let kvs ← fields ctx j ["op", "prt", "prid"] []
    pure (.read (← hexField ctx kvs "prt") (← hexField ctx kvs "prid"))
  | "end_sync" => do let _ ← fields ctx j ["op"] []; pure .endSync
  | o => throw s!"{ctx}: unknown op \"{o}\""

def byPrincipalCase (i : Nat) (j : Json) : P ByPrincipalCase := do
  let ctx := s!"grants_by_principal[{i}]"
  let kvs ← fields ctx j ["name", "ops"] []
  let ops ← indexed (← arrField ctx kvs "ops") fun k x => pop s!"{ctx}.ops[{k}]" x
  pure ⟨← strField ctx kvs "name", ops⟩

def grantBareCase (i : Nat) (j : Json) : P GrantBareCase := do
  let ctx := s!"grant_bare_id[{i}]"
  let kvs ← fields ctx j ["name", "entitlements", "grants", "lookup"] []
  let ents ← indexed (← arrField ctx kvs "entitlements") fun k x => entitlement s!"{ctx}.entitlements[{k}]" x
  if ents.eraseDups.length != ents.length then throw s!"{ctx}: repeated entitlement identity"
  pure ⟨← strField ctx kvs "name", ents, ← grantList ctx kvs "grants", ← hexField ctx kvs "lookup"⟩

/-- A family's entries; an absent family is empty. -/
def family {α : Type} (kvs : List (String × Json)) (k : String) (f : Nat → Json → P α) : P (List α) :=
  match kvs.find? (·.1 == k) with
  | none => pure []
  | some (_, v) => do indexed (← arr k v) f

/-- Decodes a request document and renders it with the model. -/
def respond (input : String) : P Families := do
  let j ← match Json.parse input with
    | .ok j => pure j
    | .error e => throw s!"request is not JSON: {e}"
  let kvs ← fields "request" j ["version"]
    ["keys", "entitlement_strip", "writes", "pagination", "bare_id", "sync",
     "grant_writes", "entitlement_writes", "grant_list", "grants_by_principal", "grant_bare_id"]
  let v ← nat "request.version" (← get "request" kvs "version")
  unless v == 1 do throw s!"request.version: unsupported version {v}"
  render ⟨← family kvs "keys" keyCase, ← family kvs "entitlement_strip" stripCase,
    ← family kvs "writes" writeCase, ← family kvs "pagination" pageCase,
    ← family kvs "bare_id" bareCase, ← family kvs "sync" syncCase,
    ← family kvs "grant_writes" grantWriteCase, ← family kvs "entitlement_writes" entWriteCase,
    ← family kvs "grant_list" grantListCase, ← family kvs "grants_by_principal" byPrincipalCase,
    ← family kvs "grant_bare_id" grantBareCase⟩

end Oracle.Request
