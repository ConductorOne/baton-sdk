/-!
# Minimal JSON emitter for the oracle

A hand-rolled emitter with fixed field order, so `lake exe c1z-oracle`
prints byte-identical output on every run. Byte strings render as
lowercase hex; `hex` rejects any element of 256 or more instead of
truncating it.
-/

namespace Oracle

/-- A JSON value. Objects keep their fields in the order given. -/
inductive J where
  | str (s : String)
  | num (n : Nat)
  | bool (b : Bool)
  | arr (xs : List J)
  | obj (kvs : List (String × J))
  | null
  deriving Inhabited

private def hexDigit (n : Nat) : Char :=
  if n < 10 then Char.ofNat (48 + n) else Char.ofNat (87 + n)

/-- Lowercase hex, two digits per byte. Fails on any value of 256 or more. -/
def hex (bs : List Nat) : Except String String := do
  let mut out := ""
  for b in bs do
    if b ≥ 256 then throw s!"byte {b} out of range"
    out := (out.push (hexDigit (b / 16))).push (hexDigit (b % 16))
  return out

/-- `hex` wrapped as a JSON string. -/
def hexJ (bs : List Nat) : Except String J := J.str <$> hex bs

private def escapeStr (s : String) : String :=
  s.foldl (init := "\"") (fun acc c =>
    match c with
    | '"' => acc ++ "\\\""
    | '\\' => acc ++ "\\\\"
    | '\n' => acc ++ "\\n"
    | '\r' => acc ++ "\\r"
    | '\t' => acc ++ "\\t"
    | c =>
      if c.toNat < 0x20 then
        let n := c.toNat
        acc ++ "\\u00" ++ String.singleton (hexDigit (n / 16)) ++ String.singleton (hexDigit (n % 16))
      else acc.push c) ++ "\""

/-- Compact rendering. -/
partial def J.render : J → String
  | .str s => escapeStr s
  | .num n => toString n
  | .bool b => if b then "true" else "false"
  | .arr xs => "[" ++ ",".intercalate (xs.map J.render) ++ "]"
  | .obj kvs => "{" ++ ",".intercalate (kvs.map fun (k, v) => escapeStr k ++ ":" ++ v.render) ++ "}"
  | .null => "null"

/-- Top-level rendering: one field per line, and one array element per
line inside top-level arrays, so diffs of the generated file stay local. -/
def J.renderDoc : J → String
  | .obj kvs =>
    let field := fun ((k, v) : String × J) =>
      match v with
      | .arr xs =>
        if xs.isEmpty then "  " ++ escapeStr k ++ ": []"
        else "  " ++ escapeStr k ++ ": [\n" ++ ",\n".intercalate (xs.map fun x => "    " ++ x.render) ++ "\n  ]"
      | v => "  " ++ escapeStr k ++ ": " ++ v.render
    "{\n" ++ ",\n".intercalate (kvs.map field) ++ "\n}"
  | v => v.render

end Oracle
