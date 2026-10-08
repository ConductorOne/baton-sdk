import Oracle.Cases
import Oracle.Random
import Oracle.Request

/-!
# `c1z-oracle` entry point

Modes (`ORACLE_SCHEMA.md`, "Oracle modes"):

- no arguments: the fixed corpus;
- `--random N --seed S`, in either order: the fixed corpus plus `N`
  cases per family from `Oracle.Random.families`;
- `--respond`: the request document on stdin, rendered by
  `Oracle.Request.respond`.

Every mode renders the whole document before printing it. On any error
it prints one line to stderr, nothing to stdout, and exits 1; a usage
error exits 2.
-/

def readStdin : IO ByteArray := do
  let stdin ← IO.getStdin
  let mut buf := ByteArray.empty
  repeat
    let chunk ← stdin.read 65536
    if chunk.isEmpty then break
    buf := buf ++ chunk
  pure buf

def usage : String := "usage: c1z-oracle [--random N --seed S | --respond]"

/-- `(N, S)` from `--random N --seed S` in either order. -/
def parseRandom : List String → Option (Nat × Nat)
  | ["--random", n, "--seed", s] | ["--seed", s, "--random", n] => do
    pure (← n.toNat?, ← s.toNat?)
  | _ => none

def run : List String → IO (Option (Except String Oracle.J))
  | [] => pure (some Oracle.document)
  | ["--respond"] => do
    let input ← readStdin
    match String.fromUTF8? input with
    | none => pure (some (.error "request is not UTF-8"))
    | some s => pure (some (Oracle.Families.toDoc <$> Oracle.Request.respond s))
  | args =>
    match parseRandom args with
    | none => pure none
    | some (n, seed) => pure <| some do
      let fixed ← Oracle.fixedFamilies
      let rand ← Oracle.Random.families n seed
      pure (fixed.append rand).toDoc

def main (args : List String) : IO UInt32 := do
  match ← run args with
  | none =>
    IO.eprintln usage
    pure 2
  | some (.ok doc) =>
    IO.println doc.renderDoc
    pure 0
  | some (.error e) =>
    IO.eprintln s!"c1z-oracle: {e}"
    pure 1
