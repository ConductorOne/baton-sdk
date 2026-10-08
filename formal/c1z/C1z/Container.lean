import C1z.Sync

/-!
# The `.c1z` container: sealArtifact, open, damage

Models the public write and read path in `pkg/dotc1z`: `NewStore` on a
new path opens a Pebble engine; `Close` on a dirty store checkpoints
the engine and writes a v3 envelope (`format/v3/envelope.go`,
`indexed.go`); `NewStore` on an existing path reads the header, picks
the engine from the manifest, extracts the payload, opens the engine,
and binds a sync through `InitCurrentSync`.

What the container adds on top of the engine is small, and this module
says only that:

- a sealed artifact reopens to the same logical state (`open_seal`),
  because save is flush, checkpoint, and envelope, with every logical
  normalization done earlier by `EndSync`;
- the public open binds the default sync, so a finished store reopened
  writable accepts writes without `ResumeSync`
  (`writeGate_publicOpen_finished`), and a read-only open refuses them;
- the damage classes the default envelope detects each fail the open:
  a truncated header, a bad magic, an unknown engine, a flipped payload
  byte, a truncated tail (`open_damage_error`). The indexed encoding
  pins the manifest and every frame by hash, so there is no path from
  these damages to a successful open with fewer records.

Out of scope, stated so the limits are visible: the `TAR` and
`TAR_ZSTD` payload encodings (plain tar has no manifest hash and a cut
between entries is not an archive error; whether the engine then opens
is unverified), SQLite v1 files, the manifest's advisory copies of
sync runs, stats, and the digest root (never read back; the keyspace is
authoritative), the id-index migration on legacy files, the dirty
tracking that decides whether `Close` writes at all, and the temp
directory lifecycle.
-/

namespace C1z
namespace Container

open Sync

/-- Payload encodings (`PayloadEncoding` in the manifest). The model
covers only the default. -/
inductive Encoding where
  | indexedZstd
  | tarZstd
  | tar
  deriving Repr, DecidableEq

/-- The engine names the open path accepts. -/
def knownEngines : List String := ["pebble3", "pebble2"]

/-- An artifact as the open path sees it: header integrity, magic,
declared engine, encoding, payload integrity, and the logical state it
carries. The bytes themselves are not modeled. -/
structure Artifact where
  headerIntact : Bool
  magicOk : Bool
  engine : String
  encoding : Encoding
  payloadIntact : Bool
  state : FileState
  deriving Repr, DecidableEq

/-- `Close` on a dirty store: the envelope a v3 writer produces (named `sealArtifact` because `seal` is a Lean keyword). -/
def sealArtifact (s : FileState) : Artifact :=
  { headerIntact := true, magicOk := true, engine := "pebble3", encoding := .indexedZstd,
    payloadIntact := true, state := s }

/-- Deterministic damage a test can apply to a sealed file. -/
inductive Damage where
  | truncateHeader
  | badMagic
  | badEngine
  | flipPayloadByte
  | truncateTail
  deriving Repr, DecidableEq

def damage (a : Artifact) : Damage → Artifact
  | .truncateHeader => { a with headerIntact := false }
  | .badMagic => { a with magicOk := false }
  | .badEngine => { a with engine := "bogus" }
  | .flipPayloadByte => { a with payloadIntact := false }
  | .truncateTail => { a with payloadIntact := false }

inductive OpenError where
  /-- `io.ErrUnexpectedEOF` / `ErrEnvelopeTruncated` before the payload. -/
  | truncated
  /-- `dotc1z.ErrInvalidFile`. -/
  | invalidFile
  /-- `ErrEngineNotAvailable`. -/
  | engineNotAvailable
  /-- A hash mismatch, trailer error, or decompression error at extract. -/
  | payloadCorrupt
  /-- An encoding the model does not cover. -/
  | unsupportedEncoding
  deriving Repr, DecidableEq

/-- An opened store: the engine state plus the read-only flag. -/
structure Opened where
  state : FileState
  readOnly : Bool
  deriving Repr, DecidableEq

inductive OpenOutcome where
  | ok (o : Opened)
  | error (e : OpenError)
  deriving Repr, DecidableEq

/-- `InitCurrentSync`: bind the default sync if one resolves. The binding
is not fresh, and `bindCurrentSync` unseals. -/
def bindDefault (s : FileState) (now : Nat) : FileState :=
  match resolveActiveSync s none now with
  | some id => { s with binding := { bound := some id, fresh := false, sealed := false } }
  | none => s

/-- `NewStore` on an existing path. -/
def openArtifact (a : Artifact) (readOnly : Bool) (now : Nat) : OpenOutcome :=
  if !a.headerIntact then .error .truncated
  else if !a.magicOk then .error .invalidFile
  else if !(knownEngines.contains a.engine) then .error .engineNotAvailable
  else if a.encoding != .indexedZstd then .error .unsupportedEncoding
  else if !a.payloadIntact then .error .payloadCorrupt
  else .ok { state := bindDefault (reopen a.state) now, readOnly := readOnly }

inductive WriteGate' where
  | allowed
  | noCurrentSync
  | engineSealed
  | readOnly
  deriving Repr, DecidableEq

/-- The write gate of an opened store. The adapter tests for a bound sync
first (`PutResources` checks `CurrentSyncID`), and only then does
`withWrite` reach the read-only and sealed checks, so an unbound
read-only store reports `noCurrentSync`, not `readOnly`. -/
def writeGate' (o : Opened) : WriteGate' :=
  match o.state.binding.bound with
  | none => .noCurrentSync
  | some _ =>
    if o.readOnly then .readOnly
    else if o.state.binding.sealed then .engineSealed
    else .allowed

/-! ## Laws -/

theorem run_bindDefault (s : FileState) (now : Nat) : (bindDefault s now).run = s.run := by
  unfold bindDefault; split <;> rfl

theorem hasRecords_bindDefault (s : FileState) (now : Nat) : (bindDefault s now).hasRecords = s.hasRecords := by
  unfold bindDefault; split <;> rfl

/-- The open of a sealed artifact reaches the success branch. -/
theorem openArtifact_seal (s : FileState) (ro : Bool) (now : Nat) :
    openArtifact (sealArtifact s) ro now = .ok { state := bindDefault (reopen s) now, readOnly := ro } := by
  rfl

/-- A sealed artifact reopens: the record and the data are what was sealed. -/
theorem open_seal (s : FileState) (ro : Bool) (now : Nat) :
    ∃ o, openArtifact (sealArtifact s) ro now = .ok o ∧ o.state.run = s.run ∧ o.state.hasRecords = s.hasRecords ∧
      o.readOnly = ro := by
  exact ⟨_, openArtifact_seal s ro now, run_bindDefault _ now, hasRecords_bindDefault _ now, rfl⟩

/-- Every modeled damage fails the open. There is no outcome in which a
damaged artifact opens with fewer records. -/
theorem open_damage_error (s : FileState) (d : Damage) (ro : Bool) (now : Nat) :
    ∃ e, openArtifact (damage (sealArtifact s) d) ro now = .error e := by
  cases d
  · exact ⟨_, rfl⟩
  · exact ⟨_, rfl⟩
  · exact ⟨_, rfl⟩
  · exact ⟨_, rfl⟩
  · exact ⟨_, rfl⟩

/-- The specific error per damage, as the open path reports it. -/
theorem open_damage_class (s : FileState) (ro : Bool) (now : Nat) :
    openArtifact (damage (sealArtifact s) .truncateHeader) ro now = .error .truncated ∧
    openArtifact (damage (sealArtifact s) .badMagic) ro now = .error .invalidFile ∧
    openArtifact (damage (sealArtifact s) .badEngine) ro now = .error .engineNotAvailable ∧
    openArtifact (damage (sealArtifact s) .flipPayloadByte) ro now = .error .payloadCorrupt ∧
    openArtifact (damage (sealArtifact s) .truncateTail) ro now = .error .payloadCorrupt := by
  exact ⟨rfl, rfl, rfl, rfl, rfl⟩

/-- A finished store reopened writable is bound to its sync and accepts
writes without `ResumeSync`. -/
theorem writeGate_publicOpen_finished {s : FileState} {r : SyncRun} (hr : s.run = some r)
    (hf : r.endedAt.isSome) (now : Nat) {o : Opened} (h : openArtifact (sealArtifact s) false now = .ok o) :
    writeGate' o = .allowed := by
  rw [openArtifact_seal, OpenOutcome.ok.injEq] at h
  subst h
  unfold writeGate' bindDefault
  simp only [resolveActiveSync_reopen_finished hr hf now, Bool.false_eq_true, ↓reduceIte]

/-- A read-only open of a finished store refuses writes as read-only: the
sync binds, so the bound check passes and the read-only check fires. -/
theorem writeGate_publicOpen_readOnly {s : FileState} {r : SyncRun} (hr : s.run = some r)
    (hf : r.endedAt.isSome) (now : Nat) {o : Opened} (h : openArtifact (sealArtifact s) true now = .ok o) :
    writeGate' o = .readOnly := by
  rw [openArtifact_seal, OpenOutcome.ok.injEq] at h
  subst h
  unfold writeGate' bindDefault
  simp only [resolveActiveSync_reopen_finished hr hf now, ↓reduceIte]

/-- A read-only open never allows a write, whatever the sync state. -/
theorem writeGate_publicOpen_readOnly_ne_allowed (a : Artifact) (now : Nat) {o : Opened}
    (h : openArtifact a true now = .ok o) : writeGate' o ≠ .allowed := by
  unfold openArtifact at h
  split at h
  · cases h
  · split at h
    · cases h
    · split at h
      · cases h
      · split at h
        · cases h
        · split at h
          · cases h
          · cases h
            unfold writeGate'
            split
            · decide
            · simp only [↓reduceIte]
              decide

/-- Negative result: an unbound read-only store reports "no current sync",
not "read only". The bound check runs first. The live property test found
the model reporting read-only here. -/
theorem writeGate_readOnly_unbound (st : FileState) (hb : st.binding.bound = none) :
    writeGate' { state := st, readOnly := true } = .noCurrentSync := by
  unfold writeGate'
  simp only [hb]

/-- An unfinished store reopened past the cutoff binds nothing: reads
report no current sync and writes are refused. -/
theorem publicOpen_unfinished_stale {s : FileState} {r : SyncRun} (hr : s.run = some r)
    (hu : r.endedAt = none) {now : Nat} (hold : r.startedAt + unfinishedCutoffSeconds < now) {o : Opened}
    (h : openArtifact (sealArtifact s) false now = .ok o) :
    o.state.binding.bound = none ∧ writeGate' o = .noCurrentSync := by
  rw [openArtifact_seal, OpenOutcome.ok.injEq] at h
  subst h
  have hb : bindDefault (reopen s) now = reopen s := by
    unfold bindDefault
    rw [resolveActiveSync_reopen_unfinished hr hu now]
    simp only [Nat.not_le.mpr hold, ↓reduceIte]
  refine ⟨by rw [hb]; rfl, ?_⟩
  unfold writeGate'
  rw [hb]
  rfl

/-- A read-only open yields the same engine state as a writable one; only
the gate differs. -/
theorem openArtifact_readOnly_state (a : Artifact) (now : Nat) {o o' : Opened}
    (h : openArtifact a false now = .ok o) (h' : openArtifact a true now = .ok o') : o.state = o'.state := by
  unfold openArtifact at h h'
  split at h
  · cases h
  rename_i c₁
  split at h
  · cases h
  rename_i c₂
  split at h
  · cases h
  rename_i c₃
  split at h
  · cases h
  rename_i c₄
  split at h
  · cases h
  rename_i c₅
  simp only [c₁, c₂, c₃, c₄, c₅] at h'
  cases h
  cases h'
  rfl

/-- The sealed artifact's logical state is unchanged by sealing. -/
theorem state_seal (s : FileState) : (sealArtifact s).state = s := rfl

/-! ## Witnesses -/

private def finishedFile : FileState :=
  match endSync started 10 with
  | .ok s => s
  | .noCurrentSync => started

example : (openArtifact (sealArtifact finishedFile) false 20 matches .ok _) = true := by decide
example : (match openArtifact (sealArtifact finishedFile) false 20 with
    | .ok o => writeGate' o
    | .error _ => .engineSealed) = .allowed := by decide
example : (match openArtifact (sealArtifact finishedFile) true 20 with
    | .ok o => writeGate' o
    | .error _ => .engineSealed) = .readOnly := by decide
example : openArtifact (damage (sealArtifact finishedFile) .badMagic) false 20 = .error .invalidFile := by decide
example : openArtifact (damage (sealArtifact finishedFile) .truncateTail) false 20 = .error .payloadCorrupt := by decide

end Container
end C1z
