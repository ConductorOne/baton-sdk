/-!
# Sync lifecycle of a v3 file

Models the sync-run state of one Pebble c1z, from
`pkg/dotc1z/engine/pebble/adapter.go` (`StartNewSync`, `ResumeSync`,
`SetCurrentSync`, `EndSync`), `engine.go` (`syncBinding`, `withWrite`,
`requireCurrentSync`), `sync_runs.go` (`LatestFinishedSyncRecord`,
`LatestUnfinishedSyncRecord`), `cleanup.go` (`ResetForNewSync`) and
`adapter_reader.go` (`resolveActiveSyncForReader`).

The central fact: a v3 file holds exactly one sync-run record at a fixed
key, and no data key carries a sync id. There is no multi-sync history,
no overlay, and no per-sync isolation to model inside one file.
"Selecting" a sync is a gate (some id must resolve) plus metadata.

What this module states, each as the implementation actually behaves:

- the write gate: record writes need a bound sync and an unsealed engine;
- `EndSync` seals and stamps `ended_at`; a later `ResumeSync` on the
  finished record rebinds and unseals, so a finished sync is NOT
  immutable (`TestResumeSyncOnEndedSyncAllowsWrites`);
- `StartNewSync` wipes the keyspace, so the previous sync's records are
  gone, not hidden;
- latest-finished selection over the single record, with the type filter;
- the reader's default-sync resolution order and its 7-day unfinished
  fallback.

Out of scope: `StartOrResumeSync`'s wipe on an unknown explicit id
(documented in README as a producer-facing hazard), the compactor's
cross-file merge, `CloneSync`, the ledger, and the id-index migration
that can re-key or drop rows at `Open` (README, "Non-guarantees").
Time is an abstract `Nat` of seconds.
-/

namespace C1z
namespace Sync

/-- `SyncRunRecord.type` (records.proto). -/
inductive SyncType where
  | unspecified
  | full
  | partialSync
  | resourcesOnly
  deriving Repr, DecidableEq

/-- The single persisted sync-run record. State is derived from
`endedAt`: `none` is in progress, `some` is finished. There is no
discarded state. -/
structure SyncRun where
  id : String
  syncType : SyncType
  startedAt : Nat
  endedAt : Option Nat
  deriving Repr, DecidableEq

def SyncRun.finished (r : SyncRun) : Bool := r.endedAt.isSome

/-- In-memory engine binding (`syncBinding{id, fresh, sealed}`). `fresh`
is set by `MarkFreshSync` (the `StartNewSync` path only) and cleared by
`bindCurrentSync` (`ResumeSync`) and `FinishSync` (`EndSync`). -/
structure Binding where
  bound : Option String
  fresh : Bool
  sealed : Bool
  deriving Repr, DecidableEq

/-- Whole-file sync state: the persisted record, the binding, and whether
the keyspace currently holds any record data. -/
structure FileState where
  run : Option SyncRun
  binding : Binding
  hasRecords : Bool
  deriving Repr, DecidableEq

/-- The state of a freshly opened file: unbound, unsealed (`engine.go` Open). -/
def opened (run : Option SyncRun) (hasRecords : Bool) : FileState :=
  { run := run, binding := { bound := none, fresh := false, sealed := false }, hasRecords := hasRecords }

inductive WriteGate where
  | allowed
  | noCurrentSync
  | engineSealed
  deriving Repr, DecidableEq

/-- A record write needs a bound, unsealed engine. The bound check runs
first (`PutResources` tests `CurrentSyncID() == ""` before `withWrite`
reaches `checkWritableLocked`), so an unbound sealed engine reports
`noCurrentSync`, not `engineSealed`. Read-only and closing engines are
out of scope. -/
def writeGate (s : FileState) : WriteGate :=
  match s.binding.bound with
  | none => .noCurrentSync
  | some _ => if s.binding.sealed then .engineSealed else .allowed

inductive StartResult where
  | ok (s : FileState)
  /-- `ResetForNewSync: refusing to reset while a sync is in progress`. -/
  | syncInProgress
  deriving Repr, DecidableEq

/-- `StartNewSync`: refuse while a fresh sync is in progress; otherwise wipe
the keyspace, bind a fresh id, and write a new record. A sync bound by
`ResumeSync` is not fresh, so `StartNewSync` replaces it without refusal. -/
def startNewSync (s : FileState) (id : String) (t : SyncType) (now : Nat) : StartResult :=
  if s.binding.fresh then .syncInProgress
  else .ok
    { run := some { id := id, syncType := t, startedAt := now, endedAt := none }
      binding := { bound := some id, fresh := true, sealed := false }
      hasRecords := false }

inductive ResumeResult where
  | ok (s : FileState)
  | notFound
  deriving Repr, DecidableEq

/-- `ResumeSync(id)`: rebind if the record exists, whatever its state. -/
def resumeSync (s : FileState) (id : String) : ResumeResult :=
  match s.run with
  | some r =>
    if r.id = id then .ok { s with binding := { bound := some id, fresh := false, sealed := false } }
    else .notFound
  | none => .notFound

inductive EndResult where
  | ok (s : FileState)
  | noCurrentSync
  deriving Repr, DecidableEq

/-- `EndSync`: stamp `ended_at`, seal, unbind. Finalize failure is out of scope. -/
def endSync (s : FileState) (now : Nat) : EndResult :=
  match s.binding.bound, s.run with
  | some _, some r =>
    .ok { s with
      run := some { r with endedAt := some now }
      binding := { bound := none, fresh := false, sealed := true } }
  | _, _ => .noCurrentSync

/-- `Close` then `Open` on a cleanly closed file: the persisted record and
the data survive; the binding resets to unbound, not fresh, and NOT
sealed (`Open` stores an empty `syncBinding`; in-process `EndSync` leaves
`sealed = true`). `Open` on such a file writes no rows and rebuilds no
index: the id-index layout is already current, the migration registry is
empty, and the digest-build marker exists only after a crash. -/
def reopen (s : FileState) : FileState :=
  { s with binding := { bound := none, fresh := false, sealed := false } }

/-- `PutSyncRunRecord` rewriting `started_at`, the seam a test uses to
place an unfinished record on either side of the 7-day cutoff. -/
def setStartedAt (s : FileState) (t : Nat) : FileState :=
  { s with run := s.run.map fun r => { r with startedAt := t } }

/-- Record a data write (the model does not track record contents here). -/
def recordWrite (s : FileState) : FileState := { s with hasRecords := true }

/-! ## Selection -/

/-- `LatestFinishedSyncRecord` with an optional type filter, over the one
record a file holds. -/
def latestFinished (s : FileState) (filter : Option SyncType) : Option SyncRun :=
  match s.run with
  | some r =>
    if r.finished && (match filter with | none => true | some t => r.syncType == t) then some r else none
  | none => none

/-- `LatestUnfinishedSyncRecord`: in progress and started within the cutoff. -/
def unfinishedCutoffSeconds : Nat := 7 * 24 * 60 * 60

def latestUnfinished (s : FileState) (now : Nat) : Option SyncRun :=
  match s.run with
  | some r =>
    if !r.finished && now ≤ r.startedAt + unfinishedCutoffSeconds then some r else none
  | none => none

/-- `resolveActiveSyncForReader`: annotation, then bound sync, then latest
finished, then recent unfinished. `none` becomes `ErrNoCurrentSync`. -/
def resolveActiveSync (s : FileState) (annotation : Option String) (now : Nat) : Option String :=
  match annotation with
  | some a => some a
  | none =>
    match s.binding.bound with
    | some b => some b
    | none =>
      match latestFinished s none with
      | some r => some r.id
      | none => (latestUnfinished s now).map (·.id)

/-! ## Write gate -/

theorem writeGate_opened (run : Option SyncRun) (hr : Bool) : writeGate (opened run hr) = .noCurrentSync := by
  rfl

theorem writeGate_startNewSync {s s' : FileState} {id : String} {t : SyncType} {now : Nat}
    (h : startNewSync s id t now = .ok s') : writeGate s' = .allowed := by
  unfold startNewSync at h
  split at h
  · cases h
  · cases h
    rfl

/-- `StartNewSync` is refused while a fresh sync is in progress. -/
theorem startNewSync_refused_of_fresh (s : FileState) (id : String) (t : SyncType) (now : Nat)
    (h : s.binding.fresh = true) : startNewSync s id t now = .syncInProgress := by
  unfold startNewSync
  simp only [h, ite_true]

/-- A second `StartNewSync` directly after a first is refused. -/
theorem startNewSync_startNewSync {s s' : FileState} {id id' : String} {t t' : SyncType} {now now' : Nat}
    (h : startNewSync s id t now = .ok s') : startNewSync s' id' t' now' = .syncInProgress := by
  unfold startNewSync at h
  split at h
  · cases h
  · cases h
    rfl

/-- After `EndSync`, `StartNewSync` is accepted again. -/
theorem startNewSync_endSync {s s' : FileState} {now : Nat} (h : endSync s now = .ok s')
    (id : String) (t : SyncType) (now' : Nat) : ∃ s'', startNewSync s' id t now' = .ok s'' := by
  unfold endSync at h
  split at h
  · cases h
    exact ⟨_, rfl⟩
  · cases h

/-- Negative result: a sync reopened by `ResumeSync` is not fresh, so a
following `StartNewSync` wipes it without refusal. -/
theorem startNewSync_resumeSync {s s' : FileState} {id : String} (h : resumeSync s id = .ok s')
    (id' : String) (t : SyncType) (now : Nat) : ∃ s'', startNewSync s' id' t now = .ok s'' := by
  unfold resumeSync at h
  split at h
  · split at h
    · cases h
      exact ⟨_, rfl⟩
    · cases h
  · cases h

/-- After `EndSync` the engine is unbound and sealed. Record writes are
refused, and the refusal observed through the adapter is `noCurrentSync`
because the bound check runs first. The conformance case "end unbinds
writes" pins this against the engine. -/
theorem writeGate_endSync {s s' : FileState} {now : Nat} (h : endSync s now = .ok s') :
    writeGate s' = .noCurrentSync := by
  unfold endSync at h
  split at h
  · cases h
    rfl
  · cases h

/-- `engineSealed` is reported only for a bound, sealed engine, a state
`EndSync` passes through before it clears the binding. -/
theorem writeGate_engineSealed_iff (s : FileState) :
    writeGate s = .engineSealed ↔ s.binding.bound.isSome ∧ s.binding.sealed = true := by
  unfold writeGate
  cases s.binding.bound <;> cases s.binding.sealed <;> simp

/-- `EndSync` marks the record finished. -/
theorem finished_endSync {s s' : FileState} {now : Nat} (h : endSync s now = .ok s') :
    ∃ r, s'.run = some r ∧ r.endedAt = some now := by
  unfold endSync at h
  split at h
  · cases h
    exact ⟨_, rfl, rfl⟩
  · cases h

/-- `EndSync` with no bound sync is refused. -/
theorem endSync_unbound (s : FileState) (now : Nat) (h : s.binding.bound = none) :
    endSync s now = .noCurrentSync := by
  unfold endSync
  split
  · simp_all
  · rfl

/-- Negative result: `ResumeSync` on a finished record reopens it for
writes. A finished sync is not immutable. -/
theorem writeGate_resumeSync_finished {s s' : FileState} {id : String}
    (h : resumeSync s id = .ok s') : writeGate s' = .allowed := by
  unfold resumeSync at h
  split at h
  · split at h
    · cases h
      rfl
    · cases h
  · cases h

/-- `ResumeSync` with an unknown id fails closed. -/
theorem resumeSync_notFound {s : FileState} {id : String}
    (h : ∀ r, s.run = some r → r.id ≠ id) : resumeSync s id = .notFound := by
  unfold resumeSync
  split
  · next r hr =>
    simp only [h r hr, ↓reduceIte]
  · rfl

/-- `StartNewSync` leaves no record data behind. -/
theorem hasRecords_startNewSync {s s' : FileState} {id : String} {t : SyncType} {now : Nat}
    (h : startNewSync s id t now = .ok s') : s'.hasRecords = false := by
  unfold startNewSync at h
  split at h
  · cases h
  · cases h
    rfl

/-- Reseal: `ResumeSync` on a finished record and a second `EndSync`
overwrite `ended_at`. The record was finished at `t₁`; after the reseal
it reads as finished at `t₂`, under the same id. -/
theorem endSync_resumeSync_endSync {s₁ s₂ : FileState} {r : SyncRun} {t₁ t₂ : Nat}
    (hr : s₁.run = some r) (hf : r.endedAt = some t₁) (h₂ : resumeSync s₁ r.id = .ok s₂) :
    ∃ s₃ r', endSync s₂ t₂ = .ok s₃ ∧ s₃.run = some r' ∧ r'.endedAt = some t₂ ∧ r'.id = r.id ∧
      (t₁ ≠ t₂ → r'.endedAt ≠ r.endedAt) := by
  unfold resumeSync at h₂
  rw [hr] at h₂
  simp only [↓reduceIte, ResumeResult.ok.injEq] at h₂
  subst h₂
  refine ⟨_, _, rfl, rfl, rfl, rfl, ?_⟩
  intro hne h
  rw [hf] at h
  exact hne (Option.some.inj h).symm

/-- Replacement hides: after `StartNewSync` nothing is finished, whatever
the previous record was. -/
theorem latestFinished_startNewSync {s s' : FileState} {id : String} {t : SyncType} {now : Nat}
    (h : startNewSync s id t now = .ok s') (f : Option SyncType) : latestFinished s' f = none := by
  unfold startNewSync at h
  split at h
  · cases h
  · cases h
    rfl

/-! ## Reopen -/

/-- After reopen the engine is unbound: record writes are refused as
"no current sync" until a rebind. -/
theorem writeGate_reopen (s : FileState) : writeGate (reopen s) = .noCurrentSync := by
  rfl

/-- Reopen keeps the record and the data. -/
theorem run_reopen (s : FileState) : (reopen s).run = s.run := by
  rfl

theorem hasRecords_reopen (s : FileState) : (reopen s).hasRecords = s.hasRecords := by
  rfl

/-- A reopened finished sync resolves as the default sync for reads. -/
theorem resolveActiveSync_reopen_finished {s : FileState} {r : SyncRun} (hr : s.run = some r)
    (hf : r.endedAt.isSome) (now : Nat) : resolveActiveSync (reopen s) none now = some r.id := by
  unfold resolveActiveSync latestFinished reopen
  simp only [hr, SyncRun.finished, hf, Bool.true_and, ↓reduceIte]

/-- A reopened unfinished sync resolves for reads only while its start is
within the cutoff. -/
theorem resolveActiveSync_reopen_unfinished {s : FileState} {r : SyncRun} (hr : s.run = some r)
    (hu : r.endedAt = none) (now : Nat) :
    resolveActiveSync (reopen s) none now = (if now ≤ r.startedAt + unfinishedCutoffSeconds then some r.id else none) := by
  unfold resolveActiveSync latestFinished latestUnfinished reopen
  by_cases hle : now ≤ r.startedAt + unfinishedCutoffSeconds
  · simp only [hr, SyncRun.finished, hu, Option.isSome_none, Bool.false_and, Bool.false_eq_true,
      ↓reduceIte, Bool.not_false, Bool.true_and, decide_eq_true_eq, hle, Option.map_some]
  · simp only [hr, SyncRun.finished, hu, Option.isSome_none, Bool.false_and, Bool.false_eq_true,
      ↓reduceIte, Bool.not_false, Bool.true_and, decide_eq_true_eq, hle, Option.map_none]

/-- `ResumeSync` after reopen rebinds and allows writes. -/
theorem writeGate_resumeSync_reopen {s s' : FileState} {r : SyncRun} (hr : s.run = some r)
    (h : resumeSync (reopen s) r.id = .ok s') : writeGate s' = .allowed := by
  unfold resumeSync reopen at h
  simp only [hr, ↓reduceIte, ResumeResult.ok.injEq] at h
  subst h
  rfl

/-- Negative result: a reopened binding is never fresh, so `StartNewSync`
after reopen is accepted even over an unfinished sync, and wipes it. This
is the path `StartOrResumeSync` takes once an unfinished record ages past
the cutoff. -/
theorem startNewSync_reopen (s : FileState) (id : String) (t : SyncType) (now : Nat) :
    ∃ s', startNewSync (reopen s) id t now = .ok s' ∧ s'.hasRecords = false := by
  exact ⟨_, rfl, rfl⟩

/-! ## Selection laws -/

/-- An unfinished record is never selected as latest finished. -/
theorem latestFinished_none_of_unfinished {s : FileState} {r : SyncRun} (hr : s.run = some r)
    (h : r.endedAt = none) (f : Option SyncType) : latestFinished s f = none := by
  unfold latestFinished
  simp only [hr, SyncRun.finished, h, Option.isSome_none, Bool.false_and, Bool.false_eq_true, ↓reduceIte]

/-- The type filter is exact. -/
theorem latestFinished_type {s : FileState} {r : SyncRun} {t : SyncType}
    (h : latestFinished s (some t) = some r) : r.syncType = t := by
  unfold latestFinished at h
  cases hrun : s.run with
  | none => simp only [hrun, reduceCtorEq] at h
  | some r' =>
    simp only [hrun, Option.ite_none_right_eq_some, Option.some.injEq, Bool.and_eq_true, beq_iff_eq] at h
    obtain ⟨⟨_, ht⟩, rfl⟩ := h
    exact ht

/-- Latest-finished only returns the file's record, finished. -/
theorem latestFinished_spec {s : FileState} {r : SyncRun} {f : Option SyncType}
    (h : latestFinished s f = some r) : s.run = some r ∧ r.finished = true := by
  unfold latestFinished at h
  cases hrun : s.run with
  | none => simp only [hrun, reduceCtorEq] at h
  | some r' =>
    simp only [hrun, Option.ite_none_right_eq_some, Option.some.injEq, Bool.and_eq_true] at h
    obtain ⟨⟨hf, _⟩, rfl⟩ := h
    exact ⟨rfl, hf⟩

/-- Resolution never invents an id: it is the annotation, the bound id, or
the record's id. -/
theorem resolveActiveSync_source (s : FileState) (annotation : Option String) (now : Nat) {id : String}
    (h : resolveActiveSync s annotation now = some id) :
    annotation = some id ∨ s.binding.bound = some id ∨ (∃ r, s.run = some r ∧ r.id = id) := by
  unfold resolveActiveSync at h
  split at h
  · cases h
    exact Or.inl rfl
  · split at h
    · next b hb =>
      cases h
      exact Or.inr (Or.inl hb)
    · split at h
      · next r hr =>
        cases h
        exact Or.inr (Or.inr ⟨r, (latestFinished_spec hr).1, rfl⟩)
      · unfold latestUnfinished at h
        split at h
        · next r hr =>
          split at h
          · cases h
            exact Or.inr (Or.inr ⟨r, hr, rfl⟩)
          · cases h
        · cases h

/-- With no annotation and no binding, an unfinished record older than the
cutoff does not resolve: the reader reports `ErrNoCurrentSync`. -/
theorem resolveActiveSync_none_of_stale {s : FileState} {r : SyncRun} {now : Nat}
    (hb : s.binding.bound = none) (hr : s.run = some r) (hu : r.endedAt = none)
    (hold : r.startedAt + unfinishedCutoffSeconds < now) :
    resolveActiveSync s none now = none := by
  have hlt : ¬ now ≤ r.startedAt + unfinishedCutoffSeconds := Nat.not_le.mpr hold
  unfold resolveActiveSync latestFinished latestUnfinished
  simp only [hb, hr, SyncRun.finished, hu, Option.isSome_none, Bool.false_and, Bool.false_eq_true,
    ↓reduceIte, Bool.not_false, Bool.true_and, decide_eq_true_eq, hlt, Option.map_none]

/-- An annotation wins over everything, including a missing record. This
is the honest statement that the resolved id is a gate, not a check. -/
theorem resolveActiveSync_annotation (s : FileState) (a : String) (now : Nat) :
    resolveActiveSync s (some a) now = some a := by
  rfl

/-! ## Witnesses -/

/-- A started file, for the witnesses below. -/
def started : FileState :=
  match startNewSync (opened none false) "s1" .full 0 with
  | .ok s => s
  | .syncInProgress => opened none false

example : writeGate started = .allowed := by decide
example : startNewSync started "s2" .full 1 = .syncInProgress := by decide
example :
    (match endSync started 10 with
      | .ok s => writeGate s
      | .noCurrentSync => .allowed) = .noCurrentSync := by decide
example :
    (match endSync started 10 with
      | .ok s => latestFinished s (some .partialSync)
      | .noCurrentSync => none) = none := by decide
example :
    (match endSync started 10 with
      | .ok s => (startNewSync s "s2" .full 11 matches .ok _)
      | .noCurrentSync => false) = true := by decide

end Sync
end C1z
