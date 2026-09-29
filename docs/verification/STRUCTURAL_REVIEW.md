# Structural review

The stage in the step-up pipeline that asks whether the code expresses a
model, and whether it is the model the brief states. A correctness review
evaluates an implementation against its own claims; every change order on
CXE-1358 passed that and the sum was a subsystem nobody could describe. This
stage reads the code first and the brief second, and the review is the
difference between the model a stranger recovers from the code and the model
the author meant. It runs on the final code of a change, and on a brief
before code when the brief introduces or moves lifecycle state.

## Independence is the instrument

The pass works only if the reader has no prior model of the code to defend or
explain. That means a new agent session, started for this pass alone, with no
conversation history, no implementation context, no correctness-review
context, and no other agent's findings pasted in. The same model in a session
that already read the code for bugs will explain the code's dependencies
instead of questioning them. Use a model family different from the
implementer's and the correctness reviewer's when you can. Two runs with
their reconstructions diffed are stronger than one; do not give a second run
the first run's output until both exist.

Below is the prompt. Fill the angle-bracketed fields; keep everything else.
Paste it into a fresh session as the first message.

---

```
You are performing a structural review of the pull request on branch <branch>
in <checkout path> (head <SHA>, base <base branch> at <base SHA>). This is a
quality pass: does the code express a model, and is it the model the brief
states. It is not a correctness pass.

This must be your first and only task in this session. If this session has
already read this code for any other purpose, reviewed it for correctness, or
been shown another reviewer's findings, stop and say so instead of proceeding.

RULE 0 — what is and is not a finding here.
A structural finding names a model, an owner, a representation, a boundary, or
a duplicate. Anything whose consequence is behavioral — a crash-recovery
outcome, a wrong or missing durable write, an ordering of commits — is not a
finding here even if you are confident it is a bug. Put it under "handed to
correctness" as one line (file:line, one clause) and stop analyzing it.

RULE 1 — landed versus pending. Treat the following as the current contract:
  landed:  <change orders implemented at this head>
  pending: <frozen change orders not yet implemented; do not report the code
            for not matching them>

PART ONE — RECONSTRUCTION. Do this before reading any document under docs/.
Read only the production diff:
  git diff <base>...HEAD -- . ':!*_test.go' ':!vendor/*' ':!*.pb.go' ':!docs/*'
and the production files it touches, in full. Do not read doc comments as
authority; read them as claims the code may or may not honor. Then write, in
at most one page, the model you infer:
  - the states the subsystem can be in, durable and in-memory, and how you
    would read each from the store;
  - the transitions between them, which function commits each, and what each
    is one atomic unit of;
  - the owners: for each piece of shared state, which goroutine or phase
    writes it and which read it;
  - the invariants the code enforces, with the check that enforces each;
  - the decisions: every place the code chooses between behaviors, and what
    it chooses on. Count how many places choose on the same fact.
Mark every item you could not determine from the code as "not recoverable",
with the file:line where you looked. Name what you would have to be told.

PART TWO — THE INTENDED MODEL. Now read, in this order:
  1. AGENTS.md — "Surface the decisions review cannot see", "Verified or
     assumed".
  2. <implementation brief path> — the state table (states, what each
     accepts, transitions with guards), the ownership table, the state
     inventory.
  3. <plan path> §11 — the change orders named in RULE 1.
  4. <requester brief path>, its structural-rules section.
  5. The primitive registries: pkg/sync/sync_primitives_meta_test.go and
     pkg/dotc1z/engine/pebble/sync_primitives_meta_test.go.
If the brief has no state table for a subsystem whose lifecycle the change
touches, say so in the report's first line; the missing table is a finding
with a read cost, and Part Three compares against the model you recovered.

PART THREE — THE DIFF. Compare your reconstruction to the intended model,
item by item. Each of these is a finding:
  - a state, transition, owner or invariant in the brief that you could not
    recover from the code ("not recoverable" in Part One): the code does not
    express its model there;
  - one you recovered that the brief does not state: the code has a model the
    author did not write down, or did not know it had;
  - one where the two disagree;
  - a decision made in more places than the brief names, or on a fact the
    brief does not name as the authority for it;
  - an interface whose method names, read without their comments, do not
    yield the states and transitions of the table (write the machine the
    names imply; each mismatch is one finding for the interface as a whole).
Every finding states its cost in one of three forms, or it is not a finding:
  bug class:  the state or ordering it makes possible that a single owner or
              a single representation would make impossible;
  change tax: the second place every future change must remember, and what
              happens when it is forgotten;
  read cost:  the fact a reader must be told that the code could carry
              instead, and where they first need it.
Order the findings by cost. At most seven. Fewer is fine; zero is a result and
says so. Each is: <what the code says> / <what the brief says> / <cost> /
<sites> / <fix in one clause>.

PART FOUR — APPENDIX. The per-declaration enumeration, for diffing runs and
for the removal-day checklist. One row per declaration; derived IDs
(A-<Type>.<field>, B-<Type>.<field>, C-<key or fact>, D-<file:line>,
E-<Interface>.<Method>, F-<file:line>, G-<counter>, H-<file:line>).
  A. Fields on the core structs (pkg/sync: syncer, ledgerRuntime, runState,
     runStats; engine: Engine, Ledger) and on any new struct: owner, lifetime,
     whether the struct has that owner and lifetime.
  B. sync.*/atomic.* declarations the registries do not mark "predates": the
     two goroutines that interleave, or "none".
  C. Durable keys, facts, markers: the question each answers, writer,
     readers, any other record answering the same question.
  D. Write sites fanning out to two containers; types holding state another
     type holds; facts copied into a second in-memory map.
  E. Methods added to a store interface or the test-hook struct: the one
     production caller, or "no production caller".
  F. Production hunks outside the files the brief allotted; changed files the
     brief froze. Requester-waived: "justified: requester waived".
  G. Counts against base: fields per core struct, test hooks, capability
     entries, keyspace kinds, fact constants, interface methods, proto fields.
  H. Comments and names against docs/COMMENTS.md and AGENTS.md diction.
     Appendix only; fixed in bulk, never a finding.
verdict ∈ {justified, misplaced, duplicate, unexplained, consolidate, split}.
Appendix rows are recorded, not fixed, unless a finding cites them.

Then two lists:
  - claims in the brief's tables the code contradicts (cite both sides);
  - handed to correctness (file:line and a clause; no analysis).

Closure is zero findings in Part Three. A Part One with more than three "not
recoverable" items is not closable by fixing rows; it returns to the brief
stage, because the code does not carry its model.

RULES. Cite file:line for every claim about existing code, from a read in this
session; mark anything unread "not checked". Do not run or change code. Do not
commit. Write the result to /tmp/structural-review.md and return it inline,
Part One first and unedited after Part Two — the reconstruction is evidence
only if it was written before the brief was read.
```

---

Running it: see "Independence is the instrument" above. Update the head SHA
and RULE 1 for every run.

What earlier runs taught, in order. Without RULE 0 the reviewers drifted into
correctness and named a lifecycle bug as the top finding. Without RULE 1 a
frozen-but-unimplemented change order was read as the current contract.
Without the granularity rule two reviewers grouped findings at different
levels and the tables could not be diffed. The first version made the table
the deliverable and zero rows the closure; that rewards volume. The second
version gated findings on cost; that fixed volume and still missed the
condition under every instance: the code had been built as thirty-nine local
change orders with no stated model, each verified against its own claim, and a
sectioned checklist can only find the defect classes it lists. Reconstruction
first is the answer to that: the review measures how much of the model a
stranger can recover from the code, and the sections become the appendix.
