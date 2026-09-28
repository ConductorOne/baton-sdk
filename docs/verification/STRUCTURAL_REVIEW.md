# Structural review

The stage in the step-up pipeline that reads the ownership table, the state
inventory and the code against each other. It exists because a correctness
review evaluates an implementation against itself: a correctly held lock on
single-owner state and a marker read consistently everywhere both pass it.
This stage asks, for every structural decision, should it exist and is it in
the right place. It runs twice: on the implementation brief before code, and
on the final code. Closure is a table with no open rows.

## Independence is the instrument

The pass works only if the reader has no prior model of the code to defend or
explain. That means a new agent session, started for this pass alone, with no
conversation history, no implementation context, no correctness-review
context, and no other agent's findings pasted in. The same model in a session
that already read the code for bugs will explain the code's dependencies
instead of questioning them; that is what two correctness reviews did on
CXE-1358, and what a stakeless read found in minutes. Use a model family
different from the implementer's and the correctness reviewer's when you can.
Two such runs with their tables diffed are stronger than one, because every row
carries a file:line and a stated difference and disagreements settle against
source. Do not give a second run the first run's table until both exist.

Below is the prompt. Fill the angle-bracketed fields; keep everything else.
Paste it into a fresh session as the first message.

---

```
You are performing a structural review of the pull request on branch <branch>
in <checkout path> (head <SHA>, base <base branch> at <base SHA>). This is a
quality pass: for every structural decision, should it exist, and is it in the
right place. It is not a correctness pass.

This must be your first and only task in this session. If this session has
already read this code for any other purpose, reviewed it for correctness, or
been shown another reviewer's findings, stop and say so instead of proceeding;
the pass depends on a reader with nothing to defend and nothing to confirm.

RULE 0 — what is and is not a finding here.
A structural finding names an owner, a representation, a boundary, or a
duplicate. Anything whose consequence is behavioral — a crash-recovery outcome,
a wrong or missing durable write, an artifact that differs, an ordering of
commits — is not a finding here even if you are confident it is a bug. Put it
under "handed to correctness" as one line (file:line, one clause) and stop
analyzing it. If a row's "why" mentions a crash, a resume, a durable image, or
an observable outcome, move it to the handoff. A correctly implemented lock on
single-owner state, or a marker read consistently everywhere, IS a finding here
even though no test can fail on it.

RULE 1 — landed versus pending. The plan's change orders are claims; not all
are implemented at this head. Treat the following as the current contract:
  landed:  <change orders implemented at this head>
  pending: <frozen change orders not yet implemented; do not report the code
            for not matching them>
Where the code implements a lifecycle a pending change order replaces, that is
expected. You may note "pending <CO>" in a row's why; do not grade it.

READ FIRST, in this order, before any production code:
1. AGENTS.md — "Surface the decisions review cannot see", "Verified or assumed".
2. docs/REVIEW_CHECKLIST.md — "Briefs and reports", "Syncer structure".
3. docs/COMMENTS.md.
4. <implementation brief path> — ownership table, state inventory. These are the
   claims you check the code against.
5. <plan path> §11 — the change orders named in RULE 1.
6. <requester brief path>, its structural-rules section. If any listed file is
   absent from your checkout, say so in the report's first line and proceed
   using the checklist's "Syncer structure" section as the structural rules; do
   not infer the brief's content.
7. The primitive registries: pkg/sync/sync_primitives_meta_test.go and
   pkg/dotc1z/engine/pebble/sync_primitives_meta_test.go, including their
   "remove:" entries.
Then: git diff <base>...HEAD -- . ':!*_test.go' ':!vendor/*' ':!*.pb.go' ':!docs/*'
Test files are out of scope for every section except where a section names them.

GRANULARITY. One row per declaration: one field, one primitive, one durable key
or fact, one method, one context key, one wrapper function, one repeated
sequence. Do not group; a group is several rows sharing a prefix. Row IDs are
derived and stable: A-<Type>.<field>, B-<Type>.<field>, C-<key or fact name>,
D-<file:line of the fan-out site>, E-<Interface>.<Method>, F-<file:line>,
G-<counter name>, H-<file:line>, I<n>-<concept>. Two reviewers on the same head
must produce the same IDs.

IMPACT. Quality is not the product; the sync is. A finding earns the report's
front page only by stating what it costs to leave as is, in one of three forms:
  bug class:  the state or ordering it makes possible that a single owner or a
              single representation would make impossible (name the class:
              divergence of two containers, a write outside the lock's
              invariant, a stale mirror read after the source changed).
  change tax: the second place every future change must remember, and what
              happens when it is forgotten (name the change and the symptom).
  read cost:  the fact a reader must hold in their head that the code could
              carry instead, and where they first need it.
A row with none of these is recorded in the appendix and is not a closure
item. Comments, names and diction (section H) are appendix-only; they are
fixed in bulk without discussion and never lead. If two findings share a
cause, report the cause once and list the sites under it; the four findings
that mattered on this branch (duplicate run accounting, locks on single-owner
state, one struct with two owners, an interface with two caller groups) were
each one cause with several sites.

SECTIONS. For the whole feature diff, not only the latest commits.

A. Fields. Every field added to the core structs the checklist names (for
   pkg/sync: syncer, ledgerRuntime, runState, runStats; for the engine: Engine,
   Ledger) or to any new struct: owner (goroutine or phase), lifetime, and
   whether the struct it sits on has that owner and lifetime. Compare to the
   ownership table; report rows the table lacks and rows the code contradicts.
B. Primitives. Every sync.*/atomic.* declaration the registries do not mark
   "predates the registry": the two goroutines that interleave on it, or
   "none". Confirm or dispute each "remove:" reason.
C. Durable state. Every new key, fact, or marker in the state inventory: the
   question it answers, writer, readers, and any other record answering the
   same question. A question with two answers is a row; its behavioral
   consequences are not.
D. Duplication. Every write site that fans out to two containers; every new
   type that holds state an existing type holds; every fact copied into a
   second in-memory map.
E. Contract surface. Every method added to a store interface or the test-hook
   struct: the single production caller that needed it, or "no production
   caller", and whether an existing method already served.
F. Frozen paths. Production hunks only. Report every production hunk outside
   the files the brief allotted to the change that is not inside the brief's
   fork predicate, and every changed production file the brief froze. Where
   the requester has waived these, the verdict is "justified: requester waived"
   and the row exists for the removal-day checklist.
G. Counts against base, one row per counter: fields on each core struct; test
   hooks; capability entries; keyspace kinds; fact-name constants; store
   interface methods; proto messages and fields. "why" names the change order
   that justified the delta or "none".
H. Comments and names. Against docs/COMMENTS.md and AGENTS.md diction: comments
   that restate the code, comments that should be a name or a test, terms on
   the hard-ban or minimize lists. Production files only.
I. Abstractions. Each item is a signal; name the concept, count the
   representations or sites, state whether one would do. Enumerate
   independently; the calibration examples in the brief set the granularity
   and must appear in your table, but they are not the answer key.
   I1. One concept, many representations, and the conversion functions between
       them. Which are storage shape, which in-memory, which exist because a
       conversion was easier than a canonical choice.
   I2. Parameters passed through context. Every context key type and what it
       carries; a value the callee cannot run without is a parameter.
   I3. Wrappers whose body is a mode switch: an authorized temporary fork (name
       the authorization) or a missing interface with two implementations.
   I4. Fat interfaces. Group methods by caller; production callers per group;
       whether a consumer of one group must depend on the others.
   I5. Leaky types. Fields mutated directly outside their type's file.
   I6. Missing abstraction. Repeated sequences with local variation; quote
       once, list sites; say whether one function would remove the variation
       or hide a real difference.
   I7. Incorrect abstraction. Name promises one thing, callers need another;
       list callers and what each uses.
   I8. Abstraction the brief forbade, and whether each instance crosses the
       boundary the brief drew. Where waived: "justified: requester waived".

OUTPUT. First, FINDINGS: at most seven, ordered by cost, each as
  <cause> — <impact form>: <the cost in one sentence> — sites: <row ids>
  — fix: <one clause>.
Fewer is fine; zero is a valid result and says so. Then the APPENDIX table,
complete, for diffing against the next run:
  | id | decision | file:line | concept / owner / authority claimed | verdict | why |
verdict ∈ {justified, misplaced, duplicate, unexplained, consolidate, split}.
  unexplained: no table, change order, or registry entry states the reason.
  consolidate: several representations or sites where one would do.
  split: one type or interface serving callers with different needs.
  justified: only when "why" states the real difference the extra
  representation, parameter route, wrapper, or interface breadth preserves. A
  justification you cannot state makes the row "unexplained"; a preference
  without a named concept, a count, and a stated difference does not get a
  verdict at all and is omitted.
Then three sections:
  - claims in the tables the code contradicts (cite both sides)
  - structural decisions the code makes that no table, change order, or
    registry mentions
  - handed to correctness (one line each: file:line and a clause; no analysis)
Closure is zero FINDINGS. Appendix rows are recorded, not fixed, unless a
later change touches them. Fixes are at most one clause per finding;
placement and representation fixes return to the brief stage.

RULES. Cite file:line for every claim about existing code, from a read in this
session; mark anything unread "not checked". Do not run or change code. Do not
commit. Write the result to /tmp/structural-review.md and return it inline.
```

---

Running it: see "Independence is the instrument" above. Update the head SHA
and RULE 1 for every run.

What earlier runs taught: without RULE 0 the reviewers drifted into
correctness and named a lifecycle bug as the top finding; without RULE 1 a
frozen-but-unimplemented change order was read as the current contract and the
code was reported for not matching it; without the granularity rule two
reviewers grouped findings at different levels and the tables could not be
diffed. The first version of this prompt made the table the deliverable and
zero rows the closure; that rewards volume, and a row costs the requester's
attention whether or not it costs the product anything. The IMPACT rule and
the seven-finding cap came from that.
