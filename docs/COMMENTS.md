# Comments

For agents writing or reviewing code comments in this repository. Diction
per `AGENTS.md`.

## Principles

Exercise judgment, not procedure. Our a priori estimate is that a comment
is worse than nothing most of the time. Everything below is what usually
follows from that prior; none of it is a rule that overrides the case in
front of you.

- The code is the authoritative description of what the code does and what
  it is intended to do: names, types, structure, and to a lesser extent
  tests.
- A comment earns its keep by stating something that is true that the code 
  does not express that the reader ought to know.
- A comment is a very strong signal that something the reader ought to know 
  is not expressed by the code. That is chiefly a flaw in the code, not the
  comment. Ask whether the code could carry it (a name, a function, a type,
  a test) and whether that change is worth making. Often it is. Sometimes
  it is too much effort, too disruptive, or touches an API you do not own.
  Ideally, only write a comment when you want to make such a signal to the
  reader.
- Our target audience is _not current us and not the reviewer_. Our
  audience is future us: someone opening the file tomorrow or months from
  now without today's context. Comments are an ongoing maintenance burden
  and often lead future us in the wrong direction or waste our time
  verifying correctness.
- Comments are often wrong *when written*. Review rounds spent on their
  wording are entirely wasted. A comment that does not exist cannot be
  wrong.
- A typical change adds zero comments (or may even net remove some at this
  point).
- A comment is judged in context, not alone. Every comment can pass on its
  own while the file has three times too many; comments compete for
  attention. A fact has one owner, usually the definition, possibly in
  another file; the same fact at the helper and at every call site is one
  comment and the rest are deletions. Before adding one, read what is
  already there, here and at the definition of whatever you are calling.
- Most code affects correctness, not performance; the clearer version is
  often the better one. In code that exists for performance (grant
  expansion, compaction, et al), the comment explaining its shape is a good
  comment. A comment describing a novel algorithm is fine.

## Good comments

1. A contract the name and signature do not carry. Ask first whether they
   should: a constructor that fails on an existing file is better named
   `CreateStore` than commented. Sometimes the answer is no; a signature can
   be made to carry anything and most of it is not worth it. Then the
   sentence is the right call. `// NewStore returns a new Store.` is never
   this and is deleted. Exported is not a reason.
2. Why this and not the obvious alternative: an external system's behavior,
   a spec clause, an ordering dependency, a past incident. Name the source.
   The alternative has to be one a competent reader would reach for, and
   choosing it has to make a material difference: a bug, a broken resume, a
   changed contract. If only you considered it, it is a reasoning trace. If
   it would have worked too, it is a preference.
3. An invariant the code cannot carry, after you have decided a type or a
   test is not worth it here. `// Caller holds mu.`
4. Code that looks wrong and is not. State what breaks if it is "fixed".
5. Which of several variants is the way forward. On the preferred one: use
   this; the others remain for X and go away after Y. On the others:
   `// Deprecated: use NewStore.` (`staticcheck` SA1019 flags callers.) The
   duplication is the defect; the comment holds the line.
6. A TODO that says what would make it done: a ticket, a condition, a
   version. `// TODO(BATON-123): ...`.

## Not comments

- Restates the code or the name.
- Narrates steps. Extract and name the function.
- Describes the change: "now handles", "previously", "moved from". Commit
  message.
- Addresses the reviewer: "safe because", "this is intentional". PR
  description, or state the invariant and drop the reassurance.
- Hedges: "should work", "probably". A limitation stated as fact with its
  condition, or nothing.
- Reasoning trace: "here we", "note that", "we need to".
- Repeats a fact stated at its owner: the definition, the type, the
  package doc. A call site restating what the callee's doc says, in this
  file or another. Once, at the owner. A pointer to it at most.
- Two comments justifying opposite decisions. That is one inconsistency in
  the code, not two facts about it.
- Commented-out code.
- Test-body narration. The name and assertion are the documentation. A test
  comment states what the test would miss if removed, when the name does
  not.

## Where it goes instead

| To record | Put it in |
| --- | --- |
| What the code does | Identifiers and tests |
| Why this change | Commit message |
| Why this approach over others | PR description, or `docs/rfcs/` |
| What broke and how it was found | The ticket; a comment only if the code must stay odd because of it |
| How a subsystem works | `docs/` or the package doc |
| Who and when | `git blame` |

## Form

One line by default. A paragraph for a protocol, a cross-function
invariant, or a non-obvious algorithm. Full sentences, present tense, period
at the end (`godot`). A date belongs
when the fact has one: "the upstream API changed this in 2025-03" is a fact,
"added 2025-03" is not.

## Existing comments

Leave comments you did not need to touch. A stale comment inside lines you
are changing gets fixed or deleted. Do not sweep a file for comment quality
in an unrelated change.

Before finishing a change, reread every comment you added against this
page.

## Reviewing

- First question: should this comment exist. If not, the finding is
  "delete", not "reword". No thread on the wording of a comment that should
  not exist.
- A comment that does exist is a signal about the code. Ask whether the
  finding is "the code should carry this" (a rename, a type, a split)
  before anything about the comment itself.
- Do not request a comment where a rename or a split would do. Request the
  rename, if sensible.
- Do not request a comment that explains the diff.
- Deleting a comment that would not pass this page needs no justification;
  do not ask for it back. Deleting one that carried an important contract, an
  invariant, or a `Deprecated` marker is a change to that thing and is
  reviewed as one.
- A comment finding blocks merge when the comment is false in a way that
  would lead a reader to write wrong code: a misstated invariant, a wrong
  contract, a `Deprecated` pointing at the wrong variant. Exported or not;
  route by consequence as in `docs/REVIEW_CHECKLIST.md`. Everything else is
  unsaid unless it would change what a reader does; then it is
  non-blocking.
- One round per code comment. If the reworded version is still imprecise,
  propose the exact text or drop it. Do not open a second thread on it.

## Fixing review findings

"This comment is imprecise" is not an instruction to reword. Ask whether the
comment should exist. Most that draw wording findings should not. Delete
and say so.

## Examples

```go
// NewStore returns a new Store.
func NewStore(path string) (*Store, error) {
```

Delete. If it fails on an existing file, the name carries that, and what
remains is the part the name cannot:

```go
// CreateStore returns ErrExists if path is already present. Use this over
// OpenOrCreateStore, which is deprecated.
func CreateStore(path string) (*Store, error) {
```

```go
// ids must be sorted before calling merge.
func merge(ids []string) {
```

The comment is asking for `func merge(ids sortedIDs)`. Internal with a few
callers: make the type. Public API with external callers: the sentence may be
the right call; it reads `// merge requires ids sorted.`

```go
// Safe: we validated len(ids) > 0 above.
first := ids[0]
```

Delete. If the validation is far away: `// ids is non-empty; validated in
parseArgs.`

```go
// Now also handles the case where the grant has no principal.
if g.Principal == nil {
```

Delete. Commit message.

```go
// Grants are written before entitlements so that a crash between the two
// leaves a resumable state; resume treats a grant with no entitlement as
// pending, never the reverse. See resumeState.
```

Keep.
