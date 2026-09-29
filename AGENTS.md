# Agent instructions

This applies to every agent working in this repository: comments, docs,
commits, PRs, review notes, and conversation with humans. It is not limited
to code edits.

Project overview, build commands, and architecture live in `CLAUDE.md`.
Review and verification live in `docs/REVIEW_CHECKLIST.md` and
`docs/BUG_CATCHING.md`. When to write a code comment, and what it may say,
lives in `docs/COMMENTS.md`. A comment is worse than nothing until it
proves otherwise; put intent in names, types, and tests first.

## Surface the decisions review cannot see

Review reads code and finds code that is wrong. Structural decisions are
never wrong in the code: a lock on state with one owner is locked correctly;
a fact used as a lifecycle marker is read consistently. Those decisions
reach the requester only if you write them down as claims, before the code
and in a form checkable without reading the diff.

Before designing anything that touches durable state, the sync lifecycle,
or a cross-process contract, write the production flows and the
producer/consumer version pairs the design depends on. If you are guessing
at any of them, ask before designing; a flow you did not know was hot
cannot be ranked.

Every turn that changes code ends with a block titled **Decisions not
requested**, listing each addition from this closed set: shared state or a
synchronization primitive; a durable key, fact, or marker; a new authority
for a lifecycle question; a contract method; a test hook; a field on
`syncer` or `Engine`; a phase, mode, or flag. One line each: what it is,
its owner or the record it is authoritative for, and the existing thing
that already answered the question, or `none`. `Decisions not requested:
none` is a valid block and must appear. The requester reads this block; it
is not a summary of the diff.

For step-up work, the implementation brief carries the ownership table and
the state inventory `docs/REVIEW_CHECKLIST.md` describes, and the
registries (`TestSyncPrimitivesRegistered`, `TestEnginePrimitivesRegistered`,
`commitPointRegistry`) hold the mechanical half.

## Verified or assumed, never remembered

A statement about existing code — a field's lifetime, a method on an
interface, where a value is resolved, what a path deletes — is either cited
(`file:line`, read in this session) or marked `assumed`. Memory of code read
earlier is not a citation. Nothing is called frozen until a reader with no
stake in it has checked each cited claim against source and listed every
one as true, false, or not checkable; a false claim is a change order, not
an edit. CXE-1358's CO-037/CO-038 text carried four false claims of this
kind past its author; the check that found them was a grep per claim.

A fence, registry, or meta-test is an instrument and follows the instrument
rule: it ships in the same commit as a planted case it catches, one per
shape it claims to cover. A fence that has never failed on a planted
violation has not been shown to see anything. The primitive registry
shipped without one and missed a mutex behind an import alias in the
package it was written for.

## Diction

Name the function, type, hook, or check. State the fact with essential
framing. Leave historical uses alone unless you are already editing that
comment. Words not listed here are fine.

### Hard ban

spine, spines, seam, seams, specimen, specimens, load-bearing,
fail-closed posture

### Minimize, do not ban

insight, insights, key insight, substrate, keystone, linchpin, bedrock,
cornerstone, throughline, constellation, residue, remnant, vestige,
affordance, taxonomy, fiber

Fiber is fine as a language primitive, a named utility, or a math term.
Drop it as a metaphor for a path, goroutine, edge, or piece of the system.

Essay glue: the crux, mental model, the shape of, the upshot, punchline,
takeaway, kicker, here's the catch, the wrinkle, crucially, importantly,
notably, furthermore, moreover, it's worth noting, worth calling out,
at its core, in other words, to be clear, delve, unpack, underscore
(as a verb), highlight, showcase.

Ad-copy: leverage, robust, holistic, nuance, nuanced, seamless,
seamlessly, landscape, tapestry, first-class, baked in, wired up,
battle-tested, golden path, north star, elegant, principled, surgical,
the cleanest, purest form, facilitate, utilize, comprehensive,
intricate, nestled, vibrant, pivotal, paradigm, interplay, ecosystem.

Prefer none of these. One is better than a paragraph of them.

### Instead of

| Instead of | Write |
| --- | --- |
| spine | chain, path, `i → i+1` edges |
| seam | hook, injection point, after X / before Y, `testSeams.Foo` |
| specimen | example, case, this bug |
| load-bearing | required; this test fails if X is wrong |
| fail-closed posture | fail-closed |
| fiber (metaphor) | goroutine, request, edge, path |
| key insight | the fact, with no preamble |
| substrate | the index / table this sits on |

### Exceptions (existing names only)

Cite these when they already exist. Do not mint new ones.

- Identifiers: `testSeams`, `seamFailureCases`, `choke_point_meta_test.go`
- Product / proto: `SecurityInsight`, sidecar files, payload bytes
- Handbook terms when following `docs/BUG_CATCHING.md`: oracle, harness,
  obligation, closure, instrument, ride-along
