# Agent instructions

This applies to every agent working in this repository: comments, docs,
commits, PRs, review notes, and conversation with humans. It is not limited
to code edits.

Project overview, build commands, and architecture live in `CLAUDE.md`.
Review and verification live in `docs/REVIEW_CHECKLIST.md` and
`docs/BUG_CATCHING.md`.

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
