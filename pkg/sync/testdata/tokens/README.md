# Golden sync-token fixtures

Each `.json` file is one sync state token in the codec's own encoding
(compact JSON, one line, trailing LF — `.gitattributes` here pins the line
ending, because a CRLF checkout would compare every case against a trailing
`\r` no writer emits). Originally captured for PR #1125.

Two kinds of file live here:

- **Recorded output** — built as a `state` through the package API and
  recorded from `Marshal()`. To reproduce one, construct the same state in
  a scratch test in `package sync` and log `Marshal()`; the fixture is that
  line verbatim. `v1_inline_graph.json` is the one recorded under a
  non-default option: it comes from the opt-in inline-graph writer
  (`WithEntitlementGraphInCheckpoints`), not the default one.
- **Hand-authored input** — bytes no current writer can produce, so they
  are edited by hand; the partner file is recorded from `Marshal()`. These
  are every `v0_*.json`, `v3_future_version.json` (a version this SDK does
  not recognize) and `v1_unknown_op.json` (a newer writer's operation
  string).

There is deliberately no `-update` flag. These bytes are a compatibility
surface rather than golden output: rewriting them from the current
`Marshal()` would let a wire-format regression re-bless itself, and could
not produce the hand-authored inputs at all. Changing a byte here is a
compatibility decision, and it belongs in a commit message.

`pkg/sync/token_golden_test.go` reads them: a token whose name has no
`.expected.json` partner must decode and re-encode to its own bytes; a token
with a partner must decode and re-encode to the partner's bytes. The token
wire format is read by older and newer SDKs against the same sync artifact,
so a change to any of these bytes is a compatibility change, not a
refactor.

| Fixture | Exercises |
|---|---|
| `empty.json` | A state with nothing set: every `omitempty` field absent, only `version`. |
| `v1_init.json` | The fresh-sync seed `Unmarshal("")` produces: one `init` action, `current_action_id` 1. |
| `v1_actions_multi.json` | Nine actions covering nine of the eleven `ActionOp` strings, page tokens, resource/parent-resource fields, `completed_actions_count`. The other two are covered elsewhere: `list-entitlements` by `v2_type_scoped.json`, `grant-expansion` by `v1_inline_graph.json`. |
| `v1_unknown_op.json` → `.expected.json` | An `operation` string this SDK does not recognize. `newActionOp` maps it to `UnknownOp`, which is 0, and `operation` is `omitempty` on a `uint8` kind — so `encoding/json` drops the key before `ActionOp.MarshalJSON` runs. The action stays on the stack with its page token and loses its operation. |
| `v2_type_scoped.json` | `type_scoped`, `spawned`, `type_scoped_planned` markers and the `version: 2` stamp they trigger. |
| `v1_fact_needs_expansion.json` | `needs_expansion` alone. |
| `v1_fact_has_external_resource_grants.json` | `has_external_resource_grants` alone. |
| `v1_fact_should_fetch_related_resources.json` | `should_fetch_related_resources` alone. |
| `v1_fact_should_skip_entitlements_and_grants.json` | `should_skip_entitlements_and_grants` alone. |
| `v1_fact_should_skip_grants.json` | `should_skip_grants` alone. |
| `v1_facts_all.json` | All five facts together. |
| `v1_run_stats.json` | `step_durations_ms`, `connector_call_stats` (recorded and merged), `session_store_stats` (including `errors`/`timeouts`), a fully populated `ingest_quality`. |
| `v1_compaction.json` | The `compaction` provenance block written by `BuildCompactedToken`, with partial timings folded into the top-level stat maps. |
| `v1_inline_graph.json` | An inline `entitlement_graph` (4 nodes, 3 edges, one expanded, one shallow, one with a nil resource-type filter) travelling with a live `grant-expansion` page token. Written by the opt-in inline-graph writer. |
| `v1_inline_graph.dropped.json` | The same token read back by the default writer: graph dropped, `grant-expansion` page token blanked. |
| `v0_current_action.json` → `.expected.json` | V0 with `current_action` present: the current action is appended last and gets the highest id. |
| `v0_no_current_action.json` → `.expected.json` | V0 with `current_action` absent, carrying the three skip/fetch facts and a nonzero `completed_actions_count`. |
| `v0_empty_object.json` → `.expected.json` | `{}` — no `version`, so the V1 parse is rejected and the V0 parser produces an empty V1 token. |
| `v3_future_version.json` → `.expected.json` | A `version` this SDK does not recognize, so `Unmarshal` falls back to the V0 parser. V0 shares the fact keys and `completed_actions_count` with V1 but not `actions_map` / `action_order`: the facts and the count survive, the whole action stack and `current_action_id` are dropped, and the token is restamped `version: 1`. The graph is still decoded, since `entitlement_graph` is a shared key, but is not written back. |

V0 upgrades one way, so the V0 pairs are (input, expected V1 output); the
expected output must also re-encode to itself.
