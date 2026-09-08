# Golden sync-token fixtures

Each `.json` file is one sync state token, exactly as the codec in
`pkg/sync` emits it (compact JSON, one line, trailing newline). Captured
from `64510b2e` (branch `kans/syncer-seams`, PR #1125) by building `state`
values through the package API and recording `Marshal()`.

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
| `v1_actions_multi.json` | Nine actions covering every `ActionOp` string, page tokens, resource/parent-resource fields, `completed_actions_count`. |
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

V0 upgrades one way, so the V0 pairs are (input, expected V1 output); the
expected output must also re-encode to itself.
