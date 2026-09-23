# Production source inventory

Baseline: eb63f1b5. Historical audits and file-size snapshots are preserved in the
[pre-trim inventory](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/source-inventory.md).

| Boundary | Current owner | Reason for the boundary |
| --- | --- | --- |
| Engine selection | syncer.setStore, store_caps | Resolve capabilities once; reject engine/capability disagreement |
| Phase ordering and workers | parallel_syncer | Retain the existing scheduler, retry and warning policy |
| Pending execution | ledger_pending, ledger_restore | Bounded stack/refill reads; work IDs distinguish repeated arguments |
| Page transaction | ledger_scheduler, ledger_page, Pebble page_unit | Publish records, facts, counts and pending transition together |
| Legacy takeover | ledger_takeover, Pebble pending_work/ledger | Parse before atomic token consumption and queue seed |
| Local expansion/import | Existing syncer handlers | Whole-phase replay; no page transaction around local writes |
| Static entitlements | ledger_static_entitlements/materialization | Capture definitions once; materialize bounded resource pages in order |
| Lifecycle and seal | ledger_lifecycle/seal, Pebble adapter | Preserve same-ID lifecycle; require terminal proof; recover interrupted disposal |
| Report | Pebble ledger_report* and ledger_archive | Streaming aggregation, optional indexed diagnostics, saved report after disposal |
| Store mutation wrappers | pebble_store | Dirty tracking and direct-write instrumentation |

No completed-history walker, request-identity claim map, replacement worker pool,
expansion wrapper or whole-import transaction remains. Compactor production code
and SQLite rollback code are unchanged. SQLite retains checkpointing.

Tests cover public durable images, real process exits, legacy takeover, bounded
pending admission, template ordering, option retention, report fallback and saved
artifact compaction. Exact executed scope and remaining gaps are in [evidence.md](evidence.md).
