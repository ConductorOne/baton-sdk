# Pagination restart evidence

Plan revision: 1. All criteria P01 through P12 are **not assessed**.

Implementation obligations, instrument-validation results, executed commands,
coverage reductions and independent review findings will be appended here.

## Implementation-obligation addendum

- M01: A versioned cursor envelope carries the action's restart generation and
  provider token. Decode before the connector call; keep the envelope as the ledger
  request identity. Reject malformed metadata without echoing tokens.
- M02: Restart transitions leave the same action pending with a nonempty encoded
  continuation. An empty provider cursor inside that envelope means restart, not
  completion. The next real provider response controls completion.
- M03: Legacy restart transitions force `Checkpoint` before another provider call.
  Serialize checkpoint writers and timestamps, including concurrent worker restarts.
  Persistence errors retain the provider cause. The host's existing close/upload
  remains the publication fence for a remotely resumed artifact; no new guarantee
  is made for unpublished work lost with a killed host.
- M04: Pebble stages the restart through its existing page writer and pending-work
  CAS. Its failed-commit path must discard the candidate generation and observations.
  Retrying first-page requests under a new encoded generation must bypass previously
  committed page replay while retaining the old records.
- M05: Completed/warning counters change only on actual action completion. Failed
  provider calls remain error observations; restart commits contain no successful
  list response. Collected-row observations can include repeated reads.
- M06: Existing provider tokens and default-disabled calls stay unchanged. Metadata
  decodes even with new restarts disabled. Spawned cursors and internal store pages
  are not restarted because their seed is not the provider's empty initial cursor.
- M07: No iterator, raw store capability, deletion, external session, or new storage
  keyspace is introduced. The existing page commit and legacy checkpoint own writes.
- M08: SDK state conflict remains a host whole-sync reset. Expiry-budget exhaustion
  becomes an ordinary failed action; C1 retains the budget and its existing bounded
  whole-sync attempt fallback. No connector IDs or provider strings enter SDK policy.
