# Resumable Pebble sanitizer: verification plan

Frozen on 2026-10-08 before implementation. The first commit containing this
file is the baseline. Later behavior changes append a change order rather than
rewriting the frozen criteria.

## Risk and scope

**HIGH.** A record/progress mismatch creates a valid-looking durable artifact.
Correctness depends on crash timing and a cold-process resume, and no single run
can show equivalence to uninterrupted execution.

This stage replaces sanitizer sync-token checkpoints and `BulkSyncImport` with
Pebble `PageLedgerStore` work. Sanitization is always resumable and writes only
Pebble. Resource types, resources, entitlements, grants, and referenced assets
commit with their source cursor. A terminal page precedes `Seal`. The CLI
reopens unfinished output and saves it on returned errors and handled signals.

Deletion of SQLite writers outside the sanitizer is a later stage.

## Claims

- **SAN-001 Engine admission.** SQLite and non-`PageLedgerStore`
  destinations fail before destination mutation.
- **SAN-002 Initialization.** New output starts one unfinished sync. Existing
  unfinished output is rebound without reset. Only a bound sync proven empty
  may initialize a missing ledger.
- **SAN-003 Atomic page.** A page commits all transformed records, referenced
  assets, and its pending-work transition, or none of them.
- **SAN-004 Ordered frontier.** Durable work advances exactly resource-types →
  resources → entitlements → grants. Empty pages and final pages advance once.
- **SAN-005 Resume equivalence.** Every selected durable stop image, resumed
  cold and resumed repeatedly, seals to the same canonical output as an
  uninterrupted run.
- **SAN-006 Seal ordering.** An empty queue receives one terminal page. A
  sealing sync only retries `Seal`; it does not read or write another source
  page.
- **SAN-007 Resume validation.** The secret fingerprint, anchor, source
  identity, and output-affecting policy are validated before page mutation.
  Missing, malformed, unknown-version, and mismatched state fail closed.
- **SAN-008 Duplicate contract.** The last source occurrence wins. Primary
  rows and derived indexes agree with that winner after seal.
- **SAN-009 Trusted import cost.** Sanitizer pages perform no per-record
  destination lookup. Work is O(records), page memory is bounded, and seal
  passes are measured separately.
- **SAN-010 Complete artifact.** The sealed file has exact transformed rows,
  assets, indexes, expansion state, stats, metadata, and `ended_at`. It has no
  sanitizer sync token or bulk-import state.
- **SAN-011 Error preservation.** Errors, cancellation, and handled signals
  leave a reopenable unfinished output that the next invocation can resume.
- **SAN-012 Writer conflict.** A stale page cannot advance work or write into
  another sync; expected work ID and revision are checked with the records.
- **SAN-013 Resource lifetime.** Page writers, iterators, batches, and store
  handles release once on success and every error exit.
- **SAN-014 Retirement.** Production sanitizer code contains no
  `CheckpointSync`, sanitizer resume-token encoding, `BulkSyncImport`, or
  SQLite output path.

## Coverage model

The deterministic lifecycle suite covers:

- family: resource-types, resources, entitlements, grants;
- page: empty, one row, full, multi-page, final;
- stop: before stage, after stage, after commit, after terminal, after seal;
- resume: same process, cold reopen, second resume of one image;
- metadata: matching, missing, malformed, wrong secret, anchor, source, policy;
- duplicates: same page, cross-page, before and after resume;
- exit: returned error, cancellation, handled signal.

Process kill without a close is measured separately: the envelope writer saves
on `Close`, so a kill can retain only the last published envelope. It is not
reported as closure for the handled-exit contract.

## Oracles

- **O1 Independent model.** Derive transformed protobufs, duplicate winners,
  assets, references, and expected phase calls without sanitizer transition
  helpers.
- **O2 Sealed differential.** Compare uninterrupted and resumed canonical
  primary/index/asset keyspaces, sync metadata, and stats.
- **O3 Lifecycle dichotomy.** Every reopened image is unfinished and
  resumable, or sealed and complete.
- **O4 Operation audit.** Count source page reads, destination gets, page
  commits, terminal commits, and seals. Trusted import permits no per-record
  destination gets.
- **O5 Rejection snapshot.** Snapshot the destination before invalid resume;
  rejected execution leaves it byte-for-byte unchanged.

Each oracle is validated with a representative planted violation: split cursor
advancement from records; drop a final row or asset; omit a phase transition;
switch last-wins to first-wins; route trusted import through ordinary upserts;
validate a fact after opening a page; skip the terminal page; suppress
work-revision checking; or discard partial CLI output.

## Verification locations and commands

Expected instruments:

- `pkg/c1zsanitize/page_ledger_test.go`
- `pkg/c1zsanitize/resume_validation_test.go`
- `pkg/c1zsanitize/trusted_import_test.go`
- `pkg/c1zsanitize/perf_test.go`
- `pkg/dotc1z/engine/pebble/adapter_page_test.go`
- `cmd/baton/sanitize_test.go`

Evidence commands:

```bash
go test -count=1 ./pkg/c1zsanitize
go test -count=1 ./cmd/baton -run '^TestSanitize'
go test -count=1 ./pkg/dotc1z/engine/pebble -run 'Page|Ledger|CommitPoint'
go test -race -count=1 ./pkg/c1zsanitize ./cmd/baton
go test ./pkg/c1zsanitize -run '^$' -bench 'Sanitize.*PageLedger' -benchmem -count=5
make lint
go test -count=1 ./...
make build
```

Before signoff, add the implementation-obligation inventory, disposition every
uncovered changed branch, validate every oracle with its plant, run an
independent evidence audit, and rerun affected evidence after the final change.

## Change order 1: risk routing

The overall PR is MODERATE. Sanitizer-only behavior is LOW because sanitization
is not mission critical. Changes to shared Pebble ledger and seal paths remain
HIGH and receive the step-up checks in this plan.

## Change order 2: trusted-import index obligation

The seal rebuilds `by_needs_expansion` only when trusted grant imports arm a
durable marker. Expanded and synthesized grant writes maintain that index
inline and must not pay for its sorter or range replacement. This remains HIGH
because the marker controls durable derived-index correctness across reopen.
