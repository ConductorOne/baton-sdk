# Resumable Pebble sanitizer: verification evidence

Plan: `docs/verification/resumable-sanitizer/plan.md`.

The plan was frozen before implementation. Change order 1 routes the overall
PR as MODERATE and retains HIGH checks for shared Pebble ledger and seal code.

## Criterion status

- SAN-001: covered by SQLite and non-ledger destination rejection tests.
- SAN-002: covered by before-first-commit interruption and cold reopen.
- SAN-003: covered by before/after-commit interruption and asset-read failure.
- SAN-004: covered by the five phase/terminal commit positions and multi-page resume.
- SAN-005: covered for SQLite and Pebble sources; resumed records, assets, and
  expansion index state match uninterrupted output.
- SAN-006: covered by terminal-commit and seal-error resumes. Sealing resume
  rejects any resource-type read.
- SAN-007: covered for secret, anchor, source-sync metadata, policy mismatch,
  missing policy, and implicit-anchor adoption. Content replacement preserving
  all source-sync metadata is not detected.
- SAN-008: covered for grants across trusted pages and seal-time expansion-index rebuild.
- SAN-009: trusted stagers contain no destination reads. Benchmark evidence is
  recorded below; no runtime destination-get counter exists.
- SAN-010: covered by end-to-end cardinality, stats, expansion, and metadata tests.
- SAN-011: covered for returned page, asset, and seal errors plus CLI rerun.
  Subprocess signal coverage is pending.
- SAN-012: inherited from `SetPendingWork` revision checks and existing page-ledger tests.
- SAN-013: page writers discard on all exits; asset readers and stores close on
  success and tested errors.
- SAN-014: production sanitizer and command code contain none of the retired APIs.

## Implementation-obligation addendum

- `processSanitizePage` owns one `PageWriter`; deferred `Discard` covers every
  transform, asset, and commit error.
- `stageAssets` closes every returned asset reader. NotFound records an omission;
  cancellation, corruption, and other read errors abort the page.
- `pageUnit.Commit` owns one `RecordBatch`; its existing deferred close covers
  trusted stager and batch failures.
- The process-local grant cache is rebuilt after reopen and does not affect
  durable work identity.
- CLI error close uses a timeout derived from `context.WithoutCancel`, preserving
  the last published partial envelope after handled cancellation.
- Durable writes are `BeginCollecting`, page record/asset/work commits, the
  pre-terminal `supports_diff` update, terminal commit, and `Seal`.

## Commands

- `go test ./pkg/c1zsanitize -count=1`: pass.
- `go test ./cmd/baton -run '^TestSanitize' -count=1`: pass.
- `go test ./pkg/dotc1z -count=1`: pass.
- `go test ./pkg/dotc1z/engine/pebble -run 'Page|Ledger|Deferred' -count=1`: pass.
- `go test -race ./pkg/c1zsanitize ./cmd/baton -run 'TestSanitize' -count=1`: pass.
- `go test ./pkg/c1zsanitize -run '^$' -bench '^BenchmarkSanitizePageLedger$' -benchmem -count=1`: pass; 2,000-grant fixture, 1.60 s/op.
- `make lint`: pass.
- `npm --prefix ./frontend run build && go build ./cmd/baton`: pass.
- `go test ./... -count=1`: inconclusive under concurrent repository gates;
  four unrelated packages reached the shared 10-minute timeout. Each named
  test passed alone, including the Pebble and compactor cases.
