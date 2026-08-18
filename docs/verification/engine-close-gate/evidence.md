# Evidence record: pebble engine close gate — review-closure stage

Findings source: three independent model reviews of PR #1088, consolidated
and re-verified against the code before any fix. Each entry names the
finding, the fix, and the instrument that now holds it.

## Findings fixed in this stage

1. Unpinned point-read surface (all three reviewers). `GetGrantRecord`,
   `GetEntitlementRecord`, `GetResourceRecord`, `GetResourceTypeRecord`,
   `GetAssetRecord`, `GetSyncRunRecord`, `readSyncStats`, `SessionGet`,
   `SessionGetMany`, `sessionGetAllChunk`, `computeSyncStats`, the digest
   read surface (`GetEntitlementDigestRoot`, `GetGrantDigestGlobalRoot`,
   `ComputeEntitlementBucketDigest`, `GetEntitlementGrantDigestNodes`,
   `dirtyPartitionBuckets` — both engines), `EnsureGrantIndexes`'s pending
   probe, and the `GrantDigestsPresent` checks in the digest repair path
   read `e.db` with no admission, so a concurrent Close could tear the
   handle down mid-read. Fix: pin at every entry point; plumb the admitted
   handle through the resolve/digest/repair helper chains
   (`lookup.go`, `digest.go`, `ingest_facts.go`, `ingest_repair.go`) so one
   admission covers the whole operation and probes inside pinned scans
   cannot be refused mid-scan by a re-pin racing the flip.
   Instrument: `TestBareHandleAccessIsGateCovered`.

2. Nested-pin refusal inside admitted scans (one reviewer).
   `ForEachDanglingGrantPrincipal` → `HasResourceRecord` re-pinned inside a
   pinned scan. Fix: `hasResourceRecordOn`/`hasEntitlementIdentity`/
   `grantIdentitiesForPrincipal`/`getGrantRecordByIdentity` take the
   caller's handle. Instrument: same as (1); behavior covered by the
   existing dangling-referent tests.

3. Racy, redundant `e.db == nil` checks (one reviewer).
   `checkWritableAllowSealed` and `EnsureGrantIndexes` read the field
   outside the gate as a pseudo-lifecycle check; `pinRead` kept a dead nil
   branch. Fix: deleted (with the reasoning written in place); the closing
   flag and gate ordering are the guarantee. The merge surface's nil checks
   stay, re-documented as sequential post-close misuse guards under the
   compactor ordering fence.

4. Lifecycle transitions outside the gate + ResumeSync TOCTOU (two
   reviewers). Transitions did bare handle reads Close could race, and
   ResumeSync validated the sync-run record before taking `lifecycleMu`.
   Fix: all five transitions assert-then-lock and run as admitted writes;
   ResumeSync validates under the lock.
   Instrument: `TestLifecycleMuTakersAreTransitionsOnly` (ordering enforced
   by token position).

5. WaitGroup Add-vs-Wait misuse in the gate (two reviewers), plus the same
   latent bug in `CompactAllRanges`/`Flush`. Fix: the gate counts under
   `countMu` and signals a condition variable; drains tolerate concurrent
   enters by construction. Instruments:
   `TestAdmissionDrainWritesToleratesConcurrentEnters`,
   `TestAdmissionEnterNeverTripsDrainingWaitGroup`.

6. Enforcement holes in the meta-tests (all three reviewers, different
   pieces). Name-prefix keying missed non-family reads; `pinRead` release
   discipline was unchecked; seek-driven iterator loops
   (`for valid := iter.First(); valid;`) were invisible to the ctx-check
   rule. Fix: `TestBareHandleAccessIsGateCovered` (keys on the field
   access), `TestPinnedReadsDeferTheirRelease`, and the extended
   `scanLoopCancellation`.

7. Unarmed Make targets (one reviewer). errorfs-soak, crash-check,
   checkpoint-cut-check, differential-check, prodscale-check compiled the
   engine without the deadlock-shape checks. Fix: armed; the per-target
   policy (and why bench/crossover/topebble/compat stay unarmed) is a
   comment in the Makefile.

8. Stale documentation (two reviewers): the pre-pin reader paragraph on
   `TestConcurrentCloseWithPaginatedReads`, and Close's unqualified
   panic-instead-of-hang claim (true only in armed builds). Rewritten.

## Findings fixed in the follow-up stage (unresolved PR review threads)

Re-verified against the code before fixing; two threads on the same file
were left to main, which had already landed a better version of them (see
"Deferred to main" below).

9. Windows-only failure in the arming tripwire (blocking).
   `TestLockChecksSuppliedByTestInvocations` classified config files by
   `strings.HasPrefix(rel, ".github/")`, but `filepath.Rel` yields
   backslashes on Windows, so the workflow floor counted zero hits and the
   test failed the run — on a tree with nothing wrong with it. CI runs
   `./...` on `windows-latest` with the tag and no `-short` skip on this
   test, so it was reachable. Fix: `filepath.ToSlash` the relative path,
   and fail loudly on a `filepath.Rel` error instead of keying the map on
   an empty string.

10. Unbounded retry in `CurrentSyncStep` (one reviewer). The
    generation-recheck loop retries until a pass sees a stable binding;
    nothing in it consults the caller's context, so the termination
    argument — transitions run out — was the only thing keeping it from
    spinning forever. Fix: check `ctx.Err()` on every pass after the
    first. The first pass stays unguarded so a caller reading the step
    while shutting down (the expiry checkpoint does) still gets an answer.
    Instrument: `TestCurrentSyncStepRetryHonorsCancellation`.

11. Tag-gated files were unlinted (one reviewer). `.golangci.yml` listed
    only `baton_lambda_support`, so every linter skipped the lock-check
    instrumentation and its tests — the least-reviewed code in the tree
    was the code asserting the concurrency contract. Fix: added
    `baton_lockchecks` to `run.build-tags`. This surfaced the unused
    `lineNo` in the tripwire, now folded into the violation message as
    `file:line: invocation`, which is what a reader needs anyway.

12. After-Close coverage stopped at the scan families (one reviewer).
    `TestReadSurfaceAfterCloseReturnsClosing` covered Paginate and
    Iterate; the point reads pinned in this PR had no lifecycle
    assertion, and they are the quieter failure — no iterator, so nothing
    on the path that happens to check. Fix: extended the table with 13
    point reads (the `Get*` family, `HasResourceRecord`,
    `ComputeEntitlementBucketDigest`, `GetEntitlementGrantDigestNodes`,
    `SessionGet`, `SessionGetMany`).

## Deferred to main

The two remaining threads were both on `pkg/sync/type_scoped_test.go`,
whose run-duration work landed on main separately as #1091. Main's
`flattenJoined` already peels single-error wrappers while looking for the
join — the exact degradation the thread described — and carries
`TestFlattenJoinedSeesWrappedJoins` to hold it. This branch's copy was the
older version, so the rebase resolves the file to main's side and the
branch no longer touches `pkg/sync` at all.

## Instrument liveness (mutation evidence)

Each new instrument was shown to fail against a seeded defect before
closure was claimed:

- `TestBareHandleAccessIsGateCovered`: run before the fixes were allowlisted,
  it reported the then-real violations (`computeSyncStats`, the digest
  repair checks, `endSyncFinalize`, and the build/repair helper family)
  — the allowlist was populated only after each entry's admission was
  verified by reading its callers.
- `TestPinnedReadsDeferTheirRelease`: mutating `GetAssetRecord`'s
  `defer release()` to a bare `release()` failed the test with the
  expected message; reverted.
- `scanLoopCancellation` extension: deleting the `ctx.Err()` check from
  `ForEachDanglingGrantPrincipal`'s seek-driven loop failed
  `TestScanReadsArePinned/ForEachDanglingGrantPrincipal`; reverted.
- `TestCurrentSyncStepRetryHonorsCancellation`: removing the `pass > 0`
  cancellation check made the test report the spin at its own 30s budget
  rather than hanging the binary until the package timeout; reverted.
- Point-read after-Close coverage: unpinning `GetResourceTypeRecord` (bare
  `e.db`) was caught twice over — `TestBareHandleAccessIsGateCovered`
  named `resource_types.go:57 GetResourceTypeRecord`, and the new
  `TestReadSurfaceAfterCloseReturnsClosing/GetResourceTypeRecord` caught
  the nil dereference the pin prevents; reverted.
- Windows path handling: replacing the `".github/"` prefix with
  `".github\\"` — what the un-normalized path would have matched on
  Windows — reproduced the reported failure verbatim ("no whole-tree
  `go test ./...` line found in any CI workflow"), confirming the floor
  assertion is what fires and that `ToSlash` is what prevents it;
  reverted.

## Suite evidence

On the final tree: `go build -tags=baton_lambda_support,baton_lockchecks
./...` clean; `golangci-lint run ./pkg/dotc1z/engine/pebble/...` zero
issues; `go test -race -tags=baton_lockchecks -count=1
./pkg/dotc1z/engine/pebble/ ./pkg/dotc1z/ ./pkg/synccompactor/...` pass
(the engine package alone is ~101s under -race; results recorded in the PR
checks on push).
