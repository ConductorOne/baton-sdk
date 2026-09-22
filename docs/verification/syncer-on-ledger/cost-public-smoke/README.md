# Public Sync cost smoke

36 isolated processes, three interleaved repetitions per configuration. The token
binary is eb63f1b5 plus ledger_cost_test.go. The ledger binary uses public Sync at
274f5c90 plus ledger_cost_public_test.go, including production handlers,
invariants, report archival and default disposal. Binary SHA-256 hashes are in
samples.json. All resource-count assertions passed, and ledger samples verified
an archive exists with no ledger facts remaining. NoSync applies to both ledger
arms. The resumed arm stops after exactly one connector resource page, closes and
reopens, then runs with the requested worker count.

All four configurations use 100 records/page. Times are median seconds.

| Pages | Workers | Token | Fresh | Resumed | Fresh/token wall | Resumed/token wall | Fresh/token write bytes | Resumed/token write bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1,000 | 1 | 0.656 | 0.787 | 1.019 | 1.199 | 1.552 | 1.216 | 1.332 |
| 1,000 | 4 | 0.485 | 0.581 | 0.761 | 1.199 | 1.569 | 1.187 | 1.320 |
| 10,000 | 1 | 7.570 | 8.519 | 10.598 | 1.125 | 1.400 | 1.177 | 1.217 |
| 10,000 | 4 | 6.289 | 5.918 | 10.519 | 0.941 | 1.673 | 1.104 | 1.206 |

Fresh wall time triggers the 1.10 tripwire in three configurations. Resumed wall
time exceeds it in all four; resumed write bytes exceed 1.25 in the two smaller
configurations. These are observations, not an acceptance verdict. The resumed
comparison is against a fresh token run and includes the cost of reopening and
continuing an existing store; it does not isolate ledger-specific resume overhead.

Report archival costs 1.7–2.3 ms at 1,000 pages and 7.3–10.6 ms at 10,000 pages.
For one worker, fresh 10,000-page Sync spends about 5.08 s in handlers, 2.77 s
inside page commits, 0.396 s sealing, 0.007 s archiving and 0.055 s disposing.
The resumed arm spends about 6.01 s in handlers and 3.75 s in commits; its walk
is below 0.1 ms. The walk and report do not explain the observed resume penalty.
Worker sums overlap in multiworker configurations; they are not wall-time shares.

Default disposal still follows scrub/purge at seal. At 10,000 pages, the one-worker
fresh run spends 23 ms scrubbing and 274 ms purging old token-bearing versions;
resumed spends 23 ms and 419 ms respectively. This redundant scrub/purge is a
candidate for removal only with equivalent crash and token-erasure guarantees.
Scrub time grows about tenfold for tenfold rows in these samples. Purge and file
sizes also depend on SST placement and compaction; the highly compressible fixed
fixture is not representative evidence of production artifact-size savings.

Machine inputs are in machine.json. No tests or compilations were run alongside
the timed samples, but the shared host/backing storage is not qualified as
unloaded. C49 remains evidence incomplete: the full matrix, production-shaped
estimate, row/bucket/fact byte decomposition, baseline timing breakdown and
unloaded-machine qualification are still absent. Later changes to option-report
fields and crash checks require a final revision rerun. The synthetic cost tables
remain historical; they are not substituted for these public-path results.

Separate CPU-profile runs at 10,000 pages/100 records/one worker locate the
resumed increase in storage lookups: Pebble getInternal accounts for 3.09 CPU
seconds in fresh ledger and 5.48 in resumed ledger; fresh token is 3.17 seconds.
The resumed resource handler spends about 3.0 seconds in its existing-resource
lookup, and committing resource records spends 3.42 seconds in stageResourceRecords.
These are cumulative CPU samples, not additional disjoint elapsed durations.
The profiles identify where to investigate; they do not prove an underlying
cause or establish how a resumed token run would behave. Top-node reports are
included as text. Profiled runs are separate from the 36 timing samples.
