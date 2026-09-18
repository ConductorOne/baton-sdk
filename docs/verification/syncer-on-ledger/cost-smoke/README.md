# Private runtime cost smoke

Historical results from the executor deleted in `a42c8a32`. These numbers
do not measure the current implementation. See `../cost-scheduler-smoke/`
for the subsequent measurements through the existing scheduler.

These 18 samples exercise the measurement instruments. They do not close
C49 or authorize handler implementation. Three repetitions rotate arm order
at 1,000 pages, 100 records/page, and one/four workers. The token executable
is built from eb63f1b5 with the standalone baseline fixture. The ledger
executable includes the private runtime and K3b phase measurements. Binary
SHA-256 values are recorded per sample.

Both ledger arms actually commit NoSync. Resume occurs after one data page,
with a measured flush and close/reopen. The full Sync-per-page resumed arm,
production handlers, complete matrix, row/bucket/fact byte decomposition,
production-shaped estimate and unloaded machine qualification are absent.
The loaded machine snapshot is retained. Times are nanoseconds, sizes bytes,
and RSS KiB. Concurrent worker handler/commit times are sums, not elapsed
critical-path components. N/A includes unsupported baseline metrics and
ratios with a zero denominator. Artifact size differences do not establish
logical equality; resource counts are the fixture's limited result check.

Reproduce after building the baseline and ledger test executables:

```sh
python3 docs/verification/syncer-on-ledger/tools/run-cost.py --baseline /tmp/syncer-ledger-baseline.test --ledger /tmp/syncer-ledger-runtime.test --output /tmp/new-cost-samples --pages 1000 --records 100 --workers 1 4 --repetitions 3
```

Raw per-arm process logs are generated alongside each result by the runner.
The committed samples and machine snapshot preserve the measured values;
the table contains medians and their ratios, not confidence intervals.
