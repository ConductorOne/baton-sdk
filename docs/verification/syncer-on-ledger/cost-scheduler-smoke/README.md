# Existing scheduler cost smoke

36 separate processes, three interleaved repetitions per cell. Every sample
verified its resource count; ledger samples also verified page commit counts.
Token source: `eb63f1b5`. Ledger source: `fafa4f74`. Both executables use
Go 1.26.0 with vendored dependencies; their SHA-256 hashes are in each sample.
The ledger arms use the existing scheduler with synthetic page handlers.
Both use NoSync. The resumed arm flushes, closes and reopens after one data page.

All cells have 100 resources per page. Times below are median seconds.
Byte ratios include WAL, flush and compaction writes observed before close.

| Pages | Workers | Token seconds | Fresh seconds | Resumed seconds | Fresh/token bytes | Resumed/token bytes |
| --- | --- | --- | --- | --- | --- | --- |
| 1,000 | 1 | 0.643 | 0.312 | 0.444 | 1.043 | 1.186 |
| 1,000 | 4 | 0.460 | 0.300 | 0.431 | 1.034 | 1.172 |
| 10,000 | 1 | 7.154 | 3.593 | 4.751 | 1.122 | 1.187 |
| 10,000 | 4 | 5.137 | 3.372 | 4.530 | 1.069 | 1.182 |

The smaller ledger wall times are not evidence of a production speedup:
its synthetic handlers omit filtering and other production work. The token
arm runs public Sync. Concurrent commit/handler durations are summed worker
durations, not disjoint parts of wall time. See `table.md` for all medians
and ratios, `samples.json` for individual values and spread, and `logs/` for
every process result. No median wall or byte tripwire fired in these four cells.
Scrub time for ten times as many rows grew by 8.4–10.3 times across the
four ledger configurations. This small sample does not establish linear
scaling or a superlinear regression. Counter-fold time is not proportional
only to rows: it folds the worker/attempt buckets.

`machine.json` was collected before the samples. `machine-after.json` was
collected after them and corrects a measurement omission: the test artifact
filesystem is ZFS, while the checkout is ext2/ext3. The visible block-device
list does not identify the backing device of the ZFS mount. The container
has a four-CPU quota and 32 GiB memory limit. Neither snapshot establishes
an unloaded machine for the duration of the run.

C49 remains incomplete. Missing: the full matrix and production-shaped
estimate, row/bucket/fact byte decomposition, baseline phase timings, actual
production handlers, unloaded-machine qualification, and disposition of
CO-009’s assumed resumed Sync behavior. These results do not authorize K5/K6.
No production or storage behavior changed in this measurement increment.

Reproduce after building the two executables:

```sh
python3 docs/verification/syncer-on-ledger/tools/run-cost.py --baseline /tmp/syncer-ledger-baseline.test --ledger /tmp/syncer-ledger-runtime.test --output /tmp/new-scheduler-cost --pages 1000 10000 --records 100 --workers 1 4 --repetitions 3
```
