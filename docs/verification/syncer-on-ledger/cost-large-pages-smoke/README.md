# Larger-page cost smoke

36 separate processes, three interleaved repetitions per cell. Each run
verified one million or ten million resources; ledger runs also verified
1,002 page commits. All samples passed. Token source is eb63f1b5; ledger
source is fafa4f74. Binary hashes are recorded per sample. Both executables
use Go 1.26.0 and vendored dependencies. No builds or other test commands
ran alongside these samples.

Every cell has 1,000 pages. Times are median seconds. Write-byte ratios
include observed WAL, flush and compaction bytes. Both ledger arms use
actual NoSync writes, as dispositioned by CO-012.

| Records/page | Workers | Token seconds | Fresh seconds | Resumed seconds | Fresh/token bytes | Resumed/token bytes |
| --- | --- | --- | --- | --- | --- | --- |
| 1,000 | 1 | 6.965 | 3.330 | 4.560 | 1.069 | 1.139 |
| 1,000 | 4 | 4.573 | 2.905 | 4.287 | 1.021 | 1.138 |
| 10,000 | 1 | 74.301 | 45.749 | 48.110 | 1.146 | 1.142 |
| 10,000 | 4 | 54.231 | 32.596 | 34.340 | 1.065 | 1.059 |

No median wall or byte tripwire fired. These runs hold page count fixed and
do not assess seal scaling with row count. The ledger handlers are synthetic
and omit production work; wall ratios do not establish a production speedup.
At 1,000 records/page, most additional write bytes appear in compaction
counters; median WAL bytes differ by less than 0.5%. At 10,000 records/page,
fresh byte ratios increase for both worker counts. The table does not support
a claim that total overhead necessarily falls with larger pages.

Source inspection identifies an additional ledger seal operation:
endSyncFinalize scrubs tokens and invokes Ledger.purgeResidue, which forces
compaction over ledger and sync-run key ranges. The measurements include
its time under seal_purge_ns. They do not isolate its write bytes from other
compactions; attribution of the entire byte difference to it is unproven.
Encoded row/bucket/fact decomposition remains missing.

The machine snapshot records the temporary ZFS filesystem separately from
the checkout, a four-CPU quota and 32 GiB memory limit. It does not establish
an unloaded machine throughout the run or identify the ZFS backing disk.
This is smoke evidence only. Together with cost-scheduler-smoke, eight of
the eighteen required workload configurations have samples; none closes
C49. The full matrix, production-shaped estimate, complete instrumentation,
production handlers, unloaded-machine evidence and final revision rerun
remain required. The durability premise is settled.

Full medians and ratios are in table.md; min/median/max in spread.md; all
samples and raw process logs are retained. Reproduce using:

```sh
python3 docs/verification/syncer-on-ledger/tools/run-cost.py --baseline /tmp/syncer-ledger-baseline.test --ledger /tmp/syncer-ledger-runtime.test --output /tmp/new-large-page-cost --pages 1000 --records 1000 10000 --workers 1 4 --repetitions 3
```
