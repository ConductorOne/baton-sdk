# Syncer ledger review guide

- [Frozen behavioral plan and change orders](plan.md)
- [Implementation history](implementation.md)
- [Current pending-work design](pending-work.md)
- [Per-criterion evidence and remaining gaps](evidence.md)
- [Targeted source audit](source-inventory.md)

## Downloadable verification evidence

[Download the evidence ZIP](https://github.com/ConductorOne/baton-sdk/archive/3b35887e02365b3b3f84ed1a54d2be8b622a0d4f.zip)
or [browse its files](https://github.com/ConductorOne/baton-sdk/tree/3b35887e02365b3b3f84ed1a54d2be8b622a0d4f).

Pinned artifact commit: `3b35887e02365b3b3f84ed1a54d2be8b622a0d4f`, reachable on
`matt.kaniaris/CXE-1358/verification-evidence`. It preserves the full verification
directory and benchmark sources at `0d87cd4c`, before PR-size cleanup. Every
original document blob was compared with this remote-backed snapshot. It is a
separate artifact branch, not a merge target or runtime dependency.

Within the downloaded `docs/verification/syncer-on-ledger/` directory:

| Artifact | Contents |
| --- | --- |
| `evidence.md`, `implementation.md` | Complete historical execution and design notes, including superseded decisions |
| `executed-coverage-d891604d.json` | Test outcomes and changed-file statement coverage; not branch/product closure |
| `attachment-coverage.json` | All 16 P3 attachment cells, verified by constructor/path tests |
| `cost-public-smoke/` | 36 public-path samples, 100 records/page |
| `cost-current-machine-r1000/` | 18 public-path samples, 1,000 records/page |
| `cost-current-machine-10million/` | Six larger samples, profiles and separate filter experiment |
| Other `cost-*` directories | Historical component/synthetic measurements; not current public-path acceptance |
| `report-*.md`, `report-phase-memory.txt` | Report studies and million-row component measurements |
| `baseline-audit.md`, `source-inventory.md` | Historical lifecycle/scheduler findings and targeted source audit |
| `tools/` and archived `pkg/sync/ledger_cost*.go` | Exact earlier drivers for reproducing historical measurements |

## Earlier collection performance finding

All samples use a deterministic zero-latency connector. They expose SDK/storage
cost rather than predicting a connector's network-dominated wall time. CO-019
accepts the current shared machine. Single samples are not confidence intervals.

| Ten million records | Token | Fresh ledger | Ledger/token |
| --- | --- | --- | --- |
| One worker, initial sample | 78.7s | 121.3s | 1.54 |
| One worker, unprofiled repeat | 81.0s | 111.5s | 1.38 |
| Four workers, initial sample | 55.4s | 58.5s | 1.05 |

Report generation took roughly 9–12ms in these samples. Profiles locate much of
the extra CPU in Pebble point lookups. The isolated Bloom-filter candidate took
104.1s but enlarged the compressible fixture file from 6.4MB to 19.0MB; it is not
adopted. Collection performance is accepted under CO-020 based on the requester-reported
latency run (fresh/token 1.004, resumed/token 0.998). Raw local samples have not
been imported. The original full C49 matrix and byte attribution remain incomplete.

## Reproduce a small public-path comparison

From the repository root, with Go 1.26 and vendored dependencies:

```sh
bash docs/verification/syncer-on-ledger/tools/build-baseline.sh /tmp/ledger-token.test
GOTOOLCHAIN=go1.26.0 go test -mod=vendor -c ./pkg/sync -o /tmp/ledger-current.test
python3 docs/verification/syncer-on-ledger/tools/run-cost.py \
  --baseline /tmp/ledger-token.test --ledger /tmp/ledger-current.test \
  --output /tmp/ledger-cost-output --pages 10 --records 10 --workers 1 4 \
  --repetitions 1
```

The output directory must not already exist. Keep new raw results outside the
PR; publish them as another pinned artifact and update the measured summary.
`tools/cells.py` emits required products; `tools/coverage-summary.py` summarizes
executed Go tests/profiles. Neither converts passing samples into full coverage.

Expansion uses main's deterministic whole-phase replay and optimized adapter,
without page rows or batch checkpoints (CO-021). Completed collection work is removed from the pending queue, preventing
refetching collected data; whole-phase accounting records completed expansion once.

External import/matching also uses main's ordinary writes, without a whole-import
ledger transaction (CO-023). Imported grant pages reach the store before the next
page is fetched. Main's pre-existing expansion-annotation replay bug is a separate
fix; this PR does not attempt to mask it with buffering.

Default finalization archives the report and recovery state, discards the ledger
and purges residue once before the finished stamp (CO-022). It does not scrub
rows destined for deletion. Debug retention still scrubs unless explicitly
retaining tokens. Archive-write failure retains scrubbed history. Report access
after disposal reuses the archive rather than scanning an empty ledger.
The current benchmark's report/disposal timings are included in seal time; they
must not be added to seal time as disjoint phases.

## Pending-work revision cost

The public SDK comparison against c9ff02ce uses three interleaved repetitions,
100 records/page, zero connector latency, and default history disposal. All
samples verified their complete record counts. Fresh samples use84811dfb;
resumed samples use6ca6a0ac (the cancellation guard). This shared machine is
accepted by CO-019; concurrent compilation makes small differences uncertain.

| One million records | Previous ledger | Pending work | Extra wall time | Written-byte ratio |
| --- | --- | --- | --- | --- |
| Fresh,1 worker | 8.290s | 8.688s | 0.398s | 1.037 |
| Fresh,4 workers | 6.853s | 7.479s | 0.625s | 1.038 |
| Resumed,1 worker | 11.761s | 12.853s | 1.093s | 1.024 |
| Resumed,4 workers | 7.637s | 8.061s | 0.424s | 1.024 |

Medians are not confidence intervals. Resumed wall time includes stop/save/reopen;
its one-worker candidate samples ranged11.95–19.91s. Written bytes sum WAL,
flush and compaction counters before close. The completed-history prototype's
faster pending lookup and bounded memory claim are separate from these ingestion
measurements. This table does not complete the original C49 matrix.
