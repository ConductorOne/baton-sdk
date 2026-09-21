# Ten-million-record public Sync measurements

Six processes: 10,000 pages × 1,000 resources/page, one/four workers, one sample
per arm/configuration. The token baseline is eb63f1b5 plus the baseline driver;
the public ledger binary is a3863c3d. Executable hashes and machine inputs are
saved alongside the samples. Every process verifies ten million resources;
ledger processes also verify report archival and default disposal. No builds or
test suites ran alongside these samples. CO-019 accepts this shared environment.

| Workers | Token seconds | Fresh seconds | Resumed seconds | Fresh/token wall | Resumed/token wall | Fresh/token write bytes | Resumed/token write bytes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 78.741 | 121.320 | 117.363 | 1.541 | 1.490 | 1.159 | 1.161 |
| 4 | 55.412 | 58.454 | 60.824 | 1.055 | 1.098 | 1.079 | 1.070 |

These are individual samples, not confidence intervals or stable regression
estimates. The single-worker wall ratios exceed the tripwire substantially.
Report generation is 8.7–10.4 ms and does not explain the increase. Fresh
single-worker handler time is 72.157 s, page commit time 46.613 s, seal 1.604 s
and disposal 0.077 s. Four-worker durations overlap and are not wall-time shares.
The baseline has no corresponding handler/commit decomposition yet. Resumed
ledger remains compared with fresh token; it includes existing-store effects.

The difference between worker configurations and dataset sizes warrants a
separate CPU/profile and pre-seal storage-metric comparison. Storage lookup/table
overlap is a hypothesis, not an established cause. No production-scale forecast
or performance acceptance is inferred from these two configurations. The C49
matrix, baseline decomposition and physical byte attribution remain incomplete.
