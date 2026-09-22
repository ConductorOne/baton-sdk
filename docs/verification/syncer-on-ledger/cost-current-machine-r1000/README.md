# Current-machine public Sync measurements

CO-019 accepts this environment for cost evaluation. These 18 processes use
three rotating, interleaved repetitions per configuration, 1,000 pages with
1,000 resources/page, and one or four workers. Baseline source is eb63f1b5 plus
the unchanged ledger_cost_test.go driver; ledger source is 0618510e. Samples
include executable hashes. All runs verified one million resources; ledger runs
also verified the saved report and default ledger disposal. No builds or other
test suites ran alongside the measurements.

| Workers | Token seconds | Fresh seconds | Resumed seconds | Fresh/token wall | Resumed/token wall | Fresh/token write bytes | Resumed/token write bytes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 7.414 | 8.103 | 10.541 | 1.093 | 1.422 | 1.100 | 1.159 |
| 4 | 4.998 | 5.891 | 7.358 | 1.179 | 1.472 | 1.043 | 1.155 |

Values are medians. Report/archive generation takes 1.9–2.7 ms; disposal takes
31–55 ms. Fresh seal time is 0.29–0.36 s, resumed seal time 0.51–0.52 s. The
four-worker fresh wall ratio exceeds the 1.10 tripwire. Resumed wall ratios
remain substantial but compare reopened ledger runs against fresh token runs;
they do not isolate ledger-specific resume overhead. No assertion that these
penalties apply to connector-bound production wall time is made.

The environment has a four-CPU quota, 32 GiB memory limit and ZFS artifact
storage on exposed EBS devices. Machine inputs and starting host load are in
machine.json; that snapshot is not an isolation guarantee. The existing harness
labels results smoke because the full matrix and byte decomposition remain
incomplete. CO-019 removes only the dedicated/unloaded-machine requirement.
These two configurations do not establish production-scale behavior or close C49.
