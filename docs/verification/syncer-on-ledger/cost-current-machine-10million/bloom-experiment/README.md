# Isolated Bloom-filter experiment

One fresh public Sync per arm, 10,000 pages × 1,000 resources, one worker.
Order: filtered ledger, unfiltered ledger, token baseline. No CPU profiling,
builds or test suites ran alongside these measurements. Each process verifies
ten million resources; the ledger arms also verify archival and default disposal.
The shared machine is accepted under CO-019. These are single samples, not a
stable estimate or a completed C49 matrix.

The candidate is e2edfdd1 plus candidate.patch, which configures the vendored
10-bit Bloom policy on the existing levels. The control is the same production
source and optional metrics driver used by the preceding profiles, now run
without profiling. The token arm is eb63f1b5 with the baseline driver. Executable
hashes, raw metrics and machine details accompany the samples.

The filtered ledger took 104.057 s; the unfiltered control took 111.504 s, a
6.7% decrease in this pair. Final file size rose from 6,387,140 to 18,979,109
bytes (2.97×). Total WAL/flush/compaction writes rose from 1,214,310,422 to
1,240,826,141 bytes (2.2%). Sampled peak RSS was 475,996 versus 501,700 KiB.
The fixture uses highly compressible resource values; the file-size ratio is
not a prediction for production artifacts.

Pre-seal metrics show four compactions and read amplification 3 in both ledger
arms. Filter utility is 98.8% with the candidate. Useful filters did not remove
the measured collection cost. This supports keeping the lookup investigation
separate from the report, which took 9.8–11.6 ms in these two runs.

Decision: do not adopt this storage setting in this PR on this evidence. The
measured improvement is modest, artifact size grows, and only one configuration
was measured. No production defaults changed. The vendored reader only loads
recognized filter blocks (sstable/reader.go, initMetaindexBlocks), but a
cross-setting reopen test and wider performance evidence would still be needed
before adoption; source inspection is not compatibility-test evidence.

The unprofiled token control took 80.989 s. Relative to token,
unfiltered ledger wall time is 1.377× and filtered ledger is
1.285×. The fresh single-worker tripwire therefore remains in this
repeat; the filter candidate does not meet the 1.10 tripwire either. The metric
ratios are recorded in comparison.json; missing baseline subdivisions remain
null rather than being inferred from overlapping samples.
