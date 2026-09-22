# Interleaved cost smoke

Public Sync including report archival and default disposal; actual resumed NoSync. Not an acceptance table.

| Pages | Records/page | Workers | Metric | Token | Fresh | Resumed | Fresh/token | Resumed/token |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 10000 | 1000 | 1 | sync_wall_ns | 7.874e+10 | 1.213e+11 | 1.174e+11 | 1.541 | 1.49 |
| 10000 | 1000 | 1 | pebble_bytes_written | 1.047e+09 | 1.214e+09 | 1.215e+09 | 1.159 | 1.161 |
| 10000 | 1000 | 1 | wal_bytes_before_close | 8.481e+08 | 8.568e+08 | 8.547e+08 | 1.01 | 1.008 |
| 10000 | 1000 | 1 | flush_bytes_before_close | 1.99e+08 | 2.009e+08 | 2.01e+08 | 1.01 | 1.01 |
| 10000 | 1000 | 1 | compaction_bytes_before_close | 0 | 1.56e+08 | 1.598e+08 | N/A | N/A |
| 10000 | 1000 | 1 | c1z_bytes | 3.623e+07 | 6.362e+06 | 6.435e+06 | 0.1756 | 0.1776 |
| 10000 | 1000 | 1 | peak_rss_kib | 4.697e+05 | 4.886e+05 | 5.344e+05 | 1.04 | 1.138 |
| 10000 | 1000 | 1 | page_commit_ns | N/A | 4.661e+10 | 4.453e+10 | N/A | N/A |
| 10000 | 1000 | 1 | handler_ns | N/A | 7.216e+10 | 7.019e+10 | N/A | N/A |
| 10000 | 1000 | 1 | seal_ns | N/A | 1.604e+09 | 1.739e+09 | N/A | N/A |
| 10000 | 1000 | 1 | seal_fold_ns | N/A | 3.375e+05 | 1.869e+05 | N/A | N/A |
| 10000 | 1000 | 1 | seal_scrub_ns | N/A | 2.517e+07 | 2.641e+07 | N/A | N/A |
| 10000 | 1000 | 1 | seal_purge_ns | N/A | 5.134e+08 | 6.999e+08 | N/A | N/A |
| 10000 | 1000 | 1 | resume_walk_ns | N/A | 6.368e+04 | 1.16e+05 | N/A | N/A |
| 10000 | 1000 | 1 | report_ns | N/A | 9.488e+06 | 1.04e+07 | N/A | N/A |
| 10000 | 1000 | 1 | disposal_ns | N/A | 7.717e+07 | 8.214e+07 | N/A | N/A |
| 10000 | 1000 | 4 | sync_wall_ns | 5.541e+10 | 5.845e+10 | 6.082e+10 | 1.055 | 1.098 |
| 10000 | 1000 | 4 | pebble_bytes_written | 1.046e+09 | 1.129e+09 | 1.119e+09 | 1.079 | 1.07 |
| 10000 | 1000 | 4 | wal_bytes_before_close | 8.481e+08 | 8.556e+08 | 8.549e+08 | 1.009 | 1.008 |
| 10000 | 1000 | 4 | flush_bytes_before_close | 1.981e+08 | 2.002e+08 | 2.002e+08 | 1.011 | 1.01 |
| 10000 | 1000 | 4 | compaction_bytes_before_close | 0 | 7.355e+07 | 6.414e+07 | N/A | N/A |
| 10000 | 1000 | 4 | c1z_bytes | 3.785e+07 | 2.428e+07 | 2.554e+07 | 0.6415 | 0.6748 |
| 10000 | 1000 | 4 | peak_rss_kib | 4.583e+05 | 4.911e+05 | 4.82e+05 | 1.072 | 1.052 |
| 10000 | 1000 | 4 | page_commit_ns | N/A | 3.863e+10 | 4.132e+10 | N/A | N/A |
| 10000 | 1000 | 4 | handler_ns | N/A | 6.551e+10 | 6.747e+10 | N/A | N/A |
| 10000 | 1000 | 4 | seal_ns | N/A | 1.357e+09 | 1.392e+09 | N/A | N/A |
| 10000 | 1000 | 4 | seal_fold_ns | N/A | 3.443e+05 | 1.933e+05 | N/A | N/A |
| 10000 | 1000 | 4 | seal_scrub_ns | N/A | 2.881e+07 | 2.527e+07 | N/A | N/A |
| 10000 | 1000 | 4 | seal_purge_ns | N/A | 2.081e+08 | 2.511e+08 | N/A | N/A |
| 10000 | 1000 | 4 | resume_walk_ns | N/A | 2.799e+04 | 9.306e+04 | N/A | N/A |
| 10000 | 1000 | 4 | report_ns | N/A | 9.03e+06 | 8.724e+06 | N/A | N/A |
| 10000 | 1000 | 4 | disposal_ns | N/A | 8.011e+07 | 4.503e+07 | N/A | N/A |
