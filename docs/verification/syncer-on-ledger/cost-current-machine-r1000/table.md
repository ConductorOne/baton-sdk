# Interleaved cost smoke

Public Sync including report archival and default disposal; actual resumed NoSync. Not an acceptance table.

| Pages | Records/page | Workers | Metric | Token | Fresh | Resumed | Fresh/token | Resumed/token |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1000 | 1000 | 1 | sync_wall_ns | 7.414e+09 | 8.103e+09 | 1.054e+10 | 1.093 | 1.422 |
| 1000 | 1000 | 1 | pebble_bytes_written | 1.047e+08 | 1.152e+08 | 1.214e+08 | 1.1 | 1.159 |
| 1000 | 1000 | 1 | wal_bytes_before_close | 8.484e+07 | 8.573e+07 | 8.552e+07 | 1.011 | 1.008 |
| 1000 | 1000 | 1 | flush_bytes_before_close | 1.99e+07 | 2e+07 | 2.005e+07 | 1.005 | 1.008 |
| 1000 | 1000 | 1 | compaction_bytes_before_close | 0 | 9.452e+06 | 1.582e+07 | N/A | N/A |
| 1000 | 1000 | 1 | c1z_bytes | 3.627e+06 | 2.202e+06 | 6.959e+05 | 0.607 | 0.1919 |
| 1000 | 1000 | 1 | peak_rss_kib | 1.621e+05 | 1.812e+05 | 1.871e+05 | 1.118 | 1.155 |
| 1000 | 1000 | 1 | page_commit_ns | N/A | 2.57e+09 | 3.812e+09 | N/A | N/A |
| 1000 | 1000 | 1 | handler_ns | N/A | 5.041e+09 | 6.09e+09 | N/A | N/A |
| 1000 | 1000 | 1 | seal_ns | N/A | 3.619e+08 | 5.119e+08 | N/A | N/A |
| 1000 | 1000 | 1 | seal_fold_ns | N/A | 1.661e+05 | 1.712e+05 | N/A | N/A |
| 1000 | 1000 | 1 | seal_scrub_ns | N/A | 2.668e+06 | 2.485e+06 | N/A | N/A |
| 1000 | 1000 | 1 | seal_purge_ns | N/A | 2.603e+08 | 4.008e+08 | N/A | N/A |
| 1000 | 1000 | 1 | resume_walk_ns | N/A | 1.659e+04 | 8.566e+04 | N/A | N/A |
| 1000 | 1000 | 1 | report_ns | N/A | 2.716e+06 | 1.887e+06 | N/A | N/A |
| 1000 | 1000 | 1 | disposal_ns | N/A | 5.501e+07 | 3.588e+07 | N/A | N/A |
| 1000 | 1000 | 4 | sync_wall_ns | 4.998e+09 | 5.891e+09 | 7.358e+09 | 1.179 | 1.472 |
| 1000 | 1000 | 4 | pebble_bytes_written | 1.046e+08 | 1.092e+08 | 1.209e+08 | 1.043 | 1.155 |
| 1000 | 1000 | 4 | wal_bytes_before_close | 8.483e+07 | 8.56e+07 | 8.554e+07 | 1.009 | 1.008 |
| 1000 | 1000 | 4 | flush_bytes_before_close | 1.982e+07 | 1.994e+07 | 1.991e+07 | 1.006 | 1.004 |
| 1000 | 1000 | 4 | compaction_bytes_before_close | 0 | 3.617e+06 | 1.54e+07 | N/A | N/A |
| 1000 | 1000 | 4 | c1z_bytes | 3.773e+06 | 3.384e+06 | 7.849e+05 | 0.8967 | 0.208 |
| 1000 | 1000 | 4 | peak_rss_kib | 1.685e+05 | 1.708e+05 | 1.851e+05 | 1.014 | 1.099 |
| 1000 | 1000 | 4 | page_commit_ns | N/A | 3.601e+09 | 5.091e+09 | N/A | N/A |
| 1000 | 1000 | 4 | handler_ns | N/A | 6.532e+09 | 7.698e+09 | N/A | N/A |
| 1000 | 1000 | 4 | seal_ns | N/A | 2.887e+08 | 5.239e+08 | N/A | N/A |
| 1000 | 1000 | 4 | seal_fold_ns | N/A | 2.016e+05 | 1.953e+05 | N/A | N/A |
| 1000 | 1000 | 4 | seal_scrub_ns | N/A | 2.548e+06 | 2.594e+06 | N/A | N/A |
| 1000 | 1000 | 4 | seal_purge_ns | N/A | 1.807e+08 | 4.09e+08 | N/A | N/A |
| 1000 | 1000 | 4 | resume_walk_ns | N/A | 1.761e+04 | 1.07e+05 | N/A | N/A |
| 1000 | 1000 | 4 | report_ns | N/A | 1.975e+06 | 1.912e+06 | N/A | N/A |
| 1000 | 1000 | 4 | disposal_ns | N/A | 3.876e+07 | 3.105e+07 | N/A | N/A |
