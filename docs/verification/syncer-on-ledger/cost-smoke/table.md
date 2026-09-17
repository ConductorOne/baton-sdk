# Interleaved cost smoke

Private runtime; actual resumed NoSync. Not the CO-009 Sync arm or an acceptance table.

| Pages | Records/page | Workers | Metric | Token | Fresh | Resumed | Fresh/token | Resumed/token |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1000 | 100 | 1 | sync_wall_ns | 6.263e+08 | 3.169e+08 | 4.582e+08 | 0.5059 | 0.7316 |
| 1000 | 100 | 1 | pebble_bytes_written | 1.051e+07 | 1.094e+07 | 1.246e+07 | 1.04 | 1.185 |
| 1000 | 100 | 1 | wal_bytes_before_close | 8.527e+06 | 8.887e+06 | 8.896e+06 | 1.042 | 1.043 |
| 1000 | 100 | 1 | flush_bytes_before_close | 1.988e+06 | 2.05e+06 | 2.055e+06 | 1.031 | 1.034 |
| 1000 | 100 | 1 | compaction_bytes_before_close | 0 | 0 | 1.511e+06 | N/A | N/A |
| 1000 | 100 | 1 | c1z_bytes | 3.645e+05 | 4.166e+05 | 1.461e+05 | 1.143 | 0.4009 |
| 1000 | 100 | 1 | peak_rss_kib | 4.788e+04 | 4.916e+04 | 5.826e+04 | 1.027 | 1.217 |
| 1000 | 100 | 1 | page_commit_ns | N/A | 2.114e+08 | 3.113e+08 | N/A | N/A |
| 1000 | 100 | 1 | handler_ns | N/A | 4.255e+07 | 4.42e+07 | N/A | N/A |
| 1000 | 100 | 1 | seal_ns | N/A | 4.776e+07 | 7.402e+07 | N/A | N/A |
| 1000 | 100 | 1 | seal_fold_ns | N/A | 1.421e+05 | 1.458e+05 | N/A | N/A |
| 1000 | 100 | 1 | seal_scrub_ns | N/A | 2.266e+06 | 2.342e+06 | N/A | N/A |
| 1000 | 100 | 1 | seal_purge_ns | N/A | 2.854e+07 | 5.718e+07 | N/A | N/A |
| 1000 | 100 | 1 | resume_walk_ns | N/A | 6220 | 1.65e+04 | N/A | N/A |
| 1000 | 100 | 4 | sync_wall_ns | 8.172e+08 | 4.48e+08 | 5.399e+08 | 0.5482 | 0.6607 |
| 1000 | 100 | 4 | pebble_bytes_written | 1.047e+07 | 1.087e+07 | 1.228e+07 | 1.038 | 1.173 |
| 1000 | 100 | 4 | wal_bytes_before_close | 8.504e+06 | 8.882e+06 | 8.858e+06 | 1.044 | 1.042 |
| 1000 | 100 | 4 | flush_bytes_before_close | 1.963e+06 | 2.017e+06 | 2.024e+06 | 1.027 | 1.031 |
| 1000 | 100 | 4 | compaction_bytes_before_close | 0 | 0 | 1.396e+06 | N/A | N/A |
| 1000 | 100 | 4 | c1z_bytes | 4.106e+05 | 4.33e+05 | 1.543e+05 | 1.055 | 0.3758 |
| 1000 | 100 | 4 | peak_rss_kib | 5.042e+04 | 4.402e+04 | 4.537e+04 | 0.8731 | 0.8999 |
| 1000 | 100 | 4 | page_commit_ns | N/A | 3.16e+08 | 3.921e+08 | N/A | N/A |
| 1000 | 100 | 4 | handler_ns | N/A | 1.383e+08 | 8.947e+07 | N/A | N/A |
| 1000 | 100 | 4 | seal_ns | N/A | 5.296e+07 | 7.624e+07 | N/A | N/A |
| 1000 | 100 | 4 | seal_fold_ns | N/A | 1.804e+05 | 1.909e+05 | N/A | N/A |
| 1000 | 100 | 4 | seal_scrub_ns | N/A | 1.81e+06 | 2.332e+06 | N/A | N/A |
| 1000 | 100 | 4 | seal_purge_ns | N/A | 3.428e+07 | 5.815e+07 | N/A | N/A |
| 1000 | 100 | 4 | resume_walk_ns | N/A | 6381 | 2.581e+04 | N/A | N/A |
