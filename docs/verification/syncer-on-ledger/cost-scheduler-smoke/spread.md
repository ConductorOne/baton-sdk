# Sample spread

Seconds for wall time; bytes for writes. Minimum/median/maximum over three repetitions.

| Pages | Workers | Arm | Wall min | Wall median | Wall max | Bytes min | Bytes median | Bytes max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1000 | 1 | token-path | 0.6196 | 0.6425 | 0.6610 | 10456693 | 10500020 | 10531363 |
| 1000 | 1 | ledger-scheduler-fresh-no-sync | 0.3115 | 0.3119 | 0.3140 | 10844630 | 10950502 | 11003379 |
| 1000 | 1 | ledger-scheduler-resume-no-sync | 0.4432 | 0.4437 | 0.4528 | 12394814 | 12457740 | 12462034 |
| 1000 | 4 | token-path | 0.4533 | 0.4602 | 0.4622 | 10475570 | 10505017 | 10505772 |
| 1000 | 4 | ledger-scheduler-fresh-no-sync | 0.2929 | 0.3005 | 0.3084 | 10842486 | 10857832 | 10882381 |
| 1000 | 4 | ledger-scheduler-resume-no-sync | 0.4285 | 0.4310 | 0.4401 | 12245396 | 12308487 | 12309834 |
| 10000 | 1 | token-path | 7.0121 | 7.1543 | 7.2442 | 104870905 | 104914249 | 104957734 |
| 10000 | 1 | ledger-scheduler-fresh-no-sync | 3.5829 | 3.5928 | 3.6599 | 117746241 | 117747059 | 117859498 |
| 10000 | 1 | ledger-scheduler-resume-no-sync | 4.7291 | 4.7510 | 4.7950 | 124443449 | 124517384 | 124619959 |
| 10000 | 4 | token-path | 5.0719 | 5.1368 | 5.1461 | 104651272 | 104706312 | 104740901 |
| 10000 | 4 | ledger-scheduler-fresh-no-sync | 3.3316 | 3.3720 | 3.3736 | 111844140 | 111956376 | 111964752 |
| 10000 | 4 | ledger-scheduler-resume-no-sync | 4.4956 | 4.5295 | 4.5379 | 123703845 | 123778517 | 123800366 |
