# Sample spread

Seconds for wall time; bytes for writes. Three repetitions per cell.

| Records/page | Workers | Arm | Wall min | Wall median | Wall max | Bytes min | Bytes median | Bytes max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1000 | 1 | token-path | 6.9159 | 6.9646 | 7.0296 | 104719266 | 104744821 | 104770924 |
| 1000 | 1 | ledger-scheduler-fresh-no-sync | 3.3064 | 3.3295 | 3.4305 | 111920913 | 111948536 | 112080806 |
| 1000 | 1 | ledger-scheduler-resume-no-sync | 4.4617 | 4.5597 | 4.6458 | 119215724 | 119354353 | 119375723 |
| 1000 | 4 | token-path | 4.4817 | 4.5732 | 4.5743 | 104587499 | 104612688 | 104675248 |
| 1000 | 4 | ledger-scheduler-fresh-no-sync | 2.8999 | 2.9047 | 2.9067 | 106699458 | 106806149 | 106890442 |
| 1000 | 4 | ledger-scheduler-resume-no-sync | 4.2534 | 4.2872 | 4.3022 | 118991272 | 119009401 | 119016419 |
| 10000 | 1 | token-path | 73.6219 | 74.3012 | 75.1204 | 1047565625 | 1047781394 | 1047822798 |
| 10000 | 1 | ledger-scheduler-fresh-no-sync | 45.5177 | 45.7489 | 46.1659 | 1200320857 | 1200962262 | 1201091939 |
| 10000 | 1 | ledger-scheduler-resume-no-sync | 47.9936 | 48.1095 | 49.0937 | 1196328457 | 1196588286 | 1197277021 |
| 10000 | 4 | token-path | 53.9371 | 54.2310 | 54.9834 | 1047560063 | 1047879942 | 1047960149 |
| 10000 | 4 | ledger-scheduler-fresh-no-sync | 32.4852 | 32.5964 | 32.8316 | 1113349420 | 1116139336 | 1117069553 |
| 10000 | 4 | ledger-scheduler-resume-no-sync | 33.9556 | 34.3404 | 34.6336 | 1106955053 | 1110032796 | 1110181090 |
