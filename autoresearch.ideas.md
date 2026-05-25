# Ideas backlog — Pebble engine perf

Free-form scratch. Append new ideas as bullets; mark tried ones with
status (kept / discarded / crashed) so we don't repeat them.

## To try (priority order)

- [ ] **P1.1** Memtable size 64 MiB → 256 MiB (`MemTableSize` in `options.go`).
- [ ] **P1.2** `L0CompactionThreshold` sweep: 2 → 4, 8.
- [ ] **P1.3** `MaxConcurrentCompactions` upper bound: 8 → 12 (gate on GOMAXPROCS).
- [ ] **P1.4** Enable bloom filters on L0 (FilterPolicy + FilterType).
- [ ] **P1.5** Mixed compression: Snappy at L0, zstd at L6.
- [ ] **P2.6** Per-record-type per-level options (grants vs resources).
- [ ] **P2.7** Codec codegen via `cmd/protoc-gen-batonstore` — replaces reflection path. Big change; may need human approval.
- [ ] **P3.8** Pool tuple encoder buffer (`AppendTupleString`) — kill per-record slice alloc.
- [ ] **P3.9** Larger SST block size (32 KiB → 64 KiB) — amortize header overhead.

## Tried — see jsonl for verdicts

(populated by the loop)
