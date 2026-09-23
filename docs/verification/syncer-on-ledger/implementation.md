# Syncer ledger implementation

The [frozen plan](plan.md) and its change orders define the observable contract.
The [pending-work design](pending-work.md) defines the current recovery algorithm.
Original briefs and intermediate designs are retained in the
[pre-trim implementation record](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/implementation.md).

## Execution and durability

Pebble uses the ledger unconditionally; SQLite retains checkpoints. Attachment
resolves capabilities once and rejects disagreement with engine metadata.

The existing scheduler runs collection handlers against PageWriter. A page commit
publishes records/indexes, facts, the worker's cumulative counter bucket, completed
report history and the pending-work transition together. An action must transition
exactly once; errors discard its staged page. In-memory progress follows commit.

Pending work has independent IDs/revisions. Equal request arguments are distinct
executions. Resume loads at most100 stack entries; the existing worker queue admits
new children in windows of64. Completed history is not traversed for recovery.
Resource-child scheduling relations live in an index rather than a growing map.
Startup constructs the runtime and scheduler from the same final read after any
required migration or seed. Prior counter folds and legacy graph payloads are
not retained in the runtime. Lifecycle preparation classifies the next operation
once; the engine still independently checks its mutation preconditions.

Attempt accounting owns cumulative run-level observations and local-phase counts.
Live stats retain historical totals for logging; they are never written as a new
attempt bucket. Response annotations are decoded before commit and the captured
values feed post-commit live updates.

Legacy V0/V1/V2 tokens are parsed before the store atomically clears the token,
saves migration provenance and seeds pending actions/facts/counters. Pending work
then becomes authoritative. Stale compaction provenance is dropped. Local expansion
rebuilds its transient graph rather than importing inline checkpoint graph state.

Expansion and external import keep their ordinary handlers and optimized store
capabilities. Only whole-phase completion removes their pending item. A crash may
repeat local processing; there is no transaction buffering those phases. The
known upstream external-import annotation replay issue remains a separate fix.

Static-entitlement responses capture templates in pending arguments. Each local
materialization page covers one resource listing page, preserving overwrite order.
This bounds staged output, not arbitrary connector response or template bytes.

## Lifecycle and reporting

Binding a finished sync preserves its identity, data and lifecycle metadata.
New requested processing clears old history and applies current diagnostic flags;
an unfinished processing declaration prevents a failed seal being mistaken for
another finished binding. Missing queue state is not an empty queue.

Default sealing saves the mechanical report/options, drops history and purges
its physical token residue once. Debug mode retains scrubbed history and enables
indexed reference checks; explicit token retention requires debug mode and warns.
Unfinished resumes honor durable retention. Archive-write failure keeps scrubbed
history. Final cleanup removes the token-free completion declaration only after
seal succeeds. Already-finished low-level reseal and durable disposal recovery
are separate from permission to finish an uninitialized collection.

The report aggregates writes, collection counts/exclusions, empty responses,
pagination, connector latency, observed waits, retry attempts/errors, phase time
and non-secret effective options. The scanner skips unneeded protobuf fields and
keeps bounded top lists. Debug references use exact work IDs/revisions and indexed
lookups; default reporting is one forward scan. No AI-generated interpretation is
part of the artifact or logs.

Ingest quality's behavior-controlling knowledge is durable fact state; numeric
observations are counters. Missing legacy knowledge remains unknown. Fresh and
resumed pages use the engine's existing NoSync policy; a crash comparison uses
the pages present in the durable image, not every returned commit.

## Verification

[evidence.md](evidence.md) records current checks and conservative per-criterion
status, with links to preserved tests/mutants and uncovered cells. The write hook
rejects unexplained writes during pages and writes during pending-state restore;
explicit session-state bypasses and lifecycle writes have their own checks.
[README.md](README.md) contains performance results and reproduction commands.
