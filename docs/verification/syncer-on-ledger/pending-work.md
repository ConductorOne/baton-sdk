# Durable pending work

CO-027 replaces completed-history reconstruction. The prior plan remains frozen;
its change-order log records the changed contract. Raw storage prototype/source
and results are preserved separately; they do not establish SDK integration.

## Representation and transaction

A pending work item has a monotonically allocated integer ID, a revision and the
existing action fields. IDs encode stack order; continuation updates the same
slot and increments its revision. Child IDs are allocated in the same order as
main pushes children, so the last child is first on the stack. Equal arguments
never identify the same execution. Completed history keys retain report grouping
and append work ID/revision so repeated arguments cannot overwrite observations.
Tokens belong in values, not work keys. Pending items and queue initialization/
allocator metadata occupy separate subranges of the ledger family.

A page validates its expected ID/revision under the store write lock and commits
records/indexes, history, facts, counter bucket, pending continuation/deletion,
children and allocator metadata in one RecordBatch. A stale revision fails before
any durable record changes. No durable running/lease state: the bounded in-memory
scheduler owns in-flight work; after a crash its entries remain pending. Terminal
seal requires an initialized empty queue and no active page writers. A missing
queue declaration is distinct from an initialized empty queue.

## Initialization and lifecycle

An initial queue seed is synced as lifecycle state before handlers run. Legacy
checkpoint decoding remains in the syncer; the decoded stack seeds pending work
in the same store transaction that consumes the token and preserves facts/stats.
The token/frontier remains diagnostic migration provenance, not a replay root
once the queue is initialized. Invalid legacy state is rejected before consumption.
No history-only experimental file is assumed complete or empty. Finished-binding
clear deletes pending/allocator history with the existing synced row-clear batch;
data, counters, timestamps and identity remain subject to the established rules.
Seal/discard/drop/reset and raw capability inventories must cover new subranges.

## Scheduler integration

Retain parallelSync, its worker pool, retry handling, warning policy and phase
switch. The ledger's runState action map becomes a bounded execution window,
refilled from durable pending order while no workers are active. Continuations
already owned by a worker may remain in that window; children persist at commit
and are admitted under a bounded policy. Do not load all pending items or retain
an all-completed child-scheduling map. Resource-child scheduling uniqueness must
use indexed durable state in the same page batch, independently of request-token
identity. Existing spawned-work semantics must be checked separately from ordinary
pagination; no blanket request-based deduplication may be introduced.

The integration must demonstrate phase barriers when the pending window splits
an operation group, serial static-template overwrite order, warning accounting,
and concurrent commits with newly inserted children. Decisions about admission
that reduce parallelism must be measured on spawned-work fixtures; a passing raw
storage prototype does not settle them.

## Commit sequence and checks

1. Storage work types, bounded read/initialization and atomic page transition.
   Test wrong revision, identical arguments, continuation/children ordering,
   empty completion, failed batch and durable crash images. Build independently.
2. Queue-aware takeover/lifecycle, actual adapter dirty tracking and bounded
   scheduler integration. Replace the history walk and its cumulative maps;
   port public crash, legacy-token and phase-order fixtures. Every changed
   boundary gets a before-fix or planted-defect assertion.
3. Completed-history/report adaptation and disposal coverage. Preserve mechanical
   report fields; explain any counts that change. Pending state is not archived
   as completed history. Rerun scale/cost checks through the public SDK.
4. Full sync/dotc1z/compactor suites, focused repeated race checks, lint, independent
   review of the final implementation and honest evidence disposition.

No separate Bloom-filter policy, forced queue compaction, cycle detector or
replacement scheduler is part of this change. The static-template local-work
representation remains a separate decision; this revision does not silently
change it or claim its replay properties from the storage prototype.

### Bounded admission detail

Keep the existing parallel queue's FIFO semantics within a batch. Start from the
same maximum100 stack actions main selects. Page commits persist children instead
of accumulating all of them in runState/queue memory. When the in-memory queue
empties, its existing workers refill a bounded window of newly created same-op
children in ascending allocated-ID order (the old append order). The read cursor
starts above the batch's initial stack high-water mark and advances monotonically;
it does not scan completed history or admit older actions across a phase barrier.
Current worker continuations remain owned in memory and update their durable slot.
Read errors cancel the batch and leave uncommitted work pending. Serial dispatch
continues to select the highest pending ID, preserving child/template order.

Child-resource scheduling evidence moves from the reconstructed all-history map
to an indexed durable relation written with the page that schedules that child.
Its read supplies the existing ingestion invariant without rebuilding the map.
It is cleared with finished page history. This relation enforces the existing
parent/child scheduling rule; it is not a pagination-token cycle detector.
