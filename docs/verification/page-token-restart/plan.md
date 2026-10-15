# Bounded pagination restart

Frozen behavioral plan, revision 1. Runtime implementation has not started.

## Scope and risk

An opt-in error classifier permits a provider listing action to restart from its
first page without abandoning the enclosing sync. The host supplies the provider
classification and a limit of two restarts. Already collected records are retained:
the result represents observations during the sync, including abandoned attempts,
rather than a point-in-time snapshot or replacement listing.

Risk: HIGH. Restart state survives process boundaries, interacts with concurrent
workers and replay, and is consumed by a separate host. A defect can lose work or
reset a retry budget. Remediation is a fresh sync. The default SDK policy remains
disabled; enabling the host policy requires the SDK implementation in this change.

The implementation must distinguish a restarted listing from cached pages of its
earlier attempt. It must not add per-grant scans, cleanup, or replacement generations.
Healthy, never-restarted pages retain their existing request tokens and behavior.

## Criteria

| ID | Required behavior | Planned instrument |
| --- | --- | --- |
| P01 | Only configured expiry errors on supported provider list actions with a nonempty provider cursor restart. Initial-page, internal-store, spawned-cursor and unrelated errors propagate. | Dispatch/classifier table and actual connector RPCs. |
| P02 | At most two restarts of one action occur per sync, including across close/reopen and new syncer instances. The third expiry fails; it never becomes completion. | Real-store budget exhaustion across repeated resumes. |
| P03 | Recovery keeps the enclosing sync ID, completed sibling work and collected records. A restarted listing must finish before the sync completes. | Deterministic connector call log and complete stored-grant comparison. |
| P04 | The restart count advances durably before the first request of the restarted listing. Failure to persist the transition propagates and prevents that request. | Restart checkpoint/commit fault injection and close/reopen. |
| P05 | Provider requests receive the original opaque token, never SDK recovery metadata. Non-restarted tokens are unchanged; malformed or unsupported recovery metadata fails without exposing its contents. | Codec round trips, malformed/version table and secret-redaction assertions. |
| P06 | Pebble uses the restarted attempt's request identity rather than replaying the abandoned attempt's pages. Pending-work revisions remain checked. | Real Pebble repeated-first-page fixture with changed response and resume. |
| P07 | Cancellation before or during a restart preserves unfinished work and cannot complete the action. A resumed checkpoint can be resumed twice without exhausting a fresh budget or dropping data. | Stop-point table, clone/close/reopen and changed worker counts. |
| P08 | Budgets belong to actions: one failing action does not consume a sibling's budget or repeat completed phases. | Two failing resources with independent call logs, 1 and 3 workers. |
| P09 | Restart commits do not count an action as complete or a connector failure as an empty successful listing. Completion and warning accounting remain coherent. | Real-store progress/ledger counter assertions and zero-result final listing. |
| P10 | Existing unwrapped checkpoints resume unchanged. An SDK with the new decoder can resume wrapped checkpoints even when new restarts are disabled. | Enabled/disabled policy close/reopen matrix and existing token fixtures. |
| P11 | The host enables precise Graph expiry classification and removes the immediate whole-sync reset for that error. SDK state-conflict recovery and the independent sync-attempt failsafe remain. | C1 consumer regression and full affected package. |
| P12 | Checkpoint calls from concurrent workers are serialized without races or lost state. Healthy-page work is O(1); restarted-token processing is O(token length), independent of stored grants. | Affected race suite, checkpoint concurrency fixture and focused allocation benchmark. |

## Coverage model

Real-store matrix: SQLite/Pebble x 1/3 workers x successful restart/exhausted
budget/resume after restart. Lifecycle faults cover the distinct legacy checkpoint
and Pebble page-commit paths. Pure classification and codec dimensions use tables;
they are not multiplied by storage where the implementation shares the exact same
function. A changed boundary invalidates that reduction.

Replayed results intentionally retain observations from prior attempts. The oracle
is the independently computed union of emitted grant identities, not the last
provider response alone. A later fresh full sync must exclude memberships no longer
returned by the provider.

Compatibility limitation: older SDK releases do not understand new recovery cursors.
Enabling the option requires a supporting SDK for subsequent resumes. No generic
old-reader compatibility claim is made; unwrapped preexisting checkpoints remain
supported. A full cross-version feature rollout is deferred to a release stage.

## Validation and review

Validate the new instruments against a disabled restart and a reset-on-resume budget
mutation before relying on their passing results. Collect changed-branch coverage,
run focused package and race tests, existing sync/store regressions, and repository
lint. The C1 consumer uses its Bazel-backed package and graph checks. Public API
comments must name the retained-observations and reader-version contracts.

All seven review passes apply; closely related passes may be combined into a
checkpoint/error/concurrency review and a classification/accounting/performance
review. An independent reader audits the evidence and final implementation after
the instruments pass. Required CI and human review retain merge authority.

## Change orders

None.
