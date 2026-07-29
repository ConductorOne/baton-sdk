package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	native_sync "sync"
)

// queueAudit is the test-only event recorder behind the parallel-scheduler
// contract checker (verifyQueueAudit, parallel_audit_test.go). The queue's
// correctness properties — exactly-once execution, dedup soundness, no
// commits after abort, exact outstanding accounting, full drain on clean
// completion — are silent when violated: a dropped action produces absence,
// not an error. The audit turns every heavy test that runs the scheduler
// into a checked execution: the queue records each event at its mutation
// site (already under q.mu, so the recorded order is the real order), and
// the checker replays the log against the contract post-hoc.
//
// Production cost is one nil check per queue operation: the syncer only
// attaches an audit when a test sets testQueueAudit.
type queueAudit struct {
	mu       native_sync.Mutex
	events   []queueAuditEvent
	batchSeq int
}

// newBatch allocates the next batch segment id.
func (a *queueAudit) newBatch() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.batchSeq++
	return a.batchSeq
}

type queueAuditEventKind uint8

const (
	// auditBatchStart opens a batch segment; carries the batch op and the
	// seeded action IDs and identity keys.
	auditBatchStart queueAuditEventKind = iota
	// auditDequeue: a worker took the action (actionID).
	auditDequeue
	// auditCommit: a transition's commit ran and succeeded; carries the
	// action IDs and identity keys of the same-op children admitted to
	// this batch's queue.
	auditCommit
	// auditReject: a transition was refused before commit (duplicate,
	// cap, or post-abort refusal); no state was mutated.
	auditReject
	// auditDone: a worker finished processing a dequeued action.
	auditDone
	// auditAbort: the batch was aborted.
	auditAbort
	// auditBatchEnd closes a batch segment; carries whether the batch
	// drained cleanly (no worker error).
	auditBatchEnd
)

type queueAuditEvent struct {
	kind  queueAuditEventKind
	batch int
	op    ActionOp
	// actionIDs: seeded IDs (auditBatchStart), enqueued child IDs
	// (auditCommit), or the single dequeued ID (auditDequeue).
	actionIDs []string
	// keys are the queue identity digests matching actionIDs
	// (auditBatchStart, auditCommit); the checker re-verifies dedup
	// soundness independently of the queue's own seen set.
	keys []parallelActionKey
	// clean marks a batch end without worker error (auditBatchEnd).
	clean bool
	// postAbort marks a reject that happened after the batch aborted
	// (auditReject); the checker asserts such transitions never commit.
	postAbort bool
}

func (a *queueAudit) record(ev queueAuditEvent) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, ev)
}

// snapshot returns a copy of the recorded events.
func (a *queueAudit) snapshot() []queueAuditEvent {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]queueAuditEvent, len(a.events))
	copy(out, a.events)
	return out
}
