package sync //nolint:revive,nolintlint // backwards-compatible package name

// Post-hoc contract checker for the parallel scheduler (Option D of the
// verification plan; see docs/BUG_CATCHING.md §2 in the overview repo).
//
// The queue's failure modes are silent: a dropped action produces absent
// data, a double execution produces duplicates, a post-abort commit
// produces state the checkpoint never admitted — none of them log. The
// queueAudit records every queue event at its mutation site (under q.mu,
// so recorded order is real order), and verifyQueueAudit replays the log
// against the contract:
//
//   C1  exactly-once: every seeded/admitted action is dequeued at most
//       once, and — on a clean batch — exactly once (full drain).
//   C2  admission before execution: nothing is dequeued that was never
//       seeded or committed.
//   C3  no commits after abort: an aborted batch admits nothing further.
//   C4  dedup soundness: no two actions in one batch share an identity
//       digest (re-verified from the recorded keys, independent of the
//       queue's own seen set).
//   C5  accounting: dones never exceed dequeues, and on any batch end
//       they match (every taken action was returned).
//
// Any heavy test that runs the scheduler gets these checks for free by
// attaching an audit (syncer.testQueueAudit) and calling verifyQueueAudit.

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type auditBatchState struct {
	op        ActionOp
	admitted  map[string]bool // actionID -> dequeued yet?
	keys      map[parallelActionKey]string
	dequeues  int
	dones     int
	aborted   bool
	ended     bool
	clean     bool
	violation []string
}

func (b *auditBatchState) violatef(format string, args ...any) {
	b.violation = append(b.violation, fmt.Sprintf(format, args...))
}

// verifyQueueAudit checks every recorded batch segment against the queue
// contract. Call after the sync attempts under test have finished.
func verifyQueueAudit(t *testing.T, audit *queueAudit) {
	t.Helper()
	batches := map[int]*auditBatchState{}

	admit := func(b *auditBatchState, batch int, ids []string, keys []parallelActionKey) {
		for i, id := range ids {
			if _, ok := b.admitted[id]; ok {
				b.violatef("action %s admitted twice", id)
			}
			b.admitted[id] = false
			key := keys[i]
			if prev, ok := b.keys[key]; ok {
				b.violatef("actions %s and %s share identity digest in batch %d (dedup unsound)", prev, id, batch)
			}
			b.keys[key] = id
		}
	}

	for _, ev := range audit.snapshot() {
		b := batches[ev.batch]
		if b == nil {
			require.Equal(t, auditBatchStart, ev.kind,
				"audit: batch %d's first event must be batch-start, got kind %d", ev.batch, ev.kind)
			b = &auditBatchState{
				op:       ev.op,
				admitted: map[string]bool{},
				keys:     map[parallelActionKey]string{},
			}
			batches[ev.batch] = b
			admit(b, ev.batch, ev.actionIDs, ev.keys)
			continue
		}
		if b.ended {
			b.violatef("event kind %d after batch end", ev.kind)
			continue
		}
		switch ev.kind {
		case auditBatchStart:
			b.violatef("duplicate batch-start")
		case auditDequeue:
			id := ev.actionIDs[0]
			dequeued, ok := b.admitted[id]
			switch {
			case !ok:
				b.violatef("action %s dequeued but never admitted (C2)", id)
			case dequeued:
				b.violatef("action %s dequeued twice (C1)", id)
			default:
				b.admitted[id] = true
			}
			b.dequeues++
			if b.aborted {
				b.violatef("dequeue of %s after abort", id)
			}
		case auditCommit:
			if b.aborted {
				b.violatef("transition committed after abort (C3)")
			}
			admit(b, ev.batch, ev.actionIDs, ev.keys)
		case auditReject:
			// Rejections mutate nothing; post-abort rejections are the
			// REQUIRED behavior (the abort path returning cancellation
			// instead of committing), so nothing to check.
		case auditDone:
			b.dones++
			if b.dones > b.dequeues {
				b.violatef("done count %d exceeds dequeue count %d (C5)", b.dones, b.dequeues)
			}
		case auditAbort:
			b.aborted = true
		case auditBatchEnd:
			b.ended = true
			b.clean = ev.clean
			if b.dones != b.dequeues {
				b.violatef("batch ended with %d dequeues but %d dones (C5)", b.dequeues, b.dones)
			}
			if ev.clean {
				if b.aborted {
					b.violatef("batch reported clean but recorded an abort")
				}
				for id, dequeued := range b.admitted {
					if !dequeued {
						b.violatef("clean batch ended with admitted action %s never dequeued (C1 drain)", id)
					}
				}
			}
		}
	}

	for batch, b := range batches {
		require.Truef(t, b.ended, "audit: batch %d (op %s) never recorded batch-end", batch, b.op.String())
		require.Emptyf(t, b.violation,
			"audit: queue contract violations in batch %d (op %s):\n%v", batch, b.op.String(), b.violation)
	}
}

// attachQueueAudit installs a fresh audit on the syncer under test and
// returns it. Same-package tests only.
func attachQueueAudit(t *testing.T, s Syncer) *queueAudit {
	t.Helper()
	sc, ok := s.(*syncer)
	require.True(t, ok, "syncer under test is not *syncer")
	audit := &queueAudit{}
	sc.testQueueAudit = audit
	return audit
}
