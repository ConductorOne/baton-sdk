package c1api

import (
	"sync"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	taskTypes "github.com/conductorone/baton-sdk/pkg/types/tasks"
)

type taskQueue struct {
	mtx        sync.Mutex
	queued     []*v1.Task
	inFlight   map[string]struct{}
	parallel   int
	nextPollAt time.Time
}

type pollAction int

const (
	pollActionNone pollAction = iota
	pollActionFetch
	pollActionWait
)

type pollDecision struct {
	action pollAction
	wait   time.Duration
}

func newTaskQueue(parallelism int) *taskQueue {
	return &taskQueue{
		inFlight: make(map[string]struct{}),
		parallel: parallelism,
	}
}

func (q *taskQueue) take() *v1.Task {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if len(q.queued) == 0 {
		return nil
	}
	task := q.queued[0]
	q.queued = q.queued[1:]
	if id := task.GetId(); id != "" {
		q.inFlight[id] = struct{}{}
	}
	return task
}

func (q *taskQueue) enqueue(batch []*v1.Task) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	for _, task := range batch {
		if task == nil || tasks.Is(task, taskTypes.NoneType) {
			continue
		}
		id := task.GetId()
		if id != "" {
			if _, ok := q.inFlight[id]; ok {
				continue
			}
			if q.isQueuedLocked(id) {
				continue
			}
		}
		q.queued = append(q.queued, task)
	}
}

func (q *taskQueue) markDone(task *v1.Task) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if id := task.GetId(); id != "" {
		delete(q.inFlight, id)
	}
}

// fetchParams returns the params for the next GetTasks request: the IDs the
// server should skip (queued + in-flight) and how many additional tasks we
// want. Computed under a single lock so the two values agree on the same
// snapshot.
func (q *taskQueue) fetchParams() ([]string, uint32) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	knownIDs := make([]string, 0, len(q.queued)+len(q.inFlight))
	for _, task := range q.queued {
		if id := task.GetId(); id != "" {
			knownIDs = append(knownIDs, id)
		}
	}
	for id := range q.inFlight {
		knownIDs = append(knownIDs, id)
	}
	target := knownTaskHighWater(q.parallel)
	known := uint32(len(knownIDs)) //nolint:gosec // bounded by local task queue and small target.
	// Callers only invoke fetchParams when pollDecision returns pollActionFetch,
	// which guarantees known < lowWater < target, so target-known never underflows.
	pageSize := target - known
	return knownIDs, pageSize
}

func (q *taskQueue) setNextPoll(nextPoll time.Duration) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	q.nextPollAt = time.Now().Add(nextPoll)
}

func (q *taskQueue) pollDecision() pollDecision {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	needsTopUp := len(q.queued)+len(q.inFlight) < int(knownTaskLowWater(q.parallel))
	if !needsTopUp {
		return pollDecision{}
	}
	if q.nextPollAt.IsZero() {
		return pollDecision{action: pollActionFetch}
	}
	d := time.Until(q.nextPollAt)
	if d <= 0 {
		return pollDecision{action: pollActionFetch}
	}
	return pollDecision{action: pollActionWait, wait: d}
}

func (q *taskQueue) isQueuedLocked(id string) bool {
	for _, task := range q.queued {
		if task.GetId() == id {
			return true
		}
	}
	return false
}

func knownTaskLowWater(parallelism int) uint32 {
	if parallelism <= 0 {
		parallelism = 1
	}
	return uint32(parallelism)
}

func knownTaskHighWater(parallelism int) uint32 {
	// Keep one full concurrency window of headroom without allowing the local
	// queue to grow unbounded.
	return knownTaskLowWater(parallelism) * 2
}
