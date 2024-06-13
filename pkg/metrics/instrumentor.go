package metrics

import (
	"context"
	"time"

	"github.com/conductorone/baton-sdk/pkg/types/tasks"
)

const (
	taskSuccessCounterName = "baton_sdk.task_success"
	taskFailureCounterName = "baton_sdk.task_failure"
	taskDurationHistoName  = "baton_sdk.task_latency"
	taskSuccessCounterDesc = "number of successful tasks by task type"
	taskFailureCounterDesc = "number of failed tasks by task type"
	taskDurationHistoDesc  = "duration of all tasks by task type and status"
)

type M struct {
	underlying Handler
}

func (m *M) RecordTaskSuccess(ctx context.Context, task tasks.TaskType, dur time.Duration) {
	c := m.underlying.Int64Counter(taskSuccessCounterName, taskSuccessCounterDesc, Dimensionless)
	h := m.underlying.Int64Histogram(taskDurationHistoName, taskDurationHistoDesc, Milliseconds)
	c.Add(ctx, 1, map[string]string{"task_type": task.String()})
	h.Record(ctx, dur.Milliseconds(), map[string]string{"task_type": task.String(), "task_status": "success"})
}

func (m *M) RecordTaskFailure(ctx context.Context, task tasks.TaskType, dur time.Duration) {
	c := m.underlying.Int64Counter(taskFailureCounterName, taskFailureCounterDesc, Dimensionless)
	h := m.underlying.Int64Histogram(taskDurationHistoName, taskDurationHistoDesc, Milliseconds)
	c.Add(ctx, 1, map[string]string{"task_type": task.String()})
	h.Record(ctx, dur.Milliseconds(), map[string]string{"task_type": task.String(), "task_status": "failure"})
}

func New(handler Handler) *M {
	return &M{underlying: handler}
}
