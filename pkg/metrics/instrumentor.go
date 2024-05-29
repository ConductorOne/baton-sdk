package metrics

import (
	"context"
	"time"

	"github.com/conductorone/baton-sdk/pkg/tasks"
)

const (
	taskSuccessCounterName = "task_success"
	taskFailureCounterName = "task_failure"
	taskDurationHistoName  = "task_latency"
	taskSuccessCounterDesc = "number of successful tasks"
	taskFailureCounterDesc = "number of failed tasks"
	taskDurationHistoDesc  = "duration of all tasks"
)

type M struct {
	underlying Handler
}

func (m *M) RecordTaskSuccess(ctx context.Context, task tasks.TaskType, dur time.Duration) {
	c := m.underlying.Int64Counter(taskSuccessCounterName, taskSuccessCounterDesc, Dimensionless)
	h := m.underlying.Int64Histogram(taskDurationHistoName, taskDurationHistoDesc, Milliseconds)
	c.Add(ctx, 1)
	h.Record(ctx, dur.Milliseconds())
}

func (m *M) RecordTaskFailure(ctx context.Context, task tasks.TaskType, dur time.Duration) {
	c := m.underlying.Int64Counter(taskFailureCounterName, taskFailureCounterDesc, Dimensionless)
	h := m.underlying.Int64Histogram(taskDurationHistoName, taskDurationHistoDesc, Milliseconds)
	c.Add(ctx, 1)
	h.Record(ctx, dur.Milliseconds())
}

func New(handler Handler) *M {
	return &M{underlying: handler}
}
