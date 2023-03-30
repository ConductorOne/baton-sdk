package tasks

import (
	"context"
)

type Task interface {
	GetTaskType() string
	GetTaskId() string
}

type Manager interface {
	Next(ctx context.Context) (Task, error)
	Finish(ctx context.Context, taskID string) error
	Add(ctx context.Context, task Task) error
}
