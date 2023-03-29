package tasks

import (
	"context"

	"github.com/segmentio/ksuid"
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

type SyncTask struct {
	taskID string
}

func (s SyncTask) GetTaskType() string {
	return "sync"
}

func (s SyncTask) GetTaskId() string {
	return s.taskID
}

func NewSyncTask() *SyncTask {
	id := ksuid.New().String()
	return &SyncTask{taskID: id}
}
