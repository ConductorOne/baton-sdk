package tasks

import (
	"github.com/segmentio/ksuid"
)

type ManualSyncTask struct {
	taskID string
	DbPath string
}

func (s ManualSyncTask) GetTaskType() string {
	return "sync"
}

func (s ManualSyncTask) GetTaskId() string {
	return s.taskID
}

func NewManualSyncTask(dbPath string) *ManualSyncTask {
	return &ManualSyncTask{
		taskID: ksuid.New().String(),
		DbPath: dbPath,
	}
}
