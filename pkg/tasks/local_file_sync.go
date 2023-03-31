package tasks

import (
	"github.com/segmentio/ksuid"
)

type LocalFileSync struct {
	taskID string
	DbPath string
}

func (s *LocalFileSync) GetTaskType() string {
	return "local_file_sync"
}

func (s *LocalFileSync) GetTaskId() string {
	return s.taskID
}

func NewLocalFileSyncTask(dbPath string) *LocalFileSync {
	return &LocalFileSync{
		taskID: ksuid.New().String(),
		DbPath: dbPath,
	}
}
