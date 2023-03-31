package tasks

type SyncUpload struct {
	taskID string
}

func (s *SyncUpload) GetTaskType() string {
	return "sync_upload"
}

func (s *SyncUpload) GetTaskId() string {
	return s.taskID
}

func NewSyncUpload(taskID string) *SyncUpload {
	return &SyncUpload{
		taskID: taskID,
	}
}
