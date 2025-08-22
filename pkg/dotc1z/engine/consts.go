package engine

import (
	"time"
)

type SyncType string

const (
	SyncTypeFull    SyncType = "full"
	SyncTypePartial SyncType = "partial"
	SyncTypeAny     SyncType = ""
)

type SyncRun struct {
	ID           string
	StartedAt    *time.Time
	EndedAt      *time.Time
	SyncToken    string
	Type         SyncType
	ParentSyncID string
}
