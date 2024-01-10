package tasks

import (
	"context"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type Manager interface {
	Next(ctx context.Context) (*v1.Task, time.Duration, error)
	Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error
}

type TaskHandler interface {
	HandleTask(ctx context.Context) error
}

type TaskType uint8

func (tt TaskType) String() string {
	switch tt {
	case FullSyncType:
		return "sync_full"
	case GrantType:
		return "grant"
	case RevokeType:
		return "revoke"
	case HelloType:
		return "hello"
	case EventFeedType:
		return "event_feed"
	case NoneType:
		return "none"
	case CreateAccountType:
		return "create_account"
	case CreateResourceType:
		return "create_resource"
	case DeleteResourceType:
		return "delete_resource"
	case RotateCredentialsType:
		return "rotate_credential"
	default:
		return "unknown"
	}
}

const (
	UnknownType TaskType = iota
	NoneType
	FullSyncType
	GrantType
	RevokeType
	HelloType
	CreateAccountType
	CreateResourceType
	DeleteResourceType
	RotateCredentialsType
	EventFeedType
)

func Is(task *v1.Task, target TaskType) bool {
	if task == nil || task.TaskType == nil {
		return false
	}

	var ok bool

	switch target {
	case FullSyncType:
		_, ok = task.GetTaskType().(*v1.Task_SyncFull)
	case GrantType:
		_, ok = task.GetTaskType().(*v1.Task_Grant)
	case RevokeType:
		_, ok = task.GetTaskType().(*v1.Task_Revoke)
	case HelloType:
		_, ok = task.GetTaskType().(*v1.Task_Hello)
	case EventFeedType:
		_, ok = task.GetTaskType().(*v1.Task_EventFeed)
	case NoneType:
		_, ok = task.GetTaskType().(*v1.Task_None)
	case CreateAccountType:
		_, ok = task.GetTaskType().(*v1.Task_CreateAccount)
	case CreateResourceType:
		_, ok = task.GetTaskType().(*v1.Task_CreateResource)
	case DeleteResourceType:
		_, ok = task.GetTaskType().(*v1.Task_DeleteResource)
	case RotateCredentialsType:
		_, ok = task.GetTaskType().(*v1.Task_RotateCredentials)
	default:
		return false
	}

	return ok
}

func GetType(task *v1.Task) TaskType {
	if task == nil || task.TaskType == nil {
		return UnknownType
	}

	switch task.GetTaskType().(type) {
	case *v1.Task_SyncFull:
		return FullSyncType
	case *v1.Task_Grant:
		return GrantType
	case *v1.Task_Revoke:
		return RevokeType
	case *v1.Task_Hello:
		return HelloType
	case *v1.Task_EventFeed:
		return EventFeedType
	case *v1.Task_None:
		return NoneType
	case *v1.Task_CreateAccount:
		return CreateAccountType
	case *v1.Task_CreateResource:
		return CreateResourceType
	case *v1.Task_DeleteResource:
		return DeleteResourceType
	case *v1.Task_RotateCredentials:
		return RotateCredentialsType
	default:
		return UnknownType
	}
}
