package tasks

import (
	"context"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types"
	taskTypes "github.com/conductorone/baton-sdk/pkg/types/tasks"
)

type Manager interface {
	Next(ctx context.Context) (*v1.Task, time.Duration, error)
	Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error
}

type TaskHandler interface {
	HandleTask(ctx context.Context) error
}

func Is(task *v1.Task, target taskTypes.TaskType) bool {
	if task == nil || task.TaskType == nil {
		return false
	}

	var ok bool

	switch target {
	case taskTypes.FullSyncType:
		_, ok = task.GetTaskType().(*v1.Task_SyncFull)
	case taskTypes.GrantType:
		_, ok = task.GetTaskType().(*v1.Task_Grant)
	case taskTypes.RevokeType:
		_, ok = task.GetTaskType().(*v1.Task_Revoke)
	case taskTypes.HelloType:
		_, ok = task.GetTaskType().(*v1.Task_Hello)
	case taskTypes.EventFeedType:
		_, ok = task.GetTaskType().(*v1.Task_EventFeed)
	case taskTypes.NoneType:
		_, ok = task.GetTaskType().(*v1.Task_None)
	case taskTypes.CreateAccountType:
		_, ok = task.GetTaskType().(*v1.Task_CreateAccount)
	case taskTypes.CreateResourceType:
		_, ok = task.GetTaskType().(*v1.Task_CreateResource)
	case taskTypes.DeleteResourceType:
		_, ok = task.GetTaskType().(*v1.Task_DeleteResource)
	case taskTypes.RotateCredentialsType:
		_, ok = task.GetTaskType().(*v1.Task_RotateCredentials)
	case taskTypes.CreateTicketType:
		_, ok = task.GetTaskType().(*v1.Task_CreateTicketTask_)
	case taskTypes.ListTicketSchemasType:
		_, ok = task.GetTaskType().(*v1.Task_ListTicketSchemas)
	case taskTypes.GetTicketType:
		_, ok = task.GetTaskType().(*v1.Task_GetTicket)
	default:
		return false
	}

	return ok
}

func GetType(task *v1.Task) taskTypes.TaskType {
	if task == nil || task.TaskType == nil {
		return taskTypes.UnknownType
	}

	switch task.GetTaskType().(type) {
	case *v1.Task_SyncFull:
		return taskTypes.FullSyncType
	case *v1.Task_Grant:
		return taskTypes.GrantType
	case *v1.Task_Revoke:
		return taskTypes.RevokeType
	case *v1.Task_Hello:
		return taskTypes.HelloType
	case *v1.Task_EventFeed:
		return taskTypes.EventFeedType
	case *v1.Task_None:
		return taskTypes.NoneType
	case *v1.Task_CreateAccount:
		return taskTypes.CreateAccountType
	case *v1.Task_CreateResource:
		return taskTypes.CreateResourceType
	case *v1.Task_DeleteResource:
		return taskTypes.DeleteResourceType
	case *v1.Task_RotateCredentials:
		return taskTypes.RotateCredentialsType
	case *v1.Task_CreateTicketTask_:
		return taskTypes.CreateTicketType
	case *v1.Task_ListTicketSchemas:
		return taskTypes.ListTicketSchemasType
	case *v1.Task_GetTicket:
		return taskTypes.GetTicketType
	default:
		return taskTypes.UnknownType
	}
}
