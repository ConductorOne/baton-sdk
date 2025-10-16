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
	ShouldDebug() bool
	GetTempDir() string
}

type TaskHandler interface {
	HandleTask(ctx context.Context) error
}

func Is(task *v1.Task, target taskTypes.TaskType) bool {
	if task == nil || !task.HasTaskType() {
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
	case taskTypes.BulkCreateTicketsType:
		_, ok = task.GetTaskType().(*v1.Task_BulkCreateTickets)
	case taskTypes.BulkGetTicketsType:
		_, ok = task.GetTaskType().(*v1.Task_BulkGetTickets)
	case taskTypes.ActionListSchemasType:
		_, ok = task.GetTaskType().(*v1.Task_ActionListSchemas)
	case taskTypes.ActionGetSchemaType:
		_, ok = task.GetTaskType().(*v1.Task_ActionGetSchema)
	case taskTypes.ActionInvokeType:
		_, ok = task.GetTaskType().(*v1.Task_ActionInvoke)
	case taskTypes.ActionStatusType:
		_, ok = task.GetTaskType().(*v1.Task_ActionStatus)
	case taskTypes.CreateSyncDiff:
		_, ok = task.GetTaskType().(*v1.Task_CreateSyncDiff)
	default:
		return false
	}

	return ok
}

func GetType(task *v1.Task) taskTypes.TaskType {
	if task == nil || !task.HasTaskType() {
		return taskTypes.UnknownType
	}

	switch task.WhichTaskType() {
	case v1.Task_SyncFull_case:
		return taskTypes.FullSyncType
	case v1.Task_Grant_case:
		return taskTypes.GrantType
	case v1.Task_Revoke_case:
		return taskTypes.RevokeType
	case v1.Task_Hello_case:
		return taskTypes.HelloType
	case v1.Task_EventFeed_case:
		return taskTypes.EventFeedType
	case v1.Task_None_case:
		return taskTypes.NoneType
	case v1.Task_CreateAccount_case:
		return taskTypes.CreateAccountType
	case v1.Task_CreateResource_case:
		return taskTypes.CreateResourceType
	case v1.Task_DeleteResource_case:
		return taskTypes.DeleteResourceType
	case v1.Task_RotateCredentials_case:
		return taskTypes.RotateCredentialsType
	case v1.Task_CreateTicketTask_case:
		return taskTypes.CreateTicketType
	case v1.Task_ListTicketSchemas_case:
		return taskTypes.ListTicketSchemasType
	case v1.Task_GetTicket_case:
		return taskTypes.GetTicketType
	case v1.Task_BulkCreateTickets_case:
		return taskTypes.BulkCreateTicketsType
	case v1.Task_BulkGetTickets_case:
		return taskTypes.BulkGetTicketsType
	case v1.Task_ActionListSchemas_case:
		return taskTypes.ActionListSchemasType
	case v1.Task_ActionGetSchema_case:
		return taskTypes.ActionGetSchemaType
	case v1.Task_ActionInvoke_case:
		return taskTypes.ActionInvokeType
	case v1.Task_ActionStatus_case:
		return taskTypes.ActionStatusType
	case v1.Task_CreateSyncDiff_case:
		return taskTypes.CreateSyncDiff
	default:
		return taskTypes.UnknownType
	}
}
