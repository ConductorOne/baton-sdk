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

	actualType := task.WhichTaskType()

	switch target {
	case taskTypes.FullSyncType:
		return actualType == v1.Task_SyncFull_case
	case taskTypes.GrantType:
		return actualType == v1.Task_Grant_case
	case taskTypes.RevokeType:
		return actualType == v1.Task_Revoke_case
	case taskTypes.HelloType:
		return actualType == v1.Task_Hello_case
	case taskTypes.EventFeedType:
		return actualType == v1.Task_EventFeed_case
	case taskTypes.NoneType:
		return actualType == v1.Task_None_case
	case taskTypes.CreateAccountType:
		return actualType == v1.Task_CreateAccount_case
	case taskTypes.CreateResourceType:
		return actualType == v1.Task_CreateResource_case
	case taskTypes.DeleteResourceType:
		return actualType == v1.Task_DeleteResource_case
	case taskTypes.RotateCredentialsType:
		return actualType == v1.Task_RotateCredentials_case
	case taskTypes.CreateTicketType:
		return actualType == v1.Task_CreateTicketTask_case
	case taskTypes.ListTicketSchemasType:
		return actualType == v1.Task_ListTicketSchemas_case
	case taskTypes.GetTicketType:
		return actualType == v1.Task_GetTicket_case
	case taskTypes.BulkCreateTicketsType:
		return actualType == v1.Task_BulkCreateTickets_case
	case taskTypes.BulkGetTicketsType:
		return actualType == v1.Task_BulkGetTickets_case
	case taskTypes.ActionListSchemasType:
		return actualType == v1.Task_ActionListSchemas_case
	case taskTypes.ActionGetSchemaType:
		return actualType == v1.Task_ActionGetSchema_case
	case taskTypes.ActionInvokeType:
		return actualType == v1.Task_ActionInvoke_case
	case taskTypes.ActionStatusType:
		return actualType == v1.Task_ActionStatus_case
	case taskTypes.CreateSyncDiff:
		return actualType == v1.Task_CreateSyncDiff_case
	default:
		return false
	}
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
