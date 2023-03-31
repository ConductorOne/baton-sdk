package c1api

import (
	"context"
	"errors"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/service_mode/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

var (
	errTimeoutDuration = time.Second * 30
)

type apiTask struct {
	task *v1.Task
}

func (a *apiTask) GetTaskType() string {
	switch a.task.GetTaskType().(type) {
	case *v1.Task_SyncFull:
		return "sync_full"
	case *v1.Task_Grant:
		return "grant"
	case *v1.Task_Revoke:
		return "revoke"
	case *v1.Task_Hello:
		return "hello"
	case *v1.Task_None:
		return "none"
	default:
		return "unknown"
	}
}

func (a *apiTask) GetTaskId() string {
	return a.task.GetId()
}

type c1ApiTaskManager struct {
	serviceClient *c1ServiceClient
}

func (c *c1ApiTaskManager) heartbeatTask(ctx context.Context, task tasks.Task, canc context.CancelCauseFunc) {
	l := ctxzap.Extract(ctx)
	var waitDuration time.Duration
	for {
		l.Debug("waiting to heartbeat", zap.Duration("wait_duration", waitDuration))
		select {
		case <-ctx.Done():
			return

		case <-time.After(waitDuration):
			resp, err := c.serviceClient.Heartbeat(ctx, &v1.HeartbeatRequest{
				TaskId: task.GetTaskId(),
			})
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				l.Error("error sending heartbeat", zap.Error(err))
				return
			}

			if resp == nil {
				l.Debug("heartbeat response was nil, cancelling task")
				return
			}

			l.Debug("heartbeat successful", zap.Duration("next_deadline", resp.GetNextDeadline().AsDuration()))
			if resp.Cancelled {
				canc(errors.New("task canceled by upstream"))
				return
			}
			waitDuration = resp.GetNextDeadline().AsDuration()
		}
	}
}

func (c *c1ApiTaskManager) Next(ctx context.Context) (tasks.Task, time.Duration, error) {
	l := ctxzap.Extract(ctx)
	resp, err := c.serviceClient.GetTask(ctx, &v1.GetTaskRequest{})
	if err != nil {
		return nil, errTimeoutDuration, err
	}

	if resp.Task == nil {
		l.Debug("no task to handle")
		return nil, resp.GetNextPoll().AsDuration(), nil
	}

	return &apiTask{resp.Task}, resp.GetNextPoll().AsDuration(), nil
}

func (c *c1ApiTaskManager) Run(ctx context.Context, task tasks.Task, cc types.ConnectorClient) error {
	l := ctxzap.Extract(ctx)
	if task == nil {
		l.Debug("no task to handle")
		return nil
	}

	l = l.With(
		zap.String("task_id", task.GetTaskId()),
		zap.String("task_type", task.GetTaskType()),
	)

	handleErr := func(err error, fatal bool) error {
		if err == nil {
			return nil
		}

		finishCtx, finishCanc := context.WithTimeout(context.Background(), time.Second*30)
		defer finishCanc()
		l.Error("error handling task", zap.Error(err))
		_, rpcErr := c.serviceClient.FinishTask(finishCtx, &v1.FinishTaskRequest{
			TaskId: task.GetTaskId(),
			FinalState: &v1.FinishTaskRequest_Error_{
				Error: &v1.FinishTaskRequest_Error{
					Error: err.Error(),
				},
			},
		})
		if rpcErr != nil {
			l.Error("error finishing task", zap.Error(rpcErr))
			return errors.Join(err, rpcErr)
		}

		return nil
	}

	taskCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	l.Info("handling task")

	// Begin heartbeat loop for task
	go c.heartbeatTask(taskCtx, task, cancel)

	// TODO(jirwin): Figure out how to handle task dispatching
	// TODO(jirwin): It would be nice if the service client didn't have to be handed to as task
	switch task.GetTaskType() {
	case "sync_full":
		err := c.handleSyncUpload(taskCtx, c.serviceClient, cc, tasks.NewSyncUpload(task.GetTaskId()))
		if err != nil {
			l.Error("error handling full sync", zap.Error(err))
			return handleErr(err, false)
		}
	default:
		return errors.New("unknown task type")
	}

	var taskErr error
	select {
	case <-taskCtx.Done():
		taskErr = taskCtx.Err()
	default:
	}
	if taskErr != nil {
		return handleErr(taskErr, false)
	}

	cancel(nil)
	finishCtx, finishCanc := context.WithTimeout(context.Background(), time.Second*30)
	defer finishCanc()
	_, err := c.serviceClient.FinishTask(finishCtx, &v1.FinishTaskRequest{
		TaskId: task.GetTaskId(),
		FinalState: &v1.FinishTaskRequest_Success_{
			Success: &v1.FinishTaskRequest_Success{},
		},
	})
	if err != nil {
		return handleErr(err, true)
	}

	return nil
}

func NewC1TaskManager(ctx context.Context, clientID string, clientSecret string) (tasks.Manager, error) {
	serviceClient, err := newServiceClient(ctx, clientID, clientSecret)
	if err != nil {
		return nil, err
	}

	return &c1ApiTaskManager{
		serviceClient: serviceClient,
	}, nil
}
