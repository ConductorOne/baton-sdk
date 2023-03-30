package c1_manager

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
	default:
		return "unknown"
	}
}

func (a *apiTask) GetTaskId() string {
	return a.task.GetId()
}

type c1TaskManager struct {
	serviceClient v1.ConnectorWorkServiceClient
}

func (c *c1TaskManager) Next(ctx context.Context) (tasks.Task, time.Duration, error) {
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

func (c *c1TaskManager) Run(ctx context.Context, task tasks.Task, cc types.ConnectorClient) error {
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

		l.Error("error handling task", zap.Error(err))
		_, rpcErr := c.serviceClient.FinishTask(ctx, &v1.FinishTaskRequest{
			TaskId: task.GetTaskId(),
			FinalState: &v1.FinishTaskRequest_Error{
				Error: &v1.Error{
					Error: err.Error(),
					Fatal: fatal,
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
	go func() {
		var waitDuration time.Duration
		for {
			l.Debug("waiting to heartbeat", zap.Duration("wait_duration", waitDuration))
			select {
			case <-taskCtx.Done():
				return
			case <-time.After(waitDuration):
				resp, err := c.serviceClient.Heartbeat(taskCtx, &v1.HeartbeatRequest{
					TaskId: task.GetTaskId(),
				})
				if err != nil {
					l.Error("error sending heartbeat", zap.Error(err))
					cancel(err)
				}
				if resp == nil {
					l.Debug("heartbeat response was nil, retrying in 30 seconds")
					waitDuration = errTimeoutDuration
					continue
				}

				l.Debug("heartbeat successful", zap.Duration("next_deadline", resp.GetNextDeadline().AsDuration()))
				if resp.Cancelled {
					l.Debug("task cancelled by upstream")
					cancel(nil)
				}
				waitDuration = resp.GetNextDeadline().AsDuration()
			}
		}
	}()

	// TODO(jirwin): Figure out how to handle task dispatching
	switch task.GetTaskType() {
	case "sync_full":
		err := c.handleLocalFileSync(taskCtx, cc, &tasks.LocalFileSync{DbPath: "sync-server.c1z"})
		if err != nil {
			l.Error("error handling local file sync", zap.Error(err))
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
	_, err := c.serviceClient.FinishTask(ctx, &v1.FinishTaskRequest{
		TaskId: task.GetTaskId(),
		FinalState: &v1.FinishTaskRequest_Success{
			Success: &v1.Success{},
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

	return &c1TaskManager{
		serviceClient: serviceClient,
	}, nil
}
