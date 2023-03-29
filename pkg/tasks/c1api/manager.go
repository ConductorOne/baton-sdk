package c1api

import (
	"context"
	"errors"
	"sync"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/service_mode/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
)

var (
	errTimeoutDuration      = time.Second * 30
	ErrTaskHeartbeatFailure = errors.New("task heart beating failed")
	ErrTaskFatality         = errors.New("task failed fatally")
	startupHelloTaskID      = ksuid.New().String()
)

type c1ApiTaskManager struct {
	startupHello  sync.Once
	serviceClient C1ServiceClient
}

func (c *c1ApiTaskManager) backoffJitter(d time.Duration) time.Duration {
	return d
}

func (c *c1ApiTaskManager) heartbeatTask(ctx context.Context, task *v1.Task) error {
	// HACK(jirwin): We don't want to heartbeat the startup hello task, so we generate a unique ID for it and skip it here.
	if task.GetId() == startupHelloTaskID {
		return nil
	}

	l := ctxzap.Extract(ctx).With(zap.String("task_id", task.GetId()), zap.Stringer("task_type", tasks.GetType(task)))
	var waitDuration time.Duration

	for {
		l.Debug("waiting to heartbeat", zap.Duration("wait_duration", waitDuration))
		select {
		case <-ctx.Done():
			return nil

		case <-time.After(waitDuration):
			resp, err := c.serviceClient.Heartbeat(ctx, &v1.HeartbeatRequest{
				TaskId: task.GetId(),
			})
			if err != nil {
				l.Error("error sending heartbeat", zap.Error(err))
				return err
			}

			if resp == nil {
				l.Debug("heartbeat response was nil, cancelling task")
				return ErrTaskHeartbeatFailure
			}

			l.Debug("heartbeat successful", zap.Duration("next_deadline", resp.GetNextDeadline().AsDuration()))
			if resp.Cancelled {
				return ErrTaskHeartbeatFailure
			}
			waitDuration = resp.GetNextDeadline().AsDuration()
		}
	}
}

func (c *c1ApiTaskManager) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	l := ctxzap.Extract(ctx)

	var task *v1.Task
	c.startupHello.Do(func() {
		l.Debug("queueing startup hello task")
		task = &v1.Task{
			Id: startupHelloTaskID,
			TaskType: &v1.Task_Hello{
				Hello: &v1.Task_HelloTask{},
			},
		}
	})

	if task != nil {
		return task, c.backoffJitter(time.Second), nil
	}

	l.Info("Checking for new tasks...")

	resp, err := c.serviceClient.GetTask(ctx, &v1.GetTaskRequest{})
	if err != nil {
		return nil, c.backoffJitter(errTimeoutDuration), err
	}

	if resp.Task == nil || tasks.Is(resp.Task, tasks.NoneType) {
		return nil, c.backoffJitter(resp.GetNextPoll().AsDuration()), nil
	}

	return resp.Task, c.backoffJitter(resp.GetNextPoll().AsDuration()), nil
}

func (c *c1ApiTaskManager) finishTask(ctx context.Context, task *v1.Task, err error) error {
	l := ctxzap.Extract(ctx)

	finishCtx, finishCanc := context.WithTimeout(context.Background(), time.Second*30)
	defer finishCanc()

	if err == nil {
		l.Debug("finishing task successfully")
		_, err = c.serviceClient.FinishTask(finishCtx, &v1.FinishTaskRequest{
			TaskId: task.GetId(),
			FinalState: &v1.FinishTaskRequest_Success_{
				Success: &v1.FinishTaskRequest_Success{},
			},
		})
		if err != nil {
			l.Error("error while attempting to finish task successfully", zap.Error(err))
			return err
		}

		return nil
	}

	l.Error("finishing task with error", zap.Error(err))
	_, rpcErr := c.serviceClient.FinishTask(finishCtx, &v1.FinishTaskRequest{
		TaskId: task.GetId(),
		FinalState: &v1.FinishTaskRequest_Error_{
			Error: &v1.FinishTaskRequest_Error{
				Error: err.Error(),
				Fatal: errors.Is(err, ErrTaskFatality),
			},
		},
	})
	if rpcErr != nil {
		l.Error("error finishing task", zap.Error(rpcErr))
		return errors.Join(err, rpcErr)
	}

	return err
}

func (c *c1ApiTaskManager) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	l := ctxzap.Extract(ctx)
	if task == nil {
		l.Debug("process called with nil task -- continuing")
		return nil
	}

	l = l.With(
		zap.String("task_id", task.GetId()),
		zap.Stringer("task_type", tasks.GetType(task)),
	)

	l.Debug("processing task")

	taskCtx, cancelTask := context.WithCancelCause(ctx)
	defer cancelTask(nil)

	// Begin heartbeat loop for task
	go func() {
		err := c.heartbeatTask(taskCtx, task)
		if err != nil {
			l.Debug("error while heart beating", zap.Error(err))
			cancelTask(err)
		}
	}()

	tHelpers := &taskHelpers{
		task:          task,
		cc:            cc,
		serviceClient: c.serviceClient,
		taskFinisher:  c.finishTask,
	}

	// Based on the task type, call a handler to process the task.
	// It is the responsibility of each handler to finish the task when it is complete.
	// Handlers may do their work in a goroutine allowing processing to move onto the next task
	var handler tasks.TaskHandler
	switch tasks.GetType(task) {
	case tasks.FullSyncType:
		handler = newFullSyncTaskHandler(task, tHelpers)

	case tasks.HelloType:
		handler = newHelloTaskHandler(task, task.GetId() != startupHelloTaskID, tHelpers)

	case tasks.GrantType:
		handler = newGrantTaskHandler(task, tHelpers)

	case tasks.RevokeType:
		handler = newRevokeTaskHandler(task, tHelpers)

	default:
		return c.finishTask(ctx, task, errors.New("unsupported task type"))
	}

	if handler == nil {
		return c.finishTask(ctx, task, errors.New("unsupported task type - no handler"))
	}

	err := handler.HandleTask(ctx)
	if err != nil {
		l.Error("error while handling task", zap.Error(err))
		cancelTask(err)
		return err
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
