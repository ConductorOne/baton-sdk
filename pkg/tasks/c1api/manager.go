package c1api

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/conductorone/baton-sdk/pkg/annotations"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	pbstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	taskTypes "github.com/conductorone/baton-sdk/pkg/types/tasks"
)

var (
	// getHeartbeatInterval configures limits on how often we heartbeat a running task.
	maxHeartbeatInterval     = time.Minute * 5
	minHeartbeatInterval     = time.Second * 1
	defaultHeartbeatInterval = time.Second * 30

	// pollInterval configures limits on how often we poll for new tasks.
	maxPollInterval = time.Minute * 5
	minPollInterval = time.Second * 0

	taskMaximumHeartbeatFailures = 10

	ErrTaskCancelled       = errors.New("task was cancelled")
	ErrTaskHeartbeatFailed = errors.New("task failed heartbeat")

	ErrTaskNonRetryable = errors.New("task failed and is non-retryable")
)

type c1ApiTaskManager struct {
	mtx           sync.Mutex
	started       bool
	queue         []*v1.Task
	serviceClient BatonServiceClient
	tempDir       string
	skipFullSync  bool
}

// getHeartbeatInterval returns an appropriate heartbeat interval. If the interval is 0, it will return the default heartbeat interval.
// Otherwise, it will be clamped between minHeartbeatInterval and maxHeartbeatInterval.
func getHeartbeatInterval(d time.Duration) time.Duration {
	switch {
	case d == 0:
		return defaultHeartbeatInterval
	case d < minHeartbeatInterval:
		return minHeartbeatInterval
	case d > maxHeartbeatInterval:
		return maxHeartbeatInterval
	default:
		return d
	}
}

// getNextPoll returns an appropriate poll interval. It will be clamped between minPollInterval and maxPollInterval.
func getNextPoll(d time.Duration) time.Duration {
	switch {
	case d < minPollInterval:
		return minPollInterval
	case d > maxPollInterval:
		return maxPollInterval
	default:
		return d
	}
}

func (c *c1ApiTaskManager) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	l := ctxzap.Extract(ctx)

	c.mtx.Lock()
	defer c.mtx.Unlock()
	if !c.started {
		l.Debug("c1_api_task_manager.Next(): queueing initial hello task")
		c.started = true
		// Append a hello task to the queue on startup.
		c.queue = append(c.queue, &v1.Task{
			Id:     "",
			Status: v1.Task_STATUS_PENDING,
			TaskType: &v1.Task_Hello{
				Hello: &v1.Task_HelloTask{},
			},
		})

		// TODO(morgabra) Get resumable tasks here and queue them.
	}

	if len(c.queue) != 0 {
		t := c.queue[0]
		c.queue = c.queue[1:]
		l.Debug("c1_api_task_manager.Next(): returning queued task", zap.String("task_id", t.GetId()), zap.Stringer("task_type", tasks.GetType(t)))
		return t, 0, nil
	}

	l.Debug("c1_api_task_manager.Next(): checking for new tasks")

	resp, err := c.serviceClient.GetTask(ctx, &v1.BatonServiceGetTaskRequest{})
	if err != nil {
		return nil, 0, err
	}

	nextPoll := getNextPoll(resp.GetNextPoll().AsDuration())
	l = l.With(zap.Duration("next_poll", nextPoll))

	if resp.GetTask() == nil || tasks.Is(resp.GetTask(), taskTypes.NoneType) {
		l.Debug("c1_api_task_manager.Next(): no tasks available")
		return nil, nextPoll, nil
	}

	l = l.With(
		zap.String("task_id", resp.GetTask().GetId()),
		zap.Stringer("task_type", tasks.GetType(resp.GetTask())),
	)

	l.Debug("c1_api_task_manager.Next(): got task")
	return resp.GetTask(), nextPoll, nil
}

func (c *c1ApiTaskManager) finishTask(ctx context.Context, task *v1.Task, resp proto.Message, annos annotations.Annotations, err error) error {
	l := ctxzap.Extract(ctx)
	l = l.With(
		zap.String("task_id", task.GetId()),
		zap.Stringer("task_type", tasks.GetType(task)),
	)

	finishCtx, finishCanc := context.WithTimeout(context.Background(), time.Second*30)
	defer finishCanc()

	var marshalledResp *anypb.Any
	if resp != nil {
		marshalledResp, err = anypb.New(resp)
		if err != nil {
			l.Error("c1_api_task_manager.finishTask(): error while attempting to marshal response", zap.Error(err))
			return err
		}
	}

	if err == nil {
		l.Info("c1_api_task_manager.finishTask(): finishing task successfully")
		_, err = c.serviceClient.FinishTask(finishCtx, &v1.BatonServiceFinishTaskRequest{
			TaskId: task.GetId(),
			Status: nil,
			FinalState: &v1.BatonServiceFinishTaskRequest_Success_{
				Success: &v1.BatonServiceFinishTaskRequest_Success{
					Annotations: annos,
					Response:    marshalledResp,
				},
			},
		})
		if err != nil {
			l.Error("c1_api_task_manager.finishTask(): error while attempting to finish task successfully", zap.Error(err))
			return err
		}

		return nil
	}

	l.Error("c1_api_task_manager.finishTask(): finishing task with error", zap.Error(err))

	statusErr, ok := status.FromError(err)
	if !ok {
		statusErr = status.New(codes.Unknown, err.Error())
	}

	_, rpcErr := c.serviceClient.FinishTask(finishCtx, &v1.BatonServiceFinishTaskRequest{
		TaskId: task.GetId(),
		Status: &pbstatus.Status{
			Code:    int32(statusErr.Code()),
			Message: statusErr.Message(),
		},
		FinalState: &v1.BatonServiceFinishTaskRequest_Error_{
			Error: &v1.BatonServiceFinishTaskRequest_Error{
				NonRetryable: errors.Is(err, ErrTaskNonRetryable),
				Annotations:  annos,
			},
		},
	})
	if rpcErr != nil {
		l.Error("c1_api_task_manager.finishTask(): error finishing task", zap.Error(rpcErr))
		return errors.Join(err, rpcErr)
	}

	return err
}

func (c *c1ApiTaskManager) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	l := ctxzap.Extract(ctx)
	if task == nil {
		l.Debug("c1_api_task_manager.Process(): process called with nil task -- continuing")
		return nil
	}

	l = l.With(
		zap.String("task_id", task.GetId()),
		zap.Stringer("task_type", tasks.GetType(task)),
	)

	l.Info("c1_api_task_manager.Process(): processing task")

	tHelpers := &taskHelpers{
		task:          task,
		cc:            cc,
		serviceClient: c.serviceClient,
		taskFinisher:  c.finishTask,
		tempDir:       c.tempDir,
	}

	// Based on the task type, call a handler to process the task.
	// It is the responsibility of each handler to finish the task when it is complete.
	// Handlers may do their work in a goroutine allowing processing to move onto the next task
	var handler tasks.TaskHandler
	switch tasks.GetType(task) {
	case taskTypes.FullSyncType:
		handler = newFullSyncTaskHandler(task, tHelpers, c.skipFullSync)

	case taskTypes.HelloType:
		handler = newHelloTaskHandler(task, tHelpers)

	case taskTypes.GrantType:
		handler = newGrantTaskHandler(task, tHelpers)

	case taskTypes.RevokeType:
		handler = newRevokeTaskHandler(task, tHelpers)

	case taskTypes.CreateAccountType:
		handler = newCreateAccountTaskHandler(task, tHelpers)

	case taskTypes.CreateResourceType:
		handler = newCreateResourceTaskHandler(task, tHelpers)

	case taskTypes.DeleteResourceType:
		handler = newDeleteResourceTaskHandler(task, tHelpers)

	case taskTypes.RotateCredentialsType:
		handler = newRotateCredentialsTaskHandler(task, tHelpers)
	case taskTypes.CreateTicketType:
		handler = newCreateTicketTaskHandler(task, tHelpers)
	case taskTypes.ListTicketSchemasType:
		handler = newListSchemasTaskHandler(task, tHelpers)
	case taskTypes.GetTicketType:
		handler = newGetTicketTaskHandler(task, tHelpers)
	default:
		return c.finishTask(ctx, task, nil, nil, errors.New("unsupported task type"))
	}

	err := handler.HandleTask(ctx)
	if err != nil {
		l.Error("c1_api_task_manager.Process(): error while handling task", zap.Error(err))
		return err
	}

	return nil
}

func NewC1TaskManager(ctx context.Context, clientID string, clientSecret string, tempDir string, skipFullSync bool) (tasks.Manager, error) {
	serviceClient, err := newServiceClient(ctx, clientID, clientSecret)
	if err != nil {
		return nil, err
	}

	return &c1ApiTaskManager{
		serviceClient: serviceClient,
		tempDir:       tempDir,
		skipFullSync:  skipFullSync,
	}, nil
}
