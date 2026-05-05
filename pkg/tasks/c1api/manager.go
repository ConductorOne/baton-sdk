package c1api

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/uotel"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	pbstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
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

	// initialHelloBackoff / maxHelloBackoff bound the exponential backoff used
	// when the startup Hello fails with a retryable error. They are package
	// vars (rather than const) so tests can override them with small values.
	initialHelloBackoff = 1 * time.Second
	maxHelloBackoff     = 5 * time.Minute
)

type c1ApiTaskManager struct {
	serviceClient                       BatonServiceClient
	tempDir                             string
	skipFullSync                        bool
	externalResourceC1Z                 string
	externalResourceEntitlementIdFilter string
	targetedSyncResources               []*v2.Resource
	syncResourceTypeIDs                 []string
	workerCount                         int

	// runnerShouldDebug is flipped by the StartDebugging task handler (which
	// runs on a task-processing goroutine) and read by the runner loop via
	// ShouldDebug(). It is atomic to avoid a data race between those two.
	runnerShouldDebug atomic.Bool
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

// Bootstrap performs the startup Hello handshake with exponential backoff,
// retrying transient failures up to maxHelloBackoff and bailing on ctx cancel
// or known-permanent gRPC codes (auth, malformed, unimplemented, etc.). The
// runner is expected to call this exactly once after construction and before
// entering the task loop.
type BootstrappingTaskManager interface {
	tasks.Manager
	Bootstrap(ctx context.Context, cc types.ConnectorClient) error
}

func (c *c1ApiTaskManager) Bootstrap(ctx context.Context, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "c1ApiTaskManager.Bootstrap")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)

	backoff := initialHelloBackoff
	attempt := 0
	for {
		attempt++
		err = sendHello(ctx, cc, c.serviceClient, "")
		if err == nil {
			l.Info("c1_api_task_manager: startup Hello succeeded.", zap.Int("attempts", attempt))
			return nil
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		if !isRetryableHelloError(err) {
			l.Error(
				"c1_api_task_manager: startup Hello failed with non-retryable error; giving up",
				zap.Error(err),
				zap.Int("attempts", attempt),
			)
			return err
		}

		l.Warn(
			"c1_api_task_manager: startup Hello failed; will retry",
			zap.Error(err),
			zap.Int("attempt", attempt),
			zap.Duration("next_backoff", backoff),
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxHelloBackoff {
			backoff = maxHelloBackoff
		}
	}
}

// isRetryableHelloError classifies Hello errors. Non-gRPC and unclassified
// errors are treated as retryable by default; known-permanent gRPC codes
// (auth, malformed, unimplemented, missing tenant/connector, server-side
// precondition) short-circuit the retry loop.
func isRetryableHelloError(err error) bool {
	if err == nil {
		return false
	}
	// If the caller's ctx already expired/cancelled, retrying would just burn
	// time we no longer have. We check this via errors.Is on the sentinels so
	// a gRPC-wrapped DeadlineExceeded that originated from our ctx still
	// counts as non-retryable. A bare codes.DeadlineExceeded from the server
	// (without a ctx sentinel in the chain) falls through to the default
	// branch below and is treated as transient — that's the server-side
	// timeout case, which can succeed on a retry.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return true
	}
	switch st.Code() {
	case codes.Unauthenticated,
		codes.PermissionDenied,
		codes.InvalidArgument,
		codes.Unimplemented,
		codes.FailedPrecondition,
		codes.NotFound:
		return false
	default:
		return true
	}
}

// Next fetches the next task to run. The connector runner calls Next serially
// from its scheduler loop, so no synchronization is required here — Process is
// the side that runs concurrently and it shares no mutable state with Next.
func (c *c1ApiTaskManager) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	ctx, span := tracer.Start(ctx, "c1ApiTaskManager.Next", trace.WithNewRoot())
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	l := ctxzap.Extract(ctx)

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

	l.Debug("c1_api_task_manager.Next(): got task", zap.Duration("next_poll", nextPoll))
	return resp.GetTask(), nextPoll, nil
}

func (c *c1ApiTaskManager) finishTask(ctx context.Context, task *v1.Task, resp proto.Message, annos annotations.Annotations, taskError error) error {
	ctx, span := tracer.Start(ctx, "c1ApiTaskManager.finishTask")
	// NOTE: this error is for internal finish Task errors, not the task error itself!
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
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

	if taskError == nil {
		l.Info("c1_api_task_manager.finishTask(): finishing task successfully")
		_, err = c.serviceClient.FinishTask(finishCtx, v1.BatonServiceFinishTaskRequest_builder{
			TaskId: task.GetId(),
			Status: nil,
			Success: v1.BatonServiceFinishTaskRequest_Success_builder{
				Annotations: annos,
				Response:    marshalledResp,
			}.Build(),
		}.Build())
		if err != nil {
			l.Error("c1_api_task_manager.finishTask(): error while attempting to finish task successfully", zap.Error(err))
			return err
		}

		return nil
	}

	l.Error("c1_api_task_manager.finishTask(): finishing task with error", zap.Error(taskError))

	statusErr, ok := status.FromError(taskError)
	if !ok {
		switch {
		case errors.Is(taskError, context.Canceled):
			statusErr = status.New(codes.Canceled, taskError.Error())
		case errors.Is(taskError, context.DeadlineExceeded):
			statusErr = status.New(codes.DeadlineExceeded, taskError.Error())
		default:
			statusErr = status.New(codes.Unknown, taskError.Error())
		}
	}

	_, err = c.serviceClient.FinishTask(finishCtx, v1.BatonServiceFinishTaskRequest_builder{
		TaskId: task.GetId(),
		Status: &pbstatus.Status{
			//nolint:gosec // No risk of overflow because `Code` is a small enum.
			Code:    int32(statusErr.Code()),
			Message: statusErr.Message(),
		},
		Error: v1.BatonServiceFinishTaskRequest_Error_builder{
			NonRetryable: errors.Is(taskError, ErrTaskNonRetryable),
			Annotations:  annos,
		}.Build(),
	}.Build())
	if err != nil {
		l.Error("c1_api_task_manager.finishTask(): error finishing task", zap.Error(err))
		return errors.Join(taskError, err)
	}

	return nil
}

func (c *c1ApiTaskManager) GetTempDir() string {
	return c.tempDir
}

func (c *c1ApiTaskManager) ShouldDebug() bool {
	return c.runnerShouldDebug.Load()
}

func (c *c1ApiTaskManager) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "c1ApiTaskManager.Process", trace.WithNewRoot())
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
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
		handler = newFullSyncTaskHandler(
			task,
			tHelpers,
			c.skipFullSync,
			c.externalResourceC1Z,
			c.externalResourceEntitlementIdFilter,
			c.targetedSyncResources,
			c.syncResourceTypeIDs,
			c.workerCount,
		)
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
	case taskTypes.StartDebugging:
		handler = newStartDebugging(c)
	case taskTypes.BulkCreateTicketsType:
		handler = newBulkCreateTicketTaskHandler(task, tHelpers)
	case taskTypes.BulkGetTicketsType:
		handler = newBulkGetTicketTaskHandler(task, tHelpers)
	case taskTypes.ActionListSchemasType:
		handler = newActionListSchemasTaskHandler(task, tHelpers)
	case taskTypes.ActionGetSchemaType:
		handler = newActionGetSchemaTaskHandler(task, tHelpers)
	case taskTypes.ActionInvokeType:
		handler = newActionInvokeTaskHandler(task, tHelpers)
	case taskTypes.ActionStatusType:
		handler = newActionStatusTaskHandler(task, tHelpers)
	case taskTypes.ListEventFeedsType:
		handler = NewListEventFeedsHandler(task, tHelpers)
	case taskTypes.ListEventsType:
		handler = NewListEventsHandler(task, tHelpers)
	default:
		return c.finishTask(ctx, task, nil, nil, errors.New("unsupported task type"))
	}

	err = handler.HandleTask(ctx)
	if err != nil {
		l.Error("c1_api_task_manager.Process(): error while handling task", zap.Error(err))
		return err
	}

	return nil
}

// ensure *c1ApiTaskManager satisfies BootstrappingTaskManager.
var _ BootstrappingTaskManager = (*c1ApiTaskManager)(nil)

func NewC1TaskManager(
	ctx context.Context,
	clientID string,
	clientSecret string,
	tempDir string,
	skipFullSync bool,
	externalC1Z string,
	externalResourceEntitlementIdFilter string,
	targetedSyncResources []*v2.Resource,
	syncResourceTypeIDs []string,
	workerCount int,
) (BootstrappingTaskManager, error) {
	serviceClient, err := newServiceClient(ctx, clientID, clientSecret)
	if err != nil {
		return nil, err
	}

	return &c1ApiTaskManager{
		serviceClient:                       serviceClient,
		tempDir:                             tempDir,
		skipFullSync:                        skipFullSync,
		externalResourceC1Z:                 externalC1Z,
		externalResourceEntitlementIdFilter: externalResourceEntitlementIdFilter,
		targetedSyncResources:               targetedSyncResources,
		syncResourceTypeIDs:                 syncResourceTypeIDs,
		workerCount:                         workerCount,
	}, nil
}
