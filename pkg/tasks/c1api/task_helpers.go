package c1api

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/tasks"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type taskHelpers struct {
	task          *v1.Task
	serviceClient BatonServiceClient
	cc            types.ConnectorClient
	tempDir       string

	taskFinisher func(ctx context.Context, task *v1.Task, resp proto.Message, annos annotations.Annotations, err error) error
}

func (t *taskHelpers) ConnectorClient() types.ConnectorClient {
	return t.cc
}

func (t *taskHelpers) Upload(ctx context.Context, r io.ReadSeeker) error {
	if t.task == nil {
		return errors.New("cannot upload: task is nil")
	}
	return t.serviceClient.Upload(ctx, t.task, r)
}

func (t *taskHelpers) FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error {
	if t.task == nil {
		return errors.New("cannot finish task: task is nil")
	}
	return t.taskFinisher(ctx, t.task, resp, annos, err)
}

func (t *taskHelpers) HelloClient() batonHelloClient {
	return t.serviceClient
}

func (t *taskHelpers) TempDir() string {
	return t.tempDir
}

// HeartbeatTask will call the heartbeat service endpoint for the task until the context is cancelled. An initial heartbeat is
// synchronously run, and if successful a goroutine is spawned that will heartbeat periodically respecting the returned heartbeat interval.
// If the given context is cancelled, the returned context will be cancelled with the same cause.
// If the heartbeat fails, this function will retry up to taskMaximumHeartbeatFailures times before cancelling the returned context with ErrTaskHeartbeatFailed.
// If the task is cancelled by the server, the returned context will be cancelled with ErrTaskCancelled.
func (t *taskHelpers) HeartbeatTask(ctx context.Context, annos annotations.Annotations) (context.Context, error) {
	l := ctxzap.Extract(ctx).With(zap.String("task_id", t.task.GetId()), zap.Stringer("task_type", tasks.GetType(t.task)))
	rCtx, rCancel := context.WithCancelCause(ctx)

	l.Debug("heartbeat: sending initial heartbeat")
	resp, err := t.serviceClient.Heartbeat(ctx, &v1.BatonServiceHeartbeatRequest{
		TaskId:      t.task.GetId(),
		Annotations: annos,
	})
	if err != nil {
		err = errors.Join(ErrTaskHeartbeatFailed, err)
		l.Error("heartbeat: failed sending initial heartbeat", zap.Error(err))
		rCancel(err)
		return nil, err
	}
	if resp.Cancelled {
		err = ErrTaskCancelled
		l.Debug("heartbeat: task was cancelled by server")
		rCancel(err)
		return nil, err
	}

	heartbeatInterval := getHeartbeatInterval(resp.GetNextHeartbeat().AsDuration())
	l.Debug("heartbeat: initial heartbeat success", zap.Duration("next_heartbeat", heartbeatInterval))

	go func() {
		attempts := 0
		for {
			attempts++
			l = l.With(zap.Int("attempts", attempts))

			if attempts >= taskMaximumHeartbeatFailures {
				l.Error("heartbeat: failed after 10 attempts")
				rCancel(ErrTaskHeartbeatFailed)
				return
			}

			l.Debug("heartbeat: waiting", zap.Duration("next_heartbeat", heartbeatInterval))
			select {
			case <-ctx.Done():
				l.Debug("heartbeat: context cancelled, stopping", zap.NamedError("cause", context.Cause(ctx)))
				rCancel(ctx.Err())
				return

			case <-time.After(heartbeatInterval):
				resp, err := t.serviceClient.Heartbeat(ctx, &v1.BatonServiceHeartbeatRequest{
					TaskId:      t.task.GetId(),
					Annotations: annos,
				})
				if err != nil {
					// If our parent context gets cancelled we can just leave.
					if ctxErr := ctx.Err(); ctxErr != nil {
						l.Debug("heartbeat: context cancelled while sending heartbeat", zap.Error(err), zap.NamedError("cause", context.Cause(ctx)))
						rCancel(ctx.Err())
						return
					}
					l.Error("heartbeat: failed sending heartbeat", zap.Error(err))

					// TODO(morgabra) Set the interval lower and then backoff on subsequent failures.
					continue
				}

				if resp == nil {
					l.Error("heartbeat: response was nil")
					heartbeatInterval = defaultHeartbeatInterval
					continue
				}

				heartbeatInterval = getHeartbeatInterval(resp.GetNextHeartbeat().AsDuration())
				l.Debug("heartbeat: success", zap.Duration("next_heartbeat", heartbeatInterval))
				if resp.Cancelled {
					l.Debug("heartbeat: task was cancelled by server")
					rCancel(ErrTaskCancelled)
					return
				}
				attempts = 0
			}
		}
	}()

	return rCtx, nil
}
