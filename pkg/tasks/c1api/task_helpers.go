package c1api

import (
	"context"
	"errors"
	"io"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/service_mode/v1"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type taskHelpers struct {
	task          *v1.Task
	serviceClient C1ServiceClient
	cc            types.ConnectorClient

	taskFinisher func(ctx context.Context, task *v1.Task, err error) error
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

func (t *taskHelpers) FinishTask(ctx context.Context, err error) error {
	if t.task == nil {
		return errors.New("cannot finish task: task is nil")
	}
	return t.taskFinisher(ctx, t.task, err)
}

func (t *taskHelpers) HelloClient() c1HelloClient {
	return t.serviceClient
}
