package c1api

import (
	"context"
	"errors"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/service_mode/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type helloHelpers interface {
	ConnectorClient() types.ConnectorClient
	HelloClient() c1HelloClient
}

type helloTaskHandler struct {
	task          *v1.Task
	includeTaskID bool
	helpers       helloHelpers
}

func (c *helloTaskHandler) HandleTask(ctx context.Context) error {
	if c.task == nil {
		return errors.New("cannot handle task: task is nil")
	}

	l := ctxzap.Extract(ctx).With(
		zap.String("task_id", c.task.GetId()),
		zap.Stringer("task_type", tasks.GetType(c.task)),
	)

	cc := c.helpers.ConnectorClient()
	mdResp, err := cc.GetMetadata(ctx, &v2.ConnectorServiceGetMetadataRequest{})
	if err != nil {
		return err
	}

	// The API changes behavior based on whether the task ID is included in the request or not
	taskID := c.task.GetId()
	if !c.includeTaskID {
		taskID = ""
	}

	_, err = c.helpers.HelloClient().Hello(ctx, &v1.HelloRequest{
		TaskId:            taskID,
		ConnectorMetadata: mdResp.GetMetadata(),
	})
	if err != nil {
		l.Error("failed while sending hello", zap.Error(err))
		return err
	}

	return nil
}

func newHelloTaskHandler(task *v1.Task, includeTaskID bool, helpers helloHelpers) *helloTaskHandler {
	return &helloTaskHandler{
		task:          task,
		helpers:       helpers,
		includeTaskID: includeTaskID,
	}
}
