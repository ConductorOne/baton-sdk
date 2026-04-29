package c1api

import (
	"context"
	"errors"
	"io"
	"testing"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/stretchr/testify/require"
)

func TestFinishTaskUsesInputError(t *testing.T) {
	client := &recordingServiceClient{}
	manager := &c1ApiTaskManager{serviceClient: client}
	task := v1.Task_builder{Id: "task-id", Hello: &v1.Task_HelloTask{}}.Build()
	inputErr := errors.New("task failed")

	err := manager.finishTask(context.Background(), task, nil, nil, inputErr)
	require.ErrorIs(t, err, inputErr)
	require.NotNil(t, client.finishReq)
	require.Nil(t, client.finishReq.GetSuccess())
	require.NotNil(t, client.finishReq.GetError())
	require.NotNil(t, client.finishReq.GetStatus())
	require.Equal(t, "task failed", client.finishReq.GetStatus().GetMessage())
}

type recordingServiceClient struct {
	finishReq *v1.BatonServiceFinishTaskRequest
}

func (r *recordingServiceClient) Hello(context.Context, *v1.BatonServiceHelloRequest) (*v1.BatonServiceHelloResponse, error) {
	return v1.BatonServiceHelloResponse_builder{}.Build(), nil
}

func (r *recordingServiceClient) GetTask(context.Context, *v1.BatonServiceGetTaskRequest) (*v1.BatonServiceGetTaskResponse, error) {
	return v1.BatonServiceGetTaskResponse_builder{}.Build(), nil
}

func (r *recordingServiceClient) Heartbeat(context.Context, *v1.BatonServiceHeartbeatRequest) (*v1.BatonServiceHeartbeatResponse, error) {
	return v1.BatonServiceHeartbeatResponse_builder{}.Build(), nil
}

func (r *recordingServiceClient) FinishTask(_ context.Context, req *v1.BatonServiceFinishTaskRequest) (*v1.BatonServiceFinishTaskResponse, error) {
	r.finishReq = req
	return v1.BatonServiceFinishTaskResponse_builder{}.Build(), nil
}

func (r *recordingServiceClient) Upload(context.Context, *v1.Task, io.ReadSeeker) error {
	return nil
}
