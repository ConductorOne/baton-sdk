package c1api

import (
	"context"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type fakeInvokeConnectorClient struct {
	types.ConnectorClient
	invokeDeadline time.Time
	hadDeadline    bool
	resp           *v2.InvokeActionResponse
}

func (f *fakeInvokeConnectorClient) InvokeAction(ctx context.Context, _ *v2.InvokeActionRequest, _ ...grpc.CallOption) (*v2.InvokeActionResponse, error) {
	f.invokeDeadline, f.hadDeadline = ctx.Deadline()
	return f.resp, nil
}

type fakeInvokeHelpers struct {
	cc         types.ConnectorClient
	finished   bool
	finishResp proto.Message
	finishErr  error
}

func (f *fakeInvokeHelpers) ConnectorClient() types.ConnectorClient { return f.cc }

func (f *fakeInvokeHelpers) FinishTask(_ context.Context, resp proto.Message, _ annotations.Annotations, err error) error {
	f.finished = true
	f.finishResp = resp
	f.finishErr = err
	return nil
}

func TestActionInvokeTaskHandlerPinsDeadlineAndAcceptsRunning(t *testing.T) {
	running := v2.InvokeActionResponse_builder{
		Id:     "action-1",
		Name:   "sleep",
		Status: v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING,
	}.Build()
	cc := &fakeInvokeConnectorClient{resp: running}
	helpers := &fakeInvokeHelpers{cc: cc}

	task := v1.Task_builder{
		Id:           "task-1",
		ActionInvoke: v1.Task_ActionInvokeTask_builder{Name: "sleep"}.Build(),
	}.Build()

	before := time.Now()
	err := newActionInvokeTaskHandler(task, helpers).HandleTask(t.Context())
	require.NoError(t, err)

	// The invoke context carries the pinned deadline even though the parent
	// had none: one second of inline wait plus the SDK's response margin.
	// The generous ceiling proves the pin is seconds-scale, not the runner
	// budget, without flaking under CI scheduling delays.
	require.True(t, cc.hadDeadline)
	pinned := cc.invokeDeadline.Sub(before)
	require.Greater(t, pinned, 1500*time.Millisecond)
	require.LessOrEqual(t, pinned, 10*time.Second)

	// A RUNNING response finishes the task successfully rather than failing it.
	require.True(t, helpers.finished)
	require.NoError(t, helpers.finishErr)
	require.Same(t, running, helpers.finishResp)
}
