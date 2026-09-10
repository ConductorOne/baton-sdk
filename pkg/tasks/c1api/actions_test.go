package c1api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type actionInvokeClient struct {
	types.ConnectorClient
	request  *v2.InvokeActionRequest
	response *v2.InvokeActionResponse
}

func (c *actionInvokeClient) InvokeAction(
	_ context.Context,
	request *v2.InvokeActionRequest,
	_ ...grpc.CallOption,
) (*v2.InvokeActionResponse, error) {
	c.request = request
	return c.response, nil
}

type actionInvokeTestHelpers struct {
	client   types.ConnectorClient
	response proto.Message
}

func (h *actionInvokeTestHelpers) ConnectorClient() types.ConnectorClient {
	return h.client
}

func (h *actionInvokeTestHelpers) FinishTask(
	_ context.Context,
	response proto.Message,
	_ annotations.Annotations,
	err error,
) error {
	h.response = response
	return err
}

func TestActionInvokeTaskThreadsEncryptionConfigs(t *testing.T) {
	encryptionConfig := v2.EncryptionConfig_builder{Provider: "test-provider"}.Build()
	response := v2.InvokeActionResponse_builder{
		Status: v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE,
		EncryptedData: []*v2.EncryptedData{
			v2.EncryptedData_builder{Name: "token", EncryptedBytes: []byte("ciphertext")}.Build(),
		},
	}.Build()
	client := &actionInvokeClient{response: response}
	helpers := &actionInvokeTestHelpers{client: client}
	task := v1.Task_builder{
		ActionInvoke: v1.Task_ActionInvokeTask_builder{
			Name:              "issue-token",
			EncryptionConfigs: []*v2.EncryptionConfig{encryptionConfig},
		}.Build(),
	}.Build()

	require.NoError(t, newActionInvokeTaskHandler(task, helpers).HandleTask(t.Context()))
	require.Len(t, client.request.GetEncryptionConfigs(), 1)
	require.True(t, proto.Equal(encryptionConfig, client.request.GetEncryptionConfigs()[0]))
	require.Same(t, response, helpers.response)
}
