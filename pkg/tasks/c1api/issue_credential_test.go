package c1api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	tasktypes "github.com/conductorone/baton-sdk/pkg/types/tasks"
)

type issueCredentialClient struct {
	types.ConnectorClient
	request  *v2.IssueCredentialRequest
	response *v2.IssueCredentialResponse
	err      error
}

func (c *issueCredentialClient) IssueCredential(_ context.Context, request *v2.IssueCredentialRequest, _ ...grpc.CallOption) (*v2.IssueCredentialResponse, error) {
	c.request = request
	return c.response, c.err
}

type issueCredentialTestHelpers struct {
	client   types.ConnectorClient
	response proto.Message
	err      error
}

func (h *issueCredentialTestHelpers) ConnectorClient() types.ConnectorClient { return h.client }
func (h *issueCredentialTestHelpers) FinishTask(_ context.Context, response proto.Message, _ annotations.Annotations, err error) error {
	h.response = response
	h.err = err
	return err
}

func issueCredentialTask() *v1.Task {
	return v1.Task_builder{
		Id: "task-123",
		IssueCredential: v1.Task_IssueCredentialTask_builder{
			IdentityId: v2.ResourceId_builder{ResourceType: "service_account", Resource: "sa-1"}.Build(),
			CredentialOptions: v2.CredentialIssueOptions_builder{
				ApiKey: &v2.CredentialIssueOptions_ApiKey{},
			}.Build(),
			EncryptionConfigs: []*v2.EncryptionConfig{{}},
		}.Build(),
	}.Build()
}

func TestIssueCredentialTaskHandler(t *testing.T) {
	for _, requestID := range []string{"", "request-456"} {
		for _, expiresAt := range []*timestamppb.Timestamp{nil, timestamppb.New(time.Date(2030, time.January, 2, 3, 4, 5, 0, time.UTC))} {
			t.Run("forwards request id and expiry/"+requestID+"/"+expiresAt.String(), func(t *testing.T) {
				task := issueCredentialTask()
				task.GetIssueCredential().SetRequestId(requestID)
				task.GetIssueCredential().SetExpiresAt(expiresAt)
				wire, err := proto.Marshal(task)
				require.NoError(t, err)
				decoded := &v1.Task{}
				require.NoError(t, proto.Unmarshal(wire, decoded))
				expectedID := requestID
				if expectedID == "" {
					expectedID = task.GetId()
				}
				response := v2.IssueCredentialResponse_builder{RequestId: expectedID}.Build()
				client := &issueCredentialClient{response: response}
				helpers := &issueCredentialTestHelpers{client: client}

				require.NoError(t, newIssueCredentialTaskHandler(decoded, helpers).HandleTask(context.Background()))
				require.Equal(t, expectedID, client.request.GetRequestId())
				require.True(t, proto.Equal(expiresAt, client.request.GetExpiresAt()))
				require.Same(t, response, helpers.response)
			})
		}
	}

	t.Run("dispatches stable task id and returns response", func(t *testing.T) {
		response := v2.IssueCredentialResponse_builder{RequestId: "task-123"}.Build()
		client := &issueCredentialClient{response: response}
		helpers := &issueCredentialTestHelpers{client: client}
		task := issueCredentialTask()

		require.True(t, tasks.Is(task, tasktypes.IssueCredentialType))
		require.Equal(t, tasktypes.IssueCredentialType, tasks.GetType(task))
		require.NoError(t, newIssueCredentialTaskHandler(task, helpers).HandleTask(context.Background()))
		require.Equal(t, "task-123", client.request.GetRequestId())
		require.Same(t, response, helpers.response)
	})

	t.Run("rejects malformed task before connector call", func(t *testing.T) {
		client := &issueCredentialClient{}
		helpers := &issueCredentialTestHelpers{client: client}
		task := v1.Task_builder{Id: "task-123", IssueCredential: &v1.Task_IssueCredentialTask{}}.Build()

		err := newIssueCredentialTaskHandler(task, helpers).HandleTask(context.Background())
		require.Error(t, err)
		require.ErrorIs(t, err, ErrTaskNonRetryable)
		require.Nil(t, client.request)
	})

	t.Run("does not retry ambiguous connector error", func(t *testing.T) {
		client := &issueCredentialClient{err: errors.New("transport lost")}
		helpers := &issueCredentialTestHelpers{client: client}

		err := newIssueCredentialTaskHandler(issueCredentialTask(), helpers).HandleTask(context.Background())
		require.Error(t, err)
		require.ErrorIs(t, err, ErrTaskNonRetryable)
		require.NotNil(t, client.request)
	})
}
