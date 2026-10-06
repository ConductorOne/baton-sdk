package c1api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	tasktypes "github.com/conductorone/baton-sdk/pkg/types/tasks"
)

// typedIssueCredentialClient records which issuance entrypoint the handler
// chose, so a test can assert the legacy one was never reached.
type typedIssueCredentialClient struct {
	issueCredentialClient
	typedRequest *v2.IssueCredentialRequest
}

func (c *typedIssueCredentialClient) IssueCredentialV2(_ context.Context, request *v2.IssueCredentialRequest, _ ...grpc.CallOption) (*v2.IssueCredentialResponse, error) {
	c.typedRequest = request
	return c.response, c.err
}

func typedIssueCredentialTask(outputContentType string) *v1.Task {
	return v1.Task_builder{
		Id: "task-typed-123",
		IssueTypedCredential: v1.Task_IssueTypedCredentialTask_builder{
			IdentityId: v2.ResourceId_builder{ResourceType: "service_account", Resource: "sa-1"}.Build(),
			CredentialOptions: v2.CredentialIssueOptions_builder{
				ApiKey: &v2.CredentialIssueOptions_ApiKey{},
			}.Build(),
			EncryptionConfigs: []*v2.EncryptionConfig{{}},
			OutputContentType: outputContentType,
		}.Build(),
	}.Build()
}

// TestIssueTypedCredentialTaskHandler pins the task-backed half of the fence.
func TestIssueTypedCredentialTaskHandler(t *testing.T) {
	t.Run("dispatches the typed method and never the legacy one", func(t *testing.T) {
		response := v2.IssueCredentialResponse_builder{RequestId: "task-typed-123"}.Build()
		client := &typedIssueCredentialClient{issueCredentialClient: issueCredentialClient{response: response}}
		helpers := &issueCredentialTestHelpers{client: client}
		task := typedIssueCredentialTask("api_key_v2")

		require.Equal(t, tasktypes.IssueTypedCredentialType, tasks.GetType(task))
		require.NotEqual(t, tasktypes.IssueCredentialType, tasks.GetType(task),
			"the typed arm must not resolve to the legacy task type")
		require.NoError(t, newIssueTypedCredentialTaskHandler(task, helpers).HandleTask(context.Background()))

		require.NotNil(t, client.typedRequest)
		require.Equal(t, "api_key_v2", client.typedRequest.GetOutputContentType())
		require.Equal(t, "task-typed-123", client.typedRequest.GetRequestId())
		require.Nil(t, client.request, "the legacy entrypoint must not be reached")
	})

	t.Run("a typed task missing its type is refused before any call", func(t *testing.T) {
		client := &typedIssueCredentialClient{}
		helpers := &issueCredentialTestHelpers{client: client}

		err := newIssueTypedCredentialTaskHandler(typedIssueCredentialTask(""), helpers).HandleTask(context.Background())
		require.ErrorIs(t, err, ErrTaskNonRetryable)
		require.Nil(t, client.typedRequest)
		require.Nil(t, client.request)
	})

	// The other direction: the legacy handler must not be able to service a
	// typed task even if it is handed one. An executor that predates the typed
	// arm routes it nowhere, and the legacy handler reads a nil IssueCredential
	// and refuses. Neither path mints.
	t.Run("the legacy handler refuses a typed task and calls nothing", func(t *testing.T) {
		client := &typedIssueCredentialClient{}
		helpers := &issueCredentialTestHelpers{client: client}

		err := newIssueCredentialTaskHandler(typedIssueCredentialTask("api_key_v2"), helpers).HandleTask(context.Background())
		require.ErrorIs(t, err, ErrTaskNonRetryable)
		require.Nil(t, client.request)
		require.Nil(t, client.typedRequest)
	})
}
