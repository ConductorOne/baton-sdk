package c1api

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// TestTaskFailureWrappingPreservesStatusDetails proves the minted-handle detail
// survives the path a real failure takes: the builder's status error is joined
// with the task's non-retryable marker before FinishTask sees it. If joining
// dropped the details, C1 would have to parse a message to find the object it
// must clean up -- or lose it.
func TestTaskFailureWrappingPreservesStatusDetails(t *testing.T) {
	minted := v2.ResourceId_builder{ResourceType: "service-account-key", Resource: "key-1"}.Build()
	st := status.New(codes.Internal, "credential was minted but cannot be delivered")
	st, err := st.WithDetails(minted)
	require.NoError(t, err)

	// The exact wrapping the handlers use.
	wrapped := errors.Join(st.Err(), ErrTaskNonRetryable)

	require.ErrorIs(t, wrapped, ErrTaskNonRetryable, "the non-retryable marker must survive")
	back, ok := status.FromError(wrapped)
	require.True(t, ok, "the status must be recoverable from the joined error")
	require.Equal(t, codes.Internal, back.Code())

	var got *v2.ResourceId
	for _, detail := range back.Details() {
		if id, isID := detail.(*v2.ResourceId); isID {
			got = id
		}
	}
	require.NotNil(t, got, "the minted identity must survive task failure wrapping")
	require.Equal(t, "key-1", got.GetResource())
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
