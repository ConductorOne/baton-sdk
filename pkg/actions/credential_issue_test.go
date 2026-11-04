package actions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/types/known/structpb"
)

func apiKeyOutputDescriptor() *v2.CredentialIssueOptionDescriptor {
	return v2.CredentialIssueOptionDescriptor_builder{
		Option:               v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
		ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
		SecretResourceTypeId: "organization-api-key",
		Output: config.Field_builder{
			Name:        "api_key",
			ContentType: "application/vnd.multipass.api_key_v2+json",
			Tags:        []string{"credential", "api_key", "multipass", "api_key_v2"},
			IsSecret:    true,
			StringField: config.StringField_builder{}.Build(),
		}.Build(),
	}.Build()
}

func TestCredentialIssueSchemaDescribesTheIssuance(t *testing.T) {
	descriptor := apiKeyOutputDescriptor()
	schema := CredentialIssueSchema("issue_api_key", descriptor)

	require.Equal(t, "issue_api_key", schema.GetName())
	require.Equal(t, []v2.ActionType{v2.ActionType_ACTION_TYPE_CREDENTIAL_ISSUE}, schema.GetActionType())
	require.True(t, proto.Equal(descriptor, schema.GetCredentialIssue()), "the descriptor travels unmodified")
	require.Equal(t, "organization-api-key", schema.GetResourceTypeId(), "resource_type_id is the secret resource type")

	arguments := schema.GetArguments()
	require.Len(t, arguments, 3)
	for _, arg := range arguments {
		require.NotNil(t, arg.GetStringField(), "argument %q is a string", arg.GetName())
		require.False(t, arg.GetIsSecret(), "argument %q is not a secret", arg.GetName())
		require.NoError(t, arg.Validate(), "argument %q", arg.GetName())
	}
	require.Equal(t, []string{ArgIdentityID, ArgExpiresAt, ArgRequestID}, []string{
		arguments[0].GetName(), arguments[1].GetName(), arguments[2].GetName(),
	})
	require.True(t, arguments[0].GetIsRequired(), "identity_id is required")
	require.True(t, arguments[2].GetIsRequired(), "request_id is required")
	require.False(t, arguments[1].GetIsRequired(), "expiry is optional")

	require.Len(t, schema.GetReturnTypes(), 1)
	returnField := schema.GetReturnTypes()[0]
	require.Equal(t, "api_key", returnField.GetName(), "the return field takes the output field's name")
	require.True(t, returnField.GetIsSecret())
	require.True(t, returnField.GetIsRequired())
	require.NotNil(t, returnField.GetStringField())
	require.Equal(t, "application/vnd.multipass.api_key_v2+json", returnField.GetContentType())
	require.Equal(t, []string{"credential", "api_key", "multipass", "api_key_v2"}, returnField.GetTags())
	require.NoError(t, returnField.Validate())
}

func TestCredentialIssueSchemaNamesTheReturnWhenTheOutputHasNoName(t *testing.T) {
	descriptor := apiKeyOutputDescriptor()
	descriptor.GetOutput().SetName("")

	schema := CredentialIssueSchema("issue_api_key", descriptor)
	require.Len(t, schema.GetReturnTypes(), 1)
	require.Equal(t, defaultCredentialReturnName, schema.GetReturnTypes()[0].GetName())
}

func TestCredentialIssueSchemaRegistersAsAResourceAction(t *testing.T) {
	ctx := context.Background()
	manager := NewActionManager(ctx)
	schema := CredentialIssueSchema("issue_api_key", apiKeyOutputDescriptor())
	handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
		response, err := structpb.NewStruct(map[string]any{"success": true})
		if err != nil {
			return nil, nil, nil, err
		}
		return response, []*v2.PlaintextData{
			v2.PlaintextData_builder{
				Name:   "api_key",
				Schema: "application/vnd.multipass.api_key_v2+json",
				Bytes:  []byte(`{"key_value":"k"}`),
			}.Build(),
		}, nil, nil
	}

	require.NoError(t, manager.RegisterResourceActionWithSecrets(ctx, schema.GetResourceTypeId(), schema, handler))

	registered := manager.resourceSchemas[schema.GetResourceTypeId()]["issue_api_key"]
	require.NotNil(t, registered, "the schema registers under its resource type and name")
	require.Equal(t, schema.GetResourceTypeId(), registered.GetResourceTypeId())
	require.Equal(t, []v2.ActionType{v2.ActionType_ACTION_TYPE_CREDENTIAL_ISSUE}, registered.GetActionType())
	require.True(t, proto.Equal(schema.GetCredentialIssue(), registered.GetCredentialIssue()))
}
