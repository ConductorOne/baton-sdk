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
	require.Len(t, arguments, 3, "a descriptor with no scopes, audiences or key profiles declares only the base arguments")
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

	returnField := secretReturnField(schema)
	require.Equal(t, "api_key", returnField.GetName(), "the secret return field takes the output field's name")
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
	secret := secretReturnField(schema)
	require.Equal(t, defaultCredentialReturnName, secret.GetName())
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

// secretReturnField picks the one secret return field, so a test names the
// field it means rather than its position in the list.
func secretReturnField(schema *v2.BatonActionSchema) *config.Field {
	for _, field := range schema.GetReturnTypes() {
		if field.GetIsSecret() {
			return field
		}
	}
	return nil
}

// returnFieldByName looks up a declared return field, so the assertions fail
// with the field's absence rather than on a list index.
func returnFieldByName(schema *v2.BatonActionSchema, name string) *config.Field {
	for _, field := range schema.GetReturnTypes() {
		if field.GetName() == name {
			return field
		}
	}
	return nil
}

func TestCredentialIssueSchemaDeclaresTheStandardReturnFields(t *testing.T) {
	schema := CredentialIssueSchema("issue_api_key", apiKeyOutputDescriptor())

	for _, name := range []string{
		ReturnSecretResourceTypeID,
		ReturnSecretResourceID,
		ReturnIdentityID,
		ReturnExpiresAt,
		ReturnResourceMode,
		ReturnRequestID,
	} {
		field := returnFieldByName(schema, name)
		require.NotNil(t, field, "return field %q", name)
		require.False(t, field.GetIsSecret(), "return field %q is public", name)
	}
	require.NotNil(t, returnFieldByName(schema, ReturnIdentityID).GetResourceIdField(),
		"the echoed identity is a ResourceId, not a string")
	require.True(t, returnFieldByName(schema, ReturnExpiresAt).GetIsRequired() == false,
		"expiry is optional")

	// The plaintext stays the only secret return field.
	secretCount := 0
	for _, field := range schema.GetReturnTypes() {
		if field.GetIsSecret() {
			secretCount++
		}
	}
	require.Equal(t, 1, secretCount, "exactly one secret return field")
}

func TestCredentialIssueSchemaAddsOnlyTheAdvertisedOptions(t *testing.T) {
	base := CredentialIssueSchema("issue_api_key", apiKeyOutputDescriptor()).GetArguments()

	withScopes := apiKeyOutputDescriptor()
	withScopes.SetScopes([]string{"read", "write"})
	scoped := argumentsByName(CredentialIssueSchema("issue_api_key", withScopes))
	require.NotNil(t, scoped[ArgScopes], "scopes are declared when the descriptor advertises them")
	require.NotNil(t, scoped[ArgScopes].GetStringSliceField())
	require.Nil(t, scoped[ArgAudiences], "audiences stay undeclared")
	require.Nil(t, scoped[ArgProfile], "a key profile stays undeclared")

	withAudiences := apiKeyOutputDescriptor()
	withAudiences.SetAudiences([]string{"api.example.test"})
	audienced := argumentsByName(CredentialIssueSchema("issue_api_key", withAudiences))
	require.NotNil(t, audienced[ArgAudiences])
	require.Nil(t, audienced[ArgScopes])

	withProfile := apiKeyOutputDescriptor()
	withProfile.SetKeyProfiles([]*v2.KeyGenerationProfile{
		v2.KeyGenerationProfile_builder{Kty: "RSA"}.Build(),
	})
	profiled := argumentsByName(CredentialIssueSchema("issue_api_key", withProfile))
	require.NotNil(t, profiled[ArgProfile])
	require.Nil(t, profiled[ArgScopes])

	require.Len(t, base, 3, "the base descriptor declares no optional arguments")
}

func argumentsByName(schema *v2.BatonActionSchema) map[string]*config.Field {
	byName := make(map[string]*config.Field, len(schema.GetArguments()))
	for _, argument := range schema.GetArguments() {
		byName[argument.GetName()] = argument
	}
	return byName
}

func TestCredentialIssueResourceRebuildsTheSecret(t *testing.T) {
	response, err := structpb.NewStruct(map[string]any{
		ReturnSecretResourceTypeID: "organization-api-key",
		ReturnSecretResourceID:     "key-123",
		ReturnIdentityID: map[string]any{
			"resource_type_id": "user",
			"resource_id":      "u-1",
		},
		ReturnExpiresAt:    "2027-01-01T00:00:00Z",
		ReturnResourceMode: "CREDENTIAL_RESOURCE_MODE_DISCOVERABLE",
		ReturnRequestID:    "req-1",
	})
	require.NoError(t, err)

	secret, mode, err := CredentialIssueResource(response)
	require.NoError(t, err)
	require.Equal(t, "organization-api-key", secret.GetId().GetResourceType())
	require.Equal(t, "key-123", secret.GetId().GetResource())
	require.Equal(t, v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE, mode)

	trait := &v2.SecretTrait{}
	annos := annotations.Annotations(secret.GetAnnotations())
	found, err := annos.Pick(trait)
	require.NoError(t, err)
	require.True(t, found, "the rebuilt resource carries a SecretTrait")
	require.Equal(t, "u-1", trait.GetIdentityId().GetResource())
	require.Equal(t, "user", trait.GetIdentityId().GetResourceType())
	require.NotNil(t, trait.GetExpiresAt(), "an expiry in the response reaches the trait")
}

func TestCredentialIssueResourceOmitsAnAbsentExpiry(t *testing.T) {
	response, err := structpb.NewStruct(map[string]any{
		ReturnSecretResourceTypeID: "organization-api-key",
		ReturnSecretResourceID:     "key-123",
		ReturnIdentityID: map[string]any{
			"resource_type_id": "user",
			"resource_id":      "u-1",
		},
		ReturnResourceMode: "CREDENTIAL_RESOURCE_MODE_DISCOVERABLE",
		ReturnRequestID:    "req-1",
	})
	require.NoError(t, err)

	secret, _, err := CredentialIssueResource(response)
	require.NoError(t, err)
	trait := &v2.SecretTrait{}
	annos := annotations.Annotations(secret.GetAnnotations())
	found, err := annos.Pick(trait)
	require.NoError(t, err)
	require.True(t, found)
	require.Nil(t, trait.GetExpiresAt(), "no expiry was declared, so none is invented")
}

func TestCredentialIssueResourceRejectsIncompleteResponses(t *testing.T) {
	full := func() map[string]any {
		return map[string]any{
			ReturnSecretResourceTypeID: "organization-api-key",
			ReturnSecretResourceID:     "key-123",
			ReturnIdentityID:           map[string]any{"resource_type_id": "user", "resource_id": "u-1"},
			ReturnResourceMode:         "CREDENTIAL_RESOURCE_MODE_DISCOVERABLE",
			ReturnRequestID:            "req-1",
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
		want   string
	}{
		{"missing resource type", func(m map[string]any) { delete(m, ReturnSecretResourceTypeID) }, ReturnSecretResourceTypeID},
		{"missing resource id", func(m map[string]any) { delete(m, ReturnSecretResourceID) }, ReturnSecretResourceID},
		{"missing identity", func(m map[string]any) { delete(m, ReturnIdentityID) }, ReturnIdentityID},
		{"identity without id", func(m map[string]any) {
			m[ReturnIdentityID] = map[string]any{"resource_type_id": "user"}
		}, ReturnIdentityID},
		{"missing mode", func(m map[string]any) { delete(m, ReturnResourceMode) }, ReturnResourceMode},
		{"unknown mode", func(m map[string]any) { m[ReturnResourceMode] = "NOT_A_MODE" }, "NOT_A_MODE"},
		{"bad expiry", func(m map[string]any) { m[ReturnExpiresAt] = "yesterday" }, ReturnExpiresAt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := full()
			tc.mutate(payload)
			response, err := structpb.NewStruct(payload)
			require.NoError(t, err)
			_, _, err = CredentialIssueResource(response)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.want)
		})
	}
}
