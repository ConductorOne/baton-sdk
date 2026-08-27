package connectorbuilder

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	resource "github.com/conductorone/baton-sdk/pkg/types/resource"
)

const (
	orgAPIKeyType     = "organization-api-key"
	serviceAccountKey = "service-account-application-key"
)

func apiKeyDescriptor(secretResourceTypeID string) *v2.CredentialIssueOptionDescriptor {
	return v2.CredentialIssueOptionDescriptor_builder{
		Option:               v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
		ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
		SecretResourceTypeId: secretResourceTypeID,
	}.Build()
}

func apiKeyDetails(secretResourceTypeIDs ...string) *v2.CredentialDetailsCredentialIssue {
	options := make([]*v2.CredentialIssueOptionDescriptor, 0, len(secretResourceTypeIDs))
	for _, id := range secretResourceTypeIDs {
		options = append(options, apiKeyDescriptor(id))
	}
	return v2.CredentialDetailsCredentialIssue_builder{
		Options:         options,
		PreferredOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
	}.Build()
}

// multiTypeIssuer advertises two API-key-shaped options that differ only in
// the resource type they mint, and mints whichever one the request selected.
type multiTypeIssuer struct {
	ResourceSyncer
	details   *v2.CredentialDetailsCredentialIssue
	lastInput *CredentialIssueInput
}

func newMultiTypeIssuer(details *v2.CredentialDetailsCredentialIssue) *multiTypeIssuer {
	return &multiTypeIssuer{ResourceSyncer: newTestResourceSyncer("user"), details: details}
}

func (m *multiTypeIssuer) IssueCapabilityDetails(context.Context) (*v2.CredentialDetailsCredentialIssue, annotations.Annotations, error) {
	return m.details, annotations.Annotations{}, nil
}

func (m *multiTypeIssuer) Issue(_ context.Context, input *CredentialIssueInput) (*CredentialIssueOutput, error) {
	m.lastInput = input
	secretType := input.SecretResourceTypeID
	if secretType == "" {
		secretType = m.details.GetOptions()[0].GetSecretResourceTypeId()
	}
	secret, err := resource.NewSecretResource(
		"Issued key",
		v2.ResourceType_builder{Id: secretType}.Build(),
		"issued-key-1",
		[]resource.SecretTraitOption{resource.WithSecretIdentityID(input.IdentityID)},
	)
	if err != nil {
		return nil, err
	}
	return &CredentialIssueOutput{
		Secret:        secret,
		PlaintextData: []*v2.PlaintextData{v2.PlaintextData_builder{Name: "api_key", Bytes: []byte("material")}.Build()},
		ResourceMode:  v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
	}, nil
}

type namedSecretDeleter struct{ ResourceSyncer }

func newNamedSecretDeleter(resourceType string) *namedSecretDeleter {
	return &namedSecretDeleter{ResourceSyncer: newTestResourceSyncer(resourceType)}
}

func (d *namedSecretDeleter) Delete(context.Context, *v2.ResourceId) (annotations.Annotations, error) {
	return nil, nil
}

func TestValidateCredentialIssueCapabilityDetailsDedupesOnShapeAndOutputType(t *testing.T) {
	t.Run("same shape with distinct output types both register", func(t *testing.T) {
		require.NoError(t, validateCredentialIssueCapabilityDetails(apiKeyDetails(orgAPIKeyType, serviceAccountKey)))
	})

	t.Run("same shape with the same output type is still a duplicate", func(t *testing.T) {
		err := validateCredentialIssueCapabilityDetails(apiKeyDetails(orgAPIKeyType, orgAPIKeyType))
		require.ErrorContains(t, err, "duplicate credential issue option")
		require.ErrorContains(t, err, orgAPIKeyType)
	})

	t.Run("preferred option matches a shape, not a single descriptor", func(t *testing.T) {
		details := apiKeyDetails(orgAPIKeyType, serviceAccountKey)
		require.NoError(t, validateCredentialIssueCapabilityDetails(details))

		details.SetPreferredOption(v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN)
		require.ErrorContains(t, validateCredentialIssueCapabilityDetails(details), "not part of the supported options")
	})
}

func TestResolveCredentialIssueDescriptor(t *testing.T) {
	apiKey := v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY
	token := v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN

	t.Run("absent output type resolves the sole descriptor for the shape", func(t *testing.T) {
		descriptor, err := resolveCredentialIssueDescriptor(apiKeyDetails(orgAPIKeyType), apiKey, "")
		require.NoError(t, err)
		require.Equal(t, orgAPIKeyType, descriptor.GetSecretResourceTypeId())
	})

	t.Run("output type selects among descriptors sharing a shape", func(t *testing.T) {
		details := apiKeyDetails(orgAPIKeyType, serviceAccountKey)
		descriptor, err := resolveCredentialIssueDescriptor(details, apiKey, serviceAccountKey)
		require.NoError(t, err)
		require.Equal(t, serviceAccountKey, descriptor.GetSecretResourceTypeId())
	})

	t.Run("absent output type is ambiguous across descriptors sharing a shape", func(t *testing.T) {
		_, err := resolveCredentialIssueDescriptor(apiKeyDetails(orgAPIKeyType, serviceAccountKey), apiKey, "")
		require.ErrorContains(t, err, "secret_resource_type_id is required to select one")
		require.ErrorContains(t, err, orgAPIKeyType)
		require.ErrorContains(t, err, serviceAccountKey)
	})

	t.Run("undeclared output type is rejected", func(t *testing.T) {
		_, err := resolveCredentialIssueDescriptor(apiKeyDetails(orgAPIKeyType), apiKey, "made-up-type")
		require.ErrorContains(t, err, `"made-up-type" is not advertised by connector`)
	})

	t.Run("undeclared shape is rejected as before", func(t *testing.T) {
		_, err := resolveCredentialIssueDescriptor(apiKeyDetails(orgAPIKeyType), token, "")
		require.ErrorContains(t, err, "is not advertised by connector")
	})
}

// A connector declaring one descriptor per shape and a caller sending no
// output type is every deployment predating this field. Resolution must be
// exactly what it was.
func TestValidateCredentialIssueInputWithoutOutputTypeIsUnchanged(t *testing.T) {
	input := &CredentialIssueInput{
		IdentityID:        v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build(),
		CredentialOptions: v2.CredentialIssueOptions_builder{ApiKey: &v2.CredentialIssueOptions_ApiKey{}}.Build(),
		RequestID:         "request-1",
	}
	descriptor, err := validateCredentialIssueInput(input, apiKeyDetails(orgAPIKeyType), time.Now())
	require.NoError(t, err)
	require.Equal(t, orgAPIKeyType, descriptor.GetSecretResourceTypeId())
}

func TestIssueCredentialSelectsDescriptorByOutputType(t *testing.T) {
	ctx := context.Background()
	newConnector := func(t *testing.T) (*builder, *multiTypeIssuer) {
		t.Helper()
		issuer := newMultiTypeIssuer(apiKeyDetails(orgAPIKeyType, serviceAccountKey))
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			issuer,
			newNamedSecretDeleter(orgAPIKeyType),
			newNamedSecretDeleter(serviceAccountKey),
		}))
		require.NoError(t, err)
		return connector.(*builder), issuer
	}

	request := func(secretResourceTypeID string) *v2.IssueCredentialRequest {
		return v2.IssueCredentialRequest_builder{
			IdentityId:           v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build(),
			CredentialOptions:    v2.CredentialIssueOptions_builder{ApiKey: &v2.CredentialIssueOptions_ApiKey{}}.Build(),
			EncryptionConfigs:    []*v2.EncryptionConfig{newIssueEncryptionConfig(t)},
			RequestId:            "request-1",
			SecretResourceTypeId: secretResourceTypeID,
		}.Build()
	}

	t.Run("both same-shape options are advertised", func(t *testing.T) {
		connector, _ := newConnector(t)
		caps, err := connector.GetCapabilities(ctx)
		require.NoError(t, err)
		var advertised []string
		for _, capability := range caps.GetResourceTypeCapabilities() {
			for _, descriptor := range capability.GetCredentialIssue().GetOptions() {
				advertised = append(advertised, descriptor.GetSecretResourceTypeId())
			}
		}
		require.ElementsMatch(t, []string{orgAPIKeyType, serviceAccountKey}, advertised)
	})

	t.Run("the request's output type reaches the connector and gates the output", func(t *testing.T) {
		connector, issuer := newConnector(t)
		resp, err := connector.IssueCredential(ctx, request(serviceAccountKey))
		require.NoError(t, err)
		require.Equal(t, serviceAccountKey, issuer.lastInput.SecretResourceTypeID)
		require.Equal(t, serviceAccountKey, resp.GetSecret().GetId().GetResourceType())
	})

	t.Run("an absent output type is rejected rather than resolved arbitrarily", func(t *testing.T) {
		connector, issuer := newConnector(t)
		_, err := connector.IssueCredential(ctx, request(""))
		require.ErrorContains(t, err, "secret_resource_type_id is required to select one")
		require.Nil(t, issuer.lastInput, "the provider must not be mutated when the request is ambiguous")
	})

	t.Run("an undeclared output type is rejected", func(t *testing.T) {
		connector, issuer := newConnector(t)
		_, err := connector.IssueCredential(ctx, request("made-up-type"))
		require.ErrorContains(t, err, "is not advertised by connector")
		require.Nil(t, issuer.lastInput)
	})
}

// Every declared output type needs its own deleter, not just the first.
func TestIssueCredentialCapabilityRequiresDeleterForEveryOutputType(t *testing.T) {
	ctx := context.Background()
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
		newMultiTypeIssuer(apiKeyDetails(orgAPIKeyType, serviceAccountKey)),
		newNamedSecretDeleter(orgAPIKeyType),
	}))
	require.NoError(t, err)

	_, err = connector.(*builder).GetCapabilities(ctx)
	require.ErrorContains(t, err, "without ResourceDeleterV2")
	require.ErrorContains(t, err, serviceAccountKey)
}
