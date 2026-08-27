package connectorbuilder

import (
	"context"
	"fmt"
	"strings"
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

func apiKeyDescriptorWithScopes(secretResourceTypeID string, scopes ...string) *v2.CredentialIssueOptionDescriptor {
	descriptor := apiKeyDescriptor(secretResourceTypeID)
	descriptor.SetScopes(scopes)
	return descriptor
}

// divergentAPIKeyDetails advertises the same shape twice with different scopes,
// so which descriptor the request selects decides which scopes it may ask for.
func divergentAPIKeyDetails() *v2.CredentialDetailsCredentialIssue {
	return v2.CredentialDetailsCredentialIssue_builder{
		Options: []*v2.CredentialIssueOptionDescriptor{
			apiKeyDescriptorWithScopes(orgAPIKeyType, "read"),
			apiKeyDescriptorWithScopes(serviceAccountKey, "write"),
		},
		PreferredOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
	}.Build()
}

func apiKeyOptionsWithScopes(secretResourceTypeID string, scopes ...string) *v2.CredentialIssueOptions {
	return v2.CredentialIssueOptions_builder{
		SecretResourceTypeId: secretResourceTypeID,
		ApiKey:               v2.CredentialIssueOptions_ApiKey_builder{Scopes: scopes}.Build(),
	}.Build()
}

func apiKeyOptions(secretResourceTypeID string) *v2.CredentialIssueOptions {
	return v2.CredentialIssueOptions_builder{
		SecretResourceTypeId: secretResourceTypeID,
		ApiKey:               &v2.CredentialIssueOptions_ApiKey{},
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
	secretType := input.CredentialOptions.GetSecretResourceTypeId()
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

	t.Run("an over-long output type is rejected at registration, not at issue time", func(t *testing.T) {
		tooLong := strings.Repeat("a", maxCredentialIssueSecretResourceTypeIDBytes+1)
		err := validateCredentialIssueCapabilityDetails(apiKeyDetails(tooLong))
		require.ErrorContains(t, err, "must be at most 1024 bytes")

		require.NoError(t, validateCredentialIssueCapabilityDetails(
			apiKeyDetails(strings.Repeat("a", maxCredentialIssueSecretResourceTypeIDBytes))))
	})

	t.Run("preferred option matches a shape, not a single descriptor", func(t *testing.T) {
		details := apiKeyDetails(orgAPIKeyType, serviceAccountKey)
		require.NoError(t, validateCredentialIssueCapabilityDetails(details))

		details.SetPreferredOption(v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN)
		require.ErrorContains(t, validateCredentialIssueCapabilityDetails(details), "not part of the supported options")
	})
}

func TestResolveCredentialIssueDescriptor(t *testing.T) {
	t.Run("the shape and output type together select one descriptor", func(t *testing.T) {
		details := apiKeyDetails(orgAPIKeyType, serviceAccountKey)
		descriptor, err := resolveCredentialIssueDescriptor(details, apiKeyOptions(serviceAccountKey))
		require.NoError(t, err)
		require.Equal(t, serviceAccountKey, descriptor.GetSecretResourceTypeId())
	})

	t.Run("a missing output type is rejected rather than guessed", func(t *testing.T) {
		_, err := resolveCredentialIssueDescriptor(apiKeyDetails(orgAPIKeyType), apiKeyOptions(""))
		require.ErrorContains(t, err, "credential_options.secret_resource_type_id is required")
	})

	t.Run("an undeclared output type names what the shape does produce", func(t *testing.T) {
		_, err := resolveCredentialIssueDescriptor(apiKeyDetails(orgAPIKeyType, serviceAccountKey), apiKeyOptions("made-up-type"))
		require.ErrorContains(t, err, `does not produce secret resource type "made-up-type"`)
		require.ErrorContains(t, err, orgAPIKeyType)
		require.ErrorContains(t, err, serviceAccountKey)
		require.NotContains(t, err.Error(), "is not advertised by connector")
	})

	t.Run("an undeclared shape is rejected", func(t *testing.T) {
		details := v2.CredentialDetailsCredentialIssue_builder{
			Options: []*v2.CredentialIssueOptionDescriptor{
				v2.CredentialIssueOptionDescriptor_builder{
					Option:               v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN,
					ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_VIRTUAL,
					SecretResourceTypeId: orgAPIKeyType,
				}.Build(),
			},
			PreferredOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN,
		}.Build()
		_, err := resolveCredentialIssueDescriptor(details, apiKeyOptions(orgAPIKeyType))
		require.ErrorContains(t, err, "credential option CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY is not advertised by connector")
		require.NotContains(t, err.Error(), orgAPIKeyType, "an unadvertised shape must not read as an unadvertised output type")
	})

	t.Run("options with no arm set are rejected", func(t *testing.T) {
		_, err := resolveCredentialIssueDescriptor(apiKeyDetails(orgAPIKeyType), nil)
		require.ErrorContains(t, err, "unsupported credential option")
	})
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
			IdentityId:        v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build(),
			CredentialOptions: apiKeyOptions(secretResourceTypeID),
			EncryptionConfigs: []*v2.EncryptionConfig{newIssueEncryptionConfig(t)},
			RequestId:         "request-1",
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
		require.Equal(t, serviceAccountKey, issuer.lastInput.CredentialOptions.GetSecretResourceTypeId())
		require.Equal(t, serviceAccountKey, resp.GetSecret().GetId().GetResourceType())
	})

	t.Run("an absent output type is rejected rather than resolved arbitrarily", func(t *testing.T) {
		connector, issuer := newConnector(t)
		_, err := connector.IssueCredential(ctx, request(""))
		require.ErrorContains(t, err, "credential_options.secret_resource_type_id is required")
		require.Nil(t, issuer.lastInput, "the provider must not be mutated when the request is ambiguous")
	})

	t.Run("an undeclared output type is rejected", func(t *testing.T) {
		connector, issuer := newConnector(t)
		_, err := connector.IssueCredential(ctx, request("made-up-type"))
		require.ErrorContains(t, err, `does not produce secret resource type "made-up-type"`)
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

// The selected descriptor must gate the request, not just name it: two
// same-shape descriptors that differ in what they advertise must accept and
// reject different requests.
func TestSelectedDescriptorGatesTheRequest(t *testing.T) {
	details := divergentAPIKeyDetails()
	now := time.Now()
	input := func(secretResourceTypeID string, scopes ...string) *CredentialIssueInput {
		return &CredentialIssueInput{
			IdentityID:        v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build(),
			CredentialOptions: apiKeyOptionsWithScopes(secretResourceTypeID, scopes...),
			RequestID:         "request-1",
		}
	}

	for _, tc := range []struct {
		name                 string
		secretResourceTypeID string
		scope                string
		wantErr              bool
	}{
		{name: "the read scope is accepted by the descriptor advertising it", secretResourceTypeID: orgAPIKeyType, scope: "read"},
		{name: "the read scope is rejected by the descriptor advertising write", secretResourceTypeID: serviceAccountKey, scope: "read", wantErr: true},
		{name: "the write scope is accepted by the descriptor advertising it", secretResourceTypeID: serviceAccountKey, scope: "write"},
		{name: "the write scope is rejected by the descriptor advertising read", secretResourceTypeID: orgAPIKeyType, scope: "write", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			descriptor, err := validateCredentialIssueInput(input(tc.secretResourceTypeID, tc.scope), details, now)
			if tc.wantErr {
				require.ErrorContains(t, err, fmt.Sprintf("scope %q is not advertised by connector", tc.scope))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.secretResourceTypeID, descriptor.GetSecretResourceTypeId())
		})
	}
}

func TestIssueCredentialAppliesTheSelectedDescriptorsConstraints(t *testing.T) {
	ctx := context.Background()
	issuer := newMultiTypeIssuer(divergentAPIKeyDetails())
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
		issuer,
		newNamedSecretDeleter(orgAPIKeyType),
		newNamedSecretDeleter(serviceAccountKey),
	}))
	require.NoError(t, err)

	request := func(secretResourceTypeID string, scopes ...string) *v2.IssueCredentialRequest {
		return v2.IssueCredentialRequest_builder{
			IdentityId:        v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build(),
			CredentialOptions: apiKeyOptionsWithScopes(secretResourceTypeID, scopes...),
			EncryptionConfigs: []*v2.EncryptionConfig{newIssueEncryptionConfig(t)},
			RequestId:         "request-1",
		}.Build()
	}

	_, err = connector.IssueCredential(ctx, request(serviceAccountKey, "read"))
	require.ErrorContains(t, err, `scope "read" is not advertised by connector`)
	require.Nil(t, issuer.lastInput, "the provider must not be mutated when the selected descriptor rejects the request")

	resp, err := connector.IssueCredential(ctx, request(serviceAccountKey, "write"))
	require.NoError(t, err)
	require.Equal(t, serviceAccountKey, resp.GetSecret().GetId().GetResourceType())
}
