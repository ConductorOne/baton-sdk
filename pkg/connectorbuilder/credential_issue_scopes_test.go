package connectorbuilder

import (
	"context"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func scopeIssueDetails(option v2.CapabilityDetailCredentialOption, scopes []string, custom bool, minimum uint32) *v2.CredentialDetailsCredentialIssue {
	return v2.CredentialDetailsCredentialIssue_builder{
		PreferredOption: option,
		Options: []*v2.CredentialIssueOptionDescriptor{v2.CredentialIssueOptionDescriptor_builder{
			Option: option, Scopes: scopes, CustomScopesAllowed: custom, MinScopes: minimum,
			ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
			SecretResourceTypeId: "secret",
		}.Build()},
	}.Build()
}

func scopeIssueOptions(option v2.CapabilityDetailCredentialOption, scopes []string) *v2.CredentialIssueOptions {
	options := v2.CredentialIssueOptions_builder{SecretResourceTypeId: "secret"}.Build()
	switch option {
	case v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY:
		options.SetApiKey(v2.CredentialIssueOptions_ApiKey_builder{Scopes: scopes}.Build())
	case v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN:
		options.SetToken(v2.CredentialIssueOptions_Token_builder{Scopes: scopes}.Build())
	default:
		panic("unsupported test credential option")
	}
	return options
}

func TestCredentialIssueRequestedScopes(t *testing.T) {
	for _, option := range []v2.CapabilityDetailCredentialOption{
		v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
		v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN,
	} {
		t.Run(option.String(), func(t *testing.T) {
			for _, tc := range []struct {
				name      string
				allowed   []string
				custom    bool
				minimum   uint32
				requested []string
				wantError string
			}{
				{name: "unsupported omitted"},
				{name: "unsupported supplied", requested: []string{"read"}, wantError: "not advertised"},
				{name: "optional omitted", allowed: []string{"read"}},
				{name: "optional allowed", allowed: []string{"read"}, requested: []string{"read"}},
				{name: "optional custom", custom: true, requested: []string{"custom"}},
				{name: "required omitted", allowed: []string{"read", "write"}, minimum: 1, wantError: "at least 1 scopes"},
				{name: "required too few", allowed: []string{"read", "write"}, minimum: 2, requested: []string{"read"}, wantError: "at least 2 scopes"},
				{name: "required allowed", allowed: []string{"read", "write"}, minimum: 2, requested: []string{"read", "write"}},
				{name: "required unknown", allowed: []string{"read"}, minimum: 1, requested: []string{"other"}, wantError: "not advertised"},
				{name: "required custom", custom: true, minimum: 1, requested: []string{"other"}},
				{name: "required duplicate", allowed: []string{"read", "write"}, minimum: 2, requested: []string{"read", "read"}, wantError: "duplicate scope"},
				{name: "required empty", custom: true, minimum: 1, requested: []string{" "}, wantError: "scope must not be empty"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					details := scopeIssueDetails(option, tc.allowed, tc.custom, tc.minimum)
					require.NoError(t, validateCredentialIssueCapabilityDetails(details))
					input := &CredentialIssueInput{
						IdentityID: v2.ResourceId_builder{ResourceType: "user", Resource: "1"}.Build(),
						RequestID:  "request-1", CredentialOptions: scopeIssueOptions(option, tc.requested),
					}
					descriptor, err := validateCredentialIssueInput(input, details, time.Now())
					if tc.wantError != "" {
						require.ErrorContains(t, err, tc.wantError)
						require.Nil(t, descriptor)
					} else {
						require.NoError(t, err)
						require.Same(t, details.GetOptions()[0], descriptor)
					}
				})
			}
		})
	}
}

func TestCredentialIssueMinimumScopeCapability(t *testing.T) {
	for _, option := range []v2.CapabilityDetailCredentialOption{
		v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
		v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN,
	} {
		t.Run(option.String(), func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				scopes  []string
				custom  bool
				minimum uint32
				valid   bool
			}{
				{name: "legacy unsupported", valid: true},
				{name: "unsupported minimum", minimum: 1},
				{name: "exceeds allowed", scopes: []string{"read"}, minimum: 2},
				{name: "duplicate allowed", scopes: []string{"read", "read"}, minimum: 2},
				{name: "blank allowed", scopes: []string{"read", " "}, minimum: 2},
				{name: "exact allowed", scopes: []string{"read", "write"}, minimum: 2, valid: true},
				{name: "custom only", custom: true, minimum: 2, valid: true},
				{name: "custom extends allowed", scopes: []string{"read"}, custom: true, minimum: 2, valid: true},
				{name: "maximum uint32", scopes: []string{"read"}, minimum: ^uint32(0)},
			} {
				t.Run(tc.name, func(t *testing.T) {
					err := validateCredentialIssueCapabilityDetails(scopeIssueDetails(option, tc.scopes, tc.custom, tc.minimum))
					if tc.valid {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, "minimum scopes exceeds")
						require.Equal(t, codes.InvalidArgument, status.Code(err))
					}
				})
			}
		})
	}
	t.Run("client secret cannot require scopes", func(t *testing.T) {
		details := scopeIssueDetails(v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_CLIENT_SECRET, nil, false, 1)
		require.ErrorContains(t, validateCredentialIssueCapabilityDetails(details), "may only advertise expiry")
	})
	t.Run("keypair cannot require scopes", func(t *testing.T) {
		details := scopeIssueDetails(v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_KEYPAIR, nil, false, 1)
		details.GetOptions()[0].SetKeyProfiles([]*v2.KeyGenerationProfile{v2.KeyGenerationProfile_builder{Kty: "EC", Crv: proto.String("P-256")}.Build()})
		require.ErrorContains(t, validateCredentialIssueCapabilityDetails(details), "may only advertise key profiles and expiry")
	})
}

func TestIssueCredentialMinimumScopesBeforeProviderCreate(t *testing.T) {
	ctx := context.Background()
	encryptionConfig := newIssueEncryptionConfig(t)
	for _, option := range []v2.CapabilityDetailCredentialOption{
		v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
		v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN,
	} {
		t.Run(option.String(), func(t *testing.T) {
			issuer := newTestCredentialIssuer("user")
			issuer.capabilityDetails = scopeIssueDetails(option, []string{"read", "write"}, false, 2)
			connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{issuer, newTestCredentialSecretDeleter()}))
			require.NoError(t, err)
			for _, requested := range [][]string{nil, {"read"}} {
				_, err = connector.IssueCredential(ctx, v2.IssueCredentialRequest_builder{
					IdentityId:        v2.ResourceId_builder{ResourceType: "user", Resource: "1"}.Build(),
					CredentialOptions: scopeIssueOptions(option, requested),
					RequestId:         "request-1", EncryptionConfigs: []*v2.EncryptionConfig{encryptionConfig},
				}.Build())
				require.ErrorContains(t, err, "at least 2 scopes")
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.Nil(t, issuer.lastInput)
			}
			requested := []string{"write", "read"}
			_, err = connector.IssueCredential(ctx, v2.IssueCredentialRequest_builder{
				IdentityId:        v2.ResourceId_builder{ResourceType: "user", Resource: "1"}.Build(),
				CredentialOptions: scopeIssueOptions(option, requested),
				RequestId:         "request-valid", EncryptionConfigs: []*v2.EncryptionConfig{encryptionConfig},
			}.Build())
			require.NoError(t, err)
			require.NotNil(t, issuer.lastInput)
			require.True(t, proto.Equal(scopeIssueOptions(option, requested), issuer.lastInput.CredentialOptions))
		})
	}
}

func TestCredentialIssueMinimumScopesMetadataRoundtrip(t *testing.T) {
	details := scopeIssueDetails(v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN, []string{"read"}, true, 2)
	metadata := v2.ConnectorServiceGetMetadataResponse_builder{Metadata: v2.ConnectorMetadata_builder{
		Capabilities: v2.ConnectorCapabilities_builder{
			ResourceTypeCapabilities: []*v2.ResourceTypeCapability{v2.ResourceTypeCapability_builder{
				ResourceType: v2.ResourceType_builder{Id: "user"}.Build(), CredentialIssue: details,
			}.Build()},
		}.Build(),
	}.Build()}.Build()
	for _, tc := range []struct {
		name      string
		marshal   func(proto.Message) ([]byte, error)
		unmarshal func([]byte, proto.Message) error
	}{
		{"protobuf", proto.Marshal, proto.Unmarshal},
		{"json", protojson.Marshal, protojson.Unmarshal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.marshal(metadata)
			require.NoError(t, err)
			decoded := &v2.ConnectorServiceGetMetadataResponse{}
			require.NoError(t, tc.unmarshal(data, decoded))
			require.True(t, proto.Equal(metadata, decoded))
			require.Equal(t, uint32(2), decoded.GetMetadata().GetCapabilities().GetResourceTypeCapabilities()[0].GetCredentialIssue().GetOptions()[0].GetMinScopes())
		})
	}
	t.Run("old protobuf omission", func(t *testing.T) {
		descriptor := &v2.CredentialIssueOptionDescriptor{}
		require.NoError(t, proto.Unmarshal([]byte{0x22, 0x04, 'r', 'e', 'a', 'd'}, descriptor))
		require.Zero(t, descriptor.GetMinScopes())
	})
	t.Run("old JSON omission", func(t *testing.T) {
		descriptor := &v2.CredentialIssueOptionDescriptor{}
		require.NoError(t, protojson.Unmarshal([]byte(`{"scopes":["read"]}`), descriptor))
		require.Zero(t, descriptor.GetMinScopes())
	})
}
