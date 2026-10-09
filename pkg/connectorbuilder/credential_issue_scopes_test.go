package connectorbuilder

import (
	"context"
	"testing"
	"time"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func scopeIssueDetails(option v2.CapabilityDetailCredentialOption, scopes []string, custom bool, minimum uint64) *v2.CredentialDetailsCredentialIssue {
	return v2.CredentialDetailsCredentialIssue_builder{
		PreferredOption: option,
		Options: []*v2.CredentialIssueOptionDescriptor{v2.CredentialIssueOptionDescriptor_builder{
			Option: option, Scopes: scopes, CustomScopesAllowed: custom, InputFields: scopeIssueFields(scopes, custom, minimum),
			ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
			SecretResourceTypeId: "secret",
		}.Build()},
	}.Build()
}

func scopeIssueFields(scopes []string, custom bool, minimum uint64) []*config.Field {
	if minimum == 0 {
		return nil
	}
	rules := config.RepeatedStringRules_builder{
		MinItems: proto.Uint64(minimum), ValidateEmpty: true, Unique: true,
		ItemRules: config.StringRules_builder{ValidateEmpty: true, Pattern: proto.String(credentialIssueNonblankScopePattern)}.Build(),
	}.Build()
	if !custom {
		if len(scopes) == 0 {
			rules.SetMaxItems(0)
		} else {
			rules.GetItemRules().SetIn(scopes)
		}
	}
	return []*config.Field{config.Field_builder{Name: "scopes", StringSliceField: config.StringSliceField_builder{Rules: rules}.Build()}.Build()}
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
				minimum   uint64
				requested []string
				wantError string
			}{
				{name: "unsupported omitted"},
				{name: "unsupported supplied", requested: []string{"read"}, wantError: "at most 0 items"},
				{name: "optional omitted", allowed: []string{"read"}},
				{name: "optional allowed", allowed: []string{"read"}, requested: []string{"read"}},
				{name: "optional custom", custom: true, requested: []string{"custom"}},
				{name: "legacy unicode blank", custom: true, requested: []string{"\u00a0\u2003\u0085"}, wantError: "must match pattern"},
				{name: "legacy unicode with text", custom: true, requested: []string{"\u00a0read\u2003"}},
				{name: "required omitted", allowed: []string{"read", "write"}, minimum: 1, wantError: "at least 1 items"},
				{name: "required too few", allowed: []string{"read", "write"}, minimum: 2, requested: []string{"read"}, wantError: "at least 2 items"},
				{name: "required allowed", allowed: []string{"read", "write"}, minimum: 2, requested: []string{"read", "write"}},
				{name: "required unknown", allowed: []string{"read"}, minimum: 1, requested: []string{"other"}, wantError: "must be one of"},
				{name: "required custom", custom: true, minimum: 1, requested: []string{"other"}},
				{name: "required duplicate", allowed: []string{"read", "write"}, minimum: 2, requested: []string{"read", "read"}, wantError: "duplicate items"},
				{name: "required empty", custom: true, minimum: 1, requested: []string{" "}, wantError: "must match pattern"},
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
				minimum uint64
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
				{name: "maximum uint64", scopes: []string{"read"}, minimum: ^uint64(0)},
			} {
				t.Run(tc.name, func(t *testing.T) {
					err := validateCredentialIssueCapabilityDetails(scopeIssueDetails(option, tc.scopes, tc.custom, tc.minimum))
					if tc.valid {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, "minimum items exceeds")
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
				require.ErrorContains(t, err, "at least 2 items")
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
			decodedOption := decoded.GetMetadata().GetCapabilities().GetResourceTypeCapabilities()[0].GetCredentialIssue().GetOptions()[0]
			require.Equal(t, uint64(2), decodedOption.GetInputFields()[0].GetStringSliceField().GetRules().GetMinItems())
		})
	}
	t.Run("old protobuf omission", func(t *testing.T) {
		descriptor := &v2.CredentialIssueOptionDescriptor{}
		require.NoError(t, proto.Unmarshal([]byte{0x22, 0x04, 'r', 'e', 'a', 'd'}, descriptor))
		require.Zero(t, descriptor.GetInputFields())
	})
	t.Run("old JSON omission", func(t *testing.T) {
		descriptor := &v2.CredentialIssueOptionDescriptor{}
		require.NoError(t, protojson.Unmarshal([]byte(`{"scopes":["read"]}`), descriptor))
		require.Zero(t, descriptor.GetInputFields())
	})
}

func TestIssueCredentialMinimumScopesNilOptionMessage(t *testing.T) {
	ctx := context.Background()
	encryptionConfig := newIssueEncryptionConfig(t)
	for _, tc := range []struct {
		option  v2.CapabilityDetailCredentialOption
		options *v2.CredentialIssueOptions
	}{
		{
			option:  v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
			options: &v2.CredentialIssueOptions{SecretResourceTypeId: "secret", Options: &v2.CredentialIssueOptions_ApiKey_{}},
		},
		{
			option:  v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN,
			options: &v2.CredentialIssueOptions{SecretResourceTypeId: "secret", Options: &v2.CredentialIssueOptions_Token_{}},
		},
	} {
		t.Run(tc.option.String(), func(t *testing.T) {
			require.Equal(t, tc.option, credentialIssueOptionKind(tc.options))
			issuer := newTestCredentialIssuer("user")
			issuer.capabilityDetails = scopeIssueDetails(tc.option, []string{"read"}, false, 1)
			connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{issuer, newTestCredentialSecretDeleter()}))
			require.NoError(t, err)
			_, err = connector.IssueCredential(ctx, v2.IssueCredentialRequest_builder{
				IdentityId:        v2.ResourceId_builder{ResourceType: "user", Resource: "1"}.Build(),
				CredentialOptions: tc.options, RequestId: "request-nil", EncryptionConfigs: []*v2.EncryptionConfig{encryptionConfig},
			}.Build())
			require.Nil(t, issuer.lastInput)
			require.ErrorContains(t, err, "at least 1 items")
			require.Equal(t, codes.InvalidArgument, status.Code(err))

			issuer.capabilityDetails.GetOptions()[0].SetInputFields(nil)
			_, err = connector.IssueCredential(ctx, v2.IssueCredentialRequest_builder{
				IdentityId:        v2.ResourceId_builder{ResourceType: "user", Resource: "1"}.Build(),
				CredentialOptions: tc.options, RequestId: "request-optional", EncryptionConfigs: []*v2.EncryptionConfig{encryptionConfig},
			}.Build())
			require.NoError(t, err)
			require.NotNil(t, issuer.lastInput)
		})
	}
}

func TestCredentialIssueSharedScopeFields(t *testing.T) {
	for _, option := range []v2.CapabilityDetailCredentialOption{
		v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
		v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN,
	} {
		t.Run(option.String(), func(t *testing.T) {
			for _, tc := range []struct {
				name       string
				inputField *config.Field
				requested  []string
				wantError  string
			}{
				{name: "optional without rules", inputField: config.Field_builder{Name: "scopes", StringSliceField: &config.StringSliceField{}}.Build()},
				{name: "custom without rules", inputField: config.Field_builder{Name: "scopes", StringSliceField: &config.StringSliceField{}}.Build(), requested: []string{"custom"}},
				{name: "field required", inputField: config.Field_builder{Name: "scopes", IsRequired: true, StringSliceField: &config.StringSliceField{}}.Build(), wantError: "marked as required"},
				{name: "rule required", inputField: scopeRuleField(config.RepeatedStringRules_builder{IsRequired: true}.Build()), wantError: "marked as required"},
				{name: "min skips empty by shared convention", inputField: scopeRuleField(config.RepeatedStringRules_builder{MinItems: proto.Uint64(1)}.Build())},
			} {
				t.Run(tc.name, func(t *testing.T) {
					details := scopeIssueDetails(option, nil, false, 0)
					details.GetOptions()[0].SetInputFields([]*config.Field{tc.inputField})
					before := proto.Clone(details)
					require.NoError(t, validateCredentialIssueCapabilityDetails(details))
					_, err := validateCredentialIssueInput(&CredentialIssueInput{
						IdentityID:        v2.ResourceId_builder{ResourceType: "user", Resource: "1"}.Build(),
						CredentialOptions: scopeIssueOptions(option, tc.requested), RequestID: "request-fields",
					}, details, time.Now())
					if tc.wantError == "" {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, tc.wantError)
					}
					require.True(t, proto.Equal(before, details))
				})
			}
		})
	}
}

func TestCredentialIssueInvalidScopeFieldsBeforeProviderCreate(t *testing.T) {
	ctx := context.Background()
	encryptionConfig := newIssueEncryptionConfig(t)
	for _, tc := range []struct {
		name      string
		fields    []*config.Field
		wantError string
	}{
		{name: "nil field", fields: []*config.Field{nil}, wantError: "must be named scopes"},
		{name: "unknown field", fields: []*config.Field{config.Field_builder{Name: "unknown"}.Build()}, wantError: "must be named scopes"},
		{name: "wrong type", fields: []*config.Field{config.Field_builder{Name: "scopes", StringField: &config.StringField{}}.Build()}, wantError: "must be a string slice"},
		{name: "nil typed field", fields: []*config.Field{{Name: "scopes", Field: &config.Field_StringSliceField{}}}, wantError: "must be a string slice"},
		{name: "duplicate field", fields: append(scopeIssueFields(nil, true, 1), scopeIssueFields(nil, true, 1)...), wantError: "duplicate"},
		{name: "invalid pattern", fields: []*config.Field{scopeRuleField(config.RepeatedStringRules_builder{
			ItemRules: config.StringRules_builder{Pattern: proto.String("[")}.Build(),
		}.Build())}, wantError: "invalid item pattern"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			issuer := newTestCredentialIssuer("user")
			issuer.capabilityDetails = scopeIssueDetails(v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY, nil, true, 0)
			connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{issuer, newTestCredentialSecretDeleter()}))
			require.NoError(t, err)
			issuer.capabilityDetails.GetOptions()[0].SetInputFields(tc.fields)
			require.ErrorContains(t, validateCredentialIssueCapabilityDetails(issuer.capabilityDetails), tc.wantError)
			_, err = connector.IssueCredential(ctx, v2.IssueCredentialRequest_builder{
				IdentityId:        v2.ResourceId_builder{ResourceType: "user", Resource: "1"}.Build(),
				CredentialOptions: scopeIssueOptions(v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY, []string{"read"}),
				RequestId:         "request-invalid-schema", EncryptionConfigs: []*v2.EncryptionConfig{encryptionConfig},
			}.Build())
			require.ErrorContains(t, err, tc.wantError)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.Nil(t, issuer.lastInput)
		})
	}
}

func scopeRuleField(rules *config.RepeatedStringRules) *config.Field {
	return config.Field_builder{Name: "scopes", StringSliceField: config.StringSliceField_builder{Rules: rules}.Build()}.Build()
}
