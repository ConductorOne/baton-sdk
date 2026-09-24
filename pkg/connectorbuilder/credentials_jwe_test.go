package connectorbuilder

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"filippo.io/hpke"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	ageprovider "github.com/conductorone/baton-sdk/pkg/crypto/providers/age"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwe"
	resource "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// jweIssuedSecretMarker stands in for connector-minted credential bytes. No
// error returned by the builder may carry it.
const jweIssuedSecretMarker = "JWE-ISSUED-SECRET-DO-NOT-ECHO"

const testJWERecipientKeyID = "xwing-recipient-1"

// publicJWKJSON renders the public half of an X-Wing key as the AKP JWK the
// profile requires. Only `kty`, `alg` and `pub` are emitted; `kid`, `use` and
// `key_ops` are optional and are added by tests that exercise them.
func publicJWKJSON(t *testing.T, publicKey []byte, extra map[string]any) []byte {
	t.Helper()
	members := map[string]any{
		"kty": "AKP",
		"alg": jwe.Algorithm,
		"pub": base64.RawURLEncoding.EncodeToString(publicKey),
	}
	for name, value := range extra {
		members[name] = value
	}
	encoded, err := json.Marshal(members)
	require.NoError(t, err)
	return encoded
}

func jweConfigForPublicKey(publicJWK []byte, mutate ...func(*v2.EncryptionConfig_JWKPublicKeyConfig)) *v2.EncryptionConfig {
	jwkConfig := v2.EncryptionConfig_JWKPublicKeyConfig_builder{PubKey: publicJWK}.Build()
	for _, apply := range mutate {
		apply(jwkConfig)
	}
	return v2.EncryptionConfig_builder{
		Provider:           jwe.EncryptionProvider,
		KeyId:              testJWERecipientKeyID,
		JwkPublicKeyConfig: jwkConfig,
	}.Build()
}

// validJWEConfig returns a JWE recipient with a freshly generated X-Wing key.
func validJWEConfig(t *testing.T) *v2.EncryptionConfig {
	t.Helper()
	privateKey, err := hpke.MLKEM768X25519().GenerateKey()
	require.NoError(t, err)
	return jweConfigForPublicKey(publicJWKJSON(t, privateKey.PublicKey().Bytes(), nil))
}

// scriptedCredentialIssuer counts Issue calls and returns a fixed plaintext
// count, so a test can prove the builder never reached the connector.
type scriptedCredentialIssuer struct {
	ResourceSyncer
	calls         int
	plaintexts    []*v2.PlaintextData
	output        *CredentialIssueOutput
	capabilityErr error
	issueErr      error
}

func newScriptedCredentialIssuer(resourceType string) *scriptedCredentialIssuer {
	return &scriptedCredentialIssuer{ResourceSyncer: newTestResourceSyncer(resourceType)}
}

func (s *scriptedCredentialIssuer) Issue(ctx context.Context, input *CredentialIssueInput) (*CredentialIssueOutput, error) {
	s.calls++
	if s.issueErr != nil {
		return nil, s.issueErr
	}
	if s.output != nil {
		return s.output, nil
	}
	secret, err := resource.NewSecretResource(
		"Issued key for "+input.IdentityID.GetResource(),
		v2.ResourceType_builder{Id: "secret"}.Build(),
		"issued-key-1",
		[]resource.SecretTraitOption{resource.WithSecretIdentityID(input.IdentityID)},
	)
	if err != nil {
		return nil, err
	}
	return &CredentialIssueOutput{
		Secret:        secret,
		PlaintextData: s.plaintexts,
		ResourceMode:  v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
	}, nil
}

func (s *scriptedCredentialIssuer) IssueCapabilityDetails(context.Context) (*v2.CredentialDetailsCredentialIssue, annotations.Annotations, error) {
	if s.capabilityErr != nil {
		return nil, nil, s.capabilityErr
	}
	return v2.CredentialDetailsCredentialIssue_builder{
		Options: []*v2.CredentialIssueOptionDescriptor{
			v2.CredentialIssueOptionDescriptor_builder{
				Option:               v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
				ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
				SecretResourceTypeId: "secret",
			}.Build(),
		},
		PreferredOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
	}.Build(), annotations.Annotations{}, nil
}

func issueRequest(configs ...*v2.EncryptionConfig) *v2.IssueCredentialRequest {
	return v2.IssueCredentialRequest_builder{
		IdentityId:        v2.ResourceId_builder{ResourceType: "service_account", Resource: "sa-1"}.Build(),
		CredentialOptions: v2.CredentialIssueOptions_builder{SecretResourceTypeId: "secret", ApiKey: &v2.CredentialIssueOptions_ApiKey{}}.Build(),
		EncryptionConfigs: configs,
		RequestId:         "request-jwe",
	}.Build()
}

func newJWETestConnector(t *testing.T, issuer *scriptedCredentialIssuer) ConnectorBuilder {
	t.Helper()
	return newTestConnector([]ResourceSyncer{issuer, newTestCredentialSecretDeleter()})
}

func TestIssueCredentialWithOneJWERecipientSucceeds(t *testing.T) {
	ctx := context.Background()
	issuer := newScriptedCredentialIssuer("service_account")
	issuer.plaintexts = []*v2.PlaintextData{
		v2.PlaintextData_builder{Name: "api_key", Bytes: []byte(jweIssuedSecretMarker)}.Build(),
	}
	connector, err := NewConnector(ctx, newJWETestConnector(t, issuer))
	require.NoError(t, err)

	response, err := connector.IssueCredential(ctx, issueRequest(validJWEConfig(t)))
	require.NoError(t, err)
	require.Equal(t, 1, issuer.calls)

	require.Len(t, response.GetEncryptedData(), 1)
	encrypted := response.GetEncryptedData()[0]
	require.Equal(t, jwe.EncryptionProvider, encrypted.GetProvider())
	require.Equal(t, []string{testJWERecipientKeyID}, encrypted.GetKeyIds())
	require.Empty(t, encrypted.GetKeyId(), "deprecated key_id must stay empty")
	require.NotEmpty(t, encrypted.GetEncryptedBytes())
	require.NotContains(t, string(encrypted.GetEncryptedBytes()), jweIssuedSecretMarker)
	require.Equal(t, "api_key", encrypted.GetName())
	require.Equal(t, "request-jwe", response.GetRequestId())
	require.NotNil(t, response.GetSecret())
}

func TestIssueCredentialJWEOutputCardinality(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name       string
		plaintexts []*v2.PlaintextData
	}{
		{"zero plaintext values", nil},
		{
			"two plaintext values",
			[]*v2.PlaintextData{
				v2.PlaintextData_builder{Name: "api_key", Bytes: []byte(jweIssuedSecretMarker)}.Build(),
				v2.PlaintextData_builder{Name: "api_key_id", Bytes: []byte("key-id-123")}.Build(),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issuer := newScriptedCredentialIssuer("service_account")
			issuer.plaintexts = tc.plaintexts
			connector, err := NewConnector(ctx, newJWETestConnector(t, issuer))
			require.NoError(t, err)

			response, err := connector.IssueCredential(ctx, issueRequest(validJWEConfig(t)))
			require.Error(t, err)
			require.NotContains(t, err.Error(), jweIssuedSecretMarker,
				"a cardinality failure must not leak plaintext into the error")
			require.Nil(t, response, "no encrypted response may be returned when cardinality is wrong")
		})
	}
}

func TestIssueCredentialJWEOutputCardinalityRequiresSingleRecipient(t *testing.T) {
	ctx := context.Background()
	issuer := newScriptedCredentialIssuer("service_account")
	issuer.plaintexts = []*v2.PlaintextData{
		v2.PlaintextData_builder{Name: "api_key", Bytes: []byte(jweIssuedSecretMarker)}.Build(),
	}
	connector, err := NewConnector(ctx, newJWETestConnector(t, issuer))
	require.NoError(t, err)

	_, err = connector.IssueCredential(ctx, issueRequest(validJWEConfig(t), validJWEConfig(t)))
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "want InvalidArgument, got %v", err)
	require.Zero(t, issuer.calls, "config fan-out must fail before the connector mints a credential")
	require.NotContains(t, err.Error(), jweIssuedSecretMarker)
}

func TestIssueCredentialJWENilOutputFailsWithoutLeak(t *testing.T) {
	ctx := context.Background()
	issuer := newScriptedCredentialIssuer("service_account")
	issuer.output = nil
	issuer.plaintexts = nil
	connector, err := NewConnector(ctx, newJWETestConnector(t, issuer))
	require.NoError(t, err)

	response, err := connector.IssueCredential(ctx, issueRequest(validJWEConfig(t)))
	require.Error(t, err)
	require.Nil(t, response)
	require.NotContains(t, err.Error(), jweIssuedSecretMarker)
}

// TestIssueCredentialRejectsConfigFanOutBeforeIssuerCall pins that any
// combination of a JWE recipient with a second recipient is refused before the
// connector is asked to mint, in either order.
func TestIssueCredentialRejectsConfigFanOutBeforeIssuerCall(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name    string
		configs func(t *testing.T) []*v2.EncryptionConfig
	}{
		{"two JWE recipients", func(t *testing.T) []*v2.EncryptionConfig {
			return []*v2.EncryptionConfig{validJWEConfig(t), validJWEConfig(t)}
		}},
		{"JWE then legacy JWK", func(t *testing.T) []*v2.EncryptionConfig {
			return []*v2.EncryptionConfig{validJWEConfig(t), newIssueEncryptionConfig(t)}
		}},
		{"legacy JWK then JWE", func(t *testing.T) []*v2.EncryptionConfig {
			return []*v2.EncryptionConfig{newIssueEncryptionConfig(t), validJWEConfig(t)}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issuer := newScriptedCredentialIssuer("service_account")
			issuer.plaintexts = []*v2.PlaintextData{
				v2.PlaintextData_builder{Name: "api_key", Bytes: []byte(jweIssuedSecretMarker)}.Build(),
			}
			connector, err := NewConnector(ctx, newJWETestConnector(t, issuer))
			require.NoError(t, err)

			response, err := connector.IssueCredential(ctx, issueRequest(tc.configs(t)...))
			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "want InvalidArgument, got %v", err)
			require.Zero(t, issuer.calls, "fan-out must fail before the connector mints a credential")
			require.Nil(t, response)
			require.NotContains(t, err.Error(), jweIssuedSecretMarker)
		})
	}
}

func TestIssueCredentialRejectsInvalidEncryptionConfigBeforeIssuerCall(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name   string
		config *v2.EncryptionConfig
	}{
		{"malformed JWE recipient", jweConfigForPublicKey([]byte(`{"kty":"AKP"}`))},
		{"nil recipient", nil},
		{"client-side emitted private-key marker", jweConfigForPublicKey([]byte(jweIssuedSecretMarker))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issuer := newScriptedCredentialIssuer("service_account")
			issuer.plaintexts = []*v2.PlaintextData{
				v2.PlaintextData_builder{Name: "api_key", Bytes: []byte(jweIssuedSecretMarker)}.Build(),
			}
			connector, err := NewConnector(ctx, newJWETestConnector(t, issuer))
			require.NoError(t, err)

			response, err := connector.IssueCredential(ctx, issueRequest(tc.config))
			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "want InvalidArgument, got %v", err)
			require.Zero(t, issuer.calls, "an invalid encryption config must fail before the connector mints a credential")
			require.Nil(t, response)
			require.NotContains(t, err.Error(), jweIssuedSecretMarker)
		})
	}
}

// TestIssueCredentialRejectsOversizedProtectedHeaderBeforeIssuerCall covers the
// protected-header limit end to end. A key id inside the key-id limit can still
// serialize past the decoded-header cap once JSON escaping is applied; that must
// fail before the connector mints anything.
func TestIssueCredentialRejectsOversizedProtectedHeaderBeforeIssuerCall(t *testing.T) {
	ctx := context.Background()
	issuer := newScriptedCredentialIssuer("service_account")
	issuer.plaintexts = []*v2.PlaintextData{
		v2.PlaintextData_builder{Name: "api_key", Bytes: []byte(jweIssuedSecretMarker)}.Build(),
	}
	connector, err := NewConnector(ctx, newJWETestConnector(t, issuer))
	require.NoError(t, err)

	config := validJWEConfig(t)
	config.SetKeyId(strings.Repeat("<", 1024))

	response, err := connector.IssueCredential(ctx, issueRequest(config))
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "want InvalidArgument, got %v", err)
	require.Zero(t, issuer.calls, "an oversized protected header must fail before minting")
	require.Nil(t, response)
	require.NotContains(t, err.Error(), "<")
	require.NotContains(t, err.Error(), jweIssuedSecretMarker)
}

// TestIssueCredentialRejectsUnsupportedAuthenticatedData covers the shared
// resolver's rejection of AAD for providers that cannot authenticate it. The
// legacy JWK recipient and an explicit age recipient configured with a JWK
// section both name a provider that is not JWE.
func TestIssueCredentialRejectsUnsupportedAuthenticatedData(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name   string
		config *v2.EncryptionConfig
	}{
		{"legacy JWK recipient", jwkConfigWithAAD(t)},
		{"explicit age recipient with a JWK section", v2.EncryptionConfig_builder{
			Provider: ageprovider.EncryptionProviderAge,
			KeyId:    "age-recipient-1",
			JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
				PubKey:                       newIssueEncryptionConfig(t).GetJwkPublicKeyConfig().GetPubKey(),
				AdditionalAuthenticatedData: []byte("bound-context"),
			}.Build(),
		}.Build()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issuer := newScriptedCredentialIssuer("service_account")
			issuer.plaintexts = []*v2.PlaintextData{
				v2.PlaintextData_builder{Name: "api_key", Bytes: []byte(jweIssuedSecretMarker)}.Build(),
			}
			connector, err := NewConnector(ctx, newJWETestConnector(t, issuer))
			require.NoError(t, err)

			response, err := connector.IssueCredential(ctx, issueRequest(tc.config))
			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "want InvalidArgument, got %v", err)
			require.Zero(t, issuer.calls, "an unsupported AAD request must fail before minting")
			require.Nil(t, response)
		})
	}
}

// TestJWECapabilityAdvertisedOnlyForCredentialIssuers checks both dimensions:
// the capability lands on the issuing resource type and, because connector
// capabilities are the union of resource-type capabilities, on the connector;
// a resource type that cannot issue must not carry it.
func TestJWECapabilityAdvertisedOnlyForCredentialIssuers(t *testing.T) {
	ctx := context.Background()

	t.Run("issuer resource type and connector", func(t *testing.T) {
		connector, err := NewConnector(ctx, newJWETestConnector(t, newScriptedCredentialIssuer("service_account")))
		require.NoError(t, err)

		caps, err := connector.(*builder).GetCapabilities(ctx)
		require.NoError(t, err)
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_CREDENTIAL_ENCRYPTION_JWE)

		byType := make(map[string][]v2.Capability)
		for _, capability := range caps.GetResourceTypeCapabilities() {
			byType[capability.GetResourceType().GetId()] = capability.GetCapabilities()
		}
		require.Contains(t, byType["service_account"], v2.Capability_CAPABILITY_CREDENTIAL_ENCRYPTION_JWE)
		require.NotContains(t, byType["secret"], v2.Capability_CAPABILITY_CREDENTIAL_ENCRYPTION_JWE,
			"a resource type that cannot issue credentials must not advertise JWE encryption")
	})

	t.Run("no issuer", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{newTestResourceSyncer("service_account")}))
		require.NoError(t, err)

		caps, err := connector.(*builder).GetCapabilities(ctx)
		require.NoError(t, err)
		require.NotContains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_CREDENTIAL_ENCRYPTION_JWE)
		for _, capability := range caps.GetResourceTypeCapabilities() {
			require.NotContains(t, capability.GetCapabilities(), v2.Capability_CAPABILITY_CREDENTIAL_ENCRYPTION_JWE)
		}
	})
}

// TestJWEAuthenticatedDataSurvivesProtoRoundTrip is the generic AAD contract:
// the field is opaque bytes, so arbitrary binary and JSON context must survive
// the wire unchanged.
func TestJWEAuthenticatedDataSurvivesProtoRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		aad  []byte
	}{
		{"empty", nil},
		{"json context", []byte(`{"tenant":"acme","purpose":"api-key"}`)},
		{"binary context", []byte{0x00, 0xff, 0xfe, 0x01, 0x80, 0x7f}},
		{"invalid utf-8", []byte{0xc3, 0x28, 0xa0, 0xa1}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := jweConfigForPublicKey(publicJWKJSON(t, make([]byte, 1216), nil), func(c *v2.EncryptionConfig_JWKPublicKeyConfig) {
				c.SetAdditionalAuthenticatedData(tc.aad)
			})

			wire, err := proto.Marshal(config)
			require.NoError(t, err)
			var decoded v2.EncryptionConfig
			require.NoError(t, proto.Unmarshal(wire, &decoded))

			if tc.aad == nil {
				require.Empty(t, decoded.GetJwkPublicKeyConfig().GetAdditionalAuthenticatedData())
				return
			}
			require.Equal(t, tc.aad, decoded.GetJwkPublicKeyConfig().GetAdditionalAuthenticatedData())
		})
	}
}
