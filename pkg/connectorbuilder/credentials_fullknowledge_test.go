package connectorbuilder

import (
	"context"
	"testing"

	"filippo.io/hpke"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
)

type fullKnowledgeTestIssuer struct {
	*testCredentialIssuer
	count     int
	advertise bool
}

func (i *fullKnowledgeTestIssuer) Issue(ctx context.Context, input *CredentialIssueInput) (*CredentialIssueOutput, error) {
	output, err := i.testCredentialIssuer.Issue(ctx, input)
	if err != nil {
		return nil, err
	}
	output.PlaintextData = output.PlaintextData[:i.count]
	return output, nil
}

func (i *fullKnowledgeTestIssuer) IssueCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsCredentialIssue, annotations.Annotations, error) {
	details, annos, err := i.testCredentialIssuer.IssueCapabilityDetails(ctx)
	if err == nil && i.advertise {
		details.GetOptions()[0].SetFullKnowledgeVaultProfiles([]v2.FullKnowledgeVaultConfig_ProtocolVersion{v2.FullKnowledgeVaultConfig_PROTOCOL_VERSION_V1})
	}
	return details, annos, err
}

func fullKnowledgeIssueRequest(t *testing.T) *v2.IssueCredentialRequest {
	t.Helper()
	key, err := hpke.MLKEM768X25519().GenerateKey()
	require.NoError(t, err)
	config := v2.FullKnowledgeVaultConfig_builder{
		ProtocolVersion: v2.FullKnowledgeVaultConfig_PROTOCOL_VERSION_V1,
		TenantId:        "tenant", TicketId: "ticket", VaultId: "vault", VaultBoundaryId: "boundary",
		SecretId: "secret", VersionId: "version", ContentType: "generic", PreparationId: "preparation", KeyId: "key",
		KeyCapsuleSuite:     v2.FullKnowledgeVaultConfig_KEY_CAPSULE_SUITE_XWING_HKDF_SHA256_CHACHA20_POLY1305_V1,
		KeyCapsulePublicKey: key.PublicKey().Bytes(), ValueSuite: v2.FullKnowledgeVaultConfig_VALUE_SUITE_NATIVE_ITEM_VALUE_V1,
	}.Build()
	return v2.IssueCredentialRequest_builder{
		IdentityId: v2.ResourceId_builder{ResourceType: "service_account", Resource: "sa-1"}.Build(),
		CredentialOptions: v2.CredentialIssueOptions_builder{
			SecretResourceTypeId: "secret", ApiKey: &v2.CredentialIssueOptions_ApiKey{},
		}.Build(),
		RequestId:         "request-1",
		EncryptionConfigs: []*v2.EncryptionConfig{v2.EncryptionConfig_builder{FullKnowledgeVaultConfig: config}.Build()},
	}.Build()
}

func TestFullKnowledgeRejectsBeforeMint(t *testing.T) {
	cases := map[string]func(*v2.IssueCredentialRequest, *fullKnowledgeTestIssuer){
		"not advertised": func(_ *v2.IssueCredentialRequest, i *fullKnowledgeTestIssuer) { i.advertise = false },
		"fanout": func(r *v2.IssueCredentialRequest, _ *fullKnowledgeTestIssuer) {
			r.SetEncryptionConfigs(append(r.GetEncryptionConfigs(), r.GetEncryptionConfigs()[0]))
		},
		"unknown suite": func(r *v2.IssueCredentialRequest, _ *fullKnowledgeTestIssuer) {
			r.GetEncryptionConfigs()[0].GetFullKnowledgeVaultConfig().SetKeyCapsuleSuite(99)
		},
		"unknown config": func(r *v2.IssueCredentialRequest, _ *fullKnowledgeTestIssuer) {
			r.GetEncryptionConfigs()[0].ClearFullKnowledgeVaultConfig()
		},
		"invalid key": func(r *v2.IssueCredentialRequest, _ *fullKnowledgeTestIssuer) {
			r.GetEncryptionConfigs()[0].GetFullKnowledgeVaultConfig().SetKeyCapsulePublicKey([]byte{1})
		},
		"wrong provider": func(r *v2.IssueCredentialRequest, _ *fullKnowledgeTestIssuer) {
			r.GetEncryptionConfigs()[0].SetProvider("baton/jwk/v1")
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			issuer := &fullKnowledgeTestIssuer{testCredentialIssuer: newTestCredentialIssuer("service_account"), count: 1, advertise: true}
			r := fullKnowledgeIssueRequest(t)
			mutate(r, issuer)
			connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{issuer, newTestCredentialSecretDeleter()}))
			require.NoError(t, err)
			result, err := connector.IssueCredential(context.Background(), r)
			require.Error(t, err)
			require.Nil(t, result)
			require.Nil(t, issuer.lastInput)
		})
	}
}

func TestFullKnowledgeRequiresOneProviderValue(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count int
	}{{"zero", 0}, {"one", 1}, {"multiple", 2}} {
		t.Run(tc.name, func(t *testing.T) {
			issuer := &fullKnowledgeTestIssuer{testCredentialIssuer: newTestCredentialIssuer("service_account"), count: tc.count, advertise: true}
			connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{issuer, newTestCredentialSecretDeleter()}))
			require.NoError(t, err)
			result, err := connector.IssueCredential(context.Background(), fullKnowledgeIssueRequest(t))
			require.NotNil(t, issuer.lastInput)
			if tc.count == 1 {
				require.NoError(t, err)
				require.Len(t, result.GetEncryptedData(), 1)
			} else {
				require.ErrorContains(t, err, "reconciliation is required")
				require.NotContains(t, err.Error(), "super-secret-key-material")
				require.Nil(t, result)
			}
		})
	}
}
