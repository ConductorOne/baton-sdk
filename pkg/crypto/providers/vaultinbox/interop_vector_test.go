package vaultinbox

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

const (
	vectorPath = "testdata/vault-inbox-submission-vector.json"
	// vectorWriteEnv regenerates the committed fixture. The HPKE encapsulation
	// is randomized, so the fixture is generated once and then pinned: every
	// run after that asserts the committed bytes still open and still decode to
	// the expected payload.
	vectorWriteEnv = "VAULT_INBOX_VECTOR_WRITE"
)

type interopVector struct {
	Profile             string          `json:"profile"`
	Note                string          `json:"note"`
	PrivateJWKJSON      string          `json:"private_jwk_json"`
	PublicJWKJSON       string          `json:"public_jwk_json"`
	PublicKeyThumbprint string          `json:"public_key_thumbprint"`
	EnvelopeJSON        string          `json:"envelope_json"`
	BindingHex          string          `json:"binding_hex"`
	Binding             interopBinding  `json:"binding"`
	ExpectedPlaintext   json.RawMessage `json:"expected_plaintext"`
}

type interopBinding struct {
	TenantID        string `json:"tenant_id"`
	VaultBoundaryID string `json:"vault_boundary_id"`
	InboxKeyID      string `json:"inbox_key_id"`
	KeyGeneration   uint64 `json:"key_generation"`
	PayloadScheme   string `json:"payload_scheme"`
	SubmissionID    string `json:"submission_id"`
}

// buildVector seals a fresh submission with the committed recipient so the
// envelope always matches the committed private key.
func buildVector(t *testing.T) interopVector {
	t.Helper()
	jwk, thumbprint := vectorPublicJWK(t)
	config := configFor(t, func(c *v2.VaultInboxRecipientConfig) {
		c.PublicJwkJson = jwk
		c.PublicKeyThumbprint = thumbprint
	})
	encrypted, err := NewProvider().Encrypt(context.Background(), config, vectorPlaintext())
	require.NoError(t, err)
	privateJWK := privateJWKJSON(t)
	binding := bindingBytes(config.GetVaultInboxRecipientConfig())
	return interopVector{
		Profile:             "latchkey.vault-inbox.submission.v1",
		Note:                "Produced by filippo.io/hpke in baton-sdk with the fixed 0x42 X-Wing seed. Must open with the unmodified Latchkey crate: latchkey_mls_core::vault_inbox::open_vault_submission, then latchkey_client_sdk::vault_inbox::decode_secret_submission_payload_for_open.",
		PrivateJWKJSON:      privateJWK,
		PublicJWKJSON:       jwk,
		PublicKeyThumbprint: thumbprint,
		EnvelopeJSON:        string(encrypted.GetEncryptedBytes()),
		BindingHex:          hex.EncodeToString(binding),
		Binding: interopBinding{
			TenantID:        vectorTenant,
			VaultBoundaryID: vectorVault,
			InboxKeyID:      vectorInboxKey,
			KeyGeneration:   vectorGeneration,
			PayloadScheme:   vectorScheme,
			SubmissionID:    vectorSubmission,
		},
		ExpectedPlaintext: json.RawMessage(expectedPayloadJSON),
	}
}

func privateJWKJSON(t *testing.T) string {
	t.Helper()
	key := vectorPrivateKey(t)
	raw, err := key.Bytes()
	require.NoError(t, err)
	require.Len(t, raw, 32)
	return `{"kty":"` + jwkKtyAKP + `","alg":"` + jwkAlg + `","pub":"` +
		base64.RawURLEncoding.EncodeToString(key.PublicKey().Bytes()) +
		`","priv":"` + base64.RawURLEncoding.EncodeToString(raw) + `"}`
}

// TestCommittedInteropVectorMatchesProvider pins the cross-language fixture: it
// must describe the coordinates this provider binds, and its envelope must open
// under those coordinates and decode to the expected payload.
func TestCommittedInteropVectorMatchesProvider(t *testing.T) {
	if os.Getenv(vectorWriteEnv) != "" {
		regenerated := buildVector(t)
		encoded, err := json.MarshalIndent(regenerated, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.MkdirAll(filepath.Dir(vectorPath), 0o755))
		require.NoError(t, os.WriteFile(vectorPath, append(encoded, '\n'), 0o644))
	}

	raw, err := os.ReadFile(vectorPath)
	require.NoError(t, err)
	var vector interopVector
	require.NoError(t, json.Unmarshal(raw, &vector))

	// The committed coordinates are the ones the provider binds, byte for byte.
	config := configFor(t, func(c *v2.VaultInboxRecipientConfig) {
		c.PublicJwkJson = vector.PublicJWKJSON
		c.PublicKeyThumbprint = vector.PublicKeyThumbprint
	})
	require.Equal(t, vector.BindingHex, hex.EncodeToString(bindingBytes(config.GetVaultInboxRecipientConfig())))

	thumbprint, err := publicKeyThumbprint(vector.PublicJWKJSON)
	require.NoError(t, err)
	require.Equal(t, thumbprint, vector.PublicKeyThumbprint)

	var envelope struct {
		Version    uint8  `json:"version"`
		Alg        string `json:"alg"`
		Enc        string `json:"enc"`
		Ciphertext string `json:"ciphertext"`
	}
	require.NoError(t, json.Unmarshal([]byte(vector.EnvelopeJSON), &envelope))
	require.Equal(t, uint8(envelopeVersion), envelope.Version)
	require.Equal(t, jwkAlg, envelope.Alg)

	opened := openVectorEnvelope(t, config, envelope.Enc, envelope.Ciphertext)
	require.JSONEq(t, string(vector.ExpectedPlaintext), string(opened))
}

// TestCommittedVectorPrivateKeyIsTheFixtureRecipient proves the fixture's
// private JWK is the recipient its public JWK describes, so a Rust opener that
// trusts only the published key opens the same envelope.
func TestCommittedVectorPrivateKeyIsTheFixtureRecipient(t *testing.T) {
	raw, err := os.ReadFile(vectorPath)
	require.NoError(t, err)
	var vector interopVector
	require.NoError(t, json.Unmarshal(raw, &vector))

	var privateJWK struct {
		Pub  string `json:"pub"`
		Priv string `json:"priv"`
	}
	require.NoError(t, json.Unmarshal([]byte(vector.PrivateJWKJSON), &privateJWK))
	require.Equal(t, privateJWK.Pub, jwkPub(t, vector.PublicJWKJSON))

	var publicJWK struct {
		Pub string `json:"pub"`
	}
	require.NoError(t, json.Unmarshal([]byte(vector.PublicJWKJSON), &publicJWK))
	require.Equal(t, publicJWK.Pub, privateJWK.Pub)
}
