package actions

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"testing"

	"filippo.io/hpke"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/vaultinbox"
	"github.com/stretchr/testify/require"
)

const gateJWKAlg = "HPKE-Base-X-Wing-Draft06-HKDF-SHA256-ChaCha20Poly1305"

func gateVaultInboxConfig(t *testing.T) *v2.EncryptionConfig {
	t.Helper()
	key, err := hpke.MLKEM768X25519().NewPrivateKey(make([]byte, 32))
	require.NoError(t, err)
	pub := base64.RawURLEncoding.EncodeToString(key.PublicKey().Bytes())
	digest := sha256.Sum256([]byte(`{"alg":"` + gateJWKAlg + `","kty":"AKP","pub":"` + pub + `"}`))
	thumbprint := base64.RawURLEncoding.EncodeToString(digest[:])
	jwk := `{"kty":"AKP","alg":"` + gateJWKAlg + `","pub":"` + pub + `","` + vaultinbox.JWKExtensionMember + `":{` +
		`"version":` + strconv.Itoa(vaultinbox.JWKExtensionVersion) + `,"suite":"` + gateJWKAlg + `",` +
		`"tenant_id":"tenant-1","vault_boundary_id":"vault-1","key_generation":1,` +
		`"payload_scheme":"` + vaultinbox.PayloadSchemeSecretV1 + `","submission_id":"submission-1",` +
		`"public_key_thumbprint":"` + thumbprint + `"}}`
	return v2.EncryptionConfig_builder{
		Provider: vaultinbox.EncryptionProvider,
		KeyId:    "inbox-key-1",
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
			PubKey: []byte(jwk),
		}.Build(),
	}.Build()
}

func gateActionValues(n int) []*v2.PlaintextData {
	names := []string{"api_key", "api_key_id"}
	out := make([]*v2.PlaintextData, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, v2.PlaintextData_builder{
			Name:  names[i%len(names)],
			Bytes: []byte("v"),
		}.Build())
	}
	return out
}

// TestRegisteredActionVaultInboxCardinality pins the vault-inbox gate on the
// registered-action path, which is the one place the encryption manager could be
// nil: it is only assigned when the handler declares secret return types, so the
// early return for a handler without them is what prevents the dereference.
func TestRegisteredActionVaultInboxCardinality(t *testing.T) {
	t.Parallel()

	t.Run("a handler with no secret return types never needs a manager", func(t *testing.T) {
		t.Parallel()
		_, err := prepareActionResult(context.Background(), registeredActionHandler{}, nil, nil, nil, nil, true)
		require.NoError(t, err)
	})

	t.Run("one value seals", func(t *testing.T) {
		t.Parallel()
		manager, err := crypto.NewEncryptionManager(nil, []*v2.EncryptionConfig{gateVaultInboxConfig(t)})
		require.NoError(t, err)

		encrypted, err := prepareActionResult(context.Background(), gateHandler(), manager, encryptForAction, nil, gateActionValues(1), true)
		require.NoError(t, err)
		require.Len(t, encrypted, 1)
		require.Equal(t, vaultinbox.EncryptionProvider, encrypted[0].GetProvider())
	})

	for _, count := range []int{0, 2} {
		t.Run(map[int]string{0: "zero values", 2: "two values"}[count]+" are refused", func(t *testing.T) {
			t.Parallel()
			manager, err := crypto.NewEncryptionManager(nil, []*v2.EncryptionConfig{gateVaultInboxConfig(t)})
			require.NoError(t, err)

			_, err = prepareActionResult(context.Background(), gateHandler(), manager, encryptForAction, nil, gateActionValues(count), true)
			require.ErrorContains(t, err, "exactly one plaintext value",
				"the cardinality gate must be what refuses this, not an earlier check")
		})
	}
}

func gateHandler() registeredActionHandler {
	// Both names are declared so the two-value case reaches the cardinality gate
	// rather than the duplicate-name or undeclared-name checks.
	return registeredActionHandler{secretReturnNames: map[string]struct{}{
		"api_key":    {},
		"api_key_id": {},
	}}
}

func encryptForAction(ctx context.Context, manager *crypto.EncryptionManager, plaintext *v2.PlaintextData) ([]*v2.EncryptedData, error) {
	return manager.Encrypt(ctx, plaintext)
}
