package crypto

import (
	"testing"

	"github.com/go-jose/go-jose/v3"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestNewPubKeyEncryptionManager(t *testing.T) {
	// generate a keypair to encrypt to
	privKey, pubKeyJWK := GenKey()

	pubKeyJWKBytes, err := pubKeyJWK.MarshalJSON()
	require.NoError(t, err)
	// create an encryption manager
	opts := &v2.CredentialOptions{}
	config := []*v2.EncryptionConfig{
		{
			Config: &v2.EncryptionConfig_JwkPublicKeyConfig{
				JwkPublicKeyConfig: &v2.EncryptionConfig_JWKPublicKeyConfig{
					PubKey: pubKeyJWKBytes,
				},
			},
		},
	}
	pkem, err := NewPubKeyEncryptionManager(opts, config)
	require.NoError(t, err)

	// encrypt a plaintext credential
	cred := &PlaintextCredential{
		Name:        "password",
		Description: "this is the password",
		Schema:      "string",
		Bytes:       []byte("hunter2"),
	}
	encryptedValues, err := pkem.Encrypt(cred)
	require.NoError(t, err)

	// assert encrypt
	require.Len(t, encryptedValues, 1)
	encryptedValue := encryptedValues[0]
	require.Equal(t, encryptedValue.Name, cred.Name)
	require.Equal(t, encryptedValue.Description, cred.Description)
	require.Equal(t, encryptedValue.Schema, cred.Schema)

	require.Equal(t, encryptedValue.KeyId, pubKeyJWK.KeyID)
	require.NotEmpty(t, encryptedValue.EncryptedBytes)

	// assert we can decrypt with our private key
	jwe, err := jose.ParseEncrypted(string(encryptedValue.EncryptedBytes))
	require.NoError(t, err)
	plaintext, err := jwe.Decrypt(privKey)
	require.NoError(t, err)
	require.Equal(t, []byte("hunter2"), plaintext)

	// assert a different private key cannot decrypt
	privKey2, _ := GenKey()
	_, err = jwe.Decrypt(privKey2)
	require.ErrorContains(t, err, "go-jose/go-jose: error in cryptographic primitive")
}
