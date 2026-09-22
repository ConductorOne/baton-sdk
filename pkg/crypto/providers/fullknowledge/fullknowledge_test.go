package fullknowledge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"filippo.io/hpke"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
	"google.golang.org/protobuf/proto"
)

func testConfig(t *testing.T) (*v2.EncryptionConfig, hpke.PrivateKey) {
	t.Helper()
	key, err := hpke.MLKEM768X25519().NewPrivateKey(bytes.Repeat([]byte{0x42}, 32))
	require.NoError(t, err)
	c := v2.FullKnowledgeVaultConfig_builder{
		ProtocolVersion: v2.FullKnowledgeVaultConfig_PROTOCOL_VERSION_V1,
		TenantId:        "tenant", TicketId: "ticket", VaultId: "vault", VaultBoundaryId: "boundary",
		SecretId: "secret", VersionId: "version", ContentType: "generic", PreparationId: "preparation", KeyId: "key",
		KeyCapsuleSuite:     v2.FullKnowledgeVaultConfig_KEY_CAPSULE_SUITE_XWING_HKDF_SHA256_CHACHA20_POLY1305_V1,
		KeyCapsulePublicKey: key.PublicKey().Bytes(), ValueSuite: v2.FullKnowledgeVaultConfig_VALUE_SUITE_NATIVE_ITEM_VALUE_V1,
	}.Build()
	return v2.EncryptionConfig_builder{FullKnowledgeVaultConfig: c}.Build(), key
}

func TestConfigRejectsInvalidPreparation(t *testing.T) {
	cases := map[string]func(*v2.EncryptionConfig){
		"missing":           func(c *v2.EncryptionConfig) { c.ClearFullKnowledgeVaultConfig() },
		"provider":          func(c *v2.EncryptionConfig) { c.SetProvider("baton/age/v1") },
		"outer key":         func(c *v2.EncryptionConfig) { c.SetKeyId("other") },
		"protocol zero":     func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetProtocolVersion(0) },
		"protocol unknown":  func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetProtocolVersion(2) },
		"capsule zero":      func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetKeyCapsuleSuite(0) },
		"capsule unknown":   func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetKeyCapsuleSuite(2) },
		"value zero":        func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetValueSuite(0) },
		"value unknown":     func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetValueSuite(2) },
		"tenant":            func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetTenantId("") },
		"ticket":            func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetTicketId("") },
		"vault":             func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetVaultId("") },
		"boundary":          func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetVaultBoundaryId("") },
		"secret":            func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetSecretId("") },
		"version":           func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetVersionId("") },
		"preparation":       func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetPreparationId("") },
		"key id":            func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetKeyId("") },
		"whitespace":        func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetTenantId(" tenant") },
		"control":           func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetTenantId("ten\x00ant") },
		"utf8":              func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetTenantId("\xff") },
		"too long":          func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetTenantId(strings.Repeat("t", 1025)) },
		"content empty":     func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetContentType("") },
		"content grammar":   func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetContentType("application/json") },
		"content length":    func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetContentType(strings.Repeat("a", 129)) },
		"public key length": func(c *v2.EncryptionConfig) { c.GetFullKnowledgeVaultConfig().SetKeyCapsulePublicKey([]byte{1}) },
		"low order":         func(c *v2.EncryptionConfig) { clear(c.GetFullKnowledgeVaultConfig().GetKeyCapsulePublicKey()[1184:]) },
		"noncanonical MLKEM": func(c *v2.EncryptionConfig) {
			copy(c.GetFullKnowledgeVaultConfig().GetKeyCapsulePublicKey(), []byte{0xff, 0xff, 0xff})
		},
		"unknown field": func(c *v2.EncryptionConfig) {
			c.GetFullKnowledgeVaultConfig().ProtoReflect().SetUnknown([]byte{0x70, 1})
		},
	}
	provider := &EncryptionProviderImpl{}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			conf, _ := testConfig(t)
			require.NoError(t, provider.ValidateConfig(context.Background(), conf))
			mutate(conf)
			require.Error(t, provider.ValidateConfig(context.Background(), conf))
		})
	}
}

func TestEnvelopeOpensCapsuleAndNativePayload(t *testing.T) {
	conf, privateKey := testConfig(t)
	c := conf.GetFullKnowledgeVaultConfig()
	provider := &EncryptionProviderImpl{}
	plaintext := v2.PlaintextData_builder{Name: "api_key", Bytes: []byte("credential-material")}.Build()
	encrypted, err := provider.Encrypt(context.Background(), conf, plaintext)
	require.NoError(t, err)
	require.Equal(t, EncryptionProvider, encrypted.GetProvider())
	require.Equal(t, []string{c.GetKeyId()}, encrypted.GetKeyIds())
	require.Empty(t, encrypted.GetKeyId())
	envelope := &v2.FullKnowledgeCredentialEnvelope{}
	require.NoError(t, proto.Unmarshal(encrypted.GetEncryptedBytes(), envelope))
	require.Equal(t, uint32(1), envelope.GetEnvelopeVersion())
	require.True(t, proto.Equal(c, envelope.GetConfig()))
	require.Len(t, envelope.GetEncapsulatedKey(), EncapsulatedKeyBytes)
	binding := capsuleContext(c, sha256.Sum256(envelope.GetValueCiphertext()))
	recipient, err := hpke.NewRecipient(envelope.GetEncapsulatedKey(), privateKey, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), binding)
	require.NoError(t, err)
	capsule, err := recipient.Open(binding, envelope.GetKeyCapsuleCiphertext())
	require.NoError(t, err)
	require.Equal(t, uint32(1), binary.BigEndian.Uint32(capsule[:4]))
	require.EqualValues(t, len(binding), binary.BigEndian.Uint32(capsule[4:8]))
	require.Equal(t, binding, capsule[8:8+len(binding)])
	require.Len(t, capsule, 8+len(binding)+32)
	cek := capsule[len(capsule)-32:]
	key := make([]byte, 32)
	require.NoError(t, func() error { _, err := io.ReadFull(hkdf.New(sha256.New, cek, nil, valueBinding(c)), key); return err }())
	aead, err := chacha20poly1305.New(key)
	require.NoError(t, err)
	value := envelope.GetValueCiphertext()
	require.Equal(t, byte(6), value[0])
	payloadBytes, err := aead.Open(nil, value[1:13], value[13:], valueBinding(c))
	require.NoError(t, err)
	payload := &v2.FullKnowledgeSecretPayloadV2{}
	require.NoError(t, proto.Unmarshal(payloadBytes, payload))
	require.Equal(t, uint32(2), payload.GetEnvelopeVersion())
	require.Equal(t, c.GetContentType(), payload.GetContentType())
	require.Equal(t, plaintext.GetBytes(), payload.GetValue())
	other, err := provider.Encrypt(context.Background(), conf, plaintext)
	require.NoError(t, err)
	require.NotEqual(t, encrypted.GetEncryptedBytes(), other.GetEncryptedBytes())

	for _, offset := range []int{0, 1088} {
		t.Run(fmtOffset(offset), func(t *testing.T) {
			tampered := bytes.Clone(envelope.GetEncapsulatedKey())
			tampered[offset] ^= 1
			r, err := hpke.NewRecipient(tampered, privateKey, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), binding)
			if err == nil {
				_, err = r.Open(binding, envelope.GetKeyCapsuleCiphertext())
			}
			require.Error(t, err)
		})
	}
}

func fmtOffset(offset int) string {
	if offset == 0 {
		return "MLKEM tamper"
	}
	return "X25519 tamper"
}

func TestSealValueRejectsEntropyFailure(t *testing.T) {
	conf, _ := testConfig(t)
	result, err := sealValue(conf.GetFullKnowledgeVaultConfig(), make([]byte, 32), []byte("value"), bytes.NewReader(nil))
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, result)
}

func TestCapsuleAuthenticatesEveryPreparationField(t *testing.T) {
	conf, privateKey := testConfig(t)
	encrypted, err := (&EncryptionProviderImpl{}).Encrypt(context.Background(), conf,
		v2.PlaintextData_builder{Name: "key", Bytes: []byte("value")}.Build())
	require.NoError(t, err)
	envelope := &v2.FullKnowledgeCredentialEnvelope{}
	require.NoError(t, proto.Unmarshal(encrypted.GetEncryptedBytes(), envelope))
	digest := sha256.Sum256(envelope.GetValueCiphertext())
	cases := map[string]func(*v2.FullKnowledgeVaultConfig){
		"protocol":      func(c *v2.FullKnowledgeVaultConfig) { c.SetProtocolVersion(2) },
		"tenant":        func(c *v2.FullKnowledgeVaultConfig) { c.SetTenantId("other") },
		"ticket":        func(c *v2.FullKnowledgeVaultConfig) { c.SetTicketId("other") },
		"vault":         func(c *v2.FullKnowledgeVaultConfig) { c.SetVaultId("other") },
		"boundary":      func(c *v2.FullKnowledgeVaultConfig) { c.SetVaultBoundaryId("other") },
		"secret":        func(c *v2.FullKnowledgeVaultConfig) { c.SetSecretId("other") },
		"version":       func(c *v2.FullKnowledgeVaultConfig) { c.SetVersionId("other") },
		"content":       func(c *v2.FullKnowledgeVaultConfig) { c.SetContentType("password") },
		"preparation":   func(c *v2.FullKnowledgeVaultConfig) { c.SetPreparationId("other") },
		"key id":        func(c *v2.FullKnowledgeVaultConfig) { c.SetKeyId("other") },
		"capsule suite": func(c *v2.FullKnowledgeVaultConfig) { c.SetKeyCapsuleSuite(2) },
		"public key":    func(c *v2.FullKnowledgeVaultConfig) { c.GetKeyCapsulePublicKey()[0] ^= 1 },
		"value suite":   func(c *v2.FullKnowledgeVaultConfig) { c.SetValueSuite(2) },
	}
	open := func(c *v2.FullKnowledgeVaultConfig, digest [32]byte) error {
		binding := capsuleContext(c, digest)
		r, err := hpke.NewRecipient(envelope.GetEncapsulatedKey(), privateKey, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), binding)
		if err != nil {
			return err
		}
		_, err = r.Open(binding, envelope.GetKeyCapsuleCiphertext())
		return err
	}
	require.NoError(t, open(conf.GetFullKnowledgeVaultConfig(), digest))
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := proto.Clone(conf.GetFullKnowledgeVaultConfig()).(*v2.FullKnowledgeVaultConfig)
			mutate(c)
			require.Error(t, open(c, digest))
		})
	}
	digest[0] ^= 1
	require.Error(t, open(conf.GetFullKnowledgeVaultConfig(), digest))
}
