package fullknowledge

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"filippo.io/hpke"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	EncryptionProvider   = "baton/full-knowledge-vault/v1"
	MaxValueBytes        = 1 << 20
	PublicKeyBytes       = 1216
	EncapsulatedKeyBytes = 1120
	contextDomain        = "c1/fk-credential-key-capsule/v1"
	valueDomain          = "latchkey/v1/item-value/"
)

var contentTypePattern = regexp.MustCompile(`^[a-z0-9]+([._-][a-z0-9]+)*$`)

type EncryptionProviderImpl struct{}

func (p *EncryptionProviderImpl) ValidateConfig(_ context.Context, conf *v2.EncryptionConfig) error {
	_, err := recipientFromConfig(conf)
	return err
}

func recipientFromConfig(conf *v2.EncryptionConfig) (hpke.PublicKey, error) {
	c := conf.GetFullKnowledgeVaultConfig()
	if c == nil {
		return nil, invalid("full knowledge vault config is required")
	}
	if name := strings.ToLower(strings.TrimSpace(conf.GetProvider())); name != "" && name != EncryptionProvider {
		return nil, invalid("provider does not match full knowledge config")
	}
	if conf.GetKeyId() != "" && conf.GetKeyId() != c.GetKeyId() {
		return nil, invalid("key id does not match preparation")
	}
	if len(conf.ProtoReflect().GetUnknown()) != 0 || len(c.ProtoReflect().GetUnknown()) != 0 {
		return nil, invalid("unknown full knowledge config fields")
	}
	if c.GetProtocolVersion() != v2.FullKnowledgeVaultConfig_PROTOCOL_VERSION_V1 ||
		c.GetKeyCapsuleSuite() != v2.FullKnowledgeVaultConfig_KEY_CAPSULE_SUITE_XWING_HKDF_SHA256_CHACHA20_POLY1305_V1 ||
		c.GetValueSuite() != v2.FullKnowledgeVaultConfig_VALUE_SUITE_NATIVE_ITEM_VALUE_V1 {
		return nil, invalid("unsupported full knowledge profile")
	}
	for _, field := range []struct{ name, value string }{
		{"tenant_id", c.GetTenantId()}, {"ticket_id", c.GetTicketId()},
		{"vault_id", c.GetVaultId()}, {"vault_boundary_id", c.GetVaultBoundaryId()},
		{"secret_id", c.GetSecretId()}, {"version_id", c.GetVersionId()},
		{"preparation_id", c.GetPreparationId()}, {"key_id", c.GetKeyId()},
	} {
		if len(field.value) == 0 || len(field.value) > 1024 || !utf8.ValidString(field.value) ||
			strings.TrimSpace(field.value) != field.value || strings.ContainsFunc(field.value, unicode.IsControl) {
			return nil, invalid("invalid " + field.name)
		}
	}
	if len(c.GetContentType()) > 128 || !contentTypePattern.MatchString(c.GetContentType()) {
		return nil, invalid("invalid content_type")
	}
	key := c.GetKeyCapsulePublicKey()
	if len(key) != PublicKeyBytes {
		return nil, invalid("invalid capsule public key length")
	}
	publicKey, err := hpke.MLKEM768X25519().NewPublicKey(key)
	if err != nil {
		return nil, invalid("invalid capsule public key")
	}
	// Parsing alone accepts low-order X25519 points; ECDH rejects them before mint.
	probe, err := ecdh.X25519().NewPrivateKey(make([]byte, 32))
	if err != nil {
		return nil, invalid("cannot validate capsule public key")
	}
	xkey, err := ecdh.X25519().NewPublicKey(key[len(key)-32:])
	if err != nil {
		return nil, invalid("invalid capsule public key")
	}
	if _, err := probe.ECDH(xkey); err != nil {
		return nil, invalid("invalid capsule public key")
	}
	return publicKey, nil
}

func (p *EncryptionProviderImpl) Encrypt(ctx context.Context, conf *v2.EncryptionConfig, plaintext *v2.PlaintextData) (*v2.EncryptedData, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	publicKey, err := recipientFromConfig(conf)
	if err != nil {
		return nil, err
	}
	if len(plaintext.GetBytes()) == 0 || len(plaintext.GetBytes()) > MaxValueBytes {
		return nil, invalid("credential value must contain 1..1048576 bytes")
	}
	c := conf.GetFullKnowledgeVaultConfig()
	payload, err := proto.Marshal(v2.FullKnowledgeSecretPayloadV2_builder{
		EnvelopeVersion: 2, ContentType: c.GetContentType(), Value: plaintext.GetBytes(),
	}.Build())
	if err != nil {
		return nil, fmt.Errorf("full knowledge: encode value payload: %w", err)
	}
	defer clear(payload)
	var cek [32]byte
	if _, err := rand.Read(cek[:]); err != nil {
		return nil, fmt.Errorf("full knowledge: generate item key: %w", err)
	}
	defer clear(cek[:])
	valueCiphertext, err := sealValue(c, cek[:], payload, rand.Reader)
	if err != nil {
		return nil, err
	}
	binding := capsuleContext(c, sha256.Sum256(valueCiphertext))
	capsule := binary.BigEndian.AppendUint32(nil, 1)
	capsule = appendField(capsule, binding)
	capsule = append(capsule, cek[:]...)
	defer clear(capsule)
	enc, sender, err := hpke.NewSender(publicKey, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), binding)
	if err != nil {
		return nil, fmt.Errorf("full knowledge: create capsule sender: %w", err)
	}
	ciphertext, err := sender.Seal(binding, capsule)
	if err != nil {
		return nil, fmt.Errorf("full knowledge: seal key capsule: %w", err)
	}
	envelope, err := proto.Marshal(v2.FullKnowledgeCredentialEnvelope_builder{
		EnvelopeVersion: 1, Config: c, ValueCiphertext: valueCiphertext,
		EncapsulatedKey: enc, KeyCapsuleCiphertext: ciphertext,
	}.Build())
	if err != nil {
		return nil, fmt.Errorf("full knowledge: encode encrypted envelope: %w", err)
	}
	return v2.EncryptedData_builder{
		Provider: EncryptionProvider, KeyIds: []string{c.GetKeyId()},
		Name: plaintext.GetName(), Description: plaintext.GetDescription(), Schema: plaintext.GetSchema(),
		EncryptedBytes: envelope,
	}.Build(), nil
}

func capsuleContext(c *v2.FullKnowledgeVaultConfig, digest [32]byte) []byte {
	// #nosec G115 -- recipientFromConfig accepts only the positive V1 enum values before this call.
	b := binary.BigEndian.AppendUint32([]byte(contextDomain), uint32(c.GetProtocolVersion()))
	for _, field := range []string{c.GetTenantId(), c.GetTicketId(), c.GetVaultId(), c.GetVaultBoundaryId(),
		c.GetSecretId(), c.GetVersionId(), c.GetContentType(), c.GetPreparationId(), c.GetKeyId()} {
		b = appendField(b, []byte(field))
	}
	// #nosec G115 -- recipientFromConfig accepts only capsule suite 1.
	b = binary.BigEndian.AppendUint32(b, uint32(c.GetKeyCapsuleSuite()))
	b = appendField(b, c.GetKeyCapsulePublicKey())
	// #nosec G115 -- recipientFromConfig accepts only value suite 1.
	b = binary.BigEndian.AppendUint32(b, uint32(c.GetValueSuite()))
	return append(b, digest[:]...)
}

func valueBinding(c *v2.FullKnowledgeVaultConfig) []byte {
	b := []byte(valueDomain)
	for _, field := range []string{c.GetTenantId(), c.GetSecretId(), c.GetVersionId(), c.GetContentType()} {
		b = appendField(b, []byte(field))
	}
	return b
}

func sealValue(c *v2.FullKnowledgeVaultConfig, cek, payload []byte, randomness io.Reader) ([]byte, error) {
	binding := valueBinding(c)
	var key [32]byte
	defer clear(key[:])
	if _, err := io.ReadFull(hkdf.New(sha256.New, cek, nil, binding), key[:]); err != nil {
		return nil, fmt.Errorf("full knowledge: derive item value key: %w", err)
	}
	cipher, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, fmt.Errorf("full knowledge: initialize item value cipher: %w", err)
	}
	frame := make([]byte, 1+chacha20poly1305.NonceSize, 1+chacha20poly1305.NonceSize+len(payload)+cipher.Overhead())
	frame[0] = 0x06
	if _, err := io.ReadFull(randomness, frame[1:]); err != nil {
		return nil, fmt.Errorf("full knowledge: generate item value nonce: %w", err)
	}
	return cipher.Seal(frame, frame[1:], payload, binding), nil
}

func appendField(dst, field []byte) []byte {
	// #nosec G115 -- fields are validated identifiers (<=1024), a 1216-byte key, or their bounded context.
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(field)))
	return append(dst, field...)
}

func invalid(message string) error {
	return status.Error(codes.InvalidArgument, "full knowledge: "+message)
}
