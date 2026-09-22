package fullknowledge

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"filippo.io/hpke"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// All fixture keys are synthetic. The shared Latchkey Rust implementation
// consumes the same fixture for native value and HPKE interoperability tests.
const interopVectorPath = "testdata/direct-fk-vectors.json"
const interopVectorEnv = "FK_INTEROP_WRITE"

const (
	interopRecipientSeedHex = "9d1f0f0e6ab0ef1f3f5ccf74a2b8f6bdbfe1a2c6f9d3f3b3f4a2b1c0d9e8f706"
	interopCEKHex           = "0e2b7a5d4c3b2a19081706990817263544536271809a1b2c3d4e5f60718293a4"
	interopNonceHex         = "a0a1a2a3a4a5a6a7a8a9aaab"
	interopValueBytes       = "direct-fk-credential-value"
	// Encapsulation is ML-KEM-768 ciphertext || X25519 public key, so the
	// second hybrid component starts 32 bytes before the end.
	interopX25519Offset = EncapsulatedKeyBytes - 32
)

var errCapsulePlaintextMismatch = errors.New("capsule plaintext mismatch")

type interopNativeValue struct {
	DomainPrefix     string `json:"domain_prefix"`
	TenantID         string `json:"tenant_id"`
	ItemID           string `json:"item_id"`
	Version          string `json:"version"`
	ContentType      string `json:"content_type"`
	CEKHex           string `json:"cek_hex"`
	NonceHex         string `json:"nonce_hex"`
	PayloadHex       string `json:"payload_hex"`
	CiphertextHex    string `json:"ciphertext_hex"`
	CiphertextSHA256 string `json:"ciphertext_sha256_hex"`
}

type interopCapsule struct {
	InfoEqualsAAD          bool   `json:"info_equals_aad"`
	ContextHex             string `json:"context_hex"`
	RecipientSeedHex       string `json:"recipient_seed_hex"`
	RecipientPublicKeyHex  string `json:"recipient_public_key_hex"`
	PlaintextHex           string `json:"capsule_plaintext_hex"`
	EncapsulationHex       string `json:"encapsulation_hex"`
	CiphertextHex          string `json:"capsule_ciphertext_hex"`
	CiphertextSHA256       string `json:"capsule_ciphertext_sha256_hex"`
	MLKEMComponentOffset   int    `json:"mlkem_component_offset"`
	X25519TamperOffset     int    `json:"x25519_component_offset"`
	X25519ComponentNote    string `json:"x25519_component_note"`
	RustEncapsulationHex   string `json:"rust_encapsulation_hex"`
	RustCiphertextHex      string `json:"rust_capsule_ciphertext_hex"`
	RustRecipientPublicKey string `json:"rust_recipient_public_key_hex"`
}

type interopConfigFields struct {
	ProtocolVersion     v2.FullKnowledgeVaultConfig_ProtocolVersion `json:"protocol_version"`
	TenantID            string                                      `json:"tenant_id"`
	TicketID            string                                      `json:"ticket_id"`
	VaultID             string                                      `json:"vault_id"`
	VaultBoundaryID     string                                      `json:"vault_boundary_id"`
	SecretID            string                                      `json:"secret_id"`
	VersionID           string                                      `json:"version_id"`
	ContentType         string                                      `json:"content_type"`
	PreparationID       string                                      `json:"preparation_id"`
	KeyID               string                                      `json:"key_id"`
	KeyCapsuleSuite     v2.FullKnowledgeVaultConfig_KeyCapsuleSuite `json:"key_capsule_suite"`
	ValueSuite          v2.FullKnowledgeVaultConfig_ValueSuite      `json:"value_suite"`
	ContextDomainPrefix string                                      `json:"context_domain_prefix"`
}

type interopVectors struct {
	Profile     string              `json:"profile"`
	Note        string              `json:"note"`
	Config      interopConfigFields `json:"config"`
	NativeValue interopNativeValue  `json:"native_value"`
	Capsule     interopCapsule      `json:"capsule"`
}

// interopConfig returns the fixed preparation. The public key is supplied
// separately because the capsule half keys off the recipient.
func interopConfig(publicKey []byte) *v2.FullKnowledgeVaultConfig {
	return v2.FullKnowledgeVaultConfig_builder{
		ProtocolVersion:     v2.FullKnowledgeVaultConfig_PROTOCOL_VERSION_V1,
		TenantId:            "tenant-1",
		TicketId:            "ticket-1",
		VaultId:             "vault-1",
		VaultBoundaryId:     "boundary-1",
		SecretId:            "secret-1",
		VersionId:           "version-1",
		ContentType:         "generic",
		PreparationId:       "preparation-1",
		KeyId:               "key-1",
		KeyCapsuleSuite:     v2.FullKnowledgeVaultConfig_KEY_CAPSULE_SUITE_XWING_HKDF_SHA256_CHACHA20_POLY1305_V1,
		KeyCapsulePublicKey: publicKey,
		ValueSuite:          v2.FullKnowledgeVaultConfig_VALUE_SUITE_NATIVE_ITEM_VALUE_V1,
	}.Build()
}

func interopRecipient(t *testing.T) hpke.PrivateKey {
	t.Helper()
	seed, err := hex.DecodeString(interopRecipientSeedHex)
	require.NoError(t, err)
	require.Len(t, seed, 32)
	key, err := hpke.MLKEM768X25519().NewPrivateKey(seed)
	require.NoError(t, err)
	return key
}

func interopPayload(t *testing.T) []byte {
	t.Helper()
	payload, err := proto.Marshal(v2.FullKnowledgeSecretPayloadV2_builder{
		EnvelopeVersion: 2, ContentType: "generic", Value: []byte(interopValueBytes),
	}.Build())
	require.NoError(t, err)
	return payload
}

// buildInteropVectors derives the Go half of the fixture: the native value
// ciphertext from a fixed CEK and nonce, and one capsule sealed to the fixed
// recipient seed.
func buildInteropVectors(t *testing.T) interopVectors {
	t.Helper()
	cek, err := hex.DecodeString(interopCEKHex)
	require.NoError(t, err)
	require.Len(t, cek, 32)
	nonce, err := hex.DecodeString(interopNonceHex)
	require.NoError(t, err)
	require.Len(t, nonce, 12)

	c := interopConfig(interopRecipient(t).PublicKey().Bytes())
	valueCiphertext, err := sealValue(c, cek, interopPayload(t), bytes.NewReader(nonce))
	require.NoError(t, err)
	context := capsuleContext(c, sha256.Sum256(valueCiphertext))

	// Capsule plaintext: u32(1) || field(context) || CEK[32], no trailing bytes.
	capsule := binary.BigEndian.AppendUint32(nil, 1)
	capsule = appendField(capsule, context)
	capsule = append(capsule, cek...)

	publicKey, err := hpke.MLKEM768X25519().NewPublicKey(c.GetKeyCapsulePublicKey())
	require.NoError(t, err)
	encapsulation, sender, err := hpke.NewSender(publicKey, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), context)
	require.NoError(t, err)
	ciphertext, err := sender.Seal(context, capsule)
	require.NoError(t, err)

	valueSum := sha256.Sum256(valueCiphertext)
	capSum := sha256.Sum256(ciphertext)
	return interopVectors{
		Profile: "direct-fk-credentials/v1",
		Note: "Synthetic fixed vectors only. Compare Go sealValue and the pinned " +
			"filippo.io/hpke capsule against multipass value_crypto.rs and hpke-rs 0.7.0.",
		Config: interopConfigFields{
			ProtocolVersion:     c.GetProtocolVersion(),
			TenantID:            c.GetTenantId(),
			TicketID:            c.GetTicketId(),
			VaultID:             c.GetVaultId(),
			VaultBoundaryID:     c.GetVaultBoundaryId(),
			SecretID:            c.GetSecretId(),
			VersionID:           c.GetVersionId(),
			ContentType:         c.GetContentType(),
			PreparationID:       c.GetPreparationId(),
			KeyID:               c.GetKeyId(),
			KeyCapsuleSuite:     c.GetKeyCapsuleSuite(),
			ValueSuite:          c.GetValueSuite(),
			ContextDomainPrefix: contextDomain,
		},
		NativeValue: interopNativeValue{
			DomainPrefix:     valueDomain,
			TenantID:         c.GetTenantId(),
			ItemID:           c.GetSecretId(),
			Version:          c.GetVersionId(),
			ContentType:      c.GetContentType(),
			CEKHex:           interopCEKHex,
			NonceHex:         interopNonceHex,
			PayloadHex:       hex.EncodeToString(interopPayload(t)),
			CiphertextHex:    hex.EncodeToString(valueCiphertext),
			CiphertextSHA256: hex.EncodeToString(valueSum[:]),
		},
		Capsule: interopCapsule{
			InfoEqualsAAD:         true,
			ContextHex:            hex.EncodeToString(context),
			RecipientSeedHex:      interopRecipientSeedHex,
			RecipientPublicKeyHex: hex.EncodeToString(c.GetKeyCapsulePublicKey()),
			PlaintextHex:          hex.EncodeToString(capsule),
			EncapsulationHex:      hex.EncodeToString(encapsulation),
			CiphertextHex:         hex.EncodeToString(ciphertext),
			CiphertextSHA256:      hex.EncodeToString(capSum[:]),
			MLKEMComponentOffset:  0,
			X25519TamperOffset:    interopX25519Offset,
			X25519ComponentNote:   "encapsulation is ML-KEM-768 ciphertext || X25519 public key",
		},
	}
}

// TestInteropVectors recomputes the Go half of the committed fixture and, when
// the Rust half is present, verifies this implementation opens it. Rewrite the
// fixture with FK_INTEROP_WRITE=1 after any profile encoding change.
func TestInteropVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Clean(interopVectorPath))
	require.NoError(t, err)
	var committed interopVectors
	require.NoError(t, json.Unmarshal(raw, &committed))

	derived := buildInteropVectors(t)
	require.Equal(t, derived.Config, committed.Config)
	require.Equal(t, derived.NativeValue, committed.NativeValue)
	require.Equal(t, derived.Capsule.RecipientPublicKeyHex, committed.Capsule.RecipientPublicKeyHex)
	require.Equal(t, derived.Capsule.ContextHex, committed.Capsule.ContextHex)
	require.Equal(t, derived.Capsule.PlaintextHex, committed.Capsule.PlaintextHex)
	// The capsule encapsulation and ciphertext are not reproducible: the sender
	// generates a fresh ephemeral key. They are committed, then opened below.

	// The Rust half of the fixture rebuilds the context and capsule plaintext
	// field by field from the committed config values (see the multipass
	// value_crypto test), so a field reorder cannot satisfy both encoders.

	contextBytes, err := hex.DecodeString(committed.Capsule.ContextHex)
	require.NoError(t, err)
	expect, err := hex.DecodeString(committed.Capsule.PlaintextHex)
	require.NoError(t, err)
	recipient := interopRecipient(t)
	openCapsule := func(encapsulationHex, ciphertextHex string) error {
		enc, err := hex.DecodeString(encapsulationHex)
		if err != nil {
			return err
		}
		ct, err := hex.DecodeString(ciphertextHex)
		if err != nil {
			return err
		}
		r, err := hpke.NewRecipient(enc, recipient, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), contextBytes)
		if err != nil {
			return err
		}
		got, err := r.Open(contextBytes, ct)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, expect) {
			return errCapsulePlaintextMismatch
		}
		return nil
	}

	sealed := []struct {
		name             string
		encapsulationHex string
		ciphertextHex    string
	}{
		{name: "go", encapsulationHex: committed.Capsule.EncapsulationHex, ciphertextHex: committed.Capsule.CiphertextHex},
		{name: "rust", encapsulationHex: committed.Capsule.RustEncapsulationHex, ciphertextHex: committed.Capsule.RustCiphertextHex},
	}
	for _, s := range sealed {
		if s.encapsulationHex == "" {
			require.Fail(t, "fixture is missing a sealed capsule", s.name)
		}
		require.NoError(t, openCapsule(s.encapsulationHex, s.ciphertextHex), s.name)
		encapsulation, err := hex.DecodeString(s.encapsulationHex)
		require.NoError(t, err)
		require.Len(t, encapsulation, EncapsulatedKeyBytes)
		for _, offset := range []int{committed.Capsule.MLKEMComponentOffset, committed.Capsule.X25519TamperOffset} {
			tampered := bytes.Clone(encapsulation)
			tampered[offset] ^= 1
			require.Error(t, openCapsule(hex.EncodeToString(tampered), s.ciphertextHex),
				"%s: tampering the KEM component at offset %d must not open", s.name, offset)
		}
		// Tampering the capsule ciphertext must not open either.
		ciphertext, err := hex.DecodeString(s.ciphertextHex)
		require.NoError(t, err)
		tamperedCiphertext := bytes.Clone(ciphertext)
		tamperedCiphertext[len(tamperedCiphertext)-1] ^= 1
		require.Error(t, openCapsule(s.encapsulationHex, hex.EncodeToString(tamperedCiphertext)), s.name)
	}
	require.Equal(t, committed.Capsule.RecipientPublicKeyHex, committed.Capsule.RustRecipientPublicKey)
}

// TestGenerateInteropVectors rewrites the fixture. It is inert unless
// FK_INTEROP_WRITE=1.
func TestGenerateInteropVectors(t *testing.T) {
	if os.Getenv(interopVectorEnv) != "1" {
		t.Skipf("set %s=1 to rewrite %s", interopVectorEnv, interopVectorPath)
	}
	vectors := buildInteropVectors(t)
	// Preserve the Rust half when the harness has already sealed its capsule.
	if raw, err := os.ReadFile(filepath.Clean(interopVectorPath)); err == nil {
		var existing interopVectors
		if json.Unmarshal(raw, &existing) == nil {
			vectors.Capsule.RustEncapsulationHex = existing.Capsule.RustEncapsulationHex
			vectors.Capsule.RustCiphertextHex = existing.Capsule.RustCiphertextHex
			vectors.Capsule.RustRecipientPublicKey = existing.Capsule.RustRecipientPublicKey
		}
	}
	encoded, err := json.MarshalIndent(vectors, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(interopVectorPath), 0o755))
	require.NoError(t, os.WriteFile(interopVectorPath, append(encoded, '\n'), 0o600))
}
