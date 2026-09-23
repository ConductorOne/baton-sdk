package vaultinbox

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"filippo.io/hpke"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

// vectorSeed is the fixed 0x42 X-Wing seed the existing Rust service-principal
// vector also uses, so one synthetic recipient covers both interop fixtures.
var vectorSeed = bytes.Repeat([]byte{0x42}, 32)

const (
	vectorTenant     = "tenant-vector"
	vectorVault      = "vault-vector"
	vectorInboxKey   = "inbox-key-vector"
	vectorGeneration = 7
	vectorScheme     = "latchkey.vault_submission.secret.v1"
	vectorSubmission = "vault-submission-vector"
	vectorValue      = "datalog-api-key-value"
	vectorName       = "api_key"
	vectorDesc       = "Datadog organization API key"
)

func vectorPrivateKey(t *testing.T) hpke.PrivateKey {
	t.Helper()
	key, err := hpke.MLKEM768X25519().NewPrivateKey(append([]byte(nil), vectorSeed...))
	require.NoError(t, err)
	return key
}

func publicJWKFor(key hpke.PrivateKey) string {
	return `{"kty":"` + jwkKtyAKP + `","alg":"` + jwkAlg + `","pub":"` +
		base64.RawURLEncoding.EncodeToString(key.PublicKey().Bytes()) + `"}`
}

// recipientParams is the whole vault-inbox config as one mutable value: the
// EncryptionConfig fields plus the members of the recipient JWK and its
// baton_vault_inbox extension. A test builds one, then changes exactly one
// member so each rejection case is isolated.
type recipientParams struct {
	Provider string
	KeyID    string

	Kty string
	Alg string
	Pub string

	Version             int
	Suite               string
	TenantID            string
	VaultBoundaryID     string
	KeyGeneration       int
	PayloadScheme       string
	SubmissionID        string
	PublicKeyThumbprint string
	ContentType         string

	// RawJWK overrides the generated document when a case must control the
	// exact bytes; OuterExtra and ExtensionExtra add members beyond the ones the
	// profile defines, so malformed and oversized shapes need no second builder.
	RawJWK         string
	OuterExtra     map[string]any
	ExtensionExtra map[string]any
}

func (p recipientParams) jwk() string {
	if p.RawJWK != "" {
		return p.RawJWK
	}
	ext := map[string]any{
		"version":               p.Version,
		"suite":                 p.Suite,
		"tenant_id":             p.TenantID,
		"vault_boundary_id":     p.VaultBoundaryID,
		"key_generation":        p.KeyGeneration,
		"payload_scheme":        p.PayloadScheme,
		"submission_id":         p.SubmissionID,
		"public_key_thumbprint": p.PublicKeyThumbprint,
		"content_type":          p.ContentType,
	}
	for name, value := range p.ExtensionExtra {
		ext[name] = value
	}
	outer := map[string]any{
		"kty":              p.Kty,
		"alg":              p.Alg,
		"pub":              p.Pub,
		JWKExtensionMember: ext,
	}
	for name, value := range p.OuterExtra {
		outer[name] = value
	}
	encoded, err := json.Marshal(outer)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func (p recipientParams) config() *v2.EncryptionConfig {
	return v2.EncryptionConfig_builder{
		Provider: p.Provider,
		KeyId:    p.KeyID,
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
			PubKey: []byte(p.jwk()),
		}.Build(),
	}.Build()
}

// vectorParams is the fully valid config every rejection case starts from.
func vectorParams(t *testing.T) recipientParams {
	t.Helper()
	key := vectorPrivateKey(t)
	pub := base64.RawURLEncoding.EncodeToString(key.PublicKey().Bytes())
	thumbprint, err := canonicalThumbprint(jwkAlg, jwkKtyAKP, pub)
	require.NoError(t, err)
	return recipientParams{
		Provider:            EncryptionProvider,
		KeyID:               vectorInboxKey,
		Kty:                 jwkKtyAKP,
		Alg:                 jwkAlg,
		Pub:                 pub,
		Version:             JWKExtensionVersion,
		Suite:               SuiteLabel,
		TenantID:            vectorTenant,
		VaultBoundaryID:     vectorVault,
		KeyGeneration:       vectorGeneration,
		PayloadScheme:       vectorScheme,
		SubmissionID:        vectorSubmission,
		PublicKeyThumbprint: thumbprint,
	}
}

// configFor builds a fully valid config, then hands the caller the chance to
// mutate it so each rejection case differs in exactly one field.
func configFor(t *testing.T, mutate func(*recipientParams)) *v2.EncryptionConfig {
	t.Helper()
	params := vectorParams(t)
	if mutate != nil {
		mutate(&params)
	}
	return params.config()
}

func vectorConfig(t *testing.T) *v2.EncryptionConfig {
	t.Helper()
	return configFor(t, nil)
}

func vectorPlaintext() *v2.PlaintextData {
	return v2.PlaintextData_builder{
		Name:        vectorName,
		Description: vectorDesc,
		Bytes:       []byte(vectorValue),
	}.Build()
}

// recipientFor builds the binding context straight from the parameters. It lets
// a tamper case set coordinates the parser itself would refuse, which is the
// point: bindingBytes depends on the values, not on where they were parsed from.
func recipientFor(p recipientParams) *recipient {
	return &recipient{
		TenantID:            p.TenantID,
		VaultBoundaryID:     p.VaultBoundaryID,
		InboxKeyID:          p.KeyID,
		KeyGeneration:       uint64(p.KeyGeneration),
		PayloadScheme:       p.PayloadScheme,
		SubmissionID:        p.SubmissionID,
		PublicKeyThumbprint: p.PublicKeyThumbprint,
		ContentType:         p.ContentType,
	}
}

// expectedPayloadJSON is the byte-for-byte SecretSubmissionPayloadV3 the
// Latchkey open path parses. Keys are the Rust serde field names in declaration
// order; content_type normalizes to "generic"; annotations is always present.
const expectedPayloadJSON = `{"version":3,"submission_id":"vault-submission-vector",` +
	`"display_name":"api_key","description":"Datadog organization API key",` +
	`"content_type":"generic","annotations":{},` +
	`"value_b64":"ZGF0YWxvZy1hcGkta2V5LXZhbHVl"}`

func TestEncryptProducesTheVaultInboxEnvelope(t *testing.T) {
	ctx := context.Background()
	config := vectorConfig(t)

	encrypted, err := NewProvider().Encrypt(ctx, config, vectorPlaintext())
	require.NoError(t, err)
	require.Equal(t, EncryptionProvider, encrypted.GetProvider())
	require.Equal(t, []string{vectorInboxKey}, encrypted.GetKeyIds())
	require.Empty(t, encrypted.GetKeyId(), "the deprecated single key_id stays unset")
	require.Equal(t, vectorName, encrypted.GetName())
	require.Equal(t, vectorDesc, encrypted.GetDescription())

	var envelope struct {
		Version    uint8  `json:"version"`
		Alg        string `json:"alg"`
		Enc        string `json:"enc"`
		Ciphertext string `json:"ciphertext"`
	}
	require.NoError(t, json.Unmarshal(encrypted.GetEncryptedBytes(), &envelope))
	require.Equal(t, uint8(envelopeVersion), envelope.Version)
	require.Equal(t, jwkAlg, envelope.Alg)

	// The sealed plaintext is exactly the container the Rust decoder accepts.
	opened := openVectorEnvelope(t, config, envelope.Enc, envelope.Ciphertext)
	require.Equal(t, expectedPayloadJSON, string(opened))

	var payload struct {
		Version      uint8             `json:"version"`
		SubmissionID string            `json:"submission_id"`
		DisplayName  string            `json:"display_name"`
		ContentType  string            `json:"content_type"`
		ValueB64     string            `json:"value_b64"`
		Annotations  map[string]string `json:"annotations"`
	}
	require.NoError(t, json.Unmarshal(opened, &payload))
	require.Equal(t, uint8(payloadVersionV3), payload.Version)
	require.Equal(t, vectorSubmission, payload.SubmissionID)
	require.Equal(t, contentTypeGeneric, payload.ContentType)
	require.Empty(t, payload.Annotations)
	require.Equal(t, vectorValue, string(mustDecodeB64(t, payload.ValueB64)))
}

// TestTwoSealsDiffer pins that the encapsulation is randomized: the same
// plaintext sealed twice to the same recipient must not produce the same
// ciphertext, or an observer could tell two identical submissions apart.
func TestTwoSealsDiffer(t *testing.T) {
	ctx := context.Background()
	config := vectorConfig(t)
	first, err := NewProvider().Encrypt(ctx, config, vectorPlaintext())
	require.NoError(t, err)
	second, err := NewProvider().Encrypt(ctx, config, vectorPlaintext())
	require.NoError(t, err)
	require.NotEqual(t, first.GetEncryptedBytes(), second.GetEncryptedBytes())
	require.Equal(t, expectedPayloadJSON, string(openVectorEnvelope(t, config,
		mustEnvelope(t, first).Enc, mustEnvelope(t, first).Ciphertext)))
}

// TestEveryBindingFieldIsAuthenticated flips one AAD field at a time. Each
// mismatch must fail the AEAD open, which is what stops a server from moving a
// ciphertext to another tenant, vault, inbox key, generation, or scheme.
func TestEveryBindingFieldIsAuthenticated(t *testing.T) {
	ctx := context.Background()
	config := vectorConfig(t)
	encrypted, err := NewProvider().Encrypt(ctx, config, vectorPlaintext())
	require.NoError(t, err)
	var envelope struct {
		Enc        string `json:"enc"`
		Ciphertext string `json:"ciphertext"`
	}
	require.NoError(t, json.Unmarshal(encrypted.GetEncryptedBytes(), &envelope))

	mutate := map[string]func(*recipientParams){
		"tenant":         func(p *recipientParams) { p.TenantID = "tenant-other" },
		"vault":          func(p *recipientParams) { p.VaultBoundaryID = "vault-other" },
		"inbox_key_id":   func(p *recipientParams) { p.KeyID = "inbox-key-other" },
		"key_generation": func(p *recipientParams) { p.KeyGeneration = vectorGeneration + 1 },
		"payload_scheme": func(p *recipientParams) { p.PayloadScheme = "latchkey.vault_submission.secret.v2" },
	}
	for name, edit := range mutate {
		t.Run(name, func(t *testing.T) {
			params := vectorParams(t)
			edit(&params)
			enc, err := base64.RawURLEncoding.DecodeString(envelope.Enc)
			require.NoError(t, err)
			ciphertext, err := base64.RawURLEncoding.DecodeString(envelope.Ciphertext)
			require.NoError(t, err)
			binding := bindingBytes(recipientFor(params))
			recipient, err := hpke.NewRecipient(enc, vectorPrivateKey(t), hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), binding)
			require.NoError(t, err)
			_, openErr := recipient.Open(binding, ciphertext)
			require.Error(t, openErr, "a rebound ciphertext must not open")
		})
	}
}

func TestValidateConfigRejectsUnsupportedProfiles(t *testing.T) {
	lowOrder := configFor(t, func(p *recipientParams) {
		raw := mustDecodeB64(t, p.Pub)
		copy(raw[len(raw)-32:], make([]byte, 32))
		p.Pub = base64.RawURLEncoding.EncodeToString(raw)
		thumbprint, err := canonicalThumbprint(p.Alg, p.Kty, p.Pub)
		require.NoError(t, err)
		p.PublicKeyThumbprint = thumbprint
	})

	unknownFields := vectorConfig(t)
	// Unknown fields on the provider-specific config are refused: its contents
	// are frozen into the HPKE binding.
	unknownFields.GetJwkPublicKeyConfig().ProtoReflect().SetUnknown([]byte{0x80, 0x7c, 0x01})
	// Unknown fields on the shared EncryptionConfig are tolerated so the message
	// stays additive for every other provider.
	extensibleConfig := vectorConfig(t)
	extensibleConfig.ProtoReflect().SetUnknown([]byte{0x80, 0x7c, 0x01})

	missingJWK := vectorConfig(t)
	missingJWK.ClearJwkPublicKeyConfig()

	cases := map[string]*v2.EncryptionConfig{
		"nil config":         nil,
		"missing jwk config": missingJWK,
		"wrong provider":     withProvider(t, "baton/age/v1"),
		"empty provider":     withProvider(t, ""),
		"unknown fields":     unknownFields,
		"extension version unset": configFor(t, func(p *recipientParams) {
			p.Version = 0
		}),
		"suite unset":           configFor(t, func(p *recipientParams) { p.Suite = "" }),
		"tenant empty":          configFor(t, func(p *recipientParams) { p.TenantID = "" }),
		"vault empty":           configFor(t, func(p *recipientParams) { p.VaultBoundaryID = "" }),
		"inbox key empty":       configFor(t, func(p *recipientParams) { p.KeyID = "" }),
		"generation zero":       configFor(t, func(p *recipientParams) { p.KeyGeneration = 0 }),
		"scheme empty":          configFor(t, func(p *recipientParams) { p.PayloadScheme = "" }),
		"unsupported scheme":    configFor(t, func(p *recipientParams) { p.PayloadScheme = "latchkey.vault_submission.secret.v2" }),
		"submission id empty":   configFor(t, func(p *recipientParams) { p.SubmissionID = "" }),
		"thumbprint empty":      configFor(t, func(p *recipientParams) { p.PublicKeyThumbprint = "" }),
		"thumbprint mismatch":   configFor(t, func(p *recipientParams) { p.PublicKeyThumbprint = "not-the-thumbprint" }),
		"oversized identifier":  configFor(t, func(p *recipientParams) { p.VaultBoundaryID = strings.Repeat("v", maxIDBytes+1) }),
		"content type too long": configFor(t, func(p *recipientParams) { p.ContentType = strings.Repeat("a", maxContentBytes+1) }),
		"low-order recipient":   lowOrder,
	}
	for name, config := range cases {
		t.Run(name, func(t *testing.T) {
			require.Error(t, NewProvider().ValidateConfig(context.Background(), config))
		})
	}
	require.NoError(t, NewProvider().ValidateConfig(context.Background(), vectorConfig(t)))
	require.NoError(t, NewProvider().ValidateConfig(context.Background(), extensibleConfig))
}

// TestPublicJWKAcceptsOrdinaryJoseMembers pins that the parse is no stricter than
// the thumbprint contract beside it: a served JWK carrying use, key_ops, or a kid
// that agrees with the authoritative key_id re-derives the same thumbprint and
// must be accepted rather than refused as "not a public AKP JWK".
func TestPublicJWKAcceptsOrdinaryJoseMembers(t *testing.T) {
	t.Parallel()
	params := vectorParams(t)
	var parsed map[string]any
	require.NoError(t, json.Unmarshal([]byte(params.jwk()), &parsed))
	parsed["kid"] = vectorInboxKey
	parsed["use"] = "enc"
	parsed["key_ops"] = []string{"deriveKey"}
	withMembers, err := json.Marshal(parsed)
	require.NoError(t, err)

	require.NoError(t, NewProvider().ValidateConfig(context.Background(), configFor(t, func(p *recipientParams) {
		p.RawJWK = string(withMembers)
	})))
}

// TestThumbprintIgnoresTheExtension pins C3: the inbox thumbprint is the canonical
// {alg,kty,pub} digest, so changing the binding context inside the extension
// cannot move it. A config whose coordinates differ but whose thumbprint is the
// canonical one still validates; if the thumbprint covered the extension, the
// change would have made it stale and refused.
func TestThumbprintIgnoresTheExtension(t *testing.T) {
	t.Parallel()
	params := vectorParams(t)
	thumbprint, err := canonicalThumbprint(params.Alg, params.Kty, params.Pub)
	require.NoError(t, err)
	require.Equal(t, params.PublicKeyThumbprint, thumbprint)

	require.NoError(t, NewProvider().ValidateConfig(context.Background(), configFor(t, func(p *recipientParams) {
		p.TenantID = "tenant-different"
		p.VaultBoundaryID = "vault-different"
		p.SubmissionID = "submission-different"
		p.KeyGeneration = 99
	})))
}

// TestValidateConfigRefusesAConflictingKid pins C7: key_id is the single
// authoritative inbox key id, so a JWK whose kid names a different key is
// refused rather than silently overridden.
func TestValidateConfigRefusesAConflictingKid(t *testing.T) {
	t.Parallel()
	conflicting := configFor(t, func(p *recipientParams) {
		var parsed map[string]any
		require.NoError(t, json.Unmarshal([]byte(p.jwk()), &parsed))
		parsed["kid"] = "some-other-key"
		encoded, err := json.Marshal(parsed)
		require.NoError(t, err)
		p.RawJWK = string(encoded)
	})
	require.Error(t, NewProvider().ValidateConfig(context.Background(), conflicting),
		"a kid that disagrees with key_id must be refused")
	require.NoError(t, NewProvider().ValidateConfig(context.Background(), vectorConfig(t)))
}

func withProvider(t *testing.T, name string) *v2.EncryptionConfig {
	t.Helper()
	return configFor(t, func(p *recipientParams) { p.Provider = name })
}

func TestValidateConfigRejectsMalformedJWK(t *testing.T) {
	cases := map[string]func(*recipientParams){
		"not json":  func(p *recipientParams) { p.RawJWK = "not-json" },
		"wrong kty": func(p *recipientParams) { p.Kty = "EC" },
		"wrong alg": func(p *recipientParams) { p.Alg = "HPKE-Base-X-Wing-Draft06Obsolete" },
		"private present": func(p *recipientParams) {
			p.OuterExtra = map[string]any{"priv": "AAAA"}
		},
		"pub empty": func(p *recipientParams) { p.Pub = "" },
		"pub short": func(p *recipientParams) { p.Pub = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 100)) },
		"pub oversized": func(p *recipientParams) {
			p.Pub = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, PublicKeyBytes+1))
		},
		"pub not base64": func(p *recipientParams) { p.Pub = "!!!!" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			require.Error(t, NewProvider().ValidateConfig(context.Background(), configFor(t, mutate)))
		})
	}
}

// TestValidateConfigRejectsMalformedExtension covers the extension as a protocol
// surface: an absent, malformed, ambiguous, unknown-membered, wrong-version, or
// incomplete extension is refused before any provider work.
func TestValidateConfigRejectsMalformedExtension(t *testing.T) {
	params := vectorParams(t)
	extensionJSON := `{"version":` + strconv.Itoa(JWKExtensionVersion) + `,"suite":"` + SuiteLabel +
		`","tenant_id":"` + vectorTenant + `","vault_boundary_id":"` + vectorVault +
		`","key_generation":` + strconv.Itoa(vectorGeneration) + `,"payload_scheme":"` + vectorScheme +
		`","submission_id":"` + vectorSubmission + `","public_key_thumbprint":"` + params.PublicKeyThumbprint + `"}`
	outer := func(body string) string {
		return `{"kty":"` + jwkKtyAKP + `","alg":"` + jwkAlg + `","pub":"` + params.Pub + `",` + body + `}`
	}

	// Two members with the same name: Go's decoder keeps the last one, so two
	// readers of the same bytes could disagree about which value it meant. Both
	// values are individually valid, so duplicate detection is the only guard
	// that can refuse these.
	duplicateOuter := params
	duplicateOuter.RawJWK = `{"kty":"` + jwkKtyAKP + `","alg":"` + jwkAlg + `","pub":"` + params.Pub +
		`","kty":"` + jwkKtyAKP + `","` + JWKExtensionMember + `":` + extensionJSON + `}`

	duplicateExtension := params
	duplicateExtension.RawJWK = outer(`"` + JWKExtensionMember + `":{"version":1,"version":1,"suite":"` +
		SuiteLabel + `","tenant_id":"` + vectorTenant + `","vault_boundary_id":"` + vectorVault +
		`","key_generation":7,"payload_scheme":"` + vectorScheme + `","submission_id":"` + vectorSubmission +
		`","public_key_thumbprint":"` + params.PublicKeyThumbprint + `"}`)

	// A second value after the object could carry context the parser did not read.
	trailingOuter := params
	trailingOuter.RawJWK = outer(`"`+JWKExtensionMember+`":`+extensionJSON) + ` {"kty":"AKP"}`

	withoutExtensionRaw := params
	withoutExtensionRaw.RawJWK = `{"kty":"` + jwkKtyAKP + `","alg":"` + jwkAlg + `","pub":"` + params.Pub + `"}`

	extensionNotObject := params
	extensionNotObject.RawJWK = outer(`"` + JWKExtensionMember + `":"not-an-object"`)

	cases := map[string]*v2.EncryptionConfig{
		"extension absent":        withoutExtensionRaw.config(),
		"extension not an object": extensionNotObject.config(),
		"duplicate outer member":  duplicateOuter.config(),
		"duplicate ext member":    duplicateExtension.config(),
		"trailing outer content":  trailingOuter.config(),
		"unknown ext member":      configFor(t, func(p *recipientParams) { p.ExtensionExtra = map[string]any{"extra": 1} }),
		"version 2":               configFor(t, func(p *recipientParams) { p.Version = JWKExtensionVersion + 1 }),
		"wrong suite":             configFor(t, func(p *recipientParams) { p.Suite = "HPKE-Base-X-Wing-Draft06Obsolete-HKDF-SHA256-ChaCha20Poly1305" }),
		"missing tenant":          configFor(t, func(p *recipientParams) { p.TenantID = "" }),
		"missing vault":           configFor(t, func(p *recipientParams) { p.VaultBoundaryID = "" }),
		"missing submission":      configFor(t, func(p *recipientParams) { p.SubmissionID = "" }),
		"missing thumbprint":      configFor(t, func(p *recipientParams) { p.PublicKeyThumbprint = "" }),
		"missing generation":      configFor(t, func(p *recipientParams) { p.KeyGeneration = 0 }),
		"missing payload scheme":  configFor(t, func(p *recipientParams) { p.PayloadScheme = "" }),
		"non-string version":      configFor(t, func(p *recipientParams) { p.ExtensionExtra = map[string]any{"version": "1"} }),
		"null tenant":             configFor(t, func(p *recipientParams) { p.ExtensionExtra = map[string]any{"tenant_id": nil} }),
		"non-string alg":          configFor(t, func(p *recipientParams) { p.OuterExtra = map[string]any{"alg": 1} }),
		"null pub":                configFor(t, func(p *recipientParams) { p.OuterExtra = map[string]any{"pub": nil} }),
		// A null kid is refused by the explicit null guard; every other null
		// member would also be refused downstream by its empty-value check, so
		// this is the case that isolates the guard.
		"null kid": configFor(t, func(p *recipientParams) { p.OuterExtra = map[string]any{"kid": nil} }),
	}
	for name, config := range cases {
		t.Run(name, func(t *testing.T) {
			require.Error(t, NewProvider().ValidateConfig(context.Background(), config))
		})
	}

	// parseExtension's own trailing-content guard is not reachable through
	// parseJWKDocument, because json.RawMessage captures exactly one value. Pin
	// the guard directly so a second extension value can never be silently
	// interpreted as context.
	_, err := parseExtension(json.RawMessage(extensionJSON + ` {"version":1}`))
	require.Error(t, err, "content after the extension object must be refused")
}

// TestOversizedPublicJWKIsRejected pins the size bound with a JWK that is valid
// in every other respect. A non-JSON string would be refused by the parse
// regardless of the bound, so this padding is an extra member on a real key: the
// thumbprint still matches and the key still parses, leaving the bound as the
// only thing that can refuse it.
func TestOversizedPublicJWKIsRejected(t *testing.T) {
	t.Parallel()
	params := vectorParams(t)
	params.OuterExtra = map[string]any{"padding": strings.Repeat("p", maxJWKBytes)}
	require.Greater(t, len(params.jwk()), maxJWKBytes)

	require.Error(t, NewProvider().ValidateConfig(context.Background(), params.config()),
		"a JWK over the bound must be refused even when it is otherwise valid")

	// The same key at the bound is accepted, so the refusal is the size and not
	// the padding member.
	require.NoError(t, NewProvider().ValidateConfig(context.Background(), vectorConfig(t)))
}

func TestEncryptRejectsUnusablePlaintext(t *testing.T) {
	ctx := context.Background()
	config := vectorConfig(t)
	for name, plaintext := range map[string]*v2.PlaintextData{
		"nil":      nil,
		"no name":  v2.PlaintextData_builder{Bytes: []byte("v")}.Build(),
		"no bytes": v2.PlaintextData_builder{Name: "api_key"}.Build(),
		"too large": v2.PlaintextData_builder{
			Name:  "api_key",
			Bytes: bytes.Repeat([]byte("a"), MaxPlaintextBytes+1),
		}.Build(),
		"oversized name": v2.PlaintextData_builder{
			Name:  strings.Repeat("n", maxNameBytes+1),
			Bytes: []byte("v"),
		}.Build(),
		"oversized description": v2.PlaintextData_builder{
			Name:        "api_key",
			Description: strings.Repeat("d", maxDescriptionBytes+1),
			Bytes:       []byte("v"),
		}.Build(),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewProvider().Encrypt(ctx, config, plaintext)
			require.Error(t, err)
		})
	}
}

func TestPublicKeyThumbprintMatchesLatchkeyVector(t *testing.T) {
	// The Latchkey proto documents this exact vector: pub "AQID" hashes the
	// canonical {"alg","kty","pub"} object to this digest.
	thumbprint, err := canonicalThumbprint(jwkAlg, jwkKtyAKP, "AQID")
	require.NoError(t, err)
	require.Equal(t, "YHoF2Vc1mqHO84GuAfiFVAX5UwAFBFJFitEkeGRz-9E", thumbprint)
}

func TestLowOrderX25519PublicKeyFailsTheEcdhProbe(t *testing.T) {
	raw := bytes.Repeat([]byte{1}, PublicKeyBytes)
	copy(raw[len(raw)-32:], make([]byte, 32))
	probe, err := ecdh.X25519().NewPrivateKey(make([]byte, 32))
	require.NoError(t, err)
	x25519, err := ecdh.X25519().NewPublicKey(raw[len(raw)-32:])
	require.NoError(t, err)
	_, err = probe.ECDH(x25519)
	require.Error(t, err)
}

// pinnedBindingHex is the binding this profile must produce, transcribed from
// the Latchkey source (`binding_bytes` over `framed` with INFO_PREFIX, version 2,
// the suite string, tenant, vault, inbox key id, the ASCII-decimal generation,
// and the payload scheme) rather than read back out of this package. It fails if
// the framing or the field order drifts in a way `Encrypt` and the local opener
// would otherwise agree on.
const pinnedBindingHex = "000000276c617463686b65792f76312f7661756c742d696e626f782d7375626d697373696f6e2f696e666f" +
	"0000000102" +
	"0000003548504b452d426173652d582d57696e672d447261667430362d484b44462d5348413235362d4368614368613230506f6c7931333035" +
	"0000000d74656e616e742d766563746f72" +
	"0000000c7661756c742d766563746f72" +
	"00000010696e626f782d6b65792d766563746f72" +
	"0000000137" +
	"000000236c617463686b65792e7661756c745f7375626d697373696f6e2e7365637265742e7631"

// TestBindingBytesMatchTheLatchkeyFraming pins the AAD bytes themselves. The
// cross-language proof lives in the Rust repo (see docs/vault-inbox-delivery.md);
// this is the in-repo guard that the framing cannot drift unnoticed. It runs the
// real parser, so it also pins that the JWK extension yields these coordinates.
func TestBindingBytesMatchTheLatchkeyFraming(t *testing.T) {
	rec, _, err := recipientFromConfig(vectorConfig(t))
	require.NoError(t, err)
	require.Equal(t, pinnedBindingHex, hex.EncodeToString(bindingBytes(rec)))
}

func openVectorEnvelope(t *testing.T, config *v2.EncryptionConfig, enc, ciphertext string) []byte {
	t.Helper()
	rawEnc, err := base64.RawURLEncoding.DecodeString(enc)
	require.NoError(t, err)
	rawCiphertext, err := base64.RawURLEncoding.DecodeString(ciphertext)
	require.NoError(t, err)
	rec, _, err := recipientFromConfig(config)
	require.NoError(t, err)
	binding := bindingBytes(rec)
	recipient, err := hpke.NewRecipient(rawEnc, vectorPrivateKey(t), hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), binding)
	require.NoError(t, err)
	plaintext, err := recipient.Open(binding, rawCiphertext)
	require.NoError(t, err)
	return plaintext
}

func mustDecodeB64(t *testing.T, value string) []byte {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(value)
	require.NoError(t, err)
	return raw
}

// mustEnvelope decodes the sealed submission envelope out of an EncryptedData.
func mustEnvelope(t *testing.T, encrypted *v2.EncryptedData) submissionEnvelope {
	t.Helper()
	var envelope submissionEnvelope
	require.NoError(t, json.Unmarshal(encrypted.GetEncryptedBytes(), &envelope))
	return envelope
}

func jwkPub(t *testing.T, jwkJSON string) string {
	t.Helper()
	var jwk struct {
		Pub string `json:"pub"`
	}
	require.NoError(t, json.Unmarshal([]byte(jwkJSON), &jwk))
	return jwk.Pub
}
