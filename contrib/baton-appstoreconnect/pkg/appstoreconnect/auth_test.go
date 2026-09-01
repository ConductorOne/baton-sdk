package appstoreconnect

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"
)

// testKeyPEM returns a freshly generated P-256 key encoded the way Apple encodes a .p8 file.
func testKeyPEM(t *testing.T) (string, *ecdsa.PrivateKey) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}

	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling key: %v", err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})), key
}

func TestTokenIsAValidES256JWT(t *testing.T) {
	keyPEM, key := testKeyPEM(t)

	source, err := NewTokenSource("KEYID123", "issuer-uuid", keyPEM)
	if err != nil {
		t.Fatalf("NewTokenSource: %v", err)
	}

	issued := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	source.now = func() time.Time { return issued }

	token, err := source.Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 JWT segments, got %d", len(parts))
	}

	var header jwtHeader
	decodeSegment(t, parts[0], &header)
	if header.Algorithm != "ES256" {
		t.Errorf("alg = %q, want ES256", header.Algorithm)
	}
	if header.KeyID != "KEYID123" {
		t.Errorf("kid = %q, want KEYID123", header.KeyID)
	}
	if header.Type != "JWT" {
		t.Errorf("typ = %q, want JWT", header.Type)
	}

	var claims jwtClaims
	decodeSegment(t, parts[1], &claims)
	if claims.Issuer != "issuer-uuid" {
		t.Errorf("iss = %q, want issuer-uuid", claims.Issuer)
	}
	if claims.Audience != jwtAudience {
		t.Errorf("aud = %q, want %q", claims.Audience, jwtAudience)
	}
	if claims.IssuedAt != issued.Unix() {
		t.Errorf("iat = %d, want %d", claims.IssuedAt, issued.Unix())
	}

	// Apple rejects tokens that claim to live longer than 20 minutes.
	lifetime := time.Duration(claims.Expiry-claims.IssuedAt) * time.Second
	if lifetime <= 0 || lifetime > maxTokenLifetime {
		t.Errorf("token lifetime %s is outside (0, %s]", lifetime, maxTokenLifetime)
	}

	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decoding signature: %v", err)
	}
	if len(signature) != 64 {
		t.Fatalf("ES256 signature must be 64 bytes, got %d", len(signature))
	}

	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	r := new(big.Int).SetBytes(signature[:32])
	s := new(big.Int).SetBytes(signature[32:])
	if !ecdsa.Verify(&key.PublicKey, digest[:], r, s) {
		t.Error("signature does not verify against the signing key")
	}
}

// TestSignatureAlwaysFixedWidth guards the R||S padding: ECDSA regularly produces coordinates
// shorter than 32 bytes, and a naive concatenation yields a signature Apple silently rejects.
func TestSignatureAlwaysFixedWidth(t *testing.T) {
	keyPEM, key := testKeyPEM(t)

	source, err := NewTokenSource("KEYID123", "issuer-uuid", keyPEM)
	if err != nil {
		t.Fatalf("NewTokenSource: %v", err)
	}

	for i := 0; i < 200; i++ {
		now := time.Now().Add(time.Duration(i) * time.Hour)
		token, err := source.sign(now, now.Add(DefaultTokenLifetime))
		if err != nil {
			t.Fatalf("sign: %v", err)
		}

		parts := strings.Split(token, ".")
		signature, err := base64.RawURLEncoding.DecodeString(parts[2])
		if err != nil {
			t.Fatalf("decoding signature: %v", err)
		}
		if len(signature) != 64 {
			t.Fatalf("iteration %d: signature is %d bytes, want 64", i, len(signature))
		}

		digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
		r := new(big.Int).SetBytes(signature[:32])
		s := new(big.Int).SetBytes(signature[32:])
		if !ecdsa.Verify(&key.PublicKey, digest[:], r, s) {
			t.Fatalf("iteration %d: signature does not verify", i)
		}
	}
}

func TestTokenIsCachedAndRefreshedBeforeExpiry(t *testing.T) {
	keyPEM, _ := testKeyPEM(t)

	source, err := NewTokenSource("KEYID123", "issuer-uuid", keyPEM)
	if err != nil {
		t.Fatalf("NewTokenSource: %v", err)
	}

	current := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	source.now = func() time.Time { return current }

	first, err := source.Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}

	// Well inside the validity window: the cached token is reused.
	current = current.Add(5 * time.Minute)
	second, err := source.Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if first != second {
		t.Error("expected the cached token to be reused inside the validity window")
	}

	// Inside the refresh window: a long sync must roll onto a fresh token before the old one dies.
	current = current.Add(DefaultTokenLifetime - tokenRefreshWindow)
	third, err := source.Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if third == second {
		t.Error("expected a new token once the cached one entered the refresh window")
	}

	var claims jwtClaims
	decodeSegment(t, strings.Split(third, ".")[1], &claims)
	if claims.IssuedAt != current.Unix() {
		t.Errorf("refreshed token iat = %d, want %d", claims.IssuedAt, current.Unix())
	}
}

func TestParsePrivateKeyRejectsUnusableMaterial(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating RSA key: %v", err)
	}
	rsaDER, err := x509.MarshalPKCS8PrivateKey(rsaKey)
	if err != nil {
		t.Fatalf("marshaling RSA key: %v", err)
	}
	rsaPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: rsaDER}))

	p384Key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatalf("generating P-384 key: %v", err)
	}
	p384DER, err := x509.MarshalPKCS8PrivateKey(p384Key)
	if err != nil {
		t.Fatalf("marshaling P-384 key: %v", err)
	}
	p384PEM := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: p384DER}))

	for _, tc := range []struct {
		name string
		key  string
	}{
		{"empty", "   "},
		{"not pem", "MIGTAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBHkwdwIBAQQg"},
		{"rsa key", rsaPEM},
		{"wrong curve", p384PEM},
		{"truncated pem", "-----BEGIN PRIVATE KEY-----\nnot base64\n-----END PRIVATE KEY-----\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParsePrivateKey(tc.key); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestParsePrivateKeyAcceptsSEC1(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling key: %v", err)
	}
	sec1 := string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}))

	parsed, err := ParsePrivateKey(sec1)
	if err != nil {
		t.Fatalf("ParsePrivateKey: %v", err)
	}
	if parsed.D.Cmp(key.D) != 0 {
		t.Error("parsed a different key than the one encoded")
	}
}

func TestNewTokenSourceRequiresIdentifiers(t *testing.T) {
	keyPEM, _ := testKeyPEM(t)

	if _, err := NewTokenSource("", "issuer", keyPEM); err == nil {
		t.Error("expected an error for a missing key id")
	}
	if _, err := NewTokenSource("key", "  ", keyPEM); err == nil {
		t.Error("expected an error for a missing issuer id")
	}
}

func decodeSegment(t *testing.T, segment string, target any) {
	t.Helper()

	raw, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		t.Fatalf("decoding segment: %v", err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatalf("unmarshaling segment: %v", err)
	}
}
