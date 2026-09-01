package appstoreconnect

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	// jwtAudience is the fixed audience claim the App Store Connect API requires.
	jwtAudience = "appstoreconnect-v1"

	// DefaultTokenLifetime is how long a minted token claims to be valid. Apple rejects any
	// token whose lifetime exceeds 20 minutes, so we stay comfortably under that ceiling.
	DefaultTokenLifetime = 15 * time.Minute

	// tokenRefreshWindow is how long before expiry we proactively mint a replacement token, so a
	// request that is dispatched just before the deadline cannot arrive after it. A sync that runs
	// longer than a token lifetime therefore rolls onto a fresh token without failing mid-run.
	tokenRefreshWindow = 2 * time.Minute

	// maxTokenLifetime is Apple's hard ceiling on the exp claim.
	maxTokenLifetime = 20 * time.Minute
)

// ErrInvalidPrivateKey is returned when the configured .p8 material is not a usable ES256 key.
var ErrInvalidPrivateKey = errors.New("baton-appstoreconnect: invalid App Store Connect private key")

// jwtHeader is the JOSE header of an App Store Connect API token.
type jwtHeader struct {
	Algorithm string `json:"alg"`
	KeyID     string `json:"kid"`
	Type      string `json:"typ"`
}

// jwtClaims is the claim set of an App Store Connect API token.
type jwtClaims struct {
	Issuer   string `json:"iss"`
	IssuedAt int64  `json:"iat"`
	Expiry   int64  `json:"exp"`
	Audience string `json:"aud"`
}

// TokenSource mints and caches the short-lived ES256 JWTs that authenticate App Store Connect API
// requests. It is safe for concurrent use: a sync issues requests from several goroutines and every
// one of them needs a token that has not expired yet.
type TokenSource struct {
	keyID    string
	issuerID string
	key      *ecdsa.PrivateKey
	lifetime time.Duration

	// now is swappable so tests can drive expiry deterministically.
	now func() time.Time

	mtx       sync.Mutex
	token     string
	expiresAt time.Time
}

// NewTokenSource builds a TokenSource from the Key ID, Issuer ID and PEM-encoded contents of the
// .p8 private key downloaded from App Store Connect.
func NewTokenSource(keyID, issuerID, privateKeyPEM string) (*TokenSource, error) {
	if strings.TrimSpace(keyID) == "" {
		return nil, fmt.Errorf("baton-appstoreconnect: key id is required")
	}
	if strings.TrimSpace(issuerID) == "" {
		return nil, fmt.Errorf("baton-appstoreconnect: issuer id is required")
	}

	key, err := ParsePrivateKey(privateKeyPEM)
	if err != nil {
		return nil, err
	}

	return &TokenSource{
		keyID:    keyID,
		issuerID: issuerID,
		key:      key,
		lifetime: DefaultTokenLifetime,
		now:      time.Now,
	}, nil
}

// ParsePrivateKey decodes the PEM-encoded contents of an App Store Connect .p8 key file. Apple
// ships PKCS#8, but SEC1 ("EC PRIVATE KEY") blocks are accepted too because customers sometimes
// re-encode the key with openssl before pasting it into a connector config.
func ParsePrivateKey(privateKeyPEM string) (*ecdsa.PrivateKey, error) {
	trimmed := strings.TrimSpace(privateKeyPEM)
	if trimmed == "" {
		return nil, fmt.Errorf("%w: private key is empty", ErrInvalidPrivateKey)
	}

	block, _ := pem.Decode([]byte(trimmed))
	if block == nil {
		return nil, fmt.Errorf("%w: not PEM encoded (expected the contents of the .p8 file)", ErrInvalidPrivateKey)
	}

	var (
		key any
		err error
	)
	switch block.Type {
	case "EC PRIVATE KEY":
		key, err = x509.ParseECPrivateKey(block.Bytes)
	default:
		key, err = x509.ParsePKCS8PrivateKey(block.Bytes)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidPrivateKey, err.Error())
	}

	ecKey, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%w: expected an ECDSA key, got %T", ErrInvalidPrivateKey, key)
	}
	if ecKey.Curve != elliptic.P256() {
		return nil, fmt.Errorf("%w: ES256 requires a P-256 key", ErrInvalidPrivateKey)
	}

	return ecKey, nil
}

// Token returns a cached token, minting a new one when the cached token is missing or close enough
// to expiry that it might not survive the round trip.
func (t *TokenSource) Token() (string, error) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	now := t.now()
	if t.token != "" && now.Add(tokenRefreshWindow).Before(t.expiresAt) {
		return t.token, nil
	}

	lifetime := t.lifetime
	if lifetime <= 0 || lifetime > maxTokenLifetime {
		lifetime = DefaultTokenLifetime
	}

	expiresAt := now.Add(lifetime)
	token, err := t.sign(now, expiresAt)
	if err != nil {
		return "", err
	}

	t.token = token
	t.expiresAt = expiresAt

	return token, nil
}

// sign produces the compact-serialized ES256 JWT for the given validity window.
func (t *TokenSource) sign(issuedAt, expiresAt time.Time) (string, error) {
	header, err := json.Marshal(jwtHeader{Algorithm: "ES256", KeyID: t.keyID, Type: "JWT"})
	if err != nil {
		return "", err
	}

	claims, err := json.Marshal(jwtClaims{
		Issuer:   t.issuerID,
		IssuedAt: issuedAt.Unix(),
		Expiry:   expiresAt.Unix(),
		Audience: jwtAudience,
	})
	if err != nil {
		return "", err
	}

	signingInput := base64URL(header) + "." + base64URL(claims)

	digest := sha256.Sum256([]byte(signingInput))
	r, s, err := ecdsa.Sign(rand.Reader, t.key, digest[:])
	if err != nil {
		return "", fmt.Errorf("baton-appstoreconnect: failed to sign token: %w", err)
	}

	// JWS ES256 signatures are the fixed-width concatenation of R and S, not the ASN.1 encoding
	// that ecdsa.SignASN1 produces. Left-pad each to the 32-byte coordinate size.
	const coordinateSize = 32
	signature := make([]byte, 0, 2*coordinateSize)
	signature = append(signature, leftPad(r.Bytes(), coordinateSize)...)
	signature = append(signature, leftPad(s.Bytes(), coordinateSize)...)

	return signingInput + "." + base64URL(signature), nil
}

// base64URL applies the unpadded base64url encoding that JWS requires.
func base64URL(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

// leftPad zero-extends b on the left to exactly size bytes.
func leftPad(b []byte, size int) []byte {
	if len(b) >= size {
		return b[len(b)-size:]
	}
	out := make([]byte, size)
	copy(out[size-len(b):], b)
	return out
}
