package udpop

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

var ErrProofInvalid = errors.New("dpop: proof is invalid")

var emptySha256AndBase64URLEncodedHash = sha256AndBase64UrlEncode(nil)

type DPoPAccessTokenValidationConfig struct {
	AccessToken                  string
	AccessTokenConfirmationClaim map[string]string
}

type DPoPValidationConfig struct {
	Method                 string
	URI                    *url.URL
	AccessToken            *DPoPAccessTokenValidationConfig
	Nonce                  string
	IssuedAtWithinDuration time.Duration
	AllowedAlgorithms      []jose.SignatureAlgorithm
}

// ValidateJWKThumbprintConfirmation validates if your proof public key is correctly bound to your access token as described in section 6.1 - JWK Thumbprint Confirmation Method
// It checks that the cnf/jkt claim exists and matches the public key contained in the provided dpop proof.
/*
{
  "sub":"someone@example.com",
  "iss":"https://server.example.com",
  "nbf":1562262611,
  "exp":1562266216,
  "cnf":
  {
    "jkt":"0ZcOCORZNYy-DWpqq30jZyJGHTN0d2HglBV3uiguA4I"
  }
}
*/

// Validate validates any DPoP headers according to https://datatracker.ietf.org/doc/html/rfc9449#name-checking-dpop-proofs
// Any errors or validation errors return an error.
// If validation is successful, it returns the raw serialized claims and the *jose.JWK representing the public key from the proof.
func Validate(ctx context.Context, headers http.Header, config *DPoPValidationConfig) (*jose.JSONWebKey, error) {
	// Validate our config makes sense
	if config == nil {
		return nil, fmt.Errorf("%w: config is required", ErrProofInvalid)
	}
	if config.Method == "" {
		return nil, fmt.Errorf("%w: config.Method is required", ErrProofInvalid)
	}
	if config.URI == nil {
		return nil, fmt.Errorf("%w: config.URI is required", ErrProofInvalid)
	}
	if config.IssuedAtWithinDuration <= 0 || config.IssuedAtWithinDuration >= time.Minute*5 {
		return nil, fmt.Errorf("%w: config.IssuedAtWithinDuration must be greater than 0 and less than 5m", ErrProofInvalid)
	}
	if len(config.AllowedAlgorithms) != 1 || config.AllowedAlgorithms[0] != jose.EdDSA {
		return nil, fmt.Errorf("%w: config.AllowedAlgorithms must be exactly [EdDSA]", ErrProofInvalid)
	}
	allowedAlgorithmMap := map[jose.SignatureAlgorithm]struct{}{}
	for _, alg := range config.AllowedAlgorithms {
		allowedAlgorithmMap[alg] = struct{}{}
	}

	now := time.Now()

	// 1. Ensure there is not more than one DPoP HTTP request header field.
	proof := ""
	for key, header := range headers {
		if strings.ToLower(key) != "dpop" {
			continue
		}
		if len(header) != 1 {
			return nil, fmt.Errorf("%w: multiple dpop headers found", ErrProofInvalid)
		}
		if proof != "" {
			return nil, fmt.Errorf("%w: multiple dpop headers found", ErrProofInvalid)
		}
		proof = header[0]
	}
	if proof == "" {
		return nil, fmt.Errorf("%w: dpop header not found", ErrProofInvalid)
	}

	// 2. Ensure the DPoP HTTP request header field value is a well-formed JWT.
	token, err := jwt.ParseSigned(proof, config.AllowedAlgorithms)
	if err != nil {
		return nil, fmt.Errorf("%w: failed parsing dpop proof: %w", ErrProofInvalid, err)
	}
	if len(token.Headers) != 1 {
		return nil, fmt.Errorf("%w: invalid number of proof jwt headers", ErrProofInvalid)
	}
	tokenHeader := token.Headers[0]

	// 4. The typ JOSE Header Parameter has the value dpop+jwt.
	if tokenHeader.ExtraHeaders["typ"] != "dpop+jwt" {
		return nil, fmt.Errorf("%w: header: invalid typ", ErrProofInvalid)
	}

	// 5. The alg JOSE Header Parameter indicates a registered asymmetric digital signature algorithm [IANA.JOSE.ALGS],
	// is not none, is supported by the application, and is acceptable per local policy.
	if tokenHeader.Algorithm == "" {
		return nil, fmt.Errorf("%w: header: missing alg", ErrProofInvalid)
	}
	if strings.ToLower(tokenHeader.Algorithm) == "none" {
		return nil, fmt.Errorf("%w: header: alg must not be 'none'", ErrProofInvalid)
	}
	_, ok := allowedAlgorithmMap[jose.SignatureAlgorithm(tokenHeader.Algorithm)]
	if !ok {
		return nil, fmt.Errorf("%w: header: unsupported alg: %s", ErrProofInvalid, tokenHeader.Algorithm)
	}

	proofPubkey := tokenHeader.JSONWebKey
	if proofPubkey == nil {
		return nil, fmt.Errorf("%w: header: missing jwk", ErrProofInvalid)
	}
	// 7. The jwk JOSE Header Parameter does not contain a private key.
	if !proofPubkey.IsPublic() {
		return nil, fmt.Errorf("%w: header: jwk is not a pubkey", ErrProofInvalid)
	}
	switch proofPubkey.Key.(type) {
	case ed25519.PublicKey:
	default:
		return nil, fmt.Errorf("%w: header: jwk is an unsupported type", ErrProofInvalid)
	}

	// 6. The JWT signature verifies with the public key contained in the jwk JOSE Header Parameter.
	claims := &DPoPClaims{}
	err = token.Claims(proofPubkey, claims)
	if err != nil {
		return nil, fmt.Errorf("%w: claims: failed to verify claims: %w", ErrProofInvalid, err)
	}

	// 3. Validate all required claims per Section 4.2. (jti, htm, htu, iat, ath, nonce)
	if claims.ID == "" {
		return nil, fmt.Errorf("%w: claims: missing jti", ErrProofInvalid)
	}

	// 8. The htm claim matches the HTTP method of the current request.
	switch claims.Htm {
	case "":
		return nil, fmt.Errorf("%w: claims: missing htm", ErrProofInvalid)
	case http.MethodGet, http.MethodPost, http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodHead, http.MethodPut, http.MethodPatch:
	default:
		return nil, fmt.Errorf("%w: claims: malformed htm: %s", ErrProofInvalid, claims.Htm)
	}
	if claims.Htm != config.Method {
		return nil, fmt.Errorf("%w: claims: invalid htm", ErrProofInvalid)
	}

	// 9. The htu claim matches the HTTP URI value for the HTTP request in which the JWT was received, ignoring any query and fragment parts.
	// To reduce the likelihood of false negatives, servers SHOULD employ syntax-based normalization (Section 6.2.2 of [RFC3986])
	// and scheme-based normalization (Section 6.2.3 of [RFC3986]) before comparing the htu claim.
	if claims.Htu == "" {
		return nil, fmt.Errorf("%w: claims: missing htu", ErrProofInvalid)
	}
	htu, err := url.Parse(claims.Htu)
	if err != nil {
		return nil, fmt.Errorf("%w: claims: malformed htu: %w", ErrProofInvalid, err)
	}

	// Normalize URLs for comparison using RFC3986 defined strategies.
	normalize := func(u *url.URL) string {
		uCopy := *u
		uCopy.Scheme = strings.ToLower(uCopy.Scheme)
		uCopy.Host = strings.ToLower(uCopy.Host)
		uCopy.RawQuery = ""
		uCopy.Fragment = ""
		if uCopy.Path == "" {
			uCopy.Path = "/"
		}
		return uCopy.String()
	}

	normalizedHtu := normalize(htu)
	normalizedConfigURI := normalize(config.URI)

	if normalizedHtu != normalizedConfigURI {
		return nil, fmt.Errorf("%w: claims: invalid htu", ErrProofInvalid)
	}

	// 10. If the server provided a nonce value to the client, the nonce claim matches the server-provided nonce value.
	if claims.Nonce != "" {
		if config.Nonce == "" {
			return nil, fmt.Errorf("%w: config.Nonce is required", ErrProofInvalid)
		}
		if claims.Nonce != config.Nonce {
			return nil, fmt.Errorf("%w: claims: invalid nonce", ErrProofInvalid)
		}
	}
	// conversely, if the nonce claim is missing we must not expect it
	if claims.Nonce == "" && config.Nonce != "" {
		return nil, fmt.Errorf("%w: claims: missing nonce", ErrProofInvalid)
	}

	// 11. The creation time of the JWT, as determined by either the iat claim or a server managed timestamp via the nonce claim,
	// is within an acceptable window (see Section 11.1).
	// NOTE(morgabra/kans): We require iat.
	if claims.IssuedAt == nil {
		return nil, fmt.Errorf("%w: claims: missing iat", ErrProofInvalid)
	}
	iat := claims.IssuedAt.Time()
	if iat.Before(now.Add(-config.IssuedAtWithinDuration)) {
		return nil, fmt.Errorf("%w: claims: invalid iat, before expected range", ErrProofInvalid)
	}
	if iat.After(now.Add(config.IssuedAtWithinDuration)) {
		return nil, fmt.Errorf("%w: claims: invalid iat, after expected range", ErrProofInvalid)
	}

	// 12. If presented to a protected resource in conjunction with an access token, ensure that the value of the ath claim equals
	// the hash of that access token, and confirm that the public key to which the access token is bound matches the public key from the DPoP proof.
	if claims.Ath != "" {
		// Disallow the 'empty' hash.
		if claims.Ath == emptySha256AndBase64URLEncodedHash {
			return nil, fmt.Errorf("%w: claims: invalid ath", ErrProofInvalid)
		}

		// Assert the access token hash matches the claim in the proof.
		if config.AccessToken == nil || config.AccessToken.AccessToken == "" {
			return nil, fmt.Errorf("%w: claims: config.AccessToken is required", ErrProofInvalid)
		}
		if claims.Ath != sha256AndBase64UrlEncode([]byte(config.AccessToken.AccessToken)) {
			return nil, fmt.Errorf("%w: claims: invalid ath", ErrProofInvalid)
		}

		// Then assert that the given access token is bound to the public key given in the proof.
		if config.AccessToken == nil || len(config.AccessToken.AccessTokenConfirmationClaim) == 0 {
			return nil, fmt.Errorf("%w: claims: config.AccessToken.AccessTokenConfirmationClaim is required", ErrProofInvalid)
		}

		jkt, ok := config.AccessToken.AccessTokenConfirmationClaim["jkt"]
		if !ok || jkt == "" {
			return nil, fmt.Errorf("%w: claims: config.AccessToken.AccessTokenConfirmationClaim missing jkt", ErrProofInvalid)
		}
		if jkt == emptySha256AndBase64URLEncodedHash {
			return nil, fmt.Errorf("%w: claims: invalid cnf", ErrProofInvalid)
		}

		pubKeyBytes, err := proofPubkey.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("%w: claims: failed to marshal proof public key: %w", ErrProofInvalid, err)
		}

		if jkt != sha256AndBase64UrlEncode(pubKeyBytes) {
			return nil, fmt.Errorf("%w: proof pubkey does not match bound pubkey", ErrProofInvalid)
		}
	}

	// conversely, if the ath claim is missing we must not expect it
	if claims.Ath == "" && config.AccessToken != nil {
		return nil, fmt.Errorf("%w: claims: missing ath", ErrProofInvalid)
	}

	return proofPubkey, nil
}
