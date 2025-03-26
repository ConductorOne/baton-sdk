package auth

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testPrivateKeyJWK = `{
		"kty": "RSA",
		"n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW` +
		`2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
		"e": "AQAB",
		"d": "X4cTteJY_gn4FYPsXB8rdXix5vwsg1FLN5E3EaG6RJoVH-HLLKD9M7dx5oo7GURknchnrRweUkC7hT5fJLM0WbFAKNLWY2vv7B6NqXSzUvxT0_YSfqijwp3RTzlBaCxWp4doFk5N2o8Gy_nHNKroADIkJ46pRUohsXywbReAdYaMwFs9tv` +
		`8d_cPVY3i07a3t8MN6TNwm0dSawm9v47UiCl3Sk5ZiG7xojPLu4sbg1U2jx4IBTNBznbJSzFHK66jT8bgkuqsk0GjskDJk19Z4qwjwbsnn4j2WBii3RL-Us2lGVkY8fkFzme1z0HbIkfz0Y6mqnOYtqc0X4jfcKoAC8Q",
		"p": "83i-7IvMGXoMXCskv73TKr8637FiO7Z27zv8oj6pbWUQyLPQBQxtPVnwD20R-60eTDmD2ujnMt5PoqMrm8RfmNhVWDtjjMmCMjOpSXicFHj7XOuVIYQyqVWlWEh6dN36GVZYk93N8Bc9vY41xy8B9RzzOGVQzXvNEvn7O0nVbfs",
		"q": "3dfOR9cuYq-0S-mkFLzgItgMEfFzB2q3hWehMuG0oCuqnb3vobLyumqjVZQO1dIrdwgTnCdpYzBcOfW5r370AFXjiWft_NGEiovonizhKpo9VVS78TzFgxkIdrecRezsZ-1kYd_s1qDbxtkDEgfAITAG9LUnADun4vIcb6yelxk",
		"dp": "G4sPXkc6Ya9y8oJW9_ILj4xuppu0lzi_H7VTkS8xj5SdX3coE0oimYwxIi2emTAue0UOa5dpgFGyBJ4c8tQ2VF402XRugKDTP8akYhFo5tAA77Qe_NmtuYZc3C3m3I24G2GvR5sSDxUyAN2zq8Lfn9EUms6rY3Ob8YeiKkTiBj0",
		"dq": "s9lAH9fggBsoFR8Oac2R_E2gw282rT2kGOAhvIllETE1efrA6huUUvMfBcMpn8lqeW6vzznYY5SSQF7pMdC_agI3nG8Ibp1BUb0JUiraRNqUfLhcQb_d9GF4Dh7e74WbRsobRonujTYN1xCaP6TO61jvWrX-L18txXw494Q_cgk",
		"qi": "GyM_p6JrXySiz1toFgKbWV-JdI3jQ4ypu9rbMWx3rQJBfmt0FoYzgUIZEVFEcOqwemRN81zoDAaa-Bk0KWNGDjJHZDdDmFhW3AN7lI-puxk_mHZGJ11rxyR8O55XLSe3SPmRfKwZI6yU24ZxvQKFYItdldUKGzO6Ia6zTKhAVRU"
	}`

	testPublicKeyJWK = `{
		"kty": "RSA",
		"n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_` +
		`FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
		"e": "AQAB"
	}`
)

func createTestToken(t *testing.T, claims map[string]interface{}) string {
	var privateKey jose.JSONWebKey
	err := json.Unmarshal([]byte(testPrivateKeyJWK), &privateKey)
	require.NoError(t, err)

	// Create a new signer with RS256 algorithm
	opts := &jose.SignerOptions{}
	opts.WithType("JWT")
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: privateKey.Key}, opts)
	require.NoError(t, err)

	// Create a new JWT and sign it
	payload, err := json.Marshal(claims)
	require.NoError(t, err)

	object, err := signer.Sign(payload)
	require.NoError(t, err)

	token, err := object.CompactSerialize()
	require.NoError(t, err)

	return token
}

func TestNewValidator(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: Config{
				PublicKeyJWK: testPublicKeyJWK,
				Issuer:       "test-issuer",
				Subject:      "test-subject",
			},
			wantErr: false,
		},
		{
			name: "invalid JWK",
			config: Config{
				PublicKeyJWK: "invalid",
				Issuer:       "test-issuer",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator, err := NewValidator(context.Background(), tt.config)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, validator)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, validator)
			}
		})
	}
}

func TestValidator_ValidateToken(t *testing.T) {
	validator, err := NewValidator(context.Background(), Config{
		PublicKeyJWK:         testPublicKeyJWK,
		Issuer:               "test-issuer",
		Subject:              "test-subject",
		Audience:             "test-audience",
		Nonce:                "test-nonce",
		MaxExpirationFromNow: 10 * time.Minute,
	})
	require.NoError(t, err)

	now := time.Now()
	tests := []struct {
		name      string
		claims    map[string]interface{}
		wantErr   bool
		errString string
	}{
		{
			name: "valid token",
			claims: map[string]interface{}{
				"iss":   "test-issuer",
				"sub":   "test-subject",
				"aud":   "test-audience",
				"nonce": "test-nonce",
				"exp":   now.Add(time.Minute).Unix(),
				"nbf":   now.Add(-time.Hour).Unix(),
				"iat":   now.Unix(),
			},
			wantErr: false,
		},
		{
			name: "expired token",
			claims: map[string]interface{}{
				"iss":   "test-issuer",
				"sub":   "test-subject",
				"aud":   "test-audience",
				"nonce": "test-nonce",
				"exp":   now.Add(-time.Hour).Unix(),
				"nbf":   now.Add(-2 * time.Hour).Unix(),
				"iat":   now.Add(-2 * time.Hour).Unix(),
			},
			wantErr:   true,
			errString: "token is invalid: xjwt: JWT expired:",
		},
		{
			name: "not yet valid token",
			claims: map[string]interface{}{
				"iss":   "test-issuer",
				"sub":   "test-subject",
				"aud":   "test-audience",
				"nonce": "test-nonce",
				"exp":   now.Add(2 * time.Hour).Unix(),
				"nbf":   now.Add(time.Hour).Unix(),
				"iat":   now.Unix(),
			},
			wantErr:   true,
			errString: "token is invalid: xjwt: JWT has invalid expiration:",
		},
		{
			name: "wrong issuer",
			claims: map[string]interface{}{
				"iss":   "wrong-issuer",
				"sub":   "test-subject",
				"aud":   "test-audience",
				"nonce": "test-nonce",
				"exp":   now.Add(time.Hour).Unix(),
				"nbf":   now.Add(-time.Hour).Unix(),
				"iat":   now.Unix(),
			},
			wantErr:   true,
			errString: "token is invalid: xjwt: Issuer mismatch.",
		},
		{
			name: "wrong subject",
			claims: map[string]interface{}{
				"iss":   "test-issuer",
				"sub":   "wrong-subject",
				"aud":   "test-audience",
				"nonce": "test-nonce",
				"exp":   now.Add(time.Hour).Unix(),
				"nbf":   now.Add(-time.Hour).Unix(),
				"iat":   now.Unix(),
			},
			wantErr:   true,
			errString: "token is invalid: xjwt: Subject mismatch.",
		},
		{
			name: "wrong audience",
			claims: map[string]interface{}{
				"iss":   "test-issuer",
				"sub":   "test-subject",
				"aud":   "wrong-audience",
				"nonce": "test-nonce",
				"exp":   now.Add(time.Hour).Unix(),
				"nbf":   now.Add(-time.Hour).Unix(),
				"iat":   now.Unix(),
			},
			wantErr:   true,
			errString: "token is invalid: xjwt: Audience mismatch.",
		},
		{
			name: "wrong nonce",
			claims: map[string]interface{}{
				"iss":   "test-issuer",
				"sub":   "test-subject",
				"aud":   "test-audience",
				"nonce": "wrong-nonce",
				"exp":   now.Add(time.Hour).Unix(),
				"nbf":   now.Add(-time.Hour).Unix(),
				"iat":   now.Unix(),
			},
			wantErr:   true,
			errString: "token is invalid: xjwt: Nonce mismatch.",
		},
		{
			name: "expiry too far in future",
			claims: map[string]interface{}{
				"iss":   "test-issuer",
				"sub":   "test-subject",
				"aud":   "test-audience",
				"nonce": "test-nonce",
				"exp":   now.Add(20 * time.Minute).Unix(), // MaxExpiry is 10 minutes
				"nbf":   now.Add(-time.Hour).Unix(),
				"iat":   now.Unix(),
			},
			wantErr:   true,
			errString: "token is invalid: xjwt: JWT has invalid expiration:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token := createTestToken(t, tt.claims)
			claims, err := validator.ValidateToken(context.Background(), token)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errString != "" {
					assert.True(t, strings.HasPrefix(err.Error(), tt.errString), "Expected error to start with %q, got %q", tt.errString, err.Error())
				}
				assert.Nil(t, claims)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, claims)
				assert.Equal(t, tt.claims["iss"], claims["iss"])
				assert.Equal(t, tt.claims["sub"], claims["sub"])
				// Convert expected int64 to float64 for comparison since JSON unmarshaling converts numbers to float64
				assert.Equal(t, float64(tt.claims["exp"].(int64)), claims["exp"])
				assert.Equal(t, float64(tt.claims["nbf"].(int64)), claims["nbf"])
				assert.Equal(t, float64(tt.claims["iat"].(int64)), claims["iat"])
			}
		})
	}
}

func TestExtractBearerToken(t *testing.T) {
	tests := []struct {
		name       string
		authHeader string
		want       string
		wantErr    bool
		errString  string
	}{
		{
			name:       "valid bearer token",
			authHeader: "Bearer token123",
			want:       "token123",
			wantErr:    false,
		},
		{
			name:       "missing header",
			authHeader: "",
			wantErr:    true,
			errString:  "missing Authorization header",
		},
		{
			name:       "invalid format",
			authHeader: "NotBearer token123",
			wantErr:    true,
			errString:  "invalid Authorization header format",
		},
		{
			name:       "just Bearer",
			authHeader: "Bearer",
			wantErr:    true,
			errString:  "invalid Authorization header format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractBearerToken(tt.authHeader)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errString != "" {
					assert.Equal(t, tt.errString, err.Error())
				}
				assert.Empty(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
