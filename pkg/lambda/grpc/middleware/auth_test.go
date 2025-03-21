package middleware

import (
	"context"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	testPublicKeyJWK = `{
		"kty": "RSA",
		"n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
		"e": "AQAB"
	}`
)

func TestAuthInterceptor(t *testing.T) {
	ctx := context.Background()
	validator, err := auth.NewValidator(ctx, auth.Config{
		PublicKeyJWK:         testPublicKeyJWK,
		Issuer:               "test-issuer",
		Subject:              "test-subject",
		Audience:             "test-audience",
		Nonce:                "test-nonce",
		MaxExpirationFromNow: 10 * time.Minute,
	})
	require.NoError(t, err)

	interceptor := AuthInterceptor(validator)

	// Mock handler that checks claims in context
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		claims, ok := ClaimsFromContext(ctx)
		assert.True(t, ok)
		assert.NotNil(t, claims)
		assert.Equal(t, "test-issuer", claims["iss"])
		assert.Equal(t, "test-subject", claims["sub"])
		assert.Equal(t, "test-audience", claims["aud"])
		assert.Equal(t, "test-nonce", claims["nonce"])
		return "success", nil
	}

	// Test cases
	tests := []struct {
		name      string
		metadata  metadata.MD
		wantError codes.Code
	}{
		{
			name:      "missing metadata",
			metadata:  nil,
			wantError: codes.Unauthenticated,
		},
		{
			name:      "missing authorization header",
			metadata:  metadata.MD{},
			wantError: codes.Unauthenticated,
		},
		{
			name: "invalid authorization header",
			metadata: metadata.MD{
				"authorization": []string{"invalid"},
			},
			wantError: codes.Unauthenticated,
		},
		{
			name: "invalid token",
			metadata: metadata.MD{
				"authorization": []string{"Bearer invalid"},
			},
			wantError: codes.Unauthenticated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.metadata != nil {
				ctx = metadata.NewIncomingContext(ctx, tt.metadata)
			}

			_, err := interceptor(ctx, nil, nil, handler)
			if tt.wantError != codes.OK {
				s, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, tt.wantError, s.Code())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestClaimsFromContext(t *testing.T) {
	claims := map[string]interface{}{
		"iss":   "test-issuer",
		"sub":   "test-subject",
		"iat":   time.Now().Unix(),
		"exp":   time.Now().Add(time.Hour).Unix(),
		"nbf":   time.Now().Add(-time.Hour).Unix(),
		"nonce": "test-nonce",
	}

	// Test with claims in context
	ctx := context.WithValue(context.Background(), claimsContextKey{}, claims)
	gotClaims, ok := ClaimsFromContext(ctx)
	assert.True(t, ok)
	assert.Equal(t, claims, gotClaims)

	// Test with no claims in context
	gotClaims, ok = ClaimsFromContext(context.Background())
	assert.False(t, ok)
	assert.Nil(t, gotClaims)
}

func TestWithAuth(t *testing.T) {
	ctx := context.Background()
	_, err := WithAuth(ctx, auth.Config{
		PublicKeyJWK:         testPublicKeyJWK,
		Issuer:               "test-issuer",
		Subject:              "test-subject",
		Audience:             "test-audience",
		Nonce:                "test-nonce",
		MaxExpirationFromNow: 10 * time.Minute,
	})
	require.NoError(t, err)
}
