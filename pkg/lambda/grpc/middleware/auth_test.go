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
	validator, err := auth.NewValidator(auth.Config{
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
		assert.Equal(t, "test-issuer", claims.Issuer)
		assert.Equal(t, "test-subject", claims.Subject)
		assert.Equal(t, []string{"test-audience"}, claims.Audience)
		assert.Equal(t, "test-nonce", claims.ID)
		return "success", nil
	}

	tests := []struct {
		name        string
		setupCtx    func() context.Context
		wantErr     bool
		wantErrCode codes.Code
	}{
		{
			name: "missing metadata",
			setupCtx: func() context.Context {
				return context.Background()
			},
			wantErr:     true,
			wantErrCode: codes.Unauthenticated,
		},
		{
			name: "missing authorization header",
			setupCtx: func() context.Context {
				md := metadata.New(map[string]string{})
				return metadata.NewIncomingContext(context.Background(), md)
			},
			wantErr:     true,
			wantErrCode: codes.Unauthenticated,
		},
		{
			name: "invalid authorization header format",
			setupCtx: func() context.Context {
				md := metadata.New(map[string]string{
					"authorization": "NotBearer token123",
				})
				return metadata.NewIncomingContext(context.Background(), md)
			},
			wantErr:     true,
			wantErrCode: codes.Unauthenticated,
		},
		{
			name: "invalid token",
			setupCtx: func() context.Context {
				md := metadata.New(map[string]string{
					"authorization": "Bearer invalid-token",
				})
				return metadata.NewIncomingContext(context.Background(), md)
			},
			wantErr:     true,
			wantErrCode: codes.Unauthenticated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setupCtx()
			_, err := interceptor(ctx, nil, nil, handler)
			if tt.wantErr {
				assert.Error(t, err)
				st, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, tt.wantErrCode, st.Code())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestClaimsFromContext(t *testing.T) {
	claims := &auth.Claims{
		Issuer:    "test-issuer",
		Subject:   "test-subject",
		IssuedAt:  time.Now().Unix(),
		Expiry:    time.Now().Add(time.Hour).Unix(),
		NotBefore: time.Now().Add(-time.Hour).Unix(),
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
	tests := []struct {
		name    string
		config  auth.Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: auth.Config{
				PublicKeyJWK:         testPublicKeyJWK,
				Issuer:               "test-issuer",
				Subject:              "test-subject",
				Audience:             "test-audience",
				Nonce:                "test-nonce",
				MaxExpirationFromNow: 10 * time.Minute,
			},
			wantErr: false,
		},
		{
			name: "invalid config",
			config: auth.Config{
				PublicKeyJWK: "invalid",
				Issuer:       "test-issuer",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt, err := WithAuth(tt.config)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, opt)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, opt)
			}
		})
	}
}
