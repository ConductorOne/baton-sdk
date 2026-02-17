package uhttp

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"golang.org/x/oauth2/jwt"
)

func TestHelpers_NoAuth_GetClient(t *testing.T) {
	n := &NoAuth{}
	ctx := context.Background()
	client, err := n.GetClient(ctx)
	require.NoError(t, err)
	require.NotNil(t, client)

	expectedClient, err := getHttpClient(ctx)
	require.NoError(t, err)
	require.EqualExportedValues(t, *expectedClient, *client)
}

func TestHelpers_BearerAuth_GetClient(t *testing.T) {
	b := &BearerAuth{
		Token: "test-token",
	}

	ctx := context.Background()
	client, err := b.GetClient(ctx)
	require.NoError(t, err)
	require.NotNil(t, client)

	// check that the token is set
	oauthTransport := client.Transport.(*oauth2.Transport)
	token, err := oauthTransport.Source.Token()
	require.NoError(t, err)
	require.Equal(t, "test-token", token.AccessToken)
}

func TestHelpers_BasicAuth_GetClient(t *testing.T) {
	b := &BasicAuth{
		Username: "test-username",
		Password: "test-password",
	}

	ctx := context.Background()
	client, err := b.GetClient(ctx)
	require.NoError(t, err)
	require.NotNil(t, client)

	// check that the token is set
	oauthTransport := client.Transport.(*oauth2.Transport)
	token, err := oauthTransport.Source.Token()
	require.NoError(t, err)

	// decode the token to check the username and password
	decoded, err := base64.StdEncoding.DecodeString(token.AccessToken)
	require.NoError(t, err)

	parts := bytes.Split(decoded, []byte(":"))
	require.Len(t, parts, 2)

	require.Equal(t, "test-username", string(parts[0]))
	require.Equal(t, "test-password", string(parts[1]))
}

func TestHelpers_OAuth2_ClientCredentials_GetClient(t *testing.T) {
	tests := []struct {
		tokenResponseJSON string
		input             struct {
			cfg *clientcredentials.Config
		}
		wanted struct {
			authHeader string
		}
	}{
		{
			tokenResponseJSON: `{"access_token": "test-access-token", "token_type": "test-token-type", "expires_in": 3600}`,
			input: struct{ cfg *clientcredentials.Config }{
				cfg: &clientcredentials.Config{
					ClientID:     "test-client-id",
					ClientSecret: "test-client-secret",
				},
			},
			wanted: struct {
				authHeader string
			}{
				authHeader: fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte("test-client-id:test-client-secret"))),
			},
		},
	}

	for _, tt := range tests {
		hitServer := false

		cc := &OAuth2ClientCredentials{cfg: tt.input.cfg}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			require.Equal(t, tt.wanted.authHeader, r.Header.Get("Authorization"))

			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(tt.tokenResponseJSON))
			w.WriteHeader(http.StatusOK)

			hitServer = true
		}))
		defer server.Close()
		cc.cfg.TokenURL = server.URL

		ctx := context.Background()
		client, err := cc.GetClient(ctx)
		require.NoError(t, err)
		require.NotNil(t, client)

		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://test-url", nil)
		//nolint:gosec // test request intentionally targets httptest token endpoint.
		res, _ := client.Do(req)
		if res != nil {
			defer res.Body.Close()
		}
		require.True(t, hitServer)
	}
}

func TestHelpers_OAuth2_JWT_GetClient(t *testing.T) {
	tests := []struct {
		tokenResponseJSON string
		input             func(tokenUrl string) *OAuth2JWT
		wanted            struct {
			grantType      string
			matchAssertion string
		}
	}{
		{
			tokenResponseJSON: `{"access_token": "test-access-token", "token_type": "test-token-type", "expires_in": 3600}`,
			input: func(tokenUrl string) *OAuth2JWT {
				return &OAuth2JWT{
					Credentials: []byte("test-credentials"),
					CreateJWTConfig: func(credentials []byte, scopes ...string) (*jwt.Config, error) {
						return &jwt.Config{
							Email:      "test-email",
							TokenURL:   tokenUrl,
							PrivateKey: getDummyPrivateKey(),
							Scopes:     scopes,
							Subject:    "test-subject",
						}, nil
					},
				}
			},
			wanted: struct {
				grantType      string
				matchAssertion string
			}{
				grantType:      "urn:ietf:params:oauth:grant-type:jwt-bearer",
				matchAssertion: `^[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]*$`,
			},
		},
	}

	for _, tt := range tests {
		hitServer := false

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			require.Equal(t, tt.wanted.grantType, r.FormValue("grant_type"))

			matched, err := regexp.MatchString(tt.wanted.matchAssertion, r.FormValue("assertion"))
			require.NoError(t, err)
			require.True(t, matched)

			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(tt.tokenResponseJSON))
			w.WriteHeader(http.StatusOK)

			hitServer = true
		}))
		defer server.Close()

		ctx := context.Background()
		client, err := tt.input(server.URL).GetClient(ctx)

		require.NoError(t, err)
		require.NotNil(t, client)

		require.NoError(t, err)
		require.NotNil(t, client)

		// To invoke token request
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://test-url", nil)
		//nolint:gosec // test request intentionally targets httptest token endpoint.
		res, _ := client.Do(req)
		if res != nil {
			defer res.Body.Close()
		}
		require.True(t, hitServer)
	}
}

func getDummyPrivateKey() []byte {
	privateKeyPEM := `-----BEGIN RSA PRIVATE KEY----- 
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
-----END RSA PRIVATE KEY-----` //#nosec G101

	return []byte(privateKeyPEM)
}

func TestHelpers_OAuth2_RefreshToken_GetClient(t *testing.T) {
	rt := &OAuth2RefreshToken{
		cfg: &oauth2.Config{
			ClientID:     "test-client-id",
			ClientSecret: "test-client-secret",
			// #nosec G101 -- static test endpoint, not a secret.
			Endpoint: oauth2.Endpoint{
				TokenURL: "https://test-token-url",
			},
		},
		accessToken:  "test-access-token",
		refreshToken: "test-refresh-token",
	}

	ctx := context.Background()
	client, err := rt.GetClient(ctx)
	require.NoError(t, err)
	require.NotNil(t, client)

	// check that the token is set
	oauthTransport := client.Transport.(*oauth2.Transport)
	token, err := oauthTransport.Source.Token()
	require.NoError(t, err)
	require.NotEmpty(t, token.AccessToken)
	require.NotEmpty(t, token.RefreshToken)
	require.NotEmpty(t, token.TokenType)

	// check if access token and refresh token are what we set them up in the config
	require.Equal(t, "test-access-token", token.AccessToken)
	require.Equal(t, "test-refresh-token", token.RefreshToken)
	require.Equal(t, "Bearer", token.TokenType)
}
