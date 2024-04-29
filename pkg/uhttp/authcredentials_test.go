package uhttp

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
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
		cc := &OAuth2ClientCredentials{cfg: tt.input.cfg}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			require.Equal(t, tt.wanted.authHeader, r.Header.Get("Authorization"))

			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(tt.tokenResponseJSON))
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()
		cc.cfg.TokenURL = server.URL

		ctx := context.Background()
		client, err := cc.GetClient(ctx)

		require.NoError(t, err)
		require.NotNil(t, client)

		// Invoke token request
		client.Head("https://test-url")
	}
}

func TestHelpers_OAuth2_JWT_GetClient(t *testing.T) {
	mockConfig := &jwt.Config{
		Email: "test-email",
	}
	jwtCreds := &OAuth2JWT{
		Credentials: []byte("test-credentials"),
		CreateJWTConfig: func(credentials []byte, scopes ...string) (*jwt.Config, error) {
			return mockConfig, nil
		},
	}

	ctx := context.Background()
	client, err := jwtCreds.GetClient(ctx)
	require.NoError(t, err)
	require.NotNil(t, client)
}
