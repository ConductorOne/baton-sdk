package uhttp

import (
	"bytes"
	"context"
	"encoding/base64"
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
	cfg := &clientcredentials.Config{
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
		TokenURL:     "https://test-token-url",
	}
	cc := &OAuth2ClientCredentials{cfg: cfg}

	ctx := context.Background()
	client, err := cc.GetClient(ctx)
	require.NoError(t, err)
	require.NotNil(t, client)
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

func TestHelpers_OAuth2_RefreshToken_GetClient(t *testing.T) {
	rt := &OAuth2RefreshToken{
		cfg: &oauth2.Config{
			ClientID:     "test-client-id",
			ClientSecret: "test-client-secret",
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
