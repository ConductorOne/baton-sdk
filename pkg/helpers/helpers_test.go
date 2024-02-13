package helpers

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"golang.org/x/oauth2/jwt"
)

func TestHelpers_SplitFullName(t *testing.T) {
	firstName, lastName := SplitFullName("Prince")
	require.Equal(t, "Prince", firstName)
	require.Equal(t, "", lastName)

	firstName, lastName = SplitFullName("John Smith")
	require.Equal(t, "John", firstName)
	require.Equal(t, "Smith", lastName)

	firstName, lastName = SplitFullName("John Jacob Jingleheimer Schmidt")
	require.Equal(t, "John", firstName)
	require.Equal(t, "Jacob Jingleheimer Schmidt", lastName)
}

func TestHelpers_NoAuth_GetClient(t *testing.T) {
	n := &NoAuth{}
	client, err := n.GetClient(context.Background())
	require.NoError(t, err)
	require.NotNil(t, client)
	require.Equal(t, http.DefaultClient, client)
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
