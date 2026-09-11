package uhttp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
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

		res, _ := client.Do(req)
		if res != nil {
			defer res.Body.Close()
		}
		require.True(t, hitServer)
	}
}

// jwtBearerServer stands up a token endpoint that records every assertion it
// receives and hands back a token with the given expires_in. It returns the
// server and a pointer to the slice of received assertions.
func jwtBearerServer(t *testing.T, expiresIn int) (*httptest.Server, *[]string) {
	t.Helper()
	assertions := make([]string, 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "urn:ietf:params:oauth:grant-type:jwt-bearer", r.FormValue("grant_type"))
		assertions = append(assertions, r.FormValue("assertion"))

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{"access_token": "test-access-token", "token_type": "bearer", "expires_in": %d}`, expiresIn)))
	}))
	return server, &assertions
}

// jtiFromAssertion decodes the (unverified) JWT payload of a jwt-bearer
// assertion and returns its "jti" claim, or "" if absent.
func jtiFromAssertion(t *testing.T, assertion string) string {
	t.Helper()
	parts := strings.Split(assertion, ".")
	require.Len(t, parts, 3, "assertion should be a JWT of form header.payload.signature")

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)

	var claims map[string]interface{}
	require.NoError(t, json.Unmarshal(payload, &claims))

	jti, _ := claims["jti"].(string)
	return jti
}

// makeRequest drives one HTTP request through the client so the underlying
// token source is exercised. The request itself targets a non-routable host —
// the token acquisition (which hits the httptest token endpoint) is what we
// care about, so any error on the final GET is intentionally ignored.
func makeRequest(t *testing.T, ctx context.Context, client *http.Client) {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://test-url", nil)
	res, _ := client.Do(req)
	if res != nil {
		defer res.Body.Close()
	}
}

// TestHelpers_OAuth2_JWT_ReinvokesCreateJWTConfig proves that when the reused
// token expires, CreateJWTConfig is invoked again on the next fetch rather than
// baking a single config in for the client's lifetime.
func TestHelpers_OAuth2_JWT_ReinvokesCreateJWTConfig(t *testing.T) {
	ctx := context.Background()

	// expires_in below the oauth2 expiry delta so every fetch sees the cached
	// token as already expired and refetches.
	server, assertions := jwtBearerServer(t, 1)
	defer server.Close()

	callCount := 0
	o := &OAuth2JWT{
		Credentials: []byte("test-credentials"),
		CreateJWTConfig: func(credentials []byte, scopes ...string) (*jwt.Config, error) {
			callCount++
			return &jwt.Config{
				Email:      "test-email",
				TokenURL:   server.URL,
				PrivateKey: getDummyPrivateKey(),
				Scopes:     scopes,
				Subject:    "test-subject",
			}, nil
		},
	}

	client, err := o.GetClient(ctx)
	require.NoError(t, err)
	require.NotNil(t, client)

	makeRequest(t, ctx, client)
	makeRequest(t, ctx, client)
	makeRequest(t, ctx, client)

	require.Greater(t, callCount, 1, "CreateJWTConfig should be re-invoked once the reused token expires")
	require.Len(t, *assertions, callCount, "each token fetch should produce a fresh assertion")
}

// TestHelpers_OAuth2_JWT_FreshJTIPerFetch proves that a CreateJWTConfig which
// stamps a fresh PrivateClaims["jti"] on every call yields a distinct jti in
// each assertion the token endpoint receives.
func TestHelpers_OAuth2_JWT_FreshJTIPerFetch(t *testing.T) {
	ctx := context.Background()

	server, assertions := jwtBearerServer(t, 1)
	defer server.Close()

	nonce := 0
	o := &OAuth2JWT{
		Credentials: []byte("test-credentials"),
		CreateJWTConfig: func(credentials []byte, scopes ...string) (*jwt.Config, error) {
			nonce++
			return &jwt.Config{
				Email:      "test-email",
				TokenURL:   server.URL,
				PrivateKey: getDummyPrivateKey(),
				Scopes:     scopes,
				Subject:    "test-subject",
				PrivateClaims: map[string]interface{}{
					"jti": fmt.Sprintf("nonce-%d", nonce),
				},
			}, nil
		},
	}

	client, err := o.GetClient(ctx)
	require.NoError(t, err)

	makeRequest(t, ctx, client)
	makeRequest(t, ctx, client)

	require.GreaterOrEqual(t, len(*assertions), 2, "expected at least two token fetches")

	seen := make(map[string]bool)
	for _, a := range *assertions {
		jti := jtiFromAssertion(t, a)
		require.NotEmpty(t, jti, "each assertion should carry a jti claim")
		require.False(t, seen[jti], "jti %q was reused across fetches", jti)
		seen[jti] = true
	}
}

// TestHelpers_OAuth2_JWT_CachesTokenForStaticConfig proves the common case is
// unchanged: a static config still benefits from oauth2.ReuseTokenSource
// caching, so a long-lived token is fetched once and reused across requests
// (CreateJWTConfig is not re-invoked while the token is still valid).
func TestHelpers_OAuth2_JWT_CachesTokenForStaticConfig(t *testing.T) {
	ctx := context.Background()

	// Long expiry so the first token stays valid across subsequent requests.
	server, assertions := jwtBearerServer(t, 3600)
	defer server.Close()

	callCount := 0
	o := &OAuth2JWT{
		Credentials: []byte("test-credentials"),
		CreateJWTConfig: func(credentials []byte, scopes ...string) (*jwt.Config, error) {
			callCount++
			return &jwt.Config{
				Email:      "test-email",
				TokenURL:   server.URL,
				PrivateKey: getDummyPrivateKey(),
				Scopes:     scopes,
				Subject:    "test-subject",
			}, nil
		},
	}

	client, err := o.GetClient(ctx)
	require.NoError(t, err)

	makeRequest(t, ctx, client)
	makeRequest(t, ctx, client)
	makeRequest(t, ctx, client)

	require.Equal(t, 1, callCount, "CreateJWTConfig should only run once while the cached token is still valid")
	require.Len(t, *assertions, 1, "token endpoint should be hit exactly once for a long-lived token")
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
