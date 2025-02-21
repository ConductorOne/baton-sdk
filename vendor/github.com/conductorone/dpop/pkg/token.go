package dpop

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/pquerna/xjwt"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	assertionType = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer-with-identity-attestation"
)

type Encodable interface {
	Encode() string
}
type ClaimsAdjuster interface {
	AdjustClaims(claims *jwt.Claims) error
	AdjustBody(body *url.Values) (Encodable, error)
	Marshal(claims *jwt.Claims) ([]byte, error)
}

func NewTokenSource(ctx context.Context, dpopSigner *DPoPProofer, tokenURL *url.URL, clientID string, clientSecret *jose.JSONWebKey, claimsAdjuster ClaimsAdjuster, client *http.Client) (*tokenSource, error) {
	if dpopSigner == nil {
		return nil, errors.New("new-token-source: dpop-signer is required")
	}

	if clientID == "" {
		return nil, errors.New("new-token-source: client-id is required")
	}

	if clientSecret == nil {
		return nil, errors.New("new-token-source: client-secret is required")
	}

	if tokenURL == nil {
		return nil, errors.New("new-token-source: token-url is required")
	}

	if client == nil {
		return nil, errors.New("new-token-source: http-client is required")
	}

	return &tokenSource{
		clientID:       clientID,
		clientSecret:   clientSecret,
		tokenURL:       tokenURL,
		httpClient:     client,
		dpopSigner:     dpopSigner,
		claimsAdjuster: claimsAdjuster,
	}, nil
}

type tokenSource struct {
	clientID       string
	clientSecret   *jose.JSONWebKey
	tokenURL       *url.URL
	httpClient     *http.Client
	dpopSigner     *DPoPProofer
	claimsAdjuster ClaimsAdjuster
}

func (c *tokenSource) Token() (*oauth2.Token, error) {
	jsigner, err := jose.NewSigner(
		jose.SigningKey{
			Algorithm: jose.EdDSA,
			Key:       c.clientSecret,
		},
		&jose.SignerOptions{
			NonceSource: &xjwt.RandomNonce{Size: 16},
		})
	if err != nil {
		return nil, err
	}

	// Our token host may include a port, but the audience never expects a port
	aud := c.tokenURL.Hostname()
	now := time.Now()

	claims := &jwt.Claims{
		Issuer:    c.clientID,
		Subject:   c.clientID,
		Audience:  jwt.Audience{aud},
		Expiry:    jwt.NewNumericDate(now.Add(time.Minute * 2)),
		IssuedAt:  jwt.NewNumericDate(now),
		NotBefore: jwt.NewNumericDate(now.Add(-time.Minute * 2)),
	}

	var marshalledClaims []byte
	if c.claimsAdjuster == nil {
		marshalledClaims, err = json.Marshal(claims)
		if err != nil {
			return nil, fmt.Errorf("token-source: failed to marshal claims: %w", err)
		}
	} else {
		err = c.claimsAdjuster.AdjustClaims(claims)
		if err != nil {
			return nil, fmt.Errorf("token-source: failed to adjust claims: %w", err)
		}

		marshalledClaims, err = c.claimsAdjuster.Marshal(claims)
		if err != nil {
			return nil, fmt.Errorf("token-source: failed to marshal claims: %w", err)
		}
	}

	method := http.MethodPost
	dpopProof, err := c.dpopSigner.Proof(method, c.tokenURL.String(), "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to get token: %w", err)
	}

	rv, err := jsigner.Sign(marshalledClaims)
	if err != nil {
		return nil, fmt.Errorf("failed to sign proof: %w", err)
	}

	s, err := rv.CompactSerialize()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize proof: %w", err)
	}

	body := url.Values{
		"client_id":             []string{c.clientID},
		"grant_type":            []string{"client_credentials"},
		"client_assertion_type": []string{assertionType},
		"client_assertion":      []string{s},
	}

	var encodeable Encodable
	if c.claimsAdjuster == nil {
		encodeable = &body
	} else {
		encodeable, err = c.claimsAdjuster.AdjustBody(&body)
		if err != nil {
			return nil, fmt.Errorf("token-source: failed to adjust body: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(context.Background(), method, c.tokenURL.String(), strings.NewReader(encodeable.Encode()))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("DPoP", dpopProof)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, status.Errorf(codes.Unauthenticated, "failed to get token: %s", resp.Status)
	}

	token := &oauth2.Token{}
	err = json.NewDecoder(resp.Body).Decode(token)
	if err != nil {
		return nil, err
	}

	if token.AccessToken == "" {
		return nil, status.Errorf(codes.Unauthenticated, "failed to get token: empty access token")
	}

	return token, nil
}
