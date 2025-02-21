package config

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	aws_config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/go-jose/go-jose/v4/jwt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	dpop "github.com/conductorone/dpop/pkg"
)

var _ dpop.ClaimsAdjuster = (*adjuster)(nil)

func sha256AndHexEncode(input string) string {
	hash := sha256.Sum256([]byte(input))
	return hex.EncodeToString(hash[:])
}

type adjuster struct {
	awsConfig *aws.Config
}

func NewAdjuster(ctx context.Context) (*adjuster, error) {
	cfg, err := aws_config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("create-sigv4-sts-get-caller-identity-request: failed to load AWS configuration: %w", err)
	}

	return &adjuster{awsConfig: &cfg}, nil
}

func (a *adjuster) AdjustBody(body *url.Values) (dpop.Encodable, error) {
	return body, nil
}

func (a *adjuster) AdjustClaims(claims *jwt.Claims) error {
	return nil
}

func (a *adjuster) Marshal(claims *jwt.Claims) ([]byte, error) {
	getIdentityAttestation, err := a.getIdentityAttestation()
	if err != nil {
		return nil, fmt.Errorf("c1-credential-provider: failed to get identity attestation: %w", err)
	}

	claimsWithIDAtt := &struct {
		jwt.Claims
		IDAtt string `json:"id_att"` // Identity Attestation: Currently only supports v1.Sigv4SignedRequestSTSGetCallerIdentity
	}{
		Claims: *claims,
		IDAtt:  getIdentityAttestation,
	}

	return json.Marshal(claimsWithIDAtt)
}

// getIdentityAttestation creates a signed sts GetCallerIdentity request and marshals it to a base64 encoded string.
func (a *adjuster) getIdentityAttestation() (string, error) {
	req, err := createSigv4STSGetCallerIdentityRequest(context.Background(), a.awsConfig)
	if err != nil {
		return "", fmt.Errorf("c1-credential-provider: failed to create sigv4 sts get caller identity request: %w", err)
	}

	anyReq, err := anypb.New(req)
	if err != nil {
		return "", fmt.Errorf("c1-credential-provider: failed to create sigv4 sts get caller identity request: %w", err)
	}

	b, err := proto.Marshal(anyReq)
	if err != nil {
		return "", fmt.Errorf("c1-credential-provider: failed to marshal sigv4 sts get caller identity request: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(b), nil
}
