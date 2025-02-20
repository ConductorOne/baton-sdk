package config

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	aws_config "github.com/aws/aws-sdk-go-v2/config"
	dpop "github.com/conductorone/dpop/pkg"
	"github.com/go-jose/go-jose/v4/jwt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	pb_connector_manager "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
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

func createSigv4STSGetCallerIdentityRequest(ctx context.Context, cfg *aws.Config) (*pb_connector_manager.Sigv4SignedRequestSTSGetCallerIdentity, error) {
	credentials, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return nil, fmt.Errorf("create-sigv4-sts-get-caller-identity-request: failed to retrieve credentials: %w", err)
	}
	region := cfg.Region
	body := "Action=GetCallerIdentity&Version=2011-06-15"
	service := "sts"
	endpoint := fmt.Sprintf("https://sts.%s.amazonaws.com", region)
	method := "POST"

	reqHeaders := map[string][]string{
		"Content-Type": {"application/x-www-form-urlencoded; charset=utf-8"},
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader([]byte(body)))
	if err != nil {
		return nil, fmt.Errorf("create-sigv4-sts-get-caller-identity-request: failed to create request: %w", err)
	}

	for headerKey, headerValues := range reqHeaders {
		for _, headerValue := range headerValues {
			req.Header.Add(headerKey, headerValue)
		}
	}

	// Use the AWS SigV4 signer
	signer := v4.NewSigner(func(options *v4.SignerOptions) {
		options.DisableHeaderHoisting = true // maybe only applicable to presigned requests
	})

	// NOTE(morgabra/kans): Expiration is sort of whack, and it appears only s3 respects the expiration header, so we can't actually clamp it down.
	// It appears anecdotally that most services expire the request after 15 minutes.
	err = signer.SignHTTP(ctx, credentials, req, sha256AndHexEncode(body), service, region, time.Now())
	if err != nil {
		return nil, fmt.Errorf("create-sigv4-sts-get-caller-identity-request: failed to sign request: %w", err)
	}

	signedHeaders := make([]*pb_connector_manager.SignedHeader, 0)
	for signedHeaderKey, signedHeaderValues := range req.Header {
		v := make([]string, len(signedHeaderValues))
		copy(v, signedHeaderValues)
		signedHeader := &pb_connector_manager.SignedHeader{
			Key:   signedHeaderKey,
			Value: v,
		}
		signedHeaders = append(signedHeaders, signedHeader)
	}

	return &pb_connector_manager.Sigv4SignedRequestSTSGetCallerIdentity{
		Method:   method,
		Endpoint: endpoint,
		Headers:  signedHeaders,
		Body:     []byte(body),
	}, nil
}
