package config

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	pb_connector_manager "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
)

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
