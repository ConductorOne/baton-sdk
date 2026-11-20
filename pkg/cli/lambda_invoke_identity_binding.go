//go:build baton_lambda_support

package cli

import (
	"crypto/subtle"

	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type lambdaIdentityBinding struct {
	secret     []byte
	thumbprint string
}

func newLambdaIdentityBinding(secret string) (*lambdaIdentityBinding, error) {
	key, err := crypto.ParseClientSecret([]byte(secret), false)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "lambda-run: invalid invoke client secret")
	}
	thumbprint, err := jwk.Thumbprint(key)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "lambda-run: invalid invoke client key")
	}
	return &lambdaIdentityBinding{secret: []byte(secret), thumbprint: thumbprint}, nil
}

func (b *lambdaIdentityBinding) check(secret string) error {
	if subtle.ConstantTimeCompare(b.secret, []byte(secret)) == 1 {
		return nil
	}
	presented, err := newLambdaIdentityBinding(secret)
	if err != nil || subtle.ConstantTimeCompare([]byte(b.thumbprint), []byte(presented.thumbprint)) != 1 {
		return status.Error(codes.FailedPrecondition, "lambda-run: execution environment is bound to a different client key; use a new execution scope")
	}
	return nil
}
