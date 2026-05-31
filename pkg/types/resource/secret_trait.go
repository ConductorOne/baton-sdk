package resource

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

type SecretTraitOption func(t *v2.SecretTrait) error

func WithSecretCreatedAt(createdAt time.Time) SecretTraitOption {
	return func(t *v2.SecretTrait) error {
		t.SetCreatedAt(timestamppb.New(createdAt))
		return nil
	}
}

func WithSecretLastUsedAt(lastUsed time.Time) SecretTraitOption {
	return func(t *v2.SecretTrait) error {
		t.SetLastUsedAt(timestamppb.New(lastUsed))
		return nil
	}
}

func WithSecretExpiresAt(expiresAt time.Time) SecretTraitOption {
	return func(t *v2.SecretTrait) error {
		t.SetExpiresAt(timestamppb.New(expiresAt))
		return nil
	}
}

func WithSecretCreatedByID(createdById *v2.ResourceId) SecretTraitOption {
	return func(t *v2.SecretTrait) error {
		t.SetCreatedById(createdById)
		return nil
	}
}

func WithSecretIdentityID(identityId *v2.ResourceId) SecretTraitOption {
	return func(t *v2.SecretTrait) error {
		t.SetIdentityId(identityId)
		return nil
	}
}

// WithSecretType sets the cryptographic class of the secret.
func WithSecretType(credentialType v2.SecretTrait_CredentialType) SecretTraitOption {
	return func(t *v2.SecretTrait) error {
		t.SetCredentialType(credentialType)
		return nil
	}
}

// WithSecretDetail sets the platform-specific credential kind that refines
// the credential type (e.g. "aws_access_key", "ssh_key", "x509").
func WithSecretDetail(detail string) SecretTraitOption {
	return func(t *v2.SecretTrait) error {
		t.SetCredentialDetail(detail)
		return nil
	}
}

// NewSecretTrait creates a new `SecretTrait` with the given options.
func NewSecretTrait(opts ...SecretTraitOption) (*v2.SecretTrait, error) {
	SecretTrait := &v2.SecretTrait{}

	for _, opt := range opts {
		err := opt(SecretTrait)
		if err != nil {
			return nil, err
		}
	}

	return SecretTrait, nil
}
