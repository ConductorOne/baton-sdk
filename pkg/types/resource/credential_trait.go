package resource

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

type CredentialTraitOption func(t *v2.CredentialTrait) error

func WithCredentialCreatedAt(createdAt time.Time) CredentialTraitOption {
	return func(t *v2.CredentialTrait) error {
		t.CreatedAt = timestamppb.New(createdAt)
		return nil
	}
}

func WithCredentialLastUsed(lastUsed time.Time) CredentialTraitOption {
	return func(t *v2.CredentialTrait) error {
		t.LastUsed = timestamppb.New(lastUsed)
		return nil
	}
}
 
func WithCredentialExpiresAt(expiresAt time.Time) CredentialTraitOption {
	return func(t *v2.CredentialTrait) error {
		t.ExpiresAt = timestamppb.New(expiresAt)
		return nil
	}
}

func WithCredentialOwnerUserID(ownerUserID string) CredentialTraitOption {
	return func(t *v2.CredentialTrait) error {
		t.OwnerUserId = ownerUserID
		return nil
	}
}

func WithCrdentialType(credentialType v2.CredentialTrait_CredentialType) CredentialTraitOption {
	return func(t *v2.CredentialTrait) error {
		t.Type = credentialType
		return nil
	}
}

// NewCredentialTrait creates a new `CredentialTrait` with the given options.
func NewCredentialTrait(opts ...CredentialTraitOption) (*v2.CredentialTrait, error) {
	credentialTrait := &v2.CredentialTrait{}

	for _, opt := range opts {
		err := opt(credentialTrait)
		if err != nil {
			return nil, err
		}
	}

	return credentialTrait, nil
}
