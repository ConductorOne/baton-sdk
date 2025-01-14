package resource

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

type SecretTraitOption func(t *v2.SecretTrait) error

func WithSecretCreatedAt(createdAt time.Time) SecretTraitOption {
	return func(t *v2.SecretTrait) error {
		t.CreatedAt = timestamppb.New(createdAt)
		return nil
	}
}

func WithSecretLastUsedAt(lastUsed time.Time) SecretTraitOption {
	return func(t *v2.SecretTrait) error {
		t.LastUsedAt = timestamppb.New(lastUsed)
		return nil
	}
}

func WithSecretExpiresAt(expiresAt time.Time) SecretTraitOption {
	return func(t *v2.SecretTrait) error {
		t.ExpiresAt = timestamppb.New(expiresAt)
		return nil
	}
}

func WithSecretCreatedByID(createdById *v2.ResourceId) SecretTraitOption {
	return func(t *v2.SecretTrait) error {
		t.CreatedById = createdById
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
