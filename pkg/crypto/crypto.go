package crypto //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	"github.com/go-jose/go-jose/v4"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type key string

const (
	ContextClientSecretKey = key("client-secret-key")
)

type PlaintextCredential struct {
	Name        string
	Description string
	Schema      string
	Bytes       []byte
}

type EncryptionManager struct {
	opts    *v2.CredentialOptions
	configs []*v2.EncryptionConfig
}

// FIXME(morgabra) Be tolerant of failures here and return the encryptions that succeeded. We've likely already
// done things to generate the credentials we want to encrypt, so we should still return the created objects
// even if your encryption provider is misconfigured.
func (pkem *EncryptionManager) Encrypt(ctx context.Context, cred *v2.PlaintextData) ([]*v2.EncryptedData, error) {
	encryptedDatas := make([]*v2.EncryptedData, 0, len(pkem.configs))

	for _, config := range pkem.configs {
		provider, err := providers.GetEncryptionProviderForConfig(ctx, config)
		if err != nil {
			return nil, err
		}

		encryptedData, err := provider.Encrypt(ctx, config, cred)
		if err != nil {
			return nil, err
		}

		encryptedDatas = append(encryptedDatas, encryptedData)
	}
	return encryptedDatas, nil
}

func NewEncryptionManager(co *v2.CredentialOptions, ec []*v2.EncryptionConfig) (*EncryptionManager, error) {
	em := &EncryptionManager{
		opts:    co,
		configs: ec,
	}
	return em, nil
}

func decryptPassword(ctx context.Context, encryptedPassword *v2.EncryptedData, decryptionConfig *providers.DecryptionConfig) (string, error) {
	if decryptionConfig == nil {
		return "", ErrInvalidCredentialOptions
	}

	provider, err := providers.GetDecryptionProviderForConfig(ctx, decryptionConfig)
	if err != nil {
		return "", status.Errorf(codes.Internal, "error getting decryption provider for config: %v", err)
	}
	key := decryptionConfig.PrivateKey
	if key == nil {
		return "", status.Errorf(codes.InvalidArgument, "decryption config key is empty")
	}

	plaintext, err := provider.Decrypt(ctx, encryptedPassword, decryptionConfig.PrivateKey)
	if err != nil {
		return "", status.Errorf(codes.Internal, "error decrypting password: %v", err)
	}

	return string(plaintext.GetBytes()), nil
}

func ConvertCredentialOptions(ctx context.Context, clientSecret *jose.JSONWebKey, opts *v2.CredentialOptions, encryptionConfigs []*v2.EncryptionConfig) (*v2.LocalCredentialOptions, error) {
	l := ctxzap.Extract(ctx)
	if opts == nil {
		return nil, nil
	}

	localOpts := v2.LocalCredentialOptions_builder{
		ForceChangeAtNextLogin: opts.GetForceChangeAtNextLogin(),
	}.Build()

	switch opts.WhichOptions() {
	case v2.CredentialOptions_RandomPassword_case:
		localOpts.SetRandomPassword(v2.LocalCredentialOptions_RandomPassword_builder{
			Length:      opts.GetRandomPassword().GetLength(),
			Constraints: opts.GetRandomPassword().GetConstraints(),
		}.Build())
	case v2.CredentialOptions_NoPassword_case:
		localOpts.SetNoPassword(&v2.LocalCredentialOptions_NoPassword{})
	case v2.CredentialOptions_Sso_case:
		localOpts.SetSso(v2.LocalCredentialOptions_SSO_builder{
			SsoProvider: opts.GetSso().GetSsoProvider(),
		}.Build())
	case v2.CredentialOptions_EncryptedPassword_case:
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid credential options")
	}

	encryptedPasswordOpt := opts.GetEncryptedPassword()
	if encryptedPasswordOpt == nil {
		return localOpts, nil
	}
	encryptedPasswords := encryptedPasswordOpt.GetEncryptedPasswords()
	if len(encryptedPasswords) == 0 {
		return localOpts, nil
	}

	// Whatever's setting the password should already know it. Don't let us encrypt it and send it back.
	if len(encryptionConfigs) > 0 {
		l.Error("error: encryption configs should never be supplied for encrypted passwords")
		return localOpts, status.Error(codes.InvalidArgument, "encryption configs should never be supplied for encrypted passwords")
	}

	if clientSecret == nil {
		return localOpts, status.Error(codes.InvalidArgument, "client-secret is required")
	}

	for _, encryptedPassword := range encryptedPasswords {
		keyIDs := encryptedPassword.GetKeyIds()
		if len(keyIDs) == 0 {
			continue
		}
		for _, keyId := range keyIDs {
			if keyId != clientSecret.KeyID {
				l.Warn("convert-credential-options: key id does not match client secret key id", zap.String("keyId", keyId), zap.String("clientSecretKeyID", clientSecret.KeyID))
				continue
			}
			password, err := decryptPassword(ctx, encryptedPassword, &providers.DecryptionConfig{
				Provider:   jwk.EncryptionProviderJwkPrivate,
				PrivateKey: clientSecret,
			})
			if err != nil {
				return nil, fmt.Errorf("convert-credential-options: error decrypting password: %w", err)
			}
			localOpts.SetPlaintextPassword(v2.LocalCredentialOptions_PlaintextPassword_builder{
				PlaintextPassword: password,
			}.Build())
			break
		}
		if localOpts.HasOptions() {
			break
		}
	}

	if !localOpts.HasOptions() {
		return nil, status.Errorf(codes.InvalidArgument, "no encrypted password matched client secret key id %q", clientSecret.KeyID)
	}

	return localOpts, nil
}
