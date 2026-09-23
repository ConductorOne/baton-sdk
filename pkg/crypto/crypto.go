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
		provider, err := providers.GetEncryptorForConfig(ctx, config)
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
	if err := validateVaultInboxConfigExclusivity(ec); err != nil {
		return nil, err
	}
	em := &EncryptionManager{
		opts:    co,
		configs: ec,
	}
	return em, nil
}

// ValidatePlaintextCardinality applies [ValidateVaultInboxPlaintextCardinality]
// to this manager's recipients before a caller encrypts a list of values.
func (pkem *EncryptionManager) ValidatePlaintextCardinality(plaintexts []*v2.PlaintextData) error {
	return ValidateVaultInboxPlaintextCardinality(pkem.configs, plaintexts)
}

// ValidatePlaintextCardinalityAtMostOne is the variant for callers that may
// legally produce no plaintext, such as CreateAccount's non-success results.
func (pkem *EncryptionManager) ValidatePlaintextCardinalityAtMostOne(plaintexts []*v2.PlaintextData) error {
	return ValidateVaultInboxPlaintextCardinalityAtMostOne(pkem.configs, plaintexts)
}

// ValidateVaultInboxPlaintextCardinalityAtMostOne permits an absent credential,
// as required by CreateAccount's non-success results. Other recipient types are unaffected.
func ValidateVaultInboxPlaintextCardinalityAtMostOne(configs []*v2.EncryptionConfig, plaintexts []*v2.PlaintextData) error {
	if !hasVaultInboxConfig(configs) {
		return nil
	}
	if len(plaintexts) > 1 {
		return status.Errorf(codes.FailedPrecondition,
			"vault inbox issuance accepts at most one plaintext value, got %d", len(plaintexts))
	}
	return nil
}

// ValidateVaultInboxPlaintextCardinality requires one value for an inbox recipient:
// multiple values would produce envelopes sharing a delivery ID. Other recipient
// types are unaffected. This checks provider output, so failure may follow minting.
func ValidateVaultInboxPlaintextCardinality(configs []*v2.EncryptionConfig, plaintexts []*v2.PlaintextData) error {
	if !hasVaultInboxConfig(configs) {
		return nil
	}
	if len(plaintexts) != 1 {
		return status.Errorf(codes.FailedPrecondition,
			"vault inbox issuance requires exactly one plaintext value, got %d", len(plaintexts))
	}
	return nil
}

// ValidateVaultInboxCredentialOptions requires RandomPassword for inbox delivery
// before account creation. Other recipient types are unaffected.
func ValidateVaultInboxCredentialOptions(configs []*v2.EncryptionConfig, opts *v2.CredentialOptions) error {
	if !hasVaultInboxConfig(configs) {
		return nil
	}
	if opts.WhichOptions() != v2.CredentialOptions_RandomPassword_case {
		return status.Error(codes.InvalidArgument,
			"a vault inbox recipient requires credential options that produce a value")
	}
	return nil
}

// ValidateVaultInboxRotateCredentialOptions allows random-password or unset options
// before rotation; unset lets the connector choose its replacement credential.
// Other recipient types are unaffected.
func ValidateVaultInboxRotateCredentialOptions(configs []*v2.EncryptionConfig, opts *v2.CredentialOptions) error {
	if !hasVaultInboxConfig(configs) {
		return nil
	}
	switch opts.WhichOptions() {
	case v2.CredentialOptions_RandomPassword_case, v2.CredentialOptions_Options_not_set_case:
		return nil
	default:
		return status.Error(codes.InvalidArgument,
			"a vault inbox recipient requires a rotation that produces a value")
	}
}

func HasVaultInboxConfig(configs []*v2.EncryptionConfig) bool {
	return hasVaultInboxConfig(configs)
}

func hasVaultInboxConfig(configs []*v2.EncryptionConfig) bool {
	for _, config := range configs {
		if providers.IsVaultInboxConfig(config) {
			return true
		}
	}
	return false
}

func validateVaultInboxConfigExclusivity(ec []*v2.EncryptionConfig) error {
	vaultInboxConfigs := 0
	for _, config := range ec {
		if providers.IsVaultInboxConfig(config) {
			vaultInboxConfigs++
		}
	}
	if vaultInboxConfigs > 1 || (vaultInboxConfigs == 1 && len(ec) != 1) {
		return status.Error(codes.InvalidArgument,
			"vault inbox encryption config must be the only encryption config")
	}
	return nil
}

// ValidateEncryptionConfigs validates recipients before an irreversible
// credential issuance without changing create/rotate compatibility.
//
// Issuance and the registered-action path call this unconditionally. CreateAccount
// and RotateCredential call it only when a vault-inbox recipient is present
// (see [HasVaultInboxConfig]), so every other recipient type keeps its existing
// create/rotate behaviour.
func ValidateEncryptionConfigs(ec []*v2.EncryptionConfig) error {
	for i, config := range ec {
		if config == nil {
			return status.Errorf(codes.InvalidArgument, "encryption config %d is empty", i)
		}
		provider, err := providers.GetEncryptorForConfig(context.Background(), config)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid encryption config %d: %v", i, err)
		}
		if validator, ok := provider.(providers.EncryptionConfigValidator); ok {
			if err := validator.ValidateConfig(context.Background(), config); err != nil {
				return status.Errorf(codes.InvalidArgument, "invalid encryption config %d: %v", i, err)
			}
		}
	}
	return validateVaultInboxConfigExclusivity(ec)
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
