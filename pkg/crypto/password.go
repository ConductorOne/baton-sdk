package crypto

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/go-jose/go-jose/v4"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	upperCaseLetters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	lowerCaseLetters = "abcdefghijklmnopqrstuvwxyz"
	digits           = "0123456789"
	symbols          = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
	characters       = upperCaseLetters + lowerCaseLetters + digits + symbols
)

var ErrInvalidCredentialOptions = errors.New("unknown credential options")
var ErrInvalidPasswordLength = errors.New("invalid password length")

func GeneratePassword(ctx context.Context, credentialOptions *v2.CredentialOptions, decryptionConfig *providers.DecryptionConfig) (string, error) {
	randomPassword := credentialOptions.GetRandomPassword()
	if randomPassword != nil {
		return GenerateRandomPassword(randomPassword)
	}

	encryptedPassword := credentialOptions.GetEncryptedPassword()
	if encryptedPassword != nil {
		return DecryptPassword(ctx, encryptedPassword, decryptionConfig)
	}

	return "", ErrInvalidCredentialOptions
}

func DecryptPassword(ctx context.Context, encryptedPassword *v2.CredentialOptions_EncryptedPassword, decryptionConfig *providers.DecryptionConfig) (string, error) {
	if decryptionConfig == nil {
		return "", ErrInvalidCredentialOptions
	}

	provider, err := providers.GetDecryptionProviderForConfig(ctx, decryptionConfig)
	if err != nil {
		return "", status.Errorf(codes.Internal, "error getting decryption provider for config: %v", err)
	}
	key := decryptionConfig.PrivateKey
	if len(key) == 0 {
		return "", status.Errorf(codes.InvalidArgument, "decryption config key is empty")
	}
	secret := &jose.JSONWebKey{}
	err = secret.UnmarshalJSON(key)
	if err != nil {
		return "", status.Errorf(codes.Internal, "error unmarshalling secret: %v", err)
	}

	plaintext, err := provider.Decrypt(ctx, encryptedPassword.GetEncryptedPassword(), secret)
	if err != nil {
		return "", status.Errorf(codes.Internal, "error decrypting password: %v", err)
	}

	return string(plaintext.Bytes), nil
}

func addBasicValidityCharacters(password *strings.Builder) error {
	sets := []string{upperCaseLetters, lowerCaseLetters, digits, symbols}
	for _, characterSet := range sets {
		err := addCharacterToPassword(password, characterSet)
		if err != nil {
			return err
		}
	}
	return nil
}

func addCharacterToPassword(password *strings.Builder, set string) error {
	index, err := rand.Int(rand.Reader, big.NewInt(int64(len(set))))
	if err != nil {
		return fmt.Errorf("failed to generate password: %w", err)
	}
	character := set[index.Int64()]
	err = password.WriteByte(character)
	if err != nil {
		return fmt.Errorf("failed to generate password: %w", err)
	}
	return nil
}

func GenerateRandomPassword(randomPassword *v2.CredentialOptions_RandomPassword) (string, error) {
	passwordLength := randomPassword.GetLength()
	if passwordLength < 8 {
		return "", ErrInvalidPasswordLength
	}
	var password strings.Builder

	constraints := randomPassword.Constraints
	if len(constraints) > 0 {
		// apply constraints
		for _, constraint := range constraints {
			for i := int64(0); i < int64(constraint.MinCount); i++ {
				err := addCharacterToPassword(&password, constraint.CharSet)
				if err != nil {
					return "", err
				}
			}
		}
	} else {
		err := addBasicValidityCharacters(&password)
		if err != nil {
			return "", err
		}
	}

	remaining := passwordLength - int64(len(password.String()))
	if remaining < 0 {
		return "", fmt.Errorf("password length %d is less than the sum of constraints minimums (%d)", passwordLength, int64(password.Len()))
	}
	for i := int64(0); i < remaining; i++ {
		err := addCharacterToPassword(&password, characters)
		if err != nil {
			return "", err
		}
	}
	stringPassword, err := getShuffledPassword(password)
	if err != nil {
		return "", err
	}
	return stringPassword, nil
}

func getShuffledPassword(password strings.Builder) (string, error) {
	runes := []rune(password.String())
	n := len(runes)
	for i := n - 1; i > 0; i-- {
		jBig, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			return "", fmt.Errorf("error while generating password")
		}
		j := int(jBig.Int64())
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes), nil
}
