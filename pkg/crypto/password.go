package crypto //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// Character classes used by random password generation. Exposed so callers
// (e.g., connectors building their own policies) can reason about the alphabet
// without duplicating the literals.
const (
	UpperCaseLetters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	LowerCaseLetters = "abcdefghijklmnopqrstuvwxyz"
	Digits           = "0123456789"
	Symbols          = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"

	// Internal aliases. Kept lowercase to avoid touching unrelated call sites
	// in this package; new code should reference the exported names above.
	upperCaseLetters = UpperCaseLetters
	lowerCaseLetters = LowerCaseLetters
	digits           = Digits
	symbols          = Symbols
	characters       = UpperCaseLetters + LowerCaseLetters + Digits + Symbols
)

var ErrInvalidCredentialOptions = errors.New("unknown credential options")
var ErrInvalidPasswordLength = errors.New("invalid password length")

func GeneratePassword(ctx context.Context, credentialOptions *v2.LocalCredentialOptions) (string, error) {
	randomPassword := credentialOptions.GetRandomPassword()
	if randomPassword != nil {
		return GenerateRandomPassword(randomPassword)
	}

	plaintextPassword := credentialOptions.GetPlaintextPassword()
	if plaintextPassword != nil {
		return plaintextPassword.GetPlaintextPassword(), nil
	}

	return "", ErrInvalidCredentialOptions
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

func GenerateRandomPassword(randomPassword *v2.LocalCredentialOptions_RandomPassword) (string, error) {
	passwordLength := randomPassword.GetLength()
	if passwordLength < 8 {
		return "", ErrInvalidPasswordLength
	}
	var password strings.Builder

	constraints := randomPassword.GetConstraints()
	if len(constraints) > 0 {
		// apply constraints
		for _, constraint := range constraints {
			for i := int64(0); i < int64(constraint.GetMinCount()); i++ {
				err := addCharacterToPassword(&password, constraint.GetCharSet())
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
