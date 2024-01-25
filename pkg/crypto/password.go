package crypto

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
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

func GeneratePassword(credentialOptions *v2.CredentialOptions) (string, error) {
	randomPassword := credentialOptions.GetRandomPassword()
	if randomPassword != nil {
		return GenerateRandomPassword(randomPassword)
	}

	return "", ErrInvalidCredentialOptions
}

func GenerateRandomPassword(randomPassword *v2.CredentialOptions_RandomPassword) (string, error) {
	passwordLength := randomPassword.GetLength()
	if passwordLength < 8 {
		return "", ErrInvalidPasswordLength
	}
	var password strings.Builder

	for i := int64(0); i < passwordLength; i++ {
		index, err := rand.Int(rand.Reader, big.NewInt(int64(len(characters))))
		if err != nil {
			return "", fmt.Errorf("failed generating password: %w", err)
		}
		character := characters[index.Int64()]
		err = password.WriteByte(character)
		if err != nil {
			return "", fmt.Errorf("failed generating password: %w", err)
		}
	}
	return password.String(), nil
}
