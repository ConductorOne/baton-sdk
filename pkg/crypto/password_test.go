package crypto

import (
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestGeneratePassword(t *testing.T) {
	t.Run("should generate password with specified length", func(t *testing.T) {
		opts := &v2.CredentialOptions{
			Options: &v2.CredentialOptions_RandomPassword_{
				RandomPassword: &v2.CredentialOptions_RandomPassword{
					Length: 12,
				},
			},
		}
		p, err := GeneratePassword(opts)
		require.NoError(t, err)
		require.Len(t, p, 12)
	})
	t.Run("should error if specified length is lower than 8", func(t *testing.T) {
		opts := &v2.CredentialOptions{
			Options: &v2.CredentialOptions_RandomPassword_{
				RandomPassword: &v2.CredentialOptions_RandomPassword{
					Length: 7,
				},
			},
		}
		p, err := GeneratePassword(opts)
		require.Error(t, err)
		require.Empty(t, p)
		require.Equal(t, ErrInvalidPasswordLength, err)
	})
	t.Run("should comply with minimum characters validation", func(t *testing.T) {
		opts := &v2.CredentialOptions{
			Options: &v2.CredentialOptions_RandomPassword_{
				RandomPassword: &v2.CredentialOptions_RandomPassword{
					Length: 8,
				},
			},
		}
		p, err := GeneratePassword(opts)
		require.NoError(t, err)
		isValid := isPasswordValid(p)
		require.True(t, isValid)
	})

	t.Run("should comply with minimum characters from password constraints", func(t *testing.T) {
		minCount := 4
		constraintDigit := v2.PasswordConstraint{
			CharSet:  digits,
			MinCount: uint32(minCount),
		}
		constraints := []*v2.PasswordConstraint{
			&constraintDigit,
		}
		opts := &v2.CredentialOptions{
			Options: &v2.CredentialOptions_RandomPassword_{
				RandomPassword: &v2.CredentialOptions_RandomPassword{
					Length:      8,
					Constraints: constraints,
				},
			},
		}
		p, err := GeneratePassword(opts)
		require.NoError(t, err)
		occurences := countOccurences(p, digits)
		require.GreaterOrEqual(t, occurences, minCount)
		require.Len(t, p, 8)
	})

	t.Run("error when sum of min counts exceeds length", func(t *testing.T) {
		constraintDigit := v2.PasswordConstraint{
			CharSet:  digits,
			MinCount: 9,
		}
		constraints := []*v2.PasswordConstraint{
			&constraintDigit,
		}
		opts := &v2.CredentialOptions{
			Options: &v2.CredentialOptions_RandomPassword_{
				RandomPassword: &v2.CredentialOptions_RandomPassword{
					Length:      8,
					Constraints: constraints,
				},
			},
		}
		_, err := GeneratePassword(opts)
		require.Error(t, err)
	})
}

func countOccurences(password string, charset string) int {
	total := 0
	for _, c := range password {
		if strings.ContainsRune(charset, c) {
			total++
		}
	}
	return total
}

func isPasswordValid(password string) bool {
	var hasUpper, hasLower, hasDigit, hasSpecial bool

	for _, c := range password {
		switch {
		case unicode.IsUpper(c):
			hasUpper = true
		case unicode.IsLower(c):
			hasLower = true
		case unicode.IsDigit(c):
			hasDigit = true
		case strings.ContainsRune(symbols, c):
			hasSpecial = true
		}
		if hasUpper && hasLower && hasDigit && hasSpecial {
			return true
		}
	}

	return hasUpper && hasLower && hasDigit && hasSpecial
}
