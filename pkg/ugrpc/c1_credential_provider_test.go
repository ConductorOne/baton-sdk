package ugrpc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSecret(t *testing.T) {
	badSecrets := []string{
		"v1:1234567890:1234567890:1234567890",
		"v1:1234567890:1234567890:1234567890:1234567890",
		"secret-token:conductorone.com:v1:eyJrdHkiOjJPS1AiLCJjcnYiOiJFZDI1NTE5IiwieCI6InpwM1BXY2NTQlZPdGhTTnN5ck1zVGVFaDlpTjhTWTNnSDZfcDBEUkZaTWMi" +
			"LCJkIjoielkzTzFybnItQUV3NW0wbVpCTzBjSEZpWWoxTmlSdk1pU1Y1X1Ffa0tiZyJ9",
	}
	for _, badSecret := range badSecrets {
		secretBytes := []byte(badSecret)
		secretKey, err := ParseSecret(secretBytes)
		require.Error(t, err)
		require.Nil(t, secretKey)
	}

	// This credential was generated in a dev environment and then revoked. It is only useful for testing parsing.
	validSecret := "secret-token:conductorone.com:v1:eyJrdHkiOiJPS1AiLCJjcnYiOiJFZDI1NTE5IiwieCI6InpwM1BXY2NTQlZPdGhTTnN5ck1zVGVFaDlpTjhTWTNnSDZfcDBEUkZaTWMi" +
		"LCJkIjoielkzTzFybnItQUV3NW0wbVpCTzBjSEZpWWoxTmlSdk1pU1Y1X1Ffa0tiZyJ9"
	secretBytes := []byte(validSecret)
	secretKey, err := ParseSecret(secretBytes)
	require.NoError(t, err)
	require.NotNil(t, secretKey)
}
