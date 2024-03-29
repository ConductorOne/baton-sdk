package crypto

import (
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestGeneratePassword(t *testing.T) {
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

	opts = &v2.CredentialOptions{
		Options: &v2.CredentialOptions_RandomPassword_{
			RandomPassword: &v2.CredentialOptions_RandomPassword{
				Length: 7,
			},
		},
	}
	p, err = GeneratePassword(opts)
	require.Error(t, err)
	require.Empty(t, p)
	require.Equal(t, ErrInvalidPasswordLength, err)
}
