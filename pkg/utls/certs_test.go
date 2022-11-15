package utls

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateMtlsCerts(t *testing.T) {
	ctx := context.Background()
	clientCreds, serverCreds, err := GenerateClientServerCredentials(ctx)
	require.NoError(t, err)
	require.NotNil(t, clientCreds)
	require.NotNil(t, serverCreds)
}
