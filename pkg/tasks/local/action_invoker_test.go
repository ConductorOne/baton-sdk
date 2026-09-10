package local

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestActionInvokerThreadsEncryptionConfigs(t *testing.T) {
	encryptionConfig := v2.EncryptionConfig_builder{Provider: "test-provider"}.Build()
	manager := NewActionInvokerWithEncryption(
		t.Context(),
		"sync.c1z",
		"issue-token",
		"",
		nil,
		[]*v2.EncryptionConfig{encryptionConfig},
	)

	task, _, err := manager.Next(t.Context())
	require.NoError(t, err)
	require.Len(t, task.GetActionInvoke().GetEncryptionConfigs(), 1)
	require.True(t, proto.Equal(encryptionConfig, task.GetActionInvoke().GetEncryptionConfigs()[0]))
}
