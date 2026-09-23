package local

import (
	"context"
	"testing"

	filippoage "filippo.io/age"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	ageprovider "github.com/conductorone/baton-sdk/pkg/crypto/providers/age"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestLocalActionInvokerSuppliesEncryptionAndDecryptsResult(t *testing.T) {
	ctx := context.Background()
	manager := NewActionInvokerWithCredentialPrinting(ctx, "test.c1z", "issue_credentials", "", nil)
	invoker, ok := manager.(*localActionInvoker)
	require.True(t, ok)
	require.NotNil(t, invoker.decryptionIdentity)

	task, _, err := invoker.Next(ctx)
	require.NoError(t, err)
	require.Len(t, task.GetActionInvoke().GetEncryptionConfigs(), 1)
	config := task.GetActionInvoke().GetEncryptionConfigs()[0]
	require.Equal(t, ageprovider.EncryptionProviderAge, config.GetProvider())

	encrypted, err := (&ageprovider.RecipientEncryptionProvider{}).Encrypt(ctx, config, v2.PlaintextData_builder{
		Name:   "credentials",
		Schema: `{"type":"object"}`,
		Bytes:  []byte(`{"access_key_id":"test-access-key","session_token":"test-session-token"}`),
	}.Build())
	require.NoError(t, err)
	public, err := structpb.NewStruct(map[string]any{"request_id": "request-1"})
	require.NoError(t, err)

	response, err := decryptLocalActionResult(public, []*v2.EncryptedData{encrypted}, invoker.decryptionIdentity)
	require.NoError(t, err)
	require.Equal(t, "request-1", response.GetFields()["request_id"].GetStringValue())
	credentials := response.GetFields()["credentials"].GetStructValue().GetFields()
	require.Equal(t, "test-access-key", credentials["access_key_id"].GetStringValue())
	require.Equal(t, "test-session-token", credentials["session_token"].GetStringValue())
}

func TestLocalActionInvokerDoesNotRequestCredentialsByDefault(t *testing.T) {
	manager := NewActionInvoker(t.Context(), "test.c1z", "issue_credentials", "", nil)
	invoker, ok := manager.(*localActionInvoker)
	require.True(t, ok)
	require.Nil(t, invoker.decryptionIdentity)

	task, _, err := invoker.Next(t.Context())
	require.NoError(t, err)
	require.Empty(t, task.GetActionInvoke().GetEncryptionConfigs())
}

func TestLocalActionInvokerPreservesExplicitEncryptionConfigs(t *testing.T) {
	identity, err := filippoage.GenerateX25519Identity()
	require.NoError(t, err)
	config := v2.EncryptionConfig_builder{
		Provider: ageprovider.EncryptionProviderAge,
		AgeRecipientConfig: v2.EncryptionConfig_AgeRecipientConfig_builder{
			Recipient: identity.Recipient().String(),
		}.Build(),
	}.Build()

	manager := NewActionInvokerWithEncryption(context.Background(), "test.c1z", "issue_credentials", "", nil, []*v2.EncryptionConfig{config})
	invoker, ok := manager.(*localActionInvoker)
	require.True(t, ok)
	require.Nil(t, invoker.decryptionIdentity)

	task, _, err := invoker.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, []*v2.EncryptionConfig{config}, task.GetActionInvoke().GetEncryptionConfigs())
}

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
