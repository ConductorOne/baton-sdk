package connectorbuilder

import (
	"bytes"
	"context"
	"testing"
	"time"

	filippoage "filippo.io/age"
	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// Connectors return *actions.ActionManager as their deprecated
// CustomActionManager (baton-microsoft-entra, baton-zendesk, ...), so its
// method set must keep satisfying that interface.
var _ CustomActionManager = (*actions.ActionManager)(nil)

// testBlockingGlobalActionProvider registers one action whose handler blocks
// until release is closed.
type testBlockingGlobalActionProvider struct {
	ConnectorBuilder
	release chan struct{}
}

func (t *testBlockingGlobalActionProvider) GlobalActions(ctx context.Context, registry actions.ActionRegistry) error {
	schema := v2.BatonActionSchema_builder{
		Name:        "blocking-action",
		DisplayName: "Blocking Action",
	}.Build()
	handler := func(hctx context.Context, _ *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
		select {
		case <-t.release:
		case <-hctx.Done():
		}
		return &structpb.Struct{}, nil, nil
	}
	return registry.Register(ctx, schema, handler)
}

// The inline_wait request field must reach the action manager: a blocking
// action invoked with a three-second wait returns RUNNING no earlier than
// that, while an unset field keeps the default short wait.
func TestInvokeActionThreadsInlineWaitFromRequest(t *testing.T) {
	ctx := t.Context()

	provider := &testBlockingGlobalActionProvider{
		ConnectorBuilder: newTestConnector([]ResourceSyncer{}),
		release:          make(chan struct{}),
	}
	t.Cleanup(func() { close(provider.release) })

	connector, err := NewConnector(ctx, provider)
	require.NoError(t, err)

	start := time.Now()
	resp, err := connector.InvokeAction(ctx, v2.InvokeActionRequest_builder{
		Name:       "blocking-action",
		Args:       &structpb.Struct{},
		InlineWait: durationpb.New(3 * time.Second),
	}.Build())
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.NotEmpty(t, resp.GetId())
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, resp.GetStatus())
	require.GreaterOrEqual(t, elapsed, 3*time.Second)

	// An unset field keeps the default wait: the ceiling only has to exclude
	// the explicit three-second wait above, leaving two seconds of slack over
	// the one-second nominal.
	start = time.Now()
	resp, err = connector.InvokeAction(ctx, v2.InvokeActionRequest_builder{
		Name: "blocking-action",
		Args: &structpb.Struct{},
	}.Build())
	elapsed = time.Since(start)

	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, resp.GetStatus())
	require.GreaterOrEqual(t, elapsed, time.Second)
	require.Less(t, elapsed, 3*time.Second)
}

// The wire ceiling rejects waits that are almost certainly caller bugs;
// values at or below it are the server-side clamp's business, and the
// lenient non-positive contract survives validation.
func TestInlineWaitValidationCeiling(t *testing.T) {
	cases := []struct {
		name    string
		wait    *durationpb.Duration
		wantErr bool
	}{
		{"unset passes", nil, false},
		{"negative is rejected", durationpb.New(-time.Second), true},
		{"zero passes", durationpb.New(0), false},
		{"at the ceiling passes", durationpb.New(24 * time.Hour), false},
		{"beyond the ceiling is rejected", durationpb.New(24*time.Hour + time.Second), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := v2.InvokeActionRequest_builder{
				Name:       "bounded-action",
				InlineWait: tc.wait,
			}.Build()
			err := req.Validate()
			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "InlineWait")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type testSecretGlobalActionProvider struct {
	ConnectorBuilder
}

func (t *testSecretGlobalActionProvider) GlobalActions(ctx context.Context, registry actions.ActionRegistry) error {
	schema := v2.BatonActionSchema_builder{
		Name: "issue-token",
		ReturnTypes: []*config.Field{
			config.Field_builder{
				Name:        "token",
				StringField: &config.StringField{},
				IsSecret:    true,
			}.Build(),
		},
	}.Build()
	handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
		return &structpb.Struct{}, []*v2.PlaintextData{
			v2.PlaintextData_builder{Name: "token", Bytes: []byte("connector-secret")}.Build(),
		}, nil, nil
	}
	return actions.RegisterWithSecrets(ctx, registry, schema, handler)
}

func TestInvokeActionReturnsEncryptedDataOnInvokeAndStatus(t *testing.T) {
	ctx := t.Context()
	identity, err := filippoage.GenerateHybridIdentity()
	require.NoError(t, err)
	encryptionConfig := v2.EncryptionConfig_builder{
		AgeRecipientConfig: v2.EncryptionConfig_AgeRecipientConfig_builder{
			Recipient: identity.Recipient().String(),
		}.Build(),
	}.Build()

	connector, err := NewConnector(ctx, &testSecretGlobalActionProvider{
		ConnectorBuilder: newTestConnector([]ResourceSyncer{}),
	})
	require.NoError(t, err)

	invokeResponse, err := connector.InvokeAction(ctx, v2.InvokeActionRequest_builder{
		Name:              "issue-token",
		EncryptionConfigs: []*v2.EncryptionConfig{encryptionConfig},
	}.Build())
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, invokeResponse.GetStatus())
	require.Len(t, invokeResponse.GetEncryptedData(), 1)
	require.NotContains(t, invokeResponse.GetResponse().GetFields(), "token")

	statusResponse, err := connector.GetActionStatus(ctx, v2.GetActionStatusRequest_builder{Id: invokeResponse.GetId()}.Build())
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, statusResponse.GetStatus())
	require.Len(t, statusResponse.GetEncryptedData(), 1)
	require.Equal(t, invokeResponse.GetEncryptedData()[0].GetEncryptedBytes(), statusResponse.GetEncryptedData()[0].GetEncryptedBytes())

	reader, err := filippoage.Decrypt(bytes.NewReader(statusResponse.GetEncryptedData()[0].GetEncryptedBytes()), identity)
	require.NoError(t, err)
	var plaintext bytes.Buffer
	_, err = plaintext.ReadFrom(reader)
	require.NoError(t, err)
	require.Equal(t, "connector-secret", plaintext.String())
}
