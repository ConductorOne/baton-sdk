//go:build baton_lambda_support

package cli

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

type stubConnectorConfigClient struct {
	configResp        *v1.GetConnectorConfigResponse
	runtimeBundleResp *v1.GetConnectorRuntimeBundleResponse
}

func (s *stubConnectorConfigClient) GetConnectorConfig(context.Context, *v1.GetConnectorConfigRequest, ...grpc.CallOption) (*v1.GetConnectorConfigResponse, error) {
	return s.configResp, nil
}

func (s *stubConnectorConfigClient) GetConnectorRuntimeState(context.Context, *v1.GetConnectorRuntimeStateRequest, ...grpc.CallOption) (*v1.GetConnectorRuntimeStateResponse, error) {
	return &v1.GetConnectorRuntimeStateResponse{}, nil
}

func (s *stubConnectorConfigClient) GetConnectorRuntimeBundle(context.Context, *v1.GetConnectorRuntimeBundleRequest, ...grpc.CallOption) (*v1.GetConnectorRuntimeBundleResponse, error) {
	return s.runtimeBundleResp, nil
}

func (s *stubConnectorConfigClient) GetConnectorOauthToken(context.Context, *v1.GetConnectorOauthTokenRequest, ...grpc.CallOption) (*v1.GetConnectorOauthTokenResponse, error) {
	return &v1.GetConnectorOauthTokenResponse{Token: []byte("token")}, nil
}

func TestFetchConnectorConfigSnapshotIncludesRuntimeSchema(t *testing.T) {
	t.Parallel()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}

	configStruct, err := structpb.NewStruct(map[string]any{
		"runtime-schema": "/runtime/runtime-schema.yaml",
		"jira-url":       "https://example.atlassian.net",
	})
	if err != nil {
		t.Fatalf("NewStruct() error = %v", err)
	}

	configJSON, err := json.Marshal(configStruct)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	encryptedConfig, err := jwk.EncryptED25519(pub, configJSON)
	if err != nil {
		t.Fatalf("EncryptED25519() error = %v", err)
	}

	client := &stubConnectorConfigClient{
		configResp: &v1.GetConnectorConfigResponse{
			Config: encryptedConfig,
			RuntimeState: &v1.RuntimeState{
				EffectiveBundleDigest:       "sha256:bundle",
				RuntimeConfigContractDigest: "sha256:schema",
			},
		},
		runtimeBundleResp: &v1.GetConnectorRuntimeBundleResponse{
			Bundle:        []byte("bundle-bytes"),
			RuntimeSchema: []byte("meta:\n  version: \"1\"\n"),
		},
	}

	snapshot, err := fetchConnectorConfigSnapshot(context.Background(), client, priv)
	if err != nil {
		t.Fatalf("fetchConnectorConfigSnapshot() error = %v", err)
	}

	if got := string(snapshot.RuntimeBundle); got != "bundle-bytes" {
		t.Fatalf("RuntimeBundle = %q, want %q", got, "bundle-bytes")
	}
	if got := string(snapshot.RuntimeSchema); got != "meta:\n  version: \"1\"\n" {
		t.Fatalf("RuntimeSchema = %q, want runtime schema bytes", got)
	}
	if got := snapshot.Config.AsMap()["jira-url"]; got != "https://example.atlassian.net" {
		t.Fatalf("Config[jira-url] = %v, want https://example.atlassian.net", got)
	}
}

var _ v1.ConnectorConfigServiceClient = (*stubConnectorConfigClient)(nil)
var _ oauth2.TokenSource = (*lambdaTokenSource)(nil)
