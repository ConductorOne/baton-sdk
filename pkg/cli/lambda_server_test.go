//go:build baton_lambda_support

package cli

import (
	"context"
	"reflect"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/spf13/viper"
)

type testLambdaConnectorServer struct {
	v2.UnimplementedConnectorServiceServer
	v2.UnimplementedResourceTypesServiceServer
	v2.UnimplementedResourcesServiceServer
	v2.UnimplementedEntitlementsServiceServer
	v2.UnimplementedGrantsServiceServer
	v2.UnimplementedAssetServiceServer
	v2.UnimplementedGrantManagerServiceServer
	v2.UnimplementedResourceManagerServiceServer
	v2.UnimplementedResourceDeleterServiceServer
	v2.UnimplementedAccountManagerServiceServer
	v2.UnimplementedCredentialManagerServiceServer
	v2.UnimplementedEventServiceServer
	v2.UnimplementedTicketsServiceServer
	v2.UnimplementedActionServiceServer
	v2.UnimplementedResourceGetterServiceServer
}

type testLambdaConnectorCloseWithoutContext struct {
	testLambdaConnectorServer
	closed bool
}

func (c *testLambdaConnectorCloseWithoutContext) Close() error {
	c.closed = true
	return nil
}

type testLambdaConnectorCloseWithContext struct {
	testLambdaConnectorServer
	closed bool
}

func (c *testLambdaConnectorCloseWithContext) Close(context.Context) error {
	c.closed = true
	return nil
}

func TestCloseLambdaConnectorGenerationSupportsCloseWithoutContext(t *testing.T) {
	connector := &testLambdaConnectorCloseWithoutContext{}

	if err := closeLambdaConnectorGeneration(context.Background(), connector); err != nil {
		t.Fatalf("closeLambdaConnectorGeneration: %v", err)
	}
	if !connector.closed {
		t.Fatal("expected Close() to be called")
	}
}

func TestCloseLambdaConnectorGenerationSupportsCloseWithContext(t *testing.T) {
	connector := &testLambdaConnectorCloseWithContext{}

	if err := closeLambdaConnectorGeneration(context.Background(), connector); err != nil {
		t.Fatalf("closeLambdaConnectorGeneration: %v", err)
	}
	if !connector.closed {
		t.Fatal("expected Close(context.Context) to be called")
	}
}

func TestEffectiveLambdaConfigSyncResourceTypeIDs(t *testing.T) {
	t.Parallel()

	base := viper.New()
	base.Set("sync-resource-types", []string{"fallback"})
	connectorConfig := map[string]any{
		"sync-resource-types": []any{"user", "group"},
	}

	effectiveConfig := effectiveLambdaConfig(base, connectorConfig)

	got := effectiveConfig.GetStringSlice("sync-resource-types")
	want := []string{"user", "group"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sync-resource-types = %#v, want %#v", got, want)
	}
}
