//go:build baton_lambda_support

package cli

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/field"
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

type testLambdaStringMapConfig struct {
	UserCustomFields map[string]any `mapstructure:"user-custom-fields"`
}

func (c *testLambdaStringMapConfig) GetStringSlice(string) []string {
	return nil
}

func (c *testLambdaStringMapConfig) GetString(string) string {
	return ""
}

func (c *testLambdaStringMapConfig) GetInt(string) int {
	return 0
}

func (c *testLambdaStringMapConfig) GetBool(string) bool {
	return false
}

func (c *testLambdaStringMapConfig) GetStringMap(fieldName string) map[string]any {
	if fieldName == "user-custom-fields" {
		return c.UserCustomFields
	}
	return nil
}

func TestCloseLambdaConnectorGenerationSupportsCloseWithoutContext(t *testing.T) {
	connector := &testLambdaConnectorCloseWithoutContext{}

	require.NoError(t, closeLambdaConnectorGeneration(context.Background(), connector))
	require.True(t, connector.closed, "expected Close() to be called")
}

func TestCloseLambdaConnectorGenerationSupportsCloseWithContext(t *testing.T) {
	connector := &testLambdaConnectorCloseWithContext{}

	require.NoError(t, closeLambdaConnectorGeneration(context.Background(), connector))
	require.True(t, connector.closed, "expected Close(context.Context) to be called")
}

func TestEffectiveLambdaConfigSyncResourceTypeIDs(t *testing.T) {
	t.Parallel()

	base := viper.New()
	base.Set("sync-resource-types", []string{"fallback"})
	connectorConfig := map[string]any{
		"sync-resource-types": []any{"user", "group"},
	}

	effectiveConfig := effectiveLambdaConfig(base, connectorConfig, field.Configuration{})

	got := effectiveConfig.GetStringSlice("sync-resource-types")
	want := []string{"user", "group"}
	require.Equal(t, want, got, "sync-resource-types")
}

func TestEffectiveLambdaConfigPreservesStringMapKeyCaseForViper(t *testing.T) {
	t.Parallel()

	base := viper.New()
	connectorConfig := map[string]any{
		"user-custom-fields": map[string]any{
			"departmentNav/name": "Department",
			"customString10":     "Custom String 10",
			"isActive":           true,
		},
	}
	schema := field.NewConfiguration([]field.SchemaField{
		field.StringMapField("user-custom-fields"),
	})

	effectiveConfig := effectiveLambdaConfig(base, connectorConfig, schema)

	gotStringMap := effectiveConfig.GetStringMap("user-custom-fields")
	wantStringMap := map[string]any{
		"departmentNav/name": "Department",
		"customString10":     "Custom String 10",
		"isActive":           "true",
	}
	require.Equal(t, wantStringMap, gotStringMap, "GetStringMap(user-custom-fields)")

	gotStringMapString := effectiveConfig.GetStringMapString("user-custom-fields")
	wantStringMapString := map[string]string{
		"departmentNav/name": "Department",
		"customString10":     "Custom String 10",
		"isActive":           "true",
	}
	require.Equal(t, wantStringMapString, gotStringMapString, "GetStringMapString(user-custom-fields)")
}

func TestMakeLambdaConnectorConfigurationPreservesStringMapKeyCase(t *testing.T) {
	t.Parallel()

	base := viper.New()
	connectorConfig := map[string]any{
		"user-custom-fields": map[string]any{
			"departmentNav/name": "Department",
			"customString10":     "Custom String 10",
		},
	}

	schema := field.NewConfiguration([]field.SchemaField{
		field.StringMapField("user-custom-fields"),
	})
	config, err := makeLambdaConnectorConfiguration[*testLambdaStringMapConfig](base, effectiveLambdaConfig(base, connectorConfig, schema), connectorConfig)
	require.NoError(t, err, "makeLambdaConnectorConfiguration")

	got := config.GetStringMap("user-custom-fields")
	want := map[string]any{
		"departmentNav/name": "Department",
		"customString10":     "Custom String 10",
	}
	require.Equal(t, want, got, "user-custom-fields")
}
