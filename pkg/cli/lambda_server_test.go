//go:build baton_lambda_support

package cli

import (
	"context"
	"reflect"
	"testing"

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
	StringMapField map[string]any `mapstructure:"string-map-field"`
	IntField       int            `mapstructure:"int-field"`
}

func (c *testLambdaStringMapConfig) GetStringSlice(string) []string {
	return nil
}

func (c *testLambdaStringMapConfig) GetString(string) string {
	return ""
}

func (c *testLambdaStringMapConfig) GetInt(fieldName string) int {
	if c != nil && fieldName == "int-field" {
		return c.IntField
	}
	return 0
}

func (c *testLambdaStringMapConfig) GetBool(string) bool {
	return false
}

func (c *testLambdaStringMapConfig) GetStringMap(fieldName string) map[string]any {
	if fieldName == "string-map-field" {
		return c.StringMapField
	}
	return nil
}

type testLambdaValueConfig struct {
	StringField string `mapstructure:"string-field"`
}

func (c testLambdaValueConfig) GetStringSlice(string) []string {
	return nil
}

func (c testLambdaValueConfig) GetString(fieldName string) string {
	if fieldName == "string-field" {
		return c.StringField
	}
	return ""
}

func (c testLambdaValueConfig) GetInt(string) int {
	return 0
}

func (c testLambdaValueConfig) GetBool(string) bool {
	return false
}

func (c testLambdaValueConfig) GetStringMap(string) map[string]any {
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
	schema := field.NewConfiguration(nil)

	effectiveConfig, err := effectiveLambdaConfig(base, connectorConfig, schema)
	if err != nil {
		t.Fatalf("effectiveLambdaConfig: %v", err)
	}

	got := effectiveConfig.GetStringSlice("sync-resource-types")
	want := []string{"user", "group"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sync-resource-types = %#v, want %#v", got, want)
	}
}

func TestMakeLambdaConnectorConfigurationPreservesStringMapKeyCase(t *testing.T) {
	t.Parallel()

	base := viper.New()
	connectorConfig := map[string]any{
		"string-map-field": map[string]any{
			"CompanyID": "123",
			"userName":  "alice",
			"URLToken":  "secret",
		},
	}
	schema := field.NewConfiguration([]field.SchemaField{
		field.StringMapField("string-map-field"),
	})
	effectiveConfig, err := effectiveLambdaConfig(base, connectorConfig, schema)
	if err != nil {
		t.Fatalf("effectiveLambdaConfig: %v", err)
	}

	config, err := makeLambdaConnectorConfiguration[*testLambdaStringMapConfig](base, effectiveConfig, connectorConfig)
	if err != nil {
		t.Fatalf("makeLambdaConnectorConfiguration: %v", err)
	}

	got := config.GetStringMap("string-map-field")
	want := map[string]any{
		"CompanyID": "123",
		"userName":  "alice",
		"URLToken":  "secret",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("string-map-field = %#v, want %#v", got, want)
	}
}

func TestMakeLambdaConnectorConfigurationReplacesBaseStringMap(t *testing.T) {
	t.Parallel()

	base := viper.New()
	base.Set("string-map-field", map[string]any{
		"fallback":  "unused",
		"CompanyID": "old",
	})
	connectorConfig := map[string]any{
		"string-map-field": map[string]any{
			"CompanyID": "123",
		},
	}
	schema := field.NewConfiguration([]field.SchemaField{
		field.StringMapField("string-map-field"),
	})
	effectiveConfig, err := effectiveLambdaConfig(base, connectorConfig, schema)
	if err != nil {
		t.Fatalf("effectiveLambdaConfig: %v", err)
	}

	config, err := makeLambdaConnectorConfiguration[*testLambdaStringMapConfig](base, effectiveConfig, connectorConfig)
	if err != nil {
		t.Fatalf("makeLambdaConnectorConfiguration: %v", err)
	}

	got := config.GetStringMap("string-map-field")
	want := map[string]any{"CompanyID": "123"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("string-map-field = %#v, want %#v", got, want)
	}
}

func TestMakeLambdaConnectorConfigurationClearsBaseStringMap(t *testing.T) {
	t.Parallel()

	base := viper.New()
	base.Set("string-map-field", map[string]any{
		"fallback": "unused",
	})
	connectorConfig := map[string]any{
		"string-map-field": map[string]any{},
	}
	schema := field.NewConfiguration([]field.SchemaField{
		field.StringMapField("string-map-field"),
	})
	effectiveConfig, err := effectiveLambdaConfig(base, connectorConfig, schema)
	if err != nil {
		t.Fatalf("effectiveLambdaConfig: %v", err)
	}

	config, err := makeLambdaConnectorConfiguration[*testLambdaStringMapConfig](base, effectiveConfig, connectorConfig)
	if err != nil {
		t.Fatalf("makeLambdaConnectorConfiguration: %v", err)
	}

	got := config.GetStringMap("string-map-field")
	want := map[string]any{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("string-map-field = %#v, want %#v", got, want)
	}
}

func TestMakeLambdaConnectorConfigurationRemoteValueOverridesInvalidBase(t *testing.T) {
	t.Parallel()

	base := viper.New()
	base.Set("int-field", "not-an-int")
	connectorConfig := map[string]any{
		"int-field": 42,
	}
	schema := field.NewConfiguration([]field.SchemaField{
		field.IntField("int-field"),
	})
	effectiveConfig, err := effectiveLambdaConfig(base, connectorConfig, schema)
	if err != nil {
		t.Fatalf("effectiveLambdaConfig: %v", err)
	}

	config, err := makeLambdaConnectorConfiguration[*testLambdaStringMapConfig](base, effectiveConfig, connectorConfig)
	if err != nil {
		t.Fatalf("makeLambdaConnectorConfiguration: %v", err)
	}

	if got, want := config.GetInt("int-field"), 42; got != want {
		t.Fatalf("int-field = %d, want %d", got, want)
	}
}

func TestMakeLambdaConnectorConfigurationSupportsValueConfig(t *testing.T) {
	t.Parallel()

	base := viper.New()
	connectorConfig := map[string]any{
		"string-field": "configured",
	}
	schema := field.NewConfiguration([]field.SchemaField{
		field.StringField("string-field"),
	})
	effectiveConfig, err := effectiveLambdaConfig(base, connectorConfig, schema)
	if err != nil {
		t.Fatalf("effectiveLambdaConfig: %v", err)
	}

	config, err := makeLambdaConnectorConfiguration[testLambdaValueConfig](base, effectiveConfig, connectorConfig)
	if err != nil {
		t.Fatalf("makeLambdaConnectorConfiguration: %v", err)
	}

	if got, want := config.GetString("string-field"), "configured"; got != want {
		t.Fatalf("string-field = %q, want %q", got, want)
	}
}

func TestMakeLambdaConnectorConfigurationPreservesStringMapKeyCaseForViperConfig(t *testing.T) {
	t.Parallel()

	base := viper.New()
	connectorConfig := map[string]any{
		"string-map-field": map[string]any{
			"CompanyID": "123",
			"userName":  "alice",
			"URLToken":  "secret",
		},
	}
	schema := field.NewConfiguration([]field.SchemaField{
		field.StringMapField("string-map-field"),
	})
	effectiveConfig, err := effectiveLambdaConfig(base, connectorConfig, schema)
	if err != nil {
		t.Fatalf("effectiveLambdaConfig: %v", err)
	}

	config, err := makeLambdaConnectorConfiguration[*viper.Viper](base, effectiveConfig, connectorConfig)
	if err != nil {
		t.Fatalf("makeLambdaConnectorConfiguration: %v", err)
	}

	got := config.GetStringMap("string-map-field")
	want := map[string]any{
		"CompanyID": "123",
		"userName":  "alice",
		"URLToken":  "secret",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("string-map-field = %#v, want %#v", got, want)
	}
}
