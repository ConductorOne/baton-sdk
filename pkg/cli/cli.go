package cli

import (
	"context"
	"fmt"
	"reflect"

	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/spf13/viper"
)

type GetConnectorFunc[T any] func(context.Context, *T) (types.ConnectorServer, error)

func MakeGenericConfiguration[T any](v *viper.Viper) (*T, error) {
	// Create an instance of the struct type T using reflection
	var config T // Create a zero-value instance of T
	// Ensure T is a struct (or pointer to struct)
	tType := reflect.TypeOf(config)
	if tType == reflect.TypeOf(viper.Viper{}) {
		return any(v).(*T), nil
	}
	if tType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("T must be a struct, but got %s", tType.Kind())
	}
	// Unmarshal into the config struct
	err := v.Unmarshal(&config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	return &config, nil
}
