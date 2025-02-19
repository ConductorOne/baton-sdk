package cli

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// Example struct for configuration
type AppConfig struct {
	Port     int    `mapstructure:"port"`
	Host     string `mapstructure:"host"`
	WithDash string `mapstructure:"with-dash"`
}

// Generic function to unmarshal Viper configuration into a struct of type T
func makeGenericConfiguration[T any](v *viper.Viper) (T, error) {
	var config T // Create a zero-value instance of T

	// Check if T is *viper.Viper and return v directly
	if _, ok := any(config).(*viper.Viper); ok {
		return any(v).(T), nil
	}

	// Get type of T and check if it's a struct or pointer to a struct
	tType := reflect.TypeOf(config)

	isPointer := false
	if tType.Kind() == reflect.Ptr {
		tType = tType.Elem() // Dereference pointer
		isPointer = true
	}

	if tType.Kind() != reflect.Struct {
		return config, fmt.Errorf("T must be a struct or pointer to struct, but got %s", tType.Kind())
	}

	// If T is a struct (not a pointer), create a pointer to it for unmarshalling
	if !isPointer {
		ptr := reflect.New(tType) // Create *T
		err := v.Unmarshal(ptr.Interface())
		if err != nil {
			return config, fmt.Errorf("failed to unmarshal config: %w", err)
		}
		return ptr.Elem().Interface().(T), nil
	}

	// If T is already a pointer to a struct, unmarshal directly
	err := v.Unmarshal(&config)
	if err != nil {
		return config, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return config, nil
}

func TestGenerics(t *testing.T) {
	// Initialize Viper
	v := viper.New()
	v.SetConfigType("yaml")

	// Simulate setting values in Viper (normally read from a file)
	v.Set("port", 8080)
	v.Set("host", "localhost")
	v.Set("with-dash", "foobar")

	// Use the generic function to load config
	config, err := makeGenericConfiguration[AppConfig](v)
	require.NoError(t, err)

	fmt.Printf("Loaded Config: %+v\n", config)
}
