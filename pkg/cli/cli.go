package cli

import (
	"context"
	"time"

	"github.com/conductorone/baton-sdk/pkg/types"
)

type GetConnectorFunc func(context.Context, ConnectorConfig) (types.ConnectorServer, error)

type ConnectorConfig interface {
	GetString(string) string
	GetStringSlice(string) []string
	GetBool(string) bool
	GetInt(string) int
	GetInt32(string) int32
	GetInt64(string) int64
	GetIntSlice(string) []int
	GetTime(string) time.Time
	GetDuration(string) time.Duration
	GetStringMap(key string) map[string]any
	GetStringMapString(key string) map[string]string
}
