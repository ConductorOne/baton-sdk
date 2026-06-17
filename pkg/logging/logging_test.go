package logging

import (
	"context"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSetLogLevelUpdatesActiveLogger(t *testing.T) {
	t.Parallel()

	ctx, err := Init(context.Background(), WithLogLevel("info"))
	require.NoError(t, err, "Init")
	logger := ctxzap.Extract(ctx)
	require.Nil(t, logger.Check(zap.DebugLevel, "debug"), "debug should be disabled at info level")

	require.NoError(t, SetLogLevel("debug"), "SetLogLevel")
	require.NotNil(t, logger.Check(zap.DebugLevel, "debug"), "debug should be enabled after SetLogLevel")
}

func TestSetLogLevelRejectsInvalidLevel(t *testing.T) {
	t.Parallel()

	err := SetLogLevel("verbose")
	require.Error(t, err, "expected invalid log level to fail")
}
