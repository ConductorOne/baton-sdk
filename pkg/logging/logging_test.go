package logging

import (
	"context"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

func TestSetLogLevelUpdatesActiveLogger(t *testing.T) {
	t.Parallel()

	ctx, err := Init(context.Background(), WithLogLevel("info"))
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	logger := ctxzap.Extract(ctx)
	if checked := logger.Check(zap.DebugLevel, "debug"); checked != nil {
		t.Fatal("debug should be disabled at info level")
	}

	if err := SetLogLevel("debug"); err != nil {
		t.Fatalf("SetLogLevel: %v", err)
	}
	if checked := logger.Check(zap.DebugLevel, "debug"); checked == nil {
		t.Fatal("debug should be enabled after SetLogLevel")
	}
}

func TestSetLogLevelRejectsInvalidLevel(t *testing.T) {
	t.Parallel()

	if err := SetLogLevel("verbose"); err == nil {
		t.Fatal("expected invalid log level to fail")
	}
}
