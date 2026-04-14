package c1api

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
)

type debugHandler struct {
	taskmanager *c1ApiTaskManager
}

func newStartDebugging(tm *c1ApiTaskManager) *debugHandler {
	return &debugHandler{taskmanager: tm}
}

func (c *debugHandler) HandleTask(ctx context.Context) error {
	_, span := tracer.Start(ctx, "debugHandler.HandleTask")
	span.SetAttributes(attribute.String("task_type", "debug"))
	defer span.End()

	c.taskmanager.runnerShouldDebug = true
	return nil
}
