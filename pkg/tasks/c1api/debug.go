package c1api

import (
	"context"
)

type debugHandler struct {
	taskmanager *c1ApiTaskManager
}

func newStartDebugging(tm *c1ApiTaskManager) *debugHandler {
	return &debugHandler{taskmanager: tm}
}

func (c *debugHandler) HandleTask(ctx context.Context) error {
	_, span := tracer.Start(ctx, "debugHandler.HandleTask")
	defer span.End()

	c.taskmanager.runnerShouldDebug = true
	return nil
}
