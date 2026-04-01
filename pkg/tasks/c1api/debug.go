package c1api

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/uotel"
)

type debugHandler struct {
	taskmanager *c1ApiTaskManager
}

func newStartDebugging(tm *c1ApiTaskManager) *debugHandler {
	return &debugHandler{taskmanager: tm}
}

func (c *debugHandler) HandleTask(ctx context.Context) (err error) {
	_, span := tracer.Start(ctx, "debugHandler.HandleTask")
	defer func() { uotel.EndSpanWithError(span, err) }()

	c.taskmanager.runnerShouldDebug = true
	return nil
}
