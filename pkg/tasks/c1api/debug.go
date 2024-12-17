package c1api

import (
	"context"
)

type debugHandler struct {
	taskmanager *C1ApiTaskManager
}

func newStartDebugging(tm *C1ApiTaskManager) *debugHandler {
	return &debugHandler{taskmanager: tm}
}

func (c *debugHandler) HandleTask(ctx context.Context) error {
	c.taskmanager.runnerShouldDebug = true
	return nil
}
