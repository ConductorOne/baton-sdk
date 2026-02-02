package c1api

import (
	"context"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
)

const (
	defaultDebugDuration = time.Hour * 24
)

type debugHandler struct {
	task        *v1.Task
	taskmanager *c1ApiTaskManager
}

func newStartDebuggingTaskHandler(task *v1.Task, tm *c1ApiTaskManager) *debugHandler {
	return &debugHandler{task: task, taskmanager: tm}
}

func (c *debugHandler) HandleTask(ctx context.Context) error {
	_, span := tracer.Start(ctx, "debugHandler.HandleTask")
	defer span.End()

	// (if value provided use it)
	c.taskmanager.runnerDebugExpiresAt = time.Now().Add(defaultDebugDuration)
	return nil
}
