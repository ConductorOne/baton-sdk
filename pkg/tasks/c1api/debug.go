package c1api

import "context"

type debugHandler struct{}

func newSetLogFilePathTaskHandler() *debugHandler {
	return &debugHandler{}
}

func (c *debugHandler) HandleTask(ctx context.Context) error {
	// NOTE(shackra): update context here?
	return nil
}
