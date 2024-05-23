package metrics

import (
	"time"

	"github.com/conductorone/baton-sdk/pkg/tasks"
)

type Handler interface {
	WithConnectorName(name string) Handler
	WithTags(tags map[string]string) Handler
	RecordTaskSuccess(task tasks.TaskType, dur time.Duration)
	RecordTaskFailure(task tasks.TaskType, dur time.Duration)
}

type noopHandler struct{}

func (noopHandler) WithConnectorName(name string) Handler {
	return noopHandler{}
}

func (noopHandler) WithTags(tags map[string]string) Handler {
	return noopHandler{}
}

func (noopHandler) RecordTaskSuccess(task tasks.TaskType, dur time.Duration) {
	return
}

func (noopHandler) RecordTaskFailure(task tasks.TaskType, dur time.Duration) {
	return
}

var _ Handler = noopHandler{}
