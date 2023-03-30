package tasks

import (
	"context"
	"time"

	"github.com/conductorone/baton-sdk/pkg/types"
)

type Task interface {
	GetTaskType() string
	GetTaskId() string
}

type Manager interface {
	Next(ctx context.Context) (Task, time.Duration, error)
	Run(ctx context.Context, task Task, cc types.ConnectorClient) error
}
