package local

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type localEventFeed struct {
	o sync.Once
}

const EventsPerPageLocally = 100

func (m *localEventFeed) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = &v1.Task{
			TaskType: &v1.Task_EventFeed{},
		}
	})
	return task, 0, nil
}

func (m *localEventFeed) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	var pageToken string
	for {
		resp, err := cc.ListEvents(ctx, &v2.ListEventsRequest{
			PageSize: EventsPerPageLocally,
			Cursor:   pageToken,
			StartAt:  task.GetEventFeed().GetStartAt(),
		})
		if err != nil {
			return err
		}
		for _, event := range resp.GetEvents() {
			bytes, err := protojson.Marshal(event)
			if err != nil {
				return err
			}
			//nolint:forbidigo
			fmt.Println(string(bytes))
		}
		pageToken = resp.GetCursor()
		if !resp.GetHasMore() {
			break
		}
	}

	return nil
}

// NewEventFeed returns a task manager that queues an event feed task.
func NewEventFeed(ctx context.Context) tasks.Manager {
	return &localEventFeed{}
}
