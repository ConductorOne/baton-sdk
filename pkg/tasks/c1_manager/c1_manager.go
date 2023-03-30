package c1_manager

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/service_mode/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/davecgh/go-spew/spew"
)

type c1TaskManager struct {
	serviceClient v1.ConnectorWorkServiceClient
}

func (c *c1TaskManager) Next(ctx context.Context) (tasks.Task, error) {
	return nil, nil
}

func (c *c1TaskManager) Finish(ctx context.Context, taskID string) error {
	return nil
}

func (c *c1TaskManager) Add(ctx context.Context, task tasks.Task) error {
	return nil
}

func NewC1TaskManager(ctx context.Context, clientID string, clientSecret string) (tasks.Manager, error) {
	serviceClient, err := newServiceClient(ctx, clientID, clientSecret)
	if err != nil {
		return nil, err
	}

	resp, err := serviceClient.Hello(ctx, &v1.HelloRequest{
		ConnectorMetadata: &v2.ConnectorMetadata{
			DisplayName: "baton-sdk",
		},
	})
	if err != nil {
		return nil, err
	}
	spew.Dump(resp)

	return &c1TaskManager{
		serviceClient: serviceClient,
	}, nil
}
