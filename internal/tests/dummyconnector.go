package tests

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
)

type Dummy struct{}

func (d *Dummy) ResourceSyncers(_ context.Context) []connectorbuilder.ResourceSyncer {
	return nil
}

func (d *Dummy) Metadata(_ context.Context) (*v2.ConnectorMetadata, error) {
	return nil, nil
}

func (d *Dummy) Validate(_ context.Context) (annotations.Annotations, error) {
	return nil, nil
}

func NewDummy() *Dummy {
	return &Dummy{}
}
