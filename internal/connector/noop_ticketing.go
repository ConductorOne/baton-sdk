package connector

import (
	"context"
	"errors"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

type noopTicketing struct{}

func (n noopTicketing) CreateTicket(ctx context.Context, request *v2.TicketsServiceCreateTicketRequest) (*v2.TicketsServiceCreateTicketResponse, error) {
	return nil, errors.New("ticketing is not enabled")
}

func (n noopTicketing) GetTicket(ctx context.Context, request *v2.TicketsServiceGetTicketRequest) (*v2.TicketsServiceGetTicketResponse, error) {
	return nil, errors.New("ticketing is not enabled")
}

func (n noopTicketing) ListTicketSchemas(ctx context.Context, request *v2.TicketsServiceListTicketSchemasRequest) (*v2.TicketsServiceListTicketSchemasResponse, error) {
	return nil, errors.New("ticketing is not enabled")
}

func (n noopTicketing) GetTicketSchema(ctx context.Context, request *v2.TicketsServiceGetTicketSchemaRequest) (*v2.TicketsServiceGetTicketSchemaResponse, error) {
	return nil, errors.New("ticketing is not enabled")
}
