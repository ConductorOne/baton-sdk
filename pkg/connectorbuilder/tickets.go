package connectorbuilder

import (
	"context"
	"fmt"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TicketManager extends ConnectorBuilder to add capabilities for ticket management.
//
// Implementing this interface indicates the connector can integrate with an external
// ticketing system, allowing Baton to create and track tickets in that system.
type TicketManager interface {
	ConnectorBuilder
	TicketManagerLimited
}

type TicketManagerLimited interface {
	GetTicket(ctx context.Context, ticketId string) (*v2.Ticket, annotations.Annotations, error)
	CreateTicket(ctx context.Context, ticket *v2.Ticket, schema *v2.TicketSchema) (*v2.Ticket, annotations.Annotations, error)
	GetTicketSchema(ctx context.Context, schemaID string) (*v2.TicketSchema, annotations.Annotations, error)
	ListTicketSchemas(ctx context.Context, pToken *pagination.Token) ([]*v2.TicketSchema, string, annotations.Annotations, error)
	BulkCreateTickets(context.Context, *v2.TicketsServiceBulkCreateTicketsRequest) (*v2.TicketsServiceBulkCreateTicketsResponse, error)
	BulkGetTickets(context.Context, *v2.TicketsServiceBulkGetTicketsRequest) (*v2.TicketsServiceBulkGetTicketsResponse, error)
}

func (b *builder) BulkCreateTickets(ctx context.Context, request *v2.TicketsServiceBulkCreateTicketsRequest) (*v2.TicketsServiceBulkCreateTicketsResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.BulkCreateTickets")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.BulkCreateTicketsType
	if b.ticketManager == nil {
		err := status.Error(codes.Unimplemented, "ticket manager not implemented")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	reqBody := request.GetTicketRequests()
	if len(reqBody) == 0 {
		err := status.Error(codes.InvalidArgument, "request body had no items")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	ticketsResponse, err := b.ticketManager.BulkCreateTickets(ctx, request)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: creating tickets failed: %w", err)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return v2.TicketsServiceBulkCreateTicketsResponse_builder{
		Tickets: ticketsResponse.GetTickets(),
	}.Build(), nil
}

func (b *builder) BulkGetTickets(ctx context.Context, request *v2.TicketsServiceBulkGetTicketsRequest) (*v2.TicketsServiceBulkGetTicketsResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.BulkGetTickets")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.BulkGetTicketsType
	if b.ticketManager == nil {
		err := status.Error(codes.Unimplemented, "ticket manager not implemented")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	reqBody := request.GetTicketRequests()
	if len(reqBody) == 0 {
		err := status.Error(codes.InvalidArgument, "request body had no items")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	ticketsResponse, err := b.ticketManager.BulkGetTickets(ctx, request)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: fetching tickets failed: %w", err)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return v2.TicketsServiceBulkGetTicketsResponse_builder{
		Tickets: ticketsResponse.GetTickets(),
	}.Build(), nil
}

func (b *builder) ListTicketSchemas(ctx context.Context, request *v2.TicketsServiceListTicketSchemasRequest) (*v2.TicketsServiceListTicketSchemasResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListTicketSchemas")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListTicketSchemasType
	if b.ticketManager == nil {
		err := status.Error(codes.Unimplemented, "ticket manager not implemented")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	retryer := retry.NewRetryer(ctx, retry.RetryConfig{
		MaxAttempts:  10,
		InitialDelay: 15 * time.Second,
		MaxDelay:     0,
	})

	for {
		out, nextPageToken, annos, err := b.ticketManager.ListTicketSchemas(ctx, &pagination.Token{
			Size:  int(request.GetPageSize()),
			Token: request.GetPageToken(),
		})
		if err == nil {
			if request.GetPageToken() != "" && request.GetPageToken() == nextPageToken {
				err := status.Error(codes.Internal, "listing ticket schemas failed: next page token unchanged - likely a connector bug")
				b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
				return nil, err
			}

			b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
			return v2.TicketsServiceListTicketSchemasResponse_builder{
				List:          out,
				NextPageToken: nextPageToken,
				Annotations:   annos,
			}.Build(), nil
		}
		if retryer.ShouldWaitAndRetry(ctx, err) {
			continue
		}
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: listing ticket schemas failed: %w", err)
	}
}

func (b *builder) CreateTicket(ctx context.Context, request *v2.TicketsServiceCreateTicketRequest) (*v2.TicketsServiceCreateTicketResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.CreateTicket")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.CreateTicketType
	if b.ticketManager == nil {
		err := status.Error(codes.Unimplemented, "ticket manager not implemented")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	reqBody := request.GetRequest()
	if reqBody == nil {
		err := status.Error(codes.InvalidArgument, "request body is nil")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}
	cTicket := v2.Ticket_builder{
		DisplayName:  reqBody.GetDisplayName(),
		Description:  reqBody.GetDescription(),
		Status:       reqBody.GetStatus(),
		Labels:       reqBody.GetLabels(),
		CustomFields: reqBody.GetCustomFields(),
		RequestedFor: reqBody.GetRequestedFor(),
	}.Build()

	ticket, annos, err := b.ticketManager.CreateTicket(ctx, cTicket, request.GetSchema())
	var resp *v2.TicketsServiceCreateTicketResponse
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		if ticket != nil {
			resp = v2.TicketsServiceCreateTicketResponse_builder{
				Ticket:      ticket,
				Annotations: annos,
			}.Build()
		}
		return resp, fmt.Errorf("error: creating ticket failed: %w", err)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return v2.TicketsServiceCreateTicketResponse_builder{
		Ticket:      ticket,
		Annotations: annos,
	}.Build(), nil
}

func (b *builder) GetTicket(ctx context.Context, request *v2.TicketsServiceGetTicketRequest) (*v2.TicketsServiceGetTicketResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.GetTicket")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.GetTicketType
	if b.ticketManager == nil {
		err := status.Error(codes.Unimplemented, "ticket manager not implemented")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	var resp *v2.TicketsServiceGetTicketResponse
	ticket, annos, err := b.ticketManager.GetTicket(ctx, request.GetId())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		if ticket != nil {
			resp = v2.TicketsServiceGetTicketResponse_builder{
				Ticket:      ticket,
				Annotations: annos,
			}.Build()
		}
		return resp, fmt.Errorf("error: getting ticket failed: %w", err)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return v2.TicketsServiceGetTicketResponse_builder{
		Ticket:      ticket,
		Annotations: annos,
	}.Build(), nil
}

func (b *builder) GetTicketSchema(ctx context.Context, request *v2.TicketsServiceGetTicketSchemaRequest) (*v2.TicketsServiceGetTicketSchemaResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.GetTicketSchema")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.GetTicketSchemaType
	if b.ticketManager == nil {
		err := status.Error(codes.Unimplemented, "ticket manager not implemented")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	ticketSchema, annos, err := b.ticketManager.GetTicketSchema(ctx, request.GetId())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: getting ticket metadata failed: %w", err)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return v2.TicketsServiceGetTicketSchemaResponse_builder{
		Schema:      ticketSchema,
		Annotations: annos,
	}.Build(), nil
}

func (b *builder) addTicketManager(_ context.Context, in interface{}) error {
	if ticketManager, ok := in.(TicketManagerLimited); ok {
		if b.ticketManager != nil {
			return fmt.Errorf("error: cannot set multiple ticket managers")
		}
		b.ticketManager = ticketManager
	}
	return nil
}
