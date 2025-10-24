package local

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/conductorone/baton-sdk/pkg/types/resource"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	sdkTicket "github.com/conductorone/baton-sdk/pkg/types/ticket"
)

type localBulkCreateTicket struct {
	o sync.Once

	templatePath string
}

type bulkCreateTicketTemplate struct {
	Tickets []ticketTemplate `json:"tickets"`
}

func (m *localBulkCreateTicket) loadTicketTemplate(ctx context.Context) (*bulkCreateTicketTemplate, error) {
	tbytes, err := os.ReadFile(m.templatePath)
	if err != nil {
		return nil, err
	}

	template := &bulkCreateTicketTemplate{}
	err = json.Unmarshal(tbytes, template)
	if err != nil {
		return nil, err
	}

	return template, nil
}

func (m *localBulkCreateTicket) GetTempDir() string {
	return ""
}

func (m *localBulkCreateTicket) ShouldDebug() bool {
	return false
}

func (m *localBulkCreateTicket) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = v1.Task_builder{
			BulkCreateTickets: &v1.Task_BulkCreateTicketsTask{},
		}.Build()
	})
	return task, 0, nil
}

func (m *localBulkCreateTicket) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "localBulkCreateTicket.Process", trace.WithNewRoot())
	defer span.End()

	l := ctxzap.Extract(ctx)

	templates, err := m.loadTicketTemplate(ctx)
	if err != nil {
		return err
	}

	ticketReqs := make([]*v2.TicketsServiceCreateTicketRequest, 0)
	for _, template := range templates.Tickets {
		schema, err := cc.GetTicketSchema(ctx, v2.TicketsServiceGetTicketSchemaRequest_builder{
			Id: template.SchemaID,
		}.Build())
		if err != nil {
			return err
		}

		ticketRequestBody := v2.TicketRequest_builder{
			DisplayName: template.DisplayName,
			Description: template.Description,
			Labels:      template.Labels,
		}.Build()

		if template.StatusId != "" {
			ticketRequestBody.SetStatus(v2.TicketStatus_builder{
				Id: template.StatusId,
			}.Build())
		}

		if template.RequestedForId != "" {
			rt := resource.NewResourceType("User", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER})
			requestedUser, err := resource.NewUserResource(template.RequestedForId, rt, template.RequestedForId, []resource.UserTraitOption{})
			if err != nil {
				return err
			}
			ticketRequestBody.SetRequestedFor(requestedUser)
		}

		cfs := make(map[string]*v2.TicketCustomField)
		for k, v := range template.CustomFields {
			newCfs, err := sdkTicket.CustomFieldForSchemaField(k, schema.GetSchema(), v)
			if err != nil {
				return err
			}
			cfs[k] = newCfs
		}
		ticketRequestBody.SetCustomFields(cfs)

		ticketReqs = append(ticketReqs, v2.TicketsServiceCreateTicketRequest_builder{
			Request: ticketRequestBody,
			Schema:  schema.GetSchema(),
		}.Build())
	}

	bulkTicketReq := v2.TicketsServiceBulkCreateTicketsRequest_builder{
		TicketRequests: ticketReqs,
	}.Build()

	resp, err := cc.BulkCreateTickets(ctx, bulkTicketReq)
	if err != nil {
		return err
	}

	l.Info("created tickets", zap.Any("resp", resp))

	return nil
}

// NewBulkTicket returns a task manager that queues a bulk create ticket task.
func NewBulkTicket(ctx context.Context, templatePath string) tasks.Manager {
	return &localBulkCreateTicket{
		templatePath: templatePath,
	}
}

type localCreateTicket struct {
	o sync.Once

	templatePath string
}

type ticketTemplate struct {
	SchemaID       string                 `json:"schema_id"`
	StatusId       string                 `json:"status_id"`
	DisplayName    string                 `json:"display_name"`
	Description    string                 `json:"description"`
	Labels         []string               `json:"labels"`
	CustomFields   map[string]interface{} `json:"custom_fields"`
	RequestedForId string                 `json:"requested_for_id"`
}

func (m *localCreateTicket) loadTicketTemplate(ctx context.Context) (*ticketTemplate, error) {
	tbytes, err := os.ReadFile(m.templatePath)
	if err != nil {
		return nil, err
	}

	template := &ticketTemplate{}
	err = json.Unmarshal(tbytes, template)
	if err != nil {
		return nil, err
	}

	return template, nil
}

func (m *localCreateTicket) GetTempDir() string {
	return ""
}

func (m *localCreateTicket) ShouldDebug() bool {
	return false
}

func (m *localCreateTicket) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = v1.Task_builder{
			CreateTicketTask: &v1.Task_CreateTicketTask{},
		}.Build()
	})
	return task, 0, nil
}

func (m *localCreateTicket) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	l := ctxzap.Extract(ctx)

	template, err := m.loadTicketTemplate(ctx)
	if err != nil {
		return err
	}

	schema, err := cc.GetTicketSchema(ctx, v2.TicketsServiceGetTicketSchemaRequest_builder{
		Id: template.SchemaID,
	}.Build())
	if err != nil {
		return err
	}

	ticketRequestBody := v2.TicketRequest_builder{
		DisplayName: template.DisplayName,
		Description: template.Description,
		Labels:      template.Labels,
	}.Build()

	if template.StatusId != "" {
		ticketRequestBody.SetStatus(v2.TicketStatus_builder{
			Id: template.StatusId,
		}.Build())
	}

	if template.RequestedForId != "" {
		rt := resource.NewResourceType("User", []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER})
		requestedUser, err := resource.NewUserResource(template.RequestedForId, rt, template.RequestedForId, []resource.UserTraitOption{})
		if err != nil {
			return err
		}
		ticketRequestBody.SetRequestedFor(requestedUser)
	}

	cfs := make(map[string]*v2.TicketCustomField)
	for k, v := range template.CustomFields {
		newCfs, err := sdkTicket.CustomFieldForSchemaField(k, schema.GetSchema(), v)
		if err != nil {
			return err
		}
		cfs[k] = newCfs
	}
	ticketRequestBody.SetCustomFields(cfs)
	ticketReq := v2.TicketsServiceCreateTicketRequest_builder{
		Request: ticketRequestBody,
		Schema:  schema.GetSchema(),
	}.Build()

	resp, err := cc.CreateTicket(ctx, ticketReq)
	if err != nil {
		return err
	}

	l.Info("created ticket", zap.Any("resp", resp))

	return nil
}

// NewTicket returns a task manager that queues a create ticket task.
func NewTicket(ctx context.Context, templatePath string) tasks.Manager {
	return &localCreateTicket{
		templatePath: templatePath,
	}
}

// Get ticket task.
type localGetTicket struct {
	o        sync.Once
	ticketId string
}

func (m *localGetTicket) GetTempDir() string {
	return ""
}

func (m *localGetTicket) ShouldDebug() bool {
	return false
}

func (m *localGetTicket) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = v1.Task_builder{
			GetTicket: v1.Task_GetTicketTask_builder{
				TicketId: m.ticketId,
			}.Build(),
		}.Build()
	})
	return task, 0, nil
}

func (m *localGetTicket) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	l := ctxzap.Extract(ctx)

	resp, err := cc.GetTicket(ctx, v2.TicketsServiceGetTicketRequest_builder{
		Id: m.ticketId,
	}.Build())
	if err != nil {
		return err
	}

	l.Info("ticket", zap.Any("resp", resp))

	return nil
}

// NewGetTicket returns a task manager that queues a get ticket task.
func NewGetTicket(ctx context.Context, ticketId string) tasks.Manager {
	return &localGetTicket{
		ticketId: ticketId,
	}
}

type localListTicketSchemas struct {
	o sync.Once
}

func (m *localListTicketSchemas) GetTempDir() string {
	return ""
}

func (m *localListTicketSchemas) ShouldDebug() bool {
	return false
}

func (m *localListTicketSchemas) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = v1.Task_builder{
			ListTicketSchemas: &v1.Task_ListTicketSchemasTask{},
		}.Build()
	})
	return task, 0, nil
}

func (m *localListTicketSchemas) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	l := ctxzap.Extract(ctx)

	resp, err := cc.ListTicketSchemas(ctx, &v2.TicketsServiceListTicketSchemasRequest{})
	if err != nil {
		return err
	}

	l.Info("Ticket Schemas", zap.Any("resp", resp))

	return nil
}

// NewSchema returns a task manager that queues a list schema task.
func NewListTicketSchema(ctx context.Context) tasks.Manager {
	return &localListTicketSchemas{}
}
