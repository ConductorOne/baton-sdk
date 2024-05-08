package local

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	sdkTicket "github.com/conductorone/baton-sdk/pkg/types/ticket"
)

type localTicket struct {
	o sync.Once

	templatePath string
}

type ticketTemplate struct {
	SchemaID     string                 `json:"schema_id"`
	StatusId     string                 `json:"status_id"`
	TypeId       string                 `json:"type_id"`
	DisplayName  string                 `json:"display_name"`
	Description  string                 `json:"description"`
	Labels       []string               `json:"labels"`
	CustomFields map[string]interface{} `json:"custom_fields"`
}

func (m *localTicket) loadTicketTemplate(ctx context.Context) (*ticketTemplate, error) {
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

func (m *localTicket) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = &v1.Task{
			TaskType: &v1.Task_CreateTicketTask_{
				CreateTicketTask: &v1.Task_CreateTicketTask{},
			},
		}
	})
	return task, 0, nil
}

func (m *localTicket) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	l := ctxzap.Extract(ctx)

	template, err := m.loadTicketTemplate(ctx)
	if err != nil {
		return err
	}

	schema, err := cc.GetTicketSchema(ctx, &v2.TicketsServiceGetTicketSchemaRequest{
		Id: template.SchemaID,
	})
	if err != nil {
		return err
	}

	ticketRequestBody := &v2.TicketRequest{
		DisplayName: template.DisplayName,
		Description: template.Description,
		Status: &v2.TicketStatus{
			Id: template.StatusId,
		},
		Type: &v2.TicketType{
			Id: template.TypeId,
		},
		Labels:   template.Labels,
		SchemaId: schema.Schema.GetId(),
	}

	cfs := make(map[string]*v2.TicketCustomField)
	for k, v := range template.CustomFields {
		newCfs, err := sdkTicket.CustomFieldForSchemaField(k, schema.Schema, v)
		if err != nil {
			return err
		}
		cfs[k] = newCfs
	}
	ticketRequestBody.CustomFields = cfs
	ticketReq := &v2.TicketsServiceCreateTicketRequest{
		Request: ticketRequestBody,
	}

	resp, err := cc.CreateTicket(ctx, ticketReq)
	if err != nil {
		return err
	}

	l.Info("created ticket", zap.Any("resp", resp))

	return nil
}

// NewTicket returns a task manager that queues a create ticket task.
func NewTicket(ctx context.Context, templatePath string) tasks.Manager {
	return &localTicket{
		templatePath: templatePath,
	}
}
