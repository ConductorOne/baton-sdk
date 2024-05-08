package local

import (
	"context"
	"sync"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	sdkTicket "github.com/conductorone/baton-sdk/pkg/types/ticket"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
)

type localTicket struct {
	dbPath string
	o      sync.Once

	taskId string
}

func (m *localTicket) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = &v1.Task{
			TaskType: &v1.Task_CreateTicketTask_{
				CreateTicketTask: &v1.Task_CreateTicketTask{
				},
			},
		}
	})
	return task, 0, nil
}

func (m *localTicket) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	l := ctxzap.Extract(ctx)
	l.Info("******** PROCESSSSSS")
	cfs := make(map[string]*v2.TicketCustomField)
	l.Info("******** PROCESSSSSS", zap.Any("cc", cc), zap.Any("task", task))
	cfs["project"] = sdkTicket.PickObjectValueField("10000", &v2.TicketCustomFieldObjectValue{
		Id: "10000",
		//DisplayName: "",
	})
	var newAnnotations []*anypb.Any
	resp, err := cc.CreateTicket(ctx, &v2.TicketsServiceCreateTicketRequest{
		Request: &v2.TicketRequest{
			DisplayName:  "yo",
			Description:  "test desc",
			Status:       &v2.TicketStatus{Id: "10001", DisplayName: "idk what status this is"},
			Type:         &v2.TicketType{Id: "10001", DisplayName: "idk what type this is"},
			Labels:       []string{},
			CustomFields: cfs,
			SchemaId:     "schemaId",
		},
		Annotations: newAnnotations,
	})
	if err != nil {
		l.Error("****************  cc.CreateTickee ", zap.Error(err))
		return err
	}
	l.Info("**************** resp", zap.Any("resp", resp))

	return nil
}

// NewTicket returns a task manager that queues an event feed task.
func NewTicket(ctx context.Context) tasks.Manager {
	return &localTicket{}
}
