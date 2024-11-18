package local

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/provisioner"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type localAccountManager struct {
	dbPath string
	o      sync.Once

	login   string
	email   string
	profile *structpb.Struct
}

func (m *localAccountManager) GetTempDir() string {
	return ""
}

func (m *localAccountManager) ShouldDebug() bool {
	return false
}

func (m *localAccountManager) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = &v1.Task{
			TaskType: &v1.Task_CreateAccount{},
		}
	})
	return task, 0, nil
}

func (m *localAccountManager) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "localAccountManager.Process", trace.WithNewRoot())
	defer span.End()

	accountManager := provisioner.NewCreateAccountManager(cc, m.dbPath, m.login, m.email, m.profile)

	err := accountManager.Run(ctx)
	if err != nil {
		return err
	}

	err = accountManager.Close(ctx)
	if err != nil {
		return err
	}

	return nil
}

// NewGranter returns a task manager that queues a sync task.
func NewCreateAccountManager(ctx context.Context, dbPath string, login string, email string, profile *structpb.Struct) tasks.Manager {
	return &localAccountManager{
		dbPath:  dbPath,
		login:   login,
		email:   email,
		profile: profile,
	}
}
