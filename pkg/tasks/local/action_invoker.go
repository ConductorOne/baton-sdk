package local

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	filippoage "filippo.io/age"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/actions"
	ageprovider "github.com/conductorone/baton-sdk/pkg/crypto/providers/age"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/conductorone/baton-sdk/pkg/uotel/uotelzap"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

type localActionInvoker struct {
	dbPath string
	o      sync.Once

	action             string
	resourceTypeID     string // Optional: if set, invokes a resource-scoped action
	args               *structpb.Struct
	encryptionConfigs  []*v2.EncryptionConfig
	decryptionIdentity filippoage.Identity
	setupErr           error
}

func (m *localActionInvoker) GetTempDir() string {
	return ""
}

func (m *localActionInvoker) ShouldDebug() bool {
	return false
}

func (m *localActionInvoker) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	if m.setupErr != nil {
		return nil, 0, m.setupErr
	}
	var task *v1.Task
	m.o.Do(func() {
		task = v1.Task_builder{
			ActionInvoke: v1.Task_ActionInvokeTask_builder{
				Name:              m.action,
				Args:              m.args,
				ResourceTypeId:    m.resourceTypeID,
				EncryptionConfigs: m.encryptionConfigs,
			}.Build(),
		}.Build()
	})
	return task, 0, nil
}

func (m *localActionInvoker) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "localActionInvoker.Process", trace.WithNewRoot())
	ctx = uotelzap.WithSpanLogFields(ctx)
	l := ctxzap.Extract(ctx)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	t := task.GetActionInvoke()
	reqBuilder := v2.InvokeActionRequest_builder{
		Name:              t.GetName(),
		Args:              t.GetArgs(),
		Annotations:       t.GetAnnotations(),
		EncryptionConfigs: t.GetEncryptionConfigs(),
	}
	if resourceTypeID := t.GetResourceTypeId(); resourceTypeID != "" {
		reqBuilder.ResourceTypeId = resourceTypeID
	}
	resp, err := cc.InvokeAction(ctx, reqBuilder.Build())
	if err != nil {
		return err
	}

	status := resp.GetStatus()
	finalResp := resp.GetResponse()
	finalEncryptedData := resp.GetEncryptedData()
	l.Info("ActionInvoke response",
		zap.String("action_id", resp.GetId()),
		zap.String("name", resp.GetName()),
		zap.String("status", resp.GetStatus().String()),
		zap.Any("response", resp.GetResponse()),
	)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for actions.IsInFlight(status) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r, err := cc.GetActionStatus(ctx, &v2.GetActionStatusRequest{
				Id: resp.GetId(),
			})
			if err != nil {
				return fmt.Errorf("failed to poll action status: %w", err)
			}
			status = r.GetStatus()
			finalResp = r.GetResponse()
			finalEncryptedData = r.GetEncryptedData()
		}
	}

	if m.decryptionIdentity != nil {
		finalResp, err = decryptLocalActionResult(finalResp, finalEncryptedData, m.decryptionIdentity)
		if err != nil {
			return err
		}
		finalEncryptedData = nil
	}

	var responseFields map[string]any
	if finalResp != nil {
		responseFields = finalResp.AsMap()
	}
	l.Info("ActionInvoke response", zap.Any("resp", responseFields), zap.Any("encrypted_data", finalEncryptedData))

	if status == v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED {
		return fmt.Errorf("action invoke failed: %v", finalResp)
	}

	return nil
}

// NewActionInvoker returns a task manager that queues an action invoke task.
// If resourceTypeID is provided, it invokes a resource-scoped action.
func NewActionInvoker(ctx context.Context, dbPath string, action string, resourceTypeID string, args *structpb.Struct) tasks.Manager {
	return newActionInvoker(dbPath, action, resourceTypeID, args, nil, false)
}

// NewActionInvokerWithEncryption returns a task manager that supplies
// recipients for encrypted action results.
func NewActionInvokerWithEncryption(
	_ context.Context,
	dbPath string,
	action string,
	resourceTypeID string,
	args *structpb.Struct,
	encryptionConfigs []*v2.EncryptionConfig,
) tasks.Manager {
	return newActionInvoker(dbPath, action, resourceTypeID, args, encryptionConfigs, false)
}

// NewActionInvokerWithCredentialPrinting logs decrypted action credentials.
func NewActionInvokerWithCredentialPrinting(
	_ context.Context,
	dbPath string,
	action string,
	resourceTypeID string,
	args *structpb.Struct,
) tasks.Manager {
	return newActionInvoker(dbPath, action, resourceTypeID, args, nil, true)
}

func newActionInvoker(
	dbPath string,
	action string,
	resourceTypeID string,
	args *structpb.Struct,
	encryptionConfigs []*v2.EncryptionConfig,
	printCredentials bool,
) tasks.Manager {
	if printCredentials {
		identity, err := filippoage.GenerateX25519Identity()
		if err != nil {
			return &localActionInvoker{
				dbPath:         dbPath,
				action:         action,
				resourceTypeID: resourceTypeID,
				args:           args,
				setupErr:       fmt.Errorf("failed to generate local action encryption identity: %w", err),
			}
		}
		encryptionConfig := v2.EncryptionConfig_builder{
			Provider: ageprovider.EncryptionProviderAge,
			AgeRecipientConfig: v2.EncryptionConfig_AgeRecipientConfig_builder{
				Recipient: identity.Recipient().String(),
			}.Build(),
		}.Build()
		return &localActionInvoker{
			dbPath:             dbPath,
			action:             action,
			resourceTypeID:     resourceTypeID,
			args:               args,
			encryptionConfigs:  []*v2.EncryptionConfig{encryptionConfig},
			decryptionIdentity: identity,
		}
	}
	return &localActionInvoker{
		dbPath:            dbPath,
		action:            action,
		resourceTypeID:    resourceTypeID,
		args:              args,
		encryptionConfigs: encryptionConfigs,
	}
}

func decryptLocalActionResult(
	response *structpb.Struct,
	encryptedData []*v2.EncryptedData,
	identity filippoage.Identity,
) (*structpb.Struct, error) {
	fields := make(map[string]any, len(response.GetFields())+len(encryptedData))
	if response != nil {
		for name, value := range response.AsMap() {
			fields[name] = value
		}
	}
	for _, encrypted := range encryptedData {
		if encrypted.GetProvider() != ageprovider.EncryptionProviderAge {
			return nil, fmt.Errorf("unsupported local action encryption provider %q", encrypted.GetProvider())
		}
		reader, err := filippoage.Decrypt(bytes.NewReader(encrypted.GetEncryptedBytes()), identity)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt local action result %q: %w", encrypted.GetName(), err)
		}
		plaintext, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read local action result %q: %w", encrypted.GetName(), err)
		}
		var value any
		if encrypted.GetSchema() != "" {
			if err := json.Unmarshal(plaintext, &value); err != nil {
				return nil, fmt.Errorf("failed to decode local action result %q: %w", encrypted.GetName(), err)
			}
		} else {
			value = string(plaintext)
		}
		fields[encrypted.GetName()] = value
	}
	rv, err := structpb.NewStruct(fields)
	if err != nil {
		return nil, fmt.Errorf("failed to build local action response: %w", err)
	}
	return rv, nil
}
