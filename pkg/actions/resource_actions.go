package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type ResourceTypeActionRegistry interface {
	Register(ctx context.Context, schema *v2.ResourceActionSchema, handler ResourceActionHandler) error
}

// ResourceActionHandler is the function signature for handling resource actions.
type ResourceActionHandler func(
	ctx context.Context,
	args *structpb.Struct,
) (*structpb.Struct, annotations.Annotations, error)

// ResourceActionManager manages resource-scoped actions.
// It extends ActionManager to support actions that are scoped to specific resource types.
type ResourceActionManager struct {
	*ActionManager
	resourceActions map[string]map[string]ResourceActionHandler    // resourceTypeID -> actionName -> handler
	resourceSchemas map[string]map[string]*v2.ResourceActionSchema // resourceTypeID -> actionName -> schema
	mu              sync.RWMutex
}

// NewResourceActionManager creates a new ResourceActionManager.
func NewResourceActionManager(ctx context.Context) *ResourceActionManager {
	return &ResourceActionManager{
		ActionManager:   NewActionManager(ctx),
		resourceActions: make(map[string]map[string]ResourceActionHandler),
		resourceSchemas: make(map[string]map[string]*v2.ResourceActionSchema),
	}
}

type resourceTypeActionRegistry struct {
	resourceTypeID string
	actionManager  *ResourceActionManager
}

// RegisterResourceAction registers a resource action for a specific resource type.
func (r *resourceTypeActionRegistry) Register(
	ctx context.Context,
	schema *v2.ResourceActionSchema,
	handler ResourceActionHandler,
) error {
	if r.resourceTypeID == "" {
		return errors.New("resource type ID cannot be empty")
	}

	if schema == nil {
		return errors.New("action schema cannot be nil")
	}
	if schema.Name == "" {
		return errors.New("action schema name cannot be empty")
	}
	if handler == nil {
		return fmt.Errorf("handler cannot be nil for action %s", schema.Name)
	}

	r.actionManager.mu.Lock()
	defer r.actionManager.mu.Unlock()

	schema.ResourceTypeId = r.resourceTypeID

	if r.actionManager.resourceSchemas[r.resourceTypeID] == nil {
		r.actionManager.resourceSchemas[r.resourceTypeID] = make(map[string]*v2.ResourceActionSchema)
	}
	if r.actionManager.resourceActions[r.resourceTypeID] == nil {
		r.actionManager.resourceActions[r.resourceTypeID] = make(map[string]ResourceActionHandler)
	}

	// Check for duplicate action names
	if _, ok := r.actionManager.resourceSchemas[r.resourceTypeID][schema.Name]; ok {
		return fmt.Errorf("action schema %s already registered for resource type %s", schema.Name, r.resourceTypeID)
	}

	// Check for duplicate action types
	if len(schema.ActionType) > 0 {
		for existingName, existingSchema := range r.actionManager.resourceSchemas[r.resourceTypeID] {
			if existingSchema == nil || len(existingSchema.ActionType) == 0 {
				continue
			}
			// Check if any ActionType in the new schema matches any in existing schemas
			for _, newActionType := range schema.ActionType {
				if newActionType == v2.ActionType_ACTION_TYPE_UNSPECIFIED || newActionType == v2.ActionType_ACTION_TYPE_DYNAMIC {
					continue // Skip unspecified and dynamic types as they can overlap
				}
				for _, existingActionType := range existingSchema.ActionType {
					if newActionType == existingActionType {
						return fmt.Errorf("action type %s already registered for resource type %s (existing action: %s)", newActionType.String(), r.resourceTypeID, existingName)
					}
				}
			}
		}
	}

	r.actionManager.resourceSchemas[r.resourceTypeID][schema.Name] = schema
	r.actionManager.resourceActions[r.resourceTypeID][schema.Name] = handler

	ctxzap.Extract(ctx).Info("registered resource action", zap.String("resource_type", r.resourceTypeID), zap.String("action_name", schema.Name))

	return nil
}

func (r *ResourceActionManager) GetTypeRegistry(ctx context.Context, resourceTypeID string) (ResourceTypeActionRegistry, error) {
	if resourceTypeID == "" {
		return nil, errors.New("resource type ID cannot be empty")
	}

	return &resourceTypeActionRegistry{resourceTypeID: resourceTypeID, actionManager: r}, nil
}

// ListResourceActions returns all resource actions for a given resource type, optionally filtered by resource ID.
func (r *ResourceActionManager) ListResourceActions(
	ctx context.Context,
	resourceTypeID string,
	resourceID *v2.ResourceId,
) ([]*v2.ResourceActionSchema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if resourceTypeID != "" {
		schemas, ok := r.resourceSchemas[resourceTypeID]
		if !ok {
			return []*v2.ResourceActionSchema{}, nil
		}

		result := make([]*v2.ResourceActionSchema, 0, len(schemas))
		for _, schema := range schemas {
			result = append(result, schema)
		}
		return result, nil
	}

	// If no resource type specified, return all
	result := make([]*v2.ResourceActionSchema, 0)
	for _, schemas := range r.resourceSchemas {
		for _, schema := range schemas {
			result = append(result, schema)
		}
	}
	return result, nil
}

// decryptSecretFields decrypts secret fields in the args struct based on the schema.
func (r *ResourceActionManager) decryptSecretFields(
	ctx context.Context,
	schema []*config.Field,
	args *structpb.Struct,
	encryptionConfigs []*v2.EncryptionConfig,
) (*structpb.Struct, error) {
	if args == nil || len(encryptionConfigs) == 0 {
		return args, nil
	}

	l := ctxzap.Extract(ctx)
	// Create a copy of the struct
	result := proto.Clone(args).(*structpb.Struct)

	// Build a map of secret field names
	secretFields := make(map[string]bool)
	for _, field := range schema {
		if field.IsSecret {
			secretFields[field.Name] = true
		}
	}

	if len(secretFields) == 0 {
		return result, nil
	}

	// Decrypt secret fields
	for fieldName := range secretFields {
		fieldValue, ok := result.Fields[fieldName]
		if !ok {
			continue
		}

		// The field value should be an EncryptedData message encoded as a struct
		encryptedDataStruct, ok := fieldValue.GetKind().(*structpb.Value_StructValue)
		if !ok {
			l.Warn("secret field is not a struct, skipping decryption", zap.String("field", fieldName))
			continue
		}

		// Convert struct to EncryptedData
		encryptedDataBytes, err := encryptedDataStruct.StructValue.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal encrypted data for field %s: %w", fieldName, err)
		}

		var encryptedData v2.EncryptedData
		if err := json.Unmarshal(encryptedDataBytes, &encryptedData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal encrypted data for field %s: %w", fieldName, err)
		}

		// For decryption, we need the private key which should be available from context
		// or from the encryption config. For now, we'll try to get it from context.
		// If decryption is not possible, we'll pass through the encrypted value.
		// The handler can decide how to handle it, or we can enhance this later.

		// Try to get private key from context (similar to how accounts.go does it)
		// For now, if we can't decrypt, we'll skip and let the handler deal with it
		// In a full implementation, you'd extract the key ID from encryptedData and
		// match it with a private key from context or config
		l.Debug("skipping decryption for secret field - decryption from encryption configs not yet implemented", zap.String("field", fieldName))
		// For now, pass through the encrypted value as-is
		// TODO: Implement proper decryption using private key from context or config
	}

	return result, nil
}

// encryptSecretFields encrypts secret fields in the response struct based on the schema.
func (r *ResourceActionManager) encryptSecretFields(
	ctx context.Context,
	schema []*config.Field,
	response *structpb.Struct,
	encryptionConfigs []*v2.EncryptionConfig,
) (*structpb.Struct, error) {
	if response == nil || len(encryptionConfigs) == 0 {
		return response, nil
	}

	l := ctxzap.Extract(ctx)
	// Create a copy of the struct
	result := proto.Clone(response).(*structpb.Struct)

	// Build a map of secret field names
	secretFields := make(map[string]bool)
	for _, field := range schema {
		if field.IsSecret {
			secretFields[field.Name] = true
		}
	}

	if len(secretFields) == 0 {
		return result, nil
	}

	// Create encryption manager
	em, err := crypto.NewEncryptionManager(nil, encryptionConfigs)
	if err != nil {
		return nil, fmt.Errorf("failed to create encryption manager: %w", err)
	}

	// Encrypt secret fields
	for fieldName := range secretFields {
		fieldValue, ok := result.Fields[fieldName]
		if !ok {
			continue
		}

		// Get the plaintext string value
		stringValue, ok := fieldValue.GetKind().(*structpb.Value_StringValue)
		if !ok {
			l.Warn("secret field is not a string, skipping encryption", zap.String("field", fieldName))
			continue
		}

		// Create PlaintextData
		plaintextData := &v2.PlaintextData{
			Name:        fieldName,
			Description: fmt.Sprintf("Secret field %s", fieldName),
			Schema:      "string",
			Bytes:       []byte(stringValue.StringValue),
		}

		// Encrypt
		encryptedDatas, err := em.Encrypt(ctx, plaintextData)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt field %s: %w", fieldName, err)
		}

		if len(encryptedDatas) == 0 {
			return nil, fmt.Errorf("no encrypted data returned for field %s", fieldName)
		}

		// Convert first encrypted data to struct
		encryptedDataBytes, err := json.Marshal(encryptedDatas[0])
		if err != nil {
			return nil, fmt.Errorf("failed to marshal encrypted data for field %s: %w", fieldName, err)
		}

		var encryptedDataStruct map[string]interface{}
		if err := json.Unmarshal(encryptedDataBytes, &encryptedDataStruct); err != nil {
			return nil, fmt.Errorf("failed to unmarshal encrypted data struct for field %s: %w", fieldName, err)
		}

		encryptedStruct, err := structpb.NewStruct(encryptedDataStruct)
		if err != nil {
			return nil, fmt.Errorf("failed to create struct for encrypted data for field %s: %w", fieldName, err)
		}

		// Replace the plaintext value with encrypted struct
		result.Fields[fieldName] = structpb.NewStructValue(encryptedStruct)
	}

	return result, nil
}

// InvokeResourceAction invokes a resource action.
func (r *ResourceActionManager) InvokeResourceAction(
	ctx context.Context,
	resourceTypeID string,
	actionName string,
	args *structpb.Struct,
	encryptionConfigs []*v2.EncryptionConfig,
) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error) {
	if resourceTypeID == "" {
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.InvalidArgument, "resource type ID is required")
	}
	if actionName == "" {
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.InvalidArgument, "action name is required")
	}

	r.mu.RLock()
	handlers, ok := r.resourceActions[resourceTypeID]
	if !ok {
		r.mu.RUnlock()
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.NotFound, fmt.Sprintf("no actions found for resource type %s", resourceTypeID))
	}

	handler, ok := handlers[actionName]
	if !ok {
		r.mu.RUnlock()
		return "",
			v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED,
			nil,
			nil,
			status.Error(codes.NotFound, fmt.Sprintf("handler for action %s not found for resource type %s", actionName, resourceTypeID))
	}

	schemas, ok := r.resourceSchemas[resourceTypeID]
	if !ok {
		r.mu.RUnlock()
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.Internal, fmt.Sprintf("schemas not found for resource type %s", resourceTypeID))
	}

	schema, ok := schemas[actionName]
	if !ok {
		r.mu.RUnlock()
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.Internal, fmt.Sprintf("schema not found for action %s", actionName))
	}
	r.mu.RUnlock()

	// Decrypt secret input fields
	decryptedArgs, err := r.decryptSecretFields(ctx, schema.Arguments, args, encryptionConfigs)
	if err != nil {
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, fmt.Errorf("failed to decrypt secret fields: %w", err)
	}

	oa := r.GetNewAction(actionName)
	done := make(chan struct{})

	// Invoke handler in goroutine
	go func() {
		oa.SetStatus(ctx, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING)
		handlerCtx, cancel := context.WithTimeoutCause(context.Background(), 1*time.Hour, errors.New("action handler timed out"))
		defer cancel()
		var oaErr error
		oa.Rv, oa.Annos, oaErr = handler(handlerCtx, decryptedArgs)
		if oaErr == nil {
			oa.SetStatus(ctx, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE)
		} else {
			oa.SetError(ctx, oaErr)
		}
		done <- struct{}{}
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		// Encrypt secret output fields
		encryptedResponse, err := r.encryptSecretFields(ctx, schema.ReturnTypes, oa.Rv, encryptionConfigs)
		if err != nil {
			return oa.Id, oa.Status, oa.Rv, oa.Annos, fmt.Errorf("failed to encrypt secret fields: %w", err)
		}
		return oa.Id, oa.Status, encryptedResponse, oa.Annos, nil
	case <-time.After(1 * time.Second):
		return oa.Id, oa.Status, oa.Rv, oa.Annos, nil
	case <-ctx.Done():
		oa.SetError(ctx, ctx.Err())
		return oa.Id, oa.Status, oa.Rv, oa.Annos, ctx.Err()
	}
}
