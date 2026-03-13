package connectorbuilder

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CredentialManager extends ResourceSyncer to add capabilities for managing credentials.
// Implementing this interface indicates the connector supports rotating credentials
// for resources of the associated type. This is commonly used for user accounts
// or service accounts that have rotatable credentials.
type CredentialManager interface {
	ResourceSyncer
	CredentialManagerLimited
}

type CredentialManagerLimited interface {
	Rotate(ctx context.Context,
		resourceId *v2.ResourceId,
		credentialOptions *v2.LocalCredentialOptions) ([]*v2.PlaintextData, annotations.Annotations, error)
	RotateCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsCredentialRotation, annotations.Annotations, error)
}

type OldCredentialManager interface {
	Rotate(ctx context.Context,
		resourceId *v2.ResourceId,
		credentialOptions *v2.CredentialOptions) ([]*v2.PlaintextData, annotations.Annotations, error)
}

func (b *builder) RotateCredential(ctx context.Context, request *v2.RotateCredentialRequest) (*v2.RotateCredentialResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.RotateCredential")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.RotateCredentialsType
	l := ctxzap.Extract(ctx)
	rt := request.GetResourceId().GetResourceType()
	manager, ok := b.credentialManagers[rt]
	if !ok {
		l.Error("error: resource type does not have credential manager configured", zap.String("resource_type", rt))
		err := status.Error(codes.Unimplemented, "resource type does not have credential manager configured")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	opts, err := crypto.ConvertCredentialOptions(ctx, b.clientSecret, request.GetCredentialOptions(), request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: converting credential options failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: converting credential options failed: %w", err)
	}

	plaintexts, annos, err := manager.Rotate(ctx, request.GetResourceId(), opts)
	if err != nil {
		l.Error("error: rotate credentials on resource failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: rotate credentials on resource failed: %w", err)
	}

	pkem, err := crypto.NewEncryptionManager(request.GetCredentialOptions(), request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: creating encryption manager failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: creating encryption manager failed: %w", err)
	}

	var encryptedDatas []*v2.EncryptedData
	for _, plaintextCredential := range plaintexts {
		encryptedData, err := pkem.Encrypt(ctx, plaintextCredential)
		if err != nil {
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
			return nil, err
		}
		encryptedDatas = append(encryptedDatas, encryptedData...)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return v2.RotateCredentialResponse_builder{
		Annotations:   annos,
		ResourceId:    request.GetResourceId(),
		EncryptedData: encryptedDatas,
	}.Build(), nil
}

func (b *builder) addCredentialManager(_ context.Context, typeId string, in interface{}) error {
	if _, ok := in.(OldCredentialManager); ok {
		return fmt.Errorf("error: old credential manager interface implemented for %s", typeId)
	}

	if credentialManagers, ok := in.(CredentialManagerLimited); ok {
		if _, ok := b.credentialManagers[typeId]; ok {
			return fmt.Errorf("error: duplicate resource type found for credential manager %s", typeId)
		}
		b.credentialManagers[typeId] = credentialManagers
	}
	return nil
}
