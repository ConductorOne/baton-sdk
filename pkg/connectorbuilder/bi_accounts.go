package connectorbuilder

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (b *builderImpl) CreateAccount(ctx context.Context, request *v2.CreateAccountRequest) (*v2.CreateAccountResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.CreateAccount")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.CreateAccountType
	l := ctxzap.Extract(ctx)
	if b.accountManager == nil {
		l.Error("error: connector does not have account manager configured")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, status.Error(codes.Unimplemented, "connector does not have credential manager configured")
	}

	opts, err := crypto.ConvertCredentialOptions(ctx, b.clientSecret, request.GetCredentialOptions(), request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: converting credential options failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: converting credential options failed: %w", err)
	}

	result, plaintexts, annos, err := b.accountManager.CreateAccount(ctx, request.GetAccountInfo(), opts)
	if err != nil {
		l.Error("error: create account failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: create account failed: %w", err)
	}

	pkem, err := crypto.NewEncryptionManager(request.GetCredentialOptions(), request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: creating encryption manager failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: creating encryption manager failed: %w", err)
	}

	var encryptedDatas []*v2.EncryptedData
	for _, plaintextCredential := range plaintexts {
		encryptedData, err := pkem.Encrypt(ctx, plaintextCredential)
		if err != nil {
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, err
		}
		encryptedDatas = append(encryptedDatas, encryptedData...)
	}

	rv := &v2.CreateAccountResponse{
		EncryptedData: encryptedDatas,
		Annotations:   annos,
	}

	switch r := result.(type) {
	case *v2.CreateAccountResponse_SuccessResult:
		rv.Result = &v2.CreateAccountResponse_Success{Success: r}
	case *v2.CreateAccountResponse_ActionRequiredResult:
		rv.Result = &v2.CreateAccountResponse_ActionRequired{ActionRequired: r}
	default:
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, status.Error(codes.Unimplemented, fmt.Sprintf("unknown result type: %T", result))
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return rv, nil
}

func (b *builderImpl) RotateCredential(ctx context.Context, request *v2.RotateCredentialRequest) (*v2.RotateCredentialResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.RotateCredential")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.RotateCredentialsType
	l := ctxzap.Extract(ctx)
	rt := request.GetResourceId().GetResourceType()
	manager, ok := b.credentialManagers[rt]
	if !ok {
		l.Error("error: resource type does not have credential manager configured", zap.String("resource_type", rt))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, status.Error(codes.Unimplemented, "resource type does not have credential manager configured")
	}

	opts, err := crypto.ConvertCredentialOptions(ctx, b.clientSecret, request.GetCredentialOptions(), request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: converting credential options failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: converting credential options failed: %w", err)
	}

	plaintexts, annos, err := manager.Rotate(ctx, request.GetResourceId(), opts)
	if err != nil {
		l.Error("error: rotate credentials on resource failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: rotate credentials on resource failed: %w", err)
	}

	pkem, err := crypto.NewEncryptionManager(request.GetCredentialOptions(), request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: creating encryption manager failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: creating encryption manager failed: %w", err)
	}

	var encryptedDatas []*v2.EncryptedData
	for _, plaintextCredential := range plaintexts {
		encryptedData, err := pkem.Encrypt(ctx, plaintextCredential)
		if err != nil {
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, err
		}
		encryptedDatas = append(encryptedDatas, encryptedData...)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.RotateCredentialResponse{
		Annotations:   annos,
		ResourceId:    request.GetResourceId(),
		EncryptedData: encryptedDatas,
	}, nil
}
