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
	"google.golang.org/protobuf/proto"
)

// CreateAccountResponse is a semi-opaque type returned from CreateAccount operations.
//
// This is used to communicate the result of account creation back to Baton.
type CreateAccountResponse interface {
	proto.Message
	GetIsCreateAccountResult() bool
}

// AccountManager extends ResourceSyncer to add capabilities for managing user accounts.
//
// Implementing this interface indicates the connector supports creating accounts
// in the external system. A resource type should implement this interface if it
// represents users or accounts that can be provisioned.
type AccountManager interface {
	ResourceSyncer
	AccountManagerLimited
}

type AccountManagerLimited interface {
	CreateAccount(ctx context.Context,
		accountInfo *v2.AccountInfo,
		credentialOptions *v2.LocalCredentialOptions) (CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error)
	CreateAccountCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error)
}

type OldAccountManager interface {
	ResourceSyncer
	CreateAccount(ctx context.Context,
		accountInfo *v2.AccountInfo,
		credentialOptions *v2.CredentialOptions) (CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error)
}

func (b *builder) CreateAccount(ctx context.Context, request *v2.CreateAccountRequest) (*v2.CreateAccountResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.CreateAccount")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.CreateAccountType
	l := ctxzap.Extract(ctx)
	if b.accountManager == nil {
		l.Error("error: connector does not have account manager configured")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, status.Error(codes.Unimplemented, "connector does not have account manager configured")
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

	rv := v2.CreateAccountResponse_builder{
		EncryptedData: encryptedDatas,
		Annotations:   annos,
	}.Build()

	switch r := result.(type) {
	case *v2.CreateAccountResponse_SuccessResult:
		rv.SetSuccess(proto.ValueOrDefault(r))
	case *v2.CreateAccountResponse_ActionRequiredResult:
		rv.SetActionRequired(proto.ValueOrDefault(r))
	default:
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, status.Error(codes.Unimplemented, fmt.Sprintf("unknown result type: %T", result))
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return rv, nil
}

func (b *builder) addAccountManager(_ context.Context, typeId string, in interface{}) error {
	if _, ok := in.(OldAccountManager); ok {
		return fmt.Errorf("error: old account manager interface implemented for %s", typeId)
	}

	if accountManager, ok := in.(AccountManagerLimited); ok {
		// NOTE(kans): currently unused - but these should probably be (resource) typed
		b.accountManagers[typeId] = accountManager
		if b.accountManager != nil {
			return fmt.Errorf("error: duplicate resource type found for account manager %s", typeId)
		}
		b.accountManager = accountManager
	}
	return nil
}
